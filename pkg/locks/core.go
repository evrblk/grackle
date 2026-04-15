package locks

import (
	"errors"
	"fmt"
	"io"

	"github.com/samber/lo"
	"google.golang.org/protobuf/proto"

	"github.com/evrblk/monstera"
	"github.com/evrblk/monstera/store"
	monsterax "github.com/evrblk/monstera/x"

	"github.com/evrblk/grackle/pkg/corepb"
	"github.com/evrblk/grackle/pkg/monsteragen"
	"github.com/evrblk/grackle/pkg/pagination"
)

type Core struct {
	badgerStore *store.BadgerStore

	locks             *locksTable
	ancestors         *lockAncestorsTable
	counters          *countersTable
	gcRecords         *gcRecordsTable
	expirationRecords *expirationRecordsTable
}

var _ monsteragen.GrackleLocksCoreApi = &Core{}

func NewCore(badgerStore *store.BadgerStore, shardGlobalIndexPrefix []byte, shardLowerBound []byte, shardUpperBound []byte) *Core {
	return &Core{
		badgerStore: badgerStore,

		locks:             newLocksTable(shardLowerBound, shardUpperBound),
		ancestors:         newLockAncestorsTable(shardLowerBound, shardUpperBound),
		counters:          newCountersTable(shardLowerBound, shardUpperBound),
		gcRecords:         newGCRecordsTable(shardGlobalIndexPrefix),
		expirationRecords: newExpirationRecordsTable(shardGlobalIndexPrefix),
	}
}

func (c *Core) ranges() []monsterax.KeyRange {
	ranges := []monsterax.KeyRange{
		c.counters.GetTableKeyRange(),
		c.ancestors.GetTableKeyRange(),
		c.gcRecords.GetTableKeyRange(),
		c.expirationRecords.GetTableKeyRange(),
	}

	ranges = append(ranges, c.locks.GetTableKeyRanges()...)

	return ranges
}

func (c *Core) Snapshot() monstera.ApplicationCoreSnapshot {
	return monsterax.Snapshot(c.badgerStore, c.ranges())
}

func (c *Core) Restore(reader io.ReadCloser) error {
	return monsterax.Restore(c.badgerStore, c.ranges(), reader)
}

func (c *Core) Close() {

}

func (c *Core) GetLock(request *corepb.GetLockRequest) (*corepb.GetLockResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	lock, err := c.locks.Get(txn, request.LockId)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			// No lock exists, return an unlocked lock
			return &corepb.GetLockResponse{
				Lock: &corepb.Lock{
					Id:       request.LockId,
					State:    corepb.LockState_UNLOCKED,
					LockedAt: 0,
				},
			}, nil
		} else {
			panic(err)
		}
	}

	// Check expiration
	updatedLock := c.checkLockExpiration(lock, request.Now)
	if updatedLock.State == corepb.LockState_UNLOCKED {
		// Get counters for that namespace
		counters, err := c.counters.Get(txn, request.LockId.AccountId, request.LockId.NamespaceId)
		panicIfNotNil(err)

		// Remove a lock from expirationRecords. It was placed at the ExpiresAt position of an original lock
		expiresAt := c.getEarliestExpiration(lock)
		err = c.expirationRecords.Delete(txn, expiresAt, lock.Id)
		panicIfNotNil(err)

		// Lock is expired, delete it
		err = c.locks.Delete(txn, lock.Id)
		panicIfNotNil(err)

		// Update ancestor entries
		c.decrementAncestors(txn, lock.Id, lock.State == corepb.LockState_EXCLUSIVE_LOCKED)

		// Update counters
		counters.NumberOfLocks -= 1
		err = c.counters.Set(txn, request.LockId.AccountId, request.LockId.NamespaceId, counters)
		panicIfNotNil(err)
	} else {
		oldExpiresAt := c.getEarliestExpiration(lock)
		newExpiresAt := c.getEarliestExpiration(updatedLock)

		// Check if earliest expiration changed
		if oldExpiresAt != newExpiresAt {
			// Remove a lock from expirationRecords at old position
			err = c.expirationRecords.Delete(txn, oldExpiresAt, lock.Id)
			panicIfNotNil(err)

			// Add a lock into expirationRecords at new position
			err = c.expirationRecords.Add(txn, newExpiresAt, lock.Id)
			panicIfNotNil(err)
		}

		// Lock is still held, update unexpired holders
		err = c.locks.Update(txn, updatedLock)
		panicIfNotNil(err)
	}

	err = txn.Commit()
	panicIfNotNil(err)

	return &corepb.GetLockResponse{
		Lock: updatedLock,
	}, nil
}

func (c *Core) ListLocks(request *corepb.ListLocksRequest) (*corepb.ListLocksResponse, error) {
	txn := c.badgerStore.View()
	defer txn.Discard()

	result, err := c.locks.List(txn, request.NamespaceId, request.PaginationToken, pagination.GetLimitWithDefaults(int(request.Limit)))
	panicIfNotNil(err)

	// Check expiration
	refreshedLocks := lo.Map(result.locks, func(lock *corepb.Lock, _ int) *corepb.Lock {
		return c.checkLockExpiration(lock, request.Now)
	})
	lockedLocks := lo.Filter(refreshedLocks, func(lock *corepb.Lock, _ int) bool {
		return lock.State != corepb.LockState_UNLOCKED
	})

	return &corepb.ListLocksResponse{
		Locks:                   lockedLocks,
		NextPaginationToken:     result.nextPaginationToken,
		PreviousPaginationToken: result.previousPaginationToken,
	}, nil
}

func (c *Core) DeleteLock(request *corepb.DeleteLockRequest) (*corepb.DeleteLockResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	lock, err := c.locks.Get(txn, request.LockId)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			// No lock exists, do nothing
			return &corepb.DeleteLockResponse{}, nil
		} else {
			panic(err)
		}
	}

	// Get counters for that namespace
	counters, err := c.counters.Get(txn, request.LockId.AccountId, request.LockId.NamespaceId)
	panicIfNotNil(err)

	// Remove a lock from expirationRecords at old position
	expiresAt := c.getEarliestExpiration(lock)
	err = c.expirationRecords.Delete(txn, expiresAt, lock.Id)
	panicIfNotNil(err)

	err = c.locks.Delete(txn, lock.Id)
	panicIfNotNil(err)

	// Update ancestor entries
	c.decrementAncestors(txn, lock.Id, lock.State == corepb.LockState_EXCLUSIVE_LOCKED)

	// Update counters
	counters.NumberOfLocks -= 1
	err = c.counters.Set(txn, request.LockId.AccountId, request.LockId.NamespaceId, counters)
	panicIfNotNil(err)

	err = txn.Commit()
	panicIfNotNil(err)

	return &corepb.DeleteLockResponse{}, nil
}

func (c *Core) AcquireLock(request *corepb.AcquireLockRequest) (*corepb.AcquireLockResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	// Get counters for that namespace
	counters, err := c.counters.Get(txn, request.LockId.AccountId, request.LockId.NamespaceId)
	panicIfNotNil(err)

	lock, err := c.locks.Get(txn, request.LockId)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			// No lock exists, create a new one
			lock = &corepb.Lock{
				Id:       request.LockId,
				State:    corepb.LockState_UNLOCKED,
				LockedAt: 0,
			}
			// Increment counter only when a new lock is really created
			counters.NumberOfLocks += 1

			// Check the total number of locks
			if counters.NumberOfLocks > request.MaxNumberOfLocksPerNamespace {
				return nil, monsterax.NewErrorWithContext(
					monsterax.ResourceExhausted,
					"max number of locks per namespace reached",
					map[string]string{"limit": fmt.Sprintf("%d", request.MaxNumberOfLocksPerNamespace)})
			}
		} else {
			panic(err)
		}
	}

	// Remove a lock from expirationRecords at old position
	oldExpiresAt := c.getEarliestExpiration(lock)
	err = c.expirationRecords.Delete(txn, oldExpiresAt, lock.Id)
	panicIfNotNil(err)

	// Capture state before expiry check for ancestor tracking
	prevState := lock.State

	// Remove expired holders
	updatedLock := c.checkLockExpiration(lock, request.Now)

	// Check hierarchical conflicts before attempting acquisition
	canAcquire, err := c.checkHierarchicalConflicts(txn, request.LockId, request.Exclusive)
	panicIfNotNil(err)
	if !canAcquire {
		// Hierarchical conflict detected - return failure
		return &corepb.AcquireLockResponse{
			Lock:    updatedLock,
			Success: false,
		}, nil
	}

	lockHolder := &corepb.LockHolder{
		ProcessId: request.ProcessId,
		LockedAt:  request.Now,
		ExpiresAt: request.ExpiresAt,
	}

	switch updatedLock.State {
	case corepb.LockState_UNLOCKED:
		if request.Exclusive {
			// Lock for writes
			updatedLock.State = corepb.LockState_EXCLUSIVE_LOCKED
			updatedLock.LockHolders = []*corepb.LockHolder{lockHolder}
		} else {
			// Lock for reads only
			updatedLock.State = corepb.LockState_SHARED_LOCKED
			updatedLock.LockHolders = []*corepb.LockHolder{lockHolder}
		}
		updatedLock.LockedAt = request.Now
	case corepb.LockState_SHARED_LOCKED:
		if request.Exclusive {
			return &corepb.AcquireLockResponse{
				Lock:    updatedLock,
				Success: false, // Already locked for reads, cannot be locked for writes.
			}, nil
		} else {
			// Already locked for reads.
			// Check if the same process_id already holds the lock here.
			existingHolder, ok := lo.Find(updatedLock.LockHolders, func(h *corepb.LockHolder) bool {
				return h.ProcessId == request.ProcessId
			})
			if ok {
				// Update expiration time (extend lock)
				existingHolder.ExpiresAt = request.ExpiresAt
				existingHolder.LockedAt = request.Now
			} else {
				// Add the new lock holder
				updatedLock.LockHolders = append(updatedLock.LockHolders, lockHolder)
			}
		}
	case corepb.LockState_EXCLUSIVE_LOCKED:
		if request.Exclusive {
			// Already locked for writes. Check if the same process_id already holds the lock here.
			if updatedLock.LockHolders[0].ProcessId == request.ProcessId {
				// This process_id already holds the lock, repeated locks are considered successful
				// Update expiration time (extend lock)
				updatedLock.LockHolders[0].ExpiresAt = request.ExpiresAt
				updatedLock.LockHolders[0].LockedAt = request.Now
			} else {
				return &corepb.AcquireLockResponse{
					Lock:    updatedLock,
					Success: false, // The lock is held by another process
				}, nil
			}

		} else {
			return &corepb.AcquireLockResponse{
				Lock:    updatedLock,
				Success: false, // Already locked for writes, cannot be locked for reads.
			}, nil

		}
	default:
		panic("invalid lock state")
	}

	err = c.locks.Update(txn, updatedLock)
	panicIfNotNil(err)

	// Update ancestor entries based on lock state transition.
	// prevState is the state from DB (UNLOCKED for a brand new lock).
	// updatedLock.State is the final acquired state.
	if prevState == corepb.LockState_UNLOCKED && updatedLock.State != corepb.LockState_UNLOCKED {
		// New lock record: increment ancestor counters
		c.incrementAncestors(txn, request.LockId, updatedLock.State == corepb.LockState_EXCLUSIVE_LOCKED)
	} else if prevState != corepb.LockState_UNLOCKED && updatedLock.State != corepb.LockState_UNLOCKED && prevState != updatedLock.State {
		// Lock was expired and re-acquired with a different mode: swap ancestor mode
		c.swapAncestorMode(txn, request.LockId,
			prevState == corepb.LockState_EXCLUSIVE_LOCKED,
			updatedLock.State == corepb.LockState_EXCLUSIVE_LOCKED)
	}

	// Add a lock into expirationRecords at new position
	newExpiresAt := c.getEarliestExpiration(updatedLock)
	err = c.expirationRecords.Add(txn, newExpiresAt, lock.Id)
	panicIfNotNil(err)

	// Update counters
	err = c.counters.Set(txn, request.LockId.AccountId, request.LockId.NamespaceId, counters)
	panicIfNotNil(err)

	err = txn.Commit()
	panicIfNotNil(err)

	return &corepb.AcquireLockResponse{
		Lock:    updatedLock,
		Success: true, // Locked successfully by the given process_id
	}, nil
}

func (c *Core) ReleaseLock(request *corepb.ReleaseLockRequest) (*corepb.ReleaseLockResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	// Get counters for that namespace
	counters, err := c.counters.Get(txn, request.LockId.AccountId, request.LockId.NamespaceId)
	panicIfNotNil(err)

	lock, err := c.locks.Get(txn, request.LockId)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			// No lock exists, return an unlocked lock
			return &corepb.ReleaseLockResponse{
				Lock: &corepb.Lock{
					Id:       request.LockId,
					State:    corepb.LockState_UNLOCKED,
					LockedAt: 0,
				},
			}, nil
		} else {
			panic(err)
		}
	}

	// Lock exists, lets check if it expired
	updatedLock := c.checkLockExpiration(lock, request.Now)

	switch updatedLock.State {
	case corepb.LockState_UNLOCKED:
		// Lock has expired, delete it
		err = c.locks.Delete(txn, updatedLock.Id)
		panicIfNotNil(err)

		// Remove a lock from expirationRecords at old position
		oldExpiresAt := c.getEarliestExpiration(lock)
		err = c.expirationRecords.Delete(txn, oldExpiresAt, lock.Id)
		panicIfNotNil(err)

		// Update ancestor entries
		c.decrementAncestors(txn, lock.Id, lock.State == corepb.LockState_EXCLUSIVE_LOCKED)

		counters.NumberOfLocks -= 1
	case corepb.LockState_SHARED_LOCKED:
		// Remove a lock from expirationRecords at old position
		oldExpiresAt := c.getEarliestExpiration(lock)
		err = c.expirationRecords.Add(txn, oldExpiresAt, lock.Id)
		panicIfNotNil(err)

		// Remove the holder
		updatedLock.LockHolders = lo.Filter(updatedLock.LockHolders, func(h *corepb.LockHolder, _ int) bool {
			return h.ProcessId != request.ProcessId
		})

		// If no read lock holders left
		if len(updatedLock.LockHolders) == 0 {
			// Unlock
			updatedLock.LockedAt = 0
			updatedLock.State = corepb.LockState_UNLOCKED
			updatedLock.LockHolders = nil

			// Delete lock
			err = c.locks.Delete(txn, updatedLock.Id)
			panicIfNotNil(err)

			// Update ancestor entries
			c.decrementAncestors(txn, lock.Id, false)

			counters.NumberOfLocks -= 1
		} else {
			// Add a lock into expirationRecords at new position
			newExpiresAt := c.getEarliestExpiration(updatedLock)
			err = c.expirationRecords.Add(txn, newExpiresAt, updatedLock.Id)
			panicIfNotNil(err)

			// Update lock
			err = c.locks.Update(txn, updatedLock)
			panicIfNotNil(err)
		}
	case corepb.LockState_EXCLUSIVE_LOCKED:
		if updatedLock.LockHolders[0].ProcessId == request.ProcessId {
			// Unlock
			updatedLock.State = corepb.LockState_UNLOCKED
			updatedLock.LockedAt = 0
			updatedLock.LockHolders = nil

			// Delete it
			err = c.locks.Delete(txn, updatedLock.Id)
			panicIfNotNil(err)

			// Remove a lock from expirationRecords at old position
			oldExpiresAt := c.getEarliestExpiration(lock)
			err = c.expirationRecords.Delete(txn, oldExpiresAt, lock.Id)
			panicIfNotNil(err)

			// Update ancestor entries
			c.decrementAncestors(txn, lock.Id, true)

			counters.NumberOfLocks -= 1
		}
	default:
		panic("invalid lock state")
	}

	// Update counters
	err = c.counters.Set(txn, request.LockId.AccountId, request.LockId.NamespaceId, counters)
	panicIfNotNil(err)

	err = txn.Commit()
	panicIfNotNil(err)

	return &corepb.ReleaseLockResponse{
		Lock: updatedLock,
	}, nil
}

func (c *Core) RunLocksGarbageCollection(request *corepb.RunLocksGarbageCollectionRequest) (*corepb.RunLocksGarbageCollectionResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	visitedLocks := int64(0)

	// List one page of GC records
	gcRecords, err := c.gcRecords.List(txn, int(request.GcRecordsPageSize))
	panicIfNotNil(err)

	for _, gcRecord := range gcRecords {
		// Delete counters for that namespace. Will not fail if counters do not exist.
		err := c.counters.Delete(txn, gcRecord.NamespaceId.AccountId, gcRecord.NamespaceId.NamespaceId)
		panicIfNotNil(err)

		// List one page of locks for that namespace
		result, err := c.locks.List(txn, gcRecord.NamespaceId, nil, int(request.GcRecordLocksPageSize))
		panicIfNotNil(err)

		// Delete those locks
		for _, lock := range result.locks {
			visitedLocks++

			// Remove a lock from expirationRecords
			expiresAt := c.getEarliestExpiration(lock)
			err = c.expirationRecords.Delete(txn, expiresAt, lock.Id)
			panicIfNotNil(err)

			// Remove from the main table
			err := c.locks.Delete(txn, lock.Id)
			panicIfNotNil(err)

			// Update ancestor entries
			c.decrementAncestors(txn, lock.Id, lock.State == corepb.LockState_EXCLUSIVE_LOCKED)

			if visitedLocks >= request.MaxVisitedLocks {
				goto commit
			}
		}

		// Delete the deleted namespace if that was the last page of locks
		if result.nextPaginationToken == nil {
			err := c.gcRecords.Delete(txn, gcRecord)
			panicIfNotNil(err)
		}
	}

	if visitedLocks < request.MaxVisitedLocks {
		// Delete locks that are expired
		err = c.expirationRecords.List(txn, 0, request.Now, func(record *corepb.LocksExpirationRecord) (bool, error) {
			visitedLocks++

			// Get the lock
			lock, err := c.locks.Get(txn, record.LockId)
			panicIfNotNil(err)

			// Remove a lock from expirationRecords
			oldExpiresAt := c.getEarliestExpiration(lock)
			err = c.expirationRecords.Delete(txn, oldExpiresAt, lock.Id)
			panicIfNotNil(err)

			updatedLock := c.checkLockExpiration(lock, request.Now)
			if updatedLock.State == corepb.LockState_UNLOCKED {
				// Get counters for lock's namespace
				counter, err := c.counters.Get(txn, record.LockId.AccountId, record.LockId.NamespaceId)
				panicIfNotNil(err)

				// Delete the lock
				err = c.locks.Delete(txn, lock.Id)
				panicIfNotNil(err)

				// Update ancestor entries
				c.decrementAncestors(txn, lock.Id, lock.State == corepb.LockState_EXCLUSIVE_LOCKED)

				// Update counters
				counter.NumberOfLocks -= 1
				err = c.counters.Set(txn, record.LockId.AccountId, record.LockId.NamespaceId, counter)
				panicIfNotNil(err)
			} else {
				err = c.locks.Update(txn, updatedLock)
				panicIfNotNil(err)

				// Add a lock into expirationRecords at new position
				newExpiresAt := c.getEarliestExpiration(updatedLock)
				err = c.expirationRecords.Add(txn, newExpiresAt, lock.Id)
				panicIfNotNil(err)
			}

			// Stop if we have visited enough locks
			return visitedLocks < request.MaxVisitedLocks, nil
		})
	}

commit:

	err = txn.Commit()
	panicIfNotNil(err)

	return &corepb.RunLocksGarbageCollectionResponse{}, nil
}

func (c *Core) LocksDeleteNamespace(request *corepb.LocksDeleteNamespaceRequest) (*corepb.LocksDeleteNamespaceResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	// Mark the namespace as deleted
	err := c.gcRecords.Create(txn, &corepb.LocksGarbageCollectionRecord{
		Id:          request.RecordId,
		NamespaceId: request.NamespaceId,
	})
	panicIfNotNil(err)

	err = txn.Commit()
	panicIfNotNil(err)

	return &corepb.LocksDeleteNamespaceResponse{}, nil
}

// checkLockExpiration ensures that the lock is still held at the moment `now`. Returns an updated copy of the lock.
func (c *Core) checkLockExpiration(lock *corepb.Lock, now int64) *corepb.Lock {
	result := proto.Clone(lock).(*corepb.Lock)

	switch lock.State {
	case corepb.LockState_UNLOCKED:
		// Lock is unlocked, return as is
		return result
	case corepb.LockState_SHARED_LOCKED:
		result.LockHolders = lo.Filter(result.LockHolders, func(h *corepb.LockHolder, _ int) bool {
			return h.ExpiresAt > now
		})
		if len(result.LockHolders) == 0 {
			result.State = corepb.LockState_UNLOCKED
			result.LockHolders = nil
			result.LockedAt = 0
		}
	case corepb.LockState_EXCLUSIVE_LOCKED:
		if result.LockHolders[0].ExpiresAt <= now {
			result.State = corepb.LockState_UNLOCKED
			result.LockHolders = nil
			result.LockedAt = 0
		}
	default:
		panic("invalid lock state")
	}

	return result
}

func (c *Core) getEarliestExpiration(lock *corepb.Lock) int64 {
	expiresAt := int64(0)
	switch lock.State {
	case corepb.LockState_UNLOCKED:
		// An unlocked lock should not be in the expiration index
	case corepb.LockState_SHARED_LOCKED:
		// Find the earliest expiration among all shared lock holders
		for _, h := range lock.LockHolders {
			if expiresAt == 0 || h.ExpiresAt < expiresAt {
				expiresAt = h.ExpiresAt
			}
		}
	case corepb.LockState_EXCLUSIVE_LOCKED:
		// Expiration from the only exclusive lock holder
		expiresAt = lock.LockHolders[0].ExpiresAt
	}

	return expiresAt
}

// incrementAncestors increments the ancestor counter for each path prefix of the given lock name.
// Called when a lock transitions from UNLOCKED to LOCKED for the first time (new lock record).
func (c *Core) incrementAncestors(txn *store.Txn, lockId *corepb.LockId, exclusive bool) {
	for _, ancestorName := range lockAncestorNames(lockId.LockName) {
		ancestorId := &corepb.LockId{
			AccountId:   lockId.AccountId,
			NamespaceId: lockId.NamespaceId,
			LockName:    ancestorName,
		}
		ancestor, err := c.ancestors.Get(txn, ancestorId)
		panicIfNotNil(err)
		ancestor.Id = ancestorId
		if exclusive {
			ancestor.ExclusiveCount++
		} else {
			ancestor.SharedCount++
		}
		err = c.ancestors.Set(txn, ancestor)
		panicIfNotNil(err)
	}
}

// decrementAncestors decrements the ancestor counter for each path prefix of the given lock name.
// Called when a lock record is deleted (last holder released or expired).
func (c *Core) decrementAncestors(txn *store.Txn, lockId *corepb.LockId, wasExclusive bool) {
	for _, ancestorName := range lockAncestorNames(lockId.LockName) {
		ancestorId := &corepb.LockId{
			AccountId:   lockId.AccountId,
			NamespaceId: lockId.NamespaceId,
			LockName:    ancestorName,
		}
		ancestor, err := c.ancestors.Get(txn, ancestorId)
		panicIfNotNil(err)
		if wasExclusive {
			ancestor.ExclusiveCount--
		} else {
			ancestor.SharedCount--
		}
		if ancestor.ExclusiveCount <= 0 && ancestor.SharedCount <= 0 {
			err = c.ancestors.Delete(txn, ancestorId)
		} else {
			ancestor.Id = ancestorId
			err = c.ancestors.Set(txn, ancestor)
		}
		panicIfNotNil(err)
	}
}

// swapAncestorMode swaps the mode contribution of a lock in ancestor entries.
// Called when a lock that was expired (in-memory UNLOCKED) is re-acquired with a different mode.
func (c *Core) swapAncestorMode(txn *store.Txn, lockId *corepb.LockId, wasExclusive bool, isExclusive bool) {
	if wasExclusive == isExclusive {
		return
	}
	for _, ancestorName := range lockAncestorNames(lockId.LockName) {
		ancestorId := &corepb.LockId{
			AccountId:   lockId.AccountId,
			NamespaceId: lockId.NamespaceId,
			LockName:    ancestorName,
		}
		ancestor, err := c.ancestors.Get(txn, ancestorId)
		panicIfNotNil(err)
		if wasExclusive {
			ancestor.ExclusiveCount--
			ancestor.SharedCount++
		} else {
			ancestor.SharedCount--
			ancestor.ExclusiveCount++
		}
		ancestor.Id = ancestorId
		err = c.ancestors.Set(txn, ancestor)
		panicIfNotNil(err)
	}
}

// checkAncestorConflicts verifies that no ancestor locks block the requested lock.
// Returns (true, nil) if no conflicts, (false, nil) if blocked by ancestor.
func (c *Core) checkAncestorConflicts(txn *store.Txn, lockId *corepb.LockId, requestExclusive bool) (bool, error) {
	ancestors := lockAncestorNames(lockId.LockName)
	for _, ancestorName := range ancestors {
		ancestorId := &corepb.LockId{
			AccountId:   lockId.AccountId,
			NamespaceId: lockId.NamespaceId,
			LockName:    ancestorName,
		}

		// Check if there's an actual lock on this ancestor path
		ancestorLock, err := c.locks.Get(txn, ancestorId)
		if err != nil {
			if err == store.ErrNotFound {
				// No lock on this ancestor, continue checking others
				continue
			}
			return false, err
		}

		// Any ancestor with exclusive lock blocks everything
		if ancestorLock.State == corepb.LockState_EXCLUSIVE_LOCKED {
			return false, nil
		}

		// Ancestor shared lock blocks descendant exclusive
		if requestExclusive && ancestorLock.State == corepb.LockState_SHARED_LOCKED {
			return false, nil
		}
	}
	return true, nil
}

// checkDescendantConflicts verifies that no descendant locks block the requested lock.
// Returns (true, nil) if no conflicts, (false, nil) if blocked by descendants.
func (c *Core) checkDescendantConflicts(txn *store.Txn, lockId *corepb.LockId, requestExclusive bool) (bool, error) {
	// Check the ancestors table for this path to see if it has any descendants with locks
	ancestor, err := c.ancestors.Get(txn, lockId)
	if err != nil {
		return false, err
	}

	// Any descendant with exclusive lock blocks everything
	if ancestor.ExclusiveCount > 0 {
		return false, nil
	}

	// Any descendant lock blocks ancestor exclusive
	if requestExclusive && ancestor.SharedCount > 0 {
		return false, nil
	}

	return true, nil
}

// checkHierarchicalConflicts checks both ancestor and descendant conflicts.
// Returns (true, nil) if lock can be acquired, (false, nil) if blocked.
func (c *Core) checkHierarchicalConflicts(txn *store.Txn, lockId *corepb.LockId, requestExclusive bool) (bool, error) {
	// Check ancestors first (direct lock lookups)
	ancestorOK, err := c.checkAncestorConflicts(txn, lockId, requestExclusive)
	if err != nil {
		return false, err
	}
	if !ancestorOK {
		return false, nil
	}

	// Check descendants (ancestor table lookup for descendant counts)
	return c.checkDescendantConflicts(txn, lockId, requestExclusive)
}

func panicIfNotNil(err error) {
	if err != nil {
		panic(err)
	}
}
