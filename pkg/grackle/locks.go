package grackle

import (
	"fmt"
	"io"

	"github.com/go-errors/errors"
	"github.com/samber/lo"
	"google.golang.org/protobuf/proto"

	"github.com/evrblk/grackle/pkg/corepb"
	"github.com/evrblk/monstera"
	monsterax "github.com/evrblk/monstera/x"
)

type LocksCore struct {
	globalIndexPrefix []byte
	badgerStore       *monstera.BadgerStore

	locksTable            *monsterax.CompositeKeyTable[*corepb.Lock, corepb.Lock]
	countersTable         *monsterax.SimpleKeyTable[*corepb.LocksCounter, corepb.LocksCounter]
	gcRecordsGlobalIndex  *monsterax.SimpleKeyTable[*corepb.LocksGCRecord, corepb.LocksGCRecord]                                       // Global index
	expirationGlobalIndex *monsterax.SimpleKeyTable[*corepb.LocksExpirationGlobalIndexRecord, corepb.LocksExpirationGlobalIndexRecord] // Global index
}

var _ GrackleLocksCoreApi = &LocksCore{}

func NewLocksCore(badgerStore *monstera.BadgerStore, globalIndexPrefix []byte, shardLowerBound []byte, shardUpperBound []byte) *LocksCore {
	return &LocksCore{
		badgerStore:       badgerStore,
		globalIndexPrefix: globalIndexPrefix,

		locksTable:            monsterax.NewCompositeKeyTable[*corepb.Lock, corepb.Lock](GrackleLocksTableId, shardLowerBound, shardUpperBound),
		countersTable:         monsterax.NewSimpleKeyTable[*corepb.LocksCounter, corepb.LocksCounter](GrackleLocksCountersTableId, shardLowerBound, shardUpperBound),
		gcRecordsGlobalIndex:  monsterax.NewSimpleKeyTable[*corepb.LocksGCRecord, corepb.LocksGCRecord](GrackleLocksGCRecordsGlobalIndexId, globalIndexPrefix, globalIndexPrefix),
		expirationGlobalIndex: monsterax.NewSimpleKeyTable[*corepb.LocksExpirationGlobalIndexRecord, corepb.LocksExpirationGlobalIndexRecord](GrackleLocksExpirationGlobalIndexId, globalIndexPrefix, globalIndexPrefix),
	}
}

func (c *LocksCore) ranges() []monstera.KeyRange {
	return []monstera.KeyRange{
		c.locksTable.GetTableKeyRange(),
		c.countersTable.GetTableKeyRange(),
		c.gcRecordsGlobalIndex.GetTableKeyRange(),
		c.expirationGlobalIndex.GetTableKeyRange(),
	}
}

func (c *LocksCore) Snapshot() monstera.ApplicationCoreSnapshot {
	return monsterax.Snapshot(c.badgerStore, c.ranges())
}

func (c *LocksCore) Restore(reader io.ReadCloser) error {
	return monsterax.Restore(c.badgerStore, c.ranges(), reader)
}

func (c *LocksCore) Close() {

}

func (c *LocksCore) GetLock(request *corepb.GetLockRequest) (*corepb.GetLockResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	lock, err := c.getLock(txn, request.LockId)
	if err != nil {
		if errors.Is(err, monstera.ErrNotFound) {
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
		counters, err := c.getCounters(txn, request.LockId.AccountId, request.LockId.NamespaceName, request.LockId.NamespaceCreatedAt)
		panicIfNotNil(err)

		// Remove a lock from expirationGlobalIndex. It was placed at the ExpiresAt position of an original lock
		expiresAt := c.getEarliestExpiration(lock)
		err = c.expirationGlobalIndex.Delete(txn, locksExpirationGlobalIndexPK(c.globalIndexPrefix, expiresAt, lock.Id.AccountId, lock.Id.NamespaceName, lock.Id.NamespaceCreatedAt, lock.Id.LockName))
		panicIfNotNil(err)

		// Lock is expired, delete it
		err = c.deleteLock(txn, lock.Id)
		panicIfNotNil(err)

		// Update counters
		counters.NumberOfLocks = counters.NumberOfLocks - 1
		err = c.setCounters(txn, request.LockId.AccountId, request.LockId.NamespaceName, request.LockId.NamespaceCreatedAt, counters)
		panicIfNotNil(err)
	} else {
		oldExpiresAt := c.getEarliestExpiration(lock)
		newExpiresAt := c.getEarliestExpiration(updatedLock)

		// Check if earliest expiration changed
		if oldExpiresAt != newExpiresAt {
			// Remove a lock from expirationGlobalIndex at old position
			err = c.deleteExpirationGlobalIndex(txn, oldExpiresAt, lock.Id)
			panicIfNotNil(err)

			// Add a lock into expirationGlobalIndex at new position
			err = c.addExpirationGlobalIndex(txn, newExpiresAt, lock.Id)
			panicIfNotNil(err)
		}

		// Lock is still held, update unexpired holders
		err = c.updateLock(txn, updatedLock)
		panicIfNotNil(err)
	}

	err = txn.Commit()
	panicIfNotNil(err)

	return &corepb.GetLockResponse{
		Lock: updatedLock,
	}, nil
}

func (c *LocksCore) ListLocks(request *corepb.ListLocksRequest) (*corepb.ListLocksResponse, error) {
	txn := c.badgerStore.View()
	defer txn.Discard()

	result, err := c.listLocks(txn, request.NamespaceTimestampedId, request.PaginationToken, getLimit(int(request.Limit)))
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

func (c *LocksCore) DeleteLock(request *corepb.DeleteLockRequest) (*corepb.DeleteLockResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	lock, err := c.getLock(txn, request.LockId)
	if err != nil {
		if errors.Is(err, monstera.ErrNotFound) {
			// No lock exists, do nothing
			return &corepb.DeleteLockResponse{}, nil
		} else {
			panic(err)
		}
	}

	// Get counters for that namespace
	counters, err := c.getCounters(txn, request.LockId.AccountId, request.LockId.NamespaceName, request.LockId.NamespaceCreatedAt)
	panicIfNotNil(err)

	// Remove a lock from expirationGlobalIndex at old position
	expiresAt := c.getEarliestExpiration(lock)
	err = c.deleteExpirationGlobalIndex(txn, expiresAt, lock.Id)
	panicIfNotNil(err)

	err = c.deleteLock(txn, lock.Id)
	panicIfNotNil(err)

	// Update counters
	counters.NumberOfLocks = counters.NumberOfLocks - 1
	err = c.setCounters(txn, request.LockId.AccountId, request.LockId.NamespaceName, request.LockId.NamespaceCreatedAt, counters)
	panicIfNotNil(err)

	err = txn.Commit()
	panicIfNotNil(err)

	return &corepb.DeleteLockResponse{}, nil
}

func (c *LocksCore) AcquireLock(request *corepb.AcquireLockRequest) (*corepb.AcquireLockResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	// Get counters for that namespace
	counters, err := c.getCounters(txn, request.LockId.AccountId, request.LockId.NamespaceName, request.LockId.NamespaceCreatedAt)
	panicIfNotNil(err)

	lock, err := c.getLock(txn, request.LockId)
	if err != nil {
		if errors.Is(err, monstera.ErrNotFound) {
			// No lock exists, create a new one
			lock = &corepb.Lock{
				Id:       request.LockId,
				State:    corepb.LockState_UNLOCKED,
				LockedAt: 0,
			}
			// Increment counter only when a new lock is really created
			counters.NumberOfLocks = counters.NumberOfLocks + 1

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

	// Remove a lock from expirationGlobalIndex at old position
	oldExpiresAt := c.getEarliestExpiration(lock)
	err = c.deleteExpirationGlobalIndex(txn, oldExpiresAt, lock.Id)
	panicIfNotNil(err)

	// Remove expired holders
	updatedLock := c.checkLockExpiration(lock, request.Now)

	lockHolder := &corepb.LockHolder{
		ProcessId: request.ProcessId,
		LockedAt:  request.Now,
		ExpiresAt: request.ExpiresAt,
	}

	switch updatedLock.State {
	case corepb.LockState_UNLOCKED:
		if request.WriteLock {
			// Lock for writes
			updatedLock.State = corepb.LockState_WRITE_LOCKED
			updatedLock.WriteLockHolder = lockHolder
		} else {
			// Lock for reads only
			updatedLock.State = corepb.LockState_READ_LOCKED
			updatedLock.ReadLockHolders = []*corepb.LockHolder{lockHolder}
		}
		updatedLock.LockedAt = request.Now
	case corepb.LockState_READ_LOCKED:
		if request.WriteLock {
			return &corepb.AcquireLockResponse{
				Lock:    updatedLock,
				Success: false, // Already locked for reads, cannot be locked for writes.
			}, nil
		} else {
			// Already locked for reads.
			// Check if the same process_id already holds the lock here.
			existingHolder, ok := lo.Find(updatedLock.ReadLockHolders, func(h *corepb.LockHolder) bool {
				return h.ProcessId == request.ProcessId
			})
			if ok {
				// Update expiration time (extend lock)
				existingHolder.ExpiresAt = request.ExpiresAt
				existingHolder.LockedAt = request.Now
			} else {
				// Add the new lock holder
				updatedLock.ReadLockHolders = append(updatedLock.ReadLockHolders, lockHolder)
			}
		}
	case corepb.LockState_WRITE_LOCKED:
		if request.WriteLock {
			// Already locked for writes. Check if the same process_id already holds the lock here.
			if updatedLock.WriteLockHolder.ProcessId == request.ProcessId {
				// This process_id already holds the lock, repeated locks are considered successful
				// Update expiration time (extend lock)
				updatedLock.WriteLockHolder.ExpiresAt = request.ExpiresAt
				updatedLock.WriteLockHolder.LockedAt = request.Now
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

	err = c.updateLock(txn, updatedLock)
	panicIfNotNil(err)

	// Add a lock into expirationGlobalIndex at new position
	newExpiresAt := c.getEarliestExpiration(updatedLock)
	err = c.addExpirationGlobalIndex(txn, newExpiresAt, lock.Id)
	panicIfNotNil(err)

	// Update counters
	err = c.setCounters(txn, request.LockId.AccountId, request.LockId.NamespaceName, request.LockId.NamespaceCreatedAt, counters)
	panicIfNotNil(err)

	err = txn.Commit()
	panicIfNotNil(err)

	return &corepb.AcquireLockResponse{
		Lock:    updatedLock,
		Success: true, // Locked successfully by the given process_id
	}, nil
}

func (c *LocksCore) ReleaseLock(request *corepb.ReleaseLockRequest) (*corepb.ReleaseLockResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	// Get counters for that namespace
	counters, err := c.getCounters(txn, request.LockId.AccountId, request.LockId.NamespaceName, request.LockId.NamespaceCreatedAt)
	panicIfNotNil(err)

	lock, err := c.getLock(txn, request.LockId)
	if err != nil {
		if errors.Is(err, monstera.ErrNotFound) {
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
		err = c.deleteLock(txn, updatedLock.Id)
		panicIfNotNil(err)

		// Remove a lock from expirationGlobalIndex at old position
		oldExpiresAt := c.getEarliestExpiration(lock)
		err = c.deleteExpirationGlobalIndex(txn, oldExpiresAt, lock.Id)
		panicIfNotNil(err)

		counters.NumberOfLocks = counters.NumberOfLocks - 1
	case corepb.LockState_READ_LOCKED:
		// Remove a lock from expirationGlobalIndex at old position
		oldExpiresAt := c.getEarliestExpiration(lock)
		err = c.addExpirationGlobalIndex(txn, oldExpiresAt, lock.Id)
		panicIfNotNil(err)

		// Remove the holder
		updatedLock.ReadLockHolders = lo.Filter(updatedLock.ReadLockHolders, func(h *corepb.LockHolder, _ int) bool {
			return h.ProcessId != request.ProcessId
		})

		// If no read lock holders left
		if len(updatedLock.ReadLockHolders) == 0 {
			// Unlock
			updatedLock.LockedAt = 0
			updatedLock.State = corepb.LockState_UNLOCKED
			updatedLock.ReadLockHolders = nil

			// Delete lock
			err = c.deleteLock(txn, updatedLock.Id)
			panicIfNotNil(err)

			counters.NumberOfLocks = counters.NumberOfLocks - 1
		} else {
			// Add a lock into expirationGlobalIndex at new position
			newExpiresAt := c.getEarliestExpiration(updatedLock)
			err = c.addExpirationGlobalIndex(txn, newExpiresAt, updatedLock.Id)
			panicIfNotNil(err)

			// Update lock
			err = c.updateLock(txn, updatedLock)
			panicIfNotNil(err)
		}
	case corepb.LockState_WRITE_LOCKED:
		if updatedLock.WriteLockHolder.ProcessId == request.ProcessId {
			// Unlock
			updatedLock.State = corepb.LockState_UNLOCKED
			updatedLock.LockedAt = 0
			updatedLock.WriteLockHolder = nil

			// Delete it
			err = c.deleteLock(txn, updatedLock.Id)
			panicIfNotNil(err)

			// Remove a lock from expirationGlobalIndex at old position
			oldExpiresAt := c.getEarliestExpiration(lock)
			err = c.deleteExpirationGlobalIndex(txn, oldExpiresAt, lock.Id)
			panicIfNotNil(err)

			counters.NumberOfLocks = counters.NumberOfLocks - 1
		}
	default:
		panic("invalid lock state")
	}

	// Update counters
	err = c.setCounters(txn, request.LockId.AccountId, request.LockId.NamespaceName, request.LockId.NamespaceCreatedAt, counters)
	panicIfNotNil(err)

	err = txn.Commit()
	panicIfNotNil(err)

	return &corepb.ReleaseLockResponse{
		Lock: updatedLock,
	}, nil
}

func (c *LocksCore) RunLocksGarbageCollection(request *corepb.RunLocksGarbageCollectionRequest) (*corepb.RunLocksGarbageCollectionResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	visitedLocks := int64(0)

	// List one page of GC records
	gcRecords, err := c.listGCRecords(txn, int(request.GcRecordsPageSize))
	panicIfNotNil(err)

	for _, gcRecord := range gcRecords {
		// Delete counters for that namespace. Will not fail if counters do not exist.
		err := c.deleteCounters(txn, gcRecord.NamespaceTimestampedId.AccountId, gcRecord.NamespaceTimestampedId.NamespaceName, gcRecord.NamespaceTimestampedId.NamespaceCreatedAt)
		panicIfNotNil(err)

		// List one page of locks for that namespace
		result, err := c.listLocks(txn, gcRecord.NamespaceTimestampedId, nil, int(request.GcRecordLocksPageSize))
		panicIfNotNil(err)

		// Delete those locks
		for _, lock := range result.locks {
			visitedLocks++

			// Remove a lock from expirationGlobalIndex
			expiresAt := c.getEarliestExpiration(lock)
			err = c.deleteExpirationGlobalIndex(txn, expiresAt, lock.Id)
			panicIfNotNil(err)

			// Remove from the main table
			err := c.deleteLock(txn, lock.Id)
			panicIfNotNil(err)

			if visitedLocks >= request.MaxVisitedLocks {
				goto commit
			}
		}

		// Delete the deleted namespace if that was the last page of locks
		if result.nextPaginationToken == nil {
			err := c.deleteGCRecord(txn, gcRecord)
			panicIfNotNil(err)
		}
	}

	if visitedLocks < request.MaxVisitedLocks {
		// Delete locks that are expired
		leftBound := locksExpirationGlobalIndexPrefix(c.globalIndexPrefix, 0)
		rightBound := locksExpirationGlobalIndexPrefix(c.globalIndexPrefix, request.Now)
		err = c.expirationGlobalIndex.ListInRange(txn, leftBound, rightBound, false, func(record *corepb.LocksExpirationGlobalIndexRecord) (bool, error) {
			visitedLocks++

			// Get the lock
			lock, err := c.getLock(txn, record.LockId)
			panicIfNotNil(err)

			// Remove a lock from expirationGlobalIndex
			oldExpiresAt := c.getEarliestExpiration(lock)
			err = c.deleteExpirationGlobalIndex(txn, oldExpiresAt, lock.Id)
			panicIfNotNil(err)

			updatedLock := c.checkLockExpiration(lock, request.Now)
			if updatedLock.State == corepb.LockState_UNLOCKED {
				// Get counters for lock's namespace
				counter, err := c.getCounters(txn, record.LockId.AccountId, record.LockId.NamespaceName, record.LockId.NamespaceCreatedAt)
				panicIfNotNil(err)

				// Delete the lock
				err = c.deleteLock(txn, lock.Id)
				panicIfNotNil(err)

				// Update counters
				counter.NumberOfLocks = counter.NumberOfLocks - 1
				err = c.setCounters(txn, record.LockId.AccountId, record.LockId.NamespaceName, record.LockId.NamespaceCreatedAt, counter)
				panicIfNotNil(err)
			} else {
				err = c.updateLock(txn, updatedLock)
				panicIfNotNil(err)

				// Add a lock into expirationGlobalIndex at new position
				newExpiresAt := c.getEarliestExpiration(updatedLock)
				err = c.addExpirationGlobalIndex(txn, newExpiresAt, lock.Id)
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

func (c *LocksCore) LocksDeleteNamespace(request *corepb.LocksDeleteNamespaceRequest) (*corepb.LocksDeleteNamespaceResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	// Mark the namespace as deleted
	err := c.createGCRecord(txn, &corepb.LocksGCRecord{
		Id:                     request.RecordId,
		NamespaceTimestampedId: request.NamespaceTimestampedId,
	})
	panicIfNotNil(err)

	err = txn.Commit()
	panicIfNotNil(err)

	return &corepb.LocksDeleteNamespaceResponse{}, nil
}

// checkLockExpiration ensures that the lock is still held at the moment `now`. Returns an updated copy of the lock.
func (c *LocksCore) checkLockExpiration(lock *corepb.Lock, now int64) *corepb.Lock {
	result := proto.Clone(lock).(*corepb.Lock)

	switch lock.State {
	case corepb.LockState_UNLOCKED:
		// Lock is unlocked, return as is
		return result
	case corepb.LockState_READ_LOCKED:
		result.ReadLockHolders = lo.Filter(result.ReadLockHolders, func(h *corepb.LockHolder, _ int) bool {
			return h.ExpiresAt > now
		})
		if len(result.ReadLockHolders) == 0 {
			result.State = corepb.LockState_UNLOCKED
			result.ReadLockHolders = nil
			result.LockedAt = 0
		}
	case corepb.LockState_WRITE_LOCKED:
		if result.WriteLockHolder.ExpiresAt <= now {
			result.State = corepb.LockState_UNLOCKED
			result.WriteLockHolder = nil
			result.LockedAt = 0
		}
	default:
		panic("invalid lock state")
	}

	return result
}

func (c *LocksCore) getEarliestExpiration(lock *corepb.Lock) int64 {
	expiresAt := int64(0)
	switch lock.State {
	case corepb.LockState_UNLOCKED:
		// An unlocked lock should not be in the expiration index
	case corepb.LockState_READ_LOCKED:
		// Find the earliest expiration among all read lock holders
		for _, h := range lock.ReadLockHolders {
			if expiresAt == 0 || h.ExpiresAt < expiresAt {
				expiresAt = h.ExpiresAt
			}
		}
	case corepb.LockState_WRITE_LOCKED:
		// Expiration from the only write lock holder
		expiresAt = lock.WriteLockHolder.ExpiresAt
	}

	return expiresAt
}

type listLocksResult struct {
	locks                   []*corepb.Lock
	nextPaginationToken     *corepb.PaginationToken
	previousPaginationToken *corepb.PaginationToken
}

func (c *LocksCore) listLocks(txn *monstera.Txn, namespaceTimestampedId *corepb.NamespaceTimestampedId, paginationToken *corepb.PaginationToken, limit int) (*listLocksResult, error) {
	result, err := c.locksTable.ListPaginated(txn, locksTablePK(namespaceTimestampedId.AccountId, namespaceTimestampedId.NamespaceName, namespaceTimestampedId.NamespaceCreatedAt), paginationTokenToMonstera(paginationToken), limit)
	if err != nil {
		return nil, err
	}

	return &listLocksResult{
		locks:                   result.Items,
		nextPaginationToken:     monsteraPaginationTokenToCore(result.NextPaginationToken),
		previousPaginationToken: monsteraPaginationTokenToCore(result.PreviousPaginationToken),
	}, nil

}

func (c *LocksCore) getLock(txn *monstera.Txn, lockId *corepb.LockId) (*corepb.Lock, error) {
	return c.locksTable.Get(txn, locksTablePK(lockId.AccountId, lockId.NamespaceName, lockId.NamespaceCreatedAt), locksTableSK(lockId.LockName))
}

func (c *LocksCore) updateLock(txn *monstera.Txn, lock *corepb.Lock) error {
	return c.locksTable.Set(txn, locksTablePK(lock.Id.AccountId, lock.Id.NamespaceName, lock.Id.NamespaceCreatedAt), locksTableSK(lock.Id.LockName), lock)
}

func (c *LocksCore) deleteLock(txn *monstera.Txn, lockId *corepb.LockId) error {
	return c.locksTable.Delete(txn, locksTablePK(lockId.AccountId, lockId.NamespaceName, lockId.NamespaceCreatedAt), locksTableSK(lockId.LockName))
}

func (c *LocksCore) getCounters(txn *monstera.Txn, accountId uint64, namespaceName string, namespaceCreatedAt int64) (*corepb.LocksCounter, error) {
	countres, err := c.countersTable.Get(txn, locksCountersTablePK(accountId, namespaceName, namespaceCreatedAt))
	if err != nil {
		if errors.Is(err, monstera.ErrNotFound) {
			return &corepb.LocksCounter{
				NamespaceTimestampedId: &corepb.NamespaceTimestampedId{
					AccountId:          accountId,
					NamespaceName:      namespaceName,
					NamespaceCreatedAt: namespaceCreatedAt,
				},
				NumberOfLocks: 0,
			}, nil
		}
		return nil, err
	}
	return countres, nil
}

func (c *LocksCore) setCounters(txn *monstera.Txn, accountId uint64, namespaceName string, namespaceCreatedAt int64, counters *corepb.LocksCounter) error {
	return c.countersTable.Set(txn, locksCountersTablePK(accountId, namespaceName, namespaceCreatedAt), counters)
}

func (c *LocksCore) deleteCounters(txn *monstera.Txn, accountId uint64, namespaceName string, namespaceCreatedAt int64) error {
	return c.countersTable.Delete(txn, locksCountersTablePK(accountId, namespaceName, namespaceCreatedAt))
}

func (c *LocksCore) createGCRecord(txn *monstera.Txn, locksGCRecord *corepb.LocksGCRecord) error {
	return c.gcRecordsGlobalIndex.Set(txn, locksGCRecordsGlobalIndexPK(c.globalIndexPrefix, locksGCRecord.Id), locksGCRecord)
}

func (c *LocksCore) deleteGCRecord(txn *monstera.Txn, locksGCRecord *corepb.LocksGCRecord) error {
	return c.gcRecordsGlobalIndex.Delete(txn, locksGCRecordsGlobalIndexPK(c.globalIndexPrefix, locksGCRecord.Id))
}

func (c *LocksCore) listGCRecords(txn *monstera.Txn, limit int) ([]*corepb.LocksGCRecord, error) {
	result, err := c.gcRecordsGlobalIndex.ListPaginated(txn, nil, limit)
	if err != nil {
		return nil, err
	}
	return result.Items, nil
}

func (c *LocksCore) addExpirationGlobalIndex(txn *monstera.Txn, expiresAt int64, lockId *corepb.LockId) error {
	return c.expirationGlobalIndex.Set(txn,
		locksExpirationGlobalIndexPK(c.globalIndexPrefix, expiresAt, lockId.AccountId, lockId.NamespaceName, lockId.NamespaceCreatedAt, lockId.LockName),
		&corepb.LocksExpirationGlobalIndexRecord{
			ExpiresAt: expiresAt,
			LockId:    lockId,
		},
	)
}

func (c *LocksCore) deleteExpirationGlobalIndex(txn *monstera.Txn, expiresAt int64, lockId *corepb.LockId) error {
	return c.expirationGlobalIndex.Delete(txn,
		locksExpirationGlobalIndexPK(c.globalIndexPrefix, expiresAt, lockId.AccountId, lockId.NamespaceName, lockId.NamespaceCreatedAt, lockId.LockName))
}

// 1. shard key (by account id and namespace name)
// 2. account id
// 3. namespace name
// 4. namespace created at
func locksTablePK(accountId uint64, namespaceName string, namespaceCreatedAt int64) []byte {
	return monstera.ConcatBytes(
		shardByAccountAndNamespace(accountId, namespaceName),
		accountId,
		namespaceName,
		namespaceCreatedAt,
	)
}

// 1. lock name
func locksTableSK(lockName string) []byte {
	return monstera.ConcatBytes(
		lockName,
	)
}

// 1. shard key (by account id and namespace name)
// 2. account id
// 3. namespace name
// 4. namespace created at
func locksCountersTablePK(accountId uint64, namespaceName string, namespaceCreatedAt int64) []byte {
	return monstera.ConcatBytes(
		shardByAccountAndNamespace(accountId, namespaceName),
		accountId,
		namespaceName,
		namespaceCreatedAt,
	)
}

// 1. shard id
// 3. gc record id
func locksGCRecordsGlobalIndexPK(shardId []byte, gcRecordId uint64) []byte {
	return monstera.ConcatBytes(
		shardId,
		gcRecordId,
	)
}

// 1. shard id
// 2. timestamp
// 3. account id
// 4. namespace name
// 5. namespace created at
// 6. lock name
func locksExpirationGlobalIndexPK(shardId []byte, time int64, accountId uint64, namespaceName string, namespaceCreatedAt int64, lockName string) []byte {
	return monstera.ConcatBytes(
		shardId,
		time,
		accountId,
		namespaceName,
		namespaceCreatedAt,
		lockName,
	)
}

// 1. shard id
// 2. timestamp
func locksExpirationGlobalIndexPrefix(shardId []byte, time int64) []byte {
	return monstera.ConcatBytes(
		shardId,
		time,
	)
}
