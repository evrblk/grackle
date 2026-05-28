package locks

import (
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/samber/lo"
	"google.golang.org/protobuf/proto"

	"github.com/evrblk/monstera"
	"github.com/evrblk/monstera/store"
	monsterax "github.com/evrblk/monstera/x"

	"github.com/evrblk/grackle/pkg/common"
	"github.com/evrblk/grackle/pkg/corepb"
	"github.com/evrblk/grackle/pkg/ids"
	"github.com/evrblk/grackle/pkg/monsteragen"
	"github.com/evrblk/grackle/pkg/pagination"
	"github.com/evrblk/grackle/pkg/tables"
)

type Core struct {
	badgerStore *store.BadgerStore

	locks     *locksTable
	ancestors *lockAncestorsTable
	counters  *countersTable
	gcRecords *gcRecordsTable
	leases    *common.LeasesTable
}

var _ monsteragen.GrackleLocksCoreApi = &Core{}

func NewCore(badgerStore *store.BadgerStore, shardGlobalIndexPrefix []byte, shardLowerBound []byte, shardUpperBound []byte) *Core {
	return &Core{
		badgerStore: badgerStore,

		locks:     newLocksTable(shardLowerBound, shardUpperBound),
		ancestors: newLockAncestorsTable(shardLowerBound, shardUpperBound),
		counters:  newCountersTable(shardLowerBound, shardUpperBound),
		gcRecords: newGCRecordsTable(shardGlobalIndexPrefix),
		leases: common.NewLeasesTable(
			shardLowerBound,
			shardUpperBound,
			shardGlobalIndexPrefix,
			tables.Grackle["Grackle.LocksCore.Leases.Table"].Bytes(),
			tables.Grackle["Grackle.LocksCore.Leases.ProcessIdIndex"].Bytes(),
			tables.Grackle["Grackle.LocksCore.Leases.ExpirationIndex"].Bytes(),
		),
	}
}

func (c *Core) ranges() []monsterax.KeyRange {
	ranges := []monsterax.KeyRange{
		c.counters.GetTableKeyRange(),
		c.ancestors.GetTableKeyRange(),
		c.gcRecords.GetTableKeyRange(),
	}

	ranges = append(ranges, c.locks.GetTableKeyRanges()...)
	ranges = append(ranges, c.leases.GetTableKeyRanges()...)

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
	updatedLock, err := c.checkLockExpiration(txn, lock, request.Now)
	panicIfNotNil(err)

	if updatedLock.State == corepb.LockState_UNLOCKED {
		// Get counters for that namespace
		counters, err := c.counters.Get(txn, request.LockId.AccountId, request.LockId.NamespaceId)
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
	lockedLocks := make([]*corepb.Lock, 0, len(result.locks))
	for _, lock := range result.locks {
		refreshedLock, err := c.checkLockExpiration(txn, lock, request.Now)
		panicIfNotNil(err)
		if refreshedLock.State != corepb.LockState_UNLOCKED {
			lockedLocks = append(lockedLocks, refreshedLock)
		}
	}

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

	// Validate and get the lease
	lease, err := c.leases.Get(txn, &corepb.LeaseId{
		AccountId:   request.LockId.AccountId,
		NamespaceId: request.LockId.NamespaceId,
		LeaseId:     request.LeaseId,
	})
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, monsterax.NewErrorWithContext(
				monsterax.NotFound,
				"lease not found",
				map[string]string{
					"lease_id": fmt.Sprintf("%d", request.LeaseId),
				})
		}
		panic(err)
	}

	// Check if lease has expired
	if lease.ExpiresAt <= request.Now {
		return nil, monsterax.NewErrorWithContext(
			monsterax.NotFound,
			"lease not found",
			map[string]string{
				"lease_id": fmt.Sprintf("%d", request.LeaseId),
			})
	}

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
					map[string]string{
						"limit": fmt.Sprintf("%d", request.MaxNumberOfLocksPerNamespace),
					})
			}
		} else {
			panic(err)
		}
	}

	// Capture state before expiry check for ancestor tracking
	prevState := lock.State

	// Remove expired holders
	updatedLock, err := c.checkLockExpiration(txn, lock, request.Now)
	panicIfNotNil(err)

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
		LeaseId:  request.LeaseId,
		LockedAt: request.Now,
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
			// Check if the same lease_id already holds the lock here.
			existingHolder, ok := lo.Find(updatedLock.LockHolders, func(h *corepb.LockHolder) bool {
				return h.LeaseId == request.LeaseId
			})
			if ok {
				// Update locked_at time (refresh lock acquisition time)
				existingHolder.LockedAt = request.Now
			} else {
				// Add the new lock holder
				updatedLock.LockHolders = append(updatedLock.LockHolders, lockHolder)
			}
		}
	case corepb.LockState_EXCLUSIVE_LOCKED:
		if request.Exclusive {
			// Already locked for writes. Check if the same lease_id already holds the lock here.
			if updatedLock.LockHolders[0].LeaseId == request.LeaseId {
				// This lease already holds the lock, repeated locks are considered successful
				// Update locked_at time (refresh lock acquisition time)
				updatedLock.LockHolders[0].LockedAt = request.Now
			} else {
				return &corepb.AcquireLockResponse{
					Lock:    updatedLock,
					Success: false, // The lock is held by another lease
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

	// Update counters
	err = c.counters.Set(txn, request.LockId.AccountId, request.LockId.NamespaceId, counters)
	panicIfNotNil(err)

	err = txn.Commit()
	panicIfNotNil(err)

	return &corepb.AcquireLockResponse{
		Lock:    updatedLock,
		Success: true, // Locked successfully by the given lease
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
	updatedLock, err := c.checkLockExpiration(txn, lock, request.Now)
	panicIfNotNil(err)

	switch updatedLock.State {
	case corepb.LockState_UNLOCKED:
		// Lock has expired, delete it
		err = c.locks.Delete(txn, updatedLock.Id)
		panicIfNotNil(err)

		// Update ancestor entries
		c.decrementAncestors(txn, lock.Id, lock.State == corepb.LockState_EXCLUSIVE_LOCKED)

		counters.NumberOfLocks -= 1
	case corepb.LockState_SHARED_LOCKED:
		// Remove the holder
		updatedLock.LockHolders = lo.Filter(updatedLock.LockHolders, func(h *corepb.LockHolder, _ int) bool {
			return h.LeaseId != request.LeaseId
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
			// Update lock
			err = c.locks.Update(txn, updatedLock)
			panicIfNotNil(err)
		}
	case corepb.LockState_EXCLUSIVE_LOCKED:
		if updatedLock.LockHolders[0].LeaseId == request.LeaseId {
			// Unlock
			updatedLock.State = corepb.LockState_UNLOCKED
			updatedLock.LockedAt = 0
			updatedLock.LockHolders = nil

			// Delete it
			err = c.locks.Delete(txn, updatedLock.Id)
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
		// Clean up expired leases and their associated locks
		err = c.leases.ListByExpiration(txn, 0, request.Now, func(lease *corepb.Lease) (bool, error) {
			// List all locks held by this expired lease
			locksResult, err := c.locks.ListByLeaseId(txn, lease.Id, nil, 1000)
			if err != nil {
				return false, err
			}

			// Release all locks held by this expired lease
			for _, lock := range locksResult.locks {
				visitedLocks++

				// Get the lock to update it
				updatedLock, err := c.checkLockExpiration(txn, lock, request.Now)
				panicIfNotNil(err)

				if updatedLock.State == corepb.LockState_UNLOCKED {
					// Get counters for lock's namespace
					counter, err := c.counters.Get(txn, lock.Id.AccountId, lock.Id.NamespaceId)
					counterNotFound := errors.Is(err, store.ErrNotFound)
					if err != nil && !counterNotFound {
						return false, err
					}

					// Delete the lock
					err = c.locks.Delete(txn, lock.Id)
					if err != nil && !errors.Is(err, store.ErrNotFound) {
						return false, err
					}

					// Update ancestor entries
					c.decrementAncestors(txn, lock.Id, lock.State == corepb.LockState_EXCLUSIVE_LOCKED)

					// Update counters only if they exist
					if !counterNotFound {
						counter.NumberOfLocks -= 1
						err = c.counters.Set(txn, lock.Id.AccountId, lock.Id.NamespaceId, counter)
						if err != nil {
							return false, err
						}
					}
				} else {
					// Lock still has unexpired holders, update it
					err = c.locks.Update(txn, updatedLock)
					if err != nil {
						return false, err
					}
				}

				if visitedLocks >= request.MaxVisitedLocks {
					return false, nil // Stop processing
				}
			}

			// Delete the expired lease
			err = c.leases.Delete(txn, lease)
			if err != nil && !errors.Is(err, store.ErrNotFound) {
				return false, err
			}

			// Continue if we haven't reached the limit
			return visitedLocks < request.MaxVisitedLocks, nil
		})
		if err != nil && !errors.Is(err, store.ErrNotFound) {
			panic(err)
		}
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

func (c *Core) CreateLockLease(request *corepb.CreateLockLeaseRequest) (*corepb.CreateLockLeaseResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	// TODO: check max number of lock leases

	// Calculate expiration time
	expiresAt := request.Now + int64(request.TtlSeconds)*1e9

	// Create the lease
	lease := &corepb.Lease{
		Id:        request.LeaseId,
		ProcessId: request.ProcessId,
		CreatedAt: request.Now,
		ExpiresAt: expiresAt,
	}

	err := c.leases.Create(txn, lease)
	panicIfNotNil(err)

	// TODO: add to expiration index

	err = txn.Commit()
	panicIfNotNil(err)

	return &corepb.CreateLockLeaseResponse{
		Lease: lease,
	}, nil
}

func (c *Core) GetLockLease(request *corepb.GetLockLeaseRequest) (*corepb.GetLockLeaseResponse, error) {
	txn := c.badgerStore.View()
	defer txn.Discard()

	lease, err := c.leases.Get(txn, request.LeaseId)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, monsterax.NewErrorWithContext(
				monsterax.NotFound,
				"lease not found",
				map[string]string{
					"lease_id": ids.EncodeLeaseId(request.LeaseId),
				})
		}
		panic(err)
	}

	return &corepb.GetLockLeaseResponse{
		Lease: lease,
	}, nil
}

func (c *Core) ListLockLeases(request *corepb.ListLockLeasesRequest) (*corepb.ListLockLeasesResponse, error) {
	txn := c.badgerStore.View()
	defer txn.Discard()

	result, err := c.leases.List(txn, request.NamespaceId, request.PaginationToken, pagination.GetLimitWithDefaults(int(request.Limit)))
	panicIfNotNil(err)

	// Filter out expired leases
	activeLease := lo.Filter(result.Leases, func(lease *corepb.Lease, _ int) bool {
		return lease.ExpiresAt > request.Now
	})

	return &corepb.ListLockLeasesResponse{
		Leases:                  activeLease,
		NextPaginationToken:     result.NextPaginationToken,
		PreviousPaginationToken: result.PreviousPaginationToken,
	}, nil
}

func (c *Core) RefreshLockLease(request *corepb.RefreshLockLeaseRequest) (*corepb.RefreshLockLeaseResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	// TODO: revoke if expired

	lease, err := c.leases.Get(txn, request.LeaseId)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, monsterax.NewErrorWithContext(
				monsterax.NotFound,
				"lease not found",
				map[string]string{
					"lease_id": ids.EncodeLeaseId(request.LeaseId),
				})
		}
		panic(err)
	}

	// Update the expiration time
	lease.ExpiresAt = request.Now + int64(request.TtlSeconds)*1e9

	// Save the updated lease
	err = c.leases.Update(txn, lease)
	panicIfNotNil(err)

	err = txn.Commit()
	panicIfNotNil(err)

	return &corepb.RefreshLockLeaseResponse{}, nil
}

func (c *Core) RevokeLockLease(request *corepb.RevokeLockLeaseRequest) (*corepb.RevokeLockLeaseResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	// Check if the lease exists
	lease, err := c.leases.Get(txn, request.LeaseId)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			// Lease doesn't exist, nothing to do
			return &corepb.RevokeLockLeaseResponse{}, nil
		}
		panic(err)
	}

	// Delete the lease
	err = c.leases.Delete(txn, lease)
	panicIfNotNil(err)

	// TODO: When lease-lock integration is added, release all locks attached to this lease here

	err = txn.Commit()
	panicIfNotNil(err)

	return &corepb.RevokeLockLeaseResponse{}, nil
}

func (c *Core) ListLockLeasesByProcessId(request *corepb.ListLockLeasesByProcessIdRequest) (*corepb.ListLockLeasesByProcessIdResponse, error) {
	txn := c.badgerStore.View()
	defer txn.Discard()

	result, err := c.leases.ListByProcessId(txn, request.NamespaceId, request.ProcessId, request.PaginationToken, pagination.GetLimitWithDefaults(int(request.Limit)))
	panicIfNotNil(err)

	// Filter out expired leases
	activeLeases := lo.Filter(result.Leases, func(lease *corepb.Lease, _ int) bool {
		return lease.ExpiresAt > request.Now
	})

	return &corepb.ListLockLeasesByProcessIdResponse{
		Leases:                  activeLeases,
		NextPaginationToken:     result.NextPaginationToken,
		PreviousPaginationToken: result.PreviousPaginationToken,
	}, nil
}

func (c *Core) ListLocksByLeaseId(request *corepb.ListLocksByLeaseIdRequest) (*corepb.ListLocksByLeaseIdResponse, error) {
	txn := c.badgerStore.View()
	defer txn.Discard()

	result, err := c.locks.ListByLeaseId(txn, request.LeaseId, request.PaginationToken, pagination.GetLimitWithDefaults(int(request.Limit)))
	panicIfNotNil(err)

	// Check expiration
	lockedLocks := make([]*corepb.Lock, 0, len(result.locks))
	for _, lock := range result.locks {
		refreshedLock, err := c.checkLockExpiration(txn, lock, request.Now)
		if err != nil {
			return nil, err
		}
		if refreshedLock.State != corepb.LockState_UNLOCKED {
			lockedLocks = append(lockedLocks, refreshedLock)
		}
	}

	return &corepb.ListLocksByLeaseIdResponse{
		Locks:                   lockedLocks,
		NextPaginationToken:     result.nextPaginationToken,
		PreviousPaginationToken: result.previousPaginationToken,
	}, nil
}

// checkLockExpiration ensures that the lock is still held at the moment `now`. Returns an updated copy of the lock.
func (c *Core) checkLockExpiration(txn *store.Txn, lock *corepb.Lock, now int64) (*corepb.Lock, error) {
	result := proto.Clone(lock).(*corepb.Lock)

	switch lock.State {
	case corepb.LockState_UNLOCKED:
		// Lock is unlocked, return as is
		return result, nil
	case corepb.LockState_SHARED_LOCKED:
		// Filter out holders whose leases have expired
		newLockHolders := make([]*corepb.LockHolder, 0, len(result.LockHolders))
		for _, h := range result.LockHolders {
			lease, err := c.leases.Get(txn, &corepb.LeaseId{
				AccountId:   lock.Id.AccountId,
				NamespaceId: lock.Id.NamespaceId,
				LeaseId:     h.LeaseId,
			})
			if err != nil {
				return nil, err
			}
			if lease.ExpiresAt > now {
				newLockHolders = append(newLockHolders, h)
			}
		}
		result.LockHolders = newLockHolders
		if len(result.LockHolders) == 0 {
			result.State = corepb.LockState_UNLOCKED
			result.LockHolders = nil
			result.LockedAt = 0
		}
	case corepb.LockState_EXCLUSIVE_LOCKED:
		lease, err := c.leases.Get(txn, &corepb.LeaseId{
			AccountId:   lock.Id.AccountId,
			NamespaceId: lock.Id.NamespaceId,
			LeaseId:     result.LockHolders[0].LeaseId,
		})
		if err != nil {
			return nil, err
		}
		if lease.ExpiresAt <= now {
			result.State = corepb.LockState_UNLOCKED
			result.LockHolders = nil
			result.LockedAt = 0
		}
	default:
		return nil, fmt.Errorf("invalid lock state: %v", lock.State)
	}

	return result, nil
}

// incrementAncestors increments the ancestor counter for each path prefix of the given lock name.
// Called when a lock transitions from UNLOCKED to LOCKED for the first time (new lock record).
func (c *Core) incrementAncestors(txn *store.Txn, lockId *corepb.LockId, exclusive bool) error {
	for _, ancestorName := range c.lockAncestorNames(lockId.LockName) {
		ancestorId := &corepb.LockId{
			AccountId:   lockId.AccountId,
			NamespaceId: lockId.NamespaceId,
			LockName:    ancestorName,
		}
		ancestor, err := c.ancestors.Get(txn, ancestorId)
		if err != nil {
			return err
		}
		ancestor.Id = ancestorId
		if exclusive {
			ancestor.ExclusiveCount++
		} else {
			ancestor.SharedCount++
		}
		err = c.ancestors.Set(txn, ancestor)
		if err != nil {
			return err
		}
	}
	return nil
}

// decrementAncestors decrements the ancestor counter for each path prefix of the given lock name.
// Called when a lock record is deleted (last holder released or expired).
func (c *Core) decrementAncestors(txn *store.Txn, lockId *corepb.LockId, wasExclusive bool) error {
	for _, ancestorName := range c.lockAncestorNames(lockId.LockName) {
		ancestorId := &corepb.LockId{
			AccountId:   lockId.AccountId,
			NamespaceId: lockId.NamespaceId,
			LockName:    ancestorName,
		}
		ancestor, err := c.ancestors.Get(txn, ancestorId)
		if err != nil {
			return err
		}
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
		if err != nil {
			return err
		}
	}
	return nil
}

// swapAncestorMode swaps the mode contribution of a lock in ancestor entries.
// Called when a lock that was expired (in-memory UNLOCKED) is re-acquired with a different mode.
func (c *Core) swapAncestorMode(txn *store.Txn, lockId *corepb.LockId, wasExclusive bool, isExclusive bool) error {
	if wasExclusive == isExclusive {
		return nil
	}
	for _, ancestorName := range c.lockAncestorNames(lockId.LockName) {
		ancestorId := &corepb.LockId{
			AccountId:   lockId.AccountId,
			NamespaceId: lockId.NamespaceId,
			LockName:    ancestorName,
		}
		ancestor, err := c.ancestors.Get(txn, ancestorId)
		if err != nil {
			return err
		}
		if wasExclusive {
			ancestor.ExclusiveCount--
			ancestor.SharedCount++
		} else {
			ancestor.SharedCount--
			ancestor.ExclusiveCount++
		}
		ancestor.Id = ancestorId
		err = c.ancestors.Set(txn, ancestor)
		if err != nil {
			return err
		}
	}
	return nil
}

// checkAncestorConflicts verifies that no ancestor locks block the requested lock.
// Returns (true, nil) if no conflicts, (false, nil) if blocked by ancestor.
func (c *Core) checkAncestorConflicts(txn *store.Txn, lockId *corepb.LockId, requestExclusive bool) (bool, error) {
	ancestors := c.lockAncestorNames(lockId.LockName)
	for _, ancestorName := range ancestors {
		ancestorId := &corepb.LockId{
			AccountId:   lockId.AccountId,
			NamespaceId: lockId.NamespaceId,
			LockName:    ancestorName,
		}

		// Check if there's an actual lock on this ancestor path
		ancestorLock, err := c.locks.Get(txn, ancestorId)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
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

// lockAncestorNames returns the ancestor path prefixes for a hierarchical lock name.
// For "a/b/c" it returns ["a", "a/b"]. For a flat name it returns nil.
func (c *Core) lockAncestorNames(lockName string) []string {
	parts := strings.Split(lockName, "/")
	if len(parts) <= 1 {
		return nil
	}
	ancestors := make([]string, 0, len(parts)-1)
	for i := 1; i < len(parts); i++ {
		ancestors = append(ancestors, strings.Join(parts[:i], "/"))
	}
	return ancestors
}

func panicIfNotNil(err error) {
	if err != nil {
		panic(err)
	}
}
