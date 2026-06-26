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

	"github.com/evrblk/grackle/pkg/coreapis"
	"github.com/evrblk/grackle/pkg/corepb"
	"github.com/evrblk/grackle/pkg/ids"
	"github.com/evrblk/grackle/pkg/pagination"
	"github.com/evrblk/grackle/pkg/tables"
)

type Core struct {
	badgerStore *store.BadgerStore

	locks     *locksTable
	ancestors *lockAncestorsTable
	counters  *tables.CountersTable[*corepb.LocksCounter, corepb.LocksCounter]
	gcRecords *tables.GCRecordsTable[*corepb.LocksGarbageCollectionRecord, corepb.LocksGarbageCollectionRecord]
	leases    *tables.LeasesTable
}

var _ coreapis.GrackleLocksCoreApi = &Core{}

// NewCore constructs a Core bound to a single shard of the locks keyspace.
// The given lower/upper bounds delimit the shard's local key range (used for
// Snapshot/Restore), while shardGlobalIndexPrefix scopes cross-shard global
// indexes such as lease secondary indexes and GC records.
func NewCore(badgerStore *store.BadgerStore, shardGlobalIndexPrefix []byte, shardLowerBound []byte, shardUpperBound []byte) *Core {
	return &Core{
		badgerStore: badgerStore,

		locks:     newLocksTable(shardLowerBound, shardUpperBound),
		ancestors: newLockAncestorsTable(shardLowerBound, shardUpperBound),
		counters: tables.NewCountersTable[*corepb.LocksCounter, corepb.LocksCounter](
			tables.Grackle["Grackle.LocksCore.Counters.Table"].Bytes(),
			shardLowerBound,
			shardUpperBound,
		),
		gcRecords: tables.NewGCRecordsTable[*corepb.LocksGarbageCollectionRecord, corepb.LocksGarbageCollectionRecord](
			tables.Grackle["Grackle.LocksCore.GarbageCollectionRecords.Table"].Bytes(),
			shardGlobalIndexPrefix,
		),
		leases: tables.NewLeasesTable(
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

// Snapshot returns a consistent snapshot of every key range owned by this
// shard's locks Core, suitable for Raft snapshot transfer.
func (c *Core) Snapshot() monstera.ApplicationCoreSnapshot {
	return monsterax.Snapshot(c.badgerStore, c.ranges())
}

// Restore replaces the contents of this shard's key ranges with the data read
// from reader. Any existing keys in those ranges are removed first.
func (c *Core) Restore(reader io.ReadCloser) error {
	return monsterax.Restore(c.badgerStore, c.ranges(), reader)
}

// Close releases any Core-owned resources. The underlying Badger store is
// shared across cores and is not closed here.
func (c *Core) Close() {

}

// GetLock returns the current state of the named lock. If no record exists,
// a synthetic UNLOCKED lock is returned (this is not an error). Expired
// holders are filtered out of the returned lock before it is returned to the
// caller. This runs on a read-only transaction: expired rows are not deleted
// here — that is left to the GC. If every holder has expired the returned lock
// has state UNLOCKED.
func (c *Core) GetLock(req *coreapis.GetLockRequest) (*coreapis.GetLockResponse, error) {
	txn := c.badgerStore.View()
	defer txn.Discard()

	lock, err := c.locks.Get(txn, req.Payload.LockId)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			// No lock exists, return an unlocked lock
			return &coreapis.GetLockResponse{
				Payload: &corepb.GetLockResponse{
					Lock: &corepb.Lock{
						Id:       req.Payload.LockId,
						State:    corepb.LockState_LOCK_STATE_UNLOCKED,
						LockedAt: 0,
					},
				},
			}, nil
		}

		return nil, err
	}

	// Filter out expired holders
	updatedLock, err := c.checkLockExpiration(txn, lock, req.Payload.Now)
	if err != nil {
		return nil, err
	}

	return &coreapis.GetLockResponse{
		Payload: &corepb.GetLockResponse{
			Lock: updatedLock,
		},
	}, nil
}

// ListLocks returns a page of locks in the given namespace. Locks whose
// holders have all expired (as observed against req.Now) are filtered out of
// the result. Unlike GetLock, this is a read-only view and does not delete
// expired rows — that is left to the GC.
func (c *Core) ListLocks(req *coreapis.ListLocksRequest) (*coreapis.ListLocksResponse, error) {
	txn := c.badgerStore.View()
	defer txn.Discard()

	result, err := c.locks.List(txn, req.Payload.NamespaceId, req.Payload.PaginationToken, pagination.GetLimitWithDefaults(int(req.Payload.Limit)))
	if err != nil {
		return nil, err
	}

	// Check expiration
	lockedLocks := make([]*corepb.Lock, 0, len(result.locks))
	for _, lock := range result.locks {
		refreshedLock, err := c.checkLockExpiration(txn, lock, req.Payload.Now)
		if err != nil {
			return nil, err
		}
		if refreshedLock.State != corepb.LockState_LOCK_STATE_UNLOCKED {
			lockedLocks = append(lockedLocks, refreshedLock)
		}
	}

	return &coreapis.ListLocksResponse{
		Payload: &corepb.ListLocksResponse{
			Locks:                   lockedLocks,
			NextPaginationToken:     result.nextPaginationToken,
			PreviousPaginationToken: result.previousPaginationToken,
		},
	}, nil
}

// DeleteLock unconditionally removes the lock record, regardless of current
// holders, and updates ancestor counters and the per-namespace lock counter.
// Deleting a lock that does not exist is a no-op and returns success.
func (c *Core) DeleteLock(req *coreapis.DeleteLockRequest) (*coreapis.DeleteLockResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	lock, err := c.locks.Get(txn, req.Payload.LockId)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			// No lock exists, do nothing
			return &coreapis.DeleteLockResponse{
				Payload: &corepb.DeleteLockResponse{},
			}, nil
		}

		return nil, err
	}

	// Get counters for that namespace
	counters, err := c.counters.Get(txn, req.Payload.LockId.AccountId, req.Payload.LockId.NamespaceId)
	if err != nil {
		return nil, err
	}

	err = c.locks.Delete(txn, lock.Id)
	if err != nil {
		return nil, err
	}

	// Update ancestor entries
	err = c.decrementAncestors(txn, lock.Id, lock.State == corepb.LockState_LOCK_STATE_EXCLUSIVE_LOCKED)
	if err != nil {
		return nil, err
	}

	// Update counters
	counters.NumberOfLocks -= 1
	err = c.counters.Set(txn, req.Payload.LockId.AccountId, req.Payload.LockId.NamespaceId, counters)
	if err != nil {
		return nil, err
	}

	err = txn.Commit()
	if err != nil {
		return nil, err
	}

	return &coreapis.DeleteLockResponse{
		Payload: &corepb.DeleteLockResponse{},
	}, nil
}

// AcquireLock attempts to acquire the named lock for the given lease in
// either shared or exclusive mode. If the lease already holds the lock, its
// LockedAt is refreshed and the call succeeds. If the lock is held in an
// incompatible mode (e.g. shared lock requested while held exclusively, or
// any conflicting hierarchical ancestor/descendant lock), Payload.Success is
// false and no state changes. Returns a NotFound application error if the
// lease is missing or expired, or ResourceExhausted if creating a new lock
// would exceed MaxNumberOfLocksPerNamespace.
func (c *Core) AcquireLock(req *coreapis.AcquireLockRequest) (*coreapis.AcquireLockResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	// Validate and get the lease
	lease, err := c.leases.Get(txn, &corepb.LeaseId{
		AccountId:   req.Payload.LockId.AccountId,
		NamespaceId: req.Payload.LockId.NamespaceId,
		LeaseId:     req.Payload.LeaseId,
	})
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return &coreapis.AcquireLockResponse{
				ApplicationError: monsterax.NewErrorWithContext(
					monsterax.NotFound,
					"lease not found",
					map[string]string{
						"lease_id": fmt.Sprintf("%d", req.Payload.LeaseId),
					},
				),
			}, nil
		}

		return nil, err
	}

	// Check if lease has expired
	if lease.ExpiresAt <= req.Payload.Now {
		// On return, the transaction will be discarded, and the expired lease will be deleted later by the garbage collector.
		return &coreapis.AcquireLockResponse{
			ApplicationError: monsterax.NewErrorWithContext(
				monsterax.NotFound,
				"lease not found",
				map[string]string{
					"lease_id": fmt.Sprintf("%d", req.Payload.LeaseId),
				},
			),
		}, nil
	}

	// Get counters for that namespace
	counters, err := c.counters.Get(txn, req.Payload.LockId.AccountId, req.Payload.LockId.NamespaceId)
	if err != nil {
		return nil, err
	}

	lock, err := c.locks.Get(txn, req.Payload.LockId)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			// No lock exists, create a new one
			lock = &corepb.Lock{
				Id:       req.Payload.LockId,
				State:    corepb.LockState_LOCK_STATE_UNLOCKED,
				LockedAt: 0,
			}
			// Increment counter only when a new lock is really created
			counters.NumberOfLocks += 1

			// Check the total number of locks
			if counters.NumberOfLocks > req.Payload.MaxNumberOfLocksPerNamespace {
				return &coreapis.AcquireLockResponse{
					ApplicationError: monsterax.NewErrorWithContext(
						monsterax.ResourceExhausted,
						"max number of locks per namespace reached",
						map[string]string{
							"limit": fmt.Sprintf("%d", req.Payload.MaxNumberOfLocksPerNamespace),
						},
					),
				}, nil
			}
		} else {
			return nil, err
		}
	}

	// Capture state before expiry check for ancestor tracking
	prevState := lock.State

	// Remove expired holders
	updatedLock, err := c.checkLockExpiration(txn, lock, req.Payload.Now)
	if err != nil {
		return nil, err
	}

	// Check hierarchical conflicts before attempting acquisition
	conflictReason, blockingLocks, err := c.checkHierarchicalConflicts(txn, req.Payload.LockId, req.Payload.Exclusive)
	if err != nil {
		return nil, err
	}
	if conflictReason != corepb.ContentionReason_CONTENTION_REASON_UNSPECIFIED {
		// Hierarchical conflict detected - return failure
		return &coreapis.AcquireLockResponse{
			Payload: &corepb.AcquireLockResponse{
				Lock:          updatedLock,
				Success:       false,
				Reason:        conflictReason,
				BlockingLocks: blockingLocks,
			},
		}, nil
	}

	lockHolder := &corepb.LockHolder{
		LeaseId:  req.Payload.LeaseId,
		LockedAt: req.Payload.Now,
		Metadata: req.Payload.Metadata,
	}

	switch updatedLock.State {
	case corepb.LockState_LOCK_STATE_UNLOCKED:
		if req.Payload.Exclusive {
			// Lock for writes
			updatedLock.State = corepb.LockState_LOCK_STATE_EXCLUSIVE_LOCKED
		} else {
			// Lock for reads only
			updatedLock.State = corepb.LockState_LOCK_STATE_SHARED_LOCKED
		}
		updatedLock.LockHolders = []*corepb.LockHolder{lockHolder}
		updatedLock.LockedAt = req.Payload.Now
	case corepb.LockState_LOCK_STATE_SHARED_LOCKED:
		if req.Payload.Exclusive {
			return &coreapis.AcquireLockResponse{
				Payload: &corepb.AcquireLockResponse{
					Lock:    updatedLock,
					Success: false, // Already locked for reads, cannot be locked for writes.
					Reason:  corepb.ContentionReason_CONTENTION_REASON_PEER,
					// BlockingLocks is left empty for PEER: the conflicting lock is
					// already returned in the Lock field above.
				},
			}, nil
		}

		// Already locked for reads.
		// Check if the same lease_id already holds the lock here.
		existingHolder, ok := lo.Find(updatedLock.LockHolders, func(h *corepb.LockHolder) bool {
			return h.LeaseId == req.Payload.LeaseId
		})
		if ok {
			// Update locked_at time (refresh lock acquisition time)
			existingHolder.LockedAt = req.Payload.Now
		} else {
			// Add the new lock holder
			updatedLock.LockHolders = append(updatedLock.LockHolders, lockHolder)
		}
	case corepb.LockState_LOCK_STATE_EXCLUSIVE_LOCKED:
		if req.Payload.Exclusive {
			// Already locked for writes. Check if the same lease_id already holds the lock here.
			if updatedLock.LockHolders[0].LeaseId == req.Payload.LeaseId {
				// This lease already holds the lock, repeated locks are considered successful
				// Update locked_at time (refresh lock acquisition time)
				updatedLock.LockHolders[0].LockedAt = req.Payload.Now
			} else {
				return &coreapis.AcquireLockResponse{
					Payload: &corepb.AcquireLockResponse{
						Lock:    updatedLock,
						Success: false, // The lock is held by another lease
						Reason:  corepb.ContentionReason_CONTENTION_REASON_PEER,
						// BlockingLocks is left empty for PEER: the conflicting lock
						// is already returned in the Lock field above.
					},
				}, nil
			}

		} else {
			return &coreapis.AcquireLockResponse{
				Payload: &corepb.AcquireLockResponse{
					Lock:    updatedLock,
					Success: false, // Already locked for writes, cannot be locked for reads.
					Reason:  corepb.ContentionReason_CONTENTION_REASON_PEER,
					// BlockingLocks is left empty for PEER: the conflicting lock is
					// already returned in the Lock field above.
				},
			}, nil

		}
	default:
		return nil, fmt.Errorf("invalid lock state")
	}

	// Record the (successful) acquire.
	updatedLock.LastActivityAt = req.Payload.Now

	// Update lock
	err = c.locks.Update(txn, updatedLock)
	if err != nil {
		return nil, err
	}

	// Update ancestor entries based on lock state transition.
	// prevState is the state from DB (UNLOCKED for a brand new lock).
	// updatedLock.State is the final acquired state.
	if prevState == corepb.LockState_LOCK_STATE_UNLOCKED && updatedLock.State != corepb.LockState_LOCK_STATE_UNLOCKED {
		// New lock record: increment ancestor counters
		err = c.incrementAncestors(txn, req.Payload.LockId, updatedLock.State == corepb.LockState_LOCK_STATE_EXCLUSIVE_LOCKED)
		if err != nil {
			return nil, err
		}
	} else if prevState != corepb.LockState_LOCK_STATE_UNLOCKED && updatedLock.State != corepb.LockState_LOCK_STATE_UNLOCKED && prevState != updatedLock.State {
		// Lock was expired and re-acquired with a different mode: swap ancestor mode
		err = c.swapAncestorMode(txn, req.Payload.LockId,
			prevState == corepb.LockState_LOCK_STATE_EXCLUSIVE_LOCKED,
			updatedLock.State == corepb.LockState_LOCK_STATE_EXCLUSIVE_LOCKED)
		if err != nil {
			return nil, err
		}
	}

	// Update counters
	err = c.counters.Set(txn, req.Payload.LockId.AccountId, req.Payload.LockId.NamespaceId, counters)
	if err != nil {
		return nil, err
	}

	err = txn.Commit()
	if err != nil {
		return nil, err
	}

	return &coreapis.AcquireLockResponse{
		Payload: &corepb.AcquireLockResponse{
			Lock:    updatedLock,
			Success: true, // Locked successfully by the given lease
		},
	}, nil
}

// ReleaseLock drops the given lease's hold on the named lock. For shared
// locks only that lease's holder entry is removed; the lock row is deleted
// once the last holder leaves. For exclusive locks the row is deleted if
// (and only if) the lease in question is the current holder; a release by a
// non-holder lease is a no-op. Releasing a non-existent lock returns a
// synthetic UNLOCKED lock without error. Expired holders are evicted before
// the release is applied.
func (c *Core) ReleaseLock(req *coreapis.ReleaseLockRequest) (*coreapis.ReleaseLockResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	// Get counters for that namespace
	counters, err := c.counters.Get(txn, req.Payload.LockId.AccountId, req.Payload.LockId.NamespaceId)
	if err != nil {
		return nil, err
	}

	lock, err := c.locks.Get(txn, req.Payload.LockId)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			// No lock exists, return an unlocked lock
			return &coreapis.ReleaseLockResponse{
				Payload: &corepb.ReleaseLockResponse{
					Lock: &corepb.Lock{
						Id:       req.Payload.LockId,
						State:    corepb.LockState_LOCK_STATE_UNLOCKED,
						LockedAt: 0,
					},
				},
			}, nil
		}

		return nil, err
	}

	// Lock exists, lets check if it expired
	updatedLock, err := c.checkLockExpiration(txn, lock, req.Payload.Now)
	if err != nil {
		return nil, err
	}

	switch updatedLock.State {
	case corepb.LockState_LOCK_STATE_UNLOCKED:
		// Lock has expired, delete it
		err = c.locks.Delete(txn, updatedLock.Id)
		if err != nil {
			return nil, err
		}

		// Update ancestor entries
		err = c.decrementAncestors(txn, lock.Id, lock.State == corepb.LockState_LOCK_STATE_EXCLUSIVE_LOCKED)
		if err != nil {
			return nil, err
		}

		counters.NumberOfLocks -= 1
	case corepb.LockState_LOCK_STATE_SHARED_LOCKED:
		// Remove the holder
		updatedLock.LockHolders = lo.Filter(updatedLock.LockHolders, func(h *corepb.LockHolder, _ int) bool {
			return h.LeaseId != req.Payload.LeaseId
		})

		// If no read lock holders left
		if len(updatedLock.LockHolders) == 0 {
			// Unlock
			updatedLock.LockedAt = 0
			updatedLock.State = corepb.LockState_LOCK_STATE_UNLOCKED
			updatedLock.LockHolders = nil

			// Delete lock
			err = c.locks.Delete(txn, updatedLock.Id)
			if err != nil {
				return nil, err
			}

			// Update ancestor entries
			err = c.decrementAncestors(txn, lock.Id, false)
			if err != nil {
				return nil, err
			}

			counters.NumberOfLocks -= 1
		} else {
			// Record the release; the lock still has other holders.
			updatedLock.LastActivityAt = req.Payload.Now

			// Update lock
			err = c.locks.Update(txn, updatedLock)
			if err != nil {
				return nil, err
			}
		}
	case corepb.LockState_LOCK_STATE_EXCLUSIVE_LOCKED:
		if updatedLock.LockHolders[0].LeaseId == req.Payload.LeaseId {
			// Unlock
			updatedLock.State = corepb.LockState_LOCK_STATE_UNLOCKED
			updatedLock.LockedAt = 0
			updatedLock.LockHolders = nil

			// Delete it
			err = c.locks.Delete(txn, updatedLock.Id)
			if err != nil {
				return nil, err
			}

			// Update ancestor entries
			err = c.decrementAncestors(txn, lock.Id, true)
			if err != nil {
				return nil, err
			}

			counters.NumberOfLocks -= 1
		}
	default:
		return nil, fmt.Errorf("invalid lock state")
	}

	// Update counters
	err = c.counters.Set(txn, req.Payload.LockId.AccountId, req.Payload.LockId.NamespaceId, counters)
	if err != nil {
		return nil, err
	}

	err = txn.Commit()
	if err != nil {
		return nil, err
	}

	return &coreapis.ReleaseLockResponse{
		Payload: &corepb.ReleaseLockResponse{
			Lock: updatedLock,
		},
	}, nil
}

// RunLocksGarbageCollection processes one page of pending GC work: deletes
// locks tied to namespaces marked for removal, and reaps expired leases
// (along with any locks they still hold). The amount of work per call is
// bounded by req.MaxVisitedLocks; records that fully drain within budget are
// removed, otherwise they are left for the next GC tick.
func (c *Core) RunLocksGarbageCollection(req *coreapis.RunLocksGarbageCollectionRequest) (*coreapis.RunLocksGarbageCollectionResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	visitedLocks := int64(0)

	// List one page of GC records
	gcRecords, err := c.gcRecords.List(txn, int(req.Payload.GcRecordsPageSize))
	if err != nil {
		return nil, err
	}

	for _, gcRecord := range gcRecords {
		// Delete counters for that namespace. Will not fail if counters do not exist.
		err := c.counters.Delete(txn, gcRecord.NamespaceId.AccountId, gcRecord.NamespaceId.NamespaceId)
		if err != nil {
			return nil, err
		}

		// List one page of locks for that namespace
		result, err := c.locks.List(txn, gcRecord.NamespaceId, nil, int(req.Payload.GcRecordLocksPageSize))
		if err != nil {
			return nil, err
		}

		// Delete those locks
		for _, lock := range result.locks {
			visitedLocks++

			// Remove from the main table
			err := c.locks.Delete(txn, lock.Id)
			if err != nil {
				return nil, err
			}

			// Update ancestor entries
			err = c.decrementAncestors(txn, lock.Id, lock.State == corepb.LockState_LOCK_STATE_EXCLUSIVE_LOCKED)
			if err != nil {
				return nil, err
			}

			if visitedLocks >= req.Payload.MaxVisitedLocks {
				goto commit
			}
		}

		// Delete the deleted namespace if that was the last page of locks
		if result.nextPaginationToken == nil {
			err := c.gcRecords.Delete(txn, gcRecord)
			if err != nil {
				return nil, err
			}
		}
	}

	if visitedLocks < req.Payload.MaxVisitedLocks {
		// Clean up expired leases and their associated locks
		err = c.leases.ListByExpiration(txn, 0, req.Payload.Now, func(lease *corepb.Lease) (bool, error) {
			// List all locks held by this expired lease
			locksResult, err := c.locks.ListByLeaseId(txn, lease.Id, nil, 1000)
			if err != nil {
				return false, err
			}

			// Release all locks held by this expired lease
			for _, lock := range locksResult.locks {
				visitedLocks++

				// Get the lock to update it
				updatedLock, err := c.checkLockExpiration(txn, lock, req.Payload.Now)
				if err != nil {
					return false, err
				}

				if updatedLock.State == corepb.LockState_LOCK_STATE_UNLOCKED {
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
					err = c.decrementAncestors(txn, lock.Id, lock.State == corepb.LockState_LOCK_STATE_EXCLUSIVE_LOCKED)
					if err != nil {
						return false, err
					}

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

				if visitedLocks >= req.Payload.MaxVisitedLocks {
					return false, nil // Stop processing
				}
			}

			// Delete the expired lease. The lease row (plus its index entries) is real
			// transactional work, so credit one visit against the budget.
			err = c.leases.Delete(txn, lease)
			if err != nil {
				return false, err
			}
			visitedLocks++

			// Decrement lease counter
			counters, err := c.counters.Get(txn, lease.Id.AccountId, lease.Id.NamespaceId)
			if err != nil {
				return false, err
			}

			counters.NumberOfLeases -= 1
			err = c.counters.Set(txn, lease.Id.AccountId, lease.Id.NamespaceId, counters)
			if err != nil {
				return false, err
			}

			// Continue if we haven't reached the limit
			return visitedLocks < req.Payload.MaxVisitedLocks, nil
		})
		if err != nil && !errors.Is(err, store.ErrNotFound) {
			return nil, err
		}
	}

commit:

	err = txn.Commit()
	if err != nil {
		return nil, err
	}

	return &coreapis.RunLocksGarbageCollectionResponse{
		Payload: &corepb.RunLocksGarbageCollectionResponse{},
	}, nil
}

// LocksDeleteNamespace records a GC marker that will, on subsequent
// RunLocksGarbageCollection ticks, delete every lock and lease belonging to
// the given namespace. The deletion itself is asynchronous; this call only
// enqueues the request.
func (c *Core) LocksDeleteNamespace(req *coreapis.LocksDeleteNamespaceRequest) (*coreapis.LocksDeleteNamespaceResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	// Mark the namespace as deleted
	err := c.gcRecords.Create(txn, &corepb.LocksGarbageCollectionRecord{
		Id:          req.Payload.RecordId,
		NamespaceId: req.Payload.NamespaceId,
	})
	if err != nil {
		return nil, err
	}

	err = txn.Commit()
	if err != nil {
		return nil, err
	}

	return &coreapis.LocksDeleteNamespaceResponse{
		Payload: &corepb.LocksDeleteNamespaceResponse{},
	}, nil
}

// CreateLockLease creates a new lease for the given process with TTL of
// req.TtlSeconds and bumps the per-namespace lease counter. Returns
// ResourceExhausted if creating it would exceed MaxNumberOfLockLeases.
func (c *Core) CreateLockLease(req *coreapis.CreateLockLeaseRequest) (*coreapis.CreateLockLeaseResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	// Get counters for that namespace
	counters, err := c.counters.Get(txn, req.Payload.LeaseId.AccountId, req.Payload.LeaseId.NamespaceId)
	if err != nil {
		return nil, err
	}

	// Increment lease counter
	counters.NumberOfLeases += 1

	// Check the total number of leases
	if counters.NumberOfLeases > req.Payload.MaxNumberOfLockLeases {
		return &coreapis.CreateLockLeaseResponse{
			ApplicationError: monsterax.NewErrorWithContext(
				monsterax.ResourceExhausted,
				"max number of lock leases per namespace reached",
				map[string]string{
					"limit": fmt.Sprintf("%d", req.Payload.MaxNumberOfLockLeases),
				},
			),
		}, nil
	}

	// Calculate expiration time
	expiresAt := req.Payload.Now + int64(req.Payload.TtlSeconds)*1e9

	// Create the lease
	lease := &corepb.Lease{
		Id:        req.Payload.LeaseId,
		ProcessId: req.Payload.ProcessId,
		CreatedAt: req.Payload.Now,
		ExpiresAt: expiresAt,
		Metadata:  req.Payload.Metadata,
	}

	err = c.leases.Create(txn, lease)
	if err != nil {
		return nil, err
	}

	// Update counters
	err = c.counters.Set(txn, req.Payload.LeaseId.AccountId, req.Payload.LeaseId.NamespaceId, counters)
	if err != nil {
		return nil, err
	}

	err = txn.Commit()
	if err != nil {
		return nil, err
	}

	return &coreapis.CreateLockLeaseResponse{
		Payload: &corepb.CreateLockLeaseResponse{
			Lease: lease,
		},
	}, nil
}

// GetLockLease returns the lease with the given id. Returns NotFound if the
// lease does not exist or has already expired as of req.Now.
func (c *Core) GetLockLease(req *coreapis.GetLockLeaseRequest) (*coreapis.GetLockLeaseResponse, error) {
	txn := c.badgerStore.View()
	defer txn.Discard()

	lease, err := c.leases.Get(txn, req.Payload.LeaseId)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return &coreapis.GetLockLeaseResponse{
				ApplicationError: monsterax.NewErrorWithContext(
					monsterax.NotFound,
					"lease not found",
					map[string]string{
						"lease_id": ids.EncodeLeaseId(req.Payload.LeaseId),
					},
				),
			}, nil
		}

		return nil, err
	}

	if lease.ExpiresAt <= req.Payload.Now {
		return &coreapis.GetLockLeaseResponse{
			ApplicationError: monsterax.NewErrorWithContext(
				monsterax.NotFound,
				"lease not found",
				map[string]string{
					"lease_id": ids.EncodeLeaseId(req.Payload.LeaseId),
				},
			),
		}, nil
	}

	return &coreapis.GetLockLeaseResponse{
		Payload: &corepb.GetLockLeaseResponse{
			Lease: lease,
		},
	}, nil
}

// ListLockLeases returns a page of leases in the given namespace, filtering
// out leases that have already expired as of req.Now. Expired rows are not
// deleted here — that is left to the GC.
func (c *Core) ListLockLeases(req *coreapis.ListLockLeasesRequest) (*coreapis.ListLockLeasesResponse, error) {
	txn := c.badgerStore.View()
	defer txn.Discard()

	result, err := c.leases.List(txn, req.Payload.NamespaceId, req.Payload.PaginationToken, pagination.GetLimitWithDefaults(int(req.Payload.Limit)))
	if err != nil {
		return nil, err
	}

	// Filter out expired leases
	activeLease := lo.Filter(result.Leases, func(lease *corepb.Lease, _ int) bool {
		return lease.ExpiresAt > req.Payload.Now
	})

	return &coreapis.ListLockLeasesResponse{
		Payload: &corepb.ListLockLeasesResponse{
			Leases:                  activeLease,
			NextPaginationToken:     result.NextPaginationToken,
			PreviousPaginationToken: result.PreviousPaginationToken,
		},
	}, nil
}

// RefreshLockLease extends the lease's expiration to req.Now +
// req.TtlSeconds. If the lease has already expired by the time the call is
// processed, it is revoked (releasing all locks it still held) and the call
// returns NotFound; otherwise the new expiration is persisted.
func (c *Core) RefreshLockLease(req *coreapis.RefreshLockLeaseRequest) (*coreapis.RefreshLockLeaseResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	lease, err := c.leases.Get(txn, req.Payload.LeaseId)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return &coreapis.RefreshLockLeaseResponse{
				ApplicationError: monsterax.NewErrorWithContext(
					monsterax.NotFound,
					"lease not found",
					map[string]string{
						"lease_id": ids.EncodeLeaseId(req.Payload.LeaseId),
					},
				),
			}, nil
		}

		return nil, err
	}

	// If the lease has already expired, revoke it instead of refreshing
	if lease.ExpiresAt <= req.Payload.Now {
		err = c.revokeLease(txn, lease)
		if err != nil {
			return nil, err
		}

		err = txn.Commit()
		if err != nil {
			return nil, err
		}

		return &coreapis.RefreshLockLeaseResponse{
			ApplicationError: monsterax.NewErrorWithContext(
				monsterax.NotFound,
				"lease not found",
				map[string]string{
					"lease_id": ids.EncodeLeaseId(req.Payload.LeaseId),
				},
			),
		}, nil
	}

	// Update the expiration time
	lease.ExpiresAt = req.Payload.Now + int64(req.Payload.TtlSeconds)*1e9

	// Save the updated lease
	err = c.leases.Update(txn, lease)
	if err != nil {
		return nil, err
	}

	err = txn.Commit()
	if err != nil {
		return nil, err
	}

	return &coreapis.RefreshLockLeaseResponse{
		Payload: &corepb.RefreshLockLeaseResponse{
			Lease: lease,
		},
	}, nil
}

// RevokeLockLease deletes the lease and synchronously releases every lock
// it currently holds, updating per-namespace counters and ancestor entries.
// Revoking a non-existent lease is a no-op and returns success.
func (c *Core) RevokeLockLease(req *coreapis.RevokeLockLeaseRequest) (*coreapis.RevokeLockLeaseResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	// Check if the lease exists
	lease, err := c.leases.Get(txn, req.Payload.LeaseId)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			// Lease doesn't exist, nothing to do
			return &coreapis.RevokeLockLeaseResponse{
				Payload: &corepb.RevokeLockLeaseResponse{},
			}, nil
		}

		return nil, err
	}

	err = c.revokeLease(txn, lease)
	if err != nil {
		return nil, err
	}

	err = txn.Commit()
	if err != nil {
		return nil, err
	}

	return &coreapis.RevokeLockLeaseResponse{
		Payload: &corepb.RevokeLockLeaseResponse{},
	}, nil
}

// ListLockLeasesByProcessId returns a page of leases in the given namespace
// that belong to req.ProcessId, filtering out leases that have already
// expired as of req.Now.
func (c *Core) ListLockLeasesByProcessId(req *coreapis.ListLockLeasesByProcessIdRequest) (*coreapis.ListLockLeasesByProcessIdResponse, error) {
	txn := c.badgerStore.View()
	defer txn.Discard()

	result, err := c.leases.ListByProcessId(txn, req.Payload.NamespaceId, req.Payload.ProcessId, req.Payload.PaginationToken, pagination.GetLimitWithDefaults(int(req.Payload.Limit)))
	if err != nil {
		return nil, err
	}

	// Filter out expired leases
	activeLeases := lo.Filter(result.Leases, func(lease *corepb.Lease, _ int) bool {
		return lease.ExpiresAt > req.Payload.Now
	})

	return &coreapis.ListLockLeasesByProcessIdResponse{
		Payload: &corepb.ListLockLeasesByProcessIdResponse{
			Leases:                  activeLeases,
			NextPaginationToken:     result.NextPaginationToken,
			PreviousPaginationToken: result.PreviousPaginationToken,
		},
	}, nil
}

// ListLocksByLeaseId returns a page of locks currently held by the given
// lease. The result is not filtered by lease expiration — callers that care
// about staleness should consult GetLockLease.
func (c *Core) ListLocksByLeaseId(req *coreapis.ListLocksByLeaseIdRequest) (*coreapis.ListLocksByLeaseIdResponse, error) {
	txn := c.badgerStore.View()
	defer txn.Discard()

	result, err := c.locks.ListByLeaseId(txn, req.Payload.LeaseId, req.Payload.PaginationToken, pagination.GetLimitWithDefaults(int(req.Payload.Limit)))
	if err != nil {
		return nil, err
	}

	// Check expiration
	lockedLocks := make([]*corepb.Lock, 0, len(result.locks))
	for _, lock := range result.locks {
		refreshedLock, err := c.checkLockExpiration(txn, lock, req.Payload.Now)
		if err != nil {
			return nil, err
		}
		if refreshedLock.State != corepb.LockState_LOCK_STATE_UNLOCKED {
			lockedLocks = append(lockedLocks, refreshedLock)
		}
	}

	return &coreapis.ListLocksByLeaseIdResponse{
		Payload: &corepb.ListLocksByLeaseIdResponse{
			Locks:                   lockedLocks,
			NextPaginationToken:     result.nextPaginationToken,
			PreviousPaginationToken: result.previousPaginationToken,
		},
	}, nil
}

// revokeLease releases all locks held by the lease, deletes the lease itself,
// and decrements the namespace counters accordingly. The caller owns the txn lifecycle.
func (c *Core) revokeLease(txn *store.Txn, lease *corepb.Lease) error {
	// Get counters for that namespace
	counters, err := c.counters.Get(txn, lease.Id.AccountId, lease.Id.NamespaceId)
	if err != nil {
		return err
	}

	// Release all locks held by this lease, paginating through all pages
	var paginationToken *corepb.PaginationToken
	for {
		// List locks held by this lease
		locksResult, err := c.locks.ListByLeaseId(txn, lease.Id, paginationToken, 1000)
		if err != nil {
			return err
		}

		// If no locks found, we're done
		if len(locksResult.locks) == 0 && locksResult.nextPaginationToken == nil {
			break
		}

		// Release locks on this page
		for _, lock := range locksResult.locks {
			switch lock.State {
			case corepb.LockState_LOCK_STATE_UNLOCKED:
				// Already unlocked, nothing to do
				continue
			case corepb.LockState_LOCK_STATE_SHARED_LOCKED:
				// Remove the holder
				lock.LockHolders = lo.Filter(lock.LockHolders, func(h *corepb.LockHolder, _ int) bool {
					return h.LeaseId != lease.Id.LeaseId
				})

				// If no read lock holders left
				if len(lock.LockHolders) == 0 {
					// Unlock and delete
					err = c.locks.Delete(txn, lock.Id)
					if err != nil {
						return err
					}

					// Update ancestor entries
					err = c.decrementAncestors(txn, lock.Id, false)
					if err != nil {
						return err
					}

					counters.NumberOfLocks -= 1
				} else {
					// Update lock
					err = c.locks.Update(txn, lock)
					if err != nil {
						return err
					}
				}
			case corepb.LockState_LOCK_STATE_EXCLUSIVE_LOCKED:
				if lock.LockHolders[0].LeaseId == lease.Id.LeaseId {
					// This lease holds the exclusive lock, delete it
					err = c.locks.Delete(txn, lock.Id)
					if err != nil {
						return err
					}

					// Update ancestor entries
					err = c.decrementAncestors(txn, lock.Id, true)
					if err != nil {
						return err
					}

					counters.NumberOfLocks -= 1
				} else {
					// ListByLeaseId returned a lock that is not held by the lease, this should not happen
					return fmt.Errorf("list locks by lease id returned a lock that is not held by the lease")
				}
			}
		}

		// Check if there are more pages
		if locksResult.nextPaginationToken == nil {
			break
		}
		paginationToken = locksResult.nextPaginationToken
	}

	// Delete the lease
	err = c.leases.Delete(txn, lease)
	if err != nil {
		return err
	}

	// Decrement lease counter
	counters.NumberOfLeases -= 1

	// Update counters
	err = c.counters.Set(txn, lease.Id.AccountId, lease.Id.NamespaceId, counters)
	if err != nil {
		return err
	}

	return nil
}

// checkLockExpiration ensures that the lock is still held at the moment `now`. Returns an updated copy of the lock.
func (c *Core) checkLockExpiration(txn *store.Txn, lock *corepb.Lock, now int64) (*corepb.Lock, error) {
	result := proto.Clone(lock).(*corepb.Lock)

	switch lock.State {
	case corepb.LockState_LOCK_STATE_UNLOCKED:
		// Lock is unlocked, return as is
		return result, nil
	case corepb.LockState_LOCK_STATE_SHARED_LOCKED:
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
			result.State = corepb.LockState_LOCK_STATE_UNLOCKED
			result.LockHolders = nil
			result.LockedAt = 0
		}
	case corepb.LockState_LOCK_STATE_EXCLUSIVE_LOCKED:
		lease, err := c.leases.Get(txn, &corepb.LeaseId{
			AccountId:   lock.Id.AccountId,
			NamespaceId: lock.Id.NamespaceId,
			LeaseId:     result.LockHolders[0].LeaseId,
		})
		if err != nil {
			return nil, err
		}
		if lease.ExpiresAt <= now {
			result.State = corepb.LockState_LOCK_STATE_UNLOCKED
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

// maxBlockingLocks bounds how many blocking locks AcquireLock reports back. The
// list is a best-effort diagnostic, so it is always capped to keep the lookup
// cheap regardless of how large a contended subtree is.
const maxBlockingLocks = 50

// checkAncestorConflicts verifies that no ancestor locks block the requested
// lock. Returns UNSPECIFIED if no conflicts, or ANCESTOR plus the blocking
// ancestor lock(s) (capped at maxBlockingLocks) if blocked.
func (c *Core) checkAncestorConflicts(txn *store.Txn, lockId *corepb.LockId, requestExclusive bool) (corepb.ContentionReason, []*corepb.Lock, error) {
	ancestors := c.lockAncestorNames(lockId.LockName)
	blockingLocks := make([]*corepb.Lock, 0)
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
			return corepb.ContentionReason_CONTENTION_REASON_UNSPECIFIED, nil, err
		}

		// An ancestor exclusive lock blocks everything; an ancestor shared lock
		// blocks a descendant exclusive acquire.
		blocks := ancestorLock.State == corepb.LockState_LOCK_STATE_EXCLUSIVE_LOCKED ||
			(requestExclusive && ancestorLock.State == corepb.LockState_LOCK_STATE_SHARED_LOCKED)
		if blocks && len(blockingLocks) < maxBlockingLocks {
			blockingLocks = append(blockingLocks, ancestorLock)
		}
	}

	if len(blockingLocks) > 0 {
		return corepb.ContentionReason_CONTENTION_REASON_ANCESTOR, blockingLocks, nil
	}
	return corepb.ContentionReason_CONTENTION_REASON_UNSPECIFIED, nil, nil
}

// checkDescendantConflicts verifies that no descendant locks block the requested
// lock. Returns UNSPECIFIED if no conflicts, or DESCENDANT plus the blocking
// descendant locks (best-effort, capped at maxBlockingLocks) if blocked.
func (c *Core) checkDescendantConflicts(txn *store.Txn, lockId *corepb.LockId, requestExclusive bool) (corepb.ContentionReason, []*corepb.Lock, error) {
	// Check the ancestors table for this path to see if it has any descendants with locks
	ancestor, err := c.ancestors.Get(txn, lockId)
	if err != nil {
		return corepb.ContentionReason_CONTENTION_REASON_UNSPECIFIED, nil, err
	}

	// A descendant exclusive lock blocks everything; a descendant shared lock
	// blocks an ancestor exclusive acquire.
	blocked := ancestor.ExclusiveCount > 0 || (requestExclusive && ancestor.SharedCount > 0)
	if !blocked {
		return corepb.ContentionReason_CONTENTION_REASON_UNSPECIFIED, nil, nil
	}

	// Bounded scan of the subtree to surface what is holding it. The aggregate
	// counts above already proved a conflict exists; this is only for reporting,
	// so the cap keeps it cheap even for large subtrees.
	descendants, err := c.locks.ListByNamePrefix(txn,
		&corepb.NamespaceId{AccountId: lockId.AccountId, NamespaceId: lockId.NamespaceId},
		lockId.LockName+"/", maxBlockingLocks)
	if err != nil {
		return corepb.ContentionReason_CONTENTION_REASON_UNSPECIFIED, nil, err
	}

	blockingLocks := make([]*corepb.Lock, 0, len(descendants))
	for _, d := range descendants {
		// For a shared acquire only exclusive descendants block; for an exclusive
		// acquire any held descendant blocks.
		if d.State == corepb.LockState_LOCK_STATE_EXCLUSIVE_LOCKED ||
			(requestExclusive && d.State == corepb.LockState_LOCK_STATE_SHARED_LOCKED) {
			blockingLocks = append(blockingLocks, d)
		}
	}

	return corepb.ContentionReason_CONTENTION_REASON_DESCENDANT, blockingLocks, nil
}

// checkHierarchicalConflicts checks both ancestor and descendant conflicts.
// Returns UNSPECIFIED if the lock can be acquired, or the reason (ANCESTOR /
// DESCENDANT) plus the blocking locks if blocked. Ancestors are checked first.
func (c *Core) checkHierarchicalConflicts(txn *store.Txn, lockId *corepb.LockId, requestExclusive bool) (corepb.ContentionReason, []*corepb.Lock, error) {
	// Check ancestors first (direct lock lookups)
	reason, blockingLocks, err := c.checkAncestorConflicts(txn, lockId, requestExclusive)
	if err != nil {
		return corepb.ContentionReason_CONTENTION_REASON_UNSPECIFIED, nil, err
	}
	if reason != corepb.ContentionReason_CONTENTION_REASON_UNSPECIFIED {
		return reason, blockingLocks, nil
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
