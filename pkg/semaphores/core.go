package semaphores

import (
	"errors"
	"fmt"
	"io"
	"math"

	"github.com/evrblk/monstera"
	"github.com/evrblk/monstera/store"
	monsterax "github.com/evrblk/monstera/x"
	"github.com/samber/lo"
	"google.golang.org/protobuf/proto"

	"github.com/evrblk/grackle/pkg/coreapis"
	"github.com/evrblk/grackle/pkg/corepb"
	"github.com/evrblk/grackle/pkg/ids"
	"github.com/evrblk/grackle/pkg/pagination"
	"github.com/evrblk/grackle/pkg/tables"
)

// Core implements the per-shard semaphores state machine on top of a Badger store.
// It is the Monstera application core for the semaphores service and owns the
// semaphores, their holders, leases, namespace counters, GC index, and semaphore expiration index.
type Core struct {
	badgerStore *store.BadgerStore

	semaphores        *semaphoresTable
	holders           *holdersTable
	counters          *tables.CountersTable[*corepb.SemaphoresCounter, corepb.SemaphoresCounter]
	gcRecords         *tables.GCRecordsTable[*corepb.SemaphoresGarbageCollectionRecord, corepb.SemaphoresGarbageCollectionRecord]
	expirationRecords *expirationRecordsTable
	leases            *tables.LeasesTable
}

var _ coreapis.GrackleSemaphoresCoreApi = &Core{}

// NewCore constructs a Core bound to a single Monstera shard.
// shardLowerBound/shardUpperBound delimit per-shard tables; shardGlobalIndexPrefix is used for
// global (non-sharded) secondary indexes such as the lease expiration index.
func NewCore(badgerStore *store.BadgerStore, shardGlobalIndexPrefix []byte, shardLowerBound []byte, shardUpperBound []byte) *Core {
	return &Core{
		badgerStore: badgerStore,

		semaphores: newSemaphoresTable(shardLowerBound, shardUpperBound),
		holders:    newHoldersTable(shardLowerBound, shardUpperBound),
		counters: tables.NewCountersTable[*corepb.SemaphoresCounter, corepb.SemaphoresCounter](
			tables.Grackle["Grackle.SemaphoresCore.Counters.Table"].Bytes(),
			shardLowerBound,
			shardUpperBound,
		),
		gcRecords: tables.NewGCRecordsTable[*corepb.SemaphoresGarbageCollectionRecord, corepb.SemaphoresGarbageCollectionRecord](
			tables.Grackle["Grackle.SemaphoresCore.GarbageCollectionRecords.Table"].Bytes(),
			shardGlobalIndexPrefix,
		),
		expirationRecords: newExpirationRecordsTable(shardGlobalIndexPrefix),
		leases: tables.NewLeasesTable(
			shardLowerBound,
			shardUpperBound,
			shardGlobalIndexPrefix,
			tables.Grackle["Grackle.SemaphoresCore.Leases.Table"].Bytes(),
			tables.Grackle["Grackle.SemaphoresCore.Leases.ProcessIdIndex"].Bytes(),
			tables.Grackle["Grackle.SemaphoresCore.Leases.ExpirationIndex"].Bytes(),
		),
	}
}

func (c *Core) ranges() []monsterax.KeyRange {
	ranges := []monsterax.KeyRange{
		c.counters.GetTableKeyRange(),
		c.gcRecords.GetTableKeyRange(),
		c.expirationRecords.GetTableKeyRange(),
	}

	ranges = append(ranges, c.semaphores.GetTableKeyRanges()...)
	ranges = append(ranges, c.holders.GetTableKeyRanges()...)
	ranges = append(ranges, c.leases.GetTableKeyRanges()...)

	return ranges
}

// Snapshot returns a Monstera snapshot of all key ranges owned by this core, suitable for
// Raft log compaction or transfer to a new replica.
func (c *Core) Snapshot() monstera.ApplicationCoreSnapshot {
	return monsterax.Snapshot(c.badgerStore, c.ranges())
}

// Restore replaces the contents of this core's key ranges with the snapshot read from reader.
// Existing data in those ranges is removed.
func (c *Core) Restore(reader io.ReadCloser) error {
	return monsterax.Restore(c.badgerStore, c.ranges(), reader)
}

// Close releases resources held by the core. Currently, a no-op; the underlying BadgerStore is
// owned by the caller.
func (c *Core) Close() {

}

// CreateSemaphore creates a new semaphore in the target namespace.
// Returns a ResourceExhausted application error when the namespace has reached
// MaxNumberOfSemaphoresPerNamespace, or AlreadyExists when a semaphore with the same name exists.
func (c *Core) CreateSemaphore(req *coreapis.CreateSemaphoreRequest) (*coreapis.CreateSemaphoreResponse, error) {
	if req.Payload.Permits == 0 {
		return &coreapis.CreateSemaphoreResponse{
			ApplicationError: monsterax.NewErrorWithContext(
				monsterax.InvalidArgument,
				"permits must be greater than 0",
				map[string]string{
					"permits": fmt.Sprintf("%d", req.Payload.Permits),
				},
			),
		}, nil
	}

	txn := c.badgerStore.Update()
	defer txn.Discard()

	// Get counters for that namespace
	counters, err := c.counters.Get(txn, req.Payload.SemaphoreId.AccountId, req.Payload.SemaphoreId.NamespaceId)
	if err != nil {
		return nil, err
	}

	// Checking max number of semaphores
	if counters.NumberOfSemaphores >= req.Payload.MaxNumberOfSemaphoresPerNamespace {
		return &coreapis.CreateSemaphoreResponse{
			ApplicationError: monsterax.NewErrorWithContext(
				monsterax.ResourceExhausted,
				"max number of semaphores per namespace reached",
				map[string]string{
					"limit": fmt.Sprintf("%d", req.Payload.MaxNumberOfSemaphoresPerNamespace),
				},
			),
		}, nil
	}

	semaphore := &corepb.Semaphore{
		Id:             req.Payload.SemaphoreId,
		Name:           req.Payload.Name,
		Description:    req.Payload.Description,
		Permits:        req.Payload.Permits,
		CreatedAt:      req.Payload.Now,
		UpdatedAt:      req.Payload.Now,
		Metadata:       req.Payload.Metadata,
		Version:        1,
		LastActivityAt: req.Payload.Now,
	}

	appError, err := c.semaphores.Create(txn, semaphore)
	if err != nil {
		return nil, err
	}
	if appError != nil {
		return &coreapis.CreateSemaphoreResponse{
			ApplicationError: appError,
		}, nil
	}

	// Update counters
	counters.NumberOfSemaphores += 1
	err = c.counters.Set(txn, req.Payload.SemaphoreId.AccountId, req.Payload.SemaphoreId.NamespaceId, counters)
	if err != nil {
		return nil, err
	}

	err = txn.Commit()
	if err != nil {
		return nil, err
	}

	return &coreapis.CreateSemaphoreResponse{
		Payload: &corepb.CreateSemaphoreResponse{
			Semaphore: semaphore,
		},
	}, nil
}

// UpdateSemaphore changes the description and permit count of an existing semaphore.
// Expired holders are pruned before the check so that a stale ActiveHolds count cannot block
// a legitimate shrink. Returns NotFound if the semaphore does not exist, or InvalidArgument if
// the new permit count is below the current ActiveHolds.
func (c *Core) UpdateSemaphore(req *coreapis.UpdateSemaphoreRequest) (*coreapis.UpdateSemaphoreResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	semaphore, err := c.semaphores.GetByName(txn, req.Payload.NamespaceId.AccountId, req.Payload.NamespaceId.NamespaceId, req.Payload.SemaphoreName)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return &coreapis.UpdateSemaphoreResponse{
				ApplicationError: monsterax.NewErrorWithContext(
					monsterax.NotFound,
					"semaphore not found",
					map[string]string{
						"semaphore_name": req.Payload.SemaphoreName,
					},
				),
			}, nil
		}

		return nil, err
	}

	if semaphore.Version != req.Payload.ExpectedVersion {
		return &coreapis.UpdateSemaphoreResponse{
			ApplicationError: monsterax.NewErrorWithContext(
				monsterax.InvalidArgument,
				"version mismatch",
				map[string]string{
					"semaphore_name":   req.Payload.SemaphoreName,
					"actual_version":   fmt.Sprintf("%d", semaphore.Version),
					"expected_version": fmt.Sprintf("%d", req.Payload.ExpectedVersion),
				},
			),
		}, nil
	}

	// Check expired holders
	updatedSemaphore, _, err := c.deleteExpiredSemaphoreHolders(txn, semaphore, req.Payload.Now)
	if err != nil {
		return nil, err
	}

	// If there are currently more holds than the new amount of permits
	if updatedSemaphore.ActiveHolds > req.Payload.Permits {
		return &coreapis.UpdateSemaphoreResponse{
			ApplicationError: monsterax.NewErrorWithContext(
				monsterax.InvalidArgument,
				"there are currently more holds than the new amount of permits",
				map[string]string{
					"semaphore_name": req.Payload.SemaphoreName,
					"actual_holds":   fmt.Sprintf("%d", updatedSemaphore.ActiveHolds),
					"new_permits":    fmt.Sprintf("%d", req.Payload.Permits),
				},
			),
		}, nil
	}

	updatedSemaphore.Description = req.Payload.Description
	updatedSemaphore.Permits = req.Payload.Permits
	updatedSemaphore.UpdatedAt = req.Payload.Now
	updatedSemaphore.Metadata = req.Payload.Metadata
	updatedSemaphore.Version += 1

	// Reconcile expirationRecords when the prune changed which holder expires first. Without
	// this, the global expiration index keeps a row pointing at the old earliest timestamp;
	// the GC sweep will revisit it forever and the namespace's lease-counter accounting can
	// drift.
	if semaphore.EarliestHolderExpiresAt != updatedSemaphore.EarliestHolderExpiresAt {
		if semaphore.EarliestHolderExpiresAt != 0 {
			err = c.expirationRecords.Delete(txn, semaphore.EarliestHolderExpiresAt, semaphore.Id)
			if err != nil {
				return nil, err
			}
		}

		if updatedSemaphore.EarliestHolderExpiresAt != 0 {
			err = c.expirationRecords.Add(txn, updatedSemaphore.EarliestHolderExpiresAt, semaphore.Id)
			if err != nil {
				return nil, err
			}
		}
	}

	err = c.semaphores.Update(txn, updatedSemaphore)
	if err != nil {
		return nil, err
	}

	err = txn.Commit()
	if err != nil {
		return nil, err
	}

	return &coreapis.UpdateSemaphoreResponse{
		Payload: &corepb.UpdateSemaphoreResponse{
			Semaphore: updatedSemaphore,
		},
	}, nil
}

// DeleteSemaphore removes a semaphore by name along with its expiration-index entry and decrements
// the namespace counter. A missing semaphore is treated as success (no error, empty response).
// Holders are not released synchronously; instead a GC record is created so that
// RunSemaphoresGarbageCollection can drain them in bounded batches.
func (c *Core) DeleteSemaphore(req *coreapis.DeleteSemaphoreRequest) (*coreapis.DeleteSemaphoreResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	semaphore, err := c.semaphores.GetByName(txn, req.Payload.NamespaceId.AccountId, req.Payload.NamespaceId.NamespaceId, req.Payload.SemaphoreName)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			// No semaphore exists, do nothing
			return &coreapis.DeleteSemaphoreResponse{
				Payload: &corepb.DeleteSemaphoreResponse{},
			}, nil
		}

		return nil, err
	}

	// Get counters for this namespace
	counters, err := c.counters.Get(txn, req.Payload.NamespaceId.AccountId, req.Payload.NamespaceId.NamespaceId)
	if err != nil {
		return nil, err
	}

	// Remove semaphore from expirationRecords
	if semaphore.EarliestHolderExpiresAt != 0 {
		err = c.expirationRecords.Delete(txn, semaphore.EarliestHolderExpiresAt, semaphore.Id)
		if err != nil {
			return nil, err
		}
	}

	err = c.semaphores.Delete(txn, semaphore.Id)
	if err != nil {
		return nil, err
	}

	// Schedule asynchronous cleanup of leftover holders. The semaphore record itself is already
	// gone; GC just needs the semaphore_id to drain the remaining holders.
	err = c.gcRecords.Create(txn, &corepb.SemaphoresGarbageCollectionRecord{
		Id: req.Payload.RecordId,
		Record: &corepb.SemaphoresGarbageCollectionRecord_SemaphoreId{
			SemaphoreId: semaphore.Id,
		},
	})
	if err != nil {
		return nil, err
	}

	// Update counters
	counters.NumberOfSemaphores -= 1
	err = c.counters.Set(txn, req.Payload.NamespaceId.AccountId, req.Payload.NamespaceId.NamespaceId, counters)
	if err != nil {
		return nil, err
	}

	err = txn.Commit()
	if err != nil {
		return nil, err
	}

	return &coreapis.DeleteSemaphoreResponse{
		Payload: &corepb.DeleteSemaphoreResponse{},
	}, nil
}

// GetSemaphore looks up a semaphore by SemaphoreId. Because it runs on a read-only transaction it
// cannot remove expired holders; instead it returns a copy of the semaphore with `ActiveHolds`,
// `ActiveHoldersCount`, and `EarliestHolderExpiresAt` adjusted as if holders that expired by `now`
// had been removed. Expired rows are cleaned up by GC. Returns NotFound if the semaphore does not
// exist.
func (c *Core) GetSemaphore(req *coreapis.GetSemaphoreRequest) (*coreapis.GetSemaphoreResponse, error) {
	txn := c.badgerStore.View()
	defer txn.Discard()

	semaphore, err := c.semaphores.Get(txn, req.Payload.SemaphoreId)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return &coreapis.GetSemaphoreResponse{
				ApplicationError: monsterax.NewErrorWithContext(
					monsterax.NotFound,
					"semaphore not found",
					map[string]string{
						"semaphore_id": ids.EncodeSemaphoreId(req.Payload.SemaphoreId),
					},
				),
			}, nil
		}

		return nil, err
	}

	// Filter out expired holders
	updatedSemaphore, _, err := c.computeExpiredSemaphoreHolders(txn, semaphore, req.Payload.Now)
	if err != nil {
		return nil, err
	}

	return &coreapis.GetSemaphoreResponse{
		Payload: &corepb.GetSemaphoreResponse{
			Semaphore: updatedSemaphore,
		},
	}, nil
}

// GetSemaphoreByName is the by-name counterpart of GetSemaphore. Because it runs on a read-only
// transaction it cannot remove expired holders; instead it returns a copy of the semaphore with
// `ActiveHolds`, `ActiveHoldersCount`, and `EarliestHolderExpiresAt` adjusted as if holders that
// expired by `now` had been removed. Returns NotFound when no semaphore with that name exists in
// the namespace.
func (c *Core) GetSemaphoreByName(req *coreapis.GetSemaphoreByNameRequest) (*coreapis.GetSemaphoreByNameResponse, error) {
	txn := c.badgerStore.View()
	defer txn.Discard()

	semaphore, err := c.semaphores.GetByName(txn, req.Payload.NamespaceId.AccountId, req.Payload.NamespaceId.NamespaceId, req.Payload.SemaphoreName)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return &coreapis.GetSemaphoreByNameResponse{
				ApplicationError: monsterax.NewErrorWithContext(
					monsterax.NotFound,
					"semaphore not found",
					map[string]string{
						"semaphore_name": req.Payload.SemaphoreName,
					},
				),
			}, nil
		}

		return nil, err
	}

	// Filter out expired holders
	updatedSemaphore, _, err := c.computeExpiredSemaphoreHolders(txn, semaphore, req.Payload.Now)
	if err != nil {
		return nil, err
	}

	return &coreapis.GetSemaphoreByNameResponse{
		Payload: &corepb.GetSemaphoreByNameResponse{
			Semaphore: updatedSemaphore,
		},
	}, nil
}

// ListSemaphoreHolders returns a page of holders for a semaphore, with holders that have expired
// by `now` filtered out. The transaction is read-only, so expired entries remain in the store
// and are cleaned up by GC or by a subsequent write path. Returns NotFound when the semaphore
// does not exist.
func (c *Core) ListSemaphoreHolders(req *coreapis.ListSemaphoreHoldersRequest) (*coreapis.ListSemaphoreHoldersResponse, error) {
	txn := c.badgerStore.View()
	defer txn.Discard()

	semaphore, err := c.semaphores.GetByName(txn, req.Payload.NamespaceId.AccountId, req.Payload.NamespaceId.NamespaceId, req.Payload.SemaphoreName)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return &coreapis.ListSemaphoreHoldersResponse{
				ApplicationError: monsterax.NewErrorWithContext(
					monsterax.NotFound,
					"semaphore not found",
					map[string]string{
						"semaphore_name": req.Payload.SemaphoreName,
					},
				),
			}, nil
		}

		return nil, err
	}

	result, err := c.holders.List(txn, req.Payload.NamespaceId.AccountId, req.Payload.NamespaceId.NamespaceId, semaphore.Id.SemaphoreId, req.Payload.PaginationToken, pagination.GetLimitWithDefaults(int(req.Payload.Limit)))
	if err != nil {
		return nil, err
	}

	activeHolders := lo.Filter(result.holders, func(h *corepb.SemaphoreHolder, _ int) bool {
		return h.ExpiresAt > req.Payload.Now
	})

	return &coreapis.ListSemaphoreHoldersResponse{
		Payload: &corepb.ListSemaphoreHoldersResponse{
			Holders:                 activeHolders,
			NextPaginationToken:     result.nextPaginationToken,
			PreviousPaginationToken: result.previousPaginationToken,
		},
	}, nil
}

// ListSemaphores returns semaphores in a namespace. Because it runs on a read-only
// transaction it cannot remove expired holders; instead it returns a copy of each
// semaphore with `ActiveHolds`, `ActiveHoldersCount`, and `EarliestHolderExpiresAt`
// adjusted as if expired holders had been removed.
func (c *Core) ListSemaphores(req *coreapis.ListSemaphoresRequest) (*coreapis.ListSemaphoresResponse, error) {
	txn := c.badgerStore.View()
	defer txn.Discard()

	result, err := c.semaphores.List(txn, req.Payload.NamespaceId.AccountId, req.Payload.NamespaceId.NamespaceId, req.Payload.PaginationToken, pagination.GetLimitWithDefaults(int(req.Payload.Limit)))
	if err != nil {
		return nil, err
	}

	semaphores := make([]*corepb.Semaphore, 0, len(result.semaphores))
	for _, semaphore := range result.semaphores {
		updatedSemaphore, _, err := c.computeExpiredSemaphoreHolders(txn, semaphore, req.Payload.Now)
		if err != nil {
			return nil, err
		}
		semaphores = append(semaphores, updatedSemaphore)
	}

	return &coreapis.ListSemaphoresResponse{
		Payload: &corepb.ListSemaphoresResponse{
			Semaphores:              semaphores,
			NextPaginationToken:     result.nextPaginationToken,
			PreviousPaginationToken: result.previousPaginationToken,
		},
	}, nil
}

// AcquireSemaphore attempts to acquire `Weight` permits on the named semaphore under the given lease.
// If the lease already holds the semaphore, the existing holder's expiration is extended to the
// lease's ExpiresAt (the weight is not changed). Expired holders are pruned before the permit
// check so an expired holder's permits become available immediately.
// Returns Payload.Success=false (without an application error) when the request is valid but
// permits are unavailable. Returns NotFound application errors for missing/expired leases or a
// missing semaphore, and InvalidArgument when Weight == 0 or Weight exceeds the semaphore's
// permits (a request that could never be satisfied no matter how long the caller waits).
func (c *Core) AcquireSemaphore(req *coreapis.AcquireSemaphoreRequest) (*coreapis.AcquireSemaphoreResponse, error) {
	if req.Payload.Weight == 0 {
		return &coreapis.AcquireSemaphoreResponse{
			ApplicationError: monsterax.NewErrorWithContext(
				monsterax.InvalidArgument,
				"weight must be greater than 0",
				map[string]string{
					"weight": fmt.Sprintf("%d", req.Payload.Weight),
				},
			),
		}, nil
	}

	txn := c.badgerStore.Update()
	defer txn.Discard()

	// Validate and get the lease
	leaseId := &corepb.LeaseId{
		AccountId:   req.Payload.NamespaceId.AccountId,
		NamespaceId: req.Payload.NamespaceId.NamespaceId,
		LeaseId:     req.Payload.LeaseId,
	}
	lease, err := c.leases.Get(txn, leaseId)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return &coreapis.AcquireSemaphoreResponse{
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
		return &coreapis.AcquireSemaphoreResponse{
			ApplicationError: monsterax.NewErrorWithContext(
				monsterax.NotFound,
				"lease not found",
				map[string]string{
					"lease_id": fmt.Sprintf("%d", req.Payload.LeaseId),
				},
			),
		}, nil
	}

	semaphore, err := c.semaphores.GetByName(txn, req.Payload.NamespaceId.AccountId, req.Payload.NamespaceId.NamespaceId, req.Payload.SemaphoreName)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return &coreapis.AcquireSemaphoreResponse{
				ApplicationError: monsterax.NewErrorWithContext(
					monsterax.NotFound,
					"semaphore not found",
					map[string]string{
						"semaphore_name": req.Payload.SemaphoreName,
					},
				),
			}, nil
		}

		return nil, err
	}

	// A weight larger than the semaphore's total permits can never be satisfied,
	// no matter how many holders release. Reject it as an invalid request rather
	// than blocking the caller until timeout on an impossible condition.
	if req.Payload.Weight > semaphore.Permits {
		return &coreapis.AcquireSemaphoreResponse{
			ApplicationError: monsterax.NewErrorWithContext(
				monsterax.InvalidArgument,
				"weight exceeds semaphore permits",
				map[string]string{
					"weight":  fmt.Sprintf("%d", req.Payload.Weight),
					"permits": fmt.Sprintf("%d", semaphore.Permits),
				},
			),
		}, nil
	}

	// Check expired holders
	updatedSemaphore, _, err := c.deleteExpiredSemaphoreHolders(txn, semaphore, req.Payload.Now)
	if err != nil {
		return nil, err
	}

	success := false

	// Check if the same process_id already holds the semaphore here.
	holderId := &corepb.SemaphoreHolderId{
		AccountId:   req.Payload.NamespaceId.AccountId,
		NamespaceId: req.Payload.NamespaceId.NamespaceId,
		SemaphoreId: semaphore.Id.SemaphoreId,
		LeaseId:     lease.Id.LeaseId,
	}
	existingHolder, err := c.holders.Get(txn, holderId)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			// Check if there are enough permits
			if req.Payload.Weight <= updatedSemaphore.Permits-updatedSemaphore.ActiveHolds {
				// Add a new lock holder
				newHolder := &corepb.SemaphoreHolder{
					Id:        holderId,
					ExpiresAt: lease.ExpiresAt,
					LockedAt:  req.Payload.Now,
					Weight:    req.Payload.Weight,
					Metadata:  req.Payload.Metadata,
				}

				err = c.holders.Create(txn, newHolder)
				if err != nil {
					return nil, err
				}

				// Add to lease ID index
				err = c.semaphores.AddLeaseToIndex(txn, semaphore.Id, lease.Id.LeaseId)
				if err != nil {
					return nil, err
				}

				updatedSemaphore.ActiveHoldersCount += 1
				updatedSemaphore.ActiveHolds += req.Payload.Weight

				// Update earliest expiration if this is the first holder or expires earlier
				if updatedSemaphore.EarliestHolderExpiresAt == 0 || newHolder.ExpiresAt < updatedSemaphore.EarliestHolderExpiresAt {
					updatedSemaphore.EarliestHolderExpiresAt = newHolder.ExpiresAt
				}

				success = true
			}
		} else {
			return nil, err
		}
	} else {
		// Same lease re-acquiring an existing hold. Compute the permit delta:
		//   - new == old: no change in committed permits.
		//   - new < old: surplus permits are freed.
		//   - new > old: only if the extra permits fit under the semaphore's permit cap.
		// When the request would grow the hold past the cap, leave the existing holder untouched
		// (weight and expiration unchanged) and report failure.
		canAcquire := true
		if req.Payload.Weight > existingHolder.Weight {
			delta := req.Payload.Weight - existingHolder.Weight
			canAcquire = updatedSemaphore.Permits >= updatedSemaphore.ActiveHolds+delta
		}
		if canAcquire {
			updatedSemaphore.ActiveHolds = updatedSemaphore.ActiveHolds - existingHolder.Weight + req.Payload.Weight
			existingHolder.Weight = req.Payload.Weight

			// Update expiration time (extend lock)
			existingHolder.ExpiresAt = lease.ExpiresAt
			existingHolder.LockedAt = req.Payload.Now
			existingHolder.Metadata = req.Payload.Metadata

			// Update earliest expiration if this holder expires earlier
			if updatedSemaphore.EarliestHolderExpiresAt == 0 || existingHolder.ExpiresAt < updatedSemaphore.EarliestHolderExpiresAt {
				updatedSemaphore.EarliestHolderExpiresAt = existingHolder.ExpiresAt
			}

			err := c.holders.Update(txn, existingHolder)
			if err != nil {
				return nil, err
			}

			success = true
		}
	}

	if semaphore.EarliestHolderExpiresAt != updatedSemaphore.EarliestHolderExpiresAt {
		// Remove a semaphore from expirationRecords at old position
		if semaphore.EarliestHolderExpiresAt != 0 {
			err = c.expirationRecords.Delete(txn, semaphore.EarliestHolderExpiresAt, semaphore.Id)
			if err != nil {
				return nil, err
			}
		}

		if updatedSemaphore.EarliestHolderExpiresAt != 0 {
			// Add a semaphore into expirationRecords at new position
			err = c.expirationRecords.Add(txn, updatedSemaphore.EarliestHolderExpiresAt, semaphore.Id)
			if err != nil {
				return nil, err
			}
		}
	}

	// Record the acquire attempt (whether or not it succeeded).
	updatedSemaphore.LastActivityAt = req.Payload.Now

	err = c.semaphores.Update(txn, updatedSemaphore)
	if err != nil {
		return nil, err
	}

	err = txn.Commit()
	if err != nil {
		return nil, err
	}

	return &coreapis.AcquireSemaphoreResponse{
		Payload: &corepb.AcquireSemaphoreResponse{
			Semaphore: updatedSemaphore,
			Success:   success,
		},
	}, nil
}

// ReleaseSemaphore releases the lease's hold on a semaphore, freeing its permits.
// Releasing a semaphore that the lease does not hold is treated as success (the semaphore is
// returned unchanged). Returns NotFound for a missing lease or semaphore.
func (c *Core) ReleaseSemaphore(req *coreapis.ReleaseSemaphoreRequest) (*coreapis.ReleaseSemaphoreResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	// Validate and get the lease
	leaseId := &corepb.LeaseId{
		AccountId:   req.Payload.NamespaceId.AccountId,
		NamespaceId: req.Payload.NamespaceId.NamespaceId,
		LeaseId:     req.Payload.LeaseId,
	}
	lease, err := c.leases.Get(txn, leaseId)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return &coreapis.ReleaseSemaphoreResponse{
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

	// Treat an expired lease the same as a missing one — its holders are already on the GC's
	// eviction path, so Release should report the lease as gone instead of mutating state.
	if lease.ExpiresAt <= req.Payload.Now {
		return &coreapis.ReleaseSemaphoreResponse{
			ApplicationError: monsterax.NewErrorWithContext(
				monsterax.NotFound,
				"lease not found",
				map[string]string{
					"lease_id": fmt.Sprintf("%d", req.Payload.LeaseId),
				},
			),
		}, nil
	}

	semaphore, err := c.semaphores.GetByName(txn, req.Payload.NamespaceId.AccountId, req.Payload.NamespaceId.NamespaceId, req.Payload.SemaphoreName)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return &coreapis.ReleaseSemaphoreResponse{
				ApplicationError: monsterax.NewErrorWithContext(
					monsterax.NotFound,
					"semaphore not found",
					map[string]string{
						"semaphore_name": req.Payload.SemaphoreName,
					},
				),
			}, nil
		}

		return nil, err
	}

	// Remove the holder by process_id (if exists)
	holderId := &corepb.SemaphoreHolderId{
		AccountId:   req.Payload.NamespaceId.AccountId,
		NamespaceId: req.Payload.NamespaceId.NamespaceId,
		SemaphoreId: semaphore.Id.SemaphoreId,
		LeaseId:     lease.Id.LeaseId,
	}
	existingHolder, err := c.holders.Get(txn, holderId)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return &coreapis.ReleaseSemaphoreResponse{
				Payload: &corepb.ReleaseSemaphoreResponse{
					Semaphore: semaphore,
				},
			}, nil
		}

		return nil, err
	}

	err = c.holders.Delete(txn, existingHolder)
	if err != nil {
		return nil, err
	}

	// Remove from lease ID index
	err = c.semaphores.RemoveLeaseFromIndex(txn, semaphore.Id, lease.Id.LeaseId)
	if err != nil {
		return nil, err
	}

	// Check expired holders
	updatedSemaphore, _, err := c.deleteExpiredSemaphoreHolders(txn, semaphore, req.Payload.Now)
	if err != nil {
		return nil, err
	}

	updatedSemaphore.ActiveHolds -= existingHolder.Weight
	updatedSemaphore.ActiveHoldersCount -= 1
	updatedSemaphore.LastActivityAt = req.Payload.Now

	if semaphore.EarliestHolderExpiresAt != updatedSemaphore.EarliestHolderExpiresAt {
		// Remove a semaphore from expirationRecords at old position
		if semaphore.EarliestHolderExpiresAt != 0 {
			err = c.expirationRecords.Delete(txn, semaphore.EarliestHolderExpiresAt, semaphore.Id)
			if err != nil {
				return nil, err
			}
		}

		if updatedSemaphore.EarliestHolderExpiresAt != 0 {
			// Add a semaphore into expirationRecords at new position
			err = c.expirationRecords.Add(txn, updatedSemaphore.EarliestHolderExpiresAt, semaphore.Id)
			if err != nil {
				return nil, err
			}
		}
	}

	err = c.semaphores.Update(txn, updatedSemaphore)
	if err != nil {
		return nil, err
	}

	err = txn.Commit()
	if err != nil {
		return nil, err
	}

	return &coreapis.ReleaseSemaphoreResponse{
		Payload: &corepb.ReleaseSemaphoreResponse{
			Semaphore: updatedSemaphore,
		},
	}, nil
}

// RunSemaphoresGarbageCollection performs a single bounded GC pass. It processes namespace
// deletion records (deleting holders for every semaphore in the namespace, then the semaphore
// itself), semaphore deletion records (draining the leftover holders of a previously deleted
// semaphore), and finally walks the global expiration index to prune expired holders from live
// semaphores. The pass stops once MaxVisited total records (holders + semaphores + leases) have
// been touched so that one invocation cannot produce an unbounded transaction. Intended to be
// invoked periodically by the scheduler.
func (c *Core) RunSemaphoresGarbageCollection(req *coreapis.RunSemaphoresGarbageCollectionRequest) (*coreapis.RunSemaphoresGarbageCollectionResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	visited := int64(0)
	holdersPageSize := int(req.Payload.GcRecordHoldersPageSize)

	// List one page of GC records
	gcRecords, err := c.gcRecords.List(txn, int(req.Payload.GcRecordsPageSize))
	if err != nil {
		return nil, err
	}

	for _, gcRecord := range gcRecords {
		switch r := gcRecord.Record.(type) {
		case *corepb.SemaphoresGarbageCollectionRecord_NamespaceId:
			// Delete counters for that namespace. Will not fail if counters do not exist.
			err := c.counters.Delete(txn, r.NamespaceId.AccountId, r.NamespaceId.NamespaceId)
			if err != nil {
				return nil, err
			}

			// List one page of semaphores for that namespace
			result, err := c.semaphores.List(txn, r.NamespaceId.AccountId, r.NamespaceId.NamespaceId, nil, int(req.Payload.GcRecordSemaphoresPageSize))
			if err != nil {
				return nil, err
			}

			allSemaphoresDeleted := true
			for _, semaphore := range result.semaphores {
				// Drain one page of holders first; if there are more holders than fit on the page,
				// leave the semaphore record in place and let a later GC pass continue.
				holdersDrained, err := c.gcDeleteSemaphoreHolders(txn, semaphore.Id, holdersPageSize, &visited, req.Payload.MaxVisited)
				if err != nil {
					return nil, err
				}
				if visited >= req.Payload.MaxVisited {
					goto commit
				}
				if !holdersDrained {
					allSemaphoresDeleted = false
					break
				}

				// All holders for this semaphore are gone — delete the semaphore record itself.
				if semaphore.EarliestHolderExpiresAt != 0 {
					err = c.expirationRecords.Delete(txn, semaphore.EarliestHolderExpiresAt, semaphore.Id)
					if err != nil {
						return nil, err
					}
				}
				err = c.semaphores.Delete(txn, semaphore.Id)
				if err != nil {
					return nil, err
				}
				visited++
				if visited >= req.Payload.MaxVisited {
					goto commit
				}
			}

			// Delete the GC record only when every semaphore in this namespace has been fully drained
			// (no remaining holders, no more semaphore pages).
			if allSemaphoresDeleted && result.nextPaginationToken == nil {
				err := c.gcRecords.Delete(txn, gcRecord)
				if err != nil {
					return nil, err
				}
			}
		case *corepb.SemaphoresGarbageCollectionRecord_SemaphoreId:
			// The semaphore record itself is already deleted by DeleteSemaphore; we just need to
			// drain whatever holders are still attached to its id.
			holdersDrained, err := c.gcDeleteSemaphoreHolders(txn, r.SemaphoreId, holdersPageSize, &visited, req.Payload.MaxVisited)
			if err != nil {
				return nil, err
			}
			if visited >= req.Payload.MaxVisited {
				goto commit
			}
			if holdersDrained {
				err = c.gcRecords.Delete(txn, gcRecord)
				if err != nil {
					return nil, err
				}
			}
		}
	}

	if visited < req.Payload.MaxVisited {
		// Update semaphores with expired holders
		err = c.expirationRecords.List(txn, 0, req.Payload.Now, func(record *corepb.SemaphoresExpirationRecord) (bool, error) {
			// One visit for the semaphore row itself; the pruned holders are counted below.
			visited++

			// Get the semaphore
			semaphore, err := c.semaphores.Get(txn, record.SemaphoreId)
			if err != nil {
				if errors.Is(err, store.ErrNotFound) {
					// Stale expirationRecord pointing at a deleted semaphore. Drop the index
					// row and continue — otherwise a single bad row blocks the sweep forever.
					err = c.expirationRecords.Delete(txn, record.ExpiresAt, record.SemaphoreId)
					if err != nil && !errors.Is(err, store.ErrNotFound) {
						return false, err
					}
					return visited < req.Payload.MaxVisited, nil
				}
				return false, err
			}

			// Delete the iterated record. Using record.ExpiresAt (not
			// semaphore.EarliestHolderExpiresAt) is what self-heals stale/duplicate rows
			// and the EarliestHolderExpiresAt == 0 case where the old code skipped the
			// delete entirely. The canonical earliest position is rewritten below.
			err = c.expirationRecords.Delete(txn, record.ExpiresAt, record.SemaphoreId)
			if err != nil && !errors.Is(err, store.ErrNotFound) {
				return false, err
			}

			updatedSemaphore, expiredCount, err := c.deleteExpiredSemaphoreHolders(txn, semaphore, req.Payload.Now)
			if err != nil {
				return false, err
			}
			// Credit every pruned holder against the budget. The transaction size grows by
			// roughly one holder row + one lease-index entry per expired holder, so
			// under-counting here is the dominant gap when a semaphore has many holders.
			visited += int64(expiredCount)

			// If semaphore still has holders it will have non-zero expiration time
			if updatedSemaphore.EarliestHolderExpiresAt != 0 {
				// Add a semaphore into expirationRecords at new position
				err = c.expirationRecords.Add(txn, updatedSemaphore.EarliestHolderExpiresAt, semaphore.Id)
				if err != nil {
					return false, err
				}
			}

			err = c.semaphores.Update(txn, updatedSemaphore)
			if err != nil {
				return false, err
			}

			// Stop if we have visited enough locks
			return visited < req.Payload.MaxVisited, nil
		})
		if err != nil {
			return nil, err
		}
	}

	if visited < req.Payload.MaxVisited {
		// Reap expired leases. Each released holder counts against MaxVisited;
		// if the budget runs out mid-lease, revokeLeaseInTransactionBounded reports
		// drained=false and the lease row is left in place for a subsequent GC pass.
		err = c.leases.ListByExpiration(txn, 0, req.Payload.Now, func(lease *corepb.Lease) (bool, error) {
			drained, err := c.revokeLeaseInTransactionBounded(txn, lease, req.Payload.Now, &visited, req.Payload.MaxVisited)
			if err != nil {
				return false, err
			}
			if !drained {
				return false, nil
			}
			return visited < req.Payload.MaxVisited, nil
		})
		if err != nil {
			return nil, err
		}
	}

commit:

	err = txn.Commit()
	if err != nil {
		return nil, err
	}

	return &coreapis.RunSemaphoresGarbageCollectionResponse{
		Payload: &corepb.RunSemaphoresGarbageCollectionResponse{},
	}, nil
}

// SemaphoresDeleteNamespace marks a namespace for asynchronous deletion by creating a GC record.
// The actual removal of the namespace's semaphores and counters happens in subsequent
// RunSemaphoresGarbageCollection passes.
func (c *Core) SemaphoresDeleteNamespace(req *coreapis.SemaphoresDeleteNamespaceRequest) (*coreapis.SemaphoresDeleteNamespaceResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	// Mark the namespace as deleted
	err := c.gcRecords.Create(txn, &corepb.SemaphoresGarbageCollectionRecord{
		Id: req.Payload.RecordId,
		Record: &corepb.SemaphoresGarbageCollectionRecord_NamespaceId{
			NamespaceId: req.Payload.NamespaceId,
		},
	})
	if err != nil {
		return nil, err
	}

	err = txn.Commit()
	if err != nil {
		return nil, err
	}

	return &coreapis.SemaphoresDeleteNamespaceResponse{
		Payload: &corepb.SemaphoresDeleteNamespaceResponse{},
	}, nil
}

// CreateSemaphoreLease creates a new semaphore lease with ExpiresAt = Now + TtlSeconds.
// Returns ResourceExhausted when the namespace has reached MaxNumberOfSemaphoreLeases.
func (c *Core) CreateSemaphoreLease(req *coreapis.CreateSemaphoreLeaseRequest) (*coreapis.CreateSemaphoreLeaseResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	// Get counters for that namespace
	counters, err := c.counters.Get(txn, req.Payload.LeaseId.AccountId, req.Payload.LeaseId.NamespaceId)
	if err != nil {
		return nil, err
	}

	// Checking max number of semaphore leases
	if counters.NumberOfLeases >= req.Payload.MaxNumberOfSemaphoreLeases {
		return &coreapis.CreateSemaphoreLeaseResponse{
			ApplicationError: monsterax.NewErrorWithContext(
				monsterax.ResourceExhausted,
				"max number of semaphore leases per namespace reached",
				map[string]string{
					"limit": fmt.Sprintf("%d", req.Payload.MaxNumberOfSemaphoreLeases),
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
	counters.NumberOfLeases += 1
	err = c.counters.Set(txn, req.Payload.LeaseId.AccountId, req.Payload.LeaseId.NamespaceId, counters)
	if err != nil {
		return nil, err
	}

	err = txn.Commit()
	if err != nil {
		return nil, err
	}

	return &coreapis.CreateSemaphoreLeaseResponse{
		Payload: &corepb.CreateSemaphoreLeaseResponse{
			Lease: lease,
		},
	}, nil
}

// GetSemaphoreLease fetches a lease by id. Returns NotFound when the lease does not exist or has
// already expired by `now`. Read-only; an expired-but-not-yet-revoked lease is left in the store
// for GC to clean up.
func (c *Core) GetSemaphoreLease(req *coreapis.GetSemaphoreLeaseRequest) (*coreapis.GetSemaphoreLeaseResponse, error) {
	txn := c.badgerStore.View()
	defer txn.Discard()

	lease, err := c.leases.Get(txn, req.Payload.LeaseId)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return &coreapis.GetSemaphoreLeaseResponse{
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
		return &coreapis.GetSemaphoreLeaseResponse{
			ApplicationError: monsterax.NewErrorWithContext(
				monsterax.NotFound,
				"lease not found",
				map[string]string{
					"lease_id": ids.EncodeLeaseId(req.Payload.LeaseId),
				},
			),
		}, nil
	}

	return &coreapis.GetSemaphoreLeaseResponse{
		Payload: &corepb.GetSemaphoreLeaseResponse{
			Lease: lease,
		},
	}, nil
}

// ListSemaphoreLeases returns a page of leases in a namespace, with leases that have expired by
// `now` filtered out of the result. Runs on a read-only transaction; expired leases remain in the
// store and are cleaned up by GC.
func (c *Core) ListSemaphoreLeases(req *coreapis.ListSemaphoreLeasesRequest) (*coreapis.ListSemaphoreLeasesResponse, error) {
	txn := c.badgerStore.View()
	defer txn.Discard()

	result, err := c.leases.List(txn, req.Payload.NamespaceId, req.Payload.PaginationToken, pagination.GetLimitWithDefaults(int(req.Payload.Limit)))
	if err != nil {
		return nil, err
	}

	// Filter out expired leases
	activeLeases := lo.Filter(result.Leases, func(lease *corepb.Lease, _ int) bool {
		return lease.ExpiresAt > req.Payload.Now
	})

	return &coreapis.ListSemaphoreLeasesResponse{
		Payload: &corepb.ListSemaphoreLeasesResponse{
			Leases:                  activeLeases,
			NextPaginationToken:     result.NextPaginationToken,
			PreviousPaginationToken: result.PreviousPaginationToken,
		},
	}, nil
}

// RefreshSemaphoreLease extends a lease's ExpiresAt to Now + TtlSeconds.
// If the lease has already expired by `now` it is revoked instead (its semaphore holders are
// released and the lease is deleted), and a NotFound application error is returned.
// Returns NotFound when the lease does not exist.
func (c *Core) RefreshSemaphoreLease(req *coreapis.RefreshSemaphoreLeaseRequest) (*coreapis.RefreshSemaphoreLeaseResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	lease, err := c.leases.Get(txn, req.Payload.LeaseId)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return &coreapis.RefreshSemaphoreLeaseResponse{
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

	// Check if the lease is expired
	if lease.ExpiresAt <= req.Payload.Now {
		// Lease is expired, revoke it by releasing all semaphores and cleaning up
		err = c.revokeLeaseInTransaction(txn, lease, req.Payload.Now)
		if err != nil {
			return nil, err
		}

		err = txn.Commit()
		if err != nil {
			return nil, err
		}

		// Return not found error since the lease is now revoked
		return &coreapis.RefreshSemaphoreLeaseResponse{
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

	// Propagate the new expiration to every SemaphoreHolder owned by this lease so that
	// expiration-driven cleanup paths (per-semaphore holder pruning and the global GC index)
	// see the refreshed time, not the stale one captured at acquisition.
	err = c.refreshLeaseHolders(txn, lease)
	if err != nil {
		return nil, err
	}

	err = txn.Commit()
	if err != nil {
		return nil, err
	}

	return &coreapis.RefreshSemaphoreLeaseResponse{
		Payload: &corepb.RefreshSemaphoreLeaseResponse{
			Lease: lease,
		},
	}, nil
}

// RevokeSemaphoreLease releases every semaphore held by the lease, deletes the lease, and
// decrements the namespace lease counter. Returns NotFound when the lease does not exist.
func (c *Core) RevokeSemaphoreLease(req *coreapis.RevokeSemaphoreLeaseRequest) (*coreapis.RevokeSemaphoreLeaseResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	// Check if the lease exists
	lease, err := c.leases.Get(txn, req.Payload.LeaseId)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return &coreapis.RevokeSemaphoreLeaseResponse{
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

	// Revoke the lease
	err = c.revokeLeaseInTransaction(txn, lease, req.Payload.Now)
	if err != nil {
		return nil, err
	}

	err = txn.Commit()
	if err != nil {
		return nil, err
	}

	return &coreapis.RevokeSemaphoreLeaseResponse{
		Payload: &corepb.RevokeSemaphoreLeaseResponse{},
	}, nil
}

// ListSemaphoreLeasesByProcessId returns a page of leases belonging to a given process_id in a
// namespace, with leases that have expired by `now` filtered out. Read-only; expired leases remain
// in the store until GC removes them.
func (c *Core) ListSemaphoreLeasesByProcessId(req *coreapis.ListSemaphoreLeasesByProcessIdRequest) (*coreapis.ListSemaphoreLeasesByProcessIdResponse, error) {
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

	return &coreapis.ListSemaphoreLeasesByProcessIdResponse{
		Payload: &corepb.ListSemaphoreLeasesByProcessIdResponse{
			Leases:                  activeLeases,
			NextPaginationToken:     result.NextPaginationToken,
			PreviousPaginationToken: result.PreviousPaginationToken,
		},
	}, nil
}

// ListSemaphoresByLeaseId returns a page of semaphores currently held by a given lease.
// Because it runs on a read-only transaction it cannot remove expired holders; instead it returns
// a copy of each semaphore with `ActiveHolds`, `ActiveHoldersCount`, and `EarliestHolderExpiresAt`
// adjusted as if holders that expired by `now` had been removed.
func (c *Core) ListSemaphoresByLeaseId(req *coreapis.ListSemaphoresByLeaseIdRequest) (*coreapis.ListSemaphoresByLeaseIdResponse, error) {
	txn := c.badgerStore.View()
	defer txn.Discard()

	result, err := c.semaphores.ListByLeaseId(txn, req.Payload.LeaseId, req.Payload.PaginationToken, pagination.GetLimitWithDefaults(int(req.Payload.Limit)))
	if err != nil {
		return nil, err
	}

	semaphores := make([]*corepb.Semaphore, 0, len(result.semaphores))
	for _, semaphore := range result.semaphores {
		updatedSemaphore, _, err := c.computeExpiredSemaphoreHolders(txn, semaphore, req.Payload.Now)
		if err != nil {
			return nil, err
		}
		semaphores = append(semaphores, updatedSemaphore)
	}

	return &coreapis.ListSemaphoresByLeaseIdResponse{
		Payload: &corepb.ListSemaphoresByLeaseIdResponse{
			Semaphores:              semaphores,
			NextPaginationToken:     result.nextPaginationToken,
			PreviousPaginationToken: result.previousPaginationToken,
		},
	}, nil
}

// gcDeleteSemaphoreHolders deletes up to one page of holders for the given semaphore, decrementing
// the visit budget for each one. Returns true if the semaphore has no remaining holders (every page
// drained); false if the page-size limit or the visit budget cut the run short. The caller owns
// the txn lifecycle.
func (c *Core) gcDeleteSemaphoreHolders(txn *store.Txn, semaphoreId *corepb.SemaphoreId, pageSize int, visited *int64, maxVisited int64) (bool, error) {
	result, err := c.holders.List(txn, semaphoreId.AccountId, semaphoreId.NamespaceId, semaphoreId.SemaphoreId, nil, pageSize)
	if err != nil {
		return false, err
	}

	for _, holder := range result.holders {
		// Remove from lease ID index so ListSemaphoresByLeaseId does not return a stale pointer.
		err = c.semaphores.RemoveLeaseFromIndex(txn, semaphoreId, holder.Id.LeaseId)
		if err != nil {
			return false, err
		}
		// Delete the holder record (also removes its expiration-index entry).
		err = c.holders.Delete(txn, holder)
		if err != nil {
			return false, err
		}
		*visited++
		if *visited >= maxVisited {
			return false, nil
		}
	}

	return result.nextPaginationToken == nil, nil
}

// refreshLeaseHolders propagates lease.ExpiresAt to every SemaphoreHolder owned by the lease
// and updates the per-semaphore EarliestHolderExpiresAt (and the global expiration index)
// when the change affects which holder expires first. The caller owns the txn lifecycle.
func (c *Core) refreshLeaseHolders(txn *store.Txn, lease *corepb.Lease) error {
	var paginationToken *corepb.PaginationToken
	for {
		semaphoresResult, err := c.semaphores.ListByLeaseId(txn, lease.Id, paginationToken, 1000)
		if err != nil {
			return err
		}

		for _, semaphore := range semaphoresResult.semaphores {
			holderId := &corepb.SemaphoreHolderId{
				AccountId:   lease.Id.AccountId,
				NamespaceId: lease.Id.NamespaceId,
				SemaphoreId: semaphore.Id.SemaphoreId,
				LeaseId:     lease.Id.LeaseId,
			}

			holder, err := c.holders.Get(txn, holderId)
			if err != nil {
				if errors.Is(err, store.ErrNotFound) {
					// Stale lease index entry — clean it up and continue.
					err = c.semaphores.RemoveLeaseFromIndex(txn, semaphore.Id, lease.Id.LeaseId)
					if err != nil {
						return err
					}
					continue
				}
				return err
			}

			if holder.ExpiresAt == lease.ExpiresAt {
				continue
			}

			holder.ExpiresAt = lease.ExpiresAt
			err = c.holders.Update(txn, holder)
			if err != nil {
				return err
			}

			// Recompute the semaphore's earliest expiration; ListByExpiration is ASC so the
			// first hit is the smallest.
			oldEarliest := semaphore.EarliestHolderExpiresAt
			semaphore.EarliestHolderExpiresAt = 0
			err = c.holders.ListByExpiration(txn, semaphore.Id, 0, math.MaxInt64, func(h *corepb.SemaphoreHolder) (bool, error) {
				semaphore.EarliestHolderExpiresAt = h.ExpiresAt
				return false, nil
			})
			if err != nil {
				return err
			}

			if oldEarliest == semaphore.EarliestHolderExpiresAt {
				continue
			}

			if oldEarliest != 0 {
				err = c.expirationRecords.Delete(txn, oldEarliest, semaphore.Id)
				if err != nil && !errors.Is(err, store.ErrNotFound) {
					return err
				}
			}
			if semaphore.EarliestHolderExpiresAt != 0 {
				err = c.expirationRecords.Add(txn, semaphore.EarliestHolderExpiresAt, semaphore.Id)
				if err != nil {
					return err
				}
			}

			err = c.semaphores.Update(txn, semaphore)
			if err != nil {
				return err
			}
		}

		if semaphoresResult.nextPaginationToken == nil {
			break
		}
		paginationToken = semaphoresResult.nextPaginationToken
	}
	return nil
}

// revokeLeaseInTransaction revokes a lease within an existing transaction by releasing all
// semaphores held by the lease and cleaning up the lease and counters. The work is unbounded —
// every holder owned by the lease is released before the function returns. Callers that need
// a visit budget (e.g. GC) should use revokeLeaseInTransactionBounded directly.
func (c *Core) revokeLeaseInTransaction(txn *store.Txn, lease *corepb.Lease, now int64) error {
	visited := int64(0)
	_, err := c.revokeLeaseInTransactionBounded(txn, lease, now, &visited, math.MaxInt64)
	return err
}

// revokeLeaseInTransactionBounded releases the lease's semaphore holders, counting each holder
// against *visited. If *visited reaches maxVisited before every holder is drained, the function
// returns (false, nil) with the lease row still present — a later GC pass will resume the work.
// Once every holder has been released the lease row is deleted and the namespace's lease counter
// is decremented (a missing counter row is tolerated, since the namespace may already have been
// GC'd while leases were still in flight), and the function returns (true, nil).
func (c *Core) revokeLeaseInTransactionBounded(txn *store.Txn, lease *corepb.Lease, now int64, visited *int64, maxVisited int64) (bool, error) {
	var paginationToken *corepb.PaginationToken
	for {
		semaphoresResult, err := c.semaphores.ListByLeaseId(txn, lease.Id, paginationToken, 1000)
		if err != nil {
			return false, err
		}

		for _, semaphore := range semaphoresResult.semaphores {
			holderId := &corepb.SemaphoreHolderId{
				AccountId:   lease.Id.AccountId,
				NamespaceId: lease.Id.NamespaceId,
				SemaphoreId: semaphore.Id.SemaphoreId,
				LeaseId:     lease.Id.LeaseId,
			}

			holder, err := c.holders.Get(txn, holderId)
			if err != nil {
				if errors.Is(err, store.ErrNotFound) {
					// Stale lease index entry — clean it up and continue.
					err = c.semaphores.RemoveLeaseFromIndex(txn, semaphore.Id, lease.Id.LeaseId)
					if err != nil {
						return false, err
					}
					continue
				}
				return false, err
			}

			err = c.holders.Delete(txn, holder)
			if err != nil {
				return false, err
			}

			err = c.semaphores.RemoveLeaseFromIndex(txn, semaphore.Id, lease.Id.LeaseId)
			if err != nil {
				return false, err
			}

			semaphore.ActiveHolds -= holder.Weight
			semaphore.ActiveHoldersCount -= 1

			// Capture the current earliest before the recompute so the expirationRecords
			// cleanup below targets the index entry's *actual* key. Using the removed
			// holder's ExpiresAt only happens to converge when the removed holder was the
			// earliest; for any other holder the delete would silently miss and the add
			// would land on a different key from the stored entry.
			oldEarliest := semaphore.EarliestHolderExpiresAt

			// Recalculate earliest expiration time
			semaphore.EarliestHolderExpiresAt = 0
			err = c.holders.ListByExpiration(txn, semaphore.Id, 0, math.MaxInt64, func(h *corepb.SemaphoreHolder) (bool, error) {
				if h.ExpiresAt > now {
					semaphore.EarliestHolderExpiresAt = h.ExpiresAt
					return false, nil
				}
				return true, nil
			})
			if err != nil {
				return false, err
			}

			err = c.semaphores.Update(txn, semaphore)
			if err != nil {
				return false, err
			}

			// Reconcile expirationRecords only when the earliest holder actually moved.
			// Tolerate NotFound on the delete so the cleanup is safe against pre-existing
			// stale entries (e.g. left over by earlier buggy paths).
			if oldEarliest != semaphore.EarliestHolderExpiresAt {
				if oldEarliest != 0 {
					err = c.expirationRecords.Delete(txn, oldEarliest, semaphore.Id)
					if err != nil && !errors.Is(err, store.ErrNotFound) {
						return false, err
					}
				}
				if semaphore.EarliestHolderExpiresAt != 0 {
					err = c.expirationRecords.Add(txn, semaphore.EarliestHolderExpiresAt, semaphore.Id)
					if err != nil {
						return false, err
					}
				}
			}

			*visited++
			if *visited >= maxVisited {
				return false, nil
			}
		}

		if semaphoresResult.nextPaginationToken == nil {
			break
		}
		paginationToken = semaphoresResult.nextPaginationToken
	}

	// Drained: delete the lease itself and decrement the namespace counter. The lease row
	// (plus its two index entries) is real transactional work, so it counts toward the budget.
	err := c.leases.Delete(txn, lease)
	if err != nil {
		return false, err
	}
	*visited++

	counters, err := c.counters.Get(txn, lease.Id.AccountId, lease.Id.NamespaceId)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			// Counters were already removed by namespace-deletion GC; nothing to decrement.
			return true, nil
		}
		return false, err
	}
	counters.NumberOfLeases -= 1
	err = c.counters.Set(txn, lease.Id.AccountId, lease.Id.NamespaceId, counters)
	if err != nil {
		return false, err
	}

	return true, nil
}

// computeExpiredSemaphoreHolders walks holders in expiration order and returns a clone of the
// semaphore with `ActiveHolds`, `ActiveHoldersCount`, and `EarliestHolderExpiresAt` adjusted as if
// holders that expired by `now` had been removed, along with the list of those expired holders.
// This function does not mutate the store, so it can be used from both read-only and update transactions.
func (c *Core) computeExpiredSemaphoreHolders(txn *store.Txn, semaphore *corepb.Semaphore, now int64) (*corepb.Semaphore, []*corepb.SemaphoreHolder, error) {
	updatedSemaphore := proto.Clone(semaphore).(*corepb.Semaphore)
	updatedSemaphore.EarliestHolderExpiresAt = 0

	var expired []*corepb.SemaphoreHolder

	err := c.holders.ListByExpiration(txn, semaphore.Id, 0, math.MaxInt64, func(holder *corepb.SemaphoreHolder) (bool, error) {
		if holder.ExpiresAt > now {
			updatedSemaphore.EarliestHolderExpiresAt = holder.ExpiresAt
			return false, nil
		}

		expired = append(expired, holder)
		updatedSemaphore.ActiveHolds -= holder.Weight
		updatedSemaphore.ActiveHoldersCount -= 1

		return true, nil
	})
	if err != nil {
		return nil, nil, err
	}

	return updatedSemaphore, expired, nil
}

// deleteExpiredSemaphoreHolders ensures that the semaphore is still held at the moment `now`.
// It deletes holders that expire before `now`, calculates `EarliestHolderExpiresAt`, and returns
// an updated copy of the semaphore together with the number of holders that were pruned.
// The count lets GC callers credit each removed holder against the visit budget; non-GC callers
// can ignore it.
func (c *Core) deleteExpiredSemaphoreHolders(txn *store.Txn, semaphore *corepb.Semaphore, now int64) (*corepb.Semaphore, int, error) {
	updatedSemaphore, expired, err := c.computeExpiredSemaphoreHolders(txn, semaphore, now)
	if err != nil {
		return nil, 0, err
	}

	for _, holder := range expired {
		err = c.holders.Delete(txn, holder)
		if err != nil {
			return nil, 0, err
		}

		// Remove from lease ID index
		err = c.semaphores.RemoveLeaseFromIndex(txn, semaphore.Id, holder.Id.LeaseId)
		if err != nil {
			return nil, 0, err
		}
	}

	return updatedSemaphore, len(expired), nil
}
