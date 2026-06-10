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

func (c *Core) Snapshot() monstera.ApplicationCoreSnapshot {
	return monsterax.Snapshot(c.badgerStore, c.ranges())
}

func (c *Core) Restore(reader io.ReadCloser) error {
	return monsterax.Restore(c.badgerStore, c.ranges(), reader)
}

func (c *Core) Close() {

}

func (c *Core) CreateSemaphore(req *coreapis.CreateSemaphoreRequest) (*coreapis.CreateSemaphoreResponse, error) {
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
		Id:          req.Payload.SemaphoreId,
		Name:        req.Payload.Name,
		Description: req.Payload.Description,
		Permits:     req.Payload.Permits,
		CreatedAt:   req.Payload.Now,
		UpdatedAt:   req.Payload.Now,
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

	// Check expired holders
	updatedSemaphore, err := c.deleteExpiredSemaphoreholders(txn, semaphore, req.Payload.Now)
	if err != nil {
		return nil, err
	}

	// If there are currently more holds than the new amount of permits
	if updatedSemaphore.ActiveHolds > req.Payload.Permits {
		return &coreapis.UpdateSemaphoreResponse{
			ApplicationError: monsterax.NewErrorWithContext(
				monsterax.InvalidArgument,
				"there are currently more holds than the new amount of permits",
				map[string]string{},
			),
		}, nil
	}

	updatedSemaphore.Description = req.Payload.Description
	updatedSemaphore.Permits = req.Payload.Permits
	updatedSemaphore.UpdatedAt = req.Payload.Now

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

	// TODO put gc record for semaphore

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

func (c *Core) GetSemaphore(req *coreapis.GetSemaphoreRequest) (*coreapis.GetSemaphoreResponse, error) {
	txn := c.badgerStore.Update()
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

	// Check expired holders
	updatedSemaphore, err := c.deleteExpiredSemaphoreholders(txn, semaphore, req.Payload.Now)
	if err != nil {
		return nil, err
	}

	// Update expiration records if earliest expiration changed
	if semaphore.EarliestHolderExpiresAt != updatedSemaphore.EarliestHolderExpiresAt {
		// Remove semaphore from expirationRecords at old position
		if semaphore.EarliestHolderExpiresAt != 0 {
			err = c.expirationRecords.Delete(txn, semaphore.EarliestHolderExpiresAt, semaphore.Id)
			if err != nil {
				return nil, err
			}
		}

		// Add semaphore to expirationRecords at new position
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

	return &coreapis.GetSemaphoreResponse{
		Payload: &corepb.GetSemaphoreResponse{
			Semaphore: updatedSemaphore,
		},
	}, nil
}

func (c *Core) GetSemaphoreByName(req *coreapis.GetSemaphoreByNameRequest) (*coreapis.GetSemaphoreByNameResponse, error) {
	txn := c.badgerStore.Update()
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

	// Check expired holders
	updatedSemaphore, err := c.deleteExpiredSemaphoreholders(txn, semaphore, req.Payload.Now)
	if err != nil {
		return nil, err
	}

	// Update expiration records if earliest expiration changed
	if semaphore.EarliestHolderExpiresAt != updatedSemaphore.EarliestHolderExpiresAt {
		// Remove semaphore from expirationRecords at old position
		if semaphore.EarliestHolderExpiresAt != 0 {
			err = c.expirationRecords.Delete(txn, semaphore.EarliestHolderExpiresAt, semaphore.Id)
			if err != nil {
				return nil, err
			}
		}

		// Add semaphore to expirationRecords at new position
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

	return &coreapis.GetSemaphoreByNameResponse{
		Payload: &corepb.GetSemaphoreByNameResponse{
			Semaphore: updatedSemaphore,
		},
	}, nil
}

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

	return &coreapis.ListSemaphoreHoldersResponse{
		Payload: &corepb.ListSemaphoreHoldersResponse{
			Holders:                 result.holders,
			NextPaginationToken:     result.nextPaginationToken,
			PreviousPaginationToken: result.previousPaginationToken,
		},
	}, nil
}

// ListSemaphores may return stale semaphores holder counters as it does not delete expired holders.
func (c *Core) ListSemaphores(req *coreapis.ListSemaphoresRequest) (*coreapis.ListSemaphoresResponse, error) {
	txn := c.badgerStore.View()
	defer txn.Discard()

	result, err := c.semaphores.List(txn, req.Payload.NamespaceId.AccountId, req.Payload.NamespaceId.NamespaceId, req.Payload.PaginationToken, pagination.GetLimitWithDefaults(int(req.Payload.Limit)))
	if err != nil {
		return nil, err
	}

	//TODO: does not delete expired holders

	return &coreapis.ListSemaphoresResponse{
		Payload: &corepb.ListSemaphoresResponse{
			Semaphores:              result.semaphores,
			NextPaginationToken:     result.nextPaginationToken,
			PreviousPaginationToken: result.previousPaginationToken,
		},
	}, nil
}

func (c *Core) AcquireSemaphore(req *coreapis.AcquireSemaphoreRequest) (*coreapis.AcquireSemaphoreResponse, error) {
	if req.Payload.Weight <= 0 {
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
	lease, err := c.leases.Get(txn, &corepb.LeaseId{
		AccountId:   req.Payload.NamespaceId.AccountId,
		NamespaceId: req.Payload.NamespaceId.NamespaceId,
		LeaseId:     req.Payload.LeaseId,
	})
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

	// Check expired holders
	updatedSemaphore, err := c.deleteExpiredSemaphoreholders(txn, semaphore, req.Payload.Now)
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
			if updatedSemaphore.Permits >= updatedSemaphore.ActiveHolds+uint64(req.Payload.Weight) {
				// Add a new lock holder
				newHolder := &corepb.SemaphoreHolder{
					Id:        holderId,
					ExpiresAt: lease.ExpiresAt,
					LockedAt:  req.Payload.Now,
					Weight:    req.Payload.Weight,
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
		// TODO check if weight changed?

		// Update expiration time (extend lock)
		existingHolder.ExpiresAt = lease.ExpiresAt
		existingHolder.LockedAt = req.Payload.Now

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

	return &coreapis.AcquireSemaphoreResponse{
		Payload: &corepb.AcquireSemaphoreResponse{
			Semaphore: updatedSemaphore,
			Success:   success,
		},
	}, nil
}

func (c *Core) ReleaseSemaphore(req *coreapis.ReleaseSemaphoreRequest) (*coreapis.ReleaseSemaphoreResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	// Validate and get the lease
	lease, err := c.leases.Get(txn, &corepb.LeaseId{
		AccountId:   req.Payload.NamespaceId.AccountId,
		NamespaceId: req.Payload.NamespaceId.NamespaceId,
		LeaseId:     req.Payload.LeaseId,
	})
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

	err = c.holders.Delete(txn, holderId)
	if err != nil {
		return nil, err
	}

	// Remove from lease ID index
	err = c.semaphores.RemoveLeaseFromIndex(txn, semaphore.Id, lease.Id.LeaseId)
	if err != nil {
		return nil, err
	}

	// Check expired holders
	updatedSemaphore, err := c.deleteExpiredSemaphoreholders(txn, semaphore, req.Payload.Now)
	if err != nil {
		return nil, err
	}

	updatedSemaphore.ActiveHolds -= existingHolder.Weight
	updatedSemaphore.ActiveHoldersCount -= 1

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

func (c *Core) RunSemaphoresGarbageCollection(req *coreapis.RunSemaphoresGarbageCollectionRequest) (*coreapis.RunSemaphoresGarbageCollectionResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	visitedSemaphores := int64(0)

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

			// Delete those semaphores
			for _, semaphore := range result.semaphores {
				// TODO remove holders

				visitedSemaphores++

				// Remove a semaphore from expirationGlobalIndex
				if semaphore.EarliestHolderExpiresAt != 0 {
					err = c.expirationRecords.Delete(txn, semaphore.EarliestHolderExpiresAt, semaphore.Id)
					if err != nil {
						return nil, err
					}
				}

				// Remove from the main table
				err := c.semaphores.Delete(txn, semaphore.Id)
				if err != nil {
					return nil, err
				}

				if visitedSemaphores >= req.Payload.MaxVisitedSemaphores {
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
		case *corepb.SemaphoresGarbageCollectionRecord_SemaphoreId:
		}
	}

	if visitedSemaphores < req.Payload.MaxVisitedSemaphores {
		// Update semaphores with expired holders
		err = c.expirationRecords.List(txn, 0, req.Payload.Now, func(record *corepb.SemaphoresExpirationRecord) (bool, error) {
			visitedSemaphores++

			// Get the semaphore
			semaphore, err := c.semaphores.Get(txn, record.SemaphoreId)
			if err != nil {
				return false, err
			}

			// Remove a semaphore from expirationRecords at old position
			// Always delete first to handle stale/duplicate expiration records
			if semaphore.EarliestHolderExpiresAt != 0 {
				err = c.expirationRecords.Delete(txn, semaphore.EarliestHolderExpiresAt, semaphore.Id)
				if err != nil {
					return false, err
				}
			}

			updatedSemaphore, err := c.deleteExpiredSemaphoreholders(txn, semaphore, req.Payload.Now)
			if err != nil {
				return false, err
			}

			// If semaphore still has holders it will have non zero expiration time
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
			return visitedSemaphores < req.Payload.MaxVisitedSemaphores, nil
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

	return &coreapis.GetSemaphoreLeaseResponse{
		Payload: &corepb.GetSemaphoreLeaseResponse{
			Lease: lease,
		},
	}, nil
}

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

func (c *Core) RevokeSemaphoreLease(req *coreapis.RevokeSemaphoreLeaseRequest) (*coreapis.RevokeSemaphoreLeaseResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	// Check if the lease exists
	lease, err := c.leases.Get(txn, req.Payload.LeaseId)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			// Lease doesn't exist, nothing to do
			return &coreapis.RevokeSemaphoreLeaseResponse{
				Payload: &corepb.RevokeSemaphoreLeaseResponse{},
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

func (c *Core) ListSemaphoresByLeaseId(req *coreapis.ListSemaphoresByLeaseIdRequest) (*coreapis.ListSemaphoresByLeaseIdResponse, error) {
	txn := c.badgerStore.View()
	defer txn.Discard()

	result, err := c.semaphores.ListByLeaseId(txn, req.Payload.LeaseId, req.Payload.PaginationToken, pagination.GetLimitWithDefaults(int(req.Payload.Limit)))
	if err != nil {
		return nil, err
	}

	//TODO: filter expired holders

	return &coreapis.ListSemaphoresByLeaseIdResponse{
		Payload: &corepb.ListSemaphoresByLeaseIdResponse{
			Semaphores:              result.semaphores,
			NextPaginationToken:     result.nextPaginationToken,
			PreviousPaginationToken: result.previousPaginationToken,
		},
	}, nil
}

// revokeLeaseInTransaction revokes a lease within an existing transaction by releasing all
// semaphores held by the lease and cleaning up the lease and counters.
func (c *Core) revokeLeaseInTransaction(txn *store.Txn, lease *corepb.Lease, now int64) error {
	// Get counters for this namespace
	counters, err := c.counters.Get(txn, lease.Id.AccountId, lease.Id.NamespaceId)
	if err != nil {
		return err
	}

	// Release all semaphores held by this lease, paginating through all pages
	var paginationToken *corepb.PaginationToken
	for {
		// List semaphores held by this lease
		semaphoresResult, err := c.semaphores.ListByLeaseId(txn, lease.Id, paginationToken, 1000)
		if err != nil {
			return err
		}

		// If no semaphores found, we're done
		if len(semaphoresResult.semaphores) == 0 && semaphoresResult.nextPaginationToken == nil {
			break
		}

		// Release semaphores on this page
		for _, semaphore := range semaphoresResult.semaphores {
			// Get the holder for this lease
			holderId := &corepb.SemaphoreHolderId{
				AccountId:   lease.Id.AccountId,
				NamespaceId: lease.Id.NamespaceId,
				SemaphoreId: semaphore.Id.SemaphoreId,
				LeaseId:     lease.Id.LeaseId,
			}

			holder, err := c.holders.Get(txn, holderId)
			if err != nil {
				if errors.Is(err, store.ErrNotFound) {
					// Holder doesn't exist, the lease index entry is stale
					// Just remove from index and continue
					err = c.semaphores.RemoveLeaseFromIndex(txn, semaphore.Id, lease.Id.LeaseId)
					if err != nil {
						return err
					}
					continue
				}
				return err
			}

			// Delete the holder
			err = c.holders.Delete(txn, holderId)
			if err != nil {
				return err
			}

			// Remove from lease ID index
			err = c.semaphores.RemoveLeaseFromIndex(txn, semaphore.Id, lease.Id.LeaseId)
			if err != nil {
				return err
			}

			// Update semaphore
			semaphore.ActiveHolds -= holder.Weight
			semaphore.ActiveHoldersCount -= 1

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
				return err
			}

			// Update semaphore in table
			err = c.semaphores.Update(txn, semaphore)
			if err != nil {
				return err
			}

			// Update expiration records
			if semaphore.EarliestHolderExpiresAt != 0 {
				// Remove old expiration record if it exists
				err = c.expirationRecords.Delete(txn, holder.ExpiresAt, semaphore.Id)
				if err != nil && !errors.Is(err, store.ErrNotFound) {
					return err
				}

				// Add new expiration record
				err = c.expirationRecords.Add(txn, semaphore.EarliestHolderExpiresAt, semaphore.Id)
				if err != nil {
					return err
				}
			} else {
				// No more holders, remove from expiration records
				err = c.expirationRecords.Delete(txn, holder.ExpiresAt, semaphore.Id)
				if err != nil && !errors.Is(err, store.ErrNotFound) {
					return err
				}
			}
		}

		// Check if there are more pages
		if semaphoresResult.nextPaginationToken == nil {
			break
		}
		paginationToken = semaphoresResult.nextPaginationToken
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

// deleteExpiredSemaphoreholders ensures that the semaphore is still held at the moment `now`.
// It deletes holders that expire before `now`, calculates `EarliestHolderExpiresAt`, and returns an updated copy
// of the semaphore.
func (c *Core) deleteExpiredSemaphoreholders(txn *store.Txn, semaphore *corepb.Semaphore, now int64) (*corepb.Semaphore, error) {
	updatedSemaphore := proto.Clone(semaphore).(*corepb.Semaphore)

	updatedSemaphore.EarliestHolderExpiresAt = 0

	err := c.holders.ListByExpiration(txn, semaphore.Id, 0, math.MaxInt64, func(holder *corepb.SemaphoreHolder) (bool, error) {
		// TODO limit the maximum number of holders to prevent too large transactions

		if holder.ExpiresAt > now {
			updatedSemaphore.EarliestHolderExpiresAt = holder.ExpiresAt
			return false, nil
		}

		err := c.holders.Delete(txn, holder.Id)
		if err != nil {
			return false, err
		}

		// Remove from lease ID index
		err = c.semaphores.RemoveLeaseFromIndex(txn, semaphore.Id, holder.Id.LeaseId)
		if err != nil {
			return false, err
		}

		updatedSemaphore.ActiveHolds -= holder.Weight
		updatedSemaphore.ActiveHoldersCount -= 1

		return true, nil
	})
	if err != nil {
		return nil, err
	}

	return updatedSemaphore, nil
}
