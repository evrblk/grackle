package semaphores

import (
	"errors"
	"fmt"
	"io"
	"math"

	"github.com/evrblk/monstera"
	"github.com/evrblk/monstera/store"
	monsterax "github.com/evrblk/monstera/x"
	"google.golang.org/protobuf/proto"

	"github.com/evrblk/grackle/pkg/corepb"
	"github.com/evrblk/grackle/pkg/ids"
	"github.com/evrblk/grackle/pkg/monsteragen"
	"github.com/evrblk/grackle/pkg/pagination"
)

type Core struct {
	badgerStore *store.BadgerStore

	semaphores        *semaphoresTable
	holders           *holdersTable
	counters          *countersTable
	gcRecords         *gcRecordsTable
	expirationRecords *expirationRecordsTable
}

var _ monsteragen.GrackleSemaphoresCoreApi = &Core{}

func NewCore(badgerStore *store.BadgerStore, shardGlobalIndexPrefix []byte, shardLowerBound []byte, shardUpperBound []byte) *Core {
	return &Core{
		badgerStore: badgerStore,

		semaphores:        newSemaphoresTable(shardLowerBound, shardUpperBound),
		holders:           newHoldersTable(shardLowerBound, shardUpperBound),
		counters:          newCountersTable(shardLowerBound, shardUpperBound),
		gcRecords:         newGCRecordsTable(shardGlobalIndexPrefix),
		expirationRecords: newExpirationRecordsTable(shardGlobalIndexPrefix),
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

func (c *Core) CreateSemaphore(request *corepb.CreateSemaphoreRequest) (*corepb.CreateSemaphoreResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	// Get counters for that namespace
	counters, err := c.counters.Get(txn, request.SemaphoreId.AccountId, request.SemaphoreId.NamespaceId)
	panicIfNotNil(err)

	// Checking max number of semaphores
	if counters.NumberOfSemaphores >= request.MaxNumberOfSemaphoresPerNamespace {
		return nil, monsterax.NewErrorWithContext(
			monsterax.ResourceExhausted,
			"max number of semaphores per namespace reached",
			map[string]string{"limit": fmt.Sprintf("%d", request.MaxNumberOfSemaphoresPerNamespace)})
	}

	semaphore := &corepb.Semaphore{
		Id:          request.SemaphoreId,
		Name:        request.Name,
		Description: request.Description,
		Permits:     request.Permits,
		CreatedAt:   request.Now,
		UpdatedAt:   request.Now,
	}

	err = c.semaphores.Create(txn, semaphore)
	if err != nil {
		merr := &monsterax.Error{}
		if errors.As(err, &merr) {
			return nil, merr
		}

		panic(err)
	}

	// Update counters
	counters.NumberOfSemaphores += 1
	err = c.counters.Set(txn, request.SemaphoreId.AccountId, request.SemaphoreId.NamespaceId, counters)
	panicIfNotNil(err)

	err = txn.Commit()
	panicIfNotNil(err)

	return &corepb.CreateSemaphoreResponse{
		Semaphore: semaphore,
	}, nil
}

func (c *Core) UpdateSemaphore(request *corepb.UpdateSemaphoreRequest) (*corepb.UpdateSemaphoreResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	semaphore, err := c.semaphores.GetByName(txn, request.NamespaceId.AccountId, request.NamespaceId.NamespaceId, request.SemaphoreName)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, monsterax.NewErrorWithContext(
				monsterax.NotFound,
				"semaphore not found",
				map[string]string{
					"semaphore_name": request.SemaphoreName,
				})
		} else {
			panic(err)
		}
	}

	// Check expired holders
	updatedSemaphore := c.deleteExpiredSemaphoreholders(txn, semaphore, request.Now)

	// If there are currently more holds than the new amount of permits
	if updatedSemaphore.ActiveHolds > request.Permits {
		return nil, monsterax.NewErrorWithContext(
			monsterax.InvalidArgument,
			"there are currently more holds than the new amount of permits",
			map[string]string{})
	}

	updatedSemaphore.Description = request.Description
	updatedSemaphore.Permits = request.Permits
	updatedSemaphore.UpdatedAt = request.Now

	err = c.semaphores.Update(txn, updatedSemaphore)
	panicIfNotNil(err)

	err = txn.Commit()
	panicIfNotNil(err)

	return &corepb.UpdateSemaphoreResponse{
		Semaphore: updatedSemaphore,
	}, nil
}

func (c *Core) DeleteSemaphore(request *corepb.DeleteSemaphoreRequest) (*corepb.DeleteSemaphoreResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	semaphore, err := c.semaphores.GetByName(txn, request.NamespaceId.AccountId, request.NamespaceId.NamespaceId, request.SemaphoreName)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			// No semaphore exists, do nothing
			return &corepb.DeleteSemaphoreResponse{}, nil
		} else {
			panic(err)
		}
	}

	// Get counters for this namespace
	counters, err := c.counters.Get(txn, request.NamespaceId.AccountId, request.NamespaceId.NamespaceId)
	panicIfNotNil(err)

	err = c.semaphores.Delete(txn, semaphore.Id)
	panicIfNotNil(err)

	// TODO put gc record for semaphore

	// Update counters
	counters.NumberOfSemaphores -= 1
	err = c.counters.Set(txn, request.NamespaceId.AccountId, request.NamespaceId.NamespaceId, counters)
	panicIfNotNil(err)

	err = txn.Commit()
	panicIfNotNil(err)

	return &corepb.DeleteSemaphoreResponse{}, nil
}

func (c *Core) GetSemaphore(request *corepb.GetSemaphoreRequest) (*corepb.GetSemaphoreResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	semaphore, err := c.semaphores.Get(txn, request.SemaphoreId)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, monsterax.NewErrorWithContext(
				monsterax.NotFound,
				"semaphore not found",
				map[string]string{
					"semaphore_id": ids.EncodeSemaphoreId(request.SemaphoreId),
				})
		} else {
			panic(err)
		}
	}

	// Check expired holders
	updatedSemaphore := c.deleteExpiredSemaphoreholders(txn, semaphore, request.Now)

	// TODO update expiration index

	err = c.semaphores.Update(txn, updatedSemaphore)
	panicIfNotNil(err)

	err = txn.Commit()
	panicIfNotNil(err)

	return &corepb.GetSemaphoreResponse{
		Semaphore: updatedSemaphore,
	}, nil
}

func (c *Core) GetSemaphoreByName(request *corepb.GetSemaphoreByNameRequest) (*corepb.GetSemaphoreByNameResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	semaphore, err := c.semaphores.GetByName(txn, request.NamespaceId.AccountId, request.NamespaceId.NamespaceId, request.SemaphoreName)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, monsterax.NewErrorWithContext(
				monsterax.NotFound,
				"semaphore not found",
				map[string]string{
					"semaphore_name": request.SemaphoreName,
				})
		} else {
			panic(err)
		}
	}

	// Check expired holders
	updatedSemaphore := c.deleteExpiredSemaphoreholders(txn, semaphore, request.Now)

	// TODO update expiration index

	err = c.semaphores.Update(txn, updatedSemaphore)
	panicIfNotNil(err)

	err = txn.Commit()
	panicIfNotNil(err)

	return &corepb.GetSemaphoreByNameResponse{
		Semaphore: updatedSemaphore,
	}, nil
}

func (c *Core) ListSemaphoreHolders(request *corepb.ListSemaphoreHoldersRequest) (*corepb.ListSemaphoreHoldersResponse, error) {
	txn := c.badgerStore.View()
	defer txn.Discard()

	semaphore, err := c.semaphores.GetByName(txn, request.NamespaceId.AccountId, request.NamespaceId.NamespaceId, request.SemaphoreName)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, monsterax.NewErrorWithContext(
				monsterax.NotFound,
				"semaphore not found",
				map[string]string{
					"semaphore_name": request.SemaphoreName,
				})
		} else {
			panic(err)
		}
	}

	result, err := c.holders.List(txn, request.NamespaceId.AccountId, request.NamespaceId.NamespaceId, semaphore.Id.SemaphoreId, request.PaginationToken, pagination.GetLimitWithDefaults(int(request.Limit)))
	panicIfNotNil(err)

	return &corepb.ListSemaphoreHoldersResponse{
		Holders:                 result.holders,
		NextPaginationToken:     result.nextPaginationToken,
		PreviousPaginationToken: result.previousPaginationToken,
	}, nil
}

// ListSemaphores may return stale semaphores holder counters as it does not delete expired holders.
func (c *Core) ListSemaphores(request *corepb.ListSemaphoresRequest) (*corepb.ListSemaphoresResponse, error) {
	txn := c.badgerStore.View()
	defer txn.Discard()

	result, err := c.semaphores.List(txn, request.NamespaceId.AccountId, request.NamespaceId.NamespaceId, request.PaginationToken, pagination.GetLimitWithDefaults(int(request.Limit)))
	panicIfNotNil(err)

	return &corepb.ListSemaphoresResponse{
		Semaphores:              result.semaphores,
		NextPaginationToken:     result.nextPaginationToken,
		PreviousPaginationToken: result.previousPaginationToken,
	}, nil
}

func (c *Core) AcquireSemaphore(request *corepb.AcquireSemaphoreRequest) (*corepb.AcquireSemaphoreResponse, error) {
	if request.Weight <= 0 {
		return nil, monsterax.NewErrorWithContext(
			monsterax.InvalidArgument,
			"weight must be greater than 0",
			map[string]string{
				"weight": fmt.Sprintf("%d", request.Weight),
			})
	}

	txn := c.badgerStore.Update()
	defer txn.Discard()

	semaphore, err := c.semaphores.GetByName(txn, request.NamespaceId.AccountId, request.NamespaceId.NamespaceId, request.SemaphoreName)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, monsterax.NewErrorWithContext(
				monsterax.NotFound,
				"semaphore not found",
				map[string]string{
					"semaphore_name": request.SemaphoreName,
				})
		} else {
			panic(err)
		}
	}

	// Check expired holders
	updatedSemaphore := c.deleteExpiredSemaphoreholders(txn, semaphore, request.Now)

	success := false

	// Check if the same process_id already holds the semaphore here.
	holderId := &corepb.SemaphoreHolderId{
		AccountId:   request.NamespaceId.AccountId,
		NamespaceId: request.NamespaceId.NamespaceId,
		SemaphoreId: semaphore.Id.SemaphoreId,
		ProcessId:   request.ProcessId,
	}
	existingHolder, err := c.holders.Get(txn, holderId)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			// Check if there are enough permits
			if updatedSemaphore.Permits >= updatedSemaphore.ActiveHolds+uint64(request.Weight) {
				// Add a new lock holder
				newHolder := &corepb.SemaphoreHolder{
					Id:        holderId,
					ExpiresAt: request.ExpiresAt,
					LockedAt:  request.Now,
					Weight:    request.Weight,
				}

				err = c.holders.Create(txn, newHolder)
				panicIfNotNil(err)

				updatedSemaphore.ActiveHoldersCount += 1
				updatedSemaphore.ActiveHolds += request.Weight

				if updatedSemaphore.EarliestHolderExpiresAt < newHolder.ExpiresAt {
					updatedSemaphore.EarliestHolderExpiresAt = newHolder.ExpiresAt
				}

				success = true
			}
		} else {
			panic(err)
		}
	} else {
		// TODO check if weight changed?

		// Update expiration time (extend lock)
		existingHolder.ExpiresAt = request.ExpiresAt
		existingHolder.LockedAt = request.Now

		if updatedSemaphore.EarliestHolderExpiresAt < existingHolder.ExpiresAt {
			updatedSemaphore.EarliestHolderExpiresAt = existingHolder.ExpiresAt
		}

		err := c.holders.Update(txn, existingHolder)
		panicIfNotNil(err)

		success = true
	}

	if semaphore.EarliestHolderExpiresAt != updatedSemaphore.EarliestHolderExpiresAt {
		// Remove a semaphore from expirationRecords at old position
		if semaphore.EarliestHolderExpiresAt != 0 {
			err = c.expirationRecords.Delete(txn, semaphore.EarliestHolderExpiresAt, semaphore.Id)
			panicIfNotNil(err)
		}

		if updatedSemaphore.EarliestHolderExpiresAt != 0 {
			// Add a semaphore into expirationRecords at new position
			err = c.expirationRecords.Add(txn, updatedSemaphore.EarliestHolderExpiresAt, semaphore.Id)
			panicIfNotNil(err)
		}
	}

	err = c.semaphores.Update(txn, updatedSemaphore)
	panicIfNotNil(err)

	err = txn.Commit()
	panicIfNotNil(err)

	return &corepb.AcquireSemaphoreResponse{
		Semaphore: updatedSemaphore,
		Success:   success,
	}, nil
}

func (c *Core) ReleaseSemaphore(request *corepb.ReleaseSemaphoreRequest) (*corepb.ReleaseSemaphoreResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	semaphore, err := c.semaphores.GetByName(txn, request.NamespaceId.AccountId, request.NamespaceId.NamespaceId, request.SemaphoreName)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, monsterax.NewErrorWithContext(
				monsterax.NotFound,
				"semaphore not found",
				map[string]string{
					"semaphore_name": request.SemaphoreName,
				})
		} else {
			panic(err)
		}
	}

	// Remove the holder by process_id (if exists)
	holderId := &corepb.SemaphoreHolderId{
		AccountId:   request.NamespaceId.AccountId,
		NamespaceId: request.NamespaceId.NamespaceId,
		SemaphoreId: semaphore.Id.SemaphoreId,
		ProcessId:   request.ProcessId,
	}
	existingHolder, err := c.holders.Get(txn, holderId)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return &corepb.ReleaseSemaphoreResponse{
				Semaphore: semaphore,
			}, nil
		} else {
			panic(err)
		}
	}

	err = c.holders.Delete(txn, holderId)
	panicIfNotNil(err)

	// Check expired holders
	updatedSemaphore := c.deleteExpiredSemaphoreholders(txn, semaphore, request.Now)

	updatedSemaphore.ActiveHolds -= existingHolder.Weight
	updatedSemaphore.ActiveHoldersCount -= 1

	if semaphore.EarliestHolderExpiresAt != updatedSemaphore.EarliestHolderExpiresAt {
		// Remove a semaphore from expirationRecords at old position
		if semaphore.EarliestHolderExpiresAt != 0 {
			err = c.expirationRecords.Delete(txn, semaphore.EarliestHolderExpiresAt, semaphore.Id)
			panicIfNotNil(err)
		}

		if updatedSemaphore.EarliestHolderExpiresAt != 0 {
			// Add a semaphore into expirationRecords at new position
			err = c.expirationRecords.Add(txn, updatedSemaphore.EarliestHolderExpiresAt, semaphore.Id)
			panicIfNotNil(err)
		}
	}

	err = c.semaphores.Update(txn, updatedSemaphore)
	panicIfNotNil(err)

	err = txn.Commit()
	panicIfNotNil(err)

	return &corepb.ReleaseSemaphoreResponse{
		Semaphore: updatedSemaphore,
	}, nil
}

func (c *Core) RunSemaphoresGarbageCollection(request *corepb.RunSemaphoresGarbageCollectionRequest) (*corepb.RunSemaphoresGarbageCollectionResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	visitedSemaphores := int64(0)

	// List one page of GC records
	gcRecords, err := c.gcRecords.List(txn, int(request.GcRecordsPageSize))
	panicIfNotNil(err)

	for _, gcRecord := range gcRecords {
		switch r := gcRecord.Record.(type) {
		case *corepb.SemaphoresGarbageCollectionRecord_NamespaceId:
			// Delete counters for that namespace. Will not fail if counters do not exist.
			err := c.counters.Delete(txn, r.NamespaceId.AccountId, r.NamespaceId.NamespaceId)
			panicIfNotNil(err)

			// List one page of semaphores for that namespace
			result, err := c.semaphores.List(txn, r.NamespaceId.AccountId, r.NamespaceId.NamespaceId, nil, int(request.GcRecordSemaphoresPageSize))
			panicIfNotNil(err)

			// Delete those semaphores
			for _, semaphore := range result.semaphores {
				// TODO remove holders

				visitedSemaphores++

				// Remove a semaphore from expirationGlobalIndex
				if semaphore.EarliestHolderExpiresAt != 0 {
					err = c.expirationRecords.Delete(txn, semaphore.EarliestHolderExpiresAt, semaphore.Id)
					panicIfNotNil(err)
				}

				// Remove from the main table
				err := c.semaphores.Delete(txn, semaphore.Id)
				panicIfNotNil(err)

				if visitedSemaphores >= request.MaxVisitedSemaphores {
					goto commit
				}
			}

			// Delete the deleted namespace if that was the last page of locks
			if result.nextPaginationToken == nil {
				err := c.gcRecords.Delete(txn, gcRecord)
				panicIfNotNil(err)
			}
		case *corepb.SemaphoresGarbageCollectionRecord_SemaphoreId:
		}
	}

	if visitedSemaphores < request.MaxVisitedSemaphores {
		// Update semaphores with expired holders
		err = c.expirationRecords.List(txn, 0, request.Now, func(record *corepb.SemaphoresExpirationRecord) (bool, error) {
			visitedSemaphores++

			// Get the semaphore
			semaphore, err := c.semaphores.Get(txn, record.SemaphoreId)
			panicIfNotNil(err)

			updatedSemaphore := c.deleteExpiredSemaphoreholders(txn, semaphore, request.Now)

			if semaphore.EarliestHolderExpiresAt != updatedSemaphore.EarliestHolderExpiresAt {
				// Remove a semaphore from expirationRecords at old position
				if semaphore.EarliestHolderExpiresAt != 0 {
					err = c.expirationRecords.Delete(txn, semaphore.EarliestHolderExpiresAt, semaphore.Id)
					panicIfNotNil(err)
				}

				// If semaphore still has holders it will have non zero expiration time
				if updatedSemaphore.EarliestHolderExpiresAt != 0 {
					// Add a semaphore into expirationRecords at new position
					err = c.expirationRecords.Add(txn, updatedSemaphore.EarliestHolderExpiresAt, semaphore.Id)
					panicIfNotNil(err)
				}
			} else {
				// This should not happen
				panic("oldExpiresAt == newExpiresAt")
			}

			err = c.semaphores.Update(txn, updatedSemaphore)
			panicIfNotNil(err)

			// Stop if we have visited enough locks
			return visitedSemaphores < request.MaxVisitedSemaphores, nil
		})
		panicIfNotNil(err)
	}

commit:

	err = txn.Commit()
	panicIfNotNil(err)

	return &corepb.RunSemaphoresGarbageCollectionResponse{}, nil
}

func (c *Core) SemaphoresDeleteNamespace(request *corepb.SemaphoresDeleteNamespaceRequest) (*corepb.SemaphoresDeleteNamespaceResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	// Mark the namespace as deleted
	err := c.gcRecords.Create(txn, &corepb.SemaphoresGarbageCollectionRecord{
		Id: request.RecordId,
		Record: &corepb.SemaphoresGarbageCollectionRecord_NamespaceId{
			NamespaceId: request.NamespaceId,
		},
	})
	panicIfNotNil(err)

	err = txn.Commit()
	panicIfNotNil(err)

	return &corepb.SemaphoresDeleteNamespaceResponse{}, nil
}

// deleteExpiredSemaphoreholders ensures that the semaphore is still held at the moment `now`.
// It deletes holders that expire before `now`, calculates `EarliestHolderExpiresAt`, and returns an updated copy
// of the semaphore.
func (c *Core) deleteExpiredSemaphoreholders(txn *store.Txn, semaphore *corepb.Semaphore, now int64) *corepb.Semaphore {
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

		updatedSemaphore.ActiveHolds -= holder.Weight
		updatedSemaphore.ActiveHoldersCount -= 1

		return true, nil
	})
	panicIfNotNil(err)

	return updatedSemaphore
}

func panicIfNotNil(err error) {
	if err != nil {
		panic(err)
	}
}
