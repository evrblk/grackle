package waitgroups

import (
	"errors"
	"fmt"
	"io"

	"github.com/evrblk/monstera"
	"github.com/evrblk/monstera/store"
	monsterax "github.com/evrblk/monstera/x"

	"github.com/evrblk/grackle/pkg/corepb"
	"github.com/evrblk/grackle/pkg/ids"
	"github.com/evrblk/grackle/pkg/monsteragen"
	"github.com/evrblk/grackle/pkg/pagination"
)

type Core struct {
	badgerStore *store.BadgerStore

	waitGroups        *waitGroupsTable
	jobs              *jobsTable
	counters          *countersTable
	gcRecords         *gcRecordsTable
	expirationRecords *expirationRecordsTable
}

var _ monsteragen.GrackleWaitGroupsCoreApi = &Core{}

func NewCore(badgerStore *store.BadgerStore, shardGlobalIndexPrefix []byte, shardLowerBound []byte, shardUpperBound []byte) *Core {
	return &Core{
		badgerStore: badgerStore,

		waitGroups:        newWaitGroupsTable(shardLowerBound, shardUpperBound),
		jobs:              newJobsTable(shardLowerBound, shardUpperBound),
		counters:          newCountersTable(shardLowerBound, shardUpperBound),
		gcRecords:         newGCRecordsTable(shardGlobalIndexPrefix),
		expirationRecords: newExpirationRecordsTable(shardGlobalIndexPrefix),
	}
}

func (c *Core) ranges() []monsterax.KeyRange {
	ranges := []monsterax.KeyRange{
		c.jobs.GetTableKeyRange(),
		c.counters.GetTableKeyRange(),
		c.gcRecords.GetTableKeyRange(),
		c.expirationRecords.GetTableKeyRange(),
	}

	ranges = append(ranges, c.waitGroups.GetTableKeyRanges()...)

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

func (c *Core) GetWaitGroup(request *corepb.GetWaitGroupRequest) (*corepb.GetWaitGroupResponse, error) {
	txn := c.badgerStore.View()
	defer txn.Discard()

	waitGroup, err := c.waitGroups.Get(txn, request.WaitGroupId)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, monsterax.NewErrorWithContext(
				monsterax.NotFound,
				"wait group not found",
				map[string]string{
					"wait_group_id": ids.EncodeWaitGroupId(request.WaitGroupId),
				})
		} else {
			panic(err)
		}
	}

	return &corepb.GetWaitGroupResponse{
		WaitGroup: waitGroup,
	}, nil
}

func (c *Core) GetWaitGroupByName(request *corepb.GetWaitGroupByNameRequest) (*corepb.GetWaitGroupByNameResponse, error) {
	txn := c.badgerStore.View()
	defer txn.Discard()

	waitGroup, err := c.waitGroups.GetByName(txn, request.NamespaceId.AccountId, request.NamespaceId.NamespaceId, request.WaitGroupName)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, monsterax.NewErrorWithContext(
				monsterax.NotFound,
				"wait group not found",
				map[string]string{
					"wait_group_name": request.WaitGroupName,
				})
		} else {
			panic(err)
		}
	}

	return &corepb.GetWaitGroupByNameResponse{
		WaitGroup: waitGroup,
	}, nil
}

func (c *Core) ListWaitGroups(request *corepb.ListWaitGroupsRequest) (*corepb.ListWaitGroupsResponse, error) {
	txn := c.badgerStore.View()
	defer txn.Discard()

	result, err := c.waitGroups.List(txn, request.NamespaceId.AccountId, request.NamespaceId.NamespaceId, request.PaginationToken, pagination.GetLimitWithDefaults(int(request.Limit)))
	panicIfNotNil(err)

	return &corepb.ListWaitGroupsResponse{
		WaitGroups:              result.waitGroups,
		NextPaginationToken:     result.nextPaginationToken,
		PreviousPaginationToken: result.previousPaginationToken,
	}, nil
}

func (c *Core) ListWaitGroupJobs(request *corepb.ListWaitGroupJobsRequest) (*corepb.ListWaitGroupJobsResponse, error) {
	txn := c.badgerStore.View()
	defer txn.Discard()

	waitGroup, err := c.waitGroups.GetByName(txn, request.NamespaceId.AccountId, request.NamespaceId.NamespaceId, request.WaitGroupName)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, monsterax.NewErrorWithContext(
				monsterax.NotFound,
				"wait group not found",
				map[string]string{
					"wait_group_name": request.WaitGroupName,
				})
		} else {
			panic(err)
		}
	}

	result, err := c.jobs.List(txn, request.NamespaceId.AccountId, request.NamespaceId.NamespaceId, waitGroup.Id.WaitGroupId, request.PaginationToken, pagination.GetLimitWithDefaults(int(request.Limit)))
	panicIfNotNil(err)

	return &corepb.ListWaitGroupJobsResponse{
		Jobs:                    result.jobs,
		NextPaginationToken:     result.nextPaginationToken,
		PreviousPaginationToken: result.previousPaginationToken,
	}, nil
}

func (c *Core) CreateWaitGroup(request *corepb.CreateWaitGroupRequest) (*corepb.CreateWaitGroupResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	// Check name uniqueness
	_, err := c.waitGroups.GetByName(txn, request.WaitGroupId.AccountId, request.WaitGroupId.NamespaceId, request.Name)
	if err != nil {
		if !errors.Is(err, store.ErrNotFound) {
			panic(err)
		}
	} else {
		return nil, monsterax.NewErrorWithContext(
			monsterax.AlreadyExists,
			"wait group with this name already exists",
			map[string]string{
				"wait_group_name": request.Name,
			})
	}

	// Get counters for that namespace
	counters, err := c.counters.Get(txn, request.WaitGroupId.AccountId, request.WaitGroupId.NamespaceId)
	panicIfNotNil(err)

	// Checking max number of wait groups
	if counters.NumberOfWaitGroups >= request.MaxNumberOfWaitGroupsPerNamespace {
		return nil, monsterax.NewErrorWithContext(
			monsterax.ResourceExhausted,
			"max number of wait groups per namespace reached",
			map[string]string{"limit": fmt.Sprintf("%d", request.MaxNumberOfWaitGroupsPerNamespace)})
	}

	waitGroup := &corepb.WaitGroup{
		Id:          request.WaitGroupId,
		Name:        request.Name,
		Description: request.Description,
		Counter:     request.Counter,
		Completed:   0,
		CreatedAt:   request.Now,
		UpdatedAt:   request.Now,
		ExpiresAt:   request.ExpiresAt,
	}

	err = c.waitGroups.Create(txn, waitGroup)
	panicIfNotNil(err)

	// Update counters
	counters.NumberOfWaitGroups += 1
	err = c.counters.Set(txn, request.WaitGroupId.AccountId, request.WaitGroupId.NamespaceId, counters)
	panicIfNotNil(err)

	err = c.expirationRecords.Add(txn, waitGroup.ExpiresAt, request.WaitGroupId)
	panicIfNotNil(err)

	err = txn.Commit()
	panicIfNotNil(err)

	return &corepb.CreateWaitGroupResponse{
		WaitGroup: waitGroup,
	}, nil
}

func (c *Core) DeleteWaitGroup(request *corepb.DeleteWaitGroupRequest) (*corepb.DeleteWaitGroupResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	waitGroup, err := c.waitGroups.GetByName(txn, request.NamespaceId.AccountId, request.NamespaceId.NamespaceId, request.WaitGroupName)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			// No wait group exists, do nothing
			return &corepb.DeleteWaitGroupResponse{}, nil
		} else {
			panic(err)
		}
	}

	// Get counters for this namespace
	counters, err := c.counters.Get(txn, request.NamespaceId.AccountId, request.NamespaceId.NamespaceId)
	panicIfNotNil(err)

	err = c.waitGroups.Delete(txn, waitGroup.Id)
	panicIfNotNil(err)

	// Mark the wait group's jobs for deletion
	err = c.gcRecords.Create(txn, &corepb.WaitGroupsGarbageCollectionRecord{
		Id: request.RecordId,
		Record: &corepb.WaitGroupsGarbageCollectionRecord_WaitGroupId{
			WaitGroupId: waitGroup.Id,
		},
	})
	panicIfNotNil(err)

	// Update counters
	counters.NumberOfWaitGroups -= 1
	err = c.counters.Set(txn, request.NamespaceId.AccountId, request.NamespaceId.NamespaceId, counters)
	panicIfNotNil(err)

	err = txn.Commit()
	panicIfNotNil(err)

	return &corepb.DeleteWaitGroupResponse{}, nil
}

func (c *Core) AddJobsToWaitGroup(request *corepb.AddJobsToWaitGroupRequest) (*corepb.AddJobsToWaitGroupResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	waitGroup, err := c.waitGroups.GetByName(txn, request.NamespaceId.AccountId, request.NamespaceId.NamespaceId, request.WaitGroupName)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, monsterax.NewErrorWithContext(
				monsterax.NotFound,
				"wait group not found",
				map[string]string{
					"wait_group_name": request.WaitGroupName,
				})
		} else {
			panic(err)
		}
	}

	// Check if wait group is too big
	if waitGroup.Counter+request.Counter > uint64(request.MaxWaitGroupSize) {
		return nil, monsterax.NewErrorWithContext(
			monsterax.ResourceExhausted,
			"wait group counter is too big",
			map[string]string{"limit": fmt.Sprintf("%d", request.MaxWaitGroupSize)})
	}

	waitGroup.Counter += request.Counter
	waitGroup.UpdatedAt = request.Now

	err = c.waitGroups.Update(txn, waitGroup)
	panicIfNotNil(err)

	err = txn.Commit()
	panicIfNotNil(err)

	return &corepb.AddJobsToWaitGroupResponse{
		WaitGroup: waitGroup,
	}, nil
}

func (c *Core) CompleteJobsFromWaitGroup(request *corepb.CompleteJobsFromWaitGroupRequest) (*corepb.CompleteJobsFromWaitGroupResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	waitGroup, err := c.waitGroups.GetByName(txn, request.NamespaceId.AccountId, request.NamespaceId.NamespaceId, request.WaitGroupName)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, monsterax.NewErrorWithContext(
				monsterax.NotFound,
				"wait group not found",
				map[string]string{
					"wait_group_name": request.WaitGroupName,
				})
		} else {
			panic(err)
		}
	}

	for _, processId := range request.ProcessIds {
		waitGroupJobId := &corepb.WaitGroupJobId{
			AccountId:   request.NamespaceId.AccountId,
			NamespaceId: request.NamespaceId.NamespaceId,
			WaitGroupId: waitGroup.Id.WaitGroupId,
			ProcessId:   processId,
		}
		_, err := c.jobs.Get(txn, waitGroupJobId)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				waitGroupJob := &corepb.WaitGroupJob{
					Id:          waitGroupJobId,
					CompletedAt: request.Now,
				}
				err := c.jobs.Create(txn, waitGroupJob)
				panicIfNotNil(err)

				// Increment counter only if we haven't seen this process_id before
				waitGroup.Completed++
			} else {
				panic(err)
			}
		}
	}

	waitGroup.UpdatedAt = request.Now

	err = c.waitGroups.Update(txn, waitGroup)
	panicIfNotNil(err)

	err = txn.Commit()
	panicIfNotNil(err)

	return &corepb.CompleteJobsFromWaitGroupResponse{
		WaitGroup: waitGroup,
	}, nil
}

func (c *Core) RunWaitGroupsGarbageCollection(request *corepb.RunWaitGroupsGarbageCollectionRequest) (*corepb.RunWaitGroupsGarbageCollectionResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	totalDeletedObjects := 0

	// List one page of GC records
	gcRecords, err := c.gcRecords.List(txn, int(request.GcRecordsPageSize))
	panicIfNotNil(err)

	for _, gcRecord := range gcRecords {
		limit := int(request.MaxDeletedObjects) - totalDeletedObjects
		deletedObjects := 0

		switch r := gcRecord.Record.(type) {
		case *corepb.WaitGroupsGarbageCollectionRecord_NamespaceId:
			deletedObjects, err = c.deleteNamespace(txn, r.NamespaceId, int(request.GcRecordWaitGroupsPageSize), limit)
			panicIfNotNil(err)

		case *corepb.WaitGroupsGarbageCollectionRecord_WaitGroupId:
			deletedObjects, err = c.deleteWaitGroupJobs(txn, r.WaitGroupId, limit)
			panicIfNotNil(err)
		}

		totalDeletedObjects = totalDeletedObjects + deletedObjects

		// If the number of actually deleted objects is less than the limit, then we have removed everything related to
		// that garbage collection record.
		if deletedObjects < limit {
			// Remove this GC record since it is completed
			err := c.gcRecords.Delete(txn, gcRecord)
			panicIfNotNil(err)
		}
	}

	err = txn.Commit()
	panicIfNotNil(err)

	return &corepb.RunWaitGroupsGarbageCollectionResponse{}, nil
}

func (c *Core) WaitGroupsDeleteNamespace(request *corepb.WaitGroupsDeleteNamespaceRequest) (*corepb.WaitGroupsDeleteNamespaceResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	// Mark the namespace as deleted
	err := c.gcRecords.Create(txn, &corepb.WaitGroupsGarbageCollectionRecord{
		Id: request.RecordId,
		Record: &corepb.WaitGroupsGarbageCollectionRecord_NamespaceId{
			NamespaceId: request.NamespaceId,
		},
	})
	panicIfNotNil(err)

	err = txn.Commit()
	panicIfNotNil(err)

	return &corepb.WaitGroupsDeleteNamespaceResponse{}, nil
}

func (c *Core) deleteWaitGroupJobs(txn *store.Txn, waitGroupId *corepb.WaitGroupId, waitGroupJobsPageSize int) (int, error) {
	deletedObjects := 0

	// Delete one page of completed jobs
	waitGroupJobsPage, err := c.jobs.List(txn, waitGroupId.AccountId, waitGroupId.NamespaceId, waitGroupId.WaitGroupId, nil, waitGroupJobsPageSize)
	if err != nil {
		return deletedObjects, err
	}
	for _, waitGroupJob := range waitGroupJobsPage.jobs {
		err := c.jobs.Delete(txn, waitGroupJob.Id)
		if err != nil {
			return deletedObjects, err
		}

		deletedObjects++
	}

	// deletedObjects holds the amount of objects that were actually deleted, can be less than waitGroupJobsPageSize.
	return deletedObjects, nil
}

func (c *Core) deleteNamespace(txn *store.Txn, namespaceId *corepb.NamespaceId, waitGroupsPageSize int, maxDeletedObjects int) (int, error) {
	deletedObjects := 0

	// List one page of wait groups for that namespace
	waitGroupsPage, err := c.waitGroups.List(txn, namespaceId.AccountId, namespaceId.NamespaceId, nil, waitGroupsPageSize)
	if err != nil {
		return deletedObjects, err
	}

	// Delete those wait groups
	for _, waitGroup := range waitGroupsPage.waitGroups {
		// -3 is for expirationGlobalIndex, counters, and main table records
		limit := maxDeletedObjects - deletedObjects - 3

		deletedJobs, err := c.deleteWaitGroupJobs(txn, waitGroup.Id, limit)
		if err != nil {
			return deletedObjects, err
		}
		deletedObjects = deletedObjects + deletedJobs

		// If the number of actually deleted jobs is less than the limit, then we have reached the end of jobs.
		if deletedJobs < limit {
			// Remove a wait group from expirationGlobalIndex. Will not fail if it was already removed.
			err = c.expirationRecords.Delete(txn, waitGroup.ExpiresAt, waitGroup.Id)
			if err != nil {
				return deletedObjects, err
			}
			deletedObjects++

			// Remove from the main table. Will not fail if it was already removed.
			err = c.waitGroups.Delete(txn, waitGroup.Id)

			if err != nil {
				return deletedObjects, err
			}
			deletedObjects++
		}
	}

	// Delete counters for that namespace.
	err = c.counters.Delete(txn, namespaceId.AccountId, namespaceId.NamespaceId)
	if err != nil {
		return deletedObjects, err
	}
	deletedObjects++

	return deletedObjects, nil
}

func panicIfNotNil(err error) {
	if err != nil {
		panic(err)
	}
}
