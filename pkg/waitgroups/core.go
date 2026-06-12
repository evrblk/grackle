package waitgroups

import (
	"errors"
	"fmt"
	"io"

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

	waitGroups        *waitGroupsTable
	jobs              *jobsTable
	counters          *tables.CountersTable[*corepb.WaitGroupsCounter, corepb.WaitGroupsCounter]
	gcRecords         *tables.GCRecordsTable[*corepb.WaitGroupsGarbageCollectionRecord, corepb.WaitGroupsGarbageCollectionRecord]
	expirationRecords *expirationRecordsTable
}

var _ coreapis.GrackleWaitGroupsCoreApi = &Core{}

func NewCore(badgerStore *store.BadgerStore, shardGlobalIndexPrefix []byte, shardLowerBound []byte, shardUpperBound []byte) *Core {
	return &Core{
		badgerStore: badgerStore,

		waitGroups: newWaitGroupsTable(shardLowerBound, shardUpperBound),
		jobs:       newJobsTable(shardLowerBound, shardUpperBound),
		counters: tables.NewCountersTable[*corepb.WaitGroupsCounter, corepb.WaitGroupsCounter](
			tables.Grackle["Grackle.WaitGroupsCore.Counters.Table"].Bytes(),
			shardLowerBound,
			shardUpperBound,
		),
		gcRecords: tables.NewGCRecordsTable[*corepb.WaitGroupsGarbageCollectionRecord, corepb.WaitGroupsGarbageCollectionRecord](
			tables.Grackle["Grackle.WaitGroupsCore.GarbageCollectionRecords.Table"].Bytes(),
			shardGlobalIndexPrefix,
		),
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

func (c *Core) GetWaitGroup(req *coreapis.GetWaitGroupRequest) (*coreapis.GetWaitGroupResponse, error) {
	txn := c.badgerStore.View()
	defer txn.Discard()

	waitGroup, err := c.waitGroups.Get(txn, req.Payload.WaitGroupId)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return &coreapis.GetWaitGroupResponse{
				ApplicationError: monsterax.NewErrorWithContext(
					monsterax.NotFound,
					"wait group not found",
					map[string]string{
						"wait_group_id": ids.EncodeWaitGroupId(req.Payload.WaitGroupId),
					}),
			}, nil
		}

		return nil, err
	}

	return &coreapis.GetWaitGroupResponse{
		Payload: &corepb.GetWaitGroupResponse{
			WaitGroup: waitGroup,
		},
	}, nil
}

func (c *Core) GetWaitGroupByName(req *coreapis.GetWaitGroupByNameRequest) (*coreapis.GetWaitGroupByNameResponse, error) {
	txn := c.badgerStore.View()
	defer txn.Discard()

	waitGroup, err := c.waitGroups.GetByName(txn, req.Payload.NamespaceId.AccountId, req.Payload.NamespaceId.NamespaceId, req.Payload.WaitGroupName)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return &coreapis.GetWaitGroupByNameResponse{
				ApplicationError: monsterax.NewErrorWithContext(
					monsterax.NotFound,
					"wait group not found",
					map[string]string{
						"wait_group_name": req.Payload.WaitGroupName,
					}),
			}, nil
		}

		return nil, err
	}

	return &coreapis.GetWaitGroupByNameResponse{
		Payload: &corepb.GetWaitGroupByNameResponse{
			WaitGroup: waitGroup,
		},
	}, nil
}

func (c *Core) ListWaitGroups(req *coreapis.ListWaitGroupsRequest) (*coreapis.ListWaitGroupsResponse, error) {
	txn := c.badgerStore.View()
	defer txn.Discard()

	result, err := c.waitGroups.List(txn, req.Payload.NamespaceId.AccountId, req.Payload.NamespaceId.NamespaceId, req.Payload.PaginationToken, pagination.GetLimitWithDefaults(int(req.Payload.Limit)))
	if err != nil {
		return nil, err
	}

	return &coreapis.ListWaitGroupsResponse{
		Payload: &corepb.ListWaitGroupsResponse{
			WaitGroups:              result.waitGroups,
			NextPaginationToken:     result.nextPaginationToken,
			PreviousPaginationToken: result.previousPaginationToken,
		},
	}, nil
}

func (c *Core) ListWaitGroupJobs(req *coreapis.ListWaitGroupJobsRequest) (*coreapis.ListWaitGroupJobsResponse, error) {
	txn := c.badgerStore.View()
	defer txn.Discard()

	waitGroup, err := c.waitGroups.GetByName(txn, req.Payload.NamespaceId.AccountId, req.Payload.NamespaceId.NamespaceId, req.Payload.WaitGroupName)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return &coreapis.ListWaitGroupJobsResponse{
				ApplicationError: monsterax.NewErrorWithContext(
					monsterax.NotFound,
					"wait group not found",
					map[string]string{
						"wait_group_name": req.Payload.WaitGroupName,
					}),
			}, nil
		}

		return nil, err
	}

	result, err := c.jobs.List(txn, req.Payload.NamespaceId.AccountId, req.Payload.NamespaceId.NamespaceId, waitGroup.Id.WaitGroupId, req.Payload.PaginationToken, pagination.GetLimitWithDefaults(int(req.Payload.Limit)))
	if err != nil {
		return nil, err
	}

	return &coreapis.ListWaitGroupJobsResponse{
		Payload: &corepb.ListWaitGroupJobsResponse{
			Jobs:                    result.jobs,
			NextPaginationToken:     result.nextPaginationToken,
			PreviousPaginationToken: result.previousPaginationToken,
		},
	}, nil
}

func (c *Core) CreateWaitGroup(req *coreapis.CreateWaitGroupRequest) (*coreapis.CreateWaitGroupResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	// Check name uniqueness
	_, err := c.waitGroups.GetByName(txn, req.Payload.WaitGroupId.AccountId, req.Payload.WaitGroupId.NamespaceId, req.Payload.Name)
	if err != nil {
		if !errors.Is(err, store.ErrNotFound) {
			return nil, err
		}
	} else {
		return &coreapis.CreateWaitGroupResponse{
			ApplicationError: monsterax.NewErrorWithContext(
				monsterax.AlreadyExists,
				"wait group with this name already exists",
				map[string]string{
					"wait_group_name": req.Payload.Name,
				}),
		}, nil
	}

	// Get counters for that namespace
	counters, err := c.counters.Get(txn, req.Payload.WaitGroupId.AccountId, req.Payload.WaitGroupId.NamespaceId)
	if err != nil {
		return nil, err
	}

	// Checking max number of wait groups
	if counters.NumberOfWaitGroups >= req.Payload.MaxNumberOfWaitGroupsPerNamespace {
		return &coreapis.CreateWaitGroupResponse{
			ApplicationError: monsterax.NewErrorWithContext(
				monsterax.ResourceExhausted,
				"max number of wait groups per namespace reached",
				map[string]string{"limit": fmt.Sprintf("%d", req.Payload.MaxNumberOfWaitGroupsPerNamespace)},
			),
		}, nil
	}

	waitGroup := &corepb.WaitGroup{
		Id:          req.Payload.WaitGroupId,
		Name:        req.Payload.Name,
		Description: req.Payload.Description,
		Counter:     req.Payload.Counter,
		Completed:   0,
		CreatedAt:   req.Payload.Now,
		UpdatedAt:   req.Payload.Now,
		ExpiresAt:   req.Payload.ExpiresAt,
	}

	err = c.waitGroups.Create(txn, waitGroup)
	if err != nil {
		return nil, err
	}

	// Update counters
	counters.NumberOfWaitGroups += 1
	err = c.counters.Set(txn, req.Payload.WaitGroupId.AccountId, req.Payload.WaitGroupId.NamespaceId, counters)
	if err != nil {
		return nil, err
	}

	err = c.expirationRecords.Add(txn, waitGroup.ExpiresAt, req.Payload.WaitGroupId)
	if err != nil {
		return nil, err
	}

	err = txn.Commit()
	if err != nil {
		return nil, err
	}

	return &coreapis.CreateWaitGroupResponse{
		Payload: &corepb.CreateWaitGroupResponse{
			WaitGroup: waitGroup,
		},
	}, nil
}

func (c *Core) DeleteWaitGroup(req *coreapis.DeleteWaitGroupRequest) (*coreapis.DeleteWaitGroupResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	waitGroup, err := c.waitGroups.GetByName(txn, req.Payload.NamespaceId.AccountId, req.Payload.NamespaceId.NamespaceId, req.Payload.WaitGroupName)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			// No wait group exists, do nothing
			return &coreapis.DeleteWaitGroupResponse{
				Payload: &corepb.DeleteWaitGroupResponse{},
			}, nil
		}

		return nil, err
	}

	// Get counters for this namespace
	counters, err := c.counters.Get(txn, req.Payload.NamespaceId.AccountId, req.Payload.NamespaceId.NamespaceId)
	if err != nil {
		return nil, err
	}

	err = c.waitGroups.Delete(txn, waitGroup.Id)
	if err != nil {
		return nil, err
	}

	// Mark the wait group's jobs for deletion
	err = c.gcRecords.Create(txn, &corepb.WaitGroupsGarbageCollectionRecord{
		Id: req.Payload.RecordId,
		Record: &corepb.WaitGroupsGarbageCollectionRecord_WaitGroupId{
			WaitGroupId: waitGroup.Id,
		},
	})
	if err != nil {
		return nil, err
	}

	// Update counters
	counters.NumberOfWaitGroups -= 1
	err = c.counters.Set(txn, req.Payload.NamespaceId.AccountId, req.Payload.NamespaceId.NamespaceId, counters)
	if err != nil {
		return nil, err
	}

	err = txn.Commit()
	if err != nil {
		return nil, err
	}

	return &coreapis.DeleteWaitGroupResponse{
		Payload: &corepb.DeleteWaitGroupResponse{},
	}, nil
}

func (c *Core) AddJobsToWaitGroup(req *coreapis.AddJobsToWaitGroupRequest) (*coreapis.AddJobsToWaitGroupResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	waitGroup, err := c.waitGroups.GetByName(txn, req.Payload.NamespaceId.AccountId, req.Payload.NamespaceId.NamespaceId, req.Payload.WaitGroupName)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return &coreapis.AddJobsToWaitGroupResponse{
				ApplicationError: monsterax.NewErrorWithContext(
					monsterax.NotFound,
					"wait group not found",
					map[string]string{
						"wait_group_name": req.Payload.WaitGroupName,
					}),
			}, nil
		}

		return nil, err
	}

	// Check if wait group is too big
	if waitGroup.Counter+req.Payload.Counter > uint64(req.Payload.MaxWaitGroupSize) {
		return &coreapis.AddJobsToWaitGroupResponse{
			ApplicationError: monsterax.NewErrorWithContext(
				monsterax.ResourceExhausted,
				"wait group counter is too big",
				map[string]string{"limit": fmt.Sprintf("%d", req.Payload.MaxWaitGroupSize)},
			),
		}, nil
	}

	waitGroup.Counter += req.Payload.Counter
	waitGroup.UpdatedAt = req.Payload.Now

	err = c.waitGroups.Update(txn, waitGroup)
	if err != nil {
		return nil, err
	}

	err = txn.Commit()
	if err != nil {
		return nil, err
	}

	return &coreapis.AddJobsToWaitGroupResponse{
		Payload: &corepb.AddJobsToWaitGroupResponse{
			WaitGroup: waitGroup,
		},
	}, nil
}

func (c *Core) CompleteJobsFromWaitGroup(req *coreapis.CompleteJobsFromWaitGroupRequest) (*coreapis.CompleteJobsFromWaitGroupResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	waitGroup, err := c.waitGroups.GetByName(txn, req.Payload.NamespaceId.AccountId, req.Payload.NamespaceId.NamespaceId, req.Payload.WaitGroupName)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return &coreapis.CompleteJobsFromWaitGroupResponse{
				ApplicationError: monsterax.NewErrorWithContext(
					monsterax.NotFound,
					"wait group not found",
					map[string]string{
						"wait_group_name": req.Payload.WaitGroupName,
					}),
			}, nil
		}

		return nil, err
	}

	for _, jobId := range req.Payload.JobIds {
		waitGroupJobId := &corepb.WaitGroupJobId{
			AccountId:   req.Payload.NamespaceId.AccountId,
			NamespaceId: req.Payload.NamespaceId.NamespaceId,
			WaitGroupId: waitGroup.Id.WaitGroupId,
			JobId:       jobId,
		}
		_, err := c.jobs.Get(txn, waitGroupJobId)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				waitGroupJob := &corepb.WaitGroupJob{
					Id:          waitGroupJobId,
					CompletedAt: req.Payload.Now,
				}
				err := c.jobs.Create(txn, waitGroupJob)
				if err != nil {
					return nil, err
				}

				// Increment counter only if we haven't seen this process_id before
				waitGroup.Completed++
			} else {
				return nil, err
			}
		}
	}

	waitGroup.UpdatedAt = req.Payload.Now

	err = c.waitGroups.Update(txn, waitGroup)
	if err != nil {
		return nil, err
	}

	err = txn.Commit()
	if err != nil {
		return nil, err
	}

	return &coreapis.CompleteJobsFromWaitGroupResponse{
		Payload: &corepb.CompleteJobsFromWaitGroupResponse{
			WaitGroup: waitGroup,
		},
	}, nil
}

func (c *Core) RunWaitGroupsGarbageCollection(req *coreapis.RunWaitGroupsGarbageCollectionRequest) (*coreapis.RunWaitGroupsGarbageCollectionResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	totalDeletedObjects := 0

	// List one page of GC records
	gcRecords, err := c.gcRecords.List(txn, int(req.Payload.GcRecordsPageSize))
	if err != nil {
		return nil, err
	}

	for _, gcRecord := range gcRecords {
		limit := int(req.Payload.MaxDeletedObjects) - totalDeletedObjects
		deletedObjects := 0

		switch r := gcRecord.Record.(type) {
		case *corepb.WaitGroupsGarbageCollectionRecord_NamespaceId:
			deletedObjects, err = c.deleteNamespace(txn, r.NamespaceId, int(req.Payload.GcRecordWaitGroupsPageSize), limit)
			if err != nil {
				return nil, err
			}

		case *corepb.WaitGroupsGarbageCollectionRecord_WaitGroupId:
			deletedObjects, err = c.deleteWaitGroupJobs(txn, r.WaitGroupId, limit)
			if err != nil {
				return nil, err
			}
		}

		totalDeletedObjects = totalDeletedObjects + deletedObjects

		// If the number of actually deleted objects is less than the limit, then we have removed everything related to
		// that garbage collection record.
		if deletedObjects < limit {
			// Remove this GC record since it is completed
			err := c.gcRecords.Delete(txn, gcRecord)
			if err != nil {
				return nil, err
			}
		}
	}

	err = txn.Commit()
	if err != nil {
		return nil, err
	}

	return &coreapis.RunWaitGroupsGarbageCollectionResponse{
		Payload: &corepb.RunWaitGroupsGarbageCollectionResponse{},
	}, nil
}

func (c *Core) WaitGroupsDeleteNamespace(req *coreapis.WaitGroupsDeleteNamespaceRequest) (*coreapis.WaitGroupsDeleteNamespaceResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	// Mark the namespace as deleted
	err := c.gcRecords.Create(txn, &corepb.WaitGroupsGarbageCollectionRecord{
		Id: req.Payload.RecordId,
		Record: &corepb.WaitGroupsGarbageCollectionRecord_NamespaceId{
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

	return &coreapis.WaitGroupsDeleteNamespaceResponse{
		Payload: &corepb.WaitGroupsDeleteNamespaceResponse{},
	}, nil
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
