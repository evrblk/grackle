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

// NewCore constructs a Core bound to a single shard of the wait-groups
// keyspace. The given lower/upper bounds delimit the shard's local key range
// (used for Snapshot/Restore), while shardGlobalIndexPrefix scopes
// cross-shard global indexes such as GC records.
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

// Snapshot returns a consistent snapshot of every key range owned by this
// shard's wait-groups Core, suitable for Raft snapshot transfer.
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

// GetWaitGroup looks up a wait group by its full WaitGroupId. Returns a
// NotFound application error if no wait group with that id exists.
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

// GetWaitGroupByName looks up a wait group by its name.
// Returns a NotFound application error if no wait group with that name
// exists in the given namespace.
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

// ListWaitGroups returns a page of wait groups in the given namespace,
// ordered by name. Use the returned NextPaginationToken / PreviousPaginationToken
// to walk forward or backward through pages.
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

// ListWaitGroupJobs returns a page of completed jobs for the named wait
// group. Returns a NotFound application error if the wait group does not
// exist.
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

// CreateWaitGroup creates a new wait group with the given counter and bumps
// the per-namespace wait-group counter. Returns AlreadyExists if a wait group
// with the same name already exists in the namespace, or ResourceExhausted
// if creating it would exceed MaxNumberOfWaitGroupsPerNamespace.
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
		Metadata:    req.Payload.Metadata,
		Version:     1,
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

// UpdateWaitGroup updates the description, expiration time, counter, and metadata
// of an existing wait group. The wait group's name, and completed count are
// immutable and are left untouched. When ExpiresAt changes the global
// expiration index is reconciled (the old entry is removed and a new one
// added) so that garbage collection fires at the new time rather than the old
// one. It is not allowed to shrink counter below the current number of completed
// jobs. Returns NotFound if the wait group does not exist.
func (c *Core) UpdateWaitGroup(req *coreapis.UpdateWaitGroupRequest) (*coreapis.UpdateWaitGroupResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	waitGroup, err := c.waitGroups.GetByName(txn, req.Payload.NamespaceId.AccountId, req.Payload.NamespaceId.NamespaceId, req.Payload.WaitGroupName)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return &coreapis.UpdateWaitGroupResponse{
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

	if waitGroup.Version != req.Payload.ExpectedVersion {
		return &coreapis.UpdateWaitGroupResponse{
			ApplicationError: monsterax.NewErrorWithContext(
				monsterax.InvalidArgument,
				"version mismatch",
				map[string]string{
					"wait_group_name":  req.Payload.WaitGroupName,
					"actual_version":   fmt.Sprintf("%d", waitGroup.Version),
					"expected_version": fmt.Sprintf("%d", req.Payload.ExpectedVersion),
				},
			),
		}, nil
	}

	if waitGroup.Completed > req.Payload.Counter {
		return &coreapis.UpdateWaitGroupResponse{
			ApplicationError: monsterax.NewErrorWithContext(
				monsterax.InvalidArgument,
				"there are currently more completed processes than the new counter",
				map[string]string{
					"wait_group_name": req.Payload.WaitGroupName,
					"completed":       fmt.Sprintf("%d", waitGroup.Completed),
					"new_counter":     fmt.Sprintf("%d", req.Payload.Counter),
				},
			),
		}, nil
	}

	// Reconcile the global expiration index when expires_at changes. Without
	// this the index keeps pointing at the old timestamp and GC would fire at
	// the wrong time.
	if waitGroup.ExpiresAt != req.Payload.ExpiresAt {
		err = c.expirationRecords.Delete(txn, waitGroup.ExpiresAt, waitGroup.Id)
		if err != nil {
			return nil, err
		}
		err = c.expirationRecords.Add(txn, req.Payload.ExpiresAt, waitGroup.Id)
		if err != nil {
			return nil, err
		}
	}

	waitGroup.Description = req.Payload.Description
	waitGroup.ExpiresAt = req.Payload.ExpiresAt
	waitGroup.Metadata = req.Payload.Metadata
	waitGroup.UpdatedAt = req.Payload.Now
	waitGroup.Version += 1
	waitGroup.Counter = req.Payload.Counter

	err = c.waitGroups.Update(txn, waitGroup)
	if err != nil {
		return nil, err
	}

	err = txn.Commit()
	if err != nil {
		return nil, err
	}

	return &coreapis.UpdateWaitGroupResponse{
		Payload: &corepb.UpdateWaitGroupResponse{
			WaitGroup: waitGroup,
		},
	}, nil
}

// DeleteWaitGroup removes the named wait group and schedules its completed
// jobs for asynchronous deletion via a GC record. Deleting a wait group that
// does not exist is a no-op and returns success.
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

// CompleteJobsFromWaitGroup marks each given job id as completed in the
// named wait group, incrementing the Completed counter once per previously
// unseen job id (re-completing an already-completed job is a no-op).
// Returns NotFound if the wait group does not exist, or InvalidArgument if
// the call would push Completed above Counter — in the latter case the
// transaction is discarded and no jobs are persisted.
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

	for _, job := range req.Payload.Jobs {
		waitGroupJobId := &corepb.WaitGroupJobId{
			AccountId:   req.Payload.NamespaceId.AccountId,
			NamespaceId: req.Payload.NamespaceId.NamespaceId,
			WaitGroupId: waitGroup.Id.WaitGroupId,
			JobId:       job.JobId,
		}
		_, err := c.jobs.Get(txn, waitGroupJobId)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				waitGroupJob := &corepb.WaitGroupJob{
					Id:          waitGroupJobId,
					CompletedAt: req.Payload.Now,
					Metadata:    job.Metadata,
				}
				err := c.jobs.Create(txn, waitGroupJob)
				if err != nil {
					return nil, err
				}

				// Increment counter only if we haven't seen this job_id before
				waitGroup.Completed++
			} else {
				return nil, err
			}
		}
	}

	// Reject if completing these jobs would overflow the wait group counter.
	if waitGroup.Completed > waitGroup.Counter {
		return &coreapis.CompleteJobsFromWaitGroupResponse{
			ApplicationError: monsterax.NewErrorWithContext(
				monsterax.InvalidArgument,
				"too many jobs to be marked completed",
				map[string]string{
					"wait_group_name": req.Payload.WaitGroupName,
					"counter":         fmt.Sprintf("%d", waitGroup.Counter),
					"completed":       fmt.Sprintf("%d", waitGroup.Completed),
				}),
		}, nil
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

// RunWaitGroupsGarbageCollection processes one page of pending GC records,
// deleting the wait groups and jobs they reference. Each record is bounded
// by req.MaxDeletedObjects across the whole call; records that fully drain
// within their budget are themselves removed, otherwise they remain and are
// resumed on the next GC tick.
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

// WaitGroupsDeleteNamespace records a GC marker that will, on subsequent
// RunWaitGroupsGarbageCollection ticks, delete every wait group and job
// belonging to the given namespace. The deletion itself is asynchronous;
// this call only enqueues the request.
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
