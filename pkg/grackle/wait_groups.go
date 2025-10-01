package grackle

import (
	"fmt"
	"io"

	"github.com/go-errors/errors"

	"github.com/evrblk/grackle/pkg/corepb"
	"github.com/evrblk/monstera"
	monsterax "github.com/evrblk/monstera/x"
)

type WaitGroupsCore struct {
	badgerStore            *monstera.BadgerStore
	shardGlobalIndexPrefix []byte

	waitGroupsTable         *monsterax.CompositeKeyTable[*corepb.WaitGroup, corepb.WaitGroup]
	waitGroupJobsTable      *monsterax.CompositeKeyTable[*corepb.WaitGroupJob, corepb.WaitGroupJob]
	waitGroupsCountersTable *monsterax.SimpleKeyTable[*corepb.WaitGroupsCounter, corepb.WaitGroupsCounter]
	gcRecordsGlobalIndex    *monsterax.SimpleKeyTable[*corepb.WaitGroupsGCRecord, corepb.WaitGroupsGCRecord]                                       // Global index
	expirationGlobalIndex   *monsterax.SimpleKeyTable[*corepb.WaitGroupsExpirationGlobalIndexRecord, corepb.WaitGroupsExpirationGlobalIndexRecord] // Global index
}

var _ GrackleWaitGroupsCoreApi = &WaitGroupsCore{}

func NewWaitGroupsCore(badgerStore *monstera.BadgerStore, shardGlobalIndexPrefix []byte, shardLowerBound []byte, shardUpperBound []byte) *WaitGroupsCore {
	return &WaitGroupsCore{
		badgerStore:            badgerStore,
		shardGlobalIndexPrefix: shardGlobalIndexPrefix,

		waitGroupsTable:         monsterax.NewCompositeKeyTable[*corepb.WaitGroup, corepb.WaitGroup](GrackleWaitGroupsTableId, shardLowerBound, shardUpperBound),
		waitGroupJobsTable:      monsterax.NewCompositeKeyTable[*corepb.WaitGroupJob, corepb.WaitGroupJob](GrackleWaitGroupJobsTableId, shardLowerBound, shardUpperBound),
		waitGroupsCountersTable: monsterax.NewSimpleKeyTable[*corepb.WaitGroupsCounter, corepb.WaitGroupsCounter](GrackleWaitGroupsCountersTableId, shardLowerBound, shardUpperBound),
		gcRecordsGlobalIndex:    monsterax.NewSimpleKeyTable[*corepb.WaitGroupsGCRecord, corepb.WaitGroupsGCRecord](GrackleWaitGroupsGCRecordsGlobalIndexId, shardGlobalIndexPrefix, shardGlobalIndexPrefix),
		expirationGlobalIndex:   monsterax.NewSimpleKeyTable[*corepb.WaitGroupsExpirationGlobalIndexRecord, corepb.WaitGroupsExpirationGlobalIndexRecord](GrackleWaitGroupsExpirationGlobalIndexId, shardGlobalIndexPrefix, shardGlobalIndexPrefix),
	}
}

func (c *WaitGroupsCore) ranges() []monstera.KeyRange {
	return []monstera.KeyRange{
		c.waitGroupsTable.GetTableKeyRange(),
		c.waitGroupJobsTable.GetTableKeyRange(),
		c.waitGroupsCountersTable.GetTableKeyRange(),
	}
}

func (c *WaitGroupsCore) Snapshot() monstera.ApplicationCoreSnapshot {
	return monsterax.Snapshot(c.badgerStore, c.ranges())
}

func (c *WaitGroupsCore) Restore(reader io.ReadCloser) error {
	return monsterax.Restore(c.badgerStore, c.ranges(), reader)
}

func (c *WaitGroupsCore) Close() {

}

func (c *WaitGroupsCore) GetWaitGroup(request *corepb.GetWaitGroupRequest) (*corepb.GetWaitGroupResponse, error) {
	txn := c.badgerStore.View()
	defer txn.Discard()

	waitGroup, err := c.getWaitGroup(txn, request.WaitGroupId)
	if err != nil {
		if errors.Is(err, monstera.ErrNotFound) {
			return nil, monsterax.NewErrorWithContext(
				monsterax.NotFound,
				"wait group not found",
				map[string]string{
					"namespace_name":  request.WaitGroupId.NamespaceName,
					"wait_group_name": request.WaitGroupId.WaitGroupName,
				})
		} else {
			panic(err)
		}
	}

	return &corepb.GetWaitGroupResponse{
		WaitGroup: waitGroup,
	}, nil
}

func (c *WaitGroupsCore) ListWaitGroups(request *corepb.ListWaitGroupsRequest) (*corepb.ListWaitGroupsResponse, error) {
	txn := c.badgerStore.View()
	defer txn.Discard()

	result, err := c.listWaitGroups(txn, request.NamespaceTimestampedId.AccountId, request.NamespaceTimestampedId.NamespaceName, request.NamespaceTimestampedId.NamespaceCreatedAt, request.PaginationToken, getLimit(int(request.Limit)))
	panicIfNotNil(err)

	return &corepb.ListWaitGroupsResponse{
		WaitGroups:              result.waitGroups,
		NextPaginationToken:     result.nextPaginationToken,
		PreviousPaginationToken: result.previousPaginationToken,
	}, nil
}

func (c *WaitGroupsCore) CreateWaitGroup(request *corepb.CreateWaitGroupRequest) (*corepb.CreateWaitGroupResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	waitGroupId := &corepb.WaitGroupId{
		AccountId:          request.NamespaceTimestampedId.AccountId,
		NamespaceName:      request.NamespaceTimestampedId.NamespaceName,
		NamespaceCreatedAt: request.NamespaceTimestampedId.NamespaceCreatedAt,
		WaitGroupName:      request.Name,
	}

	// Check name uniqueness
	_, err := c.getWaitGroup(txn, waitGroupId)
	if err != nil {
		if !errors.Is(err, monstera.ErrNotFound) {
			return nil, err
		}
	} else {
		return nil, monsterax.NewErrorWithContext(
			monsterax.AlreadyExists,
			"wait group with this name already exists",
			map[string]string{
				"namespace_name":  waitGroupId.NamespaceName,
				"wait_group_name": waitGroupId.WaitGroupName,
			})
	}

	// Get counters for that namespace
	counters, err := c.getCounters(txn, request.NamespaceTimestampedId.AccountId, request.NamespaceTimestampedId.NamespaceName, request.NamespaceTimestampedId.NamespaceCreatedAt)
	panicIfNotNil(err)

	// Checking max number of wait groups
	if counters.NumberOfWaitGroups >= request.MaxNumberOfWaitGroupsPerNamespace {
		return nil, monsterax.NewErrorWithContext(
			monsterax.ResourceExhausted,
			"max number of wait groups per namespace reached",
			map[string]string{"limit": fmt.Sprintf("%d", request.MaxNumberOfWaitGroupsPerNamespace)})
	}

	waitGroup := &corepb.WaitGroup{
		Id:          waitGroupId,
		Description: request.Description,
		Counter:     request.Counter,
		Completed:   0,
		CreatedAt:   request.Now,
		UpdatedAt:   request.Now,
		ExpiresAt:   request.ExpiresAt,
	}

	err = c.createWaitGroup(txn, waitGroup)
	panicIfNotNil(err)

	// Update counters
	counters.NumberOfWaitGroups = counters.NumberOfWaitGroups + 1
	err = c.setCounters(txn, request.NamespaceTimestampedId.AccountId, request.NamespaceTimestampedId.NamespaceName, request.NamespaceTimestampedId.NamespaceCreatedAt, counters)
	panicIfNotNil(err)

	err = txn.Commit()
	panicIfNotNil(err)

	return &corepb.CreateWaitGroupResponse{
		WaitGroup: waitGroup,
	}, nil
}

func (c *WaitGroupsCore) DeleteWaitGroup(request *corepb.DeleteWaitGroupRequest) (*corepb.DeleteWaitGroupResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	waitGroup, err := c.getWaitGroup(txn, request.WaitGroupId)
	if err != nil {
		if errors.Is(err, monstera.ErrNotFound) {
			// No wait group exists, do nothing
			return &corepb.DeleteWaitGroupResponse{}, nil
		} else {
			panic(err)
		}
	}

	// Get counters for this namespace
	counters, err := c.getCounters(txn, request.WaitGroupId.AccountId, request.WaitGroupId.NamespaceName, request.WaitGroupId.NamespaceCreatedAt)
	panicIfNotNil(err)

	err = c.deleteWaitGroup(txn, waitGroup.Id)
	panicIfNotNil(err)

	// Mark the wait group's jobs for deletion
	err = c.createGCRecord(txn, &corepb.WaitGroupsGCRecord{
		Id: request.RecordId,
		Record: &corepb.WaitGroupsGCRecord_WaitGroupId{
			WaitGroupId: waitGroup.Id,
		},
	})
	panicIfNotNil(err)

	// Update counters
	counters.NumberOfWaitGroups = counters.NumberOfWaitGroups - 1
	err = c.setCounters(txn, request.WaitGroupId.AccountId, request.WaitGroupId.NamespaceName, request.WaitGroupId.NamespaceCreatedAt, counters)
	panicIfNotNil(err)

	err = txn.Commit()
	panicIfNotNil(err)

	return &corepb.DeleteWaitGroupResponse{}, nil
}

func (c *WaitGroupsCore) AddJobsToWaitGroup(request *corepb.AddJobsToWaitGroupRequest) (*corepb.AddJobsToWaitGroupResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	waitGroup, err := c.getWaitGroup(txn, request.WaitGroupId)
	if err != nil {
		if errors.Is(err, monstera.ErrNotFound) {
			return nil, monsterax.NewErrorWithContext(
				monsterax.NotFound,
				"wait group not found",
				map[string]string{
					"namespace_name":  request.WaitGroupId.NamespaceName,
					"wait_group_name": request.WaitGroupId.WaitGroupName,
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

	err = c.updateWaitGroup(txn, waitGroup)
	panicIfNotNil(err)

	err = txn.Commit()
	panicIfNotNil(err)

	return &corepb.AddJobsToWaitGroupResponse{
		WaitGroup: waitGroup,
	}, nil
}

func (c *WaitGroupsCore) CompleteJobsFromWaitGroup(request *corepb.CompleteJobsFromWaitGroupRequest) (*corepb.CompleteJobsFromWaitGroupResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	waitGroup, err := c.getWaitGroup(txn, request.WaitGroupId)
	if err != nil {
		if errors.Is(err, monstera.ErrNotFound) {
			return nil, monsterax.NewErrorWithContext(
				monsterax.NotFound,
				"wait group not found",
				map[string]string{
					"namespace_name":  request.WaitGroupId.NamespaceName,
					"wait_group_name": request.WaitGroupId.WaitGroupName,
				})
		} else {
			panic(err)
		}
	}

	for _, processId := range request.ProcessIds {
		waitGroupJobId := &corepb.WaitGroupJobId{
			AccountId:          request.WaitGroupId.AccountId,
			NamespaceName:      request.WaitGroupId.NamespaceName,
			NamespaceCreatedAt: request.WaitGroupId.NamespaceCreatedAt,
			WaitGroupName:      request.WaitGroupId.WaitGroupName,
			ProcessId:          processId,
		}
		_, err := c.getWaitGroupJob(txn, waitGroupJobId)
		if err != nil {
			if errors.Is(err, monstera.ErrNotFound) {
				waitGroupJob := &corepb.WaitGroupJob{
					Id:          waitGroupJobId,
					CompletedAt: request.Now,
				}
				err := c.createWaitGroupJob(txn, waitGroupJob)
				panicIfNotNil(err)

				// Increment counter only if we haven't seen this process_id before
				waitGroup.Completed++
			} else {
				panic(err)
			}
		}
	}

	waitGroup.UpdatedAt = request.Now

	err = c.updateWaitGroup(txn, waitGroup)
	panicIfNotNil(err)

	err = txn.Commit()
	panicIfNotNil(err)

	return &corepb.CompleteJobsFromWaitGroupResponse{
		WaitGroup: waitGroup,
	}, nil
}

func (c *WaitGroupsCore) RunWaitGroupsGarbageCollection(request *corepb.RunWaitGroupsGarbageCollectionRequest) (*corepb.RunWaitGroupsGarbageCollectionResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	totalDeletedObjects := 0

	// List one page of GC records
	gcRecords, err := c.listGCRecords(txn, int(request.GcRecordsPageSize))
	panicIfNotNil(err)

	for _, gcRecord := range gcRecords {
		limit := int(request.MaxDeletedObjects) - totalDeletedObjects
		deletedObjects := 0

		switch r := gcRecord.Record.(type) {
		case *corepb.WaitGroupsGCRecord_NamespaceTimestampedId:
			deletedObjects, err = c.deleteNamespace(txn, r.NamespaceTimestampedId, int(request.GcRecordWaitGroupsPageSize), limit)
			panicIfNotNil(err)

		case *corepb.WaitGroupsGCRecord_WaitGroupId:
			deletedObjects, err = c.deleteWaitGroupJobs(txn, r.WaitGroupId, limit)
			panicIfNotNil(err)
		}

		totalDeletedObjects = totalDeletedObjects + deletedObjects

		// If the number of actually deleted objects is less than the limit, then we have removed everything related to
		// that garbage collection record.
		if deletedObjects < limit {
			// Remove this GC record since it is completed
			err := c.deleteGCRecord(txn, gcRecord)
			panicIfNotNil(err)
		}
	}

	err = txn.Commit()
	panicIfNotNil(err)

	return &corepb.RunWaitGroupsGarbageCollectionResponse{}, nil
}

func (c *WaitGroupsCore) WaitGroupsDeleteNamespace(request *corepb.WaitGroupsDeleteNamespaceRequest) (*corepb.WaitGroupsDeleteNamespaceResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	// Mark the namespace as deleted
	err := c.createGCRecord(txn, &corepb.WaitGroupsGCRecord{
		Id: request.RecordId,
		Record: &corepb.WaitGroupsGCRecord_NamespaceTimestampedId{
			NamespaceTimestampedId: request.NamespaceTimestampedId,
		},
	})
	panicIfNotNil(err)

	err = txn.Commit()
	panicIfNotNil(err)

	return &corepb.WaitGroupsDeleteNamespaceResponse{}, nil
}

func (c *WaitGroupsCore) deleteWaitGroupJobs(txn *monstera.Txn, waitGroupId *corepb.WaitGroupId, waitGroupJobsPageSize int) (int, error) {
	deletedObjects := 0

	// Delete one page of completed jobs
	waitGroupJobsPage, err := c.listWaitGroupJobs(txn, waitGroupId.AccountId, waitGroupId.NamespaceName, waitGroupId.NamespaceCreatedAt, waitGroupId.WaitGroupName, nil, waitGroupJobsPageSize)
	if err != nil {
		return deletedObjects, err
	}
	for _, waitGroupJob := range waitGroupJobsPage.waitGroupJobs {
		err := c.deleteWaitGroupJob(txn, waitGroupJob.Id)
		if err != nil {
			return deletedObjects, err
		}

		deletedObjects++
	}

	// deletedObjects holds the amount of objects that were actually deleted, can be less than waitGroupJobsPageSize.
	return deletedObjects, nil
}

func (c *WaitGroupsCore) deleteNamespace(txn *monstera.Txn, namespaceTimestampedId *corepb.NamespaceTimestampedId, waitGroupsPageSize int, maxDeletedObjects int) (int, error) {
	deletedObjects := 0

	// List one page of wait groups for that namespace
	waitGroupsPage, err := c.listWaitGroups(txn, namespaceTimestampedId.AccountId, namespaceTimestampedId.NamespaceName, namespaceTimestampedId.NamespaceCreatedAt, nil, waitGroupsPageSize)
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
			err = c.deleteExpirationGlobalIndex(txn, waitGroup.ExpiresAt, waitGroup.Id)
			if err != nil {
				return deletedObjects, err
			}
			deletedObjects++

			// Remove from the main table. Will not fail if it was already removed.
			err = c.deleteWaitGroup(txn, waitGroup.Id)

			if err != nil {
				return deletedObjects, err
			}
			deletedObjects++
		}
	}

	// Delete counters for that namespace.
	err = c.deleteCounters(txn, namespaceTimestampedId.AccountId, namespaceTimestampedId.NamespaceName, namespaceTimestampedId.NamespaceCreatedAt)
	if err != nil {
		return deletedObjects, err
	}
	deletedObjects++

	return deletedObjects, nil
}

func (c *WaitGroupsCore) getWaitGroup(txn *monstera.Txn, waitGroupId *corepb.WaitGroupId) (*corepb.WaitGroup, error) {
	return c.waitGroupsTable.Get(txn, waitGroupsTablePK(waitGroupId.AccountId, waitGroupId.NamespaceName, waitGroupId.NamespaceCreatedAt), waitGroupsTableSK(waitGroupId.WaitGroupName))
}

func (c *WaitGroupsCore) updateWaitGroup(txn *monstera.Txn, waitGroup *corepb.WaitGroup) error {
	return c.waitGroupsTable.Set(txn, waitGroupsTablePK(waitGroup.Id.AccountId, waitGroup.Id.NamespaceName, waitGroup.Id.NamespaceCreatedAt), waitGroupsTableSK(waitGroup.Id.WaitGroupName), waitGroup)
}

func (c *WaitGroupsCore) deleteWaitGroup(txn *monstera.Txn, waitGroupId *corepb.WaitGroupId) error {
	return c.waitGroupsTable.Delete(txn, waitGroupsTablePK(waitGroupId.AccountId, waitGroupId.NamespaceName, waitGroupId.NamespaceCreatedAt), waitGroupsTableSK(waitGroupId.WaitGroupName))
}

func (c *WaitGroupsCore) createWaitGroup(txn *monstera.Txn, waitGroup *corepb.WaitGroup) error {
	return c.waitGroupsTable.Set(txn, waitGroupsTablePK(waitGroup.Id.AccountId, waitGroup.Id.NamespaceName, waitGroup.Id.NamespaceCreatedAt), waitGroupsTableSK(waitGroup.Id.WaitGroupName), waitGroup)
}

type listWaitGroupsResult struct {
	waitGroups              []*corepb.WaitGroup
	nextPaginationToken     *corepb.PaginationToken
	previousPaginationToken *corepb.PaginationToken
}

func (c *WaitGroupsCore) listWaitGroups(txn *monstera.Txn, accountId uint64, namespaceName string, namespaceCreatedAt int64, paginationToken *corepb.PaginationToken, limit int) (*listWaitGroupsResult, error) {
	result, err := c.waitGroupsTable.ListPaginated(txn, waitGroupsTablePK(accountId, namespaceName, namespaceCreatedAt), paginationTokenToMonstera(paginationToken), limit)
	if err != nil {
		return nil, err
	}

	return &listWaitGroupsResult{
		waitGroups:              result.Items,
		nextPaginationToken:     monsteraPaginationTokenToCore(result.NextPaginationToken),
		previousPaginationToken: monsteraPaginationTokenToCore(result.PreviousPaginationToken),
	}, nil
}

type listWaitGroupJobsResult struct {
	waitGroupJobs           []*corepb.WaitGroupJob
	nextPaginationToken     *corepb.PaginationToken
	previousPaginationToken *corepb.PaginationToken
}

func (c *WaitGroupsCore) listWaitGroupJobs(txn *monstera.Txn, accountId uint64, namespaceName string, namespaceCreatedAt int64, waitGroupName string, paginationToken *corepb.PaginationToken, limit int) (*listWaitGroupJobsResult, error) {
	result, err := c.waitGroupJobsTable.ListPaginated(txn, waitGroupJobsTablePK(accountId, namespaceName, namespaceCreatedAt, waitGroupName), paginationTokenToMonstera(paginationToken), limit)
	if err != nil {
		return nil, err
	}

	return &listWaitGroupJobsResult{
		waitGroupJobs:           result.Items,
		nextPaginationToken:     monsteraPaginationTokenToCore(result.NextPaginationToken),
		previousPaginationToken: monsteraPaginationTokenToCore(result.PreviousPaginationToken),
	}, nil
}

func (c *WaitGroupsCore) getWaitGroupJob(txn *monstera.Txn, waitGroupJobId *corepb.WaitGroupJobId) (*corepb.WaitGroupJob, error) {
	return c.waitGroupJobsTable.Get(txn, waitGroupJobsTablePK(waitGroupJobId.AccountId, waitGroupJobId.NamespaceName,
		waitGroupJobId.NamespaceCreatedAt, waitGroupJobId.WaitGroupName), waitGroupJobsTableSK(waitGroupJobId.ProcessId))
}

func (c *WaitGroupsCore) createWaitGroupJob(txn *monstera.Txn, waitGroupJob *corepb.WaitGroupJob) error {
	return c.waitGroupJobsTable.Set(txn, waitGroupJobsTablePK(waitGroupJob.Id.AccountId, waitGroupJob.Id.NamespaceName,
		waitGroupJob.Id.NamespaceCreatedAt, waitGroupJob.Id.WaitGroupName), waitGroupJobsTableSK(waitGroupJob.Id.ProcessId), waitGroupJob)
}

func (c *WaitGroupsCore) deleteWaitGroupJob(txn *monstera.Txn, waitGroupJobId *corepb.WaitGroupJobId) error {
	return c.waitGroupJobsTable.Delete(txn, waitGroupJobsTablePK(waitGroupJobId.AccountId, waitGroupJobId.NamespaceName,
		waitGroupJobId.NamespaceCreatedAt, waitGroupJobId.WaitGroupName), waitGroupJobsTableSK(waitGroupJobId.ProcessId))
}

func (c *WaitGroupsCore) getCounters(txn *monstera.Txn, accountId uint64, namespaceName string, namespaceCreatedAt int64) (*corepb.WaitGroupsCounter, error) {
	countres, err := c.waitGroupsCountersTable.Get(txn, waitGroupsCountersTablePK(accountId, namespaceName, namespaceCreatedAt))
	if err != nil {
		if errors.Is(err, monstera.ErrNotFound) {
			return &corepb.WaitGroupsCounter{
				NamespaceTimestampedId: &corepb.NamespaceTimestampedId{
					AccountId:          accountId,
					NamespaceName:      namespaceName,
					NamespaceCreatedAt: namespaceCreatedAt,
				},
				NumberOfWaitGroups: 0,
			}, nil
		}
		return nil, err
	}
	return countres, nil
}

func (c *WaitGroupsCore) setCounters(txn *monstera.Txn, accountId uint64, namespaceName string, namespaceCreatedAt int64, counters *corepb.WaitGroupsCounter) error {
	return c.waitGroupsCountersTable.Set(txn, waitGroupsCountersTablePK(accountId, namespaceName, namespaceCreatedAt), counters)
}

func (c *WaitGroupsCore) deleteCounters(txn *monstera.Txn, accountId uint64, namespaceName string, namespaceCreatedAt int64) error {
	return c.waitGroupsCountersTable.Delete(txn, waitGroupsCountersTablePK(accountId, namespaceName, namespaceCreatedAt))
}

func (c *WaitGroupsCore) createGCRecord(txn *monstera.Txn, gcRecord *corepb.WaitGroupsGCRecord) error {
	return c.gcRecordsGlobalIndex.Set(txn, waitGroupsGCRecordsGlobalIndexPK(c.shardGlobalIndexPrefix, gcRecord.Id), gcRecord)
}

func (c *WaitGroupsCore) deleteGCRecord(txn *monstera.Txn, gcRecord *corepb.WaitGroupsGCRecord) error {
	return c.gcRecordsGlobalIndex.Delete(txn, waitGroupsGCRecordsGlobalIndexPK(c.shardGlobalIndexPrefix, gcRecord.Id))
}

func (c *WaitGroupsCore) listGCRecords(txn *monstera.Txn, limit int) ([]*corepb.WaitGroupsGCRecord, error) {
	result, err := c.gcRecordsGlobalIndex.ListPaginated(txn, nil, limit)
	if err != nil {
		return nil, err
	}
	return result.Items, nil
}

func (c *WaitGroupsCore) deleteExpirationGlobalIndex(txn *monstera.Txn, expiresAt int64, waitGroupId *corepb.WaitGroupId) error {
	return c.expirationGlobalIndex.Delete(txn, waitGroupsExpirationGlobalIndexPK(c.shardGlobalIndexPrefix, expiresAt,
		waitGroupId.AccountId, waitGroupId.NamespaceName, waitGroupId.NamespaceCreatedAt, waitGroupId.WaitGroupName))
}

func (c *WaitGroupsCore) addExpirationGlobalIndex(txn *monstera.Txn, expiresAt int64, waitGroupId *corepb.WaitGroupId) error {
	return c.expirationGlobalIndex.Set(txn, waitGroupsExpirationGlobalIndexPK(c.shardGlobalIndexPrefix, expiresAt,
		waitGroupId.AccountId, waitGroupId.NamespaceName, waitGroupId.NamespaceCreatedAt, waitGroupId.WaitGroupName),
		&corepb.WaitGroupsExpirationGlobalIndexRecord{
			ExpiresAt:   expiresAt,
			WaitGroupId: waitGroupId,
		})
}

// 1. shard key (by account id and namespace name)
// 2. account id
// 3. namespace name
// 4. namespace created at
func waitGroupsTablePK(accountId uint64, namespaceName string, namespaceCreatedAt int64) []byte {
	return monstera.ConcatBytes(
		shardByAccountAndNamespace(accountId, namespaceName),
		accountId,
		namespaceName,
		namespaceCreatedAt,
	)
}

// 1. wait group name
func waitGroupsTableSK(waitGroupName string) []byte {
	return monstera.ConcatBytes(
		waitGroupName,
	)
}

// 1. shard key (by account id and namespace name)
// 2. account id
// 3. namespace name
// 4. namespace created at
// 5. wait group name
func waitGroupJobsTablePK(accountId uint64, namespaceName string, namespaceCreatedAt int64, waitGroupName string) []byte {
	return monstera.ConcatBytes(
		shardByAccountAndNamespace(accountId, namespaceName),
		accountId,
		namespaceName,
		namespaceCreatedAt,
		waitGroupName,
	)
}

// 1. process id
func waitGroupJobsTableSK(processId string) []byte {
	return monstera.ConcatBytes(
		processId,
	)
}

// 1. shard key (by account id and namespace name)
// 2. account id
// 3. namespace name
// 4. namespace created at
func waitGroupsCountersTablePK(accountId uint64, namespaceName string, namespaceCreatedAt int64) []byte {
	return monstera.ConcatBytes(
		shardByAccountAndNamespace(accountId, namespaceName),
		accountId,
		namespaceName,
		namespaceCreatedAt,
	)
}

// 1. shard id
// 2. gc record id
func waitGroupsGCRecordsGlobalIndexPK(shardId []byte, gcRecordId uint64) []byte {
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
// 6. wait group name
func waitGroupsExpirationGlobalIndexPK(shardId []byte, time int64, accountId uint64, namespaceName string, namespaceCreatedAt int64, waitGroupName string) []byte {
	return monstera.ConcatBytes(
		shardId,
		time,
		accountId,
		namespaceName,
		namespaceCreatedAt,
		waitGroupName,
	)
}

// 1. shard id
// 2. timestamp
func waitGroupsExpirationGlobalIndexPrefix(shardId []byte, time int64) []byte {
	return monstera.ConcatBytes(
		shardId,
		time,
	)
}
