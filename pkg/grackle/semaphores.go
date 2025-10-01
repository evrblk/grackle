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

type SemaphoresCore struct {
	badgerStore            *monstera.BadgerStore
	shardGlobalIndexPrefix []byte

	semaphoresTable         *monsterax.CompositeKeyTable[*corepb.Semaphore, corepb.Semaphore]
	semaphoresCountersTable *monsterax.SimpleKeyTable[*corepb.SemaphoresCounter, corepb.SemaphoresCounter]
	gcRecordsGlobalIndex    *monsterax.SimpleKeyTable[*corepb.SemaphoresGCRecord, corepb.SemaphoresGCRecord]                                       // Global index
	expirationGlobalIndex   *monsterax.SimpleKeyTable[*corepb.SemaphoresExpirationGlobalIndexRecord, corepb.SemaphoresExpirationGlobalIndexRecord] // Global index
}

var _ GrackleSemaphoresCoreApi = &SemaphoresCore{}

func NewSemaphoresCore(badgerStore *monstera.BadgerStore, shardGlobalIndexPrefix []byte, shardLowerBound []byte, shardUpperBound []byte) *SemaphoresCore {
	return &SemaphoresCore{
		badgerStore:            badgerStore,
		shardGlobalIndexPrefix: shardGlobalIndexPrefix,

		semaphoresTable:         monsterax.NewCompositeKeyTable[*corepb.Semaphore, corepb.Semaphore](GrackleSemaphoresTableId, shardLowerBound, shardUpperBound),
		semaphoresCountersTable: monsterax.NewSimpleKeyTable[*corepb.SemaphoresCounter, corepb.SemaphoresCounter](GrackleSemaphoresCountersTableId, shardLowerBound, shardUpperBound),
		gcRecordsGlobalIndex:    monsterax.NewSimpleKeyTable[*corepb.SemaphoresGCRecord, corepb.SemaphoresGCRecord](GrackleSemaphoresGCRecordsGlobalIndexId, shardGlobalIndexPrefix, shardGlobalIndexPrefix),
		expirationGlobalIndex:   monsterax.NewSimpleKeyTable[*corepb.SemaphoresExpirationGlobalIndexRecord, corepb.SemaphoresExpirationGlobalIndexRecord](GrackleSemaphoresExpirationGlobalIndexId, shardGlobalIndexPrefix, shardGlobalIndexPrefix),
	}
}

func (c *SemaphoresCore) ranges() []monstera.KeyRange {
	return []monstera.KeyRange{
		c.semaphoresTable.GetTableKeyRange(),
		c.semaphoresCountersTable.GetTableKeyRange(),
		c.gcRecordsGlobalIndex.GetTableKeyRange(),
		c.expirationGlobalIndex.GetTableKeyRange(),
	}
}

func (c *SemaphoresCore) Snapshot() monstera.ApplicationCoreSnapshot {
	return monsterax.Snapshot(c.badgerStore, c.ranges())
}

func (c *SemaphoresCore) Restore(reader io.ReadCloser) error {
	return monsterax.Restore(c.badgerStore, c.ranges(), reader)
}

func (c *SemaphoresCore) Close() {

}

func (c *SemaphoresCore) CreateSemaphore(request *corepb.CreateSemaphoreRequest) (*corepb.CreateSemaphoreResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	semaphoreId := &corepb.SemaphoreId{
		AccountId:          request.NamespaceTimestampedId.AccountId,
		NamespaceName:      request.NamespaceTimestampedId.NamespaceName,
		NamespaceCreatedAt: request.NamespaceTimestampedId.NamespaceCreatedAt,
		SemaphoreName:      request.Name,
	}

	// Get counters for that namespace
	counters, err := c.getCounters(txn, request.NamespaceTimestampedId.AccountId, request.NamespaceTimestampedId.NamespaceName, request.NamespaceTimestampedId.NamespaceCreatedAt)
	panicIfNotNil(err)

	// Checking name uniqueness
	_, err = c.getSemaphore(txn, semaphoreId)
	if err != nil {
		if !errors.Is(err, monstera.ErrNotFound) {
			return nil, err
		}
	} else {
		return nil, monsterax.NewErrorWithContext(
			monsterax.AlreadyExists,
			"semaphore with this name already exists",
			map[string]string{
				"namespace_name": semaphoreId.NamespaceName,
				"semaphore_name": semaphoreId.SemaphoreName,
			})
	}

	// Checking max number of semaphores
	if counters.NumberOfSemaphores >= request.MaxNumberOfSemaphoresPerNamespace {
		return nil, monsterax.NewErrorWithContext(
			monsterax.ResourceExhausted,
			"max number of semaphores per namespace reached",
			map[string]string{"limit": fmt.Sprintf("%d", request.MaxNumberOfSemaphoresPerNamespace)})
	}

	semaphore := &corepb.Semaphore{
		Id:          semaphoreId,
		Description: request.Description,
		Permits:     request.Permits,
		CreatedAt:   request.Now,
		UpdatedAt:   request.Now,
	}

	err = c.createSemaphore(txn, semaphore)
	panicIfNotNil(err)

	// Update counters
	counters.NumberOfSemaphores = counters.NumberOfSemaphores + 1
	err = c.setCounters(txn, request.NamespaceTimestampedId.AccountId, request.NamespaceTimestampedId.NamespaceName, request.NamespaceTimestampedId.NamespaceCreatedAt, counters)
	panicIfNotNil(err)

	err = txn.Commit()
	panicIfNotNil(err)

	return &corepb.CreateSemaphoreResponse{
		Semaphore: semaphore,
	}, nil
}

func (c *SemaphoresCore) UpdateSemaphore(request *corepb.UpdateSemaphoreRequest) (*corepb.UpdateSemaphoreResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	semaphore, err := c.getSemaphore(txn, request.SemaphoreId)
	if err != nil {
		if errors.Is(err, monstera.ErrNotFound) {
			return nil, monsterax.NewErrorWithContext(
				monsterax.NotFound,
				"semaphore not found",
				map[string]string{
					"namespace_name": request.SemaphoreId.NamespaceName,
					"semaphore_name": request.SemaphoreId.SemaphoreName,
				})
		} else {
			panic(err)
		}
	}

	// Check expiration
	semaphore = c.checkSemaphoreExpiration(semaphore, request.Now)

	// If there are currently more holders than the new amount of permits
	if uint64(len(semaphore.SemaphoreHolders)) > request.Permits {
		return nil, monsterax.NewErrorWithContext(
			monsterax.InvalidArgument,
			"there are currently more holders than the new amount of permits",
			map[string]string{})
	}

	semaphore.Description = request.Description
	semaphore.Permits = request.Permits
	semaphore.UpdatedAt = request.Now

	err = c.updateSemaphore(txn, semaphore)
	panicIfNotNil(err)

	err = txn.Commit()
	panicIfNotNil(err)

	return &corepb.UpdateSemaphoreResponse{
		Semaphore: semaphore,
	}, nil
}

func (c *SemaphoresCore) DeleteSemaphore(request *corepb.DeleteSemaphoreRequest) (*corepb.DeleteSemaphoreResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	semaphore, err := c.getSemaphore(txn, request.SemaphoreId)
	if err != nil {
		if errors.Is(err, monstera.ErrNotFound) {
			// No semaphore exists, do nothing
			return &corepb.DeleteSemaphoreResponse{}, nil
		} else {
			panic(err)
		}
	}

	// Get counters for this namespace
	counters, err := c.getCounters(txn, request.SemaphoreId.AccountId, request.SemaphoreId.NamespaceName, request.SemaphoreId.NamespaceCreatedAt)
	panicIfNotNil(err)

	err = c.deleteSemaphore(txn, semaphore.Id)
	panicIfNotNil(err)

	// Update counters
	counters.NumberOfSemaphores = counters.NumberOfSemaphores - 1
	err = c.setCounters(txn, request.SemaphoreId.AccountId, request.SemaphoreId.NamespaceName, request.SemaphoreId.NamespaceCreatedAt, counters)
	panicIfNotNil(err)

	err = txn.Commit()
	panicIfNotNil(err)

	return &corepb.DeleteSemaphoreResponse{}, nil
}

func (c *SemaphoresCore) GetSemaphore(request *corepb.GetSemaphoreRequest) (*corepb.GetSemaphoreResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	semaphore, err := c.getSemaphore(txn, request.SemaphoreId)
	if err != nil {
		if errors.Is(err, monstera.ErrNotFound) {
			return nil, monsterax.NewErrorWithContext(
				monsterax.NotFound,
				"semaphore not found",
				map[string]string{
					"namespace_name": request.SemaphoreId.NamespaceName,
					"semaphore_name": request.SemaphoreId.SemaphoreName,
				})
		} else {
			panic(err)
		}
	}

	// Check expiration
	semaphore = c.checkSemaphoreExpiration(semaphore, request.Now)

	err = c.updateSemaphore(txn, semaphore)
	panicIfNotNil(err)

	err = txn.Commit()
	panicIfNotNil(err)

	return &corepb.GetSemaphoreResponse{
		Semaphore: semaphore,
	}, nil
}

func (c *SemaphoresCore) ListSemaphores(request *corepb.ListSemaphoresRequest) (*corepb.ListSemaphoresResponse, error) {
	txn := c.badgerStore.View()
	defer txn.Discard()

	result, err := c.listSemaphores(txn, request.NamespaceTimestampedId.AccountId, request.NamespaceTimestampedId.NamespaceName, request.NamespaceTimestampedId.NamespaceCreatedAt, request.PaginationToken, getLimit(int(request.Limit)))
	panicIfNotNil(err)

	return &corepb.ListSemaphoresResponse{
		Semaphores:              result.semaphores,
		NextPaginationToken:     result.nextPaginationToken,
		PreviousPaginationToken: result.previousPaginationToken,
	}, nil
}

func (c *SemaphoresCore) AcquireSemaphore(request *corepb.AcquireSemaphoreRequest) (*corepb.AcquireSemaphoreResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	semaphore, err := c.getSemaphore(txn, request.SemaphoreId)
	if err != nil {
		if errors.Is(err, monstera.ErrNotFound) {
			return nil, monsterax.NewErrorWithContext(
				monsterax.NotFound,
				"semaphore not found",
				map[string]string{
					"namespace_name": request.SemaphoreId.NamespaceName,
					"semaphore_name": request.SemaphoreId.SemaphoreName,
				})
		} else {
			panic(err)
		}
	}

	// Check expiration
	updatedSemaphore := c.checkSemaphoreExpiration(semaphore, request.Now)

	success := false

	// Check if the same process_id already holds the semaphore here.
	existingHolder, ok := lo.Find(updatedSemaphore.SemaphoreHolders, func(h *corepb.SemaphoreHolder) bool {
		return h.ProcessId == request.ProcessId
	})
	if ok {
		// Update expiration time (extend lock)
		existingHolder.ExpiresAt = request.ExpiresAt
		existingHolder.LockedAt = request.Now
		success = true
	} else {
		// Check if there are enough permits
		if updatedSemaphore.Permits > uint64(len(updatedSemaphore.SemaphoreHolders)) {
			// Add a new lock holder
			updatedSemaphore.SemaphoreHolders = append(updatedSemaphore.SemaphoreHolders, &corepb.SemaphoreHolder{
				ProcessId: request.ProcessId,
				ExpiresAt: request.ExpiresAt,
				LockedAt:  request.Now,
			})
			success = true
		}
	}

	oldExpiresAt := c.getEarliestExpiration(semaphore)
	newExpiresAt := c.getEarliestExpiration(updatedSemaphore)

	if oldExpiresAt != newExpiresAt {
		// Remove a semaphore from expirationGlobalIndex at old position
		err = c.deleteExpirationGlobalIndex(txn, oldExpiresAt, semaphore.Id)
		panicIfNotNil(err)

		if newExpiresAt != 0 {
			// Add a semaphore into expirationGlobalIndex at new position
			err = c.addExpirationGlobalIndex(txn, newExpiresAt, semaphore.Id)
			panicIfNotNil(err)
		}
	}

	err = c.updateSemaphore(txn, updatedSemaphore)
	panicIfNotNil(err)

	err = txn.Commit()
	panicIfNotNil(err)

	return &corepb.AcquireSemaphoreResponse{
		Semaphore: updatedSemaphore,
		Success:   success,
	}, nil
}

func (c *SemaphoresCore) ReleaseSemaphore(request *corepb.ReleaseSemaphoreRequest) (*corepb.ReleaseSemaphoreResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	semaphore, err := c.getSemaphore(txn, request.SemaphoreId)
	if err != nil {
		if errors.Is(err, monstera.ErrNotFound) {
			return nil, monsterax.NewErrorWithContext(
				monsterax.NotFound,
				"semaphore not found",
				map[string]string{
					"namespace_name": request.SemaphoreId.NamespaceName,
					"semaphore_name": request.SemaphoreId.SemaphoreName,
				})
		} else {
			panic(err)
		}
	}

	// Check expiration
	updatedSemaphore := c.checkSemaphoreExpiration(semaphore, request.Now)

	// Remove the holder by process_id (if exists)
	updatedSemaphore.SemaphoreHolders = lo.Filter(updatedSemaphore.SemaphoreHolders, func(h *corepb.SemaphoreHolder, _ int) bool {
		return h.ProcessId != request.ProcessId
	})

	oldExpiresAt := c.getEarliestExpiration(semaphore)
	newExpiresAt := c.getEarliestExpiration(updatedSemaphore)

	if oldExpiresAt != newExpiresAt {
		// Remove a semaphore from expirationGlobalIndex at old position
		err = c.deleteExpirationGlobalIndex(txn, oldExpiresAt, semaphore.Id)
		panicIfNotNil(err)

		if newExpiresAt != 0 {
			// Add a semaphore into expirationGlobalIndex at new position
			err = c.addExpirationGlobalIndex(txn, newExpiresAt, semaphore.Id)
			panicIfNotNil(err)
		}
	}

	err = c.updateSemaphore(txn, updatedSemaphore)
	panicIfNotNil(err)

	err = txn.Commit()
	panicIfNotNil(err)

	return &corepb.ReleaseSemaphoreResponse{
		Semaphore: updatedSemaphore,
	}, nil
}

func (c *SemaphoresCore) RunSemaphoresGarbageCollection(request *corepb.RunSemaphoresGarbageCollectionRequest) (*corepb.RunSemaphoresGarbageCollectionResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	visitedSemaphores := int64(0)

	// List one page of GC records
	gcRecords, err := c.listGCRecords(txn, int(request.GcRecordsPageSize))
	panicIfNotNil(err)

	for _, gcRecord := range gcRecords {
		// Delete counters for that namespace. Will not fail if counters do not exist.
		err := c.deleteCounters(txn, gcRecord.NamespaceTimestampedId.AccountId, gcRecord.NamespaceTimestampedId.NamespaceName, gcRecord.NamespaceTimestampedId.NamespaceCreatedAt)
		panicIfNotNil(err)

		// List one page of semaphores for that namespace
		result, err := c.listSemaphores(txn, gcRecord.NamespaceTimestampedId.AccountId, gcRecord.NamespaceTimestampedId.NamespaceName, gcRecord.NamespaceTimestampedId.NamespaceCreatedAt, nil, int(request.GcRecordSemaphoresPageSize))
		panicIfNotNil(err)

		// Delete those semaphores
		for _, semaphore := range result.semaphores {
			visitedSemaphores++

			// Remove a semaphore from expirationGlobalIndex
			expiresAt := c.getEarliestExpiration(semaphore)
			err = c.deleteExpirationGlobalIndex(txn, expiresAt, semaphore.Id)
			panicIfNotNil(err)

			// Remove from the main table
			err := c.deleteSemaphore(txn, semaphore.Id)
			panicIfNotNil(err)

			if visitedSemaphores >= request.MaxVisitedSemaphores {
				goto commit
			}
		}

		// Delete the deleted namespace if that was the last page of locks
		if result.nextPaginationToken == nil {
			err := c.deleteGCRecord(txn, gcRecord)
			panicIfNotNil(err)
		}
	}

	if visitedSemaphores < request.MaxVisitedSemaphores {
		// Update semaphores with expired holders
		leftBound := semaphoresExpirationGlobalIndexPrefix(c.shardGlobalIndexPrefix, 0)
		rightBound := semaphoresExpirationGlobalIndexPrefix(c.shardGlobalIndexPrefix, request.Now)
		err = c.expirationGlobalIndex.ListInRange(txn, leftBound, rightBound, false, func(record *corepb.SemaphoresExpirationGlobalIndexRecord) (bool, error) {
			visitedSemaphores++

			// Get the semaphore
			semaphore, err := c.getSemaphore(txn, record.SemaphoreId)
			panicIfNotNil(err)

			updatedSemaphore := c.checkSemaphoreExpiration(semaphore, request.Now)

			oldExpiresAt := c.getEarliestExpiration(semaphore)
			newExpiresAt := c.getEarliestExpiration(updatedSemaphore)

			if oldExpiresAt != newExpiresAt {
				// Remove a semaphore from expirationGlobalIndex at old position
				err = c.deleteExpirationGlobalIndex(txn, oldExpiresAt, semaphore.Id)
				panicIfNotNil(err)

				// If semaphore still has holders it will have non zero expiration time
				if newExpiresAt != 0 {
					// Add a semaphore into expirationGlobalIndex at new position
					err = c.addExpirationGlobalIndex(txn, newExpiresAt, semaphore.Id)
					panicIfNotNil(err)
				}
			} else {
				// This should not happen
				panic("oldExpiresAt == newExpiresAt")
			}

			err = c.updateSemaphore(txn, updatedSemaphore)
			panicIfNotNil(err)

			// Stop if we have visited enough locks
			return visitedSemaphores < request.MaxVisitedSemaphores, nil
		})
	}

commit:

	err = txn.Commit()
	panicIfNotNil(err)

	return &corepb.RunSemaphoresGarbageCollectionResponse{}, nil
}

func (c *SemaphoresCore) SemaphoresDeleteNamespace(request *corepb.SemaphoresDeleteNamespaceRequest) (*corepb.SemaphoresDeleteNamespaceResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	// Mark the namespace as deleted
	err := c.createGCRecord(txn, &corepb.SemaphoresGCRecord{
		Id:                     request.RecordId,
		NamespaceTimestampedId: request.NamespaceTimestampedId,
	})
	panicIfNotNil(err)

	err = txn.Commit()
	panicIfNotNil(err)

	return &corepb.SemaphoresDeleteNamespaceResponse{}, nil
}

// checkSemaphoreExpiration ensures that the semaphore is still held at the moment `now`. Returns an updated copy of the semaphore.
func (c *SemaphoresCore) checkSemaphoreExpiration(semaphore *corepb.Semaphore, now int64) *corepb.Semaphore {
	result := proto.Clone(semaphore).(*corepb.Semaphore)

	result.SemaphoreHolders = lo.Filter(result.SemaphoreHolders, func(h *corepb.SemaphoreHolder, _ int) bool {
		return h.ExpiresAt > now
	})

	return result
}

func (c *SemaphoresCore) getEarliestExpiration(semaphore *corepb.Semaphore) int64 {
	expiresAt := int64(0)

	// Find the earliest expiration among all semaphore holders
	for _, h := range semaphore.SemaphoreHolders {
		if expiresAt == 0 || h.ExpiresAt < expiresAt {
			expiresAt = h.ExpiresAt
		}
	}

	return expiresAt
}

func (c *SemaphoresCore) getSemaphore(txn *monstera.Txn, semaphoreId *corepb.SemaphoreId) (*corepb.Semaphore, error) {
	return c.semaphoresTable.Get(txn, semaphoresTablePK(semaphoreId.AccountId, semaphoreId.NamespaceName, semaphoreId.NamespaceCreatedAt), semaphoresTableSK(semaphoreId.SemaphoreName))
}

func (c *SemaphoresCore) updateSemaphore(txn *monstera.Txn, semaphore *corepb.Semaphore) error {
	return c.semaphoresTable.Set(txn, semaphoresTablePK(semaphore.Id.AccountId, semaphore.Id.NamespaceName, semaphore.Id.NamespaceCreatedAt), semaphoresTableSK(semaphore.Id.SemaphoreName), semaphore)
}

func (c *SemaphoresCore) deleteSemaphore(txn *monstera.Txn, semaphoreId *corepb.SemaphoreId) error {
	return c.semaphoresTable.Delete(txn, semaphoresTablePK(semaphoreId.AccountId, semaphoreId.NamespaceName, semaphoreId.NamespaceCreatedAt), semaphoresTableSK(semaphoreId.SemaphoreName))
}

func (c *SemaphoresCore) createSemaphore(txn *monstera.Txn, semaphore *corepb.Semaphore) error {
	return c.semaphoresTable.Set(txn, semaphoresTablePK(semaphore.Id.AccountId, semaphore.Id.NamespaceName, semaphore.Id.NamespaceCreatedAt), semaphoresTableSK(semaphore.Id.SemaphoreName), semaphore)
}

type listSemaphoresResult struct {
	semaphores              []*corepb.Semaphore
	nextPaginationToken     *corepb.PaginationToken
	previousPaginationToken *corepb.PaginationToken
}

func (c *SemaphoresCore) listSemaphores(txn *monstera.Txn, accountId uint64, namespaceName string, namespaceCreatedAt int64, paginationToken *corepb.PaginationToken, limit int) (*listSemaphoresResult, error) {
	result, err := c.semaphoresTable.ListPaginated(txn, semaphoresTablePK(accountId, namespaceName, namespaceCreatedAt), paginationTokenToMonstera(paginationToken), limit)
	if err != nil {
		return nil, err
	}

	return &listSemaphoresResult{
		semaphores:              result.Items,
		nextPaginationToken:     monsteraPaginationTokenToCore(result.NextPaginationToken),
		previousPaginationToken: monsteraPaginationTokenToCore(result.PreviousPaginationToken),
	}, nil
}

func (c *SemaphoresCore) getCounters(txn *monstera.Txn, accountId uint64, namespaceName string, namespaceCreatedAt int64) (*corepb.SemaphoresCounter, error) {
	countres, err := c.semaphoresCountersTable.Get(txn, semaphoresCountersTablePK(accountId, namespaceName, namespaceCreatedAt))
	if err != nil {
		if errors.Is(err, monstera.ErrNotFound) {
			return &corepb.SemaphoresCounter{
				NamespaceTimestampedId: &corepb.NamespaceTimestampedId{
					AccountId:          accountId,
					NamespaceName:      namespaceName,
					NamespaceCreatedAt: namespaceCreatedAt,
				},
				NumberOfSemaphores: 0,
			}, nil
		}
		return nil, err
	}
	return countres, nil
}

func (c *SemaphoresCore) setCounters(txn *monstera.Txn, accountId uint64, namespaceName string, namespaceCreatedAt int64, counters *corepb.SemaphoresCounter) error {
	return c.semaphoresCountersTable.Set(txn, semaphoresCountersTablePK(accountId, namespaceName, namespaceCreatedAt), counters)
}

func (c *SemaphoresCore) deleteCounters(txn *monstera.Txn, accountId uint64, namespaceName string, namespaceCreatedAt int64) error {
	return c.semaphoresCountersTable.Delete(txn, semaphoresCountersTablePK(accountId, namespaceName, namespaceCreatedAt))
}

func (c *SemaphoresCore) createGCRecord(txn *monstera.Txn, gcRecord *corepb.SemaphoresGCRecord) error {
	return c.gcRecordsGlobalIndex.Set(txn, semaphoresGCRecordsGlobalIndexPK(c.shardGlobalIndexPrefix, gcRecord.Id), gcRecord)
}

func (c *SemaphoresCore) deleteGCRecord(txn *monstera.Txn, gcRecord *corepb.SemaphoresGCRecord) error {
	return c.gcRecordsGlobalIndex.Delete(txn, semaphoresGCRecordsGlobalIndexPK(c.shardGlobalIndexPrefix, gcRecord.Id))
}

func (c *SemaphoresCore) listGCRecords(txn *monstera.Txn, limit int) ([]*corepb.SemaphoresGCRecord, error) {
	result, err := c.gcRecordsGlobalIndex.ListPaginated(txn, nil, limit)
	if err != nil {
		return nil, err
	}
	return result.Items, nil
}

func (c *SemaphoresCore) deleteExpirationGlobalIndex(txn *monstera.Txn, expiresAt int64, semaphoreId *corepb.SemaphoreId) error {
	return c.expirationGlobalIndex.Delete(txn,
		semaphoresExpirationGlobalIndexPK(c.shardGlobalIndexPrefix, expiresAt, semaphoreId.AccountId, semaphoreId.NamespaceName, semaphoreId.NamespaceCreatedAt, semaphoreId.SemaphoreName))
}

func (c *SemaphoresCore) addExpirationGlobalIndex(txn *monstera.Txn, expiresAt int64, semaphoreId *corepb.SemaphoreId) error {
	return c.expirationGlobalIndex.Set(txn,
		semaphoresExpirationGlobalIndexPK(c.shardGlobalIndexPrefix, expiresAt, semaphoreId.AccountId, semaphoreId.NamespaceName, semaphoreId.NamespaceCreatedAt, semaphoreId.SemaphoreName),
		&corepb.SemaphoresExpirationGlobalIndexRecord{
			ExpiresAt:   expiresAt,
			SemaphoreId: semaphoreId,
		},
	)
}

// 1. shard key (by account id and namespace name)
// 2. account id
// 3. namespace name
// 4. namespace created at
func semaphoresTablePK(accountId uint64, namespaceName string, namespaceCreatedAt int64) []byte {
	return monstera.ConcatBytes(
		shardByAccountAndNamespace(accountId, namespaceName),
		accountId,
		namespaceName,
		namespaceCreatedAt,
	)
}

// 1. semaphore name
func semaphoresTableSK(semaphoreName string) []byte {
	return monstera.ConcatBytes(
		semaphoreName,
	)
}

// 1. shard key (by account id and namespace name)
// 2. account id
// 3. namespace name
// 4. namespace created at
func semaphoresCountersTablePK(accountId uint64, namespaceName string, namespaceCreatedAt int64) []byte {
	return monstera.ConcatBytes(
		shardByAccountAndNamespace(accountId, namespaceName),
		accountId,
		namespaceName,
		namespaceCreatedAt,
	)
}

// 1. shard id
// 2. gc record id
func semaphoresGCRecordsGlobalIndexPK(shardId []byte, gcRecordId uint64) []byte {
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
// 6. semaphore name
func semaphoresExpirationGlobalIndexPK(shardId []byte, time int64, accountId uint64, namespaceName string, namespaceCreatedAt int64, semaphoreName string) []byte {
	return monstera.ConcatBytes(
		shardId,
		time,
		accountId,
		namespaceName,
		namespaceCreatedAt,
		semaphoreName,
	)
}

// 1. shard id
// 2. timestamp
func semaphoresExpirationGlobalIndexPrefix(shardId []byte, time int64) []byte {
	return monstera.ConcatBytes(
		shardId,
		time,
	)
}
