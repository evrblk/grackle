package semaphores

import (
	"errors"

	"github.com/evrblk/monstera/store"
	"github.com/evrblk/monstera/utils"
	monsterax "github.com/evrblk/monstera/x"

	"github.com/evrblk/grackle/pkg/corepb"
	"github.com/evrblk/grackle/pkg/pagination"
	"github.com/evrblk/grackle/pkg/sharding"
	"github.com/evrblk/grackle/pkg/tables"
)

// semaphoresTable is a table of semaphores indexed by semaphore ID and semaphore name.
//
// Table Primary Key:
// 1. shard key (by account id and namespace id)
// 2. account id
// 3. namespace id
//
// Table Sort Key:
// 1. semaphore id
//
// Names Index Primary Key:
// 1. shard key (by account id and namespace id)
// 2. account id
// 3. namespace id
// 4. semaphore name
//
// Lease Id Index Primary Key:
// 1. shard key (by account id and namespace id)
// 2. account id
// 3. namespace id
// 4. lease id
//
// Lease Id Index Sort Key:
// 1. semaphore id
type semaphoresTable struct {
	table        *monsterax.BinaryTable[*corepb.Semaphore, corepb.Semaphore]
	namesIndex   *monsterax.Uint64Table
	leaseIdIndex *monsterax.OneToManySortedIndex
}

func newSemaphoresTable(shardLowerBound []byte, shardUpperBound []byte) *semaphoresTable {
	return &semaphoresTable{
		table: monsterax.NewBinaryTable[*corepb.Semaphore, corepb.Semaphore](
			tables.Grackle["Grackle.SemaphoresCore.Semaphores.Table"].Bytes(),
			shardLowerBound,
			shardUpperBound,
		),
		namesIndex: monsterax.NewUint64Table(
			tables.Grackle["Grackle.SemaphoresCore.Semaphores.NamesIndex"].Bytes(),
			shardLowerBound,
			shardUpperBound,
		),
		leaseIdIndex: monsterax.NewOneToManySortedIndex(
			tables.Grackle["Grackle.SemaphoresCore.Semaphores.LeaseIdIndex"].Bytes(),
			shardLowerBound,
			shardUpperBound,
		),
	}
}

func (t *semaphoresTable) GetTableKeyRanges() []monsterax.KeyRange {
	return []monsterax.KeyRange{
		t.table.GetTableKeyRange(),
		t.namesIndex.GetTableKeyRange(),
		t.leaseIdIndex.GetTableKeyRange(),
	}
}

func (t *semaphoresTable) Get(txn *store.Txn, semaphoreId *corepb.SemaphoreId) (*corepb.Semaphore, error) {
	return t.table.Get(txn,
		utils.ConcatBytes(
			t.tablePK(semaphoreId.AccountId, semaphoreId.NamespaceId),
			t.tableSK(semaphoreId.SemaphoreId)))
}

func (t *semaphoresTable) GetByName(txn *store.Txn, accountId uint64, namespaceId uint32, semaphoreName string) (*corepb.Semaphore, error) {
	semaphoreId, err := t.namesIndex.Get(txn, t.namesIndexPK(accountId, namespaceId, semaphoreName))
	if err != nil {
		return nil, err
	}
	return t.Get(txn, &corepb.SemaphoreId{
		AccountId:   accountId,
		NamespaceId: namespaceId,
		SemaphoreId: semaphoreId,
	})
}

func (t *semaphoresTable) Update(txn *store.Txn, semaphore *corepb.Semaphore) error {
	return t.table.Set(txn,
		utils.ConcatBytes(
			t.tablePK(semaphore.Id.AccountId, semaphore.Id.NamespaceId),
			t.tableSK(semaphore.Id.SemaphoreId)),
		semaphore)
}

func (t *semaphoresTable) Delete(txn *store.Txn, semaphoreId *corepb.SemaphoreId) error {
	return t.table.Delete(txn,
		utils.ConcatBytes(
			t.tablePK(semaphoreId.AccountId, semaphoreId.NamespaceId),
			t.tableSK(semaphoreId.SemaphoreId)))
}

func (t *semaphoresTable) Create(txn *store.Txn, semaphore *corepb.Semaphore) (*monsterax.Error, error) {
	indexPK := t.namesIndexPK(semaphore.Id.AccountId, semaphore.Id.NamespaceId, semaphore.Name)
	_, err := t.namesIndex.Get(txn, indexPK)
	if err != nil {
		if !errors.Is(err, store.ErrNotFound) {
			return nil, err
		}
	} else {
		return monsterax.NewErrorWithContext(
			monsterax.AlreadyExists,
			"semaphore with this name already exists",
			map[string]string{
				"semaphore_name": semaphore.Name,
			}), nil
	}

	err = t.namesIndex.Set(txn, indexPK, semaphore.Id.SemaphoreId)
	if err != nil {
		return nil, err
	}

	return nil, t.table.Set(txn,
		utils.ConcatBytes(
			t.tablePK(semaphore.Id.AccountId, semaphore.Id.NamespaceId),
			t.tableSK(semaphore.Id.SemaphoreId)),
		semaphore)
}

type listSemaphoresResult struct {
	semaphores              []*corepb.Semaphore
	nextPaginationToken     *corepb.PaginationToken
	previousPaginationToken *corepb.PaginationToken
}

func (t *semaphoresTable) List(txn *store.Txn, accountId uint64, namespaceId uint32, paginationToken *corepb.PaginationToken, limit int) (*listSemaphoresResult, error) {
	result, err := t.table.ListPaginated(txn, t.tablePK(accountId, namespaceId), pagination.CoreToMonstera(paginationToken), limit)
	if err != nil {
		return nil, err
	}

	return &listSemaphoresResult{
		semaphores:              result.Items,
		nextPaginationToken:     pagination.MonsteraToCore(result.NextPaginationToken),
		previousPaginationToken: pagination.MonsteraToCore(result.PreviousPaginationToken),
	}, nil
}

func (t *semaphoresTable) tablePK(accountId uint64, namespaceId uint32) []byte {
	return utils.ConcatBytes(
		sharding.ByAccountAndNamespace(accountId, namespaceId),
		accountId,
		namespaceId,
	)
}

func (t *semaphoresTable) tableSK(semaphoreId uint64) []byte {
	return utils.ConcatBytes(
		semaphoreId,
	)
}

func (t *semaphoresTable) namesIndexPK(accountId uint64, namespaceId uint32, semaphoreName string) []byte {
	return utils.ConcatBytes(
		sharding.ByAccountAndNamespace(accountId, namespaceId),
		accountId,
		namespaceId,
		semaphoreName,
	)
}

func (t *semaphoresTable) leaseIdIndexPK(accountId uint64, namespaceId uint32, leaseId uint64) []byte {
	return utils.ConcatBytes(
		sharding.ByAccountAndNamespace(accountId, namespaceId),
		accountId,
		namespaceId,
		leaseId,
	)
}

func (t *semaphoresTable) ListByLeaseId(txn *store.Txn, leaseId *corepb.LeaseId, paginationToken *corepb.PaginationToken, limit int) (*listSemaphoresResult, error) {
	result, err := t.leaseIdIndex.ListPaginated(txn,
		t.leaseIdIndexPK(leaseId.AccountId, leaseId.NamespaceId, leaseId.LeaseId), pagination.CoreToMonstera(paginationToken), limit)
	if err != nil {
		return nil, err
	}

	semaphores := make([]*corepb.Semaphore, len(result.Items))
	for i, semaphoreIdBytes := range result.Items {
		semaphoreId := utils.BytesToUint64(semaphoreIdBytes)
		semaphore, err := t.Get(txn, &corepb.SemaphoreId{
			AccountId:   leaseId.AccountId,
			NamespaceId: leaseId.NamespaceId,
			SemaphoreId: semaphoreId,
		})
		if err != nil {
			return nil, err
		}
		semaphores[i] = semaphore
	}

	return &listSemaphoresResult{
		semaphores:              semaphores,
		nextPaginationToken:     pagination.MonsteraToCore(result.NextPaginationToken),
		previousPaginationToken: pagination.MonsteraToCore(result.PreviousPaginationToken),
	}, nil
}

// AddLeaseToIndex adds a lease ID to the semaphore's lease index
func (t *semaphoresTable) AddLeaseToIndex(txn *store.Txn, semaphoreId *corepb.SemaphoreId, leaseId uint64) error {
	return t.leaseIdIndex.Add(txn,
		t.leaseIdIndexPK(semaphoreId.AccountId, semaphoreId.NamespaceId, leaseId),
		utils.Uint64ToBytes(semaphoreId.SemaphoreId),
	)
}

// RemoveLeaseFromIndex removes a lease ID from the semaphore's lease index
func (t *semaphoresTable) RemoveLeaseFromIndex(txn *store.Txn, semaphoreId *corepb.SemaphoreId, leaseId uint64) error {
	return t.leaseIdIndex.Delete(txn,
		t.leaseIdIndexPK(semaphoreId.AccountId, semaphoreId.NamespaceId, leaseId),
		utils.Uint64ToBytes(semaphoreId.SemaphoreId),
	)
}
