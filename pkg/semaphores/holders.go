package semaphores

import (
	"github.com/evrblk/monstera/store"
	"github.com/evrblk/monstera/utils"
	monsterax "github.com/evrblk/monstera/x"

	"github.com/evrblk/grackle/pkg/corepb"
	"github.com/evrblk/grackle/pkg/pagination"
	"github.com/evrblk/grackle/pkg/sharding"
	"github.com/evrblk/grackle/pkg/tables"
)

// holdersTable stores semaphore holders indexed by semaphore holder id and by expiration time.
//
// Table Primary Key:
// 1. shard key (by account id and namespace id)
// 2. account id
// 3. namespace id
// 4. semaphore id
//
// Table Sort Key:
// 1. lease id
//
// Expiration Index Primay Key:
// 1. shard key (by account id and namespace id)
// 2. account id
// 3. namespace id
// 4. semaphore id
//
// Expiration Index Sort Key:
// 1. expiration time
// 2. lease id
type holdersTable struct {
	table           *monsterax.BinaryTable[*corepb.SemaphoreHolder, corepb.SemaphoreHolder]
	expirationIndex *monsterax.SortedIndex
}

func newHoldersTable(shardLowerBound []byte, shardUpperBound []byte) *holdersTable {
	return &holdersTable{
		table: monsterax.NewBinaryTable[*corepb.SemaphoreHolder, corepb.SemaphoreHolder](
			tables.Grackle["Grackle.SemaphoresCore.Holders.Table"].Bytes(),
			shardLowerBound,
			shardUpperBound),
		expirationIndex: monsterax.NewSortedIndex(
			tables.Grackle["Grackle.SemaphoresCore.Holders.ExpirationIndex"].Bytes(),
			shardLowerBound,
			shardUpperBound),
	}
}

func (t *holdersTable) GetTableKeyRanges() []monsterax.KeyRange {
	return []monsterax.KeyRange{
		t.table.GetTableKeyRange(),
		t.expirationIndex.GetTableKeyRange(),
	}
}

func (t *holdersTable) Get(txn *store.Txn, holderId *corepb.SemaphoreHolderId) (*corepb.SemaphoreHolder, error) {
	return t.table.Get(txn,
		utils.ConcatBytes(
			t.tablePK(holderId.AccountId, holderId.NamespaceId, holderId.SemaphoreId),
			t.tableSK(holderId.LeaseId)))
}

func (t *holdersTable) Create(txn *store.Txn, holder *corepb.SemaphoreHolder) error {
	err := t.expirationIndex.Add(txn,
		utils.ConcatBytes(
			t.expirationIndexPK(holder.Id.AccountId, holder.Id.NamespaceId, holder.Id.SemaphoreId),
			t.expirationIndexSK(holder.ExpiresAt, holder.Id.LeaseId)),
	)
	if err != nil {
		return err
	}

	return t.table.Set(txn,
		utils.ConcatBytes(
			t.tablePK(holder.Id.AccountId, holder.Id.NamespaceId, holder.Id.SemaphoreId),
			t.tableSK(holder.Id.LeaseId)),
		holder)
}

func (t *holdersTable) Update(txn *store.Txn, updatedHolder *corepb.SemaphoreHolder) error {
	key := utils.ConcatBytes(
		t.tablePK(updatedHolder.Id.AccountId, updatedHolder.Id.NamespaceId, updatedHolder.Id.SemaphoreId),
		t.tableSK(updatedHolder.Id.LeaseId))
	existingHolder, err := t.table.Get(txn, key)
	if err != nil {
		return err
	}

	if existingHolder.ExpiresAt != updatedHolder.ExpiresAt {
		indexPK := t.expirationIndexPK(updatedHolder.Id.AccountId, updatedHolder.Id.NamespaceId, updatedHolder.Id.SemaphoreId)

		err = t.expirationIndex.Delete(txn, utils.ConcatBytes(indexPK, t.expirationIndexSK(existingHolder.ExpiresAt, updatedHolder.Id.LeaseId)))
		if err != nil {
			return err
		}

		err = t.expirationIndex.Add(txn, utils.ConcatBytes(indexPK, t.expirationIndexSK(updatedHolder.ExpiresAt, updatedHolder.Id.LeaseId)))
		if err != nil {
			return err
		}
	}

	return t.table.Set(txn, key, updatedHolder)
}

func (t *holdersTable) Delete(txn *store.Txn, holder *corepb.SemaphoreHolder) error {
	err := t.expirationIndex.Delete(txn,
		utils.ConcatBytes(
			t.expirationIndexPK(holder.Id.AccountId, holder.Id.NamespaceId, holder.Id.SemaphoreId),
			t.expirationIndexSK(holder.ExpiresAt, holder.Id.LeaseId)))
	if err != nil {
		return err
	}

	return t.table.Delete(txn,
		utils.ConcatBytes(
			t.tablePK(holder.Id.AccountId, holder.Id.NamespaceId, holder.Id.SemaphoreId),
			t.tableSK(holder.Id.LeaseId)))
}

type listHoldersResult struct {
	holders                 []*corepb.SemaphoreHolder
	nextPaginationToken     *corepb.PaginationToken
	previousPaginationToken *corepb.PaginationToken
}

func (t *holdersTable) List(txn *store.Txn, accountId uint64, namespaceId uint64, semaphoreId uint64,
	paginationToken *corepb.PaginationToken, limit int) (*listHoldersResult, error) {
	result, err := t.table.ListPaginated(txn,
		t.tablePK(accountId, namespaceId, semaphoreId),
		pagination.CoreToMonstera(paginationToken),
		limit)
	if err != nil {
		return nil, err
	}

	return &listHoldersResult{
		holders:                 result.Items,
		nextPaginationToken:     pagination.MonsteraToCore(result.NextPaginationToken),
		previousPaginationToken: pagination.MonsteraToCore(result.PreviousPaginationToken),
	}, nil
}

func (t *holdersTable) ListByExpiration(txn *store.Txn, semaphoreId *corepb.SemaphoreId, from int64, to int64, fn func(holder *corepb.SemaphoreHolder) (bool, error)) error {
	pk := t.expirationIndexPK(semaphoreId.AccountId, semaphoreId.NamespaceId, semaphoreId.SemaphoreId)
	lowerBound := utils.ConcatBytes(pk, t.expirationIndexSKPrefix(from))
	upperBound := utils.ConcatBytes(pk, t.expirationIndexSKPrefix(to))

	return t.expirationIndex.ListInRange(txn, lowerBound, upperBound, func(key []byte) (bool, error) {
		leaseId := utils.BytesToUint64(key[len(key)-8:])
		holder, err := t.table.Get(txn,
			utils.ConcatBytes(
				t.tablePK(semaphoreId.AccountId, semaphoreId.NamespaceId, semaphoreId.SemaphoreId),
				t.tableSK(leaseId)))
		if err != nil {
			return false, err
		}
		return fn(holder)
	})
}

func (t *holdersTable) tablePK(accountId uint64, namespaceId uint64, semaphoreId uint64) []byte {
	return utils.ConcatBytes(
		sharding.ByAccountAndNamespace(accountId, namespaceId),
		accountId,
		namespaceId,
		semaphoreId,
	)
}

func (t *holdersTable) tableSK(leaseId uint64) []byte {
	return utils.ConcatBytes(
		leaseId,
	)
}

func (t *holdersTable) expirationIndexPK(accountId uint64, namespaceId uint64, semaphoreId uint64) []byte {
	return utils.ConcatBytes(
		sharding.ByAccountAndNamespace(accountId, namespaceId),
		accountId,
		namespaceId,
		semaphoreId,
	)
}

func (t *holdersTable) expirationIndexSK(expirationTime int64, leaseId uint64) []byte {
	return utils.ConcatBytes(
		expirationTime,
		leaseId,
	)
}

func (t *holdersTable) expirationIndexSKPrefix(expirationTime int64) []byte {
	return utils.ConcatBytes(
		expirationTime,
	)
}
