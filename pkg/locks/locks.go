package locks

import (
	"github.com/evrblk/monstera/store"
	"github.com/evrblk/monstera/utils"
	monsterax "github.com/evrblk/monstera/x"

	"github.com/evrblk/grackle/pkg/corepb"
	"github.com/evrblk/grackle/pkg/pagination"
	"github.com/evrblk/grackle/pkg/sharding"
	"github.com/evrblk/grackle/pkg/tables"
)

// locksTable
//
// Table Primary Key:
// 1. shard key (by account id and namespace id)
// 2. account id
// 3. namespace id
//
// Table Sort Key:
// 1. lock name
type locksTable struct {
	table *monsterax.BinaryTable[*corepb.Lock, corepb.Lock]
}

func newLocksTable(shardLowerBound []byte, shardUpperBound []byte) *locksTable {
	return &locksTable{
		table: monsterax.NewBinaryTable[*corepb.Lock, corepb.Lock](tables.GrackleLocksTableId, shardLowerBound, shardUpperBound),
	}
}

func (t *locksTable) GetTableKeyRanges() []monsterax.KeyRange {
	return []monsterax.KeyRange{
		t.table.GetTableKeyRange(),
	}
}

type listLocksResult struct {
	locks                   []*corepb.Lock
	nextPaginationToken     *corepb.PaginationToken
	previousPaginationToken *corepb.PaginationToken
}

func (t *locksTable) List(txn *store.Txn, namespaceId *corepb.NamespaceId, paginationToken *corepb.PaginationToken, limit int) (*listLocksResult, error) {
	result, err := t.table.ListPaginated(txn,
		t.tablePK(namespaceId.AccountId, namespaceId.NamespaceId), pagination.CoreToMonstera(paginationToken), limit)
	if err != nil {
		return nil, err
	}

	return &listLocksResult{
		locks:                   result.Items,
		nextPaginationToken:     pagination.MonsteraToCore(result.NextPaginationToken),
		previousPaginationToken: pagination.MonsteraToCore(result.PreviousPaginationToken),
	}, nil

}

func (t *locksTable) Get(txn *store.Txn, lockId *corepb.LockId) (*corepb.Lock, error) {
	return t.table.Get(txn,
		utils.ConcatBytes(
			t.tablePK(lockId.AccountId, lockId.NamespaceId),
			t.tableSK(lockId.LockName)))
}

func (t *locksTable) Update(txn *store.Txn, lock *corepb.Lock) error {
	return t.table.Set(txn,
		utils.ConcatBytes(
			t.tablePK(lock.Id.AccountId, lock.Id.NamespaceId),
			t.tableSK(lock.Id.LockName)),
		lock)
}

func (t *locksTable) Delete(txn *store.Txn, lockId *corepb.LockId) error {
	return t.table.Delete(txn,
		utils.ConcatBytes(
			t.tablePK(lockId.AccountId, lockId.NamespaceId),
			t.tableSK(lockId.LockName)))
}

func (t *locksTable) tablePK(accountId uint64, namespaceId uint32) []byte {
	return utils.ConcatBytes(
		sharding.ByAccountAndNamespace(accountId, namespaceId),
		accountId,
		namespaceId,
	)
}

func (t *locksTable) tableSK(lockName string) []byte {
	return utils.ConcatBytes(
		lockName,
	)
}
