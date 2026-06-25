package locks

import (
	"errors"

	"github.com/evrblk/monstera/store"
	"github.com/evrblk/monstera/utils"
	monsterax "github.com/evrblk/monstera/x"

	"github.com/evrblk/grackle/pkg/corepb"
	"github.com/evrblk/grackle/pkg/sharding"
	"github.com/evrblk/grackle/pkg/tables"
)

// lockAncestorsTable stores ancestor nodes for hierarchical lock names.
//
// For a lock named "a/b/c", ancestor entries are stored for "a" and "a/b",
// each tracking how many exclusively and shared-locked descendants they have.
//
// Table Primary Key:
// 1. shard key (by account id and namespace id)
// 2. account id
// 3. namespace id
//
// Table Sort Key:
// 1. ancestor name (path prefix)
type lockAncestorsTable struct {
	table *monsterax.BinaryTable[*corepb.LockAncestor, corepb.LockAncestor]
}

func newLockAncestorsTable(shardLowerBound []byte, shardUpperBound []byte) *lockAncestorsTable {
	return &lockAncestorsTable{
		table: monsterax.NewBinaryTable[*corepb.LockAncestor, corepb.LockAncestor](
			tables.Grackle["Grackle.LocksCore.Ancestors.Table"].Bytes(),
			shardLowerBound,
			shardUpperBound,
		),
	}
}

func (t *lockAncestorsTable) GetTableKeyRange() monsterax.KeyRange {
	return t.table.GetTableKeyRange()
}

func (t *lockAncestorsTable) Get(txn *store.Txn, lockId *corepb.LockId) (*corepb.LockAncestor, error) {
	ancestor, err := t.table.Get(txn,
		utils.ConcatBytes(
			t.tablePK(lockId.AccountId, lockId.NamespaceId),
			t.tableSK(lockId.LockName)))
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return &corepb.LockAncestor{
				Id:             lockId,
				ExclusiveCount: 0,
				SharedCount:    0,
			}, nil
		}
		return nil, err
	}
	return ancestor, nil
}

func (t *lockAncestorsTable) Set(txn *store.Txn, ancestor *corepb.LockAncestor) error {
	return t.table.Set(txn,
		utils.ConcatBytes(
			t.tablePK(ancestor.Id.AccountId, ancestor.Id.NamespaceId),
			t.tableSK(ancestor.Id.LockName)),
		ancestor)
}

func (t *lockAncestorsTable) Delete(txn *store.Txn, lockId *corepb.LockId) error {
	return t.table.Delete(txn,
		utils.ConcatBytes(
			t.tablePK(lockId.AccountId, lockId.NamespaceId),
			t.tableSK(lockId.LockName)))
}

func (t *lockAncestorsTable) tablePK(accountId uint64, namespaceId uint64) []byte {
	return utils.ConcatBytes(
		sharding.ByAccountAndNamespace(accountId, namespaceId),
		accountId,
		namespaceId,
	)
}

func (t *lockAncestorsTable) tableSK(ancestorName string) []byte {
	return utils.ConcatBytes(
		ancestorName,
	)
}
