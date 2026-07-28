package locks

import (
	"errors"

	"github.com/evrblk/monstera/store"
	"github.com/evrblk/monstera/utils"
	"github.com/evrblk/yellowstone-common/honey"

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
// 1. account id
// 2. namespace id
//
// Table Sort Key:
// 1. ancestor name (path prefix)
type lockAncestorsTable struct {
	table *honey.BinaryTable[*corepb.LockAncestor, corepb.LockAncestor]
}

// newLockAncestorsTable scopes the table under the shard-unique prefix
// (nested under the registry table id); see newLocksTable. Keys carry no
// shard key material, so honey gets nil bounds.
func newLockAncestorsTable(replicaPrefix []byte) *lockAncestorsTable {
	return &lockAncestorsTable{
		table: honey.NewBinaryTable[*corepb.LockAncestor, corepb.LockAncestor](
			utils.ConcatBytes(replicaPrefix, tablePrefixAncestors),
		),
	}
}

// Clear deletes every ancestor rollup row.
func (t *lockAncestorsTable) Clear(badgerStore *store.BadgerStore) error {
	return badgerStore.DeletePrefix(t.table.TableId())
}

// EachEntity streams every ancestor rollup as (canonical key, stored value).
func (t *lockAncestorsTable) EachEntity(txn *store.Txn, fn func(key []byte, value []byte) (bool, error)) error {
	return t.table.EachEntry(txn, fn)
}

// RestoreEntity decodes one streamed ancestor rollup and, if owned, inserts
// it under this table's own keys.
func (t *lockAncestorsTable) RestoreEntity(txn *store.Txn, key []byte, value []byte, bounds tables.ShardRange) (bool, error) {
	ancestor := &corepb.LockAncestor{}
	if err := ancestor.UnmarshalBinary(value); err != nil {
		return false, err
	}
	if !bounds.Owns(sharding.ByAccountAndNamespace(ancestor.Id.AccountId, ancestor.Id.NamespaceId)) {
		return false, nil
	}
	return true, t.Set(txn, ancestor)
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
		accountId,
		namespaceId,
	)
}

func (t *lockAncestorsTable) tableSK(ancestorName string) []byte {
	return utils.ConcatBytes(
		ancestorName,
	)
}
