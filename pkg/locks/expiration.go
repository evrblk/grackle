package locks

import (
	"github.com/evrblk/monstera/store"
	"github.com/evrblk/monstera/utils"
	monsterax "github.com/evrblk/monstera/x"

	"github.com/evrblk/grackle/pkg/corepb"
	"github.com/evrblk/grackle/pkg/tables"
)

// expirationRecordsTable
//
// 1. shard id
// 2. timestamp
// 3. account id
// 4. namespace Id
// 5. lock name
type expirationRecordsTable struct {
	shardGlobalIndexPrefix []byte

	table *monsterax.BinaryTable[*corepb.LocksExpirationRecord, corepb.LocksExpirationRecord]
}

func newExpirationRecordsTable(shardGlobalIndexPrefix []byte) *expirationRecordsTable {
	return &expirationRecordsTable{
		shardGlobalIndexPrefix: shardGlobalIndexPrefix,

		table: monsterax.NewBinaryTable[*corepb.LocksExpirationRecord, corepb.LocksExpirationRecord](
			tables.GrackleLocksExpirationRecordsTableId,
			shardGlobalIndexPrefix,
			shardGlobalIndexPrefix,
		),
	}
}

func (t *expirationRecordsTable) GetTableKeyRange() monsterax.KeyRange {
	return t.table.GetTableKeyRange()
}

func (t *expirationRecordsTable) Add(txn *store.Txn, expiresAt int64, lockId *corepb.LockId) error {
	return t.table.Set(txn,
		t.tablePK(expiresAt, lockId.AccountId, lockId.NamespaceId, lockId.LockName),
		&corepb.LocksExpirationRecord{
			ExpiresAt: expiresAt,
			LockId:    lockId,
		},
	)
}

func (t *expirationRecordsTable) Delete(txn *store.Txn, expiresAt int64, lockId *corepb.LockId) error {
	return t.table.Delete(txn,
		t.tablePK(expiresAt, lockId.AccountId, lockId.NamespaceId, lockId.LockName))
}

func (t *expirationRecordsTable) List(txn *store.Txn, from int64, to int64, fn func(record *corepb.LocksExpirationRecord) (bool, error)) error {
	return t.table.ListInRange(txn, t.tablePrefix(from), t.tablePrefix(to), false, func(record *corepb.LocksExpirationRecord) (bool, error) {
		return fn(record)
	})
}

func (t *expirationRecordsTable) tablePK(time int64, accountId uint64, namespaceId uint32, lockName string) []byte {
	return utils.ConcatBytes(
		t.shardGlobalIndexPrefix,
		time,
		accountId,
		namespaceId,
		lockName,
	)
}

func (t *expirationRecordsTable) tablePrefix(time int64) []byte {
	return utils.ConcatBytes(
		t.shardGlobalIndexPrefix,
		time,
	)
}
