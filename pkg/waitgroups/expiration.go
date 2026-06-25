package waitgroups

import (
	"github.com/evrblk/monstera/store"
	"github.com/evrblk/monstera/utils"
	monsterax "github.com/evrblk/monstera/x"

	"github.com/evrblk/grackle/pkg/corepb"
	"github.com/evrblk/grackle/pkg/tables"
)

// expirationRecordsTable stores wait group expiration records indexed by wait group ID and expiration time.
//
// Table Primary Key:
// 1. shard id
// 2. timestamp
// 3. account id
// 4. namespace id
// 5. wait group id
//
// Table Prefix:
// 1. shard id
// 2. timestamp
type expirationRecordsTable struct {
	shardGlobalIndexPrefix []byte

	table *monsterax.BinaryTable[*corepb.WaitGroupsExpirationRecord, corepb.WaitGroupsExpirationRecord]
}

func newExpirationRecordsTable(shardGlobalIndexPrefix []byte) *expirationRecordsTable {
	return &expirationRecordsTable{
		shardGlobalIndexPrefix: shardGlobalIndexPrefix,

		table: monsterax.NewBinaryTable[*corepb.WaitGroupsExpirationRecord, corepb.WaitGroupsExpirationRecord](
			tables.Grackle["Grackle.WaitGroupsCore.ExpirationRecords.Table"].Bytes(),
			shardGlobalIndexPrefix,
			shardGlobalIndexPrefix,
		),
	}
}

func (t *expirationRecordsTable) GetTableKeyRange() monsterax.KeyRange {
	return t.table.GetTableKeyRange()
}

func (t *expirationRecordsTable) Delete(txn *store.Txn, expiresAt int64, waitGroupId *corepb.WaitGroupId) error {
	return t.table.Delete(txn,
		t.tablePK(expiresAt, waitGroupId.AccountId, waitGroupId.NamespaceId, waitGroupId.WaitGroupId),
	)
}

func (t *expirationRecordsTable) Add(txn *store.Txn, expiresAt int64, waitGroupId *corepb.WaitGroupId) error {
	return t.table.Set(txn,
		t.tablePK(expiresAt, waitGroupId.AccountId, waitGroupId.NamespaceId, waitGroupId.WaitGroupId),
		&corepb.WaitGroupsExpirationRecord{
			ExpiresAt:   expiresAt,
			WaitGroupId: waitGroupId,
		})
}

func (t *expirationRecordsTable) ListByExpiration(txn *store.Txn, from int64, to int64, fn func(record *corepb.WaitGroupsExpirationRecord) (bool, error)) error {
	return t.table.ListInRange(txn, t.tablePrefix(from), t.tablePrefix(to), false, func(record *corepb.WaitGroupsExpirationRecord) (bool, error) {
		return fn(record)
	})
}

func (t *expirationRecordsTable) tablePK(time int64, accountId uint64, namespaceId uint64, waitGroupId uint64) []byte {
	return utils.ConcatBytes(
		t.shardGlobalIndexPrefix,
		time,
		accountId,
		namespaceId,
		waitGroupId,
	)
}

func (t *expirationRecordsTable) tablePrefix(time int64) []byte {
	return utils.ConcatBytes(
		t.shardGlobalIndexPrefix,
		time,
	)
}
