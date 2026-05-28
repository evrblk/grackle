package barriers

import (
	"github.com/evrblk/monstera/store"
	"github.com/evrblk/monstera/utils"
	monsterax "github.com/evrblk/monstera/x"

	"github.com/evrblk/grackle/pkg/corepb"
	"github.com/evrblk/grackle/pkg/tables"
)

// expirationRecordsTable stores expiration queue items indexed by barrier id
//
// Index Primary Key:
// 1. shard id
// 2. timestamp
// 3. account id
// 4. namespace id
// 5. barrier id
//
// Index Prefix:
// 1. shard id
// 2. timestamp
type expirationRecordsTable struct {
	shardGlobalIndexPrefix []byte

	table *monsterax.BinaryTable[*corepb.BarriersExpirationRecord, corepb.BarriersExpirationRecord]
}

func newExpirationRecordsTable(shardGlobalIndexPrefix []byte) *expirationRecordsTable {
	return &expirationRecordsTable{
		shardGlobalIndexPrefix: shardGlobalIndexPrefix,

		table: monsterax.NewBinaryTable[*corepb.BarriersExpirationRecord, corepb.BarriersExpirationRecord](
			tables.Grackle["Grackle.BarriersCore.ExpirationRecords.Table"].Bytes(),
			shardGlobalIndexPrefix,
			shardGlobalIndexPrefix,
		),
	}
}

func (t *expirationRecordsTable) GetTableKeyRange() monsterax.KeyRange {
	return t.table.GetTableKeyRange()
}

func (t *expirationRecordsTable) List(txn *store.Txn, from int64, to int64, fn func(record *corepb.BarriersExpirationRecord) (bool, error)) error {
	return t.table.ListInRange(txn, t.tablePrefix(from), t.tablePrefix(to), false, func(record *corepb.BarriersExpirationRecord) (bool, error) {
		return fn(record)
	})
}

func (t *expirationRecordsTable) Delete(txn *store.Txn, expiresAt int64, barrierId *corepb.BarrierId) error {
	return t.table.Delete(txn,
		t.tablePK(expiresAt, barrierId.AccountId, barrierId.NamespaceId, barrierId.BarrierId))
}

func (t *expirationRecordsTable) Add(txn *store.Txn, expiresAt int64, barrierId *corepb.BarrierId) error {
	return t.table.Set(txn,
		t.tablePK(expiresAt, barrierId.AccountId, barrierId.NamespaceId, barrierId.BarrierId),
		&corepb.BarriersExpirationRecord{
			ExpiresAt: expiresAt,
			BarrierId: barrierId,
		},
	)
}

func (t *expirationRecordsTable) tablePK(time int64, accountId uint64, namespaceId uint32, barrierId uint64) []byte {
	return utils.ConcatBytes(
		t.shardGlobalIndexPrefix,
		time,
		accountId,
		namespaceId,
		barrierId,
	)
}

func (t *expirationRecordsTable) tablePrefix(time int64) []byte {
	return utils.ConcatBytes(
		t.shardGlobalIndexPrefix,
		time,
	)
}
