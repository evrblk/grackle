package barriers

import (
	"github.com/evrblk/monstera/store"
	"github.com/evrblk/monstera/utils"
	monsterax "github.com/evrblk/monstera/x"

	"github.com/evrblk/grackle/pkg/corepb"
	"github.com/evrblk/grackle/pkg/tables"
)

// deletionRecordsTable stores barrier deletion records indexed by deletion time
// and barrier ID. A barrier is auto-deleted after a period of inactivity: its
// deletion record is keyed at last_activity_at + delete_inactive_after_seconds
// and is reconciled on every activity so the deletion always trails the most
// recent activity.
//
// Table Primary Key:
// 1. shard id
// 2. timestamp (delete_at)
// 3. account id
// 4. namespace id
// 5. barrier id
//
// Table Prefix:
// 1. shard id
// 2. timestamp (delete_at)
type deletionRecordsTable struct {
	shardGlobalIndexPrefix []byte

	table *monsterax.BinaryTable[*corepb.BarriersDeletionRecord, corepb.BarriersDeletionRecord]
}

func newDeletionRecordsTable(shardGlobalIndexPrefix []byte) *deletionRecordsTable {
	return &deletionRecordsTable{
		shardGlobalIndexPrefix: shardGlobalIndexPrefix,

		table: monsterax.NewBinaryTable[*corepb.BarriersDeletionRecord, corepb.BarriersDeletionRecord](
			tables.Grackle["Grackle.BarriersCore.DeletionRecords.Table"].Bytes(),
			shardGlobalIndexPrefix,
			shardGlobalIndexPrefix,
		),
	}
}

func (t *deletionRecordsTable) GetTableKeyRange() monsterax.KeyRange {
	return t.table.GetTableKeyRange()
}

func (t *deletionRecordsTable) Delete(txn *store.Txn, deleteAt int64, barrierId *corepb.BarrierId) error {
	return t.table.Delete(txn,
		t.tablePK(deleteAt, barrierId.AccountId, barrierId.NamespaceId, barrierId.BarrierId))
}

func (t *deletionRecordsTable) Add(txn *store.Txn, deleteAt int64, barrierId *corepb.BarrierId) error {
	return t.table.Set(txn,
		t.tablePK(deleteAt, barrierId.AccountId, barrierId.NamespaceId, barrierId.BarrierId),
		&corepb.BarriersDeletionRecord{
			DeleteAt:  deleteAt,
			BarrierId: barrierId,
		},
	)
}

func (t *deletionRecordsTable) ListByDeletion(txn *store.Txn, from int64, to int64, fn func(record *corepb.BarriersDeletionRecord) (bool, error)) error {
	return t.table.ListInRange(txn, t.tablePrefix(from), t.tablePrefix(to), false, func(record *corepb.BarriersDeletionRecord) (bool, error) {
		return fn(record)
	})
}

func (t *deletionRecordsTable) tablePK(time int64, accountId uint64, namespaceId uint32, barrierId uint64) []byte {
	return utils.ConcatBytes(
		t.shardGlobalIndexPrefix,
		time,
		accountId,
		namespaceId,
		barrierId,
	)
}

func (t *deletionRecordsTable) tablePrefix(time int64) []byte {
	return utils.ConcatBytes(
		t.shardGlobalIndexPrefix,
		time,
	)
}
