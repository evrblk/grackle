package waitgroups

import (
	"github.com/evrblk/monstera/store"
	"github.com/evrblk/monstera/utils"
	"github.com/evrblk/yellowstone-common/honey"

	"github.com/evrblk/grackle/pkg/corepb"
	"github.com/evrblk/grackle/pkg/tables"
)

// deletionRecordsTable stores wait group deletion records indexed by deletion
// time and wait group ID. A deletion record is created when a wait group
// becomes finished (completed or expired); garbage collection deletes the wait
// group once delete_at (finished_at + delete_after_finished_seconds) passes.
//
// Table Primary Key:
// 1. shard id
// 2. timestamp (delete_at)
// 3. account id
// 4. namespace id
// 5. wait group id
//
// Table Prefix:
// 1. shard id
// 2. timestamp (delete_at)
type deletionRecordsTable struct {
	shardGlobalIndexPrefix []byte

	table *honey.BinaryTable[*corepb.WaitGroupsDeletionRecord, corepb.WaitGroupsDeletionRecord]
}

func newDeletionRecordsTable(shardGlobalIndexPrefix []byte) *deletionRecordsTable {
	return &deletionRecordsTable{
		shardGlobalIndexPrefix: shardGlobalIndexPrefix,

		table: honey.NewBinaryTable[*corepb.WaitGroupsDeletionRecord, corepb.WaitGroupsDeletionRecord](
			tables.Grackle["Grackle.WaitGroupsCore.DeletionRecords.Table"].Bytes(),
			shardGlobalIndexPrefix,
			shardGlobalIndexPrefix,
		),
	}
}

func (t *deletionRecordsTable) GetTableKeyRange() honey.KeyRange {
	return t.table.GetTableKeyRange()
}

func (t *deletionRecordsTable) Delete(txn *store.Txn, deleteAt int64, waitGroupId *corepb.WaitGroupId) error {
	return t.table.Delete(txn,
		t.tablePK(deleteAt, waitGroupId.AccountId, waitGroupId.NamespaceId, waitGroupId.WaitGroupId),
	)
}

func (t *deletionRecordsTable) Add(txn *store.Txn, deleteAt int64, waitGroupId *corepb.WaitGroupId) error {
	return t.table.Set(txn,
		t.tablePK(deleteAt, waitGroupId.AccountId, waitGroupId.NamespaceId, waitGroupId.WaitGroupId),
		&corepb.WaitGroupsDeletionRecord{
			DeleteAt:    deleteAt,
			WaitGroupId: waitGroupId,
		})
}

func (t *deletionRecordsTable) ListByDeletion(txn *store.Txn, from int64, to int64, fn func(record *corepb.WaitGroupsDeletionRecord) (bool, error)) error {
	return t.table.ListInRange(txn, t.tablePrefix(from), t.tablePrefix(to), false, func(record *corepb.WaitGroupsDeletionRecord) (bool, error) {
		return fn(record)
	})
}

func (t *deletionRecordsTable) tablePK(time int64, accountId uint64, namespaceId uint64, waitGroupId uint64) []byte {
	return utils.ConcatBytes(
		t.shardGlobalIndexPrefix,
		time,
		accountId,
		namespaceId,
		waitGroupId,
	)
}

func (t *deletionRecordsTable) tablePrefix(time int64) []byte {
	return utils.ConcatBytes(
		t.shardGlobalIndexPrefix,
		time,
	)
}
