package barriers

import (
	"github.com/evrblk/monstera/store"
	"github.com/evrblk/monstera/utils"
	"github.com/evrblk/yellowstone-common/honey"

	"github.com/evrblk/grackle/pkg/corepb"
	"github.com/evrblk/grackle/pkg/sharding"
	"github.com/evrblk/grackle/pkg/tables"
)

// deletionRecordsTable stores barrier deletion records indexed by deletion time
// and barrier ID. A barrier is auto-deleted after a period of inactivity: its
// deletion record is keyed at last_activity_at + delete_inactive_after_seconds
// and is reconciled on every activity so the deletion always trails the most
// recent activity.
//
// Keys are ordered by time (not by identity), so the shard prefix travels
// in-key instead of in the table id.
//
// Table Primary Key:
// 1. shard prefix
// 2. timestamp (delete_at)
// 3. account id
// 4. namespace id
// 5. barrier id
//
// Table Prefix:
// 1. shard prefix
// 2. timestamp (delete_at)
type deletionRecordsTable struct {
	shardPrefix []byte

	table *honey.BinaryTable[*corepb.BarriersDeletionRecord, corepb.BarriersDeletionRecord]
}

func newDeletionRecordsTable(shardPrefix []byte) *deletionRecordsTable {
	return &deletionRecordsTable{
		shardPrefix: shardPrefix,

		table: honey.NewBinaryTable[*corepb.BarriersDeletionRecord, corepb.BarriersDeletionRecord](
			tables.Grackle["Grackle.BarriersCore.DeletionRecords.Table"].Bytes(),
			shardPrefix,
			shardPrefix,
		),
	}
}

// Clear deletes every deletion record row of this shard (keys carry the
// shard prefix in-key under the registry table id).
func (t *deletionRecordsTable) Clear(badgerStore *store.BadgerStore) error {
	return badgerStore.DropPrefix(utils.ConcatBytes(t.table.TableId(), t.shardPrefix))
}

// EachEntity streams every deletion record as (canonical key, stored value).
// The stored key embeds this shard's prefix — identity of the producing
// shard, which must not travel in a portable stream — so the canonical key
// starts at the timestamp.
func (t *deletionRecordsTable) EachEntity(txn *store.Txn, fn func(key []byte, value []byte) (bool, error)) error {
	return t.table.EachEntry(txn, func(key []byte, value []byte) (bool, error) {
		return fn(key[len(t.shardPrefix):], value)
	})
}

// RestoreEntity decodes one streamed deletion record and, if owned, inserts
// it through Add — which re-derives its key under this table's own prefix.
func (t *deletionRecordsTable) RestoreEntity(txn *store.Txn, key []byte, value []byte, bounds tables.ShardRange) (bool, error) {
	record := &corepb.BarriersDeletionRecord{}
	if err := record.UnmarshalBinary(value); err != nil {
		return false, err
	}
	if !bounds.Owns(sharding.ByAccountAndNamespace(record.BarrierId.AccountId, record.BarrierId.NamespaceId)) {
		return false, nil
	}
	return true, t.Add(txn, record.DeleteAt, record.BarrierId)
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

func (t *deletionRecordsTable) tablePK(time int64, accountId uint64, namespaceId uint64, barrierId uint64) []byte {
	return utils.ConcatBytes(
		t.shardPrefix,
		time,
		accountId,
		namespaceId,
		barrierId,
	)
}

func (t *deletionRecordsTable) tablePrefix(time int64) []byte {
	return utils.ConcatBytes(
		t.shardPrefix,
		time,
	)
}
