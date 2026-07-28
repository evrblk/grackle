package waitgroups

import (
	"github.com/evrblk/monstera/store"
	"github.com/evrblk/monstera/utils"
	"github.com/evrblk/yellowstone-common/honey"

	"github.com/evrblk/grackle/pkg/corepb"
	"github.com/evrblk/grackle/pkg/sharding"
	"github.com/evrblk/grackle/pkg/tables"
)

// deletionRecordsTable stores wait group deletion records indexed by deletion
// time and wait group ID. A deletion record is created when a wait group
// becomes finished (completed or expired); garbage collection deletes the wait
// group once delete_at (finished_at + delete_after_finished_seconds) passes.
//
// Keys are ordered by time (not by identity); the shard prefix lives in the
// table id, so record keys start at the timestamp.
//
// Table Primary Key:
// 1. timestamp (delete_at)
// 2. account id
// 3. namespace id
// 4. wait group id
//
// Table Prefix:
// 1. timestamp (delete_at)
type deletionRecordsTable struct {
	table *honey.BinaryTable[*corepb.WaitGroupsDeletionRecord, corepb.WaitGroupsDeletionRecord]
}

func newDeletionRecordsTable(replicaPrefix []byte) *deletionRecordsTable {
	return &deletionRecordsTable{
		table: honey.NewBinaryTable[*corepb.WaitGroupsDeletionRecord, corepb.WaitGroupsDeletionRecord](
			utils.ConcatBytes(replicaPrefix, tablePrefixDeletionRecords),
		),
	}
}

// Clear deletes every deletion record row of this shard.
func (t *deletionRecordsTable) Clear(badgerStore *store.BadgerStore) error {
	return badgerStore.DeletePrefix(t.table.TableId())
}

// EachEntity streams every deletion record as (canonical key, stored value).
func (t *deletionRecordsTable) EachEntity(txn *store.Txn, fn func(key []byte, value []byte) (bool, error)) error {
	return t.table.EachEntry(txn, fn)
}

// RestoreEntity decodes one streamed deletion record and, if owned, inserts it
// through Add — which re-derives its key under this table's own prefix.
func (t *deletionRecordsTable) RestoreEntity(txn *store.Txn, key []byte, value []byte, bounds tables.ShardRange) (bool, error) {
	record := &corepb.WaitGroupsDeletionRecord{}
	if err := record.UnmarshalBinary(value); err != nil {
		return false, err
	}
	if !bounds.Owns(sharding.ByAccountAndNamespace(record.WaitGroupId.AccountId, record.WaitGroupId.NamespaceId)) {
		return false, nil
	}
	return true, t.Add(txn, record.DeleteAt, record.WaitGroupId)
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
		time,
		accountId,
		namespaceId,
		waitGroupId,
	)
}

func (t *deletionRecordsTable) tablePrefix(time int64) []byte {
	return utils.ConcatBytes(
		time,
	)
}
