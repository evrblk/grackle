package locks

import (
	"github.com/evrblk/monstera/store"
	"github.com/evrblk/monstera/utils"
	monsterax "github.com/evrblk/monstera/x"

	"github.com/evrblk/grackle/pkg/corepb"
	"github.com/evrblk/grackle/pkg/tables"
)

// gcRecordsTable
//
// Table Primary Key:
// 1. shard id
// 2. gc record id
type gcRecordsTable struct {
	shardGlobalIndexPrefix []byte

	table *monsterax.BinaryTable[*corepb.LocksGarbageCollectionRecord, corepb.LocksGarbageCollectionRecord]
}

func newGCRecordsTable(shardGlobalIndexPrefix []byte) *gcRecordsTable {
	return &gcRecordsTable{
		shardGlobalIndexPrefix: shardGlobalIndexPrefix,

		table: monsterax.NewBinaryTable[*corepb.LocksGarbageCollectionRecord, corepb.LocksGarbageCollectionRecord](
			tables.Grackle["Grackle.LocksCore.GarbageCollectionRecords.Table"].Bytes(),
			shardGlobalIndexPrefix,
			shardGlobalIndexPrefix,
		),
	}
}

func (t *gcRecordsTable) GetTableKeyRange() monsterax.KeyRange {
	return t.table.GetTableKeyRange()
}

func (t *gcRecordsTable) Create(txn *store.Txn, locksGCRecord *corepb.LocksGarbageCollectionRecord) error {
	return t.table.Set(txn, t.tablePK(locksGCRecord.Id), locksGCRecord)
}

func (t *gcRecordsTable) Delete(txn *store.Txn, locksGCRecord *corepb.LocksGarbageCollectionRecord) error {
	return t.table.Delete(txn, t.tablePK(locksGCRecord.Id))
}

func (t *gcRecordsTable) List(txn *store.Txn, limit int) ([]*corepb.LocksGarbageCollectionRecord, error) {
	result, err := t.table.ListPaginated(txn, nil, nil, limit)
	if err != nil {
		return nil, err
	}
	return result.Items, nil
}

func (t *gcRecordsTable) tablePK(gcRecordId uint64) []byte {
	return utils.ConcatBytes(
		t.shardGlobalIndexPrefix,
		gcRecordId,
	)
}
