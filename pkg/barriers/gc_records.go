package barriers

import (
	"github.com/evrblk/monstera/store"
	"github.com/evrblk/monstera/utils"
	monsterax "github.com/evrblk/monstera/x"

	"github.com/evrblk/grackle/pkg/corepb"
	"github.com/evrblk/grackle/pkg/tables"
)

// gcRecordsTable stores GC records for deleted namespaces and barriers.
//
// Table Primary Key:
// 1. shard id
// 2. gc record id
type gcRecordsTable struct {
	shardGlobalIndexPrefix []byte

	table *monsterax.BinaryTable[*corepb.BarriersGarbageCollectionRecord, corepb.BarriersGarbageCollectionRecord]
}

func newGCRecordsTable(shardGlobalIndexPrefix []byte) *gcRecordsTable {
	return &gcRecordsTable{
		shardGlobalIndexPrefix: shardGlobalIndexPrefix,

		table: monsterax.NewBinaryTable[*corepb.BarriersGarbageCollectionRecord, corepb.BarriersGarbageCollectionRecord](
			tables.GrackleBarriersGarbageCollectionRecordsTableId,
			shardGlobalIndexPrefix,
			shardGlobalIndexPrefix,
		),
	}
}

func (t *gcRecordsTable) GetTableKeyRange() monsterax.KeyRange {
	return t.table.GetTableKeyRange()
}

func (t *gcRecordsTable) Create(txn *store.Txn, gcRecord *corepb.BarriersGarbageCollectionRecord) error {
	return t.table.Set(txn, t.tablePK(gcRecord.Id), gcRecord)
}

func (t *gcRecordsTable) Delete(txn *store.Txn, gcRecord *corepb.BarriersGarbageCollectionRecord) error {
	return t.table.Delete(txn, t.tablePK(gcRecord.Id))
}

func (t *gcRecordsTable) List(txn *store.Txn, limit int) ([]*corepb.BarriersGarbageCollectionRecord, error) {
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
