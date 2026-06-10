package tables

import (
	"encoding"

	"github.com/evrblk/monstera/store"
	"github.com/evrblk/monstera/utils"
	monsterax "github.com/evrblk/monstera/x"
)

type gcptr[T any] interface {
	*T
	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler
	GetId() uint64
}

// GCRecordsTable stores GC records for deleted namespaces and entities.
//
// Table Primary Key:
// 1. shard id
// 2. gc record id
type GCRecordsTable[T gcptr[U], U any] struct {
	shardGlobalIndexPrefix []byte

	table *monsterax.BinaryTable[T, U]
}

func NewGCRecordsTable[T gcptr[U], U any](tableId []byte, shardGlobalIndexPrefix []byte) *GCRecordsTable[T, U] {
	return &GCRecordsTable[T, U]{
		shardGlobalIndexPrefix: shardGlobalIndexPrefix,

		table: monsterax.NewBinaryTable[T, U](tableId, shardGlobalIndexPrefix, shardGlobalIndexPrefix),
	}
}

func (t *GCRecordsTable[T, U]) GetTableKeyRange() monsterax.KeyRange {
	return t.table.GetTableKeyRange()
}

func (t *GCRecordsTable[T, U]) Create(txn *store.Txn, gcRecord T) error {
	return t.table.Set(txn, t.tablePK(gcRecord.GetId()), gcRecord)
}

func (t *GCRecordsTable[T, U]) Delete(txn *store.Txn, gcRecord T) error {
	return t.table.Delete(txn, t.tablePK(gcRecord.GetId()))
}

func (t *GCRecordsTable[T, U]) List(txn *store.Txn, limit int) ([]T, error) {
	result, err := t.table.ListPaginated(txn, nil, nil, limit)
	if err != nil {
		return nil, err
	}
	return result.Items, nil
}

func (t *GCRecordsTable[T, U]) tablePK(gcRecordId uint64) []byte {
	return utils.ConcatBytes(
		t.shardGlobalIndexPrefix,
		gcRecordId,
	)
}
