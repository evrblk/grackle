package tables

import (
	"encoding"
	"fmt"

	"github.com/evrblk/monstera/store"
	"github.com/evrblk/monstera/utils"
	"github.com/evrblk/yellowstone-common/honey"

	"github.com/evrblk/grackle/pkg/sharding"
)

type gcptr[T any] interface {
	*T
	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler
	GetId() uint64
	// Identity returns the (account id, namespace id) of the entity the
	// record targets, regardless of which oneof variant it carries (see
	// corepb/gc_identity.go); portable snapshot restores filter by it. ok is
	// false when the record carries no target.
	Identity() (accountId uint64, namespaceId uint64, ok bool)
}

// GCRecordsTable stores GC records for deleted namespaces and entities.
//
// Table Primary Key:
// 1. shard id
// 2. gc record id
type GCRecordsTable[T gcptr[U], U any] struct {
	shardGlobalIndexPrefix []byte

	table *honey.BinaryTable[T, U]
}

func NewGCRecordsTable[T gcptr[U], U any](tableId []byte, shardGlobalIndexPrefix []byte) *GCRecordsTable[T, U] {
	return &GCRecordsTable[T, U]{
		shardGlobalIndexPrefix: shardGlobalIndexPrefix,

		table: honey.NewBinaryTable[T, U](tableId, shardGlobalIndexPrefix, shardGlobalIndexPrefix),
	}
}

func (t *GCRecordsTable[T, U]) GetTableKeyRange() honey.KeyRange {
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

// Clear deletes every GC record row of this shard (keys carry the shard
// prefix in-key under the registry table id).
func (t *GCRecordsTable[T, U]) Clear(badgerStore *store.BadgerStore) error {
	return badgerStore.DropPrefix(utils.ConcatBytes(t.table.TableId(), t.shardGlobalIndexPrefix))
}

// EachEntity streams every GC record as (canonical key, stored value). The
// stored key embeds this shard's global index prefix — identity of the
// producing shard, which must not travel in a portable stream — so the
// canonical key is the record id alone.
func (t *GCRecordsTable[T, U]) EachEntity(txn *store.Txn, fn func(key []byte, value []byte) (bool, error)) error {
	return t.table.EachEntry(txn, func(key []byte, value []byte) (bool, error) {
		return fn(key[len(t.shardGlobalIndexPrefix):], value)
	})
}

// RestoreEntity decodes one streamed GC record and, if owned, inserts it
// through Create — which re-derives its key under this table's own prefix
// (the canonical key is not needed; identity comes from the record itself).
func (t *GCRecordsTable[T, U]) RestoreEntity(txn *store.Txn, key []byte, value []byte, bounds ShardRange) (bool, error) {
	var record U
	if err := T(&record).UnmarshalBinary(value); err != nil {
		return false, err
	}
	accountId, namespaceId, ok := T(&record).Identity()
	if !ok {
		return false, fmt.Errorf("gc record %d carries no target identity", T(&record).GetId())
	}
	if !bounds.Owns(sharding.ByAccountAndNamespace(accountId, namespaceId)) {
		return false, nil
	}
	return true, t.Create(txn, T(&record))
}

func (t *GCRecordsTable[T, U]) tablePK(gcRecordId uint64) []byte {
	return utils.ConcatBytes(
		t.shardGlobalIndexPrefix,
		gcRecordId,
	)
}
