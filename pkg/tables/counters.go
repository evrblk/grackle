package tables

import (
	"encoding"
	"errors"
	"fmt"

	"github.com/evrblk/monstera/store"
	"github.com/evrblk/monstera/utils"
	"github.com/evrblk/yellowstone-common/honey"

	"github.com/evrblk/grackle/pkg/sharding"
)

type ptr[T any] interface {
	*T
	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler
}

// CountersTable stores counters indexed by namespace id.
//
// Exclusive-store layout (CoreTypePersistedExclusive): tableId embeds the
// shard-unique prefix, keys carry no shard key material, and bounds checking
// is the owning core's job — honey gets nil bounds.
//
// Table Primary Key:
// 1. account id
// 2. namespace id
type CountersTable[T ptr[U], U any] struct {
	table *honey.BinaryTable[T, U]
}

func NewCountersTable[T ptr[U], U any](tableId []byte) *CountersTable[T, U] {
	return &CountersTable[T, U]{
		table: honey.NewBinaryTable[T, U](tableId, nil, nil),
	}
}

func (t *CountersTable[T, U]) GetTableKeyRange() honey.KeyRange {
	return t.table.GetTableKeyRange()
}

func (t *CountersTable[T, U]) Get(txn *store.Txn, accountId uint64, namespaceId uint64) (T, error) {
	counters, err := t.table.Get(txn, t.tablePK(accountId, namespaceId))
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			var counters U
			return &counters, nil
		}
		return nil, err
	}
	return counters, nil
}

func (t *CountersTable[T, U]) Set(txn *store.Txn, accountId uint64, namespaceId uint64, counters T) error {
	return t.table.Set(txn, t.tablePK(accountId, namespaceId), counters)
}

func (t *CountersTable[T, U]) Delete(txn *store.Txn, accountId uint64, namespaceId uint64) error {
	return t.table.Delete(txn, t.tablePK(accountId, namespaceId))
}

// Clear deletes every counter row.
func (t *CountersTable[T, U]) Clear(badgerStore *store.BadgerStore) error {
	return badgerStore.DeletePrefix(t.table.TableId())
}

// EachEntity streams every counter as (canonical key, stored value).
func (t *CountersTable[T, U]) EachEntity(txn *store.Txn, fn func(key []byte, value []byte) (bool, error)) error {
	return t.table.EachEntry(txn, fn)
}

// RestoreEntity decodes one streamed counter and, if owned, inserts it. A
// counter value carries no identity of its own — the identity lives in the
// canonical key, whose fixed-width layout this table defines (see tablePK):
// <8-byte account id><8-byte namespace id>.
func (t *CountersTable[T, U]) RestoreEntity(txn *store.Txn, key []byte, value []byte, bounds ShardRange) (bool, error) {
	if len(key) != 8+8 {
		return false, fmt.Errorf("counter key has %d bytes, want %d", len(key), 8+8)
	}
	accountId := utils.BytesToUint64(key[0:8])
	namespaceId := utils.BytesToUint64(key[8:16])
	if !bounds.Owns(sharding.ByAccountAndNamespace(accountId, namespaceId)) {
		return false, nil
	}

	var counter U
	if err := T(&counter).UnmarshalBinary(value); err != nil {
		return false, err
	}
	return true, t.Set(txn, accountId, namespaceId, &counter)
}

func (t *CountersTable[T, U]) tablePK(accountId uint64, namespaceId uint64) []byte {
	return utils.ConcatBytes(
		accountId,
		namespaceId,
	)
}
