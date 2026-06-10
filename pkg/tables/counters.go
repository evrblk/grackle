package tables

import (
	"encoding"
	"errors"

	"github.com/evrblk/monstera/store"
	"github.com/evrblk/monstera/utils"
	monsterax "github.com/evrblk/monstera/x"

	"github.com/evrblk/grackle/pkg/sharding"
)

type ptr[T any] interface {
	*T
	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler
}

// CountersTable stores counters indexed by namespace id
//
// Table Primary Key:
// 1. shard key (by account id and namespace id)
// 2. account id
// 3. namespace id
type CountersTable[T ptr[U], U any] struct {
	table *monsterax.BinaryTable[T, U]
}

func NewCountersTable[T ptr[U], U any](tableId []byte, shardLowerBound []byte, shardUpperBound []byte) *CountersTable[T, U] {
	return &CountersTable[T, U]{
		table: monsterax.NewBinaryTable[T, U](tableId, shardLowerBound, shardUpperBound),
	}
}

func (t *CountersTable[T, U]) GetTableKeyRange() monsterax.KeyRange {
	return t.table.GetTableKeyRange()
}

func (t *CountersTable[T, U]) Get(txn *store.Txn, accountId uint64, namespaceId uint32) (T, error) {
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

func (t *CountersTable[T, U]) Set(txn *store.Txn, accountId uint64, namespaceId uint32, counters T) error {
	return t.table.Set(txn, t.tablePK(accountId, namespaceId), counters)
}

func (t *CountersTable[T, U]) Delete(txn *store.Txn, accountId uint64, namespaceId uint32) error {
	return t.table.Delete(txn, t.tablePK(accountId, namespaceId))
}

func (t *CountersTable[T, U]) tablePK(accountId uint64, namespaceId uint32) []byte {
	return utils.ConcatBytes(
		sharding.ByAccountAndNamespace(accountId, namespaceId),
		accountId,
		namespaceId,
	)
}
