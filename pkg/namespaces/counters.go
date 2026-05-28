package namespaces

import (
	"errors"

	"github.com/evrblk/monstera/store"
	"github.com/evrblk/monstera/utils"
	monsterax "github.com/evrblk/monstera/x"

	"github.com/evrblk/grackle/pkg/corepb"
	"github.com/evrblk/grackle/pkg/sharding"
	"github.com/evrblk/grackle/pkg/tables"
)

// countersTable is a table of namespace counters indexed by account ID.
//
// Table Primary Key:
// 1. shard key (by account id)
// 2. account id
type countersTable struct {
	table *monsterax.BinaryTable[*corepb.NamespacesCounter, corepb.NamespacesCounter]
}

func newCountersTable(shardLowerBound []byte, shardUpperBound []byte) *countersTable {
	return &countersTable{
		table: monsterax.NewBinaryTable[*corepb.NamespacesCounter, corepb.NamespacesCounter](
			tables.Grackle["Grackle.NamespacesCore.Counters.Table"].Bytes(),
			shardLowerBound,
			shardUpperBound,
		),
	}
}

func (t *countersTable) GetTableKeyRange() monsterax.KeyRange {
	return t.table.GetTableKeyRange()
}

func (t *countersTable) Get(txn *store.Txn, accountId uint64) (*corepb.NamespacesCounter, error) {
	counters, err := t.table.Get(txn, t.tablePK(accountId))
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return &corepb.NamespacesCounter{}, nil
		}
		return nil, err
	}
	return counters, nil
}

func (t *countersTable) Set(txn *store.Txn, accountId uint64, counters *corepb.NamespacesCounter) error {
	return t.table.Set(txn, t.tablePK(accountId), counters)
}

func (t *countersTable) tablePK(accountId uint64) []byte {
	return utils.ConcatBytes(
		sharding.ByAccount(accountId),
		accountId,
	)
}
