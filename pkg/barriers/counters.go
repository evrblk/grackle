package barriers

import (
	"errors"

	"github.com/evrblk/monstera/store"
	"github.com/evrblk/monstera/utils"
	monsterax "github.com/evrblk/monstera/x"

	"github.com/evrblk/grackle/pkg/corepb"
	"github.com/evrblk/grackle/pkg/sharding"
	"github.com/evrblk/grackle/pkg/tables"
)

// countersTable
//
// Table Primary Key:
// 1. shard key (by account id and namespace id)
// 2. account id
// 3. namespace id
type countersTable struct {
	table *monsterax.BinaryTable[*corepb.BarriersCounter, corepb.BarriersCounter]
}

func newCountersTable(shardLowerBound []byte, shardUpperBound []byte) *countersTable {
	return &countersTable{
		table: monsterax.NewBinaryTable[*corepb.BarriersCounter, corepb.BarriersCounter](
			tables.Grackle["Grackle.BarriersCore.Counters.Table"].Bytes(),
			shardLowerBound,
			shardUpperBound,
		),
	}
}

func (t *countersTable) GetTableKeyRange() monsterax.KeyRange {
	return t.table.GetTableKeyRange()
}

func (t *countersTable) Get(txn *store.Txn, accountId uint64, namespaceId uint32) (*corepb.BarriersCounter, error) {
	counters, err := t.table.Get(txn, t.tablePK(accountId, namespaceId))
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return &corepb.BarriersCounter{
				NamespaceId: &corepb.NamespaceId{
					AccountId:   accountId,
					NamespaceId: namespaceId,
				},
				NumberOfBarriers: 0,
			}, nil
		}
		return nil, err
	}
	return counters, nil
}

func (t *countersTable) Set(txn *store.Txn, accountId uint64, namespaceId uint32, counters *corepb.BarriersCounter) error {
	return t.table.Set(txn, t.tablePK(accountId, namespaceId), counters)
}

func (t *countersTable) Delete(txn *store.Txn, accountId uint64, namespaceId uint32) error {
	return t.table.Delete(txn, t.tablePK(accountId, namespaceId))
}

func (t *countersTable) tablePK(accountId uint64, namespaceId uint32) []byte {
	return utils.ConcatBytes(
		sharding.ByAccountAndNamespace(accountId, namespaceId),
		accountId,
		namespaceId,
	)
}
