package waitgroups

import (
	"errors"

	"github.com/evrblk/monstera/store"
	"github.com/evrblk/monstera/utils"
	monsterax "github.com/evrblk/monstera/x"

	"github.com/evrblk/grackle/pkg/corepb"
	"github.com/evrblk/grackle/pkg/sharding"
	"github.com/evrblk/grackle/pkg/tables"
)

// countersTable is a table for wait group counters indexed by namespace ID.
//
// Table Primary Key:
// 1. shard key (by account id and namespace id)
// 2. account id
// 3. namespace id
type countersTable struct {
	table *monsterax.BinaryTable[*corepb.WaitGroupsCounter, corepb.WaitGroupsCounter]
}

func newCountersTable(shardLowerBound []byte, shardUpperBound []byte) *countersTable {
	return &countersTable{
		table: monsterax.NewBinaryTable[*corepb.WaitGroupsCounter, corepb.WaitGroupsCounter](
			tables.GrackleWaitGroupsCountersTableId,
			shardLowerBound,
			shardUpperBound,
		),
	}
}

func (t *countersTable) GetTableKeyRange() monsterax.KeyRange {
	return t.table.GetTableKeyRange()
}

func (t *countersTable) Get(txn *store.Txn, accountId uint64, namespaceId uint32) (*corepb.WaitGroupsCounter, error) {
	counters, err := t.table.Get(txn, t.tablePK(accountId, namespaceId))
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return &corepb.WaitGroupsCounter{
				NamespaceId: &corepb.NamespaceId{
					AccountId:   accountId,
					NamespaceId: namespaceId,
				},
				NumberOfWaitGroups: 0,
			}, nil
		}
		return nil, err
	}
	return counters, nil
}

func (t *countersTable) Set(txn *store.Txn, accountId uint64, namespaceId uint32, counters *corepb.WaitGroupsCounter) error {
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
