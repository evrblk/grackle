package waitgroups

import (
	"errors"

	"github.com/evrblk/monstera/store"
	"github.com/evrblk/monstera/utils"
	monsterax "github.com/evrblk/monstera/x"

	"github.com/evrblk/grackle/pkg/corepb"
	"github.com/evrblk/grackle/pkg/pagination"
	"github.com/evrblk/grackle/pkg/sharding"
	"github.com/evrblk/grackle/pkg/tables"
)

// waitGroupsTable is a table of wait groups indexed by wait group ID and wait group name.
//
// Table Primary Key:
// 1. shard key (by account id and namespace id)
// 2. account id
// 3. namespace id
//
// Table Sort Key:
// 1. wait group id
//
// Names Index Primary Key:
// 1. shard key (by account id and namespace id)
// 2. account id
// 3. namespace id
// 4. wait group name
type waitGroupsTable struct {
	table      *monsterax.BinaryTable[*corepb.WaitGroup, corepb.WaitGroup]
	namesIndex *monsterax.Uint64Table
}

func newWaitGroupsTable(shardLowerBound []byte, shardUpperBound []byte) *waitGroupsTable {
	return &waitGroupsTable{
		table: monsterax.NewBinaryTable[*corepb.WaitGroup, corepb.WaitGroup](
			tables.Grackle["Grackle.WaitGroupsCore.WaitGroups.Table"].Bytes(),
			shardLowerBound,
			shardUpperBound,
		),
		namesIndex: monsterax.NewUint64Table(
			tables.Grackle["Grackle.WaitGroupsCore.WaitGroups.NamesIndex"].Bytes(),
			shardLowerBound,
			shardUpperBound,
		),
	}
}

func (t *waitGroupsTable) GetTableKeyRanges() []monsterax.KeyRange {
	return []monsterax.KeyRange{
		t.table.GetTableKeyRange(),
		t.namesIndex.GetTableKeyRange(),
	}
}

func (t *waitGroupsTable) Get(txn *store.Txn, waitGroupId *corepb.WaitGroupId) (*corepb.WaitGroup, error) {
	return t.table.Get(txn,
		utils.ConcatBytes(
			t.tablePK(waitGroupId.AccountId, waitGroupId.NamespaceId),
			t.tableSK(waitGroupId.WaitGroupId)))
}

func (t *waitGroupsTable) GetByName(txn *store.Txn, accountId uint64, namespaceId uint32, waitGroupName string) (*corepb.WaitGroup, error) {
	waitGroupId, err := t.namesIndex.Get(txn, t.namesIndexPK(accountId, namespaceId, waitGroupName))
	if err != nil {
		return nil, err
	}

	return t.Get(txn, &corepb.WaitGroupId{
		AccountId:   accountId,
		NamespaceId: namespaceId,
		WaitGroupId: waitGroupId,
	})
}

type listWaitGroupsResult struct {
	waitGroups              []*corepb.WaitGroup
	nextPaginationToken     *corepb.PaginationToken
	previousPaginationToken *corepb.PaginationToken
}

func (t *waitGroupsTable) List(txn *store.Txn, accountId uint64, namespaceId uint32, paginationToken *corepb.PaginationToken, limit int) (*listWaitGroupsResult, error) {
	result, err := t.table.ListPaginated(txn, t.tablePK(accountId, namespaceId), pagination.CoreToMonstera(paginationToken), limit)
	if err != nil {
		return nil, err
	}

	return &listWaitGroupsResult{
		waitGroups:              result.Items,
		nextPaginationToken:     pagination.MonsteraToCore(result.NextPaginationToken),
		previousPaginationToken: pagination.MonsteraToCore(result.PreviousPaginationToken),
	}, nil
}

func (t *waitGroupsTable) Create(txn *store.Txn, waitGroup *corepb.WaitGroup) error {
	err := t.namesIndex.Set(txn, t.namesIndexPK(waitGroup.Id.AccountId, waitGroup.Id.NamespaceId, waitGroup.Name), waitGroup.Id.WaitGroupId)
	if err != nil {
		return err
	}

	return t.table.Set(txn,
		utils.ConcatBytes(
			t.tablePK(waitGroup.Id.AccountId, waitGroup.Id.NamespaceId),
			t.tableSK(waitGroup.Id.WaitGroupId)),
		waitGroup)
}

func (t *waitGroupsTable) Update(txn *store.Txn, waitGroup *corepb.WaitGroup) error {
	return t.table.Set(txn,
		utils.ConcatBytes(
			t.tablePK(waitGroup.Id.AccountId, waitGroup.Id.NamespaceId),
			t.tableSK(waitGroup.Id.WaitGroupId)),
		waitGroup)
}

func (t *waitGroupsTable) Delete(txn *store.Txn, waitGroupId *corepb.WaitGroupId) error {
	waitGroup, err := t.Get(txn, waitGroupId)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			// Wait group doesn't exist, nothing to delete
			return nil
		}
		return err
	}

	err = t.namesIndex.Delete(txn, t.namesIndexPK(waitGroup.Id.AccountId, waitGroup.Id.NamespaceId, waitGroup.Name))
	if err != nil {
		return err
	}

	return t.table.Delete(txn,
		utils.ConcatBytes(
			t.tablePK(waitGroupId.AccountId, waitGroupId.NamespaceId),
			t.tableSK(waitGroupId.WaitGroupId)))
}

func (t *waitGroupsTable) tablePK(accountId uint64, namespaceId uint32) []byte {
	return utils.ConcatBytes(
		sharding.ByAccountAndNamespace(accountId, namespaceId),
		accountId,
		namespaceId,
	)
}

func (t *waitGroupsTable) tableSK(waitGroupId uint64) []byte {
	return utils.ConcatBytes(
		waitGroupId,
	)
}

func (t *waitGroupsTable) namesIndexPK(accountId uint64, namespaceId uint32, waitGroupName string) []byte {
	return utils.ConcatBytes(
		sharding.ByAccountAndNamespace(accountId, namespaceId),
		accountId,
		namespaceId,
		waitGroupName,
	)
}
