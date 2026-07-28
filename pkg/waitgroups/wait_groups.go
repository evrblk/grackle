package waitgroups

import (
	"errors"

	"github.com/evrblk/monstera/store"
	"github.com/evrblk/monstera/utils"
	"github.com/evrblk/yellowstone-common/honey"

	"github.com/evrblk/grackle/pkg/corepb"
	"github.com/evrblk/grackle/pkg/pagination"
	"github.com/evrblk/grackle/pkg/sharding"
	"github.com/evrblk/grackle/pkg/tables"
)

// waitGroupsTable is a table of wait groups indexed by wait group ID and wait group name.
//
// Table Primary Key:
// 1. account id
// 2. namespace id
//
// Table Sort Key:
// 1. wait group id
//
// Names Index Primary Key:
// 1. account id
// 2. namespace id
// 3. wait group name
type waitGroupsTable struct {
	table      *honey.BinaryTable[*corepb.WaitGroup, corepb.WaitGroup]
	namesIndex *honey.Uint64Table
}

// newWaitGroupsTable scopes both tables under the shard-unique prefix
// (nested under the registry table ids), so no row is shared with any other
// core (CoreTypePersistedExclusive). Keys carry no shard key material — the
// prefix is the isolation, so honey gets nil bounds; routing violations are
// rejected upstream by the generated core adapter.
func newWaitGroupsTable(shardPrefix []byte) *waitGroupsTable {
	return &waitGroupsTable{
		table: honey.NewBinaryTable[*corepb.WaitGroup, corepb.WaitGroup](
			utils.ConcatBytes(tables.Grackle["Grackle.WaitGroupsCore.WaitGroups.Table"].Bytes(), shardPrefix),
			nil,
			nil,
		),
		namesIndex: honey.NewUint64Table(
			utils.ConcatBytes(tables.Grackle["Grackle.WaitGroupsCore.WaitGroups.NamesIndex"].Bytes(), shardPrefix),
			nil,
			nil,
		),
	}
}

// Clear deletes every row this table owns: the primary wait group rows and
// the names index.
func (t *waitGroupsTable) Clear(badgerStore *store.BadgerStore) error {
	for _, prefix := range [][]byte{t.table.TableId(), t.namesIndex.TableId()} {
		if err := badgerStore.DeletePrefix(prefix); err != nil {
			return err
		}
	}
	return nil
}

// EachEntity streams every wait group as (canonical key, stored value) — the
// primary table only; the names index is rebuilt from the wait groups on
// restore.
func (t *waitGroupsTable) EachEntity(txn *store.Txn, fn func(key []byte, value []byte) (bool, error)) error {
	return t.table.EachEntry(txn, fn)
}

// RestoreEntity decodes one streamed wait group and, if owned, inserts it
// through Create — re-deriving its keys and rebuilding the names index from
// the wait group's own identity fields.
func (t *waitGroupsTable) RestoreEntity(txn *store.Txn, key []byte, value []byte, bounds tables.ShardRange) (bool, error) {
	waitGroup := &corepb.WaitGroup{}
	if err := waitGroup.UnmarshalBinary(value); err != nil {
		return false, err
	}
	if !bounds.Owns(sharding.ByAccountAndNamespace(waitGroup.Id.AccountId, waitGroup.Id.NamespaceId)) {
		return false, nil
	}
	return true, t.Create(txn, waitGroup)
}

func (t *waitGroupsTable) Get(txn *store.Txn, waitGroupId *corepb.WaitGroupId) (*corepb.WaitGroup, error) {
	return t.table.Get(txn,
		utils.ConcatBytes(
			t.tablePK(waitGroupId.AccountId, waitGroupId.NamespaceId),
			t.tableSK(waitGroupId.WaitGroupId)))
}

func (t *waitGroupsTable) GetByName(txn *store.Txn, accountId uint64, namespaceId uint64, waitGroupName string) (*corepb.WaitGroup, error) {
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

func (t *waitGroupsTable) List(txn *store.Txn, accountId uint64, namespaceId uint64, paginationToken *corepb.PaginationToken, limit int) (*listWaitGroupsResult, error) {
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

func (t *waitGroupsTable) tablePK(accountId uint64, namespaceId uint64) []byte {
	return utils.ConcatBytes(
		accountId,
		namespaceId,
	)
}

func (t *waitGroupsTable) tableSK(waitGroupId uint64) []byte {
	return utils.ConcatBytes(
		waitGroupId,
	)
}

func (t *waitGroupsTable) namesIndexPK(accountId uint64, namespaceId uint64, waitGroupName string) []byte {
	return utils.ConcatBytes(
		accountId,
		namespaceId,
		waitGroupName,
	)
}
