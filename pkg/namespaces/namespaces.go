package namespaces

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

// namespacesTable is a table of namespaces indexed by namespace ID and namespace name.
//
// Table Primary Key:
// 1. shard key (by account id)
// 2. account id
//
// Table Sort Key:
// 1. namespace id
//
// Names Index Primary Key:
// 1. shard key (by account id)
// 2. account id
// 3. namespace name
type namespacesTable struct {
	table      *monsterax.BinaryTable[*corepb.Namespace, corepb.Namespace]
	namesIndex *monsterax.Uint64Table
}

func newNamespacesTable(shardLowerBound []byte, shardUpperBound []byte) *namespacesTable {
	return &namespacesTable{
		table: monsterax.NewBinaryTable[*corepb.Namespace, corepb.Namespace](
			tables.Grackle["Grackle.NamespacesCore.Namespaces.Table"].Bytes(),
			shardLowerBound,
			shardUpperBound,
		),
		namesIndex: monsterax.NewUint64Table(
			tables.Grackle["Grackle.NamespacesCore.Namespaces.NamesIndex"].Bytes(),
			shardLowerBound,
			shardUpperBound,
		),
	}
}

func (t *namespacesTable) GetTableKeyRanges() []monsterax.KeyRange {
	return []monsterax.KeyRange{
		t.table.GetTableKeyRange(),
		t.namesIndex.GetTableKeyRange(),
	}
}

func (t *namespacesTable) Get(txn *store.Txn, namespaceId *corepb.NamespaceId) (*corepb.Namespace, error) {
	return t.table.Get(txn,
		utils.ConcatBytes(
			t.tablePK(namespaceId.AccountId),
			t.tableSK(namespaceId.NamespaceId)))
}

func (t *namespacesTable) GetByName(txn *store.Txn, accountId uint64, namespaceName string) (*corepb.Namespace, error) {
	namespaceId, err := t.namesIndex.Get(txn, t.namesIndexPK(accountId, namespaceName))
	if err != nil {
		return nil, err
	}

	return t.Get(txn, &corepb.NamespaceId{
		AccountId:   accountId,
		NamespaceId: namespaceId,
	})
}

type listNamespacesResult struct {
	Namespaces              []*corepb.Namespace
	NextPaginationToken     *corepb.PaginationToken
	PreviousPaginationToken *corepb.PaginationToken
}

func (t *namespacesTable) List(txn *store.Txn, accountId uint64, paginationToken *corepb.PaginationToken, limit int) (*listNamespacesResult, error) {
	result, err := t.table.ListPaginated(txn, t.tablePK(accountId), pagination.CoreToMonstera(paginationToken), limit)
	if err != nil {
		return nil, err
	}

	return &listNamespacesResult{
		Namespaces:              result.Items,
		NextPaginationToken:     pagination.MonsteraToCore(result.NextPaginationToken),
		PreviousPaginationToken: pagination.MonsteraToCore(result.PreviousPaginationToken),
	}, nil
}

func (t *namespacesTable) Create(txn *store.Txn, namespace *corepb.Namespace) error {
	err := t.namesIndex.Set(txn, t.namesIndexPK(namespace.Id.AccountId, namespace.Name), namespace.Id.NamespaceId)
	if err != nil {
		return err
	}

	return t.table.Set(txn,
		utils.ConcatBytes(
			t.tablePK(namespace.Id.AccountId),
			t.tableSK(namespace.Id.NamespaceId)),
		namespace)
}

func (t *namespacesTable) Update(txn *store.Txn, namespace *corepb.Namespace) error {
	return t.table.Set(txn,
		utils.ConcatBytes(
			t.tablePK(namespace.Id.AccountId),
			t.tableSK(namespace.Id.NamespaceId)),
		namespace)
}

func (t *namespacesTable) Delete(txn *store.Txn, namespace *corepb.Namespace) error {
	// Delete from names index (ignore if not found)
	err := t.namesIndex.Delete(txn, t.namesIndexPK(namespace.Id.AccountId, namespace.Name))
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return err
	}

	// Delete from main table (ignore if not found)
	err = t.table.Delete(txn,
		utils.ConcatBytes(
			t.tablePK(namespace.Id.AccountId),
			t.tableSK(namespace.Id.NamespaceId)))
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return err
	}

	return nil
}

func (t *namespacesTable) tablePK(accountId uint64) []byte {
	return utils.ConcatBytes(
		sharding.ByAccount(accountId),
		accountId,
	)
}

func (t *namespacesTable) tableSK(namespaceId uint64) []byte {
	return utils.ConcatBytes(
		namespaceId,
	)
}

func (t *namespacesTable) namesIndexPK(accountId uint64, namespaceName string) []byte {
	return utils.ConcatBytes(
		sharding.ByAccount(accountId),
		accountId,
		namespaceName,
	)
}
