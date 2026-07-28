package namespaces

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

// namespacesTable is a table of namespaces indexed by namespace ID and namespace name.
//
// Table Primary Key:
// 1. account id
//
// Table Sort Key:
// 1. namespace id
//
// Names Index Primary Key:
// 1. account id
// 2. namespace name
type namespacesTable struct {
	table      *honey.BinaryTable[*corepb.Namespace, corepb.Namespace]
	namesIndex *honey.Uint64Table
}

// newNamespacesTable scopes the table and the names index under the
// shard-unique prefix (nested under the registry table ids), so no row is
// shared with any other core (CoreTypePersistedExclusive). Keys carry no
// shard key material — the prefix is the isolation, so honey gets nil bounds;
// routing violations are rejected upstream by the generated core adapter.
func newNamespacesTable(shardPrefix []byte) *namespacesTable {
	return &namespacesTable{
		table: honey.NewBinaryTable[*corepb.Namespace, corepb.Namespace](
			utils.ConcatBytes(tables.Grackle["Grackle.NamespacesCore.Namespaces.Table"].Bytes(), shardPrefix),
			nil,
			nil,
		),
		namesIndex: honey.NewUint64Table(
			utils.ConcatBytes(tables.Grackle["Grackle.NamespacesCore.Namespaces.NamesIndex"].Bytes(), shardPrefix),
			nil,
			nil,
		),
	}
}

// Clear deletes every row this table owns: the primary namespace rows and
// the names index.
func (t *namespacesTable) Clear(badgerStore *store.BadgerStore) error {
	for _, prefix := range [][]byte{t.table.TableId(), t.namesIndex.TableId()} {
		if err := badgerStore.DeletePrefix(prefix); err != nil {
			return err
		}
	}
	return nil
}

// EachEntity streams every namespace as (canonical key, stored value) — the
// primary table only; the names index is rebuilt from the namespaces on
// restore.
func (t *namespacesTable) EachEntity(txn *store.Txn, fn func(key []byte, value []byte) (bool, error)) error {
	return t.table.EachEntry(txn, fn)
}

// RestoreEntity decodes one streamed namespace and, if owned, inserts it
// through Create — re-deriving its keys and rebuilding the names index from
// the namespace's own identity fields.
func (t *namespacesTable) RestoreEntity(txn *store.Txn, key []byte, value []byte, bounds tables.ShardRange) (bool, error) {
	namespace := &corepb.Namespace{}
	if err := namespace.UnmarshalBinary(value); err != nil {
		return false, err
	}
	if !bounds.Owns(sharding.ByAccount(namespace.Id.AccountId)) {
		return false, nil
	}
	return true, t.Create(txn, namespace)
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
		accountId,
		namespaceName,
	)
}
