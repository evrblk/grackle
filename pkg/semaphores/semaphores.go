package semaphores

import (
	"errors"
	"fmt"

	mrpc "github.com/evrblk/monstera/rpc"
	"github.com/evrblk/monstera/store"
	"github.com/evrblk/monstera/utils"
	"github.com/evrblk/yellowstone-common/honey"

	"github.com/evrblk/grackle/pkg/corepb"
	"github.com/evrblk/grackle/pkg/pagination"
	"github.com/evrblk/grackle/pkg/sharding"
	"github.com/evrblk/grackle/pkg/tables"
)

// semaphoresTable is a table of semaphores indexed by semaphore ID and semaphore name.
//
// Table Primary Key:
// 1. account id
// 2. namespace id
//
// Table Sort Key:
// 1. semaphore id
//
// Names Index Primary Key:
// 1. account id
// 2. namespace id
// 3. semaphore name
type semaphoresTable struct {
	table      *honey.BinaryTable[*corepb.Semaphore, corepb.Semaphore]
	namesIndex *honey.Uint64Table
}

// newSemaphoresTable scopes both tables under the shard-unique prefix
// (nested under the registry table ids), so no row is shared with any other
// core (CoreTypePersistedExclusive). Keys carry no shard key material — the
// prefix is the isolation, so honey gets nil bounds; routing violations are
// rejected upstream by the generated core adapter.
func newSemaphoresTable(shardPrefix []byte) *semaphoresTable {
	return &semaphoresTable{
		table: honey.NewBinaryTable[*corepb.Semaphore, corepb.Semaphore](
			utils.ConcatBytes(tables.Grackle["Grackle.SemaphoresCore.Semaphores.Table"].Bytes(), shardPrefix),
			nil,
			nil,
		),
		namesIndex: honey.NewUint64Table(
			utils.ConcatBytes(tables.Grackle["Grackle.SemaphoresCore.Semaphores.NamesIndex"].Bytes(), shardPrefix),
			nil,
			nil,
		),
	}
}

// Clear deletes every row this table owns: the primary semaphore rows and
// the names index.
func (t *semaphoresTable) Clear(badgerStore *store.BadgerStore) error {
	for _, prefix := range [][]byte{t.table.TableId(), t.namesIndex.TableId()} {
		if err := badgerStore.DropPrefix(prefix); err != nil {
			return err
		}
	}
	return nil
}

// EachEntity streams every semaphore as (canonical key, stored value) — the
// primary table only; the names index is rebuilt from the semaphores on
// restore.
func (t *semaphoresTable) EachEntity(txn *store.Txn, fn func(key []byte, value []byte) (bool, error)) error {
	return t.table.EachEntry(txn, fn)
}

// RestoreEntity decodes one streamed semaphore and, if owned, inserts it
// directly (bypassing Create's uniqueness gates — the stream is
// authoritative), rebuilding the names index from the semaphore's own
// identity fields.
func (t *semaphoresTable) RestoreEntity(txn *store.Txn, key []byte, value []byte, bounds tables.ShardRange) (bool, error) {
	semaphore := &corepb.Semaphore{}
	if err := semaphore.UnmarshalBinary(value); err != nil {
		return false, err
	}
	if !bounds.Owns(sharding.ByAccountAndNamespace(semaphore.Id.AccountId, semaphore.Id.NamespaceId)) {
		return false, nil
	}
	err := t.namesIndex.Set(txn, t.namesIndexPK(semaphore.Id.AccountId, semaphore.Id.NamespaceId, semaphore.Name), semaphore.Id.SemaphoreId)
	if err != nil {
		return false, err
	}
	return true, t.Update(txn, semaphore)
}

func (t *semaphoresTable) Get(txn *store.Txn, semaphoreId *corepb.SemaphoreId) (*corepb.Semaphore, error) {
	return t.table.Get(txn,
		utils.ConcatBytes(
			t.tablePK(semaphoreId.AccountId, semaphoreId.NamespaceId),
			t.tableSK(semaphoreId.SemaphoreId)))
}

func (t *semaphoresTable) GetByName(txn *store.Txn, accountId uint64, namespaceId uint64, semaphoreName string) (*corepb.Semaphore, error) {
	semaphoreId, err := t.namesIndex.Get(txn, t.namesIndexPK(accountId, namespaceId, semaphoreName))
	if err != nil {
		return nil, err
	}
	return t.Get(txn, &corepb.SemaphoreId{
		AccountId:   accountId,
		NamespaceId: namespaceId,
		SemaphoreId: semaphoreId,
	})
}

func (t *semaphoresTable) Update(txn *store.Txn, semaphore *corepb.Semaphore) error {
	return t.table.Set(txn,
		utils.ConcatBytes(
			t.tablePK(semaphore.Id.AccountId, semaphore.Id.NamespaceId),
			t.tableSK(semaphore.Id.SemaphoreId)),
		semaphore)
}

func (t *semaphoresTable) Delete(txn *store.Txn, semaphoreId *corepb.SemaphoreId) error {
	semaphore, err := t.Get(txn, semaphoreId)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil
		}
		return err
	}

	err = t.namesIndex.Delete(txn, t.namesIndexPK(semaphore.Id.AccountId, semaphore.Id.NamespaceId, semaphore.Name))
	if err != nil {
		return err
	}

	return t.table.Delete(txn,
		utils.ConcatBytes(
			t.tablePK(semaphoreId.AccountId, semaphoreId.NamespaceId),
			t.tableSK(semaphoreId.SemaphoreId)))
}

func (t *semaphoresTable) Create(txn *store.Txn, semaphore *corepb.Semaphore) (*mrpc.Error, error) {
	indexPK := t.namesIndexPK(semaphore.Id.AccountId, semaphore.Id.NamespaceId, semaphore.Name)
	_, err := t.namesIndex.Get(txn, indexPK)
	if err != nil {
		if !errors.Is(err, store.ErrNotFound) {
			return nil, err
		}
	} else {
		return mrpc.NewErrorWithContext(
			mrpc.AlreadyExists,
			"semaphore with this name already exists",
			map[string]string{
				"semaphore_name": semaphore.Name,
			}), nil
	}

	// Checking ID uniqueness. The ID is randomly generated by the caller, so a
	// collision is expected to be rare; when it happens we return IDCollision so
	// the caller can regenerate the ID and retry. This is not a user-facing error.
	// Without this check t.table.Set would silently overwrite the colliding semaphore.
	_, err = t.Get(txn, semaphore.Id)
	if err != nil {
		if !errors.Is(err, store.ErrNotFound) {
			return nil, err
		}
	} else {
		return mrpc.NewErrorWithContext(
			mrpc.IDCollision,
			"semaphore with this id already exists",
			map[string]string{
				"semaphore_id": fmt.Sprintf("%d", semaphore.Id.SemaphoreId),
			}), nil
	}

	err = t.namesIndex.Set(txn, indexPK, semaphore.Id.SemaphoreId)
	if err != nil {
		return nil, err
	}

	return nil, t.table.Set(txn,
		utils.ConcatBytes(
			t.tablePK(semaphore.Id.AccountId, semaphore.Id.NamespaceId),
			t.tableSK(semaphore.Id.SemaphoreId)),
		semaphore)
}

type listSemaphoresResult struct {
	semaphores              []*corepb.Semaphore
	nextPaginationToken     *corepb.PaginationToken
	previousPaginationToken *corepb.PaginationToken
}

func (t *semaphoresTable) List(txn *store.Txn, accountId uint64, namespaceId uint64, paginationToken *corepb.PaginationToken, limit int) (*listSemaphoresResult, error) {
	result, err := t.table.ListPaginated(txn, t.tablePK(accountId, namespaceId), pagination.CoreToMonstera(paginationToken), limit)
	if err != nil {
		return nil, err
	}

	return &listSemaphoresResult{
		semaphores:              result.Items,
		nextPaginationToken:     pagination.MonsteraToCore(result.NextPaginationToken),
		previousPaginationToken: pagination.MonsteraToCore(result.PreviousPaginationToken),
	}, nil
}

func (t *semaphoresTable) tablePK(accountId uint64, namespaceId uint64) []byte {
	return utils.ConcatBytes(
		accountId,
		namespaceId,
	)
}

func (t *semaphoresTable) tableSK(semaphoreId uint64) []byte {
	return utils.ConcatBytes(
		semaphoreId,
	)
}

func (t *semaphoresTable) namesIndexPK(accountId uint64, namespaceId uint64, semaphoreName string) []byte {
	return utils.ConcatBytes(
		accountId,
		namespaceId,
		semaphoreName,
	)
}
