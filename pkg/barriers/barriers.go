package barriers

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

// barriersTable is a table of barriers indexed by barrier ID and barrier name.
//
// Table Primary Key:
// 1. shard key (by account id and namespace id)
// 2. account id
// 3. namespace id
//
// Table Sort Key:
// 1. barrier id
//
// Names Index Primary Key:
// 1. shard key (by account id and namespace id)
// 2. account id
// 3. namespace id
// 4. barrier name
type barriersTable struct {
	table      *monsterax.BinaryTable[*corepb.Barrier, corepb.Barrier]
	namesIndex *monsterax.Uint64Table
}

func newBarriersTable(shardLowerBound []byte, shardUpperBound []byte) *barriersTable {
	return &barriersTable{
		table: monsterax.NewBinaryTable[*corepb.Barrier, corepb.Barrier](
			tables.Grackle["Grackle.BarriersCore.Barriers.Table"].Bytes(),
			shardLowerBound,
			shardUpperBound,
		),
		namesIndex: monsterax.NewUint64Table(
			tables.Grackle["Grackle.BarriersCore.Barriers.NamesIndex"].Bytes(),
			shardLowerBound,
			shardUpperBound,
		),
	}
}

func (t *barriersTable) GetTableKeyRanges() []monsterax.KeyRange {
	return []monsterax.KeyRange{
		t.table.GetTableKeyRange(),
		t.namesIndex.GetTableKeyRange(),
	}
}

func (t *barriersTable) Get(txn *store.Txn, barrierId *corepb.BarrierId) (*corepb.Barrier, error) {
	return t.table.Get(txn,
		utils.ConcatBytes(
			t.tablePK(barrierId.AccountId, barrierId.NamespaceId),
			t.tableSK(barrierId.BarrierId)))
}

func (t *barriersTable) GetByName(txn *store.Txn, accountId uint64, namespaceId uint32, barrierName string) (*corepb.Barrier, error) {
	barrierId, err := t.namesIndex.Get(txn, t.namesIndexPK(accountId, namespaceId, barrierName))
	if err != nil {
		return nil, err
	}
	return t.Get(txn, &corepb.BarrierId{
		AccountId:   accountId,
		NamespaceId: namespaceId,
		BarrierId:   barrierId,
	})
}

func (t *barriersTable) Update(txn *store.Txn, barrier *corepb.Barrier) error {
	return t.table.Set(txn,
		utils.ConcatBytes(
			t.tablePK(barrier.Id.AccountId, barrier.Id.NamespaceId),
			t.tableSK(barrier.Id.BarrierId)),
		barrier)
}

func (t *barriersTable) Delete(txn *store.Txn, barrierId *corepb.BarrierId) error {
	barrier, err := t.Get(txn, barrierId)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			// Barrier doesn't exist, nothing to delete
			return nil
		}
		return err
	}

	err = t.namesIndex.Delete(txn, t.namesIndexPK(barrier.Id.AccountId, barrier.Id.NamespaceId, barrier.Name))
	if err != nil {
		return err
	}

	return t.table.Delete(txn,
		utils.ConcatBytes(
			t.tablePK(barrierId.AccountId, barrierId.NamespaceId),
			t.tableSK(barrierId.BarrierId)))
}

func (t *barriersTable) Create(txn *store.Txn, barrier *corepb.Barrier) (*monsterax.Error, error) {
	indexPK := t.namesIndexPK(barrier.Id.AccountId, barrier.Id.NamespaceId, barrier.Name)
	_, err := t.namesIndex.Get(txn, indexPK)
	if err != nil {
		if !errors.Is(err, store.ErrNotFound) {
			return nil, err
		}
	} else {
		return monsterax.NewErrorWithContext(
			monsterax.AlreadyExists,
			"barrier with this name already exists",
			map[string]string{
				"barrier_name": barrier.Name,
			}), nil
	}

	err = t.namesIndex.Set(txn, indexPK, barrier.Id.BarrierId)
	if err != nil {
		return nil, err
	}

	return nil, t.table.Set(txn,
		utils.ConcatBytes(
			t.tablePK(barrier.Id.AccountId, barrier.Id.NamespaceId),
			t.tableSK(barrier.Id.BarrierId)),
		barrier)
}

type listBarriersResult struct {
	barriers                []*corepb.Barrier
	nextPaginationToken     *corepb.PaginationToken
	previousPaginationToken *corepb.PaginationToken
}

func (t *barriersTable) List(txn *store.Txn, accountId uint64, namespaceId uint32, paginationToken *corepb.PaginationToken, limit int) (*listBarriersResult, error) {
	result, err := t.table.ListPaginated(txn, t.tablePK(accountId, namespaceId), pagination.CoreToMonstera(paginationToken), limit)
	if err != nil {
		return nil, err
	}

	return &listBarriersResult{
		barriers:                result.Items,
		nextPaginationToken:     pagination.MonsteraToCore(result.NextPaginationToken),
		previousPaginationToken: pagination.MonsteraToCore(result.PreviousPaginationToken),
	}, nil
}

func (t *barriersTable) tablePK(accountId uint64, namespaceId uint32) []byte {
	return utils.ConcatBytes(
		sharding.ByAccountAndNamespace(accountId, namespaceId),
		accountId,
		namespaceId,
	)
}

func (t *barriersTable) tableSK(barrierId uint64) []byte {
	return utils.ConcatBytes(
		barrierId,
	)
}

func (t *barriersTable) namesIndexPK(accountId uint64, namespaceId uint32, barrierName string) []byte {
	return utils.ConcatBytes(
		sharding.ByAccountAndNamespace(accountId, namespaceId),
		accountId,
		namespaceId,
		barrierName,
	)
}
