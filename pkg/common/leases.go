package common

import (
	"github.com/evrblk/monstera/store"
	"github.com/evrblk/monstera/utils"
	monsterax "github.com/evrblk/monstera/x"

	"github.com/evrblk/grackle/pkg/corepb"
	"github.com/evrblk/grackle/pkg/pagination"
	"github.com/evrblk/grackle/pkg/sharding"
)

// LeasesTable
//
// Table Primary Key:
// 1. shard key (by account id and namespace id)
// 2. account id
// 3. namespace id
//
// Table Sort Key:
// 1. lease id
//
// Process Id Index Primary Key:
// 1. shard key (by account id and namespace id)
// 2. account id
// 3. namespace id
// 4. process id
//
// Expiration Global Index Primary Key:
// 1. shard id
// 2. timestamp
// 3. account id
// 4. namespace Id
// 5. lease id
type LeasesTable struct {
	shardGlobalIndexPrefix []byte

	table           *monsterax.BinaryTable[*corepb.Lease, corepb.Lease]
	processIdIndex  *monsterax.OneToManyUint64Index
	expirationIndex *monsterax.SortedIndex
}

func NewLeasesTable(shardLowerBound []byte, shardUpperBound []byte, shardGlobalIndexPrefix []byte, tableId []byte, processIdIndexId []byte, expirationIndexId []byte) *LeasesTable {
	return &LeasesTable{
		shardGlobalIndexPrefix: shardGlobalIndexPrefix,

		table: monsterax.NewBinaryTable[*corepb.Lease, corepb.Lease](
			tableId,
			shardLowerBound,
			shardUpperBound,
		),
		processIdIndex: monsterax.NewOneToManyUint64Index(
			processIdIndexId,
			shardLowerBound,
			shardUpperBound,
		),
		expirationIndex: monsterax.NewSortedIndex(
			expirationIndexId,
			shardGlobalIndexPrefix,
			shardGlobalIndexPrefix,
		),
	}
}

func (t *LeasesTable) GetTableKeyRanges() []monsterax.KeyRange {
	return []monsterax.KeyRange{
		t.table.GetTableKeyRange(),
		t.processIdIndex.GetTableKeyRange(),
		t.expirationIndex.GetTableKeyRange(),
	}
}

type listLeasesResult struct {
	Leases                  []*corepb.Lease
	NextPaginationToken     *corepb.PaginationToken
	PreviousPaginationToken *corepb.PaginationToken
}

func (t *LeasesTable) List(txn *store.Txn, namespaceId *corepb.NamespaceId, paginationToken *corepb.PaginationToken, limit int) (*listLeasesResult, error) {
	result, err := t.table.ListPaginated(txn,
		t.tablePK(namespaceId.AccountId, namespaceId.NamespaceId), pagination.CoreToMonstera(paginationToken), limit)
	if err != nil {
		return nil, err
	}

	return &listLeasesResult{
		Leases:                  result.Items,
		NextPaginationToken:     pagination.MonsteraToCore(result.NextPaginationToken),
		PreviousPaginationToken: pagination.MonsteraToCore(result.PreviousPaginationToken),
	}, nil
}

func (t *LeasesTable) ListByExpiration(txn *store.Txn, from int64, to int64, fn func(lease *corepb.Lease) (bool, error)) error {
	return t.expirationIndex.ListInRange(txn, t.expirationIndexPrefix(from), t.expirationIndexPrefix(to), func(key []byte) (bool, error) {
		// time := utils.BytesToUint64(key[len(t.shardGlobalIndexPrefix) : len(t.shardGlobalIndexPrefix)+8])
		accountId := utils.BytesToUint64(key[len(t.shardGlobalIndexPrefix)+8 : len(t.shardGlobalIndexPrefix)+8+8])
		namespaceId := utils.BytesToUint32(key[len(t.shardGlobalIndexPrefix)+8+8 : len(t.shardGlobalIndexPrefix)+8+8+4])
		leaseId := utils.BytesToUint64(key[len(t.shardGlobalIndexPrefix)+8+8+4 : len(t.shardGlobalIndexPrefix)+8+8+4+8])

		lease, err := t.table.Get(txn,
			utils.ConcatBytes(
				t.tablePK(accountId, namespaceId),
				t.tableSK(leaseId)))
		if err != nil {
			return false, err
		}

		return fn(lease)
	})
}

func (t *LeasesTable) ListByProcessId(txn *store.Txn, namespaceId *corepb.NamespaceId, processId string, paginationToken *corepb.PaginationToken, limit int) (*listLeasesResult, error) {
	result, err := t.processIdIndex.ListPaginated(txn,
		t.processIdIndexPK(namespaceId.AccountId, namespaceId.NamespaceId, processId), pagination.CoreToMonstera(paginationToken), limit)
	if err != nil {
		return nil, err
	}

	leases := make([]*corepb.Lease, 0, len(result.Items))
	for _, leaseId := range result.Items {
		lease, err := t.table.Get(txn,
			utils.ConcatBytes(
				t.tablePK(namespaceId.AccountId, namespaceId.NamespaceId),
				t.tableSK(leaseId)))
		if err != nil {
			return nil, err
		}
		leases = append(leases, lease)
	}

	return &listLeasesResult{
		Leases:                  leases,
		NextPaginationToken:     pagination.MonsteraToCore(result.NextPaginationToken),
		PreviousPaginationToken: pagination.MonsteraToCore(result.PreviousPaginationToken),
	}, nil
}

func (t *LeasesTable) Get(txn *store.Txn, leaseId *corepb.LeaseId) (*corepb.Lease, error) {
	return t.table.Get(txn,
		utils.ConcatBytes(
			t.tablePK(leaseId.AccountId, leaseId.NamespaceId),
			t.tableSK(leaseId.LeaseId)))
}

func (t *LeasesTable) Update(txn *store.Txn, lease *corepb.Lease) error {
	oldLease, err := t.Get(txn, lease.Id)
	if err != nil {
		return err
	}

	// Update expiration index if the lease's expiration time has changed
	if oldLease.ExpiresAt != lease.ExpiresAt {
		err = t.expirationIndex.Delete(txn, t.expirationIndexPK(oldLease.ExpiresAt, lease.Id.AccountId, lease.Id.NamespaceId, lease.Id.LeaseId))
		if err != nil {
			return err
		}
		err = t.expirationIndex.Add(txn, t.expirationIndexPK(lease.ExpiresAt, lease.Id.AccountId, lease.Id.NamespaceId, lease.Id.LeaseId))
		if err != nil {
			return err
		}
	}

	// No need to update process id index, as process id for a lease is immutable

	// Update lease table
	return t.table.Set(txn,
		utils.ConcatBytes(
			t.tablePK(lease.Id.AccountId, lease.Id.NamespaceId),
			t.tableSK(lease.Id.LeaseId)),
		lease)
}

func (t *LeasesTable) Create(txn *store.Txn, lease *corepb.Lease) error {
	// Add to process id index
	processIdIndexPK := t.processIdIndexPK(lease.Id.AccountId, lease.Id.NamespaceId, lease.ProcessId)
	if err := t.processIdIndex.Add(txn, processIdIndexPK, lease.Id.LeaseId); err != nil {
		return err
	}

	// Add to expiration index
	expirationIndexPK := t.expirationIndexPK(lease.ExpiresAt, lease.Id.AccountId, lease.Id.NamespaceId, lease.Id.LeaseId)
	if err := t.expirationIndex.Add(txn, expirationIndexPK); err != nil {
		return err
	}

	// Add to lease table
	return t.table.Set(txn,
		utils.ConcatBytes(
			t.tablePK(lease.Id.AccountId, lease.Id.NamespaceId),
			t.tableSK(lease.Id.LeaseId)),
		lease)
}

func (t *LeasesTable) Delete(txn *store.Txn, lease *corepb.Lease) error {
	// Delete from process id index
	indexPK := t.processIdIndexPK(lease.Id.AccountId, lease.Id.NamespaceId, lease.ProcessId)
	if err := t.processIdIndex.Delete(txn, indexPK, lease.Id.LeaseId); err != nil {
		return err
	}

	// Delete from expiration index
	expirationIndexPK := t.expirationIndexPK(lease.ExpiresAt, lease.Id.AccountId, lease.Id.NamespaceId, lease.Id.LeaseId)
	if err := t.expirationIndex.Delete(txn, expirationIndexPK); err != nil {
		return err
	}

	// Delete from lease table
	return t.table.Delete(txn,
		utils.ConcatBytes(
			t.tablePK(lease.Id.AccountId, lease.Id.NamespaceId),
			t.tableSK(lease.Id.LeaseId)))
}

func (t *LeasesTable) tablePK(accountId uint64, namespaceId uint32) []byte {
	return utils.ConcatBytes(
		sharding.ByAccountAndNamespace(accountId, namespaceId),
		accountId,
		namespaceId,
	)
}

func (t *LeasesTable) tableSK(leaseId uint64) []byte {
	return utils.ConcatBytes(
		leaseId,
	)
}

func (t *LeasesTable) processIdIndexPK(accountId uint64, namespaceId uint32, processId string) []byte {
	return utils.ConcatBytes(
		sharding.ByAccountAndNamespace(accountId, namespaceId),
		accountId,
		namespaceId,
		processId,
	)
}

func (t *LeasesTable) expirationIndexPK(time int64, accountId uint64, namespaceId uint32, leaseId uint64) []byte {
	return utils.ConcatBytes(
		t.shardGlobalIndexPrefix,
		time,
		accountId,
		namespaceId,
		leaseId,
	)
}

func (t *LeasesTable) expirationIndexPrefix(time int64) []byte {
	return utils.ConcatBytes(
		t.shardGlobalIndexPrefix,
		time,
	)
}
