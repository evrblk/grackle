package tables

import (
	"github.com/evrblk/monstera/store"
	"github.com/evrblk/monstera/utils"
	"github.com/evrblk/yellowstone-common/honey"

	"github.com/evrblk/grackle/pkg/sharding"

	"github.com/evrblk/grackle/pkg/corepb"
	"github.com/evrblk/grackle/pkg/pagination"
)

// LeasesTable is a table that stores leases for accounts and namespaces. This is a
// common implementation that is shared by locks and semaphores.
//
// Exclusive-store layout (CoreTypePersistedExclusive): the table ids embed
// the shard-unique prefix, keys carry no shard key material, and bounds
// checking is the owning core's job — honey gets nil bounds.
//
// Table Primary Key:
// 1. account id
// 2. namespace id
//
// Table Sort Key:
// 1. lease id
//
// Process Id Index Primary Key:
// 1. account id
// 2. namespace id
// 3. process id
//
// Expiration Index Primary Key:
// 1. shard prefix
// 2. timestamp
// 3. account id
// 4. namespace Id
// 5. lease id
type LeasesTable struct {
	// shardPrefix scopes the expiration index rows, whose keys are ordered by
	// time (not by identity) and so carry the prefix in-key instead of in the
	// table id.
	shardPrefix []byte

	table           *honey.BinaryTable[*corepb.Lease, corepb.Lease]
	processIdIndex  *honey.OneToManyUint64Index
	expirationIndex *honey.SortedIndex
}

func NewLeasesTable(shardPrefix []byte, tableId []byte, processIdIndexId []byte, expirationIndexId []byte) *LeasesTable {
	return &LeasesTable{
		shardPrefix: shardPrefix,

		table: honey.NewBinaryTable[*corepb.Lease, corepb.Lease](
			tableId,
			nil,
			nil,
		),
		processIdIndex: honey.NewOneToManyUint64Index(
			processIdIndexId,
			nil,
			nil,
		),
		expirationIndex: honey.NewSortedIndex(
			expirationIndexId,
			shardPrefix,
			shardPrefix,
		),
	}
}

// Clear deletes every row this table owns: the primary lease rows, both
// index tables, and this shard's slice of the expiration index (whose keys
// carry the shard prefix in-key).
func (t *LeasesTable) Clear(badgerStore *store.BadgerStore) error {
	for _, prefix := range [][]byte{
		t.table.TableId(),
		t.processIdIndex.TableId(),
		utils.ConcatBytes(t.expirationIndex.TableId(), t.shardPrefix),
	} {
		if err := badgerStore.DropPrefix(prefix); err != nil {
			return err
		}
	}
	return nil
}

// EachEntity streams every lease as (canonical key, stored value) — the
// primary table only; the process id and expiration indexes are rebuilt from
// the leases on restore.
func (t *LeasesTable) EachEntity(txn *store.Txn, fn func(key []byte, value []byte) (bool, error)) error {
	return t.table.EachEntry(txn, fn)
}

// RestoreEntity decodes one streamed lease and, if owned, inserts it through
// Create — re-deriving its keys and rebuilding both indexes from the lease's
// own identity fields.
func (t *LeasesTable) RestoreEntity(txn *store.Txn, key []byte, value []byte, bounds ShardRange) (bool, error) {
	lease := &corepb.Lease{}
	if err := lease.UnmarshalBinary(value); err != nil {
		return false, err
	}
	if !bounds.Owns(sharding.ByAccountAndNamespace(lease.Id.AccountId, lease.Id.NamespaceId)) {
		return false, nil
	}
	return true, t.Create(txn, lease)
}

func (t *LeasesTable) GetTableKeyRanges() []honey.KeyRange {
	return []honey.KeyRange{
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
		// time := utils.BytesToUint64(key[len(t.shardPrefix) : len(t.shardPrefix)+8])
		accountId := utils.BytesToUint64(key[len(t.shardPrefix)+8 : len(t.shardPrefix)+8+8])
		namespaceId := utils.BytesToUint64(key[len(t.shardPrefix)+8+8 : len(t.shardPrefix)+8+8+8])
		leaseId := utils.BytesToUint64(key[len(t.shardPrefix)+8+8+8 : len(t.shardPrefix)+8+8+8+8])

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

func (t *LeasesTable) tablePK(accountId uint64, namespaceId uint64) []byte {
	return utils.ConcatBytes(
		accountId,
		namespaceId,
	)
}

func (t *LeasesTable) tableSK(leaseId uint64) []byte {
	return utils.ConcatBytes(
		leaseId,
	)
}

func (t *LeasesTable) processIdIndexPK(accountId uint64, namespaceId uint64, processId string) []byte {
	return utils.ConcatBytes(
		accountId,
		namespaceId,
		processId,
	)
}

func (t *LeasesTable) expirationIndexPK(time int64, accountId uint64, namespaceId uint64, leaseId uint64) []byte {
	return utils.ConcatBytes(
		t.shardPrefix,
		time,
		accountId,
		namespaceId,
		leaseId,
	)
}

func (t *LeasesTable) expirationIndexPrefix(time int64) []byte {
	return utils.ConcatBytes(
		t.shardPrefix,
		time,
	)
}
