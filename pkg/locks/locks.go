package locks

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

// locksTable
//
// Table Primary Key:
// 1. shard key (by account id and namespace id)
// 2. account id
// 3. namespace id
//
// Table Sort Key:
// 1. lock name
//
// Lease Id Index Primary Key:
// 1. shard key (by account id and namespace id)
// 2. account id
// 3. namespace id
// 3. lease id
//
// Lease Id Index Sort Key:
// 1. lock name
type locksTable struct {
	table        *honey.BinaryTable[*corepb.Lock, corepb.Lock]
	leaseIdIndex *honey.OneToManySortedIndex
}

func newLocksTable(shardLowerBound []byte, shardUpperBound []byte) *locksTable {
	return &locksTable{
		table: honey.NewBinaryTable[*corepb.Lock, corepb.Lock](
			tables.Grackle["Grackle.LocksCore.Locks.Table"].Bytes(),
			shardLowerBound,
			shardUpperBound,
		),
		leaseIdIndex: honey.NewOneToManySortedIndex(
			tables.Grackle["Grackle.LocksCore.Locks.LeaseIdIndex"].Bytes(),
			shardLowerBound,
			shardUpperBound,
		),
	}
}

func (t *locksTable) GetTableKeyRanges() []honey.KeyRange {
	return []honey.KeyRange{
		t.table.GetTableKeyRange(),
		t.leaseIdIndex.GetTableKeyRange(),
	}
}

type listLocksResult struct {
	locks                   []*corepb.Lock
	nextPaginationToken     *corepb.PaginationToken
	previousPaginationToken *corepb.PaginationToken
}

func (t *locksTable) List(txn *store.Txn, namespaceId *corepb.NamespaceId, paginationToken *corepb.PaginationToken, limit int) (*listLocksResult, error) {
	result, err := t.table.ListPaginated(txn,
		t.tablePK(namespaceId.AccountId, namespaceId.NamespaceId), pagination.CoreToMonstera(paginationToken), limit)
	if err != nil {
		return nil, err
	}

	return &listLocksResult{
		locks:                   result.Items,
		nextPaginationToken:     pagination.MonsteraToCore(result.NextPaginationToken),
		previousPaginationToken: pagination.MonsteraToCore(result.PreviousPaginationToken),
	}, nil

}

func (t *locksTable) ListByLeaseId(txn *store.Txn, leaseId *corepb.LeaseId, paginationToken *corepb.PaginationToken, limit int) (*listLocksResult, error) {
	result, err := t.leaseIdIndex.ListPaginated(txn,
		t.leaseIdIndexPK(leaseId.AccountId, leaseId.NamespaceId, leaseId.LeaseId), pagination.CoreToMonstera(paginationToken), limit)
	if err != nil {
		return nil, err
	}

	locks := make([]*corepb.Lock, len(result.Items))
	for i, lockName := range result.Items {
		lock, err := t.table.Get(txn,
			utils.ConcatBytes(
				t.tablePK(leaseId.AccountId, leaseId.NamespaceId),
				t.tableSK(string(lockName))))
		if err != nil {
			return nil, err
		}
		locks[i] = lock
	}

	return &listLocksResult{
		locks:                   locks,
		nextPaginationToken:     pagination.MonsteraToCore(result.NextPaginationToken),
		previousPaginationToken: pagination.MonsteraToCore(result.PreviousPaginationToken),
	}, nil
}

// ListByNamePrefix returns up to limit locks within the namespace whose name
// starts with namePrefix. Locks are sorted by name, so passing "a/b/" yields
// the descendants of "a/b". The scan is bounded by limit.
func (t *locksTable) ListByNamePrefix(txn *store.Txn, namespaceId *corepb.NamespaceId, namePrefix string, limit int) ([]*corepb.Lock, error) {
	result, err := t.table.ListPaginated(txn,
		utils.ConcatBytes(
			t.tablePK(namespaceId.AccountId, namespaceId.NamespaceId),
			t.tableSK(namePrefix)),
		nil, limit)
	if err != nil {
		return nil, err
	}

	return result.Items, nil
}

func (t *locksTable) Get(txn *store.Txn, lockId *corepb.LockId) (*corepb.Lock, error) {
	return t.table.Get(txn,
		utils.ConcatBytes(
			t.tablePK(lockId.AccountId, lockId.NamespaceId),
			t.tableSK(lockId.LockName)))
}

func (t *locksTable) Update(txn *store.Txn, lock *corepb.Lock) error {
	tableKey := utils.ConcatBytes(
		t.tablePK(lock.Id.AccountId, lock.Id.NamespaceId),
		t.tableSK(lock.Id.LockName))

	oldLock, err := t.table.Get(txn, tableKey)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return err
	}

	// If lock doesn't exist, treat as a creation (oldLeaseIds will be empty)
	oldLeaseIds := make(map[uint64]struct{})
	if err == nil {
		// Lock exists, get old lease IDs
		for _, holder := range oldLock.LockHolders {
			oldLeaseIds[holder.LeaseId] = struct{}{}
		}
	}

	newLeaseIds := make(map[uint64]struct{}, len(lock.LockHolders))
	for _, holder := range lock.LockHolders {
		newLeaseIds[holder.LeaseId] = struct{}{}
	}

	lockName := []byte(lock.Id.LockName)

	// Delete old lease IDs that are no longer present in the new lock
	for leaseId := range oldLeaseIds {
		if _, ok := newLeaseIds[leaseId]; !ok {
			err = t.leaseIdIndex.Delete(txn,
				t.leaseIdIndexPK(lock.Id.AccountId, lock.Id.NamespaceId, leaseId),
				lockName,
			)
			if err != nil {
				return err
			}
		}
	}

	// Add new lease IDs that are not present in the old lock
	for leaseId := range newLeaseIds {
		if _, ok := oldLeaseIds[leaseId]; !ok {
			err = t.leaseIdIndex.Add(txn,
				t.leaseIdIndexPK(lock.Id.AccountId, lock.Id.NamespaceId, leaseId),
				lockName,
			)
			if err != nil {
				return err
			}
		}
	}

	// Update the lock in the table
	return t.table.Set(txn, tableKey, lock)
}

func (t *locksTable) Delete(txn *store.Txn, lockId *corepb.LockId) error {
	// First, get the lock to find its lease IDs for index cleanup
	tableKey := utils.ConcatBytes(
		t.tablePK(lockId.AccountId, lockId.NamespaceId),
		t.tableSK(lockId.LockName))

	lock, err := t.table.Get(txn, tableKey)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			// Lock doesn't exist, nothing to delete
			return nil
		}
		return err
	}

	// Remove all lease ID index entries for this lock
	lockName := []byte(lockId.LockName)
	for _, holder := range lock.LockHolders {
		err = t.leaseIdIndex.Delete(txn,
			t.leaseIdIndexPK(lockId.AccountId, lockId.NamespaceId, holder.LeaseId),
			lockName,
		)
		if err != nil {
			return err
		}
	}

	// Delete the lock from the main table
	return t.table.Delete(txn, tableKey)
}

func (t *locksTable) tablePK(accountId uint64, namespaceId uint64) []byte {
	return utils.ConcatBytes(
		sharding.ByAccountAndNamespace(accountId, namespaceId),
		accountId,
		namespaceId,
	)
}

func (t *locksTable) tableSK(lockName string) []byte {
	return utils.ConcatBytes(
		lockName,
	)
}

func (t *locksTable) leaseIdIndexPK(accountId uint64, namespaceId uint64, leaseId uint64) []byte {
	return utils.ConcatBytes(
		sharding.ByAccountAndNamespace(accountId, namespaceId),
		accountId,
		namespaceId,
		leaseId,
	)
}
