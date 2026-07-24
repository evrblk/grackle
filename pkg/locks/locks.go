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
// 1. account id
// 2. namespace id
//
// Table Sort Key:
// 1. lock name
//
// Lease Id Index Primary Key:
// 1. account id
// 2. namespace id
// 3. lease id
//
// Lease Id Index Sort Key:
// 1. lock name
type locksTable struct {
	table        *honey.BinaryTable[*corepb.Lock, corepb.Lock]
	leaseIdIndex *honey.OneToManySortedIndex
}

// newLocksTable scopes both the table and the lease id index under the
// shard-unique prefix (nested under the registry table ids), so no row is
// shared with any other core (CoreTypePersistedExclusive). Keys carry no
// shard key material — the prefix is the isolation, so honey gets nil bounds;
// routing violations are rejected upstream by the generated core adapter.
func newLocksTable(shardPrefix []byte) *locksTable {
	return &locksTable{
		table: honey.NewBinaryTable[*corepb.Lock, corepb.Lock](
			utils.ConcatBytes(tables.Grackle["Grackle.LocksCore.Locks.Table"].Bytes(), shardPrefix),
			nil,
			nil,
		),
		leaseIdIndex: honey.NewOneToManySortedIndex(
			utils.ConcatBytes(tables.Grackle["Grackle.LocksCore.Locks.LeaseIdIndex"].Bytes(), shardPrefix),
			nil,
			nil,
		),
	}
}

// Clear deletes every row this table owns: the primary lock rows and the
// lease id index.
func (t *locksTable) Clear(badgerStore *store.BadgerStore) error {
	for _, prefix := range [][]byte{t.table.TableId(), t.leaseIdIndex.TableId()} {
		if err := badgerStore.DropPrefix(prefix); err != nil {
			return err
		}
	}
	return nil
}

// EachEntity streams every lock as (canonical key, stored value) — the
// primary table only; the lease id index is rebuilt from the locks on restore.
func (t *locksTable) EachEntity(txn *store.Txn, fn func(key []byte, value []byte) (bool, error)) error {
	return t.table.EachEntry(txn, fn)
}

// RestoreEntity decodes one streamed lock and, if owned, inserts it through
// Update — re-deriving its keys and rebuilding the lease id index from the
// lock's own identity fields.
func (t *locksTable) RestoreEntity(txn *store.Txn, key []byte, value []byte, bounds tables.ShardRange) (bool, error) {
	lock := &corepb.Lock{}
	if err := lock.UnmarshalBinary(value); err != nil {
		return false, err
	}
	if !bounds.Owns(sharding.ByAccountAndNamespace(lock.Id.AccountId, lock.Id.NamespaceId)) {
		return false, nil
	}
	return true, t.Update(txn, lock)
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
		accountId,
		namespaceId,
		leaseId,
	)
}
