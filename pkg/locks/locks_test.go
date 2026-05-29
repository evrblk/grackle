package locks

import (
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/evrblk/monstera/store"
	"github.com/stretchr/testify/require"

	"github.com/evrblk/grackle/pkg/corepb"
)

func TestLocksTable_Get(t *testing.T) {
	t.Run("gets a lock", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newLocksTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()
		lockName := "test_lock"

		lock := &corepb.Lock{
			Id: &corepb.LockId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
				LockName:    lockName,
			},
			State:    corepb.LockState_EXCLUSIVE_LOCKED,
			LockedAt: 12345,
			LockHolders: []*corepb.LockHolder{
				{LeaseId: 100, LockedAt: 12345},
			},
		}

		// Create lock
		txn := badgerStore.Update()
		err = table.Update(txn, lock)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		// Get lock
		txn = badgerStore.View()
		actual, err := table.Get(txn, lock.Id)
		txn.Discard()

		require.NoError(t, err)
		require.NotNil(t, actual)
		require.Equal(t, lock.Id.AccountId, actual.Id.AccountId)
		require.Equal(t, lock.Id.NamespaceId, actual.Id.NamespaceId)
		require.Equal(t, lock.Id.LockName, actual.Id.LockName)
		require.Equal(t, lock.State, actual.State)
		require.Equal(t, lock.LockedAt, actual.LockedAt)
		require.Len(t, actual.LockHolders, 1)
		require.Equal(t, lock.LockHolders[0].LeaseId, actual.LockHolders[0].LeaseId)
	})

	t.Run("gets a non-existent lock", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newLocksTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		lockId := &corepb.LockId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LockName:    "nonexistent",
		}

		txn := badgerStore.View()
		_, err = table.Get(txn, lockId)
		txn.Discard()

		require.Error(t, err)
		require.Contains(t, err.Error(), "not found")
	})
}

func TestLocksTable_Update(t *testing.T) {
	t.Run("updates a new lock", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newLocksTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()
		leaseId := rand.Uint64()

		lock := &corepb.Lock{
			Id: &corepb.LockId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
				LockName:    "new_lock",
			},
			State:    corepb.LockState_EXCLUSIVE_LOCKED,
			LockedAt: 12345,
			LockHolders: []*corepb.LockHolder{
				{LeaseId: leaseId, LockedAt: 12345},
			},
		}

		// Update (create) lock
		txn := badgerStore.Update()
		err = table.Update(txn, lock)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		// Verify lock exists in main table
		txn = badgerStore.View()
		actual, err := table.Get(txn, lock.Id)
		require.NoError(t, err)
		require.NotNil(t, actual)
		require.Equal(t, lock.State, actual.State)

		// Verify lock is indexed by lease ID
		locksForLease, err := table.ListByLeaseId(txn, &corepb.LeaseId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LeaseId:     leaseId,
		}, nil, 10)
		txn.Discard()

		require.NoError(t, err)
		require.Len(t, locksForLease.locks, 1)
		require.Equal(t, lock.Id.LockName, locksForLease.locks[0].Id.LockName)
	})
	t.Run("updates an existing lock to add a lease", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newLocksTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()
		leaseId1 := rand.Uint64()
		leaseId2 := rand.Uint64()

		// Create lock with one lease
		lock := &corepb.Lock{
			Id: &corepb.LockId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
				LockName:    "shared_lock",
			},
			State:    corepb.LockState_SHARED_LOCKED,
			LockedAt: 12345,
			LockHolders: []*corepb.LockHolder{
				{LeaseId: leaseId1, LockedAt: 12345},
			},
		}

		txn := badgerStore.Update()
		err = table.Update(txn, lock)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		// Update lock to add second lease
		lock.LockHolders = append(lock.LockHolders, &corepb.LockHolder{LeaseId: leaseId2, LockedAt: 12346})

		txn = badgerStore.Update()
		err = table.Update(txn, lock)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		// Verify both leases can find the lock
		txn = badgerStore.View()

		locksForLease1, err := table.ListByLeaseId(txn, &corepb.LeaseId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LeaseId:     leaseId1,
		}, nil, 10)
		require.NoError(t, err)
		require.Len(t, locksForLease1.locks, 1)

		locksForLease2, err := table.ListByLeaseId(txn, &corepb.LeaseId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LeaseId:     leaseId2,
		}, nil, 10)
		require.NoError(t, err)
		require.Len(t, locksForLease2.locks, 1)

		txn.Discard()
	})

	t.Run("updates an existing lock to remove a lease", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newLocksTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()
		leaseId1 := rand.Uint64()
		leaseId2 := rand.Uint64()

		// Create lock with two leases
		lock := &corepb.Lock{
			Id: &corepb.LockId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
				LockName:    "shared_lock",
			},
			State:    corepb.LockState_SHARED_LOCKED,
			LockedAt: 12345,
			LockHolders: []*corepb.LockHolder{
				{LeaseId: leaseId1, LockedAt: 12345},
				{LeaseId: leaseId2, LockedAt: 12345},
			},
		}

		txn := badgerStore.Update()
		err = table.Update(txn, lock)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		// Update lock to remove first lease
		lock.LockHolders = []*corepb.LockHolder{
			{LeaseId: leaseId2, LockedAt: 12345},
		}

		txn = badgerStore.Update()
		err = table.Update(txn, lock)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		// Verify lease1 can no longer find the lock
		txn = badgerStore.View()

		locksForLease1, err := table.ListByLeaseId(txn, &corepb.LeaseId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LeaseId:     leaseId1,
		}, nil, 10)
		require.NoError(t, err)
		require.Len(t, locksForLease1.locks, 0)

		// Verify lease2 can still find the lock
		locksForLease2, err := table.ListByLeaseId(txn, &corepb.LeaseId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LeaseId:     leaseId2,
		}, nil, 10)
		require.NoError(t, err)
		require.Len(t, locksForLease2.locks, 1)

		txn.Discard()
	})

	t.Run("updates an existing lock to replace a lease", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newLocksTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()
		leaseId1 := rand.Uint64()
		leaseId2 := rand.Uint64()

		// Create lock with lease1
		lock := &corepb.Lock{
			Id: &corepb.LockId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
				LockName:    "exclusive_lock",
			},
			State:    corepb.LockState_EXCLUSIVE_LOCKED,
			LockedAt: 12345,
			LockHolders: []*corepb.LockHolder{
				{LeaseId: leaseId1, LockedAt: 12345},
			},
		}

		txn := badgerStore.Update()
		err = table.Update(txn, lock)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		// Update lock to replace with lease2
		lock.LockHolders = []*corepb.LockHolder{
			{LeaseId: leaseId2, LockedAt: 12346},
		}

		txn = badgerStore.Update()
		err = table.Update(txn, lock)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		// Verify lease1 can no longer find the lock
		txn = badgerStore.View()

		locksForLease1, err := table.ListByLeaseId(txn, &corepb.LeaseId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LeaseId:     leaseId1,
		}, nil, 10)
		require.NoError(t, err)
		require.Len(t, locksForLease1.locks, 0)

		// Verify lease2 can find the lock
		locksForLease2, err := table.ListByLeaseId(txn, &corepb.LeaseId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LeaseId:     leaseId2,
		}, nil, 10)
		require.NoError(t, err)
		require.Len(t, locksForLease2.locks, 1)

		txn.Discard()
	})
}
func TestLocksTable_Delete(t *testing.T) {
	t.Run("deletes a lock", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newLocksTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()
		leaseId := rand.Uint64()

		lock := &corepb.Lock{
			Id: &corepb.LockId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
				LockName:    "test_lock",
			},
			State:    corepb.LockState_EXCLUSIVE_LOCKED,
			LockedAt: 12345,
			LockHolders: []*corepb.LockHolder{
				{LeaseId: leaseId, LockedAt: 12345},
			},
		}

		// Create lock
		txn := badgerStore.Update()
		err = table.Update(txn, lock)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		// Delete lock
		txn = badgerStore.Update()
		err = table.Delete(txn, lock.Id)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		// Verify lock is deleted
		txn = badgerStore.View()
		_, err = table.Get(txn, lock.Id)
		require.Error(t, err)
		require.Contains(t, err.Error(), "not found")

		// Verify lease index is cleaned up
		locksForLease, err := table.ListByLeaseId(txn, &corepb.LeaseId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LeaseId:     leaseId,
		}, nil, 10)
		txn.Discard()

		require.NoError(t, err)
		require.Len(t, locksForLease.locks, 0)
	})

	t.Run("deletes a non-existent lock", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newLocksTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		lockId := &corepb.LockId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LockName:    "nonexistent",
		}

		txn := badgerStore.Update()
		err = table.Delete(txn, lockId)
		txn.Discard()

		// Should not error on deleting non-existent lock (idempotent)
		require.NoError(t, err)
	})

	t.Run("deletes a lock with multiple leases", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newLocksTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()
		leaseId1 := rand.Uint64()
		leaseId2 := rand.Uint64()

		lock := &corepb.Lock{
			Id: &corepb.LockId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
				LockName:    "shared_lock",
			},
			State:    corepb.LockState_SHARED_LOCKED,
			LockedAt: 12345,
			LockHolders: []*corepb.LockHolder{
				{LeaseId: leaseId1, LockedAt: 12345},
				{LeaseId: leaseId2, LockedAt: 12345},
			},
		}

		// Create lock
		txn := badgerStore.Update()
		err = table.Update(txn, lock)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		// Delete lock
		txn = badgerStore.Update()
		err = table.Delete(txn, lock.Id)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		// Verify both lease indexes are cleaned up
		txn = badgerStore.View()

		locksForLease1, err := table.ListByLeaseId(txn, &corepb.LeaseId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LeaseId:     leaseId1,
		}, nil, 10)
		require.NoError(t, err)
		require.Len(t, locksForLease1.locks, 0)

		locksForLease2, err := table.ListByLeaseId(txn, &corepb.LeaseId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LeaseId:     leaseId2,
		}, nil, 10)
		require.NoError(t, err)
		require.Len(t, locksForLease2.locks, 0)

		txn.Discard()
	})
}

func TestLocksTable_List(t *testing.T) {
	t.Run("lists locks", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newLocksTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		// Create multiple locks
		for i := 0; i < 5; i++ {
			lock := &corepb.Lock{
				Id: &corepb.LockId{
					AccountId:   accountId,
					NamespaceId: namespaceId,
					LockName:    fmt.Sprintf("lock_%d", i),
				},
				State:    corepb.LockState_EXCLUSIVE_LOCKED,
				LockedAt: int64(12345 + i),
				LockHolders: []*corepb.LockHolder{
					{LeaseId: uint64(100 + i), LockedAt: int64(12345 + i)},
				},
			}

			txn := badgerStore.Update()
			err = table.Update(txn, lock)
			require.NoError(t, err)
			require.NoError(t, txn.Commit())
		}

		// List locks
		txn := badgerStore.View()
		result, err := table.List(txn, &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
		}, nil, 10)
		txn.Discard()

		require.NoError(t, err)
		require.Len(t, result.locks, 5)
		require.Nil(t, result.nextPaginationToken)
	})

	t.Run("lists locks with pagination", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newLocksTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		// Create 10 locks
		for i := 0; i < 10; i++ {
			lock := &corepb.Lock{
				Id: &corepb.LockId{
					AccountId:   accountId,
					NamespaceId: namespaceId,
					LockName:    fmt.Sprintf("lock_%02d", i),
				},
				State:    corepb.LockState_EXCLUSIVE_LOCKED,
				LockedAt: int64(12345 + i),
				LockHolders: []*corepb.LockHolder{
					{LeaseId: uint64(100 + i), LockedAt: int64(12345 + i)},
				},
			}

			txn := badgerStore.Update()
			err = table.Update(txn, lock)
			require.NoError(t, err)
			require.NoError(t, txn.Commit())
		}

		// List first page
		txn := badgerStore.View()
		page1, err := table.List(txn, &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
		}, nil, 3)
		require.NoError(t, err)
		require.Len(t, page1.locks, 3)
		require.NotNil(t, page1.nextPaginationToken)
		require.Nil(t, page1.previousPaginationToken)

		// List second page
		page2, err := table.List(txn, &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
		}, page1.nextPaginationToken, 3)
		require.NoError(t, err)
		require.Len(t, page2.locks, 3)
		require.NotNil(t, page2.nextPaginationToken)
		require.NotNil(t, page2.previousPaginationToken)

		// Verify no overlap
		for _, lock1 := range page1.locks {
			for _, lock2 := range page2.locks {
				require.NotEqual(t, lock1.Id.LockName, lock2.Id.LockName)
			}
		}

		txn.Discard()
	})
}

func TestLocksTable_ListByLeaseId(t *testing.T) {
	t.Run("empty list", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newLocksTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()
		leaseId := rand.Uint64()

		txn := badgerStore.View()
		result, err := table.ListByLeaseId(txn, &corepb.LeaseId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LeaseId:     leaseId,
		}, nil, 10)
		txn.Discard()

		require.NoError(t, err)
		require.Len(t, result.locks, 0)
		require.Nil(t, result.nextPaginationToken)
	})

	t.Run("lists locks by lease id", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newLocksTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()
		leaseId := rand.Uint64()

		// Create 5 locks with the same lease
		for i := 0; i < 5; i++ {
			lock := &corepb.Lock{
				Id: &corepb.LockId{
					AccountId:   accountId,
					NamespaceId: namespaceId,
					LockName:    fmt.Sprintf("lock_%d", i),
				},
				State:    corepb.LockState_EXCLUSIVE_LOCKED,
				LockedAt: int64(12345 + i),
				LockHolders: []*corepb.LockHolder{
					{LeaseId: leaseId, LockedAt: int64(12345 + i)},
				},
			}

			txn := badgerStore.Update()
			err = table.Update(txn, lock)
			require.NoError(t, err)
			require.NoError(t, txn.Commit())
		}

		// List locks by lease
		txn := badgerStore.View()
		result, err := table.ListByLeaseId(txn, &corepb.LeaseId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LeaseId:     leaseId,
		}, nil, 10)
		txn.Discard()

		require.NoError(t, err)
		require.Len(t, result.locks, 5)
		require.Nil(t, result.nextPaginationToken)
	})

	t.Run("lists locks by lease id with pagination", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newLocksTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()
		leaseId := rand.Uint64()

		// Create 10 locks with the same lease
		for i := 0; i < 10; i++ {
			lock := &corepb.Lock{
				Id: &corepb.LockId{
					AccountId:   accountId,
					NamespaceId: namespaceId,
					LockName:    fmt.Sprintf("lock_%02d", i),
				},
				State:    corepb.LockState_EXCLUSIVE_LOCKED,
				LockedAt: int64(12345 + i),
				LockHolders: []*corepb.LockHolder{
					{LeaseId: leaseId, LockedAt: int64(12345 + i)},
				},
			}

			txn := badgerStore.Update()
			err = table.Update(txn, lock)
			require.NoError(t, err)
			require.NoError(t, txn.Commit())
		}

		// List first page
		txn := badgerStore.View()
		page1, err := table.ListByLeaseId(txn, &corepb.LeaseId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LeaseId:     leaseId,
		}, nil, 3)
		require.NoError(t, err)
		require.Len(t, page1.locks, 3)
		require.NotNil(t, page1.nextPaginationToken)
		require.Nil(t, page1.previousPaginationToken)

		// List second page
		page2, err := table.ListByLeaseId(txn, &corepb.LeaseId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LeaseId:     leaseId,
		}, page1.nextPaginationToken, 3)
		require.NoError(t, err)
		require.Len(t, page2.locks, 3)
		require.NotNil(t, page2.nextPaginationToken)
		require.NotNil(t, page2.previousPaginationToken)

		// List third page
		page3, err := table.ListByLeaseId(txn, &corepb.LeaseId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LeaseId:     leaseId,
		}, page2.nextPaginationToken, 3)
		require.NoError(t, err)
		require.Len(t, page3.locks, 3)
		require.NotNil(t, page3.nextPaginationToken)
		require.NotNil(t, page3.previousPaginationToken)

		// List fourth page (should only have 1 item)
		page4, err := table.ListByLeaseId(txn, &corepb.LeaseId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LeaseId:     leaseId,
		}, page3.nextPaginationToken, 3)
		require.NoError(t, err)
		require.Len(t, page4.locks, 1)
		require.Nil(t, page4.nextPaginationToken)
		require.NotNil(t, page4.previousPaginationToken)

		// Verify no overlap and all locks are present
		allLocks := make(map[string]bool)
		for _, lock := range page1.locks {
			allLocks[lock.Id.LockName] = true
		}
		for _, lock := range page2.locks {
			require.False(t, allLocks[lock.Id.LockName], "duplicate lock in page 2")
			allLocks[lock.Id.LockName] = true
		}
		for _, lock := range page3.locks {
			require.False(t, allLocks[lock.Id.LockName], "duplicate lock in page 3")
			allLocks[lock.Id.LockName] = true
		}
		for _, lock := range page4.locks {
			require.False(t, allLocks[lock.Id.LockName], "duplicate lock in page 4")
			allLocks[lock.Id.LockName] = true
		}
		require.Len(t, allLocks, 10, "should have all 10 locks")

		txn.Discard()
	})

	t.Run("lists locks by lease id with large pagination", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newLocksTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()
		leaseId := rand.Uint64()

		// Create 50 locks to test multi-page iteration
		numLocks := 50
		for i := 0; i < numLocks; i++ {
			lock := &corepb.Lock{
				Id: &corepb.LockId{
					AccountId:   accountId,
					NamespaceId: namespaceId,
					LockName:    fmt.Sprintf("lock_%04d", i),
				},
				State:    corepb.LockState_EXCLUSIVE_LOCKED,
				LockedAt: int64(12345 + i),
				LockHolders: []*corepb.LockHolder{
					{LeaseId: leaseId, LockedAt: int64(12345 + i)},
				},
			}

			txn := badgerStore.Update()
			err = table.Update(txn, lock)
			require.NoError(t, err)
			require.NoError(t, txn.Commit())
		}

		// Iterate through all pages with page size 20
		txn := badgerStore.View()
		var allLocks []*corepb.Lock
		var paginationToken *corepb.PaginationToken

		for {
			result, err := table.ListByLeaseId(txn, &corepb.LeaseId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
				LeaseId:     leaseId,
			}, paginationToken, 20)
			require.NoError(t, err)

			allLocks = append(allLocks, result.locks...)

			if result.nextPaginationToken == nil {
				break
			}
			paginationToken = result.nextPaginationToken
		}

		txn.Discard()

		// Verify we got all locks
		require.Len(t, allLocks, numLocks)

		// Verify no duplicates
		seen := make(map[string]bool)
		for _, lock := range allLocks {
			require.False(t, seen[lock.Id.LockName], "duplicate lock found")
			seen[lock.Id.LockName] = true
		}
	})

	t.Run("lists locks by lease id with multiple leases", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newLocksTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()
		leaseId1 := rand.Uint64()
		leaseId2 := rand.Uint64()

		// Create 3 locks with lease1
		for i := 0; i < 3; i++ {
			lock := &corepb.Lock{
				Id: &corepb.LockId{
					AccountId:   accountId,
					NamespaceId: namespaceId,
					LockName:    fmt.Sprintf("lock_lease1_%d", i),
				},
				State:    corepb.LockState_EXCLUSIVE_LOCKED,
				LockedAt: int64(12345 + i),
				LockHolders: []*corepb.LockHolder{
					{LeaseId: leaseId1, LockedAt: int64(12345 + i)},
				},
			}

			txn := badgerStore.Update()
			err = table.Update(txn, lock)
			require.NoError(t, err)
			require.NoError(t, txn.Commit())
		}

		// Create 2 locks with lease2
		for i := 0; i < 2; i++ {
			lock := &corepb.Lock{
				Id: &corepb.LockId{
					AccountId:   accountId,
					NamespaceId: namespaceId,
					LockName:    fmt.Sprintf("lock_lease2_%d", i),
				},
				State:    corepb.LockState_EXCLUSIVE_LOCKED,
				LockedAt: int64(12345 + i),
				LockHolders: []*corepb.LockHolder{
					{LeaseId: leaseId2, LockedAt: int64(12345 + i)},
				},
			}

			txn := badgerStore.Update()
			err = table.Update(txn, lock)
			require.NoError(t, err)
			require.NoError(t, txn.Commit())
		}

		// List locks for lease1
		txn := badgerStore.View()
		result1, err := table.ListByLeaseId(txn, &corepb.LeaseId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LeaseId:     leaseId1,
		}, nil, 10)
		require.NoError(t, err)
		require.Len(t, result1.locks, 3)

		// List locks for lease2
		result2, err := table.ListByLeaseId(txn, &corepb.LeaseId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LeaseId:     leaseId2,
		}, nil, 10)
		require.NoError(t, err)
		require.Len(t, result2.locks, 2)

		txn.Discard()
	})
}
