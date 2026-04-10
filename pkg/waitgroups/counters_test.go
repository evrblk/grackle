package waitgroups

import (
	"math/rand/v2"
	"testing"

	"github.com/evrblk/monstera/store"
	"github.com/stretchr/testify/require"

	"github.com/evrblk/grackle/pkg/corepb"
)

func TestCountersTable_Get(t *testing.T) {
	t.Run("get existing counter", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newCountersTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		counter := &corepb.WaitGroupsCounter{
			NamespaceId: &corepb.NamespaceId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
			},
			NumberOfWaitGroups: 42,
		}

		txn := badgerStore.Update()
		err = table.Set(txn, accountId, namespaceId, counter)
		require.NoError(t, err)
		err = txn.Commit()
		require.NoError(t, err)

		// Get counter
		txn = badgerStore.View()
		defer txn.Discard()

		retrieved, err := table.Get(txn, accountId, namespaceId)
		require.NoError(t, err)
		require.NotNil(t, retrieved)
		require.Equal(t, accountId, retrieved.NamespaceId.AccountId)
		require.Equal(t, namespaceId, retrieved.NamespaceId.NamespaceId)
		require.Equal(t, int64(42), retrieved.NumberOfWaitGroups)
	})

	t.Run("get nonexistent counter returns default", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newCountersTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		txn := badgerStore.View()
		defer txn.Discard()

		retrieved, err := table.Get(txn, accountId, namespaceId)
		require.NoError(t, err)
		require.NotNil(t, retrieved)
		require.Equal(t, accountId, retrieved.NamespaceId.AccountId)
		require.Equal(t, namespaceId, retrieved.NamespaceId.NamespaceId)
		require.Equal(t, int64(0), retrieved.NumberOfWaitGroups)
	})
}

func TestCountersTable_Set(t *testing.T) {
	t.Run("set counter", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newCountersTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		counter := &corepb.WaitGroupsCounter{
			NamespaceId: &corepb.NamespaceId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
			},
			NumberOfWaitGroups: 10,
		}

		txn := badgerStore.Update()
		err = table.Set(txn, accountId, namespaceId, counter)
		require.NoError(t, err)
		err = txn.Commit()
		require.NoError(t, err)

		// Verify counter was set
		txn = badgerStore.View()
		defer txn.Discard()

		retrieved, err := table.Get(txn, accountId, namespaceId)
		require.NoError(t, err)
		require.NotNil(t, retrieved)
		require.Equal(t, int64(10), retrieved.NumberOfWaitGroups)
	})

	t.Run("set overwrites existing counter", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newCountersTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		counter1 := &corepb.WaitGroupsCounter{
			NamespaceId: &corepb.NamespaceId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
			},
			NumberOfWaitGroups: 10,
		}

		txn := badgerStore.Update()
		err = table.Set(txn, accountId, namespaceId, counter1)
		require.NoError(t, err)
		err = txn.Commit()
		require.NoError(t, err)

		// Overwrite with new value
		counter2 := &corepb.WaitGroupsCounter{
			NamespaceId: &corepb.NamespaceId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
			},
			NumberOfWaitGroups: 25,
		}

		txn = badgerStore.Update()
		err = table.Set(txn, accountId, namespaceId, counter2)
		require.NoError(t, err)
		err = txn.Commit()
		require.NoError(t, err)

		// Verify new value
		txn = badgerStore.View()
		defer txn.Discard()

		retrieved, err := table.Get(txn, accountId, namespaceId)
		require.NoError(t, err)
		require.NotNil(t, retrieved)
		require.Equal(t, int64(25), retrieved.NumberOfWaitGroups)
	})

	t.Run("set counters for different namespaces are isolated", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newCountersTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId1 := rand.Uint32()
		namespaceId2 := rand.Uint32()

		counter1 := &corepb.WaitGroupsCounter{
			NamespaceId: &corepb.NamespaceId{
				AccountId:   accountId,
				NamespaceId: namespaceId1,
			},
			NumberOfWaitGroups: 10,
		}

		counter2 := &corepb.WaitGroupsCounter{
			NamespaceId: &corepb.NamespaceId{
				AccountId:   accountId,
				NamespaceId: namespaceId2,
			},
			NumberOfWaitGroups: 20,
		}

		txn := badgerStore.Update()
		err = table.Set(txn, accountId, namespaceId1, counter1)
		require.NoError(t, err)
		err = table.Set(txn, accountId, namespaceId2, counter2)
		require.NoError(t, err)
		err = txn.Commit()
		require.NoError(t, err)

		// Verify both counters are independent
		txn = badgerStore.View()
		defer txn.Discard()

		retrieved1, err := table.Get(txn, accountId, namespaceId1)
		require.NoError(t, err)
		require.Equal(t, int64(10), retrieved1.NumberOfWaitGroups)

		retrieved2, err := table.Get(txn, accountId, namespaceId2)
		require.NoError(t, err)
		require.Equal(t, int64(20), retrieved2.NumberOfWaitGroups)
	})
}

func TestCountersTable_Delete(t *testing.T) {
	t.Run("delete existing counter", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newCountersTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		counter := &corepb.WaitGroupsCounter{
			NamespaceId: &corepb.NamespaceId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
			},
			NumberOfWaitGroups: 42,
		}

		txn := badgerStore.Update()
		err = table.Set(txn, accountId, namespaceId, counter)
		require.NoError(t, err)
		err = txn.Commit()
		require.NoError(t, err)

		// Delete counter
		txn = badgerStore.Update()
		err = table.Delete(txn, accountId, namespaceId)
		require.NoError(t, err)
		err = txn.Commit()
		require.NoError(t, err)

		// Verify deletion - Get should return default counter
		txn = badgerStore.View()
		defer txn.Discard()

		retrieved, err := table.Get(txn, accountId, namespaceId)
		require.NoError(t, err)
		require.NotNil(t, retrieved)
		require.Equal(t, int64(0), retrieved.NumberOfWaitGroups)
	})

	t.Run("delete nonexistent counter does not error", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newCountersTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		txn := badgerStore.Update()
		err = table.Delete(txn, accountId, namespaceId)
		require.NoError(t, err)
		err = txn.Commit()
		require.NoError(t, err)
	})

	t.Run("delete one counter does not affect others", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newCountersTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId1 := rand.Uint32()
		namespaceId2 := rand.Uint32()

		counter1 := &corepb.WaitGroupsCounter{
			NamespaceId: &corepb.NamespaceId{
				AccountId:   accountId,
				NamespaceId: namespaceId1,
			},
			NumberOfWaitGroups: 10,
		}

		counter2 := &corepb.WaitGroupsCounter{
			NamespaceId: &corepb.NamespaceId{
				AccountId:   accountId,
				NamespaceId: namespaceId2,
			},
			NumberOfWaitGroups: 20,
		}

		txn := badgerStore.Update()
		err = table.Set(txn, accountId, namespaceId1, counter1)
		require.NoError(t, err)
		err = table.Set(txn, accountId, namespaceId2, counter2)
		require.NoError(t, err)
		err = txn.Commit()
		require.NoError(t, err)

		// Delete counter1
		txn = badgerStore.Update()
		err = table.Delete(txn, accountId, namespaceId1)
		require.NoError(t, err)
		err = txn.Commit()
		require.NoError(t, err)

		// Verify counter1 is deleted (returns default)
		txn = badgerStore.View()
		defer txn.Discard()

		retrieved1, err := table.Get(txn, accountId, namespaceId1)
		require.NoError(t, err)
		require.Equal(t, int64(0), retrieved1.NumberOfWaitGroups)

		// Verify counter2 still exists
		retrieved2, err := table.Get(txn, accountId, namespaceId2)
		require.NoError(t, err)
		require.Equal(t, int64(20), retrieved2.NumberOfWaitGroups)
	})
}

func TestCountersTable_IncrementDecrement(t *testing.T) {
	t.Run("increment counter from zero", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newCountersTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		txn := badgerStore.Update()

		// Get default counter
		counter, err := table.Get(txn, accountId, namespaceId)
		require.NoError(t, err)
		require.Equal(t, int64(0), counter.NumberOfWaitGroups)

		// Increment
		counter.NumberOfWaitGroups++
		err = table.Set(txn, accountId, namespaceId, counter)
		require.NoError(t, err)
		err = txn.Commit()
		require.NoError(t, err)

		// Verify
		txn = badgerStore.View()
		defer txn.Discard()

		retrieved, err := table.Get(txn, accountId, namespaceId)
		require.NoError(t, err)
		require.Equal(t, int64(1), retrieved.NumberOfWaitGroups)
	})

	t.Run("increment and decrement counter", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newCountersTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		// Increment 5 times
		for i := 0; i < 5; i++ {
			txn := badgerStore.Update()
			counter, err := table.Get(txn, accountId, namespaceId)
			require.NoError(t, err)
			counter.NumberOfWaitGroups++
			err = table.Set(txn, accountId, namespaceId, counter)
			require.NoError(t, err)
			err = txn.Commit()
			require.NoError(t, err)
		}

		// Verify it's 5
		txn := badgerStore.View()
		retrieved, err := table.Get(txn, accountId, namespaceId)
		require.NoError(t, err)
		require.Equal(t, int64(5), retrieved.NumberOfWaitGroups)
		txn.Discard()

		// Decrement 2 times
		for i := 0; i < 2; i++ {
			txn := badgerStore.Update()
			counter, err := table.Get(txn, accountId, namespaceId)
			require.NoError(t, err)
			counter.NumberOfWaitGroups--
			err = table.Set(txn, accountId, namespaceId, counter)
			require.NoError(t, err)
			err = txn.Commit()
			require.NoError(t, err)
		}

		// Verify it's 3
		txn = badgerStore.View()
		defer txn.Discard()

		retrieved, err = table.Get(txn, accountId, namespaceId)
		require.NoError(t, err)
		require.Equal(t, int64(3), retrieved.NumberOfWaitGroups)
	})
}

func TestCountersTable_GetTableKeyRange(t *testing.T) {
	t.Run("get table key range", func(t *testing.T) {
		table := newCountersTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		keyRange := table.GetTableKeyRange()
		require.NotNil(t, keyRange)
	})
}
