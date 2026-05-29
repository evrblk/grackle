package locks

import (
	"math/rand/v2"
	"testing"

	"github.com/evrblk/monstera/store"
	"github.com/stretchr/testify/require"

	"github.com/evrblk/grackle/pkg/corepb"
)

func TestCountersTable_Get(t *testing.T) {
	t.Run("gets a counter", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newCountersTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		// Set counter
		txn := badgerStore.Update()
		counter := &corepb.LocksCounter{
			NamespaceId: &corepb.NamespaceId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
			},
			NumberOfLocks:  5,
			NumberOfLeases: 3,
		}
		err = table.Set(txn, accountId, namespaceId, counter)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		// Get counter
		txn = badgerStore.View()
		actual, err := table.Get(txn, accountId, namespaceId)
		txn.Discard()

		require.NoError(t, err)
		require.NotNil(t, actual)
		require.Equal(t, accountId, actual.NamespaceId.AccountId)
		require.Equal(t, namespaceId, actual.NamespaceId.NamespaceId)
		require.EqualValues(t, 5, actual.NumberOfLocks)
		require.EqualValues(t, 3, actual.NumberOfLeases)
	})

	t.Run("gets a non-existent counter", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newCountersTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		// Get non-existent counter - should return default
		txn := badgerStore.View()
		actual, err := table.Get(txn, accountId, namespaceId)
		txn.Discard()

		require.NoError(t, err)
		require.NotNil(t, actual)
		require.Equal(t, accountId, actual.NamespaceId.AccountId)
		require.Equal(t, namespaceId, actual.NamespaceId.NamespaceId)
		require.EqualValues(t, 0, actual.NumberOfLocks)
		require.EqualValues(t, 0, actual.NumberOfLeases)
	})
}

func TestCountersTable_Set(t *testing.T) {
	t.Run("sets a counter", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newCountersTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		// Set counter
		txn := badgerStore.Update()
		counter := &corepb.LocksCounter{
			NamespaceId: &corepb.NamespaceId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
			},
			NumberOfLocks:  10,
			NumberOfLeases: 7,
		}
		err = table.Set(txn, accountId, namespaceId, counter)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		// Verify
		txn = badgerStore.View()
		actual, err := table.Get(txn, accountId, namespaceId)
		txn.Discard()

		require.NoError(t, err)
		require.EqualValues(t, 10, actual.NumberOfLocks)
		require.EqualValues(t, 7, actual.NumberOfLeases)
	})

	t.Run("updates a counter", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newCountersTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		// Set initial counter
		txn := badgerStore.Update()
		counter := &corepb.LocksCounter{
			NamespaceId: &corepb.NamespaceId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
			},
			NumberOfLocks:  5,
			NumberOfLeases: 3,
		}
		err = table.Set(txn, accountId, namespaceId, counter)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		// Update counter
		txn = badgerStore.Update()
		counter.NumberOfLocks = 8
		counter.NumberOfLeases = 4
		err = table.Set(txn, accountId, namespaceId, counter)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		// Verify update
		txn = badgerStore.View()
		actual, err := table.Get(txn, accountId, namespaceId)
		txn.Discard()

		require.NoError(t, err)
		require.EqualValues(t, 8, actual.NumberOfLocks)
		require.EqualValues(t, 4, actual.NumberOfLeases)
	})
}

func TestCountersTable_Delete(t *testing.T) {
	t.Run("deletes a counter", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newCountersTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		// Set counter
		txn := badgerStore.Update()
		counter := &corepb.LocksCounter{
			NamespaceId: &corepb.NamespaceId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
			},
			NumberOfLocks:  5,
			NumberOfLeases: 3,
		}
		err = table.Set(txn, accountId, namespaceId, counter)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		// Delete counter
		txn = badgerStore.Update()
		err = table.Delete(txn, accountId, namespaceId)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		// Verify deletion - should return default
		txn = badgerStore.View()
		actual, err := table.Get(txn, accountId, namespaceId)
		txn.Discard()

		require.NoError(t, err)
		require.NotNil(t, actual)
		require.EqualValues(t, 0, actual.NumberOfLocks)
		require.EqualValues(t, 0, actual.NumberOfLeases)
	})

	t.Run("deletes a non-existent counter", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newCountersTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		// Delete non-existent counter - should succeed (idempotent)
		txn := badgerStore.Update()
		err = table.Delete(txn, accountId, namespaceId)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		// Verify still returns default
		txn = badgerStore.View()
		actual, err := table.Get(txn, accountId, namespaceId)
		txn.Discard()

		require.NoError(t, err)
		require.NotNil(t, actual)
		require.EqualValues(t, 0, actual.NumberOfLocks)
		require.EqualValues(t, 0, actual.NumberOfLeases)
	})
}
func TestCountersTable_MultipleNamespaces(t *testing.T) {
	badgerStore, err := store.NewBadgerInMemoryStore()
	require.NoError(t, err)

	table := newCountersTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

	accountId := rand.Uint64()
	namespaceId1 := rand.Uint32()
	namespaceId2 := rand.Uint32()

	// Set counter for namespace 1
	txn := badgerStore.Update()
	counter1 := &corepb.LocksCounter{
		NamespaceId: &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: namespaceId1,
		},
		NumberOfLocks:  5,
		NumberOfLeases: 3,
	}
	err = table.Set(txn, accountId, namespaceId1, counter1)
	require.NoError(t, err)

	// Set counter for namespace 2
	counter2 := &corepb.LocksCounter{
		NamespaceId: &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: namespaceId2,
		},
		NumberOfLocks:  8,
		NumberOfLeases: 4,
	}
	err = table.Set(txn, accountId, namespaceId2, counter2)
	require.NoError(t, err)
	require.NoError(t, txn.Commit())

	// Verify namespace 1 counter
	txn = badgerStore.View()
	actual1, err := table.Get(txn, accountId, namespaceId1)
	require.NoError(t, err)
	require.EqualValues(t, 5, actual1.NumberOfLocks)
	require.EqualValues(t, 3, actual1.NumberOfLeases)

	// Verify namespace 2 counter
	actual2, err := table.Get(txn, accountId, namespaceId2)
	require.NoError(t, err)
	require.EqualValues(t, 8, actual2.NumberOfLocks)
	require.EqualValues(t, 4, actual2.NumberOfLeases)
	txn.Discard()
}

func TestCountersTable_MultipleAccounts(t *testing.T) {
	badgerStore, err := store.NewBadgerInMemoryStore()
	require.NoError(t, err)

	table := newCountersTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

	accountId1 := rand.Uint64()
	accountId2 := rand.Uint64()
	namespaceId := rand.Uint32()

	// Set counter for account 1
	txn := badgerStore.Update()
	counter1 := &corepb.LocksCounter{
		NamespaceId: &corepb.NamespaceId{
			AccountId:   accountId1,
			NamespaceId: namespaceId,
		},
		NumberOfLocks:  10,
		NumberOfLeases: 6,
	}
	err = table.Set(txn, accountId1, namespaceId, counter1)
	require.NoError(t, err)

	// Set counter for account 2
	counter2 := &corepb.LocksCounter{
		NamespaceId: &corepb.NamespaceId{
			AccountId:   accountId2,
			NamespaceId: namespaceId,
		},
		NumberOfLocks:  15,
		NumberOfLeases: 9,
	}
	err = table.Set(txn, accountId2, namespaceId, counter2)
	require.NoError(t, err)
	require.NoError(t, txn.Commit())

	// Verify account 1 counter
	txn = badgerStore.View()
	actual1, err := table.Get(txn, accountId1, namespaceId)
	require.NoError(t, err)
	require.EqualValues(t, 10, actual1.NumberOfLocks)
	require.EqualValues(t, 6, actual1.NumberOfLeases)

	// Verify account 2 counter
	actual2, err := table.Get(txn, accountId2, namespaceId)
	require.NoError(t, err)
	require.EqualValues(t, 15, actual2.NumberOfLocks)
	require.EqualValues(t, 9, actual2.NumberOfLeases)
	txn.Discard()
}

func TestCountersTable_ZeroValues(t *testing.T) {
	badgerStore, err := store.NewBadgerInMemoryStore()
	require.NoError(t, err)

	table := newCountersTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

	accountId := rand.Uint64()
	namespaceId := rand.Uint32()

	// Set counter with zero values
	txn := badgerStore.Update()
	counter := &corepb.LocksCounter{
		NamespaceId: &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
		},
		NumberOfLocks:  0,
		NumberOfLeases: 0,
	}
	err = table.Set(txn, accountId, namespaceId, counter)
	require.NoError(t, err)
	require.NoError(t, txn.Commit())

	// Verify zero values are stored correctly
	txn = badgerStore.View()
	actual, err := table.Get(txn, accountId, namespaceId)
	txn.Discard()

	require.NoError(t, err)
	require.EqualValues(t, 0, actual.NumberOfLocks)
	require.EqualValues(t, 0, actual.NumberOfLeases)
}

func TestCountersTable_LargeValues(t *testing.T) {
	badgerStore, err := store.NewBadgerInMemoryStore()
	require.NoError(t, err)

	table := newCountersTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

	accountId := rand.Uint64()
	namespaceId := rand.Uint32()

	// Set counter with large values
	txn := badgerStore.Update()
	counter := &corepb.LocksCounter{
		NamespaceId: &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
		},
		NumberOfLocks:  1000000,
		NumberOfLeases: 500000,
	}
	err = table.Set(txn, accountId, namespaceId, counter)
	require.NoError(t, err)
	require.NoError(t, txn.Commit())

	// Verify large values
	txn = badgerStore.View()
	actual, err := table.Get(txn, accountId, namespaceId)
	txn.Discard()

	require.NoError(t, err)
	require.EqualValues(t, 1000000, actual.NumberOfLocks)
	require.EqualValues(t, 500000, actual.NumberOfLeases)
}

func TestCountersTable_IncrementDecrement(t *testing.T) {
	badgerStore, err := store.NewBadgerInMemoryStore()
	require.NoError(t, err)

	table := newCountersTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

	accountId := rand.Uint64()
	namespaceId := rand.Uint32()

	// Start with initial counter
	txn := badgerStore.Update()
	counter := &corepb.LocksCounter{
		NamespaceId: &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
		},
		NumberOfLocks:  5,
		NumberOfLeases: 3,
	}
	err = table.Set(txn, accountId, namespaceId, counter)
	require.NoError(t, err)
	require.NoError(t, txn.Commit())

	// Increment
	txn = badgerStore.Update()
	counter, err = table.Get(txn, accountId, namespaceId)
	require.NoError(t, err)
	counter.NumberOfLocks += 1
	counter.NumberOfLeases += 1
	err = table.Set(txn, accountId, namespaceId, counter)
	require.NoError(t, err)
	require.NoError(t, txn.Commit())

	// Verify increment
	txn = badgerStore.View()
	actual, err := table.Get(txn, accountId, namespaceId)
	require.NoError(t, err)
	require.EqualValues(t, 6, actual.NumberOfLocks)
	require.EqualValues(t, 4, actual.NumberOfLeases)
	txn.Discard()

	// Decrement
	txn = badgerStore.Update()
	counter, err = table.Get(txn, accountId, namespaceId)
	require.NoError(t, err)
	counter.NumberOfLocks -= 2
	counter.NumberOfLeases -= 1
	err = table.Set(txn, accountId, namespaceId, counter)
	require.NoError(t, err)
	require.NoError(t, txn.Commit())

	// Verify decrement
	txn = badgerStore.View()
	actual, err = table.Get(txn, accountId, namespaceId)
	require.NoError(t, err)
	require.EqualValues(t, 4, actual.NumberOfLocks)
	require.EqualValues(t, 3, actual.NumberOfLeases)
	txn.Discard()
}

func TestCountersTable_DeleteAndRecreate(t *testing.T) {
	badgerStore, err := store.NewBadgerInMemoryStore()
	require.NoError(t, err)

	table := newCountersTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

	accountId := rand.Uint64()
	namespaceId := rand.Uint32()

	// Set initial counter
	txn := badgerStore.Update()
	counter := &corepb.LocksCounter{
		NamespaceId: &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
		},
		NumberOfLocks:  5,
		NumberOfLeases: 3,
	}
	err = table.Set(txn, accountId, namespaceId, counter)
	require.NoError(t, err)
	require.NoError(t, txn.Commit())

	// Delete counter
	txn = badgerStore.Update()
	err = table.Delete(txn, accountId, namespaceId)
	require.NoError(t, err)
	require.NoError(t, txn.Commit())

	// Recreate with different values
	txn = badgerStore.Update()
	newCounter := &corepb.LocksCounter{
		NamespaceId: &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
		},
		NumberOfLocks:  10,
		NumberOfLeases: 7,
	}
	err = table.Set(txn, accountId, namespaceId, newCounter)
	require.NoError(t, err)
	require.NoError(t, txn.Commit())

	// Verify new values
	txn = badgerStore.View()
	actual, err := table.Get(txn, accountId, namespaceId)
	txn.Discard()

	require.NoError(t, err)
	require.EqualValues(t, 10, actual.NumberOfLocks)
	require.EqualValues(t, 7, actual.NumberOfLeases)
}
