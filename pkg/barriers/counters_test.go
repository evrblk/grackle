package barriers

import (
	"math/rand/v2"
	"testing"

	"github.com/evrblk/monstera/store"
	"github.com/stretchr/testify/require"

	"github.com/evrblk/grackle/pkg/corepb"
)

func TestCountersTable_Get(t *testing.T) {
	t.Run("nonexistent", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		countersTable := newCountersTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		txn := badgerStore.View()
		defer txn.Discard()

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		// Get nonexistent counter - should return default with 0 barriers
		counter, err := countersTable.Get(txn, accountId, namespaceId)

		require.NoError(t, err)
		require.NotNil(t, counter)
		require.NotNil(t, counter.NamespaceId)
		require.Equal(t, accountId, counter.NamespaceId.AccountId)
		require.Equal(t, namespaceId, counter.NamespaceId.NamespaceId)
		require.EqualValues(t, 0, counter.NumberOfBarriers)
	})

	t.Run("existing", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		countersTable := newCountersTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		// Set counter
		txn := badgerStore.Update()
		counter := &corepb.BarriersCounter{
			NamespaceId: &corepb.NamespaceId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
			},
			NumberOfBarriers: 5,
		}

		err = countersTable.Set(txn, accountId, namespaceId, counter)
		require.NoError(t, err)

		err = txn.Commit()
		require.NoError(t, err)

		// Get counter
		txn2 := badgerStore.View()
		defer txn2.Discard()

		retrievedCounter, err := countersTable.Get(txn2, accountId, namespaceId)

		require.NoError(t, err)
		require.NotNil(t, retrievedCounter)
		require.NotNil(t, retrievedCounter.NamespaceId)
		require.Equal(t, accountId, retrievedCounter.NamespaceId.AccountId)
		require.Equal(t, namespaceId, retrievedCounter.NamespaceId.NamespaceId)
		require.EqualValues(t, 5, retrievedCounter.NumberOfBarriers)
	})
}

func TestCountersTable_Set(t *testing.T) {
	t.Run("create", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		countersTable := newCountersTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		txn := badgerStore.Update()
		counter := &corepb.BarriersCounter{
			NamespaceId: &corepb.NamespaceId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
			},
			NumberOfBarriers: 3,
		}

		err = countersTable.Set(txn, accountId, namespaceId, counter)
		require.NoError(t, err)

		err = txn.Commit()
		require.NoError(t, err)

		// Verify the counter was created
		txn2 := badgerStore.View()
		defer txn2.Discard()

		retrievedCounter, err := countersTable.Get(txn2, accountId, namespaceId)
		require.NoError(t, err)
		require.EqualValues(t, 3, retrievedCounter.NumberOfBarriers)
	})

	t.Run("update", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		countersTable := newCountersTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		// Create initial counter
		txn := badgerStore.Update()
		counter := &corepb.BarriersCounter{
			NamespaceId: &corepb.NamespaceId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
			},
			NumberOfBarriers: 3,
		}

		err = countersTable.Set(txn, accountId, namespaceId, counter)
		require.NoError(t, err)

		err = txn.Commit()
		require.NoError(t, err)

		// Update counter
		txn2 := badgerStore.Update()
		updatedCounter := &corepb.BarriersCounter{
			NamespaceId: &corepb.NamespaceId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
			},
			NumberOfBarriers: 7,
		}

		err = countersTable.Set(txn2, accountId, namespaceId, updatedCounter)
		require.NoError(t, err)

		err = txn2.Commit()
		require.NoError(t, err)

		// Verify the counter was updated
		txn3 := badgerStore.View()
		defer txn3.Discard()

		retrievedCounter, err := countersTable.Get(txn3, accountId, namespaceId)
		require.NoError(t, err)
		require.EqualValues(t, 7, retrievedCounter.NumberOfBarriers)
	})

	t.Run("zero value", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		countersTable := newCountersTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		txn := badgerStore.Update()
		counter := &corepb.BarriersCounter{
			NamespaceId: &corepb.NamespaceId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
			},
			NumberOfBarriers: 0,
		}

		err = countersTable.Set(txn, accountId, namespaceId, counter)
		require.NoError(t, err)

		err = txn.Commit()
		require.NoError(t, err)

		// Verify the counter was set to 0
		txn2 := badgerStore.View()
		defer txn2.Discard()

		retrievedCounter, err := countersTable.Get(txn2, accountId, namespaceId)
		require.NoError(t, err)
		require.EqualValues(t, 0, retrievedCounter.NumberOfBarriers)
	})

	t.Run("multiple namespaces", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		countersTable := newCountersTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId1 := rand.Uint32()
		namespaceId2 := rand.Uint32()

		// Set counter for namespace 1
		txn := badgerStore.Update()
		counter1 := &corepb.BarriersCounter{
			NamespaceId: &corepb.NamespaceId{
				AccountId:   accountId,
				NamespaceId: namespaceId1,
			},
			NumberOfBarriers: 3,
		}

		err = countersTable.Set(txn, accountId, namespaceId1, counter1)
		require.NoError(t, err)

		// Set counter for namespace 2
		counter2 := &corepb.BarriersCounter{
			NamespaceId: &corepb.NamespaceId{
				AccountId:   accountId,
				NamespaceId: namespaceId2,
			},
			NumberOfBarriers: 7,
		}

		err = countersTable.Set(txn, accountId, namespaceId2, counter2)
		require.NoError(t, err)

		err = txn.Commit()
		require.NoError(t, err)

		// Verify both counters exist independently
		txn2 := badgerStore.View()
		defer txn2.Discard()

		retrievedCounter1, err := countersTable.Get(txn2, accountId, namespaceId1)
		require.NoError(t, err)
		require.EqualValues(t, 3, retrievedCounter1.NumberOfBarriers)

		retrievedCounter2, err := countersTable.Get(txn2, accountId, namespaceId2)
		require.NoError(t, err)
		require.EqualValues(t, 7, retrievedCounter2.NumberOfBarriers)
	})
}

func TestCountersTable_Delete(t *testing.T) {
	t.Run("existing", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		countersTable := newCountersTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		// Create counter
		txn := badgerStore.Update()
		counter := &corepb.BarriersCounter{
			NamespaceId: &corepb.NamespaceId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
			},
			NumberOfBarriers: 5,
		}

		err = countersTable.Set(txn, accountId, namespaceId, counter)
		require.NoError(t, err)

		err = txn.Commit()
		require.NoError(t, err)

		// Verify counter exists
		txn2 := badgerStore.View()
		retrievedCounter, err := countersTable.Get(txn2, accountId, namespaceId)
		txn2.Discard()
		require.NoError(t, err)
		require.EqualValues(t, 5, retrievedCounter.NumberOfBarriers)

		// Delete counter
		txn3 := badgerStore.Update()
		err = countersTable.Delete(txn3, accountId, namespaceId)
		require.NoError(t, err)

		err = txn3.Commit()
		require.NoError(t, err)

		// Verify counter no longer exists (returns default)
		txn4 := badgerStore.View()
		defer txn4.Discard()

		retrievedCounter2, err := countersTable.Get(txn4, accountId, namespaceId)
		require.NoError(t, err)
		require.EqualValues(t, 0, retrievedCounter2.NumberOfBarriers)
	})

	t.Run("nonexistent", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		countersTable := newCountersTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		// Delete nonexistent counter - should not error
		txn := badgerStore.Update()
		err = countersTable.Delete(txn, accountId, namespaceId)
		require.NoError(t, err)

		err = txn.Commit()
		require.NoError(t, err)
	})
}
