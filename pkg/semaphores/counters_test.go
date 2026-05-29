package semaphores

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

		counter := &corepb.SemaphoresCounter{
			NamespaceId: &corepb.NamespaceId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
			},
			NumberOfSemaphores: 5,
			NumberOfLeases:     10,
		}

		// Set counter
		txn := badgerStore.Update()
		err = table.Set(txn, accountId, namespaceId, counter)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		// Get counter
		txn = badgerStore.View()
		result, err := table.Get(txn, accountId, namespaceId)
		txn.Discard()

		require.NoError(t, err)
		require.Equal(t, accountId, result.NamespaceId.AccountId)
		require.Equal(t, namespaceId, result.NamespaceId.NamespaceId)
		require.Equal(t, int64(5), result.NumberOfSemaphores)
		require.Equal(t, int64(10), result.NumberOfLeases)
	})

	t.Run("gets a non-existent counter", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newCountersTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		// Get non-existent counter
		txn := badgerStore.View()
		result, err := table.Get(txn, accountId, namespaceId)
		txn.Discard()

		require.NoError(t, err)
		require.Equal(t, accountId, result.NamespaceId.AccountId)
		require.Equal(t, namespaceId, result.NamespaceId.NamespaceId)
		require.Equal(t, int64(0), result.NumberOfSemaphores)
		require.Equal(t, int64(0), result.NumberOfLeases)
	})

	t.Run("gets counters for different namespaces", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newCountersTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId1 := rand.Uint32()
		namespaceId2 := rand.Uint32()

		counter1 := &corepb.SemaphoresCounter{
			NamespaceId: &corepb.NamespaceId{
				AccountId:   accountId,
				NamespaceId: namespaceId1,
			},
			NumberOfSemaphores: 3,
			NumberOfLeases:     7,
		}

		counter2 := &corepb.SemaphoresCounter{
			NamespaceId: &corepb.NamespaceId{
				AccountId:   accountId,
				NamespaceId: namespaceId2,
			},
			NumberOfSemaphores: 8,
			NumberOfLeases:     15,
		}

		// Set both counters
		txn := badgerStore.Update()
		err = table.Set(txn, accountId, namespaceId1, counter1)
		require.NoError(t, err)
		err = table.Set(txn, accountId, namespaceId2, counter2)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		// Get first counter
		txn = badgerStore.View()
		result1, err := table.Get(txn, accountId, namespaceId1)
		require.NoError(t, err)
		require.Equal(t, int64(3), result1.NumberOfSemaphores)
		require.Equal(t, int64(7), result1.NumberOfLeases)

		// Get second counter
		result2, err := table.Get(txn, accountId, namespaceId2)
		txn.Discard()

		require.NoError(t, err)
		require.Equal(t, int64(8), result2.NumberOfSemaphores)
		require.Equal(t, int64(15), result2.NumberOfLeases)
	})

	t.Run("gets counters for different accounts", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newCountersTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId1 := rand.Uint64()
		accountId2 := rand.Uint64()
		namespaceId := rand.Uint32()

		counter1 := &corepb.SemaphoresCounter{
			NamespaceId: &corepb.NamespaceId{
				AccountId:   accountId1,
				NamespaceId: namespaceId,
			},
			NumberOfSemaphores: 2,
			NumberOfLeases:     4,
		}

		counter2 := &corepb.SemaphoresCounter{
			NamespaceId: &corepb.NamespaceId{
				AccountId:   accountId2,
				NamespaceId: namespaceId,
			},
			NumberOfSemaphores: 6,
			NumberOfLeases:     12,
		}

		// Set both counters
		txn := badgerStore.Update()
		err = table.Set(txn, accountId1, namespaceId, counter1)
		require.NoError(t, err)
		err = table.Set(txn, accountId2, namespaceId, counter2)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		// Get first counter
		txn = badgerStore.View()
		result1, err := table.Get(txn, accountId1, namespaceId)
		require.NoError(t, err)
		require.Equal(t, int64(2), result1.NumberOfSemaphores)
		require.Equal(t, int64(4), result1.NumberOfLeases)

		// Get second counter
		result2, err := table.Get(txn, accountId2, namespaceId)
		txn.Discard()

		require.NoError(t, err)
		require.Equal(t, int64(6), result2.NumberOfSemaphores)
		require.Equal(t, int64(12), result2.NumberOfLeases)
	})
}

func TestCountersTable_Set(t *testing.T) {
	t.Run("sets a counter", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newCountersTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		counter := &corepb.SemaphoresCounter{
			NamespaceId: &corepb.NamespaceId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
			},
			NumberOfSemaphores: 42,
			NumberOfLeases:     100,
		}

		// Set counter
		txn := badgerStore.Update()
		err = table.Set(txn, accountId, namespaceId, counter)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		// Verify it was set
		txn = badgerStore.View()
		result, err := table.Get(txn, accountId, namespaceId)
		txn.Discard()

		require.NoError(t, err)
		require.Equal(t, int64(42), result.NumberOfSemaphores)
		require.Equal(t, int64(100), result.NumberOfLeases)
	})

	t.Run("updates an existing counter", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newCountersTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		// Set initial counter
		txn := badgerStore.Update()
		counter1 := &corepb.SemaphoresCounter{
			NamespaceId: &corepb.NamespaceId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
			},
			NumberOfSemaphores: 5,
			NumberOfLeases:     10,
		}
		err = table.Set(txn, accountId, namespaceId, counter1)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		// Update counter
		txn = badgerStore.Update()
		counter2 := &corepb.SemaphoresCounter{
			NamespaceId: &corepb.NamespaceId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
			},
			NumberOfSemaphores: 20,
			NumberOfLeases:     30,
		}
		err = table.Set(txn, accountId, namespaceId, counter2)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		// Verify it was updated
		txn = badgerStore.View()
		result, err := table.Get(txn, accountId, namespaceId)
		txn.Discard()

		require.NoError(t, err)
		require.Equal(t, int64(20), result.NumberOfSemaphores)
		require.Equal(t, int64(30), result.NumberOfLeases)
	})

	t.Run("sets zero values", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newCountersTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		counter := &corepb.SemaphoresCounter{
			NamespaceId: &corepb.NamespaceId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
			},
			NumberOfSemaphores: 0,
			NumberOfLeases:     0,
		}

		// Set counter with zero values
		txn := badgerStore.Update()
		err = table.Set(txn, accountId, namespaceId, counter)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		// Verify it was set
		txn = badgerStore.View()
		result, err := table.Get(txn, accountId, namespaceId)
		txn.Discard()

		require.NoError(t, err)
		require.Equal(t, int64(0), result.NumberOfSemaphores)
		require.Equal(t, int64(0), result.NumberOfLeases)
	})

	t.Run("sets large values", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newCountersTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		counter := &corepb.SemaphoresCounter{
			NamespaceId: &corepb.NamespaceId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
			},
			NumberOfSemaphores: 9223372036854775807, // max int64
			NumberOfLeases:     9223372036854775807, // max int64
		}

		// Set counter with large values
		txn := badgerStore.Update()
		err = table.Set(txn, accountId, namespaceId, counter)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		// Verify it was set
		txn = badgerStore.View()
		result, err := table.Get(txn, accountId, namespaceId)
		txn.Discard()

		require.NoError(t, err)
		require.Equal(t, int64(9223372036854775807), result.NumberOfSemaphores)
		require.Equal(t, int64(9223372036854775807), result.NumberOfLeases)
	})
}

func TestCountersTable_Delete(t *testing.T) {
	t.Run("deletes a counter", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newCountersTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		counter := &corepb.SemaphoresCounter{
			NamespaceId: &corepb.NamespaceId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
			},
			NumberOfSemaphores: 7,
			NumberOfLeases:     14,
		}

		// Set counter
		txn := badgerStore.Update()
		err = table.Set(txn, accountId, namespaceId, counter)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		// Delete counter
		txn = badgerStore.Update()
		err = table.Delete(txn, accountId, namespaceId)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		// Verify it was deleted (should return default values)
		txn = badgerStore.View()
		result, err := table.Get(txn, accountId, namespaceId)
		txn.Discard()

		require.NoError(t, err)
		require.Equal(t, int64(0), result.NumberOfSemaphores)
		require.Equal(t, int64(0), result.NumberOfLeases)
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

		// Verify get still returns default values
		txn = badgerStore.View()
		result, err := table.Get(txn, accountId, namespaceId)
		txn.Discard()

		require.NoError(t, err)
		require.Equal(t, int64(0), result.NumberOfSemaphores)
		require.Equal(t, int64(0), result.NumberOfLeases)
	})

	t.Run("increments and decrements counter", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newCountersTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		// Start with 0,0
		txn := badgerStore.View()
		result, err := table.Get(txn, accountId, namespaceId)
		txn.Discard()
		require.NoError(t, err)
		require.Equal(t, int64(0), result.NumberOfSemaphores)
		require.Equal(t, int64(0), result.NumberOfLeases)

		// Increment to 1,1
		txn = badgerStore.Update()
		counter := &corepb.SemaphoresCounter{
			NamespaceId: &corepb.NamespaceId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
			},
			NumberOfSemaphores: 1,
			NumberOfLeases:     1,
		}
		err = table.Set(txn, accountId, namespaceId, counter)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		// Verify increment
		txn = badgerStore.View()
		result, err = table.Get(txn, accountId, namespaceId)
		txn.Discard()
		require.NoError(t, err)
		require.Equal(t, int64(1), result.NumberOfSemaphores)
		require.Equal(t, int64(1), result.NumberOfLeases)

		// Decrement back to 0,0
		txn = badgerStore.Update()
		counter = &corepb.SemaphoresCounter{
			NamespaceId: &corepb.NamespaceId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
			},
			NumberOfSemaphores: 0,
			NumberOfLeases:     0,
		}
		err = table.Set(txn, accountId, namespaceId, counter)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		// Verify decrement
		txn = badgerStore.View()
		result, err = table.Get(txn, accountId, namespaceId)
		txn.Discard()
		require.NoError(t, err)
		require.Equal(t, int64(0), result.NumberOfSemaphores)
		require.Equal(t, int64(0), result.NumberOfLeases)
	})

	t.Run("deletes and recreates counter", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newCountersTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		// Create counter
		txn := badgerStore.Update()
		counter := &corepb.SemaphoresCounter{
			NamespaceId: &corepb.NamespaceId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
			},
			NumberOfSemaphores: 10,
			NumberOfLeases:     20,
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
		counter = &corepb.SemaphoresCounter{
			NamespaceId: &corepb.NamespaceId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
			},
			NumberOfSemaphores: 30,
			NumberOfLeases:     40,
		}
		err = table.Set(txn, accountId, namespaceId, counter)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		// Verify new values
		txn = badgerStore.View()
		result, err := table.Get(txn, accountId, namespaceId)
		txn.Discard()

		require.NoError(t, err)
		require.Equal(t, int64(30), result.NumberOfSemaphores)
		require.Equal(t, int64(40), result.NumberOfLeases)
	})
}
