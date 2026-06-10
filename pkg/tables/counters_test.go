package tables

import (
	"encoding/binary"
	"math/rand/v2"
	"testing"

	"github.com/evrblk/monstera/store"
	"github.com/stretchr/testify/require"
)

type testCounters struct {
	X uint64
}

func (c *testCounters) MarshalBinary() ([]byte, error) {
	data := make([]byte, 8)
	binary.BigEndian.PutUint64(data, c.X)
	return data, nil
}

func (c *testCounters) UnmarshalBinary(data []byte) error {
	c.X = binary.BigEndian.Uint64(data)
	return nil
}

func TestCountersTable_Get(t *testing.T) {
	t.Run("gets a counter", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := NewCountersTable[*testCounters, testCounters]([]byte{0x01}, []byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		counter := &testCounters{
			X: 5,
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
		require.EqualValues(t, 5, result.X)
	})

	t.Run("gets a non-existent counter", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := NewCountersTable[*testCounters, testCounters]([]byte{0x01}, []byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		// Get non-existent counter
		txn := badgerStore.View()
		result, err := table.Get(txn, accountId, namespaceId)
		txn.Discard()

		require.NoError(t, err)
		require.EqualValues(t, 0, result.X)
	})

	t.Run("gets counters for different namespaces", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := NewCountersTable[*testCounters, testCounters]([]byte{0x01}, []byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId1 := rand.Uint32()
		namespaceId2 := rand.Uint32()

		counter1 := &testCounters{
			X: 3,
		}

		counter2 := &testCounters{
			X: 8,
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
		require.EqualValues(t, 3, result1.X)

		// Get second counter
		result2, err := table.Get(txn, accountId, namespaceId2)
		txn.Discard()

		require.NoError(t, err)
		require.EqualValues(t, 8, result2.X)
	})

	t.Run("gets counters for different accounts", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := NewCountersTable[*testCounters, testCounters]([]byte{0x01}, []byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId1 := rand.Uint64()
		accountId2 := rand.Uint64()
		namespaceId := rand.Uint32()

		counter1 := &testCounters{
			X: 2,
		}

		counter2 := &testCounters{
			X: 6,
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
		require.EqualValues(t, 2, result1.X)

		// Get second counter
		result2, err := table.Get(txn, accountId2, namespaceId)
		txn.Discard()

		require.NoError(t, err)
		require.EqualValues(t, 6, result2.X)
	})
}

func TestCountersTable_Set(t *testing.T) {
	t.Run("sets a counter", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := NewCountersTable[*testCounters, testCounters]([]byte{0x01}, []byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		counter := &testCounters{
			X: 42,
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
		require.EqualValues(t, 42, result.X)
	})

	t.Run("updates an existing counter", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := NewCountersTable[*testCounters, testCounters]([]byte{0x01}, []byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		// Set initial counter
		txn := badgerStore.Update()
		counter1 := &testCounters{
			X: 5,
		}
		err = table.Set(txn, accountId, namespaceId, counter1)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		// Update counter
		txn = badgerStore.Update()
		counter2 := &testCounters{
			X: 20,
		}
		err = table.Set(txn, accountId, namespaceId, counter2)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		// Verify it was updated
		txn = badgerStore.View()
		result, err := table.Get(txn, accountId, namespaceId)
		txn.Discard()

		require.NoError(t, err)
		require.EqualValues(t, 20, result.X)
	})

	t.Run("sets zero values", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := NewCountersTable[*testCounters, testCounters]([]byte{0x01}, []byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		counter := &testCounters{
			X: 0,
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
		require.EqualValues(t, 0, result.X)
	})

	t.Run("sets large values", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := NewCountersTable[*testCounters, testCounters]([]byte{0x01}, []byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		counter := &testCounters{
			X: 9223372036854775807, // Max int64
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
		require.EqualValues(t, 9223372036854775807, result.X)
	})
}

func TestCountersTable_Delete(t *testing.T) {
	t.Run("deletes a counter", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := NewCountersTable[*testCounters, testCounters]([]byte{0x01}, []byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		counter := &testCounters{
			X: 7,
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
		require.EqualValues(t, 0, result.X)
	})

	t.Run("deletes a non-existent counter", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := NewCountersTable[*testCounters, testCounters]([]byte{0x01}, []byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

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
		require.EqualValues(t, 0, result.X)
	})

	t.Run("increments and decrements counter", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := NewCountersTable[*testCounters, testCounters]([]byte{0x01}, []byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		// Start with 0,0
		txn := badgerStore.View()
		result, err := table.Get(txn, accountId, namespaceId)
		txn.Discard()
		require.NoError(t, err)
		require.EqualValues(t, 0, result.X)

		// Increment to 1
		txn = badgerStore.Update()
		counter := &testCounters{
			X: 1,
		}
		err = table.Set(txn, accountId, namespaceId, counter)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		// Verify increment
		txn = badgerStore.View()
		result, err = table.Get(txn, accountId, namespaceId)
		txn.Discard()
		require.NoError(t, err)
		require.EqualValues(t, 1, result.X)

		// Decrement back to 0
		txn = badgerStore.Update()
		counter = &testCounters{
			X: 0,
		}
		err = table.Set(txn, accountId, namespaceId, counter)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		// Verify decrement
		txn = badgerStore.View()
		result, err = table.Get(txn, accountId, namespaceId)
		txn.Discard()
		require.NoError(t, err)
		require.EqualValues(t, 0, result.X)
	})

	t.Run("deletes and recreates counter", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := NewCountersTable[*testCounters, testCounters]([]byte{0x01}, []byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		// Create counter
		txn := badgerStore.Update()
		counter := &testCounters{
			X: 10,
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
		counter = &testCounters{
			X: 30,
		}
		err = table.Set(txn, accountId, namespaceId, counter)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		// Verify new values
		txn = badgerStore.View()
		result, err := table.Get(txn, accountId, namespaceId)
		txn.Discard()

		require.NoError(t, err)
		require.EqualValues(t, 30, result.X)
	})
}
