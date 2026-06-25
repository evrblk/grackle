package tables

import (
	"encoding/binary"
	"math/rand/v2"
	"testing"

	"github.com/evrblk/monstera/store"
	"github.com/stretchr/testify/require"
)

type testGCRecord struct {
	Id uint64
}

func (r *testGCRecord) MarshalBinary() ([]byte, error) {
	data := make([]byte, 8)
	binary.BigEndian.PutUint64(data, r.Id)
	return data, nil
}

func (r *testGCRecord) UnmarshalBinary(data []byte) error {
	r.Id = binary.BigEndian.Uint64(data)
	return nil
}

func (r *testGCRecord) GetId() uint64 {
	return r.Id
}

func TestGCRecordsTable_Create(t *testing.T) {
	t.Run("creates a gc record", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := NewGCRecordsTable[*testGCRecord, testGCRecord]([]byte{0x01}, []byte{0x01})

		gcRecordId := rand.Uint64()
		record := &testGCRecord{
			Id: gcRecordId,
		}

		// Create record
		txn := badgerStore.Update()
		err = table.Create(txn, record)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		// Verify it was created by listing
		txn = badgerStore.View()
		records, err := table.List(txn, 10)
		txn.Discard()

		require.NoError(t, err)
		require.Len(t, records, 1)
		require.Equal(t, gcRecordId, records[0].Id)
	})

	t.Run("creates multiple gc records", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := NewGCRecordsTable[*testGCRecord, testGCRecord]([]byte{0x01}, []byte{0x01})

		// Create multiple records
		txn := badgerStore.Update()
		for i := range 5 {
			record := &testGCRecord{
				Id: uint64(i + 1),
			}
			err = table.Create(txn, record)
			require.NoError(t, err)
		}
		require.NoError(t, txn.Commit())

		// Verify all were created
		txn = badgerStore.View()
		records, err := table.List(txn, 10)
		txn.Discard()

		require.NoError(t, err)
		require.Len(t, records, 5)
	})
}

func TestGCRecordsTable_Delete(t *testing.T) {
	t.Run("deletes a gc record", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := NewGCRecordsTable[*testGCRecord, testGCRecord]([]byte{0x01}, []byte{0x01})

		record := &testGCRecord{
			Id: rand.Uint64(),
		}

		// Create record
		txn := badgerStore.Update()
		err = table.Create(txn, record)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		// Delete record
		txn = badgerStore.Update()
		err = table.Delete(txn, record)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		// Verify it was deleted
		txn = badgerStore.View()
		records, err := table.List(txn, 10)
		txn.Discard()

		require.NoError(t, err)
		require.Len(t, records, 0)
	})

	t.Run("deletes a non-existent gc record", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := NewGCRecordsTable[*testGCRecord, testGCRecord]([]byte{0x01}, []byte{0x01})

		record := &testGCRecord{
			Id: rand.Uint64(),
		}

		// Delete non-existent record - should succeed (idempotent)
		txn := badgerStore.Update()
		err = table.Delete(txn, record)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		// Verify list is still empty
		txn = badgerStore.View()
		records, err := table.List(txn, 10)
		txn.Discard()

		require.NoError(t, err)
		require.Len(t, records, 0)
	})

	t.Run("deletes one of multiple gc records", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := NewGCRecordsTable[*testGCRecord, testGCRecord]([]byte{0x01}, []byte{0x01})

		// Create 3 records
		txn := badgerStore.Update()
		records := make([]*testGCRecord, 3)
		for i := range 3 {
			records[i] = &testGCRecord{
				Id: uint64(i + 1),
			}
			err = table.Create(txn, records[i])
			require.NoError(t, err)
		}
		require.NoError(t, txn.Commit())

		// Delete the middle record
		txn = badgerStore.Update()
		err = table.Delete(txn, records[1])
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		// Verify only 2 records remain
		txn = badgerStore.View()
		remaining, err := table.List(txn, 10)
		txn.Discard()

		require.NoError(t, err)
		require.Len(t, remaining, 2)
		require.EqualValues(t, 1, remaining[0].Id)
		require.EqualValues(t, 3, remaining[1].Id)
	})
}

func TestGCRecordsTable_List(t *testing.T) {
	t.Run("lists empty gc records", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := NewGCRecordsTable[*testGCRecord, testGCRecord]([]byte{0x01}, []byte{0x01})

		// List from empty table
		txn := badgerStore.View()
		records, err := table.List(txn, 10)
		txn.Discard()

		require.NoError(t, err)
		require.Len(t, records, 0)
	})

	t.Run("lists multiple gc records", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := NewGCRecordsTable[*testGCRecord, testGCRecord]([]byte{0x01}, []byte{0x01})

		// Create 5 records
		txn := badgerStore.Update()
		for i := range 5 {
			record := &testGCRecord{
				Id: uint64(i + 1),
			}
			err = table.Create(txn, record)
			require.NoError(t, err)
		}
		require.NoError(t, txn.Commit())

		// List records
		txn = badgerStore.View()
		records, err := table.List(txn, 10)
		txn.Discard()

		require.NoError(t, err)
		require.Len(t, records, 5)
		// Verify they are in order by ID
		for i := range 5 {
			require.EqualValues(t, i+1, records[i].Id)
		}
	})

	t.Run("lists gc records with limit", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := NewGCRecordsTable[*testGCRecord, testGCRecord]([]byte{0x01}, []byte{0x01})

		// Create 10 records
		txn := badgerStore.Update()
		for i := range 10 {
			record := &testGCRecord{
				Id: uint64(i + 1),
			}
			err = table.Create(txn, record)
			require.NoError(t, err)
		}
		require.NoError(t, txn.Commit())

		// List with limit of 5
		txn = badgerStore.View()
		records, err := table.List(txn, 5)
		txn.Discard()

		require.NoError(t, err)
		require.Len(t, records, 5)
		// Verify we get the first 5 records
		for i := range 5 {
			require.EqualValues(t, i+1, records[i].Id)
		}
	})

	t.Run("lists gc records with zero limit", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := NewGCRecordsTable[*testGCRecord, testGCRecord]([]byte{0x01}, []byte{0x01})

		// Create 3 records
		txn := badgerStore.Update()
		for i := range 3 {
			record := &testGCRecord{
				Id: uint64(i + 1),
			}
			err = table.Create(txn, record)
			require.NoError(t, err)
		}
		require.NoError(t, txn.Commit())

		// List with limit of 0
		txn = badgerStore.View()
		records, err := table.List(txn, 0)
		txn.Discard()

		require.NoError(t, err)
		require.Len(t, records, 0)
	})
}
