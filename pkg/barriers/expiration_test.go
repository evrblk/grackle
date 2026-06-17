package barriers

import (
	"math/rand/v2"
	"testing"

	"github.com/evrblk/monstera/store"
	"github.com/stretchr/testify/require"

	"github.com/evrblk/grackle/pkg/corepb"
)

func TestExpirationRecordsTable_Add(t *testing.T) {
	t.Run("adds an expiration record", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newExpirationRecordsTable([]byte{0x01})

		expiresAt := int64(1000)
		barrierId := &corepb.BarrierId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
			BarrierId:   rand.Uint64(),
		}

		// Add record
		txn := badgerStore.Update()
		err = table.Add(txn, expiresAt, barrierId)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		// Verify it was added by listing
		txn = badgerStore.View()
		var records []*corepb.BarriersExpirationRecord
		err = table.List(txn, 0, 2000, func(record *corepb.BarriersExpirationRecord) (bool, error) {
			records = append(records, record)
			return true, nil
		})
		txn.Discard()

		require.NoError(t, err)
		require.Len(t, records, 1)
		require.Equal(t, expiresAt, records[0].ExpiresAt)
		require.Equal(t, barrierId, records[0].BarrierId)
	})

	t.Run("adds multiple expiration records", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newExpirationRecordsTable([]byte{0x01})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		// Add 5 records with different timestamps and barrier IDs
		txn := badgerStore.Update()
		for i := range 5 {
			barrierId := &corepb.BarrierId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
				BarrierId:   uint64(i + 1),
			}
			err = table.Add(txn, int64((i+1)*1000), barrierId)
			require.NoError(t, err)
		}
		require.NoError(t, txn.Commit())

		// Verify all were added
		txn = badgerStore.View()
		var records []*corepb.BarriersExpirationRecord
		err = table.List(txn, 0, 10000, func(record *corepb.BarriersExpirationRecord) (bool, error) {
			records = append(records, record)
			return true, nil
		})
		txn.Discard()

		require.NoError(t, err)
		require.Len(t, records, 5)
		// Verify they are sorted by timestamp
		for i := range 5 {
			require.Equal(t, int64((i+1)*1000), records[i].ExpiresAt)
		}
	})

	t.Run("overwrites existing expiration record with same key", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newExpirationRecordsTable([]byte{0x01})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()
		expiresAt := int64(1000)
		barrierId := &corepb.BarrierId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			BarrierId:   rand.Uint64(),
		}

		// Add first record
		txn := badgerStore.Update()
		err = table.Add(txn, expiresAt, barrierId)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		// Add again with same key (should overwrite)
		txn = badgerStore.Update()
		err = table.Add(txn, expiresAt, barrierId)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		// Verify only one record exists
		txn = badgerStore.View()
		var records []*corepb.BarriersExpirationRecord
		err = table.List(txn, 0, 2000, func(record *corepb.BarriersExpirationRecord) (bool, error) {
			records = append(records, record)
			return true, nil
		})
		txn.Discard()

		require.NoError(t, err)
		require.Len(t, records, 1)
	})
}

func TestExpirationRecordsTable_Delete(t *testing.T) {
	t.Run("deletes an expiration record", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newExpirationRecordsTable([]byte{0x01})

		barrierId := &corepb.BarrierId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
			BarrierId:   rand.Uint64(),
		}
		expiresAt := int64(1000)

		// Add record
		txn := badgerStore.Update()
		err = table.Add(txn, expiresAt, barrierId)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		// Delete record
		txn = badgerStore.Update()
		err = table.Delete(txn, expiresAt, barrierId)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		// Verify it was deleted
		txn = badgerStore.View()
		var records []*corepb.BarriersExpirationRecord
		err = table.List(txn, 0, 2000, func(record *corepb.BarriersExpirationRecord) (bool, error) {
			records = append(records, record)
			return true, nil
		})
		txn.Discard()

		require.NoError(t, err)
		require.Empty(t, records)
	})

	t.Run("deletes a non-existent expiration record", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newExpirationRecordsTable([]byte{0x01})

		barrierId := &corepb.BarrierId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
			BarrierId:   rand.Uint64(),
		}

		// Delete non-existent record - should succeed (idempotent)
		txn := badgerStore.Update()
		err = table.Delete(txn, 1000, barrierId)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		// Verify list is still empty
		txn = badgerStore.View()
		var records []*corepb.BarriersExpirationRecord
		err = table.List(txn, 0, 2000, func(record *corepb.BarriersExpirationRecord) (bool, error) {
			records = append(records, record)
			return true, nil
		})
		txn.Discard()

		require.NoError(t, err)
		require.Empty(t, records)
	})

	t.Run("deletes one of multiple expiration records", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newExpirationRecordsTable([]byte{0x01})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		// Add 3 records
		txn := badgerStore.Update()
		bIds := make([]*corepb.BarrierId, 3)
		for i := range 3 {
			bIds[i] = &corepb.BarrierId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
				BarrierId:   uint64(i + 1),
			}
			err = table.Add(txn, int64((i+1)*1000), bIds[i])
			require.NoError(t, err)
		}
		require.NoError(t, txn.Commit())

		// Delete the middle record
		txn = badgerStore.Update()
		err = table.Delete(txn, 2000, bIds[1])
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		// Verify only 2 records remain
		txn = badgerStore.View()
		var records []*corepb.BarriersExpirationRecord
		err = table.List(txn, 0, 10000, func(record *corepb.BarriersExpirationRecord) (bool, error) {
			records = append(records, record)
			return true, nil
		})
		txn.Discard()

		require.NoError(t, err)
		require.Len(t, records, 2)
		require.Equal(t, int64(1000), records[0].ExpiresAt)
		require.Equal(t, int64(3000), records[1].ExpiresAt)
	})
}

func TestExpirationRecordsTable_List(t *testing.T) {
	t.Run("lists empty expiration records", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newExpirationRecordsTable([]byte{0x01})

		txn := badgerStore.View()
		var records []*corepb.BarriersExpirationRecord
		err = table.List(txn, 0, 10000, func(record *corepb.BarriersExpirationRecord) (bool, error) {
			records = append(records, record)
			return true, nil
		})
		txn.Discard()

		require.NoError(t, err)
		require.Empty(t, records)
	})

	t.Run("lists single expiration record", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newExpirationRecordsTable([]byte{0x01})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()
		expiresAt := int64(1000)
		barrierId := &corepb.BarrierId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			BarrierId:   rand.Uint64(),
		}

		txn := badgerStore.Update()
		err = table.Add(txn, expiresAt, barrierId)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		txn = badgerStore.View()
		var records []*corepb.BarriersExpirationRecord
		err = table.List(txn, 0, 2000, func(record *corepb.BarriersExpirationRecord) (bool, error) {
			records = append(records, record)
			return true, nil
		})
		txn.Discard()

		require.NoError(t, err)
		require.Len(t, records, 1)
		require.Equal(t, expiresAt, records[0].ExpiresAt)
		require.Equal(t, barrierId, records[0].BarrierId)
	})

	t.Run("lists multiple expiration records in order", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newExpirationRecordsTable([]byte{0x01})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		txn := badgerStore.Update()
		for i := range 5 {
			barrierId := &corepb.BarrierId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
				BarrierId:   uint64(i + 1),
			}
			err = table.Add(txn, int64((i+1)*1000), barrierId)
			require.NoError(t, err)
		}
		require.NoError(t, txn.Commit())

		txn = badgerStore.View()
		var records []*corepb.BarriersExpirationRecord
		err = table.List(txn, 0, 10000, func(record *corepb.BarriersExpirationRecord) (bool, error) {
			records = append(records, record)
			return true, nil
		})
		txn.Discard()

		require.NoError(t, err)
		require.Len(t, records, 5)
		for i := range 5 {
			require.Equal(t, int64((i+1)*1000), records[i].ExpiresAt)
		}
	})

	t.Run("lists expiration records within time range", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newExpirationRecordsTable([]byte{0x01})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		// Add 10 records with timestamps 1000, 2000, ..., 10000
		txn := badgerStore.Update()
		for i := range 10 {
			barrierId := &corepb.BarrierId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
				BarrierId:   uint64(i + 1),
			}
			err = table.Add(txn, int64((i+1)*1000), barrierId)
			require.NoError(t, err)
		}
		require.NoError(t, txn.Commit())

		// List records from 3000 to 7000 (should include 3000, 4000, 5000, 6000, 7000)
		txn = badgerStore.View()
		var records []*corepb.BarriersExpirationRecord
		err = table.List(txn, 3000, 7000, func(record *corepb.BarriersExpirationRecord) (bool, error) {
			records = append(records, record)
			return true, nil
		})
		txn.Discard()

		require.NoError(t, err)
		require.Len(t, records, 5)
		require.Equal(t, int64(3000), records[0].ExpiresAt)
		require.Equal(t, int64(7000), records[4].ExpiresAt)
	})

	t.Run("lists expiration records with early stop", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newExpirationRecordsTable([]byte{0x01})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		txn := badgerStore.Update()
		for i := range 5 {
			barrierId := &corepb.BarrierId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
				BarrierId:   uint64(i + 1),
			}
			err = table.Add(txn, int64((i+1)*1000), barrierId)
			require.NoError(t, err)
		}
		require.NoError(t, txn.Commit())

		// List records but stop after 2
		txn = badgerStore.View()
		var records []*corepb.BarriersExpirationRecord
		err = table.List(txn, 0, 10000, func(record *corepb.BarriersExpirationRecord) (bool, error) {
			records = append(records, record)
			return len(records) < 2, nil // Continue only if we have less than 2 records
		})
		txn.Discard()

		require.NoError(t, err)
		require.Len(t, records, 2)
		require.Equal(t, int64(1000), records[0].ExpiresAt)
		require.Equal(t, int64(2000), records[1].ExpiresAt)
	})

	t.Run("lists expiration records from different barriers", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newExpirationRecordsTable([]byte{0x01})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		// Add records for different barriers with the same timestamp
		txn := badgerStore.Update()
		for i := range 3 {
			barrierId := &corepb.BarrierId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
				BarrierId:   uint64(i + 1),
			}
			err = table.Add(txn, 1000, barrierId)
			require.NoError(t, err)
		}
		require.NoError(t, txn.Commit())

		txn = badgerStore.View()
		var records []*corepb.BarriersExpirationRecord
		err = table.List(txn, 0, 2000, func(record *corepb.BarriersExpirationRecord) (bool, error) {
			records = append(records, record)
			return true, nil
		})
		txn.Discard()

		require.NoError(t, err)
		require.Len(t, records, 3)
		for i := range 3 {
			require.Equal(t, int64(1000), records[i].ExpiresAt)
		}
	})

	t.Run("lists expiration records from different accounts", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newExpirationRecordsTable([]byte{0x01})

		namespaceId := rand.Uint32()
		barrierId := rand.Uint64()

		// Add records for different accounts at the same timestamp
		txn := badgerStore.Update()
		for i := range 3 {
			barrierId := &corepb.BarrierId{
				AccountId:   uint64(i + 1),
				NamespaceId: namespaceId,
				BarrierId:   barrierId,
			}
			err = table.Add(txn, 1000, barrierId)
			require.NoError(t, err)
		}
		require.NoError(t, txn.Commit())

		txn = badgerStore.View()
		var records []*corepb.BarriersExpirationRecord
		err = table.List(txn, 0, 2000, func(record *corepb.BarriersExpirationRecord) (bool, error) {
			records = append(records, record)
			return true, nil
		})
		txn.Discard()

		require.NoError(t, err)
		require.Len(t, records, 3)
		accountsSeen := make(map[uint64]bool)
		for _, record := range records {
			accountsSeen[record.BarrierId.AccountId] = true
		}
		require.Len(t, accountsSeen, 3)
	})

	t.Run("lists no records outside time range", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newExpirationRecordsTable([]byte{0x01})

		barrierId := &corepb.BarrierId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
			BarrierId:   rand.Uint64(),
		}

		// Add a record at timestamp 1000
		txn := badgerStore.Update()
		err = table.Add(txn, 1000, barrierId)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		// List records from 2000 to 3000 (should be empty)
		txn = badgerStore.View()
		var records []*corepb.BarriersExpirationRecord
		err = table.List(txn, 2000, 3000, func(record *corepb.BarriersExpirationRecord) (bool, error) {
			records = append(records, record)
			return true, nil
		})
		txn.Discard()

		require.NoError(t, err)
		require.Empty(t, records)
	})
}

func TestExpirationRecordsTable_GetTableKeyRange(t *testing.T) {
	t.Run("get table key range", func(t *testing.T) {
		table := newExpirationRecordsTable([]byte{0x01})

		keyRange := table.GetTableKeyRange()
		require.NotNil(t, keyRange)
	})
}
