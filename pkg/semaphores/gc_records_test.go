package semaphores

import (
	"math/rand/v2"
	"testing"

	"github.com/evrblk/monstera/store"
	"github.com/stretchr/testify/require"

	"github.com/evrblk/grackle/pkg/corepb"
)

func TestGCRecordsTable_Create(t *testing.T) {
	t.Run("creates a gc record with namespace id", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newGCRecordsTable([]byte{0x01})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()
		gcRecordId := rand.Uint64()

		record := &corepb.SemaphoresGarbageCollectionRecord{
			Id: gcRecordId,
			Record: &corepb.SemaphoresGarbageCollectionRecord_NamespaceId{
				NamespaceId: &corepb.NamespaceId{
					AccountId:   accountId,
					NamespaceId: namespaceId,
				},
			},
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
		require.NotNil(t, records[0].GetNamespaceId())
		require.Equal(t, accountId, records[0].GetNamespaceId().AccountId)
		require.Equal(t, namespaceId, records[0].GetNamespaceId().NamespaceId)
	})

	t.Run("creates a gc record with semaphore id", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newGCRecordsTable([]byte{0x01})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()
		semaphoreId := rand.Uint64()
		gcRecordId := rand.Uint64()

		record := &corepb.SemaphoresGarbageCollectionRecord{
			Id: gcRecordId,
			Record: &corepb.SemaphoresGarbageCollectionRecord_SemaphoreId{
				SemaphoreId: &corepb.SemaphoreId{
					AccountId:   accountId,
					NamespaceId: namespaceId,
					SemaphoreId: semaphoreId,
				},
			},
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
		require.NotNil(t, records[0].GetSemaphoreId())
		require.Equal(t, accountId, records[0].GetSemaphoreId().AccountId)
		require.Equal(t, namespaceId, records[0].GetSemaphoreId().NamespaceId)
		require.Equal(t, semaphoreId, records[0].GetSemaphoreId().SemaphoreId)
	})

	t.Run("creates multiple gc records", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newGCRecordsTable([]byte{0x01})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		// Create multiple records (mix of namespace and semaphore)
		txn := badgerStore.Update()
		for i := 0; i < 5; i++ {
			var record *corepb.SemaphoresGarbageCollectionRecord
			if i%2 == 0 {
				// Namespace record
				record = &corepb.SemaphoresGarbageCollectionRecord{
					Id: uint64(i + 1),
					Record: &corepb.SemaphoresGarbageCollectionRecord_NamespaceId{
						NamespaceId: &corepb.NamespaceId{
							AccountId:   accountId,
							NamespaceId: namespaceId,
						},
					},
				}
			} else {
				// Semaphore record
				record = &corepb.SemaphoresGarbageCollectionRecord{
					Id: uint64(i + 1),
					Record: &corepb.SemaphoresGarbageCollectionRecord_SemaphoreId{
						SemaphoreId: &corepb.SemaphoreId{
							AccountId:   accountId,
							NamespaceId: namespaceId,
							SemaphoreId: uint64(i + 1),
						},
					},
				}
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

	t.Run("overwrites existing gc record with same id", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newGCRecordsTable([]byte{0x01})

		gcRecordId := rand.Uint64()
		accountId1 := rand.Uint64()
		accountId2 := rand.Uint64()
		namespaceId := rand.Uint32()

		// Create first record
		txn := badgerStore.Update()
		record1 := &corepb.SemaphoresGarbageCollectionRecord{
			Id: gcRecordId,
			Record: &corepb.SemaphoresGarbageCollectionRecord_NamespaceId{
				NamespaceId: &corepb.NamespaceId{
					AccountId:   accountId1,
					NamespaceId: namespaceId,
				},
			},
		}
		err = table.Create(txn, record1)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		// Create second record with same ID but different account
		txn = badgerStore.Update()
		record2 := &corepb.SemaphoresGarbageCollectionRecord{
			Id: gcRecordId,
			Record: &corepb.SemaphoresGarbageCollectionRecord_NamespaceId{
				NamespaceId: &corepb.NamespaceId{
					AccountId:   accountId2,
					NamespaceId: namespaceId,
				},
			},
		}
		err = table.Create(txn, record2)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		// Verify only one record exists with the new account
		txn = badgerStore.View()
		records, err := table.List(txn, 10)
		txn.Discard()

		require.NoError(t, err)
		require.Len(t, records, 1)
		require.Equal(t, gcRecordId, records[0].Id)
		require.Equal(t, accountId2, records[0].GetNamespaceId().AccountId)
	})
}

func TestGCRecordsTable_Delete(t *testing.T) {
	t.Run("deletes a gc record with namespace id", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newGCRecordsTable([]byte{0x01})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()
		gcRecordId := rand.Uint64()

		record := &corepb.SemaphoresGarbageCollectionRecord{
			Id: gcRecordId,
			Record: &corepb.SemaphoresGarbageCollectionRecord_NamespaceId{
				NamespaceId: &corepb.NamespaceId{
					AccountId:   accountId,
					NamespaceId: namespaceId,
				},
			},
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

	t.Run("deletes a gc record with semaphore id", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newGCRecordsTable([]byte{0x01})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()
		semaphoreId := rand.Uint64()
		gcRecordId := rand.Uint64()

		record := &corepb.SemaphoresGarbageCollectionRecord{
			Id: gcRecordId,
			Record: &corepb.SemaphoresGarbageCollectionRecord_SemaphoreId{
				SemaphoreId: &corepb.SemaphoreId{
					AccountId:   accountId,
					NamespaceId: namespaceId,
					SemaphoreId: semaphoreId,
				},
			},
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

		table := newGCRecordsTable([]byte{0x01})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()
		gcRecordId := rand.Uint64()

		record := &corepb.SemaphoresGarbageCollectionRecord{
			Id: gcRecordId,
			Record: &corepb.SemaphoresGarbageCollectionRecord_NamespaceId{
				NamespaceId: &corepb.NamespaceId{
					AccountId:   accountId,
					NamespaceId: namespaceId,
				},
			},
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

		table := newGCRecordsTable([]byte{0x01})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		// Create 3 records
		txn := badgerStore.Update()
		records := make([]*corepb.SemaphoresGarbageCollectionRecord, 3)
		for i := 0; i < 3; i++ {
			records[i] = &corepb.SemaphoresGarbageCollectionRecord{
				Id: uint64(i + 1),
				Record: &corepb.SemaphoresGarbageCollectionRecord_NamespaceId{
					NamespaceId: &corepb.NamespaceId{
						AccountId:   accountId,
						NamespaceId: namespaceId,
					},
				},
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
		require.Equal(t, uint64(1), remaining[0].Id)
		require.Equal(t, uint64(3), remaining[1].Id)
	})
}

func TestGCRecordsTable_List(t *testing.T) {
	t.Run("lists empty gc records", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newGCRecordsTable([]byte{0x01})

		// List from empty table
		txn := badgerStore.View()
		records, err := table.List(txn, 10)
		txn.Discard()

		require.NoError(t, err)
		require.Len(t, records, 0)
	})

	t.Run("lists single gc record", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newGCRecordsTable([]byte{0x01})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()
		gcRecordId := rand.Uint64()

		// Create one record
		txn := badgerStore.Update()
		record := &corepb.SemaphoresGarbageCollectionRecord{
			Id: gcRecordId,
			Record: &corepb.SemaphoresGarbageCollectionRecord_NamespaceId{
				NamespaceId: &corepb.NamespaceId{
					AccountId:   accountId,
					NamespaceId: namespaceId,
				},
			},
		}
		err = table.Create(txn, record)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		// List records
		txn = badgerStore.View()
		records, err := table.List(txn, 10)
		txn.Discard()

		require.NoError(t, err)
		require.Len(t, records, 1)
		require.Equal(t, gcRecordId, records[0].Id)
		require.Equal(t, accountId, records[0].GetNamespaceId().AccountId)
		require.Equal(t, namespaceId, records[0].GetNamespaceId().NamespaceId)
	})

	t.Run("lists multiple gc records", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newGCRecordsTable([]byte{0x01})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		// Create 5 records
		txn := badgerStore.Update()
		for i := 0; i < 5; i++ {
			record := &corepb.SemaphoresGarbageCollectionRecord{
				Id: uint64(i + 1),
				Record: &corepb.SemaphoresGarbageCollectionRecord_NamespaceId{
					NamespaceId: &corepb.NamespaceId{
						AccountId:   accountId,
						NamespaceId: namespaceId,
					},
				},
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
		for i := 0; i < 5; i++ {
			require.Equal(t, uint64(i+1), records[i].Id)
		}
	})

	t.Run("lists gc records with limit", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newGCRecordsTable([]byte{0x01})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		// Create 10 records
		txn := badgerStore.Update()
		for i := 0; i < 10; i++ {
			record := &corepb.SemaphoresGarbageCollectionRecord{
				Id: uint64(i + 1),
				Record: &corepb.SemaphoresGarbageCollectionRecord_NamespaceId{
					NamespaceId: &corepb.NamespaceId{
						AccountId:   accountId,
						NamespaceId: namespaceId,
					},
				},
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
		for i := 0; i < 5; i++ {
			require.Equal(t, uint64(i+1), records[i].Id)
		}
	})

	t.Run("lists gc records from different namespaces", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newGCRecordsTable([]byte{0x01})

		accountId1 := rand.Uint64()
		accountId2 := rand.Uint64()
		namespaceId1 := rand.Uint32()
		namespaceId2 := rand.Uint32()

		// Create records for different namespaces
		txn := badgerStore.Update()
		record1 := &corepb.SemaphoresGarbageCollectionRecord{
			Id: 1,
			Record: &corepb.SemaphoresGarbageCollectionRecord_NamespaceId{
				NamespaceId: &corepb.NamespaceId{
					AccountId:   accountId1,
					NamespaceId: namespaceId1,
				},
			},
		}
		err = table.Create(txn, record1)
		require.NoError(t, err)

		record2 := &corepb.SemaphoresGarbageCollectionRecord{
			Id: 2,
			Record: &corepb.SemaphoresGarbageCollectionRecord_NamespaceId{
				NamespaceId: &corepb.NamespaceId{
					AccountId:   accountId2,
					NamespaceId: namespaceId2,
				},
			},
		}
		err = table.Create(txn, record2)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		// List all records
		txn = badgerStore.View()
		records, err := table.List(txn, 10)
		txn.Discard()

		require.NoError(t, err)
		require.Len(t, records, 2)
		// Both namespaces should be present
		require.Equal(t, accountId1, records[0].GetNamespaceId().AccountId)
		require.Equal(t, accountId2, records[1].GetNamespaceId().AccountId)
	})

	t.Run("lists mixed namespace and semaphore gc records", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newGCRecordsTable([]byte{0x01})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		// Create namespace record
		txn := badgerStore.Update()
		record1 := &corepb.SemaphoresGarbageCollectionRecord{
			Id: 1,
			Record: &corepb.SemaphoresGarbageCollectionRecord_NamespaceId{
				NamespaceId: &corepb.NamespaceId{
					AccountId:   accountId,
					NamespaceId: namespaceId,
				},
			},
		}
		err = table.Create(txn, record1)
		require.NoError(t, err)

		// Create semaphore record
		record2 := &corepb.SemaphoresGarbageCollectionRecord{
			Id: 2,
			Record: &corepb.SemaphoresGarbageCollectionRecord_SemaphoreId{
				SemaphoreId: &corepb.SemaphoreId{
					AccountId:   accountId,
					NamespaceId: namespaceId,
					SemaphoreId: rand.Uint64(),
				},
			},
		}
		err = table.Create(txn, record2)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		// List all records
		txn = badgerStore.View()
		records, err := table.List(txn, 10)
		txn.Discard()

		require.NoError(t, err)
		require.Len(t, records, 2)
		// Verify one is namespace, one is semaphore
		require.NotNil(t, records[0].GetNamespaceId())
		require.NotNil(t, records[1].GetSemaphoreId())
	})

	t.Run("lists gc records with zero limit", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newGCRecordsTable([]byte{0x01})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		// Create 3 records
		txn := badgerStore.Update()
		for i := 0; i < 3; i++ {
			record := &corepb.SemaphoresGarbageCollectionRecord{
				Id: uint64(i + 1),
				Record: &corepb.SemaphoresGarbageCollectionRecord_NamespaceId{
					NamespaceId: &corepb.NamespaceId{
						AccountId:   accountId,
						NamespaceId: namespaceId,
					},
				},
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
