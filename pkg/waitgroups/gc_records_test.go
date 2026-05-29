package waitgroups

import (
	"math/rand/v2"
	"testing"

	"github.com/evrblk/monstera/store"
	"github.com/stretchr/testify/require"

	"github.com/evrblk/grackle/pkg/corepb"
)

func TestGCRecordsTable_Create(t *testing.T) {
	t.Run("create gc record for wait group", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		shardPrefix := []byte{0x1d, 0x36, 0x00, 0x00}
		table := newGCRecordsTable(shardPrefix)

		gcRecord := &corepb.WaitGroupsGarbageCollectionRecord{
			Id: rand.Uint64(),
			Record: &corepb.WaitGroupsGarbageCollectionRecord_WaitGroupId{
				WaitGroupId: &corepb.WaitGroupId{
					AccountId:   rand.Uint64(),
					NamespaceId: rand.Uint32(),
					WaitGroupId: rand.Uint64(),
				},
			},
		}

		txn := badgerStore.Update()
		err = table.Create(txn, gcRecord)
		require.NoError(t, err)
		err = txn.Commit()
		require.NoError(t, err)

		// Verify record was created by listing
		txn = badgerStore.View()
		defer txn.Discard()

		records, err := table.List(txn, 100)
		require.NoError(t, err)
		require.Len(t, records, 1)
		require.Equal(t, gcRecord.Id, records[0].Id)
		require.NotNil(t, records[0].GetWaitGroupId())
	})

	t.Run("create gc record for namespace", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		shardPrefix := []byte{0x1d, 0x36, 0x00, 0x00}
		table := newGCRecordsTable(shardPrefix)

		gcRecord := &corepb.WaitGroupsGarbageCollectionRecord{
			Id: rand.Uint64(),
			Record: &corepb.WaitGroupsGarbageCollectionRecord_NamespaceId{
				NamespaceId: &corepb.NamespaceId{
					AccountId:   rand.Uint64(),
					NamespaceId: rand.Uint32(),
				},
			},
		}

		txn := badgerStore.Update()
		err = table.Create(txn, gcRecord)
		require.NoError(t, err)
		err = txn.Commit()
		require.NoError(t, err)

		// Verify record was created by listing
		txn = badgerStore.View()
		defer txn.Discard()

		records, err := table.List(txn, 100)
		require.NoError(t, err)
		require.Len(t, records, 1)
		require.Equal(t, gcRecord.Id, records[0].Id)
		require.NotNil(t, records[0].GetNamespaceId())
	})

	t.Run("create multiple gc records", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		shardPrefix := []byte{0x1d, 0x36, 0x00, 0x00}
		table := newGCRecordsTable(shardPrefix)

		numRecords := 5
		recordIds := make([]uint64, numRecords)

		for i := range numRecords {
			recordIds[i] = rand.Uint64()
			gcRecord := &corepb.WaitGroupsGarbageCollectionRecord{
				Id: recordIds[i],
				Record: &corepb.WaitGroupsGarbageCollectionRecord_WaitGroupId{
					WaitGroupId: &corepb.WaitGroupId{
						AccountId:   rand.Uint64(),
						NamespaceId: rand.Uint32(),
						WaitGroupId: rand.Uint64(),
					},
				},
			}

			txn := badgerStore.Update()
			err := table.Create(txn, gcRecord)
			require.NoError(t, err)
			err = txn.Commit()
			require.NoError(t, err)
		}

		// Verify all records were created
		txn := badgerStore.View()
		defer txn.Discard()

		records, err := table.List(txn, 100)
		require.NoError(t, err)
		require.Len(t, records, numRecords)
	})
}

func TestGCRecordsTable_Delete(t *testing.T) {
	t.Run("delete existing gc record", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		shardPrefix := []byte{0x1d, 0x36, 0x00, 0x00}
		table := newGCRecordsTable(shardPrefix)

		gcRecord := &corepb.WaitGroupsGarbageCollectionRecord{
			Id: rand.Uint64(),
			Record: &corepb.WaitGroupsGarbageCollectionRecord_WaitGroupId{
				WaitGroupId: &corepb.WaitGroupId{
					AccountId:   rand.Uint64(),
					NamespaceId: rand.Uint32(),
					WaitGroupId: rand.Uint64(),
				},
			},
		}

		// Create record
		txn := badgerStore.Update()
		err = table.Create(txn, gcRecord)
		require.NoError(t, err)
		err = txn.Commit()
		require.NoError(t, err)

		// Delete record
		txn = badgerStore.Update()
		err = table.Delete(txn, gcRecord)
		require.NoError(t, err)
		err = txn.Commit()
		require.NoError(t, err)

		// Verify deletion
		txn = badgerStore.View()
		defer txn.Discard()

		records, err := table.List(txn, 100)
		require.NoError(t, err)
		require.Len(t, records, 0)
	})

	t.Run("delete nonexistent gc record does not error", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		shardPrefix := []byte{0x1d, 0x36, 0x00, 0x00}
		table := newGCRecordsTable(shardPrefix)

		gcRecord := &corepb.WaitGroupsGarbageCollectionRecord{
			Id: rand.Uint64(),
			Record: &corepb.WaitGroupsGarbageCollectionRecord_WaitGroupId{
				WaitGroupId: &corepb.WaitGroupId{
					AccountId:   rand.Uint64(),
					NamespaceId: rand.Uint32(),
					WaitGroupId: rand.Uint64(),
				},
			},
		}

		txn := badgerStore.Update()
		err = table.Delete(txn, gcRecord)
		require.NoError(t, err)
		err = txn.Commit()
		require.NoError(t, err)
	})

	t.Run("delete one record does not affect others", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		shardPrefix := []byte{0x1d, 0x36, 0x00, 0x00}
		table := newGCRecordsTable(shardPrefix)

		gcRecord1 := &corepb.WaitGroupsGarbageCollectionRecord{
			Id: rand.Uint64(),
			Record: &corepb.WaitGroupsGarbageCollectionRecord_WaitGroupId{
				WaitGroupId: &corepb.WaitGroupId{
					AccountId:   rand.Uint64(),
					NamespaceId: rand.Uint32(),
					WaitGroupId: rand.Uint64(),
				},
			},
		}

		gcRecord2 := &corepb.WaitGroupsGarbageCollectionRecord{
			Id: rand.Uint64(),
			Record: &corepb.WaitGroupsGarbageCollectionRecord_NamespaceId{
				NamespaceId: &corepb.NamespaceId{
					AccountId:   rand.Uint64(),
					NamespaceId: rand.Uint32(),
				},
			},
		}

		// Create two records
		txn := badgerStore.Update()
		err = table.Create(txn, gcRecord1)
		require.NoError(t, err)
		err = table.Create(txn, gcRecord2)
		require.NoError(t, err)
		err = txn.Commit()
		require.NoError(t, err)

		// Delete first record
		txn = badgerStore.Update()
		err = table.Delete(txn, gcRecord1)
		require.NoError(t, err)
		err = txn.Commit()
		require.NoError(t, err)

		// Verify first record is deleted
		txn = badgerStore.View()
		defer txn.Discard()

		records, err := table.List(txn, 100)
		require.NoError(t, err)
		require.Len(t, records, 1)
		require.Equal(t, gcRecord2.Id, records[0].Id)
	})
}

func TestGCRecordsTable_List(t *testing.T) {
	t.Run("list gc records", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		shardPrefix := []byte{0x1d, 0x36, 0x00, 0x00}
		table := newGCRecordsTable(shardPrefix)

		numRecords := 5
		for range numRecords {
			gcRecord := &corepb.WaitGroupsGarbageCollectionRecord{
				Id: rand.Uint64(),
				Record: &corepb.WaitGroupsGarbageCollectionRecord_WaitGroupId{
					WaitGroupId: &corepb.WaitGroupId{
						AccountId:   rand.Uint64(),
						NamespaceId: rand.Uint32(),
						WaitGroupId: rand.Uint64(),
					},
				},
			}

			txn := badgerStore.Update()
			err := table.Create(txn, gcRecord)
			require.NoError(t, err)
			err = txn.Commit()
			require.NoError(t, err)
		}

		// List all records
		txn := badgerStore.View()
		defer txn.Discard()

		records, err := table.List(txn, 100)
		require.NoError(t, err)
		require.Len(t, records, numRecords)
	})

	t.Run("list with limit", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		shardPrefix := []byte{0x1d, 0x36, 0x00, 0x00}
		table := newGCRecordsTable(shardPrefix)

		numRecords := 10
		for i := range numRecords {
			gcRecord := &corepb.WaitGroupsGarbageCollectionRecord{
				Id: uint64(i), // Use sequential IDs for predictable ordering
				Record: &corepb.WaitGroupsGarbageCollectionRecord_WaitGroupId{
					WaitGroupId: &corepb.WaitGroupId{
						AccountId:   rand.Uint64(),
						NamespaceId: rand.Uint32(),
						WaitGroupId: rand.Uint64(),
					},
				},
			}

			txn := badgerStore.Update()
			err := table.Create(txn, gcRecord)
			require.NoError(t, err)
			err = txn.Commit()
			require.NoError(t, err)
		}

		// List with limit
		txn := badgerStore.View()
		defer txn.Discard()

		records, err := table.List(txn, 3)
		require.NoError(t, err)
		require.Len(t, records, 3)
	})

	t.Run("list empty table", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		shardPrefix := []byte{0x1d, 0x36, 0x00, 0x00}
		table := newGCRecordsTable(shardPrefix)

		txn := badgerStore.View()
		defer txn.Discard()

		records, err := table.List(txn, 100)
		require.NoError(t, err)
		require.Len(t, records, 0)
	})

	t.Run("list both wait group and namespace records", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		shardPrefix := []byte{0x1d, 0x36, 0x00, 0x00}
		table := newGCRecordsTable(shardPrefix)

		// Create wait group GC record
		gcRecord1 := &corepb.WaitGroupsGarbageCollectionRecord{
			Id: rand.Uint64(),
			Record: &corepb.WaitGroupsGarbageCollectionRecord_WaitGroupId{
				WaitGroupId: &corepb.WaitGroupId{
					AccountId:   rand.Uint64(),
					NamespaceId: rand.Uint32(),
					WaitGroupId: rand.Uint64(),
				},
			},
		}

		// Create namespace GC record
		gcRecord2 := &corepb.WaitGroupsGarbageCollectionRecord{
			Id: rand.Uint64(),
			Record: &corepb.WaitGroupsGarbageCollectionRecord_NamespaceId{
				NamespaceId: &corepb.NamespaceId{
					AccountId:   rand.Uint64(),
					NamespaceId: rand.Uint32(),
				},
			},
		}

		txn := badgerStore.Update()
		err = table.Create(txn, gcRecord1)
		require.NoError(t, err)
		err = table.Create(txn, gcRecord2)
		require.NoError(t, err)
		err = txn.Commit()
		require.NoError(t, err)

		// List all records
		txn = badgerStore.View()
		defer txn.Discard()

		records, err := table.List(txn, 100)
		require.NoError(t, err)
		require.Len(t, records, 2)

		// Verify we have one of each type
		hasWaitGroupRecord := false
		hasNamespaceRecord := false

		for _, record := range records {
			if record.GetWaitGroupId() != nil {
				hasWaitGroupRecord = true
			}
			if record.GetNamespaceId() != nil {
				hasNamespaceRecord = true
			}
		}

		require.True(t, hasWaitGroupRecord)
		require.True(t, hasNamespaceRecord)
	})
}

func TestGCRecordsTable_GetTableKeyRange(t *testing.T) {
	t.Run("get table key range", func(t *testing.T) {
		shardPrefix := []byte{0x1d, 0x36, 0x00, 0x00}
		table := newGCRecordsTable(shardPrefix)

		keyRange := table.GetTableKeyRange()
		require.NotNil(t, keyRange)
	})
}

func TestGCRecordsTable_RecordTypes(t *testing.T) {
	t.Run("wait group gc record preserves all fields", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		shardPrefix := []byte{0x1d, 0x36, 0x00, 0x00}
		table := newGCRecordsTable(shardPrefix)

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()
		waitGroupId := rand.Uint64()

		gcRecord := &corepb.WaitGroupsGarbageCollectionRecord{
			Id: rand.Uint64(),
			Record: &corepb.WaitGroupsGarbageCollectionRecord_WaitGroupId{
				WaitGroupId: &corepb.WaitGroupId{
					AccountId:   accountId,
					NamespaceId: namespaceId,
					WaitGroupId: waitGroupId,
				},
			},
		}

		txn := badgerStore.Update()
		err = table.Create(txn, gcRecord)
		require.NoError(t, err)
		err = txn.Commit()
		require.NoError(t, err)

		// Retrieve and verify
		txn = badgerStore.View()
		defer txn.Discard()

		records, err := table.List(txn, 100)
		require.NoError(t, err)
		require.Len(t, records, 1)

		retrievedWaitGroupId := records[0].GetWaitGroupId()
		require.NotNil(t, retrievedWaitGroupId)
		require.Equal(t, accountId, retrievedWaitGroupId.AccountId)
		require.Equal(t, namespaceId, retrievedWaitGroupId.NamespaceId)
		require.Equal(t, waitGroupId, retrievedWaitGroupId.WaitGroupId)
	})

	t.Run("namespace gc record preserves all fields", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		shardPrefix := []byte{0x1d, 0x36, 0x00, 0x00}
		table := newGCRecordsTable(shardPrefix)

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		gcRecord := &corepb.WaitGroupsGarbageCollectionRecord{
			Id: rand.Uint64(),
			Record: &corepb.WaitGroupsGarbageCollectionRecord_NamespaceId{
				NamespaceId: &corepb.NamespaceId{
					AccountId:   accountId,
					NamespaceId: namespaceId,
				},
			},
		}

		txn := badgerStore.Update()
		err = table.Create(txn, gcRecord)
		require.NoError(t, err)
		err = txn.Commit()
		require.NoError(t, err)

		// Retrieve and verify
		txn = badgerStore.View()
		defer txn.Discard()

		records, err := table.List(txn, 100)
		require.NoError(t, err)
		require.Len(t, records, 1)

		retrievedNamespaceId := records[0].GetNamespaceId()
		require.NotNil(t, retrievedNamespaceId)
		require.Equal(t, accountId, retrievedNamespaceId.AccountId)
		require.Equal(t, namespaceId, retrievedNamespaceId.NamespaceId)
	})
}
