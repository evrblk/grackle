package waitgroups

import (
	"math/rand/v2"
	"testing"
	"time"

	"github.com/evrblk/monstera/store"
	"github.com/stretchr/testify/require"

	"github.com/evrblk/grackle/pkg/corepb"
)

func TestExpirationRecordsTable_Add(t *testing.T) {
	t.Run("add expiration record", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		shardPrefix := []byte{0x1d, 0x36, 0x00, 0x00}
		table := newExpirationRecordsTable(shardPrefix)

		now := time.Now()
		expiresAt := now.Add(time.Hour).UnixNano()

		waitGroupId := &corepb.WaitGroupId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
			WaitGroupId: rand.Uint64(),
		}

		txn := badgerStore.Update()
		err = table.Add(txn, expiresAt, waitGroupId)
		require.NoError(t, err)
		err = txn.Commit()
		require.NoError(t, err)

		// Verify record was added by attempting to read it directly from the table
		txn = badgerStore.View()
		defer txn.Discard()

		record, err := table.table.Get(txn, table.tablePK(expiresAt, waitGroupId.AccountId, waitGroupId.NamespaceId, waitGroupId.WaitGroupId))
		require.NoError(t, err)
		require.NotNil(t, record)
		require.Equal(t, expiresAt, record.ExpiresAt)
		require.Equal(t, waitGroupId.AccountId, record.WaitGroupId.AccountId)
		require.Equal(t, waitGroupId.NamespaceId, record.WaitGroupId.NamespaceId)
		require.Equal(t, waitGroupId.WaitGroupId, record.WaitGroupId.WaitGroupId)
	})

	t.Run("add multiple expiration records", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		shardPrefix := []byte{0x1d, 0x36, 0x00, 0x00}
		table := newExpirationRecordsTable(shardPrefix)

		now := time.Now()
		numRecords := 5

		waitGroupIds := make([]*corepb.WaitGroupId, numRecords)
		expiresAts := make([]int64, numRecords)

		for i := 0; i < numRecords; i++ {
			waitGroupIds[i] = &corepb.WaitGroupId{
				AccountId:   rand.Uint64(),
				NamespaceId: rand.Uint32(),
				WaitGroupId: rand.Uint64(),
			}
			expiresAts[i] = now.Add(time.Duration(i) * time.Hour).UnixNano()

			txn := badgerStore.Update()
			err := table.Add(txn, expiresAts[i], waitGroupIds[i])
			require.NoError(t, err)
			err = txn.Commit()
			require.NoError(t, err)
		}

		// Verify all records exist
		txn := badgerStore.View()
		defer txn.Discard()

		for i := 0; i < numRecords; i++ {
			record, err := table.table.Get(txn, table.tablePK(expiresAts[i], waitGroupIds[i].AccountId, waitGroupIds[i].NamespaceId, waitGroupIds[i].WaitGroupId))
			require.NoError(t, err)
			require.NotNil(t, record)
			require.Equal(t, expiresAts[i], record.ExpiresAt)
		}
	})

	t.Run("add records with same expiration time but different wait groups", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		shardPrefix := []byte{0x1d, 0x36, 0x00, 0x00}
		table := newExpirationRecordsTable(shardPrefix)

		now := time.Now()
		expiresAt := now.Add(time.Hour).UnixNano()

		waitGroupId1 := &corepb.WaitGroupId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
			WaitGroupId: rand.Uint64(),
		}

		waitGroupId2 := &corepb.WaitGroupId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
			WaitGroupId: rand.Uint64(),
		}

		txn := badgerStore.Update()
		err = table.Add(txn, expiresAt, waitGroupId1)
		require.NoError(t, err)
		err = table.Add(txn, expiresAt, waitGroupId2)
		require.NoError(t, err)
		err = txn.Commit()
		require.NoError(t, err)

		// Verify both records exist
		txn = badgerStore.View()
		defer txn.Discard()

		record1, err := table.table.Get(txn, table.tablePK(expiresAt, waitGroupId1.AccountId, waitGroupId1.NamespaceId, waitGroupId1.WaitGroupId))
		require.NoError(t, err)
		require.NotNil(t, record1)
		require.Equal(t, waitGroupId1.WaitGroupId, record1.WaitGroupId.WaitGroupId)

		record2, err := table.table.Get(txn, table.tablePK(expiresAt, waitGroupId2.AccountId, waitGroupId2.NamespaceId, waitGroupId2.WaitGroupId))
		require.NoError(t, err)
		require.NotNil(t, record2)
		require.Equal(t, waitGroupId2.WaitGroupId, record2.WaitGroupId.WaitGroupId)
	})
}

func TestExpirationRecordsTable_Delete(t *testing.T) {
	t.Run("delete existing expiration record", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		shardPrefix := []byte{0x1d, 0x36, 0x00, 0x00}
		table := newExpirationRecordsTable(shardPrefix)

		now := time.Now()
		expiresAt := now.Add(time.Hour).UnixNano()

		waitGroupId := &corepb.WaitGroupId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
			WaitGroupId: rand.Uint64(),
		}

		// Add record
		txn := badgerStore.Update()
		err = table.Add(txn, expiresAt, waitGroupId)
		require.NoError(t, err)
		err = txn.Commit()
		require.NoError(t, err)

		// Delete record
		txn = badgerStore.Update()
		err = table.Delete(txn, expiresAt, waitGroupId)
		require.NoError(t, err)
		err = txn.Commit()
		require.NoError(t, err)

		// Verify deletion
		txn = badgerStore.View()
		defer txn.Discard()

		record, err := table.table.Get(txn, table.tablePK(expiresAt, waitGroupId.AccountId, waitGroupId.NamespaceId, waitGroupId.WaitGroupId))
		require.Error(t, err)
		require.Nil(t, record)
		require.ErrorIs(t, err, store.ErrNotFound)
	})

	t.Run("delete nonexistent expiration record does not error", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		shardPrefix := []byte{0x1d, 0x36, 0x00, 0x00}
		table := newExpirationRecordsTable(shardPrefix)

		now := time.Now()
		expiresAt := now.Add(time.Hour).UnixNano()

		waitGroupId := &corepb.WaitGroupId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
			WaitGroupId: rand.Uint64(),
		}

		txn := badgerStore.Update()
		err = table.Delete(txn, expiresAt, waitGroupId)
		require.NoError(t, err)
		err = txn.Commit()
		require.NoError(t, err)
	})

	t.Run("delete one record does not affect others", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		shardPrefix := []byte{0x1d, 0x36, 0x00, 0x00}
		table := newExpirationRecordsTable(shardPrefix)

		now := time.Now()
		expiresAt1 := now.Add(time.Hour).UnixNano()
		expiresAt2 := now.Add(2 * time.Hour).UnixNano()

		waitGroupId1 := &corepb.WaitGroupId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
			WaitGroupId: rand.Uint64(),
		}

		waitGroupId2 := &corepb.WaitGroupId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
			WaitGroupId: rand.Uint64(),
		}

		// Add two records
		txn := badgerStore.Update()
		err = table.Add(txn, expiresAt1, waitGroupId1)
		require.NoError(t, err)
		err = table.Add(txn, expiresAt2, waitGroupId2)
		require.NoError(t, err)
		err = txn.Commit()
		require.NoError(t, err)

		// Delete first record
		txn = badgerStore.Update()
		err = table.Delete(txn, expiresAt1, waitGroupId1)
		require.NoError(t, err)
		err = txn.Commit()
		require.NoError(t, err)

		// Verify first record is deleted
		txn = badgerStore.View()
		defer txn.Discard()

		record1, err := table.table.Get(txn, table.tablePK(expiresAt1, waitGroupId1.AccountId, waitGroupId1.NamespaceId, waitGroupId1.WaitGroupId))
		require.Error(t, err)
		require.Nil(t, record1)

		// Verify second record still exists
		record2, err := table.table.Get(txn, table.tablePK(expiresAt2, waitGroupId2.AccountId, waitGroupId2.NamespaceId, waitGroupId2.WaitGroupId))
		require.NoError(t, err)
		require.NotNil(t, record2)
		require.Equal(t, expiresAt2, record2.ExpiresAt)
	})

	t.Run("delete with same expiration time but different wait group", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		shardPrefix := []byte{0x1d, 0x36, 0x00, 0x00}
		table := newExpirationRecordsTable(shardPrefix)

		now := time.Now()
		expiresAt := now.Add(time.Hour).UnixNano()

		waitGroupId1 := &corepb.WaitGroupId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
			WaitGroupId: rand.Uint64(),
		}

		waitGroupId2 := &corepb.WaitGroupId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
			WaitGroupId: rand.Uint64(),
		}

		// Add two records with same expiration time
		txn := badgerStore.Update()
		err = table.Add(txn, expiresAt, waitGroupId1)
		require.NoError(t, err)
		err = table.Add(txn, expiresAt, waitGroupId2)
		require.NoError(t, err)
		err = txn.Commit()
		require.NoError(t, err)

		// Delete first record
		txn = badgerStore.Update()
		err = table.Delete(txn, expiresAt, waitGroupId1)
		require.NoError(t, err)
		err = txn.Commit()
		require.NoError(t, err)

		// Verify first record is deleted
		txn = badgerStore.View()
		defer txn.Discard()

		record1, err := table.table.Get(txn, table.tablePK(expiresAt, waitGroupId1.AccountId, waitGroupId1.NamespaceId, waitGroupId1.WaitGroupId))
		require.Error(t, err)
		require.Nil(t, record1)

		// Verify second record still exists
		record2, err := table.table.Get(txn, table.tablePK(expiresAt, waitGroupId2.AccountId, waitGroupId2.NamespaceId, waitGroupId2.WaitGroupId))
		require.NoError(t, err)
		require.NotNil(t, record2)
		require.Equal(t, waitGroupId2.WaitGroupId, record2.WaitGroupId.WaitGroupId)
	})
}

func TestExpirationRecordsTable_GetTableKeyRange(t *testing.T) {
	t.Run("get table key range", func(t *testing.T) {
		shardPrefix := []byte{0x1d, 0x36, 0x00, 0x00}
		table := newExpirationRecordsTable(shardPrefix)

		keyRange := table.GetTableKeyRange()
		require.NotNil(t, keyRange)
	})
}

func TestExpirationRecordsTable_List(t *testing.T) {
	t.Run("list records in time range", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		shardPrefix := []byte{0x1d, 0x36, 0x00, 0x00}
		table := newExpirationRecordsTable(shardPrefix)

		now := time.Now()

		// Add records with different expiration times
		expirationTimes := []time.Duration{
			1 * time.Hour,
			2 * time.Hour,
			3 * time.Hour,
			4 * time.Hour,
			5 * time.Hour,
		}

		for i, duration := range expirationTimes {
			expiresAt := now.Add(duration).UnixNano()
			waitGroupId := &corepb.WaitGroupId{
				AccountId:   uint64(i),
				NamespaceId: uint32(i),
				WaitGroupId: uint64(i),
			}

			txn := badgerStore.Update()
			err := table.Add(txn, expiresAt, waitGroupId)
			require.NoError(t, err)
			err = txn.Commit()
			require.NoError(t, err)
		}

		// List all records
		txn := badgerStore.View()
		defer txn.Discard()

		fromTime := now.UnixNano()
		toTime := now.Add(6 * time.Hour).UnixNano()

		result := make([]*corepb.WaitGroupsExpirationRecord, 0)
		err = table.ListByExpiration(txn, fromTime, toTime, func(record *corepb.WaitGroupsExpirationRecord) (bool, error) {
			result = append(result, record)
			return len(result) < 100, nil
		})
		require.NoError(t, err)
		require.Len(t, result, 5)

		// Verify records are ordered by expiration time
		for i := 0; i < len(result)-1; i++ {
			require.LessOrEqual(t, result[i].ExpiresAt, result[i+1].ExpiresAt,
				"Records should be ordered by expiration time")
		}
	})

	t.Run("list records with partial time range", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		shardPrefix := []byte{0x1d, 0x36, 0x00, 0x00}
		table := newExpirationRecordsTable(shardPrefix)

		now := time.Now()

		// Add records with different expiration times
		for i := 0; i < 10; i++ {
			expiresAt := now.Add(time.Duration(i) * time.Hour).UnixNano()
			waitGroupId := &corepb.WaitGroupId{
				AccountId:   uint64(i),
				NamespaceId: uint32(i),
				WaitGroupId: uint64(i),
			}

			txn := badgerStore.Update()
			err := table.Add(txn, expiresAt, waitGroupId)
			require.NoError(t, err)
			err = txn.Commit()
			require.NoError(t, err)
		}

		// List records from hour 3 to hour 7 (should get 5 records: 3, 4, 5, 6, 7)
		txn := badgerStore.View()
		defer txn.Discard()

		fromTime := now.Add(3 * time.Hour).UnixNano()
		toTime := now.Add(7 * time.Hour).UnixNano()

		result := make([]*corepb.WaitGroupsExpirationRecord, 0)
		err = table.ListByExpiration(txn, fromTime, toTime, func(record *corepb.WaitGroupsExpirationRecord) (bool, error) {
			result = append(result, record)
			return len(result) < 100, nil
		})
		require.NoError(t, err)
		require.Len(t, result, 5)

		// Verify we got the correct records (hours 3, 4, 5, 6, 7)
		for i, record := range result {
			expectedTime := now.Add(time.Duration(3+i) * time.Hour).UnixNano()
			require.Equal(t, expectedTime, record.ExpiresAt)
		}
	})

	t.Run("list empty time range", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		shardPrefix := []byte{0x1d, 0x36, 0x00, 0x00}
		table := newExpirationRecordsTable(shardPrefix)

		now := time.Now()

		// Add records
		for i := 0; i < 5; i++ {
			expiresAt := now.Add(time.Duration(i) * time.Hour).UnixNano()
			waitGroupId := &corepb.WaitGroupId{
				AccountId:   uint64(i),
				NamespaceId: uint32(i),
				WaitGroupId: uint64(i),
			}

			txn := badgerStore.Update()
			err := table.Add(txn, expiresAt, waitGroupId)
			require.NoError(t, err)
			err = txn.Commit()
			require.NoError(t, err)
		}

		// List records in a time range where no records exist
		txn := badgerStore.View()
		defer txn.Discard()

		fromTime := now.Add(10 * time.Hour).UnixNano()
		toTime := now.Add(20 * time.Hour).UnixNano()

		result := make([]*corepb.WaitGroupsExpirationRecord, 0)
		err = table.ListByExpiration(txn, fromTime, toTime, func(record *corepb.WaitGroupsExpirationRecord) (bool, error) {
			result = append(result, record)
			return len(result) < 100, nil
		})
		require.NoError(t, err)
		require.Len(t, result, 0)
	})
}
