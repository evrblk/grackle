package waitgroups

import (
	"math/rand/v2"
	"testing"
	"time"

	"github.com/evrblk/monstera/store"
	"github.com/stretchr/testify/require"

	"github.com/evrblk/grackle/pkg/corepb"
)

func TestDeletionRecordsTable_Add(t *testing.T) {
	t.Run("add deletion record", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		shardPrefix := []byte{0x1d, 0x36, 0x00, 0x00}
		table := newDeletionRecordsTable(shardPrefix)

		now := time.Now()
		deleteAt := now.Add(time.Hour).UnixNano()

		waitGroupId := &corepb.WaitGroupId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
			WaitGroupId: rand.Uint64(),
		}

		txn := badgerStore.Update()
		err = table.Add(txn, deleteAt, waitGroupId)
		require.NoError(t, err)
		err = txn.Commit()
		require.NoError(t, err)

		// Verify record was added by reading it directly from the table
		txn = badgerStore.View()
		defer txn.Discard()

		record, err := table.table.Get(txn, table.tablePK(deleteAt, waitGroupId.AccountId, waitGroupId.NamespaceId, waitGroupId.WaitGroupId))
		require.NoError(t, err)
		require.NotNil(t, record)
		require.Equal(t, deleteAt, record.DeleteAt)
		require.Equal(t, waitGroupId.WaitGroupId, record.WaitGroupId.WaitGroupId)
	})
}

func TestDeletionRecordsTable_Delete(t *testing.T) {
	t.Run("delete existing deletion record", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		shardPrefix := []byte{0x1d, 0x36, 0x00, 0x00}
		table := newDeletionRecordsTable(shardPrefix)

		now := time.Now()
		deleteAt := now.Add(time.Hour).UnixNano()

		waitGroupId := &corepb.WaitGroupId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
			WaitGroupId: rand.Uint64(),
		}

		txn := badgerStore.Update()
		err = table.Add(txn, deleteAt, waitGroupId)
		require.NoError(t, err)
		err = txn.Commit()
		require.NoError(t, err)

		txn = badgerStore.Update()
		err = table.Delete(txn, deleteAt, waitGroupId)
		require.NoError(t, err)
		err = txn.Commit()
		require.NoError(t, err)

		txn = badgerStore.View()
		defer txn.Discard()

		record, err := table.table.Get(txn, table.tablePK(deleteAt, waitGroupId.AccountId, waitGroupId.NamespaceId, waitGroupId.WaitGroupId))
		require.Error(t, err)
		require.Nil(t, record)
		require.ErrorIs(t, err, store.ErrNotFound)
	})

	t.Run("delete nonexistent deletion record does not error", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		shardPrefix := []byte{0x1d, 0x36, 0x00, 0x00}
		table := newDeletionRecordsTable(shardPrefix)

		now := time.Now()
		deleteAt := now.Add(time.Hour).UnixNano()

		waitGroupId := &corepb.WaitGroupId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
			WaitGroupId: rand.Uint64(),
		}

		txn := badgerStore.Update()
		err = table.Delete(txn, deleteAt, waitGroupId)
		require.NoError(t, err)
		err = txn.Commit()
		require.NoError(t, err)
	})
}

func TestDeletionRecordsTable_List(t *testing.T) {
	t.Run("list records in time range ordered by deletion time", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		shardPrefix := []byte{0x1d, 0x36, 0x00, 0x00}
		table := newDeletionRecordsTable(shardPrefix)

		now := time.Now()

		for i := range 5 {
			deleteAt := now.Add(time.Duration(i) * time.Hour).UnixNano()
			waitGroupId := &corepb.WaitGroupId{
				AccountId:   uint64(i),
				NamespaceId: uint32(i),
				WaitGroupId: uint64(i),
			}

			txn := badgerStore.Update()
			err := table.Add(txn, deleteAt, waitGroupId)
			require.NoError(t, err)
			err = txn.Commit()
			require.NoError(t, err)
		}

		txn := badgerStore.View()
		defer txn.Discard()

		fromTime := now.UnixNano()
		toTime := now.Add(6 * time.Hour).UnixNano()

		result := make([]*corepb.WaitGroupsDeletionRecord, 0)
		err = table.ListByDeletion(txn, fromTime, toTime, func(record *corepb.WaitGroupsDeletionRecord) (bool, error) {
			result = append(result, record)
			return len(result) < 100, nil
		})
		require.NoError(t, err)
		require.Len(t, result, 5)

		for i := 0; i < len(result)-1; i++ {
			require.LessOrEqual(t, result[i].DeleteAt, result[i+1].DeleteAt,
				"Records should be ordered by deletion time")
		}
	})

	t.Run("list excludes records past the upper bound", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		shardPrefix := []byte{0x1d, 0x36, 0x00, 0x00}
		table := newDeletionRecordsTable(shardPrefix)

		now := time.Now()

		for i := range 10 {
			deleteAt := now.Add(time.Duration(i) * time.Hour).UnixNano()
			waitGroupId := &corepb.WaitGroupId{
				AccountId:   uint64(i),
				NamespaceId: uint32(i),
				WaitGroupId: uint64(i),
			}

			txn := badgerStore.Update()
			err := table.Add(txn, deleteAt, waitGroupId)
			require.NoError(t, err)
			err = txn.Commit()
			require.NoError(t, err)
		}

		txn := badgerStore.View()
		defer txn.Discard()

		// Only records with delete_at in [now, now+3h] should be returned.
		fromTime := now.UnixNano()
		toTime := now.Add(3 * time.Hour).UnixNano()

		result := make([]*corepb.WaitGroupsDeletionRecord, 0)
		err = table.ListByDeletion(txn, fromTime, toTime, func(record *corepb.WaitGroupsDeletionRecord) (bool, error) {
			result = append(result, record)
			return len(result) < 100, nil
		})
		require.NoError(t, err)
		require.Len(t, result, 4)
	})
}
