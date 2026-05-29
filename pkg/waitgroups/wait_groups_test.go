package waitgroups

import (
	"math/rand/v2"
	"testing"

	"github.com/evrblk/monstera/store"
	"github.com/stretchr/testify/require"

	"github.com/evrblk/grackle/pkg/corepb"
)

func TestWaitGroupsTable_Create(t *testing.T) {
	t.Run("create wait group in table", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newWaitGroupsTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		waitGroup := &corepb.WaitGroup{
			Id: &corepb.WaitGroupId{
				AccountId:   rand.Uint64(),
				NamespaceId: rand.Uint32(),
				WaitGroupId: rand.Uint64(),
			},
			Name:        "test_wait_group",
			Description: "test description",
			Counter:     10,
			Completed:   0,
			CreatedAt:   rand.Int64(),
			UpdatedAt:   rand.Int64(),
			ExpiresAt:   rand.Int64(),
		}

		txn := badgerStore.Update()
		err = table.Create(txn, waitGroup)
		require.NoError(t, err)

		err = txn.Commit()
		require.NoError(t, err)

		// Verify wait group was created
		txn = badgerStore.View()
		defer txn.Discard()

		actual, err := table.Get(txn, waitGroup.Id)
		require.NoError(t, err)
		require.NotNil(t, actual)
		require.Equal(t, waitGroup.Name, actual.Name)
		require.Equal(t, waitGroup.Description, actual.Description)
		require.Equal(t, waitGroup.Counter, actual.Counter)
		require.Equal(t, waitGroup.Completed, actual.Completed)
	})

	t.Run("create wait group creates name index entry", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newWaitGroupsTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		waitGroup := &corepb.WaitGroup{
			Id: &corepb.WaitGroupId{
				AccountId:   rand.Uint64(),
				NamespaceId: rand.Uint32(),
				WaitGroupId: rand.Uint64(),
			},
			Name:        "test_wait_group",
			Description: "test description",
			Counter:     10,
			Completed:   0,
			CreatedAt:   rand.Int64(),
			UpdatedAt:   rand.Int64(),
			ExpiresAt:   rand.Int64(),
		}

		txn := badgerStore.Update()
		err = table.Create(txn, waitGroup)
		require.NoError(t, err)

		err = txn.Commit()
		require.NoError(t, err)

		// Verify wait group can be actual by name
		txn = badgerStore.View()
		defer txn.Discard()

		actual, err := table.GetByName(txn, waitGroup.Id.AccountId, waitGroup.Id.NamespaceId, waitGroup.Name)
		require.NoError(t, err)
		require.NotNil(t, actual)
		require.Equal(t, waitGroup.Id.WaitGroupId, actual.Id.WaitGroupId)
		require.Equal(t, waitGroup.Name, actual.Name)
	})
}

func TestWaitGroupsTable_Get(t *testing.T) {
	t.Run("get existing wait group", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newWaitGroupsTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		waitGroup := &corepb.WaitGroup{
			Id: &corepb.WaitGroupId{
				AccountId:   rand.Uint64(),
				NamespaceId: rand.Uint32(),
				WaitGroupId: rand.Uint64(),
			},
			Name:        "test_wait_group",
			Description: "test description",
			Counter:     10,
			Completed:   5,
			CreatedAt:   rand.Int64(),
			UpdatedAt:   rand.Int64(),
			ExpiresAt:   rand.Int64(),
		}

		txn := badgerStore.Update()
		err = table.Create(txn, waitGroup)
		require.NoError(t, err)
		err = txn.Commit()
		require.NoError(t, err)

		// Get wait group
		txn = badgerStore.View()
		defer txn.Discard()

		actual, err := table.Get(txn, waitGroup.Id)
		require.NoError(t, err)
		require.NotNil(t, actual)
		require.Equal(t, waitGroup.Id.WaitGroupId, actual.Id.WaitGroupId)
		require.Equal(t, waitGroup.Name, actual.Name)
		require.Equal(t, waitGroup.Description, actual.Description)
		require.Equal(t, waitGroup.Counter, actual.Counter)
		require.Equal(t, waitGroup.Completed, actual.Completed)
		require.Equal(t, waitGroup.CreatedAt, actual.CreatedAt)
		require.Equal(t, waitGroup.UpdatedAt, actual.UpdatedAt)
		require.Equal(t, waitGroup.ExpiresAt, actual.ExpiresAt)
	})

	t.Run("get nonexistent wait group", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newWaitGroupsTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		txn := badgerStore.View()
		defer txn.Discard()

		waitGroupId := &corepb.WaitGroupId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
			WaitGroupId: rand.Uint64(),
		}

		actual, err := table.Get(txn, waitGroupId)
		require.Error(t, err)
		require.Nil(t, actual)
		require.ErrorIs(t, err, store.ErrNotFound)
	})
}

func TestWaitGroupsTable_GetByName(t *testing.T) {
	t.Run("get wait group by name", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newWaitGroupsTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		waitGroup := &corepb.WaitGroup{
			Id: &corepb.WaitGroupId{
				AccountId:   rand.Uint64(),
				NamespaceId: rand.Uint32(),
				WaitGroupId: rand.Uint64(),
			},
			Name:        "test_wait_group",
			Description: "test description",
			Counter:     10,
			Completed:   0,
			CreatedAt:   rand.Int64(),
			UpdatedAt:   rand.Int64(),
			ExpiresAt:   rand.Int64(),
		}

		txn := badgerStore.Update()
		err = table.Create(txn, waitGroup)
		require.NoError(t, err)
		err = txn.Commit()
		require.NoError(t, err)

		// Get by name
		txn = badgerStore.View()
		defer txn.Discard()

		actual, err := table.GetByName(txn, waitGroup.Id.AccountId, waitGroup.Id.NamespaceId, waitGroup.Name)
		require.NoError(t, err)
		require.NotNil(t, actual)
		require.Equal(t, waitGroup.Id.WaitGroupId, actual.Id.WaitGroupId)
		require.Equal(t, waitGroup.Name, actual.Name)
		require.Equal(t, waitGroup.Description, actual.Description)
	})

	t.Run("get nonexistent wait group by name", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newWaitGroupsTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		txn := badgerStore.View()
		defer txn.Discard()

		actual, err := table.GetByName(txn, rand.Uint64(), rand.Uint32(), "nonexistent_wait_group")
		require.Error(t, err)
		require.Nil(t, actual)
		require.ErrorIs(t, err, store.ErrNotFound)
	})

	t.Run("get wait group by name from different namespace", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newWaitGroupsTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId1 := rand.Uint32()
		namespaceId2 := rand.Uint32()

		waitGroup := &corepb.WaitGroup{
			Id: &corepb.WaitGroupId{
				AccountId:   accountId,
				NamespaceId: namespaceId1,
				WaitGroupId: rand.Uint64(),
			},
			Name:        "test_wait_group",
			Description: "test description",
			Counter:     10,
			Completed:   0,
			CreatedAt:   rand.Int64(),
			UpdatedAt:   rand.Int64(),
			ExpiresAt:   rand.Int64(),
		}

		txn := badgerStore.Update()
		err = table.Create(txn, waitGroup)
		require.NoError(t, err)
		err = txn.Commit()
		require.NoError(t, err)

		// Try to get from different namespace
		txn = badgerStore.View()
		defer txn.Discard()

		actual, err := table.GetByName(txn, accountId, namespaceId2, "test_wait_group")
		require.Error(t, err)
		require.Nil(t, actual)
		require.ErrorIs(t, err, store.ErrNotFound)
	})
}

func TestWaitGroupsTable_Update(t *testing.T) {
	t.Run("update wait group", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newWaitGroupsTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		waitGroup := &corepb.WaitGroup{
			Id: &corepb.WaitGroupId{
				AccountId:   rand.Uint64(),
				NamespaceId: rand.Uint32(),
				WaitGroupId: rand.Uint64(),
			},
			Name:        "test_wait_group",
			Description: "test description",
			Counter:     10,
			Completed:   0,
			CreatedAt:   rand.Int64(),
			UpdatedAt:   rand.Int64(),
			ExpiresAt:   rand.Int64(),
		}

		txn := badgerStore.Update()
		err = table.Create(txn, waitGroup)
		require.NoError(t, err)
		err = txn.Commit()
		require.NoError(t, err)

		// Update wait group
		waitGroup.Counter = 20
		waitGroup.Completed = 5
		newUpdateTime := rand.Int64()
		waitGroup.UpdatedAt = newUpdateTime

		txn = badgerStore.Update()
		err = table.Update(txn, waitGroup)
		require.NoError(t, err)
		err = txn.Commit()
		require.NoError(t, err)

		// Verify update
		txn = badgerStore.View()
		defer txn.Discard()

		actual, err := table.Get(txn, waitGroup.Id)
		require.NoError(t, err)
		require.NotNil(t, actual)
		require.Equal(t, uint64(20), actual.Counter)
		require.Equal(t, uint64(5), actual.Completed)
		require.Equal(t, newUpdateTime, actual.UpdatedAt)
		require.Equal(t, waitGroup.Name, actual.Name)
		require.Equal(t, waitGroup.Description, actual.Description)
	})

	t.Run("update nonexistent wait group creates it", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newWaitGroupsTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		waitGroup := &corepb.WaitGroup{
			Id: &corepb.WaitGroupId{
				AccountId:   rand.Uint64(),
				NamespaceId: rand.Uint32(),
				WaitGroupId: rand.Uint64(),
			},
			Name:        "test_wait_group",
			Description: "test description",
			Counter:     10,
			Completed:   0,
			CreatedAt:   rand.Int64(),
			UpdatedAt:   rand.Int64(),
			ExpiresAt:   rand.Int64(),
		}

		txn := badgerStore.Update()
		err = table.Update(txn, waitGroup)
		require.NoError(t, err)
		err = txn.Commit()
		require.NoError(t, err)

		// Verify wait group exists
		txn = badgerStore.View()
		defer txn.Discard()

		actual, err := table.Get(txn, waitGroup.Id)
		require.NoError(t, err)
		require.NotNil(t, actual)
		require.Equal(t, waitGroup.Name, actual.Name)
		require.Equal(t, waitGroup.Description, actual.Description)
	})
}

func TestWaitGroupsTable_Delete(t *testing.T) {
	t.Run("delete existing wait group", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newWaitGroupsTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		waitGroup := &corepb.WaitGroup{
			Id: &corepb.WaitGroupId{
				AccountId:   rand.Uint64(),
				NamespaceId: rand.Uint32(),
				WaitGroupId: rand.Uint64(),
			},
			Name:        "test_wait_group",
			Description: "test description",
			Counter:     10,
			Completed:   0,
			CreatedAt:   rand.Int64(),
			UpdatedAt:   rand.Int64(),
			ExpiresAt:   rand.Int64(),
		}

		txn := badgerStore.Update()
		err = table.Create(txn, waitGroup)
		require.NoError(t, err)
		err = txn.Commit()
		require.NoError(t, err)

		// Delete wait group (should delete from both main table and name index)
		txn = badgerStore.Update()
		err = table.Delete(txn, waitGroup.Id)
		require.NoError(t, err)
		err = txn.Commit()
		require.NoError(t, err)

		// Verify deletion
		txn = badgerStore.View()
		defer txn.Discard()

		actual, err := table.Get(txn, waitGroup.Id)
		require.Error(t, err)
		require.Nil(t, actual)
		require.ErrorIs(t, err, store.ErrNotFound)

		_, err = table.namesIndex.Get(txn, table.namesIndexPK(waitGroup.Id.AccountId, waitGroup.Id.NamespaceId, waitGroup.Name))
		require.Error(t, err)
		require.ErrorIs(t, err, store.ErrNotFound)

		// GetByName should also fail
		actual, err = table.GetByName(txn, waitGroup.Id.AccountId, waitGroup.Id.NamespaceId, waitGroup.Name)
		require.Error(t, err)
		require.Nil(t, actual)
		require.ErrorIs(t, err, store.ErrNotFound)
	})

	t.Run("delete nonexistent wait group does not error", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newWaitGroupsTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		waitGroupId := &corepb.WaitGroupId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
			WaitGroupId: rand.Uint64(),
		}

		txn := badgerStore.Update()
		err = table.Delete(txn, waitGroupId)
		require.NoError(t, err)
		err = txn.Commit()
		require.NoError(t, err)
	})
}

func TestWaitGroupsTable_List(t *testing.T) {
	t.Run("list wait groups in namespace", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newWaitGroupsTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		// Create multiple wait groups
		numWaitGroups := 5
		for i := range numWaitGroups {
			waitGroup := &corepb.WaitGroup{
				Id: &corepb.WaitGroupId{
					AccountId:   accountId,
					NamespaceId: namespaceId,
					WaitGroupId: uint64(i),
				},
				Name:        "test_wait_group_" + string(rune(i)),
				Description: "test description",
				Counter:     10,
				Completed:   0,
				CreatedAt:   rand.Int64(),
				UpdatedAt:   rand.Int64(),
				ExpiresAt:   rand.Int64(),
			}

			txn := badgerStore.Update()
			err := table.Create(txn, waitGroup)
			require.NoError(t, err)
			err = txn.Commit()
			require.NoError(t, err)
		}

		// List all wait groups
		txn := badgerStore.View()
		defer txn.Discard()

		result, err := table.List(txn, accountId, namespaceId, nil, 100)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Len(t, result.waitGroups, numWaitGroups)
		require.Nil(t, result.nextPaginationToken)
		require.Nil(t, result.previousPaginationToken)
	})

	t.Run("list wait groups with pagination", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newWaitGroupsTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		// Create multiple wait groups
		numWaitGroups := 10
		for i := range numWaitGroups {
			waitGroup := &corepb.WaitGroup{
				Id: &corepb.WaitGroupId{
					AccountId:   accountId,
					NamespaceId: namespaceId,
					WaitGroupId: uint64(i),
				},
				Name:        "test_wait_group_" + string(rune(i)),
				Description: "test description",
				Counter:     10,
				Completed:   0,
				CreatedAt:   rand.Int64(),
				UpdatedAt:   rand.Int64(),
				ExpiresAt:   rand.Int64(),
			}

			txn := badgerStore.Update()
			err := table.Create(txn, waitGroup)
			require.NoError(t, err)
			err = txn.Commit()
			require.NoError(t, err)
		}

		// List first page
		txn := badgerStore.View()
		defer txn.Discard()

		page1, err := table.List(txn, accountId, namespaceId, nil, 3)
		require.NoError(t, err)
		require.NotNil(t, page1)
		require.Len(t, page1.waitGroups, 3)
		require.NotNil(t, page1.nextPaginationToken)
		require.Nil(t, page1.previousPaginationToken)

		// List second page
		page2, err := table.List(txn, accountId, namespaceId, page1.nextPaginationToken, 3)
		require.NoError(t, err)
		require.NotNil(t, page2)
		require.Len(t, page2.waitGroups, 3)
		require.NotNil(t, page2.nextPaginationToken)
		require.NotNil(t, page2.previousPaginationToken)

		// Verify different pages have different wait groups
		require.NotEqual(t, page1.waitGroups[0].Id.WaitGroupId, page2.waitGroups[0].Id.WaitGroupId)
	})

	t.Run("list empty namespace", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newWaitGroupsTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		txn := badgerStore.View()
		defer txn.Discard()

		result, err := table.List(txn, rand.Uint64(), rand.Uint32(), nil, 100)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Len(t, result.waitGroups, 0)
		require.Nil(t, result.nextPaginationToken)
		require.Nil(t, result.previousPaginationToken)
	})

	t.Run("list wait groups from different namespaces are isolated", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newWaitGroupsTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId1 := rand.Uint32()
		namespaceId2 := rand.Uint32()

		// Create wait groups in namespace 1
		for i := range 3 {
			waitGroup := &corepb.WaitGroup{
				Id: &corepb.WaitGroupId{
					AccountId:   accountId,
					NamespaceId: namespaceId1,
					WaitGroupId: uint64(i),
				},
				Name:        "test_wait_group_ns1_" + string(rune(i)),
				Description: "test description",
				Counter:     10,
				Completed:   0,
				CreatedAt:   rand.Int64(),
				UpdatedAt:   rand.Int64(),
				ExpiresAt:   rand.Int64(),
			}

			txn := badgerStore.Update()
			err := table.Create(txn, waitGroup)
			require.NoError(t, err)
			err = txn.Commit()
			require.NoError(t, err)
		}

		// Create wait groups in namespace 2
		for i := range 5 {
			waitGroup := &corepb.WaitGroup{
				Id: &corepb.WaitGroupId{
					AccountId:   accountId,
					NamespaceId: namespaceId2,
					WaitGroupId: uint64(i),
				},
				Name:        "test_wait_group_ns2_" + string(rune(i)),
				Description: "test description",
				Counter:     10,
				Completed:   0,
				CreatedAt:   rand.Int64(),
				UpdatedAt:   rand.Int64(),
				ExpiresAt:   rand.Int64(),
			}

			txn := badgerStore.Update()
			err := table.Create(txn, waitGroup)
			require.NoError(t, err)
			err = txn.Commit()
			require.NoError(t, err)
		}

		txn := badgerStore.View()
		defer txn.Discard()

		// List namespace 1
		result1, err := table.List(txn, accountId, namespaceId1, nil, 100)
		require.NoError(t, err)
		require.Len(t, result1.waitGroups, 3)

		// List namespace 2
		result2, err := table.List(txn, accountId, namespaceId2, nil, 100)
		require.NoError(t, err)
		require.Len(t, result2.waitGroups, 5)
	})
}

func TestWaitGroupsTable_GetTableKeyRanges(t *testing.T) {
	t.Run("get table key ranges", func(t *testing.T) {
		table := newWaitGroupsTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		keyRanges := table.GetTableKeyRanges()
		require.NotNil(t, keyRanges)
		require.Len(t, keyRanges, 2)
	})
}
