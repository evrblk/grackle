package namespaces

import (
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/evrblk/monstera/store"

	"github.com/evrblk/grackle/pkg/corepb"
)

func TestNamespacesTable_Create(t *testing.T) {
	t.Run("create namespace", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newNamespacesTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		namespace := &corepb.Namespace{
			Id: &corepb.NamespaceId{
				AccountId:   rand.Uint64(),
				NamespaceId: rand.Uint64(),
			},
			Name:        "test_namespace",
			Description: "test description",
			CreatedAt:   rand.Int64(),
			UpdatedAt:   rand.Int64(),
		}

		txn := badgerStore.Update()
		err = table.Create(txn, namespace)
		require.NoError(t, err)

		err = txn.Commit()
		require.NoError(t, err)

		// Verify namespace was created
		txn = badgerStore.View()
		defer txn.Discard()

		retrieved, err := table.Get(txn, namespace.Id)
		require.NoError(t, err)
		require.NotNil(t, retrieved)
		require.Equal(t, namespace.Name, retrieved.Name)
		require.Equal(t, namespace.Description, retrieved.Description)
		require.Equal(t, namespace.CreatedAt, retrieved.CreatedAt)
		require.Equal(t, namespace.UpdatedAt, retrieved.UpdatedAt)
	})

	t.Run("create namespace creates name index entry", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newNamespacesTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		namespace := &corepb.Namespace{
			Id: &corepb.NamespaceId{
				AccountId:   rand.Uint64(),
				NamespaceId: rand.Uint64(),
			},
			Name:        "test_namespace",
			Description: "test description",
			CreatedAt:   rand.Int64(),
			UpdatedAt:   rand.Int64(),
		}

		txn := badgerStore.Update()
		err = table.Create(txn, namespace)
		require.NoError(t, err)

		err = txn.Commit()
		require.NoError(t, err)

		// Verify namespace can be retrieved by name
		txn = badgerStore.View()
		defer txn.Discard()

		retrieved, err := table.GetByName(txn, namespace.Id.AccountId, namespace.Name)
		require.NoError(t, err)
		require.NotNil(t, retrieved)
		require.Equal(t, namespace.Id.NamespaceId, retrieved.Id.NamespaceId)
		require.Equal(t, namespace.Name, retrieved.Name)
	})
}

func TestNamespacesTable_Get(t *testing.T) {
	t.Run("get existing namespace", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newNamespacesTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		namespace := &corepb.Namespace{
			Id: &corepb.NamespaceId{
				AccountId:   rand.Uint64(),
				NamespaceId: rand.Uint64(),
			},
			Name:        "test_namespace",
			Description: "test description",
			CreatedAt:   rand.Int64(),
			UpdatedAt:   rand.Int64(),
		}

		txn := badgerStore.Update()
		err = table.Create(txn, namespace)
		require.NoError(t, err)
		err = txn.Commit()
		require.NoError(t, err)

		// Get namespace
		txn = badgerStore.View()
		defer txn.Discard()

		retrieved, err := table.Get(txn, namespace.Id)
		require.NoError(t, err)
		require.NotNil(t, retrieved)
		require.Equal(t, namespace.Id.AccountId, retrieved.Id.AccountId)
		require.Equal(t, namespace.Id.NamespaceId, retrieved.Id.NamespaceId)
		require.Equal(t, namespace.Name, retrieved.Name)
		require.Equal(t, namespace.Description, retrieved.Description)
		require.Equal(t, namespace.CreatedAt, retrieved.CreatedAt)
		require.Equal(t, namespace.UpdatedAt, retrieved.UpdatedAt)
	})

	t.Run("get nonexistent namespace", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newNamespacesTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		txn := badgerStore.View()
		defer txn.Discard()

		namespaceId := &corepb.NamespaceId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint64(),
		}

		retrieved, err := table.Get(txn, namespaceId)
		require.Error(t, err)
		require.Nil(t, retrieved)
		require.ErrorIs(t, err, store.ErrNotFound)
	})
}

func TestNamespacesTable_GetByName(t *testing.T) {
	t.Run("get namespace by name", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newNamespacesTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		namespace := &corepb.Namespace{
			Id: &corepb.NamespaceId{
				AccountId:   rand.Uint64(),
				NamespaceId: rand.Uint64(),
			},
			Name:        "test_namespace",
			Description: "test description",
			CreatedAt:   rand.Int64(),
			UpdatedAt:   rand.Int64(),
		}

		txn := badgerStore.Update()
		err = table.Create(txn, namespace)
		require.NoError(t, err)
		err = txn.Commit()
		require.NoError(t, err)

		// Get by name
		txn = badgerStore.View()
		defer txn.Discard()

		retrieved, err := table.GetByName(txn, namespace.Id.AccountId, namespace.Name)
		require.NoError(t, err)
		require.NotNil(t, retrieved)
		require.Equal(t, namespace.Id.NamespaceId, retrieved.Id.NamespaceId)
		require.Equal(t, namespace.Name, retrieved.Name)
		require.Equal(t, namespace.Description, retrieved.Description)
	})

	t.Run("get nonexistent namespace by name", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newNamespacesTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		txn := badgerStore.View()
		defer txn.Discard()

		retrieved, err := table.GetByName(txn, rand.Uint64(), "nonexistent_namespace")
		require.Error(t, err)
		require.Nil(t, retrieved)
		require.ErrorIs(t, err, store.ErrNotFound)
	})

	t.Run("get namespace by name from different account", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newNamespacesTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId1 := rand.Uint64()
		accountId2 := rand.Uint64()

		namespace := &corepb.Namespace{
			Id: &corepb.NamespaceId{
				AccountId:   accountId1,
				NamespaceId: rand.Uint64(),
			},
			Name:        "test_namespace",
			Description: "test description",
			CreatedAt:   rand.Int64(),
			UpdatedAt:   rand.Int64(),
		}

		txn := badgerStore.Update()
		err = table.Create(txn, namespace)
		require.NoError(t, err)
		err = txn.Commit()
		require.NoError(t, err)

		// Try to get from different account
		txn = badgerStore.View()
		defer txn.Discard()

		retrieved, err := table.GetByName(txn, accountId2, "test_namespace")
		require.Error(t, err)
		require.Nil(t, retrieved)
		require.ErrorIs(t, err, store.ErrNotFound)
	})
}

func TestNamespacesTable_Update(t *testing.T) {
	t.Run("update namespace", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newNamespacesTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		namespace := &corepb.Namespace{
			Id: &corepb.NamespaceId{
				AccountId:   rand.Uint64(),
				NamespaceId: rand.Uint64(),
			},
			Name:        "test_namespace",
			Description: "original description",
			CreatedAt:   rand.Int64(),
			UpdatedAt:   rand.Int64(),
		}

		txn := badgerStore.Update()
		err = table.Create(txn, namespace)
		require.NoError(t, err)
		err = txn.Commit()
		require.NoError(t, err)

		// Update namespace
		namespace.Description = "updated description"
		newUpdateTime := rand.Int64()
		namespace.UpdatedAt = newUpdateTime

		txn = badgerStore.Update()
		err = table.Update(txn, namespace)
		require.NoError(t, err)
		err = txn.Commit()
		require.NoError(t, err)

		// Verify update
		txn = badgerStore.View()
		defer txn.Discard()

		retrieved, err := table.Get(txn, namespace.Id)
		require.NoError(t, err)
		require.NotNil(t, retrieved)
		require.Equal(t, "updated description", retrieved.Description)
		require.Equal(t, newUpdateTime, retrieved.UpdatedAt)
		require.Equal(t, namespace.Name, retrieved.Name)
	})
}

func TestNamespacesTable_Delete(t *testing.T) {
	t.Run("delete existing namespace", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newNamespacesTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		namespace := &corepb.Namespace{
			Id: &corepb.NamespaceId{
				AccountId:   rand.Uint64(),
				NamespaceId: rand.Uint64(),
			},
			Name:        "test_namespace",
			Description: "test description",
			CreatedAt:   rand.Int64(),
			UpdatedAt:   rand.Int64(),
		}

		txn := badgerStore.Update()
		err = table.Create(txn, namespace)
		require.NoError(t, err)
		err = txn.Commit()
		require.NoError(t, err)

		// Delete namespace (should delete from both main table and name index)
		txn = badgerStore.Update()
		err = table.Delete(txn, namespace)
		require.NoError(t, err)
		err = txn.Commit()
		require.NoError(t, err)

		// Verify name index entry is also deleted
		txn = badgerStore.View()
		defer txn.Discard()

		_, err = table.namesIndex.Get(txn, table.namesIndexPK(namespace.Id.AccountId, namespace.Name))
		require.Error(t, err)
		require.ErrorIs(t, err, store.ErrNotFound)

		// GetByName should also fail
		retrieved, err := table.GetByName(txn, namespace.Id.AccountId, namespace.Name)
		require.Error(t, err)
		require.Nil(t, retrieved)
		require.ErrorIs(t, err, store.ErrNotFound)
	})

	t.Run("delete nonexistent namespace does not error", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newNamespacesTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		namespace := &corepb.Namespace{
			Id: &corepb.NamespaceId{
				AccountId:   rand.Uint64(),
				NamespaceId: rand.Uint64(),
			},
			Name:        "test_namespace",
			Description: "test description",
			CreatedAt:   rand.Int64(),
			UpdatedAt:   rand.Int64(),
		}

		txn := badgerStore.Update()
		err = table.Delete(txn, namespace)
		require.NoError(t, err)
		err = txn.Commit()
		require.NoError(t, err)
	})
}

func TestNamespacesTable_List(t *testing.T) {
	t.Run("list namespaces in account", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newNamespacesTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()

		// Create multiple namespaces
		numNamespaces := 5
		for i := range numNamespaces {
			namespace := &corepb.Namespace{
				Id: &corepb.NamespaceId{
					AccountId:   accountId,
					NamespaceId: uint64(i),
				},
				Name:        "test_namespace_" + string(rune(i)),
				Description: "test description",
				CreatedAt:   rand.Int64(),
				UpdatedAt:   rand.Int64(),
			}

			txn := badgerStore.Update()
			err := table.Create(txn, namespace)
			require.NoError(t, err)
			err = txn.Commit()
			require.NoError(t, err)
		}

		// List all namespaces
		txn := badgerStore.View()
		defer txn.Discard()

		result, err := table.List(txn, accountId, nil, 100)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Len(t, result.Namespaces, numNamespaces)
		require.Nil(t, result.NextPaginationToken)
		require.Nil(t, result.PreviousPaginationToken)
	})

	t.Run("list namespaces with pagination", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newNamespacesTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()

		// Create multiple namespaces
		numNamespaces := 10
		for i := range numNamespaces {
			namespace := &corepb.Namespace{
				Id: &corepb.NamespaceId{
					AccountId:   accountId,
					NamespaceId: uint64(i),
				},
				Name:        "test_namespace_" + string(rune(i)),
				Description: "test description",
				CreatedAt:   rand.Int64(),
				UpdatedAt:   rand.Int64(),
			}

			txn := badgerStore.Update()
			err := table.Create(txn, namespace)
			require.NoError(t, err)
			err = txn.Commit()
			require.NoError(t, err)
		}

		// List first page
		txn := badgerStore.View()
		defer txn.Discard()

		page1, err := table.List(txn, accountId, nil, 3)
		require.NoError(t, err)
		require.NotNil(t, page1)
		require.Len(t, page1.Namespaces, 3)
		require.NotNil(t, page1.NextPaginationToken)
		require.Nil(t, page1.PreviousPaginationToken)

		// List second page
		page2, err := table.List(txn, accountId, page1.NextPaginationToken, 3)
		require.NoError(t, err)
		require.NotNil(t, page2)
		require.Len(t, page2.Namespaces, 3)
		require.NotNil(t, page2.NextPaginationToken)
		require.NotNil(t, page2.PreviousPaginationToken)

		// Verify different pages have different namespaces
		require.NotEqual(t, page1.Namespaces[0].Id.NamespaceId, page2.Namespaces[0].Id.NamespaceId)
	})

	t.Run("list empty account", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newNamespacesTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		txn := badgerStore.View()
		defer txn.Discard()

		result, err := table.List(txn, rand.Uint64(), nil, 100)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Len(t, result.Namespaces, 0)
		require.Nil(t, result.NextPaginationToken)
		require.Nil(t, result.PreviousPaginationToken)
	})

	t.Run("list namespaces from different accounts are isolated", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newNamespacesTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId1 := rand.Uint64()
		accountId2 := rand.Uint64()

		// Create namespaces in account 1
		for i := range 3 {
			namespace := &corepb.Namespace{
				Id: &corepb.NamespaceId{
					AccountId:   accountId1,
					NamespaceId: uint64(i),
				},
				Name:        "test_namespace_acc1_" + string(rune(i)),
				Description: "test description",
				CreatedAt:   rand.Int64(),
				UpdatedAt:   rand.Int64(),
			}

			txn := badgerStore.Update()
			err := table.Create(txn, namespace)
			require.NoError(t, err)
			err = txn.Commit()
			require.NoError(t, err)
		}

		// Create namespaces in account 2
		for i := range 5 {
			namespace := &corepb.Namespace{
				Id: &corepb.NamespaceId{
					AccountId:   accountId2,
					NamespaceId: uint64(i),
				},
				Name:        "test_namespace_acc2_" + string(rune(i)),
				Description: "test description",
				CreatedAt:   rand.Int64(),
				UpdatedAt:   rand.Int64(),
			}

			txn := badgerStore.Update()
			err := table.Create(txn, namespace)
			require.NoError(t, err)
			err = txn.Commit()
			require.NoError(t, err)
		}

		txn := badgerStore.View()
		defer txn.Discard()

		// List account 1
		result1, err := table.List(txn, accountId1, nil, 100)
		require.NoError(t, err)
		require.Len(t, result1.Namespaces, 3)

		// List account 2
		result2, err := table.List(txn, accountId2, nil, 100)
		require.NoError(t, err)
		require.Len(t, result2.Namespaces, 5)
	})
}

func TestNamespacesTable_GetTableKeyRanges(t *testing.T) {
	t.Run("get table key ranges", func(t *testing.T) {
		table := newNamespacesTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		keyRanges := table.GetTableKeyRanges()
		require.NotNil(t, keyRanges)
		require.Len(t, keyRanges, 2)
	})
}
