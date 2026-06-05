package semaphores

import (
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/evrblk/monstera/store"
	monsterax "github.com/evrblk/monstera/x"
	"github.com/stretchr/testify/require"

	"github.com/evrblk/grackle/pkg/corepb"
)

func TestSemaphoresTable_Get(t *testing.T) {
	t.Run("gets a semaphore", func(t *testing.T) {
		store, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newSemaphoresTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()
		semaphoreId := rand.Uint64()
		now := time.Now()

		semaphore := &corepb.Semaphore{
			Id: &corepb.SemaphoreId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
				SemaphoreId: semaphoreId,
			},
			Name:               "test_semaphore",
			Description:        "test description",
			CreatedAt:          now.UnixNano(),
			UpdatedAt:          now.UnixNano(),
			Permits:            5,
			ActiveHolds:        2,
			ActiveHoldersCount: 1,
		}

		// Create semaphore
		txn := store.Update()
		appErr, err := table.Create(txn, semaphore)
		require.NoError(t, err)
		require.Nil(t, appErr)
		require.NoError(t, txn.Commit())

		// Get semaphore
		txn = store.View()
		actual, err := table.Get(txn, semaphore.Id)
		txn.Discard()

		require.NoError(t, err)
		require.NotNil(t, actual)
		require.Equal(t, semaphore.Id.AccountId, actual.Id.AccountId)
		require.Equal(t, semaphore.Id.NamespaceId, actual.Id.NamespaceId)
		require.Equal(t, semaphore.Id.SemaphoreId, actual.Id.SemaphoreId)
		require.Equal(t, semaphore.Name, actual.Name)
		require.Equal(t, semaphore.Description, actual.Description)
		require.Equal(t, semaphore.Permits, actual.Permits)
		require.Equal(t, semaphore.ActiveHolds, actual.ActiveHolds)
		require.Equal(t, semaphore.ActiveHoldersCount, actual.ActiveHoldersCount)
	})
	t.Run("gets a non-existent semaphore", func(t *testing.T) {
		store, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newSemaphoresTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()
		semaphoreId := rand.Uint64()

		semaphoreIdProto := &corepb.SemaphoreId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			SemaphoreId: semaphoreId,
		}

		txn := store.View()
		_, err = table.Get(txn, semaphoreIdProto)
		txn.Discard()

		require.Error(t, err)
		require.Contains(t, err.Error(), "not found")
	})
}
func TestSemaphoresTable_GetByName(t *testing.T) {
	t.Run("gets a semaphore by name", func(t *testing.T) {
		store, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newSemaphoresTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()
		semaphoreId := rand.Uint64()
		now := time.Now()

		semaphore := &corepb.Semaphore{
			Id: &corepb.SemaphoreId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
				SemaphoreId: semaphoreId,
			},
			Name:               "test_semaphore",
			Description:        "test description",
			CreatedAt:          now.UnixNano(),
			UpdatedAt:          now.UnixNano(),
			Permits:            5,
			ActiveHolds:        0,
			ActiveHoldersCount: 0,
		}

		// Create semaphore
		txn := store.Update()
		appErr, err := table.Create(txn, semaphore)
		require.NoError(t, err)
		require.Nil(t, appErr)
		require.NoError(t, txn.Commit())

		// Get semaphore by name
		txn = store.View()
		actual, err := table.GetByName(txn, accountId, namespaceId, "test_semaphore")
		txn.Discard()

		require.NoError(t, err)
		require.NotNil(t, actual)
		require.Equal(t, semaphore.Id.SemaphoreId, actual.Id.SemaphoreId)
		require.Equal(t, semaphore.Name, actual.Name)
		require.Equal(t, semaphore.Description, actual.Description)
	})

	t.Run("gets a non-existent semaphore by name", func(t *testing.T) {
		store, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newSemaphoresTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		txn := store.View()
		_, err = table.GetByName(txn, accountId, namespaceId, "nonexistent_semaphore")
		txn.Discard()

		require.Error(t, err)
		require.Contains(t, err.Error(), "not found")
	})
}

func TestSemaphoresTable_Create(t *testing.T) {
	t.Run("creates a semaphore", func(t *testing.T) {
		store, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newSemaphoresTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()
		semaphoreId := rand.Uint64()
		now := time.Now()

		semaphore := &corepb.Semaphore{
			Id: &corepb.SemaphoreId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
				SemaphoreId: semaphoreId,
			},
			Name:               "test_semaphore",
			Description:        "test description",
			CreatedAt:          now.UnixNano(),
			UpdatedAt:          now.UnixNano(),
			Permits:            10,
			ActiveHolds:        0,
			ActiveHoldersCount: 0,
		}

		// Create semaphore
		txn := store.Update()
		appErr, err := table.Create(txn, semaphore)
		require.NoError(t, err)
		require.Nil(t, appErr)
		require.NoError(t, txn.Commit())

		// Verify semaphore was created in main table
		txn = store.View()
		actual, err := table.Get(txn, semaphore.Id)
		require.NoError(t, err)
		require.NotNil(t, actual)
		require.Equal(t, semaphore.Name, actual.Name)

		// Verify semaphore was indexed by name
		semaphoreIdFromIndex, err := table.namesIndex.Get(txn,
			table.namesIndexPK(accountId, namespaceId, "test_semaphore"))
		txn.Discard()

		require.NoError(t, err)
		require.Equal(t, semaphoreId, semaphoreIdFromIndex)
	})
	t.Run("creates a semaphore with a duplicate name", func(t *testing.T) {
		store, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newSemaphoresTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()
		now := time.Now()

		semaphore1 := &corepb.Semaphore{
			Id: &corepb.SemaphoreId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
				SemaphoreId: rand.Uint64(),
			},
			Name:               "duplicate_name",
			Description:        "first semaphore",
			CreatedAt:          now.UnixNano(),
			UpdatedAt:          now.UnixNano(),
			Permits:            5,
			ActiveHolds:        0,
			ActiveHoldersCount: 0,
		}

		semaphore2 := &corepb.Semaphore{
			Id: &corepb.SemaphoreId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
				SemaphoreId: rand.Uint64(),
			},
			Name:               "duplicate_name",
			Description:        "second semaphore",
			CreatedAt:          now.UnixNano(),
			UpdatedAt:          now.UnixNano(),
			Permits:            10,
			ActiveHolds:        0,
			ActiveHoldersCount: 0,
		}

		// Create first semaphore
		txn := store.Update()
		appErr, err := table.Create(txn, semaphore1)
		require.NoError(t, err)
		require.Nil(t, appErr)
		require.NoError(t, txn.Commit())

		// Try to create second semaphore with same name
		txn = store.Update()
		appErr, err = table.Create(txn, semaphore2)
		txn.Discard()

		require.NoError(t, err)
		require.NotNil(t, appErr)
		require.Equal(t, monsterax.AlreadyExists, appErr.Code)
		require.Contains(t, appErr.Message, "already exists")
	})
}
func TestSemaphoresTable_Update(t *testing.T) {
	t.Run("updates a semaphore", func(t *testing.T) {
		store, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newSemaphoresTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()
		semaphoreId := rand.Uint64()
		now := time.Now()

		semaphore := &corepb.Semaphore{
			Id: &corepb.SemaphoreId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
				SemaphoreId: semaphoreId,
			},
			Name:               "test_semaphore",
			Description:        "original description",
			CreatedAt:          now.UnixNano(),
			UpdatedAt:          now.UnixNano(),
			Permits:            5,
			ActiveHolds:        1,
			ActiveHoldersCount: 1,
		}

		// Create semaphore
		txn := store.Update()
		appErr, err := table.Create(txn, semaphore)
		require.NoError(t, err)
		require.Nil(t, appErr)
		require.NoError(t, txn.Commit())

		// Update semaphore
		updatedSemaphore := &corepb.Semaphore{
			Id: &corepb.SemaphoreId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
				SemaphoreId: semaphoreId,
			},
			Name:               "test_semaphore",
			Description:        "updated description",
			CreatedAt:          semaphore.CreatedAt,
			UpdatedAt:          now.Add(time.Hour).UnixNano(),
			Permits:            10,
			ActiveHolds:        3,
			ActiveHoldersCount: 2,
		}

		txn = store.Update()
		err = table.Update(txn, updatedSemaphore)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		// Verify semaphore was updated
		txn = store.View()
		actual, err := table.Get(txn, semaphore.Id)
		txn.Discard()

		require.NoError(t, err)
		require.Equal(t, "updated description", actual.Description)
		require.Equal(t, uint64(10), actual.Permits)
		require.Equal(t, uint64(3), actual.ActiveHolds)
		require.Equal(t, uint64(2), actual.ActiveHoldersCount)
		require.Equal(t, updatedSemaphore.UpdatedAt, actual.UpdatedAt)
	})
}

func TestSemaphoresTable_Delete(t *testing.T) {
	t.Run("deletes a semaphore", func(t *testing.T) {
		store, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newSemaphoresTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()
		semaphoreId := rand.Uint64()
		now := time.Now()

		semaphore := &corepb.Semaphore{
			Id: &corepb.SemaphoreId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
				SemaphoreId: semaphoreId,
			},
			Name:               "test_semaphore",
			Description:        "test description",
			CreatedAt:          now.UnixNano(),
			UpdatedAt:          now.UnixNano(),
			Permits:            5,
			ActiveHolds:        0,
			ActiveHoldersCount: 0,
		}

		// Create semaphore
		txn := store.Update()
		appErr, err := table.Create(txn, semaphore)
		require.NoError(t, err)
		require.Nil(t, appErr)
		require.NoError(t, txn.Commit())

		// Delete semaphore
		txn = store.Update()
		err = table.Delete(txn, semaphore.Id)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		// Verify semaphore was deleted from main table
		txn = store.View()
		_, err = table.Get(txn, semaphore.Id)
		txn.Discard()

		require.Error(t, err)
		require.Contains(t, err.Error(), "not found")
	})

	t.Run("deletes a non-existent semaphore", func(t *testing.T) {
		store, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newSemaphoresTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()
		semaphoreId := rand.Uint64()

		semaphoreIdProto := &corepb.SemaphoreId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			SemaphoreId: semaphoreId,
		}

		// Deleting nonexistent semaphore should succeed without error
		txn := store.Update()
		err = table.Delete(txn, semaphoreIdProto)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())
	})
}

func TestSemaphoresTable_List(t *testing.T) {
	t.Run("lists semaphores", func(t *testing.T) {
		store, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newSemaphoresTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()
		now := time.Now()

		// Create multiple semaphores
		semaphores := make([]*corepb.Semaphore, 5)
		for i := range semaphores {
			semaphores[i] = &corepb.Semaphore{
				Id: &corepb.SemaphoreId{
					AccountId:   accountId,
					NamespaceId: namespaceId,
					SemaphoreId: uint64(i + 1),
				},
				Name:               fmt.Sprintf("semaphore_%d", i),
				Description:        fmt.Sprintf("description %d", i),
				CreatedAt:          now.Add(time.Duration(i) * time.Minute).UnixNano(),
				UpdatedAt:          now.Add(time.Duration(i) * time.Minute).UnixNano(),
				Permits:            uint64(i + 1),
				ActiveHolds:        0,
				ActiveHoldersCount: 0,
			}
		}

		// Create all semaphores
		txn := store.Update()
		for _, semaphore := range semaphores {
			appErr, err := table.Create(txn, semaphore)
			require.NoError(t, err)
			require.Nil(t, appErr)
		}
		require.NoError(t, txn.Commit())

		// List all semaphores
		txn = store.View()
		result, err := table.List(txn, accountId, namespaceId, nil, 10)
		txn.Discard()

		require.NoError(t, err)
		require.Len(t, result.semaphores, 5)
		require.Nil(t, result.nextPaginationToken)

		// Verify semaphores are sorted by semaphore id (sort key)
		for i, semaphore := range result.semaphores {
			require.Equal(t, uint64(i+1), semaphore.Id.SemaphoreId)
			require.Equal(t, fmt.Sprintf("semaphore_%d", i), semaphore.Name)
		}
	})

	t.Run("lists semaphores with pagination", func(t *testing.T) {
		store, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newSemaphoresTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()
		now := time.Now()

		// Create multiple semaphores
		numSemaphores := 10
		for i := range numSemaphores {
			semaphore := &corepb.Semaphore{
				Id: &corepb.SemaphoreId{
					AccountId:   accountId,
					NamespaceId: namespaceId,
					SemaphoreId: uint64(i + 1),
				},
				Name:               fmt.Sprintf("semaphore_%02d", i),
				Description:        fmt.Sprintf("description %d", i),
				CreatedAt:          now.Add(time.Duration(i) * time.Minute).UnixNano(),
				UpdatedAt:          now.Add(time.Duration(i) * time.Minute).UnixNano(),
				Permits:            uint64(i + 1),
				ActiveHolds:        0,
				ActiveHoldersCount: 0,
			}

			txn := store.Update()
			appErr, err := table.Create(txn, semaphore)
			require.NoError(t, err)
			require.Nil(t, appErr)
			require.NoError(t, txn.Commit())
		}

		// List first page
		txn := store.View()
		result1, err := table.List(txn, accountId, namespaceId, nil, 3)
		txn.Discard()

		require.NoError(t, err)
		require.Len(t, result1.semaphores, 3)
		require.NotNil(t, result1.nextPaginationToken)
		require.Nil(t, result1.previousPaginationToken)
		require.Equal(t, uint64(1), result1.semaphores[0].Id.SemaphoreId)
		require.Equal(t, uint64(2), result1.semaphores[1].Id.SemaphoreId)
		require.Equal(t, uint64(3), result1.semaphores[2].Id.SemaphoreId)

		// List second page
		txn = store.View()
		result2, err := table.List(txn, accountId, namespaceId, result1.nextPaginationToken, 3)
		txn.Discard()

		require.NoError(t, err)
		require.Len(t, result2.semaphores, 3)
		require.NotNil(t, result2.nextPaginationToken)
		require.NotNil(t, result2.previousPaginationToken)
		require.Equal(t, uint64(4), result2.semaphores[0].Id.SemaphoreId)
		require.Equal(t, uint64(5), result2.semaphores[1].Id.SemaphoreId)
		require.Equal(t, uint64(6), result2.semaphores[2].Id.SemaphoreId)

		// List third page
		txn = store.View()
		result3, err := table.List(txn, accountId, namespaceId, result2.nextPaginationToken, 3)
		txn.Discard()

		require.NoError(t, err)
		require.Len(t, result3.semaphores, 3)
		require.NotNil(t, result3.nextPaginationToken)
		require.NotNil(t, result3.previousPaginationToken)

		// List fourth page (last page)
		txn = store.View()
		result4, err := table.List(txn, accountId, namespaceId, result3.nextPaginationToken, 3)
		txn.Discard()

		require.NoError(t, err)
		require.Len(t, result4.semaphores, 1)
		require.Nil(t, result4.nextPaginationToken)
		require.NotNil(t, result4.previousPaginationToken)
		require.Equal(t, uint64(10), result4.semaphores[0].Id.SemaphoreId)
	})

	t.Run("lists empty semaphores", func(t *testing.T) {
		store, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newSemaphoresTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		txn := store.View()
		result, err := table.List(txn, accountId, namespaceId, nil, 10)
		txn.Discard()

		require.NoError(t, err)
		require.Empty(t, result.semaphores)
		require.Nil(t, result.nextPaginationToken)
	})
	t.Run("lists semaphores in multiple namespaces", func(t *testing.T) {
		store, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newSemaphoresTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId1 := rand.Uint32()
		namespaceId2 := rand.Uint32()
		now := time.Now()

		// Create semaphore in first namespace
		semaphore1 := &corepb.Semaphore{
			Id: &corepb.SemaphoreId{
				AccountId:   accountId,
				NamespaceId: namespaceId1,
				SemaphoreId: rand.Uint64(),
			},
			Name:               "semaphore_ns1",
			Description:        "namespace 1 semaphore",
			CreatedAt:          now.UnixNano(),
			UpdatedAt:          now.UnixNano(),
			Permits:            5,
			ActiveHolds:        0,
			ActiveHoldersCount: 0,
		}

		// Create semaphore in second namespace
		semaphore2 := &corepb.Semaphore{
			Id: &corepb.SemaphoreId{
				AccountId:   accountId,
				NamespaceId: namespaceId2,
				SemaphoreId: rand.Uint64(),
			},
			Name:               "semaphore_ns2",
			Description:        "namespace 2 semaphore",
			CreatedAt:          now.UnixNano(),
			UpdatedAt:          now.UnixNano(),
			Permits:            10,
			ActiveHolds:        0,
			ActiveHoldersCount: 0,
		}

		txn := store.Update()
		appErr, err := table.Create(txn, semaphore1)
		require.NoError(t, err)
		require.Nil(t, appErr)
		appErr, err = table.Create(txn, semaphore2)
		require.NoError(t, err)
		require.Nil(t, appErr)
		require.NoError(t, txn.Commit())

		// List semaphores in first namespace
		txn = store.View()
		result1, err := table.List(txn, accountId, namespaceId1, nil, 10)
		require.NoError(t, err)
		require.Len(t, result1.semaphores, 1)
		require.Equal(t, "semaphore_ns1", result1.semaphores[0].Name)

		// List semaphores in second namespace
		result2, err := table.List(txn, accountId, namespaceId2, nil, 10)
		txn.Discard()

		require.NoError(t, err)
		require.Len(t, result2.semaphores, 1)
		require.Equal(t, "semaphore_ns2", result2.semaphores[0].Name)
	})
}
func TestSemaphoresTable_SameNameDifferentNamespaces(t *testing.T) {
	store, err := store.NewBadgerInMemoryStore()
	require.NoError(t, err)

	table := newSemaphoresTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

	accountId := rand.Uint64()
	namespaceId1 := rand.Uint32()
	namespaceId2 := rand.Uint32()
	now := time.Now()

	// Create semaphore with same name in first namespace
	semaphore1 := &corepb.Semaphore{
		Id: &corepb.SemaphoreId{
			AccountId:   accountId,
			NamespaceId: namespaceId1,
			SemaphoreId: rand.Uint64(),
		},
		Name:               "shared_name",
		Description:        "namespace 1 semaphore",
		CreatedAt:          now.UnixNano(),
		UpdatedAt:          now.UnixNano(),
		Permits:            5,
		ActiveHolds:        0,
		ActiveHoldersCount: 0,
	}

	// Create semaphore with same name in second namespace (should succeed)
	semaphore2 := &corepb.Semaphore{
		Id: &corepb.SemaphoreId{
			AccountId:   accountId,
			NamespaceId: namespaceId2,
			SemaphoreId: rand.Uint64(),
		},
		Name:               "shared_name",
		Description:        "namespace 2 semaphore",
		CreatedAt:          now.UnixNano(),
		UpdatedAt:          now.UnixNano(),
		Permits:            10,
		ActiveHolds:        0,
		ActiveHoldersCount: 0,
	}

	txn := store.Update()
	appErr, err := table.Create(txn, semaphore1)
	require.NoError(t, err)
	require.Nil(t, appErr)
	appErr, err = table.Create(txn, semaphore2)
	require.NoError(t, err)
	require.Nil(t, appErr)
	require.NoError(t, txn.Commit())

	// Get by name in first namespace
	txn = store.View()
	retrieved1, err := table.GetByName(txn, accountId, namespaceId1, "shared_name")
	require.NoError(t, err)
	require.Equal(t, semaphore1.Id.SemaphoreId, retrieved1.Id.SemaphoreId)
	require.Equal(t, "namespace 1 semaphore", retrieved1.Description)

	// Get by name in second namespace
	retrieved2, err := table.GetByName(txn, accountId, namespaceId2, "shared_name")
	txn.Discard()

	require.NoError(t, err)
	require.Equal(t, semaphore2.Id.SemaphoreId, retrieved2.Id.SemaphoreId)
	require.Equal(t, "namespace 2 semaphore", retrieved2.Description)
}

func TestSemaphoresTable_GetTableKeyRanges(t *testing.T) {
	shardLower := []byte{0x00, 0x00, 0x00, 0x00}
	shardUpper := []byte{0xff, 0xff, 0xff, 0xff}
	table := newSemaphoresTable(shardLower, shardUpper)

	keyRanges := table.GetTableKeyRanges()

	require.Len(t, keyRanges, 3)
	require.NotNil(t, keyRanges[0])
	require.NotNil(t, keyRanges[1])
	require.NotNil(t, keyRanges[2])
}

func TestSemaphoresTable_NameIndexConsistency(t *testing.T) {
	store, err := store.NewBadgerInMemoryStore()
	require.NoError(t, err)

	table := newSemaphoresTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

	accountId := rand.Uint64()
	namespaceId := rand.Uint32()
	semaphoreId := rand.Uint64()
	now := time.Now()

	semaphore := &corepb.Semaphore{
		Id: &corepb.SemaphoreId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			SemaphoreId: semaphoreId,
		},
		Name:               "test_semaphore",
		Description:        "test description",
		CreatedAt:          now.UnixNano(),
		UpdatedAt:          now.UnixNano(),
		Permits:            5,
		ActiveHolds:        0,
		ActiveHoldersCount: 0,
	}

	// Create semaphore
	txn := store.Update()
	appErr, err := table.Create(txn, semaphore)
	require.NoError(t, err)
	require.Nil(t, appErr)
	require.NoError(t, txn.Commit())

	// Verify Get and GetByName return the same semaphore
	txn = store.View()
	semaphoreById, err := table.Get(txn, semaphore.Id)
	require.NoError(t, err)

	semaphoreByName, err := table.GetByName(txn, accountId, namespaceId, "test_semaphore")
	txn.Discard()

	require.NoError(t, err)
	require.Equal(t, semaphoreById.Id.SemaphoreId, semaphoreByName.Id.SemaphoreId)
	require.Equal(t, semaphoreById.Name, semaphoreByName.Name)
	require.Equal(t, semaphoreById.Description, semaphoreByName.Description)
	require.Equal(t, semaphoreById.Permits, semaphoreByName.Permits)
}
