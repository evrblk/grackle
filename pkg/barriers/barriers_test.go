package barriers

import (
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/evrblk/monstera/store"
	"github.com/stretchr/testify/require"

	"github.com/evrblk/grackle/pkg/corepb"
)

func TestBarriersTable_Get(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		barriersTable := newBarriersTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()
		barrierId := rand.Uint64()

		barrier := &corepb.Barrier{
			Id: &corepb.BarrierId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
				BarrierId:   barrierId,
			},
			Name:              "test_barrier",
			Description:       "Test barrier",
			ExpectedProcesses: 5,
			ArrivedProcesses:  2,
			Generation:        1,
			CreatedAt:         12345,
			UpdatedAt:         12345,
		}

		// Create barrier
		txn := badgerStore.Update()
		err = barriersTable.Create(txn, barrier)
		require.NoError(t, err)
		err = txn.Commit()
		require.NoError(t, err)

		// Get barrier
		txn2 := badgerStore.View()
		defer txn2.Discard()

		retrievedBarrier, err := barriersTable.Get(txn2, &corepb.BarrierId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			BarrierId:   barrierId,
		})

		require.NoError(t, err)
		require.NotNil(t, retrievedBarrier)
		require.Equal(t, barrierId, retrievedBarrier.Id.BarrierId)
		require.Equal(t, "test_barrier", retrievedBarrier.Name)
		require.Equal(t, "Test barrier", retrievedBarrier.Description)
		require.EqualValues(t, 5, retrievedBarrier.ExpectedProcesses)
		require.EqualValues(t, 2, retrievedBarrier.ArrivedProcesses)
		require.EqualValues(t, 1, retrievedBarrier.Generation)
	})

	t.Run("nonexistent", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		barriersTable := newBarriersTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		txn := badgerStore.View()
		defer txn.Discard()

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()
		barrierId := rand.Uint64()

		// Try to get nonexistent barrier
		_, err = barriersTable.Get(txn, &corepb.BarrierId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			BarrierId:   barrierId,
		})

		require.Error(t, err)
	})
}

func TestBarriersTable_GetByName(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		barriersTable := newBarriersTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()
		barrierId := rand.Uint64()
		barrierName := "named_barrier"

		barrier := &corepb.Barrier{
			Id: &corepb.BarrierId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
				BarrierId:   barrierId,
			},
			Name:              barrierName,
			Description:       "Barrier found by name",
			ExpectedProcesses: 3,
			ArrivedProcesses:  0,
			Generation:        1,
			CreatedAt:         12345,
			UpdatedAt:         12345,
		}

		// Create barrier
		txn := badgerStore.Update()
		err = barriersTable.Create(txn, barrier)
		require.NoError(t, err)
		err = txn.Commit()
		require.NoError(t, err)

		// Get barrier by name
		txn2 := badgerStore.View()
		defer txn2.Discard()

		retrievedBarrier, err := barriersTable.GetByName(txn2, accountId, namespaceId, barrierName)

		require.NoError(t, err)
		require.NotNil(t, retrievedBarrier)
		require.Equal(t, barrierId, retrievedBarrier.Id.BarrierId)
		require.Equal(t, barrierName, retrievedBarrier.Name)
	})

	t.Run("nonexistent", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		barriersTable := newBarriersTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		txn := badgerStore.View()
		defer txn.Discard()

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		// Try to get nonexistent barrier by name
		_, err = barriersTable.GetByName(txn, accountId, namespaceId, "nonexistent")

		require.Error(t, err)
	})
}

func TestBarriersTable_Create(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		barriersTable := newBarriersTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()
		barrierId := rand.Uint64()

		barrier := &corepb.Barrier{
			Id: &corepb.BarrierId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
				BarrierId:   barrierId,
			},
			Name:              "test_barrier",
			Description:       "Test barrier",
			ExpectedProcesses: 5,
			ArrivedProcesses:  0,
			Generation:        1,
			CreatedAt:         12345,
			UpdatedAt:         12345,
		}

		txn := badgerStore.Update()
		err = barriersTable.Create(txn, barrier)
		require.NoError(t, err)
		err = txn.Commit()
		require.NoError(t, err)

		// Verify barrier was created
		txn2 := badgerStore.View()
		defer txn2.Discard()

		retrievedBarrier, err := barriersTable.Get(txn2, barrier.Id)
		require.NoError(t, err)
		require.Equal(t, "test_barrier", retrievedBarrier.Name)
		require.EqualValues(t, 5, retrievedBarrier.ExpectedProcesses)
	})

	t.Run("duplicate name", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		barriersTable := newBarriersTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()
		barrierName := "duplicate_barrier"

		barrier1 := &corepb.Barrier{
			Id: &corepb.BarrierId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
				BarrierId:   rand.Uint64(),
			},
			Name:              barrierName,
			Description:       "First barrier",
			ExpectedProcesses: 3,
			ArrivedProcesses:  0,
			Generation:        1,
			CreatedAt:         12345,
			UpdatedAt:         12345,
		}

		// Create first barrier
		txn := badgerStore.Update()
		err = barriersTable.Create(txn, barrier1)
		require.NoError(t, err)
		err = txn.Commit()
		require.NoError(t, err)

		// Try to create another barrier with the same name
		barrier2 := &corepb.Barrier{
			Id: &corepb.BarrierId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
				BarrierId:   rand.Uint64(),
			},
			Name:              barrierName,
			Description:       "Second barrier",
			ExpectedProcesses: 5,
			ArrivedProcesses:  0,
			Generation:        1,
			CreatedAt:         12345,
			UpdatedAt:         12345,
		}

		txn2 := badgerStore.Update()
		err = barriersTable.Create(txn2, barrier2)
		require.Error(t, err)
		require.Contains(t, err.Error(), "already exists")
		txn2.Discard()
	})

	t.Run("different namespaces", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		barriersTable := newBarriersTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId1 := rand.Uint32()
		namespaceId2 := rand.Uint32()
		barrierName := "same_name"

		// Create barrier in namespace 1
		barrier1 := &corepb.Barrier{
			Id: &corepb.BarrierId{
				AccountId:   accountId,
				NamespaceId: namespaceId1,
				BarrierId:   rand.Uint64(),
			},
			Name:              barrierName,
			Description:       "Namespace 1 barrier",
			ExpectedProcesses: 3,
			ArrivedProcesses:  0,
			Generation:        1,
			CreatedAt:         12345,
			UpdatedAt:         12345,
		}

		txn := badgerStore.Update()
		err = barriersTable.Create(txn, barrier1)
		require.NoError(t, err)
		err = txn.Commit()
		require.NoError(t, err)

		// Create barrier with same name in namespace 2 - should succeed
		barrier2 := &corepb.Barrier{
			Id: &corepb.BarrierId{
				AccountId:   accountId,
				NamespaceId: namespaceId2,
				BarrierId:   rand.Uint64(),
			},
			Name:              barrierName,
			Description:       "Namespace 2 barrier",
			ExpectedProcesses: 5,
			ArrivedProcesses:  0,
			Generation:        1,
			CreatedAt:         12345,
			UpdatedAt:         12345,
		}

		txn2 := badgerStore.Update()
		err = barriersTable.Create(txn2, barrier2)
		require.NoError(t, err)
		err = txn2.Commit()
		require.NoError(t, err)
	})
}

func TestBarriersTable_Update(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		barriersTable := newBarriersTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()
		barrierId := rand.Uint64()

		barrier := &corepb.Barrier{
			Id: &corepb.BarrierId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
				BarrierId:   barrierId,
			},
			Name:              "test_barrier",
			Description:       "Original description",
			ExpectedProcesses: 5,
			ArrivedProcesses:  0,
			Generation:        1,
			CreatedAt:         12345,
			UpdatedAt:         12345,
		}

		// Create barrier
		txn := badgerStore.Update()
		err = barriersTable.Create(txn, barrier)
		require.NoError(t, err)
		err = txn.Commit()
		require.NoError(t, err)

		// Update barrier
		barrier.ArrivedProcesses = 3
		barrier.UpdatedAt = 54321

		txn2 := badgerStore.Update()
		err = barriersTable.Update(txn2, barrier)
		require.NoError(t, err)
		err = txn2.Commit()
		require.NoError(t, err)

		// Verify update
		txn3 := badgerStore.View()
		defer txn3.Discard()

		retrievedBarrier, err := barriersTable.Get(txn3, barrier.Id)
		require.NoError(t, err)
		require.EqualValues(t, 3, retrievedBarrier.ArrivedProcesses)
		require.EqualValues(t, 54321, retrievedBarrier.UpdatedAt)
	})
}

func TestBarriersTable_Delete(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		barriersTable := newBarriersTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()
		barrierId := rand.Uint64()

		barrier := &corepb.Barrier{
			Id: &corepb.BarrierId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
				BarrierId:   barrierId,
			},
			Name:              "test_barrier",
			Description:       "Test barrier",
			ExpectedProcesses: 5,
			ArrivedProcesses:  0,
			Generation:        1,
			CreatedAt:         12345,
			UpdatedAt:         12345,
		}

		// Create barrier
		txn := badgerStore.Update()
		err = barriersTable.Create(txn, barrier)
		require.NoError(t, err)
		err = txn.Commit()
		require.NoError(t, err)

		// Verify barrier exists
		txn2 := badgerStore.View()
		_, err = barriersTable.Get(txn2, barrier.Id)
		txn2.Discard()
		require.NoError(t, err)

		// Delete barrier
		txn3 := badgerStore.Update()
		err = barriersTable.Delete(txn3, barrier.Id)
		require.NoError(t, err)
		err = txn3.Commit()
		require.NoError(t, err)

		// Verify barrier no longer exists
		txn4 := badgerStore.View()
		defer txn4.Discard()

		_, err = barriersTable.Get(txn4, barrier.Id)
		require.Error(t, err)
	})

	t.Run("nonexistent", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		barriersTable := newBarriersTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()
		barrierId := rand.Uint64()

		// Delete nonexistent barrier - should not error
		txn := badgerStore.Update()
		err = barriersTable.Delete(txn, &corepb.BarrierId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			BarrierId:   barrierId,
		})
		require.NoError(t, err)
		err = txn.Commit()
		require.NoError(t, err)
	})
}

func TestBarriersTable_List(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		barriersTable := newBarriersTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		txn := badgerStore.View()
		defer txn.Discard()

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		result, err := barriersTable.List(txn, accountId, namespaceId, nil, 10)

		require.NoError(t, err)
		require.NotNil(t, result)
		require.Empty(t, result.barriers)
	})

	t.Run("multiple barriers", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		barriersTable := newBarriersTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		// Create multiple barriers
		txn := badgerStore.Update()
		for i := 0; i < 5; i++ {
			barrier := &corepb.Barrier{
				Id: &corepb.BarrierId{
					AccountId:   accountId,
					NamespaceId: namespaceId,
					BarrierId:   rand.Uint64(),
				},
				Name:              fmt.Sprintf("barrier_%d", i),
				Description:       fmt.Sprintf("Barrier %d", i),
				ExpectedProcesses: uint64(i + 1),
				ArrivedProcesses:  0,
				Generation:        1,
				CreatedAt:         12345,
				UpdatedAt:         12345,
			}
			err := barriersTable.Create(txn, barrier)
			require.NoError(t, err)
		}
		err = txn.Commit()
		require.NoError(t, err)

		// List barriers
		txn2 := badgerStore.View()
		defer txn2.Discard()

		result, err := barriersTable.List(txn2, accountId, namespaceId, nil, 10)

		require.NoError(t, err)
		require.NotNil(t, result)
		require.Len(t, result.barriers, 5)
	})

	t.Run("namespace isolation", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		barriersTable := newBarriersTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId1 := rand.Uint32()
		namespaceId2 := rand.Uint32()

		// Create barriers in namespace 1
		txn := badgerStore.Update()
		for i := 0; i < 3; i++ {
			barrier := &corepb.Barrier{
				Id: &corepb.BarrierId{
					AccountId:   accountId,
					NamespaceId: namespaceId1,
					BarrierId:   rand.Uint64(),
				},
				Name:              fmt.Sprintf("ns1_barrier_%d", i),
				Description:       fmt.Sprintf("NS1 Barrier %d", i),
				ExpectedProcesses: 3,
				ArrivedProcesses:  0,
				Generation:        1,
				CreatedAt:         12345,
				UpdatedAt:         12345,
			}
			err := barriersTable.Create(txn, barrier)
			require.NoError(t, err)
		}

		// Create barriers in namespace 2
		for i := 0; i < 2; i++ {
			barrier := &corepb.Barrier{
				Id: &corepb.BarrierId{
					AccountId:   accountId,
					NamespaceId: namespaceId2,
					BarrierId:   rand.Uint64(),
				},
				Name:              fmt.Sprintf("ns2_barrier_%d", i),
				Description:       fmt.Sprintf("NS2 Barrier %d", i),
				ExpectedProcesses: 5,
				ArrivedProcesses:  0,
				Generation:        1,
				CreatedAt:         12345,
				UpdatedAt:         12345,
			}
			err := barriersTable.Create(txn, barrier)
			require.NoError(t, err)
		}
		err = txn.Commit()
		require.NoError(t, err)

		// List barriers in namespace 1
		txn2 := badgerStore.View()
		defer txn2.Discard()

		result1, err := barriersTable.List(txn2, accountId, namespaceId1, nil, 10)
		require.NoError(t, err)
		require.Len(t, result1.barriers, 3)

		// List barriers in namespace 2
		result2, err := barriersTable.List(txn2, accountId, namespaceId2, nil, 10)
		require.NoError(t, err)
		require.Len(t, result2.barriers, 2)
	})

	t.Run("pagination", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		barriersTable := newBarriersTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		// Create 10 barriers
		txn := badgerStore.Update()
		for i := 0; i < 10; i++ {
			barrier := &corepb.Barrier{
				Id: &corepb.BarrierId{
					AccountId:   accountId,
					NamespaceId: namespaceId,
					BarrierId:   uint64(i), // Use sequential IDs for predictable ordering
				},
				Name:              fmt.Sprintf("barrier_%02d", i),
				Description:       fmt.Sprintf("Barrier %d", i),
				ExpectedProcesses: 3,
				ArrivedProcesses:  0,
				Generation:        1,
				CreatedAt:         12345,
				UpdatedAt:         12345,
			}
			err := barriersTable.Create(txn, barrier)
			require.NoError(t, err)
		}
		err = txn.Commit()
		require.NoError(t, err)

		// List first page
		txn2 := badgerStore.View()
		defer txn2.Discard()

		result1, err := barriersTable.List(txn2, accountId, namespaceId, nil, 3)
		require.NoError(t, err)
		require.Len(t, result1.barriers, 3)
		require.NotNil(t, result1.nextPaginationToken)

		// List second page
		result2, err := barriersTable.List(txn2, accountId, namespaceId, result1.nextPaginationToken, 3)
		require.NoError(t, err)
		require.Len(t, result2.barriers, 3)
		require.NotNil(t, result2.nextPaginationToken)

		// Verify different barriers returned
		require.NotEqual(t, result1.barriers[0].Id.BarrierId, result2.barriers[0].Id.BarrierId)
	})
}
