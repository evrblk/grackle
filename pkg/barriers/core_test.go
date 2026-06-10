package barriers

import (
	"bytes"
	"fmt"
	"io"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/evrblk/monstera/store"
	monsterax "github.com/evrblk/monstera/x"
	"github.com/stretchr/testify/require"

	"github.com/evrblk/grackle/pkg/coreapis"
	"github.com/evrblk/grackle/pkg/corepb"
	"github.com/evrblk/grackle/pkg/tables"
)

func init() {
	registry := monsterax.NewBaseTableRegistry(1)
	tables.RegisterGracklePrefixes(registry)
}

func TestCore_CreateBarrier(t *testing.T) {
	t.Run("create barrier", func(t *testing.T) {
		core := newBarriersCore(t)
		now := time.Now()
		barrierId := &corepb.BarrierId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
			BarrierId:   rand.Uint64(),
		}

		// T+0: Create barrier
		barrier := createBarrier(t, core, barrierId, "test_barrier", 3, now)

		require.Equal(t, barrierId.BarrierId, barrier.Id.BarrierId)
		require.Equal(t, "test_barrier", barrier.Name)
		require.Equal(t, "Test barrier description", barrier.Description)
		require.EqualValues(t, 3, barrier.ExpectedProcesses)
		require.EqualValues(t, 0, barrier.ArrivedProcesses)
		require.EqualValues(t, 1, barrier.Generation)
		require.EqualValues(t, now.UnixNano(), barrier.CreatedAt)
		require.EqualValues(t, now.UnixNano(), barrier.UpdatedAt)
	})

	t.Run("duplicate name", func(t *testing.T) {
		core := newBarriersCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespaceId := rand.Uint32()
		barrierName := "duplicate_barrier"
		barrierId1 := &corepb.BarrierId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			BarrierId:   rand.Uint64(),
		}

		// T+0: Create first barrier
		_ = createBarrier(t, core, barrierId1, barrierName, 3, now)

		// T+1m: Try to create another barrier with the same name - should fail
		barrierId2 := &corepb.BarrierId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			BarrierId:   rand.Uint64(),
		}

		resp1, err := core.CreateBarrier(&coreapis.CreateBarrierRequest{
			Payload: &corepb.CreateBarrierRequest{
				BarrierId:                       barrierId2,
				Name:                            barrierName,
				Description:                     "Second barrier",
				ExpectedProcesses:               5,
				MaxNumberOfBarriersPerNamespace: 10,
				Now:                             now.Add(time.Minute).UnixNano(),
			},
		})

		require.NoError(t, err)
		require.NotNil(t, resp1)
		require.Nil(t, resp1.Payload)
		require.NotNil(t, resp1.ApplicationError)
		require.Equal(t, monsterax.AlreadyExists, resp1.ApplicationError.Code)
		require.Contains(t, resp1.ApplicationError.Message, "already exists")

		// Verify the first barrier is still accessible and unchanged
		barrier := getBarrier(t, core, barrierId1)

		require.Equal(t, barrierId1.BarrierId, barrier.Id.BarrierId)
	})

	t.Run("max number of barriers per namespace", func(t *testing.T) {
		core := newBarriersCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespaceId := rand.Uint32()
		maxBarriers := int64(3)

		// Create barriers up to the limit
		for i := 0; i < int(maxBarriers); i++ {
			barrierId := &corepb.BarrierId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
				BarrierId:   rand.Uint64(),
			}

			resp1, err := core.CreateBarrier(&coreapis.CreateBarrierRequest{
				Payload: &corepb.CreateBarrierRequest{
					BarrierId:                       barrierId,
					Name:                            fmt.Sprintf("barrier_%d", i),
					Description:                     fmt.Sprintf("Barrier %d", i),
					ExpectedProcesses:               3,
					MaxNumberOfBarriersPerNamespace: maxBarriers,
					Now:                             now.UnixNano(),
				},
			})

			require.NoError(t, err)
			require.NotNil(t, resp1)
			require.NotNil(t, resp1.Payload)
			require.NotNil(t, resp1.Payload.Barrier)
		}

		// Try to create one more barrier - should fail
		barrierId := &corepb.BarrierId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			BarrierId:   rand.Uint64(),
		}

		resp2, err := core.CreateBarrier(&coreapis.CreateBarrierRequest{
			Payload: &corepb.CreateBarrierRequest{
				BarrierId:                       barrierId,
				Name:                            "barrier_exceeding_limit",
				Description:                     "Exceeding limit",
				ExpectedProcesses:               3,
				MaxNumberOfBarriersPerNamespace: maxBarriers,
				Now:                             now.Add(time.Minute).UnixNano(),
			},
		})

		require.NoError(t, err)
		require.NotNil(t, resp2)
		require.Nil(t, resp2.Payload)
		require.NotNil(t, resp2.ApplicationError)
		require.Equal(t, monsterax.ResourceExhausted, resp2.ApplicationError.Code)
		require.Contains(t, resp2.ApplicationError.Message, "max number of barriers per namespace reached")
	})
}

func TestCore_GetBarrier(t *testing.T) {
	t.Run("get existing barrier", func(t *testing.T) {
		core := newBarriersCore(t)
		now := time.Now()
		barrierId := &corepb.BarrierId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
			BarrierId:   rand.Uint64(),
		}

		// T+0: Create barrier
		_ = createBarrier(t, core, barrierId, "test_barrier", 3, now)

		// T+1m: Get barrier
		barrier := getBarrier(t, core, barrierId)

		require.Equal(t, barrierId.BarrierId, barrier.Id.BarrierId)
		require.Equal(t, "test_barrier", barrier.Name)
		require.Equal(t, "Test barrier description", barrier.Description)
		require.EqualValues(t, 3, barrier.ExpectedProcesses)
		require.EqualValues(t, 0, barrier.ArrivedProcesses)
	})

	t.Run("get nonexistent barrier", func(t *testing.T) {
		core := newBarriersCore(t)
		barrierId := &corepb.BarrierId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
			BarrierId:   rand.Uint64(),
		}

		// Get nonexistent barrier
		resp1, err := core.GetBarrier(&coreapis.GetBarrierRequest{
			Payload: &corepb.GetBarrierRequest{
				BarrierId: barrierId,
			},
		})

		require.NoError(t, err)
		require.NotNil(t, resp1)
		require.Nil(t, resp1.Payload)
		require.NotNil(t, resp1.ApplicationError)
		require.Equal(t, monsterax.NotFound, resp1.ApplicationError.Code)
		require.Contains(t, resp1.ApplicationError.Message, "barrier not found")
	})
}

func TestCore_GetBarrierByName(t *testing.T) {
	t.Run("get existing barrier", func(t *testing.T) {
		core := newBarriersCore(t)
		now := time.Now()
		namespaceId := &corepb.NamespaceId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
		}
		barrierId := &corepb.BarrierId{
			AccountId:   namespaceId.AccountId,
			NamespaceId: namespaceId.NamespaceId,
			BarrierId:   rand.Uint64(),
		}

		// T+0: Create barrier
		_ = createBarrier(t, core, barrierId, "named_barrier", 3, now)

		// T+1m: Get barrier by name
		resp1, err := core.GetBarrierByName(&coreapis.GetBarrierByNameRequest{
			Payload: &corepb.GetBarrierByNameRequest{
				NamespaceId: namespaceId,
				BarrierName: "named_barrier",
			},
		})

		require.NoError(t, err)
		require.NotNil(t, resp1.Payload.Barrier)
		require.Equal(t, barrierId.BarrierId, resp1.Payload.Barrier.Id.BarrierId)
		require.Equal(t, "named_barrier", resp1.Payload.Barrier.Name)
	})

	t.Run("get nonexistent barrier", func(t *testing.T) {
		core := newBarriersCore(t)
		namespaceId := &corepb.NamespaceId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
		}

		// Get nonexistent barrier by name
		resp1, err := core.GetBarrierByName(&coreapis.GetBarrierByNameRequest{
			Payload: &corepb.GetBarrierByNameRequest{
				NamespaceId: namespaceId,
				BarrierName: "nonexistent_barrier",
			},
		})

		require.NoError(t, err)
		require.NotNil(t, resp1)
		require.Nil(t, resp1.Payload)
		require.NotNil(t, resp1.ApplicationError)
		require.Equal(t, monsterax.NotFound, resp1.ApplicationError.Code)
		require.Contains(t, resp1.ApplicationError.Message, "barrier not found")
	})
}

func TestCore_ListBarriers(t *testing.T) {
	t.Run("empty namespace", func(t *testing.T) {
		core := newBarriersCore(t)
		namespaceId := &corepb.NamespaceId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
		}

		// List barriers in empty namespace
		response, err := core.ListBarriers(&coreapis.ListBarriersRequest{
			Payload: &corepb.ListBarriersRequest{
				NamespaceId: namespaceId,
			},
		})

		require.NoError(t, err)
		require.NotNil(t, response)
		require.Empty(t, response.Payload.Barriers)
	})

	t.Run("multiple barriers", func(t *testing.T) {
		core := newBarriersCore(t)
		now := time.Now()
		namespaceId := &corepb.NamespaceId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
		}

		// Create multiple barriers
		for i := range 5 {
			barrierId := &corepb.BarrierId{
				AccountId:   namespaceId.AccountId,
				NamespaceId: namespaceId.NamespaceId,
				BarrierId:   rand.Uint64(),
			}

			_ = createBarrier(t, core, barrierId, fmt.Sprintf("barrier_%d", i), uint64(i+1), now)
		}

		// List barriers
		listResponse, err := core.ListBarriers(&coreapis.ListBarriersRequest{
			Payload: &corepb.ListBarriersRequest{
				NamespaceId: namespaceId,
			},
		})

		require.NoError(t, err)
		require.NotNil(t, listResponse)
		require.Len(t, listResponse.Payload.Barriers, 5)
	})

	t.Run("multiple namespaces", func(t *testing.T) {
		core := newBarriersCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespaceId1 := &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: rand.Uint32(),
		}
		namespaceId2 := &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: rand.Uint32(),
		}

		// Create barriers in namespace 1
		for i := range 3 {
			barrierId := &corepb.BarrierId{
				AccountId:   namespaceId1.AccountId,
				NamespaceId: namespaceId1.NamespaceId,
				BarrierId:   rand.Uint64(),
			}

			_ = createBarrier(t, core, barrierId, fmt.Sprintf("barrier_ns1_%d", i), 3, now)
		}

		// Create barriers in namespace 2
		for i := range 2 {
			barrierId := &corepb.BarrierId{
				AccountId:   namespaceId2.AccountId,
				NamespaceId: namespaceId2.NamespaceId,
				BarrierId:   rand.Uint64(),
			}

			_ = createBarrier(t, core, barrierId, fmt.Sprintf("barrier_ns2_%d", i), 5, now)
		}

		// List barriers in namespace 1
		listResponse1, err := core.ListBarriers(&coreapis.ListBarriersRequest{
			Payload: &corepb.ListBarriersRequest{
				NamespaceId: namespaceId1,
			},
		})

		require.NoError(t, err)
		require.NotNil(t, listResponse1)
		require.Len(t, listResponse1.Payload.Barriers, 3)

		// List barriers in namespace 2
		listResponse2, err := core.ListBarriers(&coreapis.ListBarriersRequest{
			Payload: &corepb.ListBarriersRequest{
				NamespaceId: namespaceId2,
			},
		})

		require.NoError(t, err)
		require.NotNil(t, listResponse2)
		require.Len(t, listResponse2.Payload.Barriers, 2)
	})
}

func TestCore_DeleteBarrier(t *testing.T) {
	t.Run("delete existing barrier", func(t *testing.T) {
		core := newBarriersCore(t)
		now := time.Now()
		namespaceId := &corepb.NamespaceId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
		}

		barrierId := &corepb.BarrierId{
			AccountId:   namespaceId.AccountId,
			NamespaceId: namespaceId.NamespaceId,
			BarrierId:   rand.Uint64(),
		}

		// T+0: Create barrier
		_ = createBarrier(t, core, barrierId, "test_barrier", 3, now)

		// T+1m: Delete barrier
		resp1, err := core.DeleteBarrier(&coreapis.DeleteBarrierRequest{
			Payload: &corepb.DeleteBarrierRequest{
				NamespaceId: namespaceId,
				BarrierName: "test_barrier",
			},
		})

		require.NoError(t, err)
		require.NotNil(t, resp1)
		require.Nil(t, resp1.ApplicationError)
		require.NotNil(t, resp1.Payload)

		// T+2m: Try to get deleted barrier
		resp2, err := core.GetBarrierByName(&coreapis.GetBarrierByNameRequest{
			Payload: &corepb.GetBarrierByNameRequest{
				NamespaceId: namespaceId,
				BarrierName: "test_barrier",
			},
		})

		require.NoError(t, err)
		require.NotNil(t, resp2)
		require.Nil(t, resp2.Payload)
		require.NotNil(t, resp2.ApplicationError)
		require.Contains(t, resp2.ApplicationError.Message, "barrier not found")
	})

	t.Run("delete nonexistent barrier", func(t *testing.T) {
		core := newBarriersCore(t)
		namespaceId := &corepb.NamespaceId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
		}

		// Delete nonexistent barrier - should not error
		deleteResponse, err := core.DeleteBarrier(&coreapis.DeleteBarrierRequest{
			Payload: &corepb.DeleteBarrierRequest{
				NamespaceId: namespaceId,
				BarrierName: "nonexistent_barrier",
			},
		})

		require.NoError(t, err)
		require.NotNil(t, deleteResponse)
		require.Nil(t, deleteResponse.ApplicationError)
	})

	t.Run("updates counter", func(t *testing.T) {
		core := newBarriersCore(t)
		now := time.Now()
		namespaceId := &corepb.NamespaceId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
		}

		// Create two barriers
		for i := range 2 {
			barrierId := &corepb.BarrierId{
				AccountId:   namespaceId.AccountId,
				NamespaceId: namespaceId.NamespaceId,
				BarrierId:   rand.Uint64(),
			}

			_ = createBarrier(t, core, barrierId, fmt.Sprintf("barrier_%d", i), 3, now)
		}

		// List barriers - should have 2
		listResponse1, err := core.ListBarriers(&coreapis.ListBarriersRequest{
			Payload: &corepb.ListBarriersRequest{
				NamespaceId: namespaceId,
			},
		})

		require.NoError(t, err)
		require.Len(t, listResponse1.Payload.Barriers, 2)

		// Delete one barrier
		deleteResponse, err := core.DeleteBarrier(&coreapis.DeleteBarrierRequest{
			Payload: &corepb.DeleteBarrierRequest{
				NamespaceId: namespaceId,
				BarrierName: "barrier_0",
			},
		})

		require.NoError(t, err)
		require.NotNil(t, deleteResponse)

		// Create another barrier - should succeed because we now have room
		barrierId := &corepb.BarrierId{
			AccountId:   namespaceId.AccountId,
			NamespaceId: namespaceId.NamespaceId,
			BarrierId:   rand.Uint64(),
		}

		_ = createBarrier(t, core, barrierId, "barrier_new", 3, now.Add(time.Minute))
	})
}

func TestCore_UpdateBarrier(t *testing.T) {
	t.Run("update barrier successfully", func(t *testing.T) {
		core := newBarriersCore(t)
		now := time.Now()
		barrierId := &corepb.BarrierId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
			BarrierId:   rand.Uint64(),
		}

		// T+0: Create barrier
		barrier := createBarrier(t, core, barrierId, "test_barrier", 3, now)

		require.Equal(t, "Test barrier description", barrier.Description)
		require.EqualValues(t, 3, barrier.ExpectedProcesses)
		require.Equal(t, now.UnixNano(), barrier.UpdatedAt)

		// T+1m: Update barrier
		updateTime := now.Add(time.Minute)
		updateResponse, err := core.UpdateBarrier(&coreapis.UpdateBarrierRequest{
			Payload: &corepb.UpdateBarrierRequest{
				BarrierId:         barrierId,
				Description:       "Updated description",
				ExpectedProcesses: 5,
				Now:               updateTime.UnixNano(),
			},
		})

		require.NoError(t, err)
		require.NotNil(t, updateResponse.Payload.Barrier)
		require.Equal(t, "Updated description", updateResponse.Payload.Barrier.Description)
		require.EqualValues(t, 5, updateResponse.Payload.Barrier.ExpectedProcesses)
		require.Equal(t, updateTime.UnixNano(), updateResponse.Payload.Barrier.UpdatedAt)
		require.Equal(t, now.UnixNano(), updateResponse.Payload.Barrier.CreatedAt) // CreatedAt should not change

		// T+2m: Get barrier to verify update persisted
		barrier = getBarrier(t, core, barrierId)

		require.Equal(t, "Updated description", barrier.Description)
		require.EqualValues(t, 5, barrier.ExpectedProcesses)
		require.Equal(t, updateTime.UnixNano(), barrier.UpdatedAt)
	})

	t.Run("update nonexistent barrier", func(t *testing.T) {
		core := newBarriersCore(t)
		now := time.Now()
		barrierId := &corepb.BarrierId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
			BarrierId:   rand.Uint64(),
		}

		// Try to update a barrier that doesn't exist
		resp1, err := core.UpdateBarrier(&coreapis.UpdateBarrierRequest{
			Payload: &corepb.UpdateBarrierRequest{
				BarrierId:         barrierId,
				Description:       "Updated description",
				ExpectedProcesses: 5,
				Now:               now.UnixNano(),
			},
		})

		require.NoError(t, err)
		require.NotNil(t, resp1)
		require.Nil(t, resp1.Payload)
		require.NotNil(t, resp1.ApplicationError)
		require.Equal(t, monsterax.NotFound, resp1.ApplicationError.Code)
		require.Contains(t, resp1.ApplicationError.Message, "barrier not found")
	})

	t.Run("cannot reduce expected_processes below arrived_processes", func(t *testing.T) {
		core := newBarriersCore(t)
		now := time.Now()
		namespaceId := &corepb.NamespaceId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
		}
		barrierId := &corepb.BarrierId{
			AccountId:   namespaceId.AccountId,
			NamespaceId: namespaceId.NamespaceId,
			BarrierId:   rand.Uint64(),
		}

		// T+0: Create barrier with expected_processes = 5
		_ = createBarrier(t, core, barrierId, "test_barrier", 5, now)

		// T+1m: Have 3 processes arrive at the barrier
		for i := range 3 {
			_ = arriveAtBarrier(t, core, namespaceId, "test_barrier", fmt.Sprintf("process-%d", i), 1, now.Add(time.Minute))
		}

		// T+2m: Try to update expected_processes to 2 (less than 3 arrived processes)
		resp1, err := core.UpdateBarrier(&coreapis.UpdateBarrierRequest{
			Payload: &corepb.UpdateBarrierRequest{
				BarrierId:         barrierId,
				Description:       "Updated description",
				ExpectedProcesses: 2,
				Now:               now.Add(2 * time.Minute).UnixNano(),
			},
		})

		require.NoError(t, err)
		require.NotNil(t, resp1)
		require.NotNil(t, resp1.ApplicationError)
		require.Contains(t, resp1.ApplicationError.Message, "there are currently more arrived processes than the new expected processes")

		// T+3m: Update to expected_processes = 3 (equal to arrived_processes) should succeed
		updateResponse, err := core.UpdateBarrier(&coreapis.UpdateBarrierRequest{
			Payload: &corepb.UpdateBarrierRequest{
				BarrierId:         barrierId,
				Description:       "Updated description",
				ExpectedProcesses: 3,
				Now:               now.Add(3 * time.Minute).UnixNano(),
			},
		})

		require.NoError(t, err)
		require.NotNil(t, updateResponse.Payload.Barrier)
		require.EqualValues(t, 3, updateResponse.Payload.Barrier.ExpectedProcesses)

		// T+4m: Update to expected_processes = 10 (greater than arrived_processes) should succeed
		updateResponse2, err := core.UpdateBarrier(&coreapis.UpdateBarrierRequest{
			Payload: &corepb.UpdateBarrierRequest{
				BarrierId:         barrierId,
				Description:       "Updated description again",
				ExpectedProcesses: 10,
				Now:               now.Add(4 * time.Minute).UnixNano(),
			},
		})

		require.NoError(t, err)
		require.NotNil(t, updateResponse2.Payload.Barrier)
		require.EqualValues(t, 10, updateResponse2.Payload.Barrier.ExpectedProcesses)
	})
}

func TestCore_ArriveAtBarrier(t *testing.T) {
	t.Run("multiple processes", func(t *testing.T) {
		core := newBarriersCore(t)
		now := time.Now()
		namespaceId := &corepb.NamespaceId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
		}
		barrierId := &corepb.BarrierId{
			AccountId:   namespaceId.AccountId,
			NamespaceId: namespaceId.NamespaceId,
			BarrierId:   rand.Uint64(),
		}

		// T+0: Create barrier
		_ = createBarrier(t, core, barrierId, "test_barrier", 3, now)

		// T+1m: First process arrives
		_ = arriveAtBarrier(t, core, namespaceId, "test_barrier", "process_1", 1, now.Add(time.Minute))

		// Get barrier and verify arrived processes count
		barrier := getBarrier(t, core, barrierId)

		require.EqualValues(t, 1, barrier.ArrivedProcesses)
		require.EqualValues(t, 3, barrier.ExpectedProcesses)

		// T+2m: Second process arrives
		_ = arriveAtBarrier(t, core, namespaceId, "test_barrier", "process_2", 1, now.Add(2*time.Minute))

		// Get barrier and verify arrived processes count
		barrier = getBarrier(t, core, barrierId)

		require.EqualValues(t, 2, barrier.ArrivedProcesses)

		// T+3m: Third process arrives
		_ = arriveAtBarrier(t, core, namespaceId, "test_barrier", "process_3", 1, now.Add(3*time.Minute))

		// Get barrier and verify all processes have arrived
		barrier = getBarrier(t, core, barrierId)

		require.EqualValues(t, 3, barrier.ArrivedProcesses)
		require.EqualValues(t, 3, barrier.ExpectedProcesses)
	})

	t.Run("arrive twice", func(t *testing.T) {
		core := newBarriersCore(t)
		now := time.Now()
		namespaceId := &corepb.NamespaceId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
		}
		barrierId := &corepb.BarrierId{
			AccountId:   namespaceId.AccountId,
			NamespaceId: namespaceId.NamespaceId,
			BarrierId:   rand.Uint64(),
		}

		// T+0: Create barrier
		_ = createBarrier(t, core, barrierId, "test_barrier", 3, now)

		// T+1m: Process arrives
		_ = arriveAtBarrier(t, core, namespaceId, "test_barrier", "process_1", 1, now.Add(time.Minute))

		// T+2m: Same process arrives again - should not increment count
		_ = arriveAtBarrier(t, core, namespaceId, "test_barrier", "process_1", 1, now.Add(2*time.Minute))

		// Get barrier and verify count is still 1
		barrier := getBarrier(t, core, barrierId)
		require.EqualValues(t, 1, barrier.ArrivedProcesses)
	})

	t.Run("nonexistent barrier", func(t *testing.T) {
		core := newBarriersCore(t)
		now := time.Now()
		namespaceId := &corepb.NamespaceId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
		}

		// Try to arrive at nonexistent barrier
		resp1, err := core.ArriveAtBarrier(&coreapis.ArriveAtBarrierRequest{
			Payload: &corepb.ArriveAtBarrierRequest{
				NamespaceId: namespaceId,
				BarrierName: "nonexistent_barrier",
				ProcessId:   "process_1",
				Generation:  1,
				Now:         now.UnixNano(),
			},
		})

		require.NoError(t, err)
		require.NotNil(t, resp1)
		require.Nil(t, resp1.Payload)
		require.NotNil(t, resp1.ApplicationError)
		require.Equal(t, monsterax.NotFound, resp1.ApplicationError.Code)
		require.Contains(t, resp1.ApplicationError.Message, "barrier not found")
	})
}

func TestCore_ListBarrierParticipants(t *testing.T) {
	t.Run("multiple participants", func(t *testing.T) {
		core := newBarriersCore(t)
		now := time.Now()
		namespaceId := &corepb.NamespaceId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
		}
		barrierId := &corepb.BarrierId{
			AccountId:   namespaceId.AccountId,
			NamespaceId: namespaceId.NamespaceId,
			BarrierId:   rand.Uint64(),
		}

		// T+0: Create barrier
		_ = createBarrier(t, core, barrierId, "test_barrier", 3, now)

		// Have multiple processes arrive
		for i := range 3 {
			_ = arriveAtBarrier(t, core, namespaceId, "test_barrier", fmt.Sprintf("process_%d", i), 1, now.Add(time.Duration(i+1)*time.Minute))
		}

		// List participants
		resp1, err := core.ListBarrierParticipants(&coreapis.ListBarrierParticipantsRequest{
			Payload: &corepb.ListBarrierParticipantsRequest{
				NamespaceId: namespaceId,
				BarrierName: "test_barrier",
			},
		})

		require.NoError(t, err)
		require.NotNil(t, resp1)
		require.Nil(t, resp1.ApplicationError)
		require.NotNil(t, resp1.Payload)
		require.Len(t, resp1.Payload.Participants, 3)

		// Verify all participants are present
		participantIds := make([]string, len(resp1.Payload.Participants))
		for i, participant := range resp1.Payload.Participants {
			participantIds[i] = participant.ProcessId
			require.EqualValues(t, 1, participant.Generation)
		}

		require.Contains(t, participantIds, "process_0")
		require.Contains(t, participantIds, "process_1")
		require.Contains(t, participantIds, "process_2")
	})

	t.Run("empty", func(t *testing.T) {
		core := newBarriersCore(t)
		now := time.Now()
		namespaceId := &corepb.NamespaceId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
		}
		barrierId := &corepb.BarrierId{
			AccountId:   namespaceId.AccountId,
			NamespaceId: namespaceId.NamespaceId,
			BarrierId:   rand.Uint64(),
		}

		// T+0: Create barrier
		_ = createBarrier(t, core, barrierId, "test_barrier", 3, now)

		// List participants before any have arrived
		resp1, err := core.ListBarrierParticipants(&coreapis.ListBarrierParticipantsRequest{
			Payload: &corepb.ListBarrierParticipantsRequest{
				NamespaceId: namespaceId,
				BarrierName: "test_barrier",
			},
		})

		require.NoError(t, err)
		require.NotNil(t, resp1)
		require.Nil(t, resp1.ApplicationError)
		require.NotNil(t, resp1.Payload)
		require.Empty(t, resp1.Payload.Participants)
	})

	t.Run("nonexistent barrier", func(t *testing.T) {
		core := newBarriersCore(t)
		namespaceId := &corepb.NamespaceId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
		}

		// Try to list participants of nonexistent barrier
		resp1, err := core.ListBarrierParticipants(&coreapis.ListBarrierParticipantsRequest{
			Payload: &corepb.ListBarrierParticipantsRequest{
				NamespaceId: namespaceId,
				BarrierName: "nonexistent_barrier",
			},
		})

		require.NoError(t, err)
		require.NotNil(t, resp1)
		require.Nil(t, resp1.Payload)
		require.NotNil(t, resp1.ApplicationError)
		require.Contains(t, resp1.ApplicationError.Message, "barrier not found")
		require.Equal(t, monsterax.NotFound, resp1.ApplicationError.Code)
	})
}

func TestCore_SnapshotAndRestore(t *testing.T) {
	now := time.Now()
	namespaceId := &corepb.NamespaceId{
		AccountId:   rand.Uint64(),
		NamespaceId: rand.Uint32(),
	}
	barrierId := &corepb.BarrierId{
		AccountId:   namespaceId.AccountId,
		NamespaceId: namespaceId.NamespaceId,
		BarrierId:   rand.Uint64(),
	}

	// Create two barrier cores for testing snapshot and restore
	core1 := newBarriersCore(t)
	core2 := newBarriersCore(t)

	// T+0: Create barrier
	_ = createBarrier(t, core1, barrierId, "test_barrier", 3, now)

	// T+1m: First process arrives
	_ = arriveAtBarrier(t, core1, namespaceId, "test_barrier", "process_1", 1, now.Add(time.Minute))

	// Take snapshot at this point
	snapshot := core1.Snapshot()

	// T+2m: Second process arrives (after snapshot)
	_ = arriveAtBarrier(t, core1, namespaceId, "test_barrier", "process_2", 1, now.Add(2*time.Minute))

	// Write snapshot to buffer
	buf := bytes.NewBuffer(nil)
	err := snapshot.Write(buf)
	require.NoError(t, err)

	// Restore snapshot to second core
	err = core2.Restore(io.NopCloser(buf))
	require.NoError(t, err)

	// Verify the restored state matches the snapshot state
	barrier := getBarrier(t, core2, barrierId)

	require.Equal(t, "test_barrier", barrier.Name)
	require.EqualValues(t, 1, barrier.ArrivedProcesses) // Only process_1 arrived before snapshot

	// List participants in restored state
	listResponse, err := core2.ListBarrierParticipants(&coreapis.ListBarrierParticipantsRequest{
		Payload: &corepb.ListBarrierParticipantsRequest{
			NamespaceId: namespaceId,
			BarrierName: "test_barrier",
		},
	})

	require.NoError(t, err)
	require.NotNil(t, listResponse)
	require.Len(t, listResponse.Payload.Participants, 1)
	require.Equal(t, "process_1", listResponse.Payload.Participants[0].ProcessId)

	// Verify that the original core has different state (it should have 2 participants)
	listResponse2, err := core1.ListBarrierParticipants(&coreapis.ListBarrierParticipantsRequest{
		Payload: &corepb.ListBarrierParticipantsRequest{
			NamespaceId: namespaceId,
			BarrierName: "test_barrier",
		},
	})

	require.NoError(t, err)
	require.NotNil(t, listResponse2)
	require.Len(t, listResponse2.Payload.Participants, 2)
}

func newBarriersCore(t *testing.T) *Core {
	t.Helper()

	badgerStore, err := store.NewBadgerInMemoryStore()
	require.NoError(t, err)
	return NewCore(badgerStore, []byte{0x1d, 0x36, 0x00, 0x00}, []byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})
}

func createBarrier(t *testing.T, core *Core, barrierId *corepb.BarrierId, name string, expectedProcesses uint64, now time.Time) *corepb.Barrier {
	t.Helper()

	resp, err := core.CreateBarrier(&coreapis.CreateBarrierRequest{
		Payload: &corepb.CreateBarrierRequest{
			BarrierId:                       barrierId,
			Name:                            name,
			Description:                     "Test barrier description",
			ExpectedProcesses:               expectedProcesses,
			MaxNumberOfBarriersPerNamespace: 10,
			Now:                             now.UnixNano(),
		},
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.NotNil(t, resp.Payload)
	require.NotNil(t, resp.Payload.Barrier)
	require.Equal(t, barrierId.BarrierId, resp.Payload.Barrier.Id.BarrierId)

	return resp.Payload.Barrier
}

func arriveAtBarrier(t *testing.T, core *Core, namespaceId *corepb.NamespaceId, barrierName string, processId string, generation uint64, now time.Time) *corepb.Barrier {
	t.Helper()

	resp, err := core.ArriveAtBarrier(&coreapis.ArriveAtBarrierRequest{
		Payload: &corepb.ArriveAtBarrierRequest{
			NamespaceId: namespaceId,
			BarrierName: barrierName,
			ProcessId:   processId,
			Generation:  generation,
			Now:         now.UnixNano(),
		},
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Nil(t, resp.ApplicationError)
	require.NotNil(t, resp.Payload)
	require.NotNil(t, resp.Payload.Barrier)

	return resp.Payload.Barrier
}

func getBarrier(t *testing.T, core *Core, barrierId *corepb.BarrierId) *corepb.Barrier {
	t.Helper()

	resp, err := core.GetBarrier(&coreapis.GetBarrierRequest{
		Payload: &corepb.GetBarrierRequest{
			BarrierId: barrierId,
		},
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Nil(t, resp.ApplicationError)
	require.NotNil(t, resp.Payload)
	require.NotNil(t, resp.Payload.Barrier)

	return resp.Payload.Barrier
}
