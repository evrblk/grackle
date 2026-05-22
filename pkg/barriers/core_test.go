package barriers

import (
	"bytes"
	"fmt"
	"io"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/evrblk/grackle/pkg/corepb"
	"github.com/evrblk/monstera/store"
)

func TestCore_CreateBarrier(t *testing.T) {
	t.Run("create barrier", func(t *testing.T) {
		barriersCore := newBarriersCore(t)

		now := time.Now()

		barrierId := &corepb.BarrierId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
			BarrierId:   rand.Uint64(),
		}

		// T+0: Create barrier
		response1, err := barriersCore.CreateBarrier(&corepb.CreateBarrierRequest{
			BarrierId:                       barrierId,
			Name:                            "test_barrier",
			Description:                     "Test barrier description",
			ExpectedProcesses:               3,
			MaxNumberOfBarriersPerNamespace: 10,
			Now:                             now.UnixNano(),
		})

		require.NoError(t, err)
		require.NotNil(t, response1.Barrier)
		require.Equal(t, barrierId.BarrierId, response1.Barrier.Id.BarrierId)
		require.Equal(t, "test_barrier", response1.Barrier.Name)
		require.Equal(t, "Test barrier description", response1.Barrier.Description)
		require.EqualValues(t, 3, response1.Barrier.ExpectedProcesses)
		require.EqualValues(t, 0, response1.Barrier.ArrivedProcesses)
		require.EqualValues(t, 1, response1.Barrier.Generation)
		require.EqualValues(t, now.UnixNano(), response1.Barrier.CreatedAt)
		require.EqualValues(t, now.UnixNano(), response1.Barrier.UpdatedAt)
	})

	t.Run("duplicate name", func(t *testing.T) {
		barriersCore := newBarriersCore(t)

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
		response1, err := barriersCore.CreateBarrier(&corepb.CreateBarrierRequest{
			BarrierId:                       barrierId1,
			Name:                            barrierName,
			Description:                     "First barrier",
			ExpectedProcesses:               3,
			MaxNumberOfBarriersPerNamespace: 10,
			Now:                             now.UnixNano(),
		})

		require.NoError(t, err)
		require.NotNil(t, response1.Barrier)

		// T+1m: Try to create another barrier with the same name - should fail
		barrierId2 := &corepb.BarrierId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			BarrierId:   rand.Uint64(),
		}

		_, err = barriersCore.CreateBarrier(&corepb.CreateBarrierRequest{
			BarrierId:                       barrierId2,
			Name:                            barrierName,
			Description:                     "Second barrier",
			ExpectedProcesses:               5,
			MaxNumberOfBarriersPerNamespace: 10,
			Now:                             now.Add(time.Minute).UnixNano(),
		})

		require.Error(t, err)
		require.Contains(t, err.Error(), "already exists")

		// Verify the first barrier is still accessible and unchanged
		getByIdResponse, err := barriersCore.GetBarrier(&corepb.GetBarrierRequest{
			BarrierId: barrierId1,
		})

		require.NoError(t, err)
		require.NotNil(t, getByIdResponse.Barrier)
		require.Equal(t, barrierId1.BarrierId, getByIdResponse.Barrier.Id.BarrierId)
		require.Equal(t, "First barrier", getByIdResponse.Barrier.Description)
	})

	t.Run("max number of barriers per namespace", func(t *testing.T) {
		barriersCore := newBarriersCore(t)

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

			response, err := barriersCore.CreateBarrier(&corepb.CreateBarrierRequest{
				BarrierId:                       barrierId,
				Name:                            fmt.Sprintf("barrier_%d", i),
				Description:                     fmt.Sprintf("Barrier %d", i),
				ExpectedProcesses:               3,
				MaxNumberOfBarriersPerNamespace: maxBarriers,
				Now:                             now.UnixNano(),
			})

			require.NoError(t, err)
			require.NotNil(t, response.Barrier)
		}

		// Try to create one more barrier - should fail
		barrierId := &corepb.BarrierId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			BarrierId:   rand.Uint64(),
		}

		_, err := barriersCore.CreateBarrier(&corepb.CreateBarrierRequest{
			BarrierId:                       barrierId,
			Name:                            "barrier_exceeding_limit",
			Description:                     "Exceeding limit",
			ExpectedProcesses:               3,
			MaxNumberOfBarriersPerNamespace: maxBarriers,
			Now:                             now.Add(time.Minute).UnixNano(),
		})

		require.Error(t, err)
		require.Contains(t, err.Error(), "max number of barriers per namespace reached")
	})
}

func TestCore_GetBarrier(t *testing.T) {
	t.Run("get existing barrier", func(t *testing.T) {
		barriersCore := newBarriersCore(t)

		now := time.Now()

		barrierId := &corepb.BarrierId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
			BarrierId:   rand.Uint64(),
		}

		// T+0: Create barrier
		createResponse, err := barriersCore.CreateBarrier(&corepb.CreateBarrierRequest{
			BarrierId:                       barrierId,
			Name:                            "test_barrier",
			Description:                     "Test barrier",
			ExpectedProcesses:               3,
			MaxNumberOfBarriersPerNamespace: 10,
			Now:                             now.UnixNano(),
		})

		require.NoError(t, err)
		require.NotNil(t, createResponse.Barrier)

		// T+1m: Get barrier
		getResponse, err := barriersCore.GetBarrier(&corepb.GetBarrierRequest{
			BarrierId: barrierId,
		})

		require.NoError(t, err)
		require.NotNil(t, getResponse.Barrier)
		require.Equal(t, barrierId.BarrierId, getResponse.Barrier.Id.BarrierId)
		require.Equal(t, "test_barrier", getResponse.Barrier.Name)
		require.Equal(t, "Test barrier", getResponse.Barrier.Description)
		require.EqualValues(t, 3, getResponse.Barrier.ExpectedProcesses)
		require.EqualValues(t, 0, getResponse.Barrier.ArrivedProcesses)
	})

	t.Run("get nonexistent barrier", func(t *testing.T) {
		barriersCore := newBarriersCore(t)

		barrierId := &corepb.BarrierId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
			BarrierId:   rand.Uint64(),
		}

		// Get nonexistent barrier
		_, err := barriersCore.GetBarrier(&corepb.GetBarrierRequest{
			BarrierId: barrierId,
		})

		require.Error(t, err)
		require.Contains(t, err.Error(), "barrier not found")
	})
}

func TestCore_GetBarrierByName(t *testing.T) {
	t.Run("get existing barrier", func(t *testing.T) {
		barriersCore := newBarriersCore(t)

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
		createResponse, err := barriersCore.CreateBarrier(&corepb.CreateBarrierRequest{
			BarrierId:                       barrierId,
			Name:                            "named_barrier",
			Description:                     "Barrier found by name",
			ExpectedProcesses:               3,
			MaxNumberOfBarriersPerNamespace: 10,
			Now:                             now.UnixNano(),
		})

		require.NoError(t, err)
		require.NotNil(t, createResponse.Barrier)

		// T+1m: Get barrier by name
		getResponse, err := barriersCore.GetBarrierByName(&corepb.GetBarrierByNameRequest{
			NamespaceId: namespaceId,
			BarrierName: "named_barrier",
		})

		require.NoError(t, err)
		require.NotNil(t, getResponse.Barrier)
		require.Equal(t, barrierId.BarrierId, getResponse.Barrier.Id.BarrierId)
		require.Equal(t, "named_barrier", getResponse.Barrier.Name)
	})

	t.Run("get nonexistent barrier", func(t *testing.T) {
		barriersCore := newBarriersCore(t)

		namespaceId := &corepb.NamespaceId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
		}

		// Get nonexistent barrier by name
		_, err := barriersCore.GetBarrierByName(&corepb.GetBarrierByNameRequest{
			NamespaceId: namespaceId,
			BarrierName: "nonexistent_barrier",
		})

		require.Error(t, err)
		require.Contains(t, err.Error(), "barrier not found")
	})
}

func TestCore_ListBarriers(t *testing.T) {
	t.Run("empty namespace", func(t *testing.T) {
		barriersCore := newBarriersCore(t)

		namespaceId := &corepb.NamespaceId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
		}

		// List barriers in empty namespace
		response, err := barriersCore.ListBarriers(&corepb.ListBarriersRequest{
			NamespaceId: namespaceId,
		})

		require.NoError(t, err)
		require.NotNil(t, response)
		require.Empty(t, response.Barriers)
	})

	t.Run("multiple barriers", func(t *testing.T) {
		barriersCore := newBarriersCore(t)

		now := time.Now()

		namespaceId := &corepb.NamespaceId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
		}

		// Create multiple barriers
		for i := 0; i < 5; i++ {
			barrierId := &corepb.BarrierId{
				AccountId:   namespaceId.AccountId,
				NamespaceId: namespaceId.NamespaceId,
				BarrierId:   rand.Uint64(),
			}

			response, err := barriersCore.CreateBarrier(&corepb.CreateBarrierRequest{
				BarrierId:                       barrierId,
				Name:                            fmt.Sprintf("barrier_%d", i),
				Description:                     fmt.Sprintf("Barrier %d", i),
				ExpectedProcesses:               uint64(i + 1),
				MaxNumberOfBarriersPerNamespace: 10,
				Now:                             now.UnixNano(),
			})

			require.NoError(t, err)
			require.NotNil(t, response.Barrier)
		}

		// List barriers
		listResponse, err := barriersCore.ListBarriers(&corepb.ListBarriersRequest{
			NamespaceId: namespaceId,
		})

		require.NoError(t, err)
		require.NotNil(t, listResponse)
		require.Len(t, listResponse.Barriers, 5)
	})

	t.Run("multiple namespaces", func(t *testing.T) {
		barriersCore := newBarriersCore(t)

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
		for i := 0; i < 3; i++ {
			barrierId := &corepb.BarrierId{
				AccountId:   namespaceId1.AccountId,
				NamespaceId: namespaceId1.NamespaceId,
				BarrierId:   rand.Uint64(),
			}

			response, err := barriersCore.CreateBarrier(&corepb.CreateBarrierRequest{
				BarrierId:                       barrierId,
				Name:                            fmt.Sprintf("barrier_ns1_%d", i),
				Description:                     fmt.Sprintf("Barrier NS1 %d", i),
				ExpectedProcesses:               3,
				MaxNumberOfBarriersPerNamespace: 10,
				Now:                             now.UnixNano(),
			})

			require.NoError(t, err)
			require.NotNil(t, response.Barrier)
		}

		// Create barriers in namespace 2
		for i := 0; i < 2; i++ {
			barrierId := &corepb.BarrierId{
				AccountId:   namespaceId2.AccountId,
				NamespaceId: namespaceId2.NamespaceId,
				BarrierId:   rand.Uint64(),
			}

			response, err := barriersCore.CreateBarrier(&corepb.CreateBarrierRequest{
				BarrierId:                       barrierId,
				Name:                            fmt.Sprintf("barrier_ns2_%d", i),
				Description:                     fmt.Sprintf("Barrier NS2 %d", i),
				ExpectedProcesses:               5,
				MaxNumberOfBarriersPerNamespace: 10,
				Now:                             now.UnixNano(),
			})

			require.NoError(t, err)
			require.NotNil(t, response.Barrier)
		}

		// List barriers in namespace 1
		listResponse1, err := barriersCore.ListBarriers(&corepb.ListBarriersRequest{
			NamespaceId: namespaceId1,
		})

		require.NoError(t, err)
		require.NotNil(t, listResponse1)
		require.Len(t, listResponse1.Barriers, 3)

		// List barriers in namespace 2
		listResponse2, err := barriersCore.ListBarriers(&corepb.ListBarriersRequest{
			NamespaceId: namespaceId2,
		})

		require.NoError(t, err)
		require.NotNil(t, listResponse2)
		require.Len(t, listResponse2.Barriers, 2)
	})
}

func TestCore_DeleteBarrier(t *testing.T) {
	t.Run("delete existing barrier", func(t *testing.T) {
		barriersCore := newBarriersCore(t)

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
		createResponse, err := barriersCore.CreateBarrier(&corepb.CreateBarrierRequest{
			BarrierId:                       barrierId,
			Name:                            "test_barrier",
			Description:                     "Test barrier",
			ExpectedProcesses:               3,
			MaxNumberOfBarriersPerNamespace: 10,
			Now:                             now.UnixNano(),
		})

		require.NoError(t, err)
		require.NotNil(t, createResponse.Barrier)

		// T+1m: Delete barrier
		deleteResponse, err := barriersCore.DeleteBarrier(&corepb.DeleteBarrierRequest{
			NamespaceId: namespaceId,
			BarrierName: "test_barrier",
		})

		require.NoError(t, err)
		require.NotNil(t, deleteResponse)

		// T+2m: Try to get deleted barrier
		_, err = barriersCore.GetBarrierByName(&corepb.GetBarrierByNameRequest{
			NamespaceId: namespaceId,
			BarrierName: "test_barrier",
		})

		require.Error(t, err)
		require.Contains(t, err.Error(), "barrier not found")
	})

	t.Run("delete nonexistent barrier", func(t *testing.T) {
		barriersCore := newBarriersCore(t)

		namespaceId := &corepb.NamespaceId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
		}

		// Delete nonexistent barrier - should not error
		deleteResponse, err := barriersCore.DeleteBarrier(&corepb.DeleteBarrierRequest{
			NamespaceId: namespaceId,
			BarrierName: "nonexistent_barrier",
		})

		require.NoError(t, err)
		require.NotNil(t, deleteResponse)
	})

	t.Run("updates counter", func(t *testing.T) {
		barriersCore := newBarriersCore(t)

		now := time.Now()

		namespaceId := &corepb.NamespaceId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
		}

		// Create two barriers
		for i := 0; i < 2; i++ {
			barrierId := &corepb.BarrierId{
				AccountId:   namespaceId.AccountId,
				NamespaceId: namespaceId.NamespaceId,
				BarrierId:   rand.Uint64(),
			}

			response, err := barriersCore.CreateBarrier(&corepb.CreateBarrierRequest{
				BarrierId:                       barrierId,
				Name:                            fmt.Sprintf("barrier_%d", i),
				Description:                     fmt.Sprintf("Barrier %d", i),
				ExpectedProcesses:               3,
				MaxNumberOfBarriersPerNamespace: 10,
				Now:                             now.UnixNano(),
			})

			require.NoError(t, err)
			require.NotNil(t, response.Barrier)
		}

		// List barriers - should have 2
		listResponse1, err := barriersCore.ListBarriers(&corepb.ListBarriersRequest{
			NamespaceId: namespaceId,
		})

		require.NoError(t, err)
		require.Len(t, listResponse1.Barriers, 2)

		// Delete one barrier
		deleteResponse, err := barriersCore.DeleteBarrier(&corepb.DeleteBarrierRequest{
			NamespaceId: namespaceId,
			BarrierName: "barrier_0",
		})

		require.NoError(t, err)
		require.NotNil(t, deleteResponse)

		// Create another barrier - should succeed because we now have room
		barrierId := &corepb.BarrierId{
			AccountId:   namespaceId.AccountId,
			NamespaceId: namespaceId.NamespaceId,
			BarrierId:   rand.Uint64(),
		}

		createResponse, err := barriersCore.CreateBarrier(&corepb.CreateBarrierRequest{
			BarrierId:                       barrierId,
			Name:                            "barrier_new",
			Description:                     "New barrier",
			ExpectedProcesses:               3,
			MaxNumberOfBarriersPerNamespace: 10,
			Now:                             now.Add(time.Minute).UnixNano(),
		})

		require.NoError(t, err)
		require.NotNil(t, createResponse.Barrier)
	})
}

func TestCore_UpdateBarrier(t *testing.T) {
	t.Run("update barrier successfully", func(t *testing.T) {
		barriersCore := newBarriersCore(t)

		now := time.Now()

		barrierId := &corepb.BarrierId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
			BarrierId:   rand.Uint64(),
		}

		// T+0: Create barrier
		createResponse, err := barriersCore.CreateBarrier(&corepb.CreateBarrierRequest{
			BarrierId:                       barrierId,
			Name:                            "test_barrier",
			Description:                     "Original description",
			ExpectedProcesses:               3,
			MaxNumberOfBarriersPerNamespace: 10,
			Now:                             now.UnixNano(),
		})

		require.NoError(t, err)
		require.NotNil(t, createResponse.Barrier)
		require.Equal(t, "Original description", createResponse.Barrier.Description)
		require.EqualValues(t, 3, createResponse.Barrier.ExpectedProcesses)
		require.Equal(t, now.UnixNano(), createResponse.Barrier.UpdatedAt)

		// T+1m: Update barrier
		updateTime := now.Add(time.Minute)
		updateResponse, err := barriersCore.UpdateBarrier(&corepb.UpdateBarrierRequest{
			BarrierId:         barrierId,
			Description:       "Updated description",
			ExpectedProcesses: 5,
			Now:               updateTime.UnixNano(),
		})

		require.NoError(t, err)
		require.NotNil(t, updateResponse.Barrier)
		require.Equal(t, "Updated description", updateResponse.Barrier.Description)
		require.EqualValues(t, 5, updateResponse.Barrier.ExpectedProcesses)
		require.Equal(t, updateTime.UnixNano(), updateResponse.Barrier.UpdatedAt)
		require.Equal(t, now.UnixNano(), updateResponse.Barrier.CreatedAt) // CreatedAt should not change

		// T+2m: Get barrier to verify update persisted
		getResponse, err := barriersCore.GetBarrier(&corepb.GetBarrierRequest{
			BarrierId: barrierId,
		})

		require.NoError(t, err)
		require.NotNil(t, getResponse.Barrier)
		require.Equal(t, "Updated description", getResponse.Barrier.Description)
		require.EqualValues(t, 5, getResponse.Barrier.ExpectedProcesses)
		require.Equal(t, updateTime.UnixNano(), getResponse.Barrier.UpdatedAt)
	})

	t.Run("update nonexistent barrier", func(t *testing.T) {
		barriersCore := newBarriersCore(t)

		now := time.Now()

		barrierId := &corepb.BarrierId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
			BarrierId:   rand.Uint64(),
		}

		// Try to update a barrier that doesn't exist
		_, err := barriersCore.UpdateBarrier(&corepb.UpdateBarrierRequest{
			BarrierId:         barrierId,
			Description:       "Updated description",
			ExpectedProcesses: 5,
			Now:               now.UnixNano(),
		})

		require.Error(t, err)
		require.Contains(t, err.Error(), "barrier not found")
	})

	t.Run("cannot reduce expected_processes below arrived_processes", func(t *testing.T) {
		barriersCore := newBarriersCore(t)

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
		createResponse, err := barriersCore.CreateBarrier(&corepb.CreateBarrierRequest{
			BarrierId:                       barrierId,
			Name:                            "test_barrier",
			Description:                     "Test barrier",
			ExpectedProcesses:               5,
			MaxNumberOfBarriersPerNamespace: 10,
			Now:                             now.UnixNano(),
		})

		require.NoError(t, err)
		require.NotNil(t, createResponse.Barrier)

		// T+1m: Have 3 processes arrive at the barrier
		for i := 0; i < 3; i++ {
			_, err := barriersCore.ArriveAtBarrier(&corepb.ArriveAtBarrierRequest{
				NamespaceId: namespaceId,
				BarrierName: "test_barrier",
				ProcessId:   fmt.Sprintf("process-%d", i),
				Generation:  1,
				Now:         now.Add(time.Minute).UnixNano(),
			})
			require.NoError(t, err)
		}

		// T+2m: Try to update expected_processes to 2 (less than 3 arrived processes)
		_, err = barriersCore.UpdateBarrier(&corepb.UpdateBarrierRequest{
			BarrierId:         barrierId,
			Description:       "Updated description",
			ExpectedProcesses: 2,
			Now:               now.Add(2 * time.Minute).UnixNano(),
		})

		require.Error(t, err)
		require.Contains(t, err.Error(), "there are currently more arrived processes than the new expected processes")

		// T+3m: Update to expected_processes = 3 (equal to arrived_processes) should succeed
		updateResponse, err := barriersCore.UpdateBarrier(&corepb.UpdateBarrierRequest{
			BarrierId:         barrierId,
			Description:       "Updated description",
			ExpectedProcesses: 3,
			Now:               now.Add(3 * time.Minute).UnixNano(),
		})

		require.NoError(t, err)
		require.NotNil(t, updateResponse.Barrier)
		require.EqualValues(t, 3, updateResponse.Barrier.ExpectedProcesses)

		// T+4m: Update to expected_processes = 10 (greater than arrived_processes) should succeed
		updateResponse2, err := barriersCore.UpdateBarrier(&corepb.UpdateBarrierRequest{
			BarrierId:         barrierId,
			Description:       "Updated description again",
			ExpectedProcesses: 10,
			Now:               now.Add(4 * time.Minute).UnixNano(),
		})

		require.NoError(t, err)
		require.NotNil(t, updateResponse2.Barrier)
		require.EqualValues(t, 10, updateResponse2.Barrier.ExpectedProcesses)
	})
}

func TestCore_ArriveAtBarrier(t *testing.T) {
	t.Run("multiple processes", func(t *testing.T) {
		barriersCore := newBarriersCore(t)

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
		createResponse, err := barriersCore.CreateBarrier(&corepb.CreateBarrierRequest{
			BarrierId:                       barrierId,
			Name:                            "test_barrier",
			Description:                     "Test barrier",
			ExpectedProcesses:               3,
			MaxNumberOfBarriersPerNamespace: 10,
			Now:                             now.UnixNano(),
		})

		require.NoError(t, err)
		require.NotNil(t, createResponse.Barrier)

		// T+1m: First process arrives
		arriveResponse1, err := barriersCore.ArriveAtBarrier(&corepb.ArriveAtBarrierRequest{
			NamespaceId: namespaceId,
			BarrierName: "test_barrier",
			ProcessId:   "process_1",
			Generation:  1,
			Now:         now.Add(time.Minute).UnixNano(),
		})

		require.NoError(t, err)
		require.NotNil(t, arriveResponse1)

		// Get barrier and verify arrived processes count
		getResponse1, err := barriersCore.GetBarrier(&corepb.GetBarrierRequest{
			BarrierId: barrierId,
		})

		require.NoError(t, err)
		require.EqualValues(t, 1, getResponse1.Barrier.ArrivedProcesses)
		require.EqualValues(t, 3, getResponse1.Barrier.ExpectedProcesses)

		// T+2m: Second process arrives
		arriveResponse2, err := barriersCore.ArriveAtBarrier(&corepb.ArriveAtBarrierRequest{
			NamespaceId: namespaceId,
			BarrierName: "test_barrier",
			ProcessId:   "process_2",
			Generation:  1,
			Now:         now.Add(2 * time.Minute).UnixNano(),
		})

		require.NoError(t, err)
		require.NotNil(t, arriveResponse2)

		// Get barrier and verify arrived processes count
		getResponse2, err := barriersCore.GetBarrier(&corepb.GetBarrierRequest{
			BarrierId: barrierId,
		})

		require.NoError(t, err)
		require.EqualValues(t, 2, getResponse2.Barrier.ArrivedProcesses)

		// T+3m: Third process arrives
		arriveResponse3, err := barriersCore.ArriveAtBarrier(&corepb.ArriveAtBarrierRequest{
			NamespaceId: namespaceId,
			BarrierName: "test_barrier",
			ProcessId:   "process_3",
			Generation:  1,
			Now:         now.Add(3 * time.Minute).UnixNano(),
		})

		require.NoError(t, err)
		require.NotNil(t, arriveResponse3)

		// Get barrier and verify all processes have arrived
		getResponse3, err := barriersCore.GetBarrier(&corepb.GetBarrierRequest{
			BarrierId: barrierId,
		})

		require.NoError(t, err)
		require.EqualValues(t, 3, getResponse3.Barrier.ArrivedProcesses)
		require.EqualValues(t, 3, getResponse3.Barrier.ExpectedProcesses)
	})

	t.Run("arrive twice", func(t *testing.T) {
		barriersCore := newBarriersCore(t)

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
		createResponse, err := barriersCore.CreateBarrier(&corepb.CreateBarrierRequest{
			BarrierId:                       barrierId,
			Name:                            "test_barrier",
			Description:                     "Test barrier",
			ExpectedProcesses:               3,
			MaxNumberOfBarriersPerNamespace: 10,
			Now:                             now.UnixNano(),
		})

		require.NoError(t, err)
		require.NotNil(t, createResponse.Barrier)

		// T+1m: Process arrives
		arriveResponse1, err := barriersCore.ArriveAtBarrier(&corepb.ArriveAtBarrierRequest{
			NamespaceId: namespaceId,
			BarrierName: "test_barrier",
			ProcessId:   "process_1",
			Generation:  1,
			Now:         now.Add(time.Minute).UnixNano(),
		})

		require.NoError(t, err)
		require.NotNil(t, arriveResponse1)

		// T+2m: Same process arrives again - should not increment count
		arriveResponse2, err := barriersCore.ArriveAtBarrier(&corepb.ArriveAtBarrierRequest{
			NamespaceId: namespaceId,
			BarrierName: "test_barrier",
			ProcessId:   "process_1",
			Generation:  1,
			Now:         now.Add(2 * time.Minute).UnixNano(),
		})

		require.NoError(t, err)
		require.NotNil(t, arriveResponse2)

		// Get barrier and verify count is still 1
		getResponse, err := barriersCore.GetBarrier(&corepb.GetBarrierRequest{
			BarrierId: barrierId,
		})

		require.NoError(t, err)
		require.EqualValues(t, 1, getResponse.Barrier.ArrivedProcesses)
	})

	t.Run("nonexistent barrier", func(t *testing.T) {
		barriersCore := newBarriersCore(t)

		now := time.Now()

		namespaceId := &corepb.NamespaceId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
		}

		// Try to arrive at nonexistent barrier
		_, err := barriersCore.ArriveAtBarrier(&corepb.ArriveAtBarrierRequest{
			NamespaceId: namespaceId,
			BarrierName: "nonexistent_barrier",
			ProcessId:   "process_1",
			Generation:  1,
			Now:         now.UnixNano(),
		})

		require.Error(t, err)
		require.Contains(t, err.Error(), "barrier not found")
	})
}

func TestCore_ListBarrierParticipants(t *testing.T) {
	t.Run("multiple participants", func(t *testing.T) {
		barriersCore := newBarriersCore(t)

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
		createResponse, err := barriersCore.CreateBarrier(&corepb.CreateBarrierRequest{
			BarrierId:                       barrierId,
			Name:                            "test_barrier",
			Description:                     "Test barrier",
			ExpectedProcesses:               3,
			MaxNumberOfBarriersPerNamespace: 10,
			Now:                             now.UnixNano(),
		})

		require.NoError(t, err)
		require.NotNil(t, createResponse.Barrier)

		// Have multiple processes arrive
		for i := 0; i < 3; i++ {
			arriveResponse, err := barriersCore.ArriveAtBarrier(&corepb.ArriveAtBarrierRequest{
				NamespaceId: namespaceId,
				BarrierName: "test_barrier",
				ProcessId:   fmt.Sprintf("process_%d", i),
				Generation:  1,
				Now:         now.Add(time.Duration(i+1) * time.Minute).UnixNano(),
			})

			require.NoError(t, err)
			require.NotNil(t, arriveResponse)
		}

		// List participants
		listResponse, err := barriersCore.ListBarrierParticipants(&corepb.ListBarrierParticipantsRequest{
			NamespaceId: namespaceId,
			BarrierName: "test_barrier",
		})

		require.NoError(t, err)
		require.NotNil(t, listResponse)
		require.Len(t, listResponse.Participants, 3)

		// Verify all participants are present
		participantIds := make([]string, len(listResponse.Participants))
		for i, participant := range listResponse.Participants {
			participantIds[i] = participant.ProcessId
			require.EqualValues(t, 1, participant.Generation)
		}

		require.Contains(t, participantIds, "process_0")
		require.Contains(t, participantIds, "process_1")
		require.Contains(t, participantIds, "process_2")
	})

	t.Run("empty", func(t *testing.T) {
		barriersCore := newBarriersCore(t)

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
		createResponse, err := barriersCore.CreateBarrier(&corepb.CreateBarrierRequest{
			BarrierId:                       barrierId,
			Name:                            "test_barrier",
			Description:                     "Test barrier",
			ExpectedProcesses:               3,
			MaxNumberOfBarriersPerNamespace: 10,
			Now:                             now.UnixNano(),
		})

		require.NoError(t, err)
		require.NotNil(t, createResponse.Barrier)

		// List participants before any have arrived
		listResponse, err := barriersCore.ListBarrierParticipants(&corepb.ListBarrierParticipantsRequest{
			NamespaceId: namespaceId,
			BarrierName: "test_barrier",
		})

		require.NoError(t, err)
		require.NotNil(t, listResponse)
		require.Empty(t, listResponse.Participants)
	})

	t.Run("nonexistent barrier", func(t *testing.T) {
		barriersCore := newBarriersCore(t)

		namespaceId := &corepb.NamespaceId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
		}

		// Try to list participants of nonexistent barrier
		_, err := barriersCore.ListBarrierParticipants(&corepb.ListBarrierParticipantsRequest{
			NamespaceId: namespaceId,
			BarrierName: "nonexistent_barrier",
		})

		require.Error(t, err)
		require.Contains(t, err.Error(), "barrier not found")
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
	barriersCore1 := newBarriersCore(t)
	barriersCore2 := newBarriersCore(t)

	// T+0: Create barrier
	createResponse, err := barriersCore1.CreateBarrier(&corepb.CreateBarrierRequest{
		BarrierId:                       barrierId,
		Name:                            "test_barrier",
		Description:                     "Test barrier",
		ExpectedProcesses:               3,
		MaxNumberOfBarriersPerNamespace: 10,
		Now:                             now.UnixNano(),
	})

	require.NoError(t, err)
	require.NotNil(t, createResponse.Barrier)

	// T+1m: First process arrives
	arriveResponse1, err := barriersCore1.ArriveAtBarrier(&corepb.ArriveAtBarrierRequest{
		NamespaceId: namespaceId,
		BarrierName: "test_barrier",
		ProcessId:   "process_1",
		Generation:  1,
		Now:         now.Add(time.Minute).UnixNano(),
	})

	require.NoError(t, err)
	require.NotNil(t, arriveResponse1)

	// Take snapshot at this point
	snapshot := barriersCore1.Snapshot()

	// T+2m: Second process arrives (after snapshot)
	arriveResponse2, err := barriersCore1.ArriveAtBarrier(&corepb.ArriveAtBarrierRequest{
		NamespaceId: namespaceId,
		BarrierName: "test_barrier",
		ProcessId:   "process_2",
		Generation:  1,
		Now:         now.Add(2 * time.Minute).UnixNano(),
	})

	require.NoError(t, err)
	require.NotNil(t, arriveResponse2)

	// Write snapshot to buffer
	buf := bytes.NewBuffer(nil)
	err = snapshot.Write(buf)
	require.NoError(t, err)

	// Restore snapshot to second core
	err = barriersCore2.Restore(io.NopCloser(buf))
	require.NoError(t, err)

	// Verify the restored state matches the snapshot state
	getResponse, err := barriersCore2.GetBarrier(&corepb.GetBarrierRequest{
		BarrierId: barrierId,
	})

	require.NoError(t, err)
	require.NotNil(t, getResponse.Barrier)
	require.Equal(t, "test_barrier", getResponse.Barrier.Name)
	require.EqualValues(t, 1, getResponse.Barrier.ArrivedProcesses) // Only process_1 arrived before snapshot

	// List participants in restored state
	listResponse, err := barriersCore2.ListBarrierParticipants(&corepb.ListBarrierParticipantsRequest{
		NamespaceId: namespaceId,
		BarrierName: "test_barrier",
	})

	require.NoError(t, err)
	require.NotNil(t, listResponse)
	require.Len(t, listResponse.Participants, 1)
	require.Equal(t, "process_1", listResponse.Participants[0].ProcessId)

	// Verify that the original core has different state (it should have 2 participants)
	listResponse2, err := barriersCore1.ListBarrierParticipants(&corepb.ListBarrierParticipantsRequest{
		NamespaceId: namespaceId,
		BarrierName: "test_barrier",
	})

	require.NoError(t, err)
	require.NotNil(t, listResponse2)
	require.Len(t, listResponse2.Participants, 2)
}

func newBarriersCore(t *testing.T) *Core {
	badgerStore, err := store.NewBadgerInMemoryStore()
	require.NoError(t, err)
	return NewCore(badgerStore, []byte{0x1d, 0x36, 0x00, 0x00}, []byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})
}
