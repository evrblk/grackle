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
		barrier := createBarrier(t, core, barrierId, "test_barrier", 3, 10, now)

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
		_ = createBarrier(t, core, barrierId1, barrierName, 3, 10, now)

		// T+1m: Try to create another barrier with the same name - should fail
		barrierId2 := &corepb.BarrierId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			BarrierId:   rand.Uint64(),
		}

		appErr := createBarrierWithError(t, core, barrierId2, barrierName, 5, 10, now.Add(time.Minute))
		require.NotNil(t, appErr)
		require.Equal(t, monsterax.AlreadyExists, appErr.Code)
		require.Contains(t, appErr.Message, "already exists")

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

			_ = createBarrier(t, core, barrierId, fmt.Sprintf("barrier_%d", i), 3, maxBarriers, now)
		}

		// Try to create one more barrier - should fail
		barrierId := &corepb.BarrierId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			BarrierId:   rand.Uint64(),
		}

		appErr := createBarrierWithError(t, core, barrierId, "barrier_exceeding_limit", 3, maxBarriers, now.Add(time.Minute))
		require.NotNil(t, appErr)
		require.Equal(t, monsterax.ResourceExhausted, appErr.Code)
		require.Contains(t, appErr.Message, "max number of barriers per namespace reached")
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
		_ = createBarrier(t, core, barrierId, "test_barrier", 3, 10, now)

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
		appErr := getBarrierWithError(t, core, barrierId)
		require.NotNil(t, appErr)
		require.Equal(t, monsterax.NotFound, appErr.Code)
		require.Contains(t, appErr.Message, "barrier not found")
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
		_ = createBarrier(t, core, barrierId, "named_barrier", 3, 10, now)

		// T+1m: Get barrier by name
		resp := getBarrierByName(t, core, namespaceId, "named_barrier")
		require.NotNil(t, resp.Barrier)
		require.Equal(t, barrierId.BarrierId, resp.Barrier.Id.BarrierId)
		require.Equal(t, "named_barrier", resp.Barrier.Name)
	})

	t.Run("get nonexistent barrier", func(t *testing.T) {
		core := newBarriersCore(t)
		namespaceId := &corepb.NamespaceId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
		}

		// Get nonexistent barrier by name
		appErr := getBarrierByNameWithError(t, core, namespaceId, "nonexistent_barrier")
		require.Equal(t, monsterax.NotFound, appErr.Code)
		require.Contains(t, appErr.Message, "barrier not found")
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
		resp := listBarriers(t, core, namespaceId)
		require.Empty(t, resp.Barriers)
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

			_ = createBarrier(t, core, barrierId, fmt.Sprintf("barrier_%d", i), uint64(i+1), 10, now)
		}

		// List barriers
		resp := listBarriers(t, core, namespaceId)
		require.Len(t, resp.Barriers, 5)
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

			_ = createBarrier(t, core, barrierId, fmt.Sprintf("barrier_ns1_%d", i), 3, 10, now)
		}

		// Create barriers in namespace 2
		for i := range 2 {
			barrierId := &corepb.BarrierId{
				AccountId:   namespaceId2.AccountId,
				NamespaceId: namespaceId2.NamespaceId,
				BarrierId:   rand.Uint64(),
			}

			_ = createBarrier(t, core, barrierId, fmt.Sprintf("barrier_ns2_%d", i), 5, 10, now)
		}

		// List barriers in namespace 1
		resp1 := listBarriers(t, core, namespaceId1)
		require.Len(t, resp1.Barriers, 3)

		// List barriers in namespace 2
		resp2 := listBarriers(t, core, namespaceId2)
		require.Len(t, resp2.Barriers, 2)
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
		_ = createBarrier(t, core, barrierId, "test_barrier", 3, 10, now)

		// T+1m: Delete barrier
		_ = deleteBarrier(t, core, namespaceId, "test_barrier", rand.Uint64(), now.Add(time.Minute))

		// T+2m: Try to get deleted barrier
		appErr := getBarrierByNameWithError(t, core, namespaceId, "test_barrier")
		require.Contains(t, appErr.Message, "barrier not found")
	})

	t.Run("delete nonexistent barrier", func(t *testing.T) {
		core := newBarriersCore(t)
		namespaceId := &corepb.NamespaceId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
		}

		// Delete nonexistent barrier - should not error
		_ = deleteBarrier(t, core, namespaceId, "nonexistent_barrier", rand.Uint64(), time.Now())
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

			_ = createBarrier(t, core, barrierId, fmt.Sprintf("barrier_%d", i), 3, 10, now)
		}

		// List barriers - should have 2
		resp1 := listBarriers(t, core, namespaceId)
		require.Len(t, resp1.Barriers, 2)

		// Delete one barrier
		_ = deleteBarrier(t, core, namespaceId, "barrier_0", rand.Uint64(), now)

		// Create another barrier - should succeed because we now have room
		barrierId := &corepb.BarrierId{
			AccountId:   namespaceId.AccountId,
			NamespaceId: namespaceId.NamespaceId,
			BarrierId:   rand.Uint64(),
		}

		_ = createBarrier(t, core, barrierId, "barrier_new", 3, 10, now.Add(time.Minute))
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
		barrier := createBarrier(t, core, barrierId, "test_barrier", 3, 10, now)

		require.Equal(t, "Test barrier description", barrier.Description)
		require.EqualValues(t, 3, barrier.ExpectedProcesses)
		require.Equal(t, now.UnixNano(), barrier.UpdatedAt)

		// T+1m: Update barrier
		updateTime := now.Add(time.Minute)
		resp := updateBarrier(t, core, barrierId, "Updated description", 5, updateTime)
		require.NotNil(t, resp.Barrier)
		require.Equal(t, "Updated description", resp.Barrier.Description)
		require.EqualValues(t, 5, resp.Barrier.ExpectedProcesses)
		require.Equal(t, updateTime.UnixNano(), resp.Barrier.UpdatedAt)
		require.Equal(t, now.UnixNano(), resp.Barrier.CreatedAt) // CreatedAt should not change

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
		appErr := updateBarrierWithError(t, core, barrierId, "Updated description", 5, now)
		require.Equal(t, monsterax.NotFound, appErr.Code)
		require.Contains(t, appErr.Message, "barrier not found")
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
		_ = createBarrier(t, core, barrierId, "test_barrier", 5, 10, now)

		// T+1m: Have 3 processes arrive at the barrier
		for i := range 3 {
			_ = arriveAtBarrier(t, core, namespaceId, "test_barrier", fmt.Sprintf("process-%d", i), 1, now.Add(time.Minute))
		}

		// T+2m: Try to update expected_processes to 2 (less than 3 arrived processes)
		appErr := updateBarrierWithError(t, core, barrierId, "Updated description", 2, now.Add(2*time.Minute))
		require.NotNil(t, appErr)
		require.Equal(t, monsterax.InvalidArgument, appErr.Code)
		require.Contains(t, appErr.Message, "there are currently more arrived processes than the new expected processes")

		// T+3m: Update to expected_processes = 3 (equal to arrived_processes) should succeed
		resp1 := updateBarrier(t, core, barrierId, "Updated description", 3, now.Add(3*time.Minute))
		require.NotNil(t, resp1.Barrier)
		require.EqualValues(t, 3, resp1.Barrier.ExpectedProcesses)

		// T+4m: Update to expected_processes = 10 (greater than arrived_processes) should succeed
		resp2 := updateBarrier(t, core, barrierId, "Updated description again", 10, now.Add(4*time.Minute))
		require.NotNil(t, resp2.Barrier)
		require.EqualValues(t, 10, resp2.Barrier.ExpectedProcesses)
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

		// T+0: Create barrier expecting 3 participants (initial generation is 1)
		_ = createBarrier(t, core, barrierId, "test_barrier", 3, 10, now)

		// T+1m: First process arrives
		_ = arriveAtBarrier(t, core, namespaceId, "test_barrier", "process_1", 1, now.Add(time.Minute))

		barrier := getBarrier(t, core, barrierId)
		require.EqualValues(t, 1, barrier.ArrivedProcesses)
		require.EqualValues(t, 3, barrier.ExpectedProcesses)
		require.EqualValues(t, 1, barrier.Generation)

		// T+2m: Second process arrives
		_ = arriveAtBarrier(t, core, namespaceId, "test_barrier", "process_2", 1, now.Add(2*time.Minute))

		barrier = getBarrier(t, core, barrierId)
		require.EqualValues(t, 2, barrier.ArrivedProcesses)
		require.EqualValues(t, 1, barrier.Generation)

		// T+3m: Third process arrives — this is the last expected arrival, so the
		// barrier auto-trips: ArrivedProcesses resets to 0 and Generation advances.
		_ = arriveAtBarrier(t, core, namespaceId, "test_barrier", "process_3", 1, now.Add(3*time.Minute))

		barrier = getBarrier(t, core, barrierId)
		require.EqualValues(t, 0, barrier.ArrivedProcesses)
		require.EqualValues(t, 3, barrier.ExpectedProcesses)
		require.EqualValues(t, 2, barrier.Generation)

		// All three participant rows from generation 1 are preserved (clients can read
		// them e.g. via ListBarrierParticipants until GC reaps the barrier).
		resp := listBarrierParticipants(t, core, namespaceId, "test_barrier")
		require.Len(t, resp.Participants, 3)
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
		_ = createBarrier(t, core, barrierId, "test_barrier", 3, 10, now)

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
		err := arriveAtBarrierWithError(t, core, namespaceId, "nonexistent_barrier", "process_1", 1, now)
		require.NotNil(t, err)
		require.Equal(t, monsterax.NotFound, err.Code)
		require.Contains(t, err.Message, "barrier not found")
	})

	t.Run("auto trip on last arrival", func(t *testing.T) {
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

		// T+0: Create barrier expecting 2 participants
		_ = createBarrier(t, core, barrierId, "test_barrier", 2, 10, now)

		// T+1m: First arrival — counter goes to 1, no trip yet.
		_ = arriveAtBarrier(t, core, namespaceId, "test_barrier", "process_1", 1, now.Add(time.Minute))
		barrier := getBarrier(t, core, barrierId)
		require.EqualValues(t, 1, barrier.ArrivedProcesses)
		require.EqualValues(t, 1, barrier.Generation)

		// T+2m: Second arrival is the last expected one — auto-trip.
		_ = arriveAtBarrier(t, core, namespaceId, "test_barrier", "process_2", 1, now.Add(2*time.Minute))
		barrier = getBarrier(t, core, barrierId)
		require.EqualValues(t, 0, barrier.ArrivedProcesses)
		require.EqualValues(t, 2, barrier.ExpectedProcesses)
		require.EqualValues(t, 2, barrier.Generation)

		// Both participant rows from generation 1 are preserved.
		resp := listBarrierParticipants(t, core, namespaceId, "test_barrier")
		require.Len(t, resp.Participants, 2)

		// T+3m: A third process that still references generation 1 is now stale and
		// must be rejected (the trip already happened, generation moved to 2).
		appErr := arriveAtBarrierWithError(t, core, namespaceId, "test_barrier", "process_3", 1, now.Add(3*time.Minute))
		require.Equal(t, monsterax.InvalidArgument, appErr.Code)
		require.Contains(t, appErr.Message, "generation")

		// The next round at generation 2 starts clean.
		_ = arriveAtBarrier(t, core, namespaceId, "test_barrier", "process_1", 2, now.Add(4*time.Minute))
		barrier = getBarrier(t, core, barrierId)
		require.EqualValues(t, 1, barrier.ArrivedProcesses)
		require.EqualValues(t, 2, barrier.Generation)
	})

	t.Run("auto trip with single expected process", func(t *testing.T) {
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

		// Single-participant barrier: the first arrival is also the trip.
		_ = createBarrier(t, core, barrierId, "test_barrier", 1, 10, now)
		_ = arriveAtBarrier(t, core, namespaceId, "test_barrier", "process_1", 1, now.Add(time.Minute))

		barrier := getBarrier(t, core, barrierId)
		require.EqualValues(t, 0, barrier.ArrivedProcesses)
		require.EqualValues(t, 2, barrier.Generation)
	})

	t.Run("old generation", func(t *testing.T) {
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

		// T+0: Create barrier — initial generation is 1
		_ = createBarrier(t, core, barrierId, "test_barrier", 3, 10, now)

		// Arrival with a generation older than the current one must be rejected
		appErr := arriveAtBarrierWithError(t, core, namespaceId, "test_barrier", "process_1", 0, now.Add(time.Minute))
		require.Equal(t, monsterax.InvalidArgument, appErr.Code)
		require.Contains(t, appErr.Message, "generation")

		// No state should have been persisted
		barrier := getBarrier(t, core, barrierId)
		require.EqualValues(t, 0, barrier.ArrivedProcesses)

		resp := listBarrierParticipants(t, core, namespaceId, "test_barrier")
		require.Empty(t, resp.Participants)

		// Arrival with the current generation still succeeds
		_ = arriveAtBarrier(t, core, namespaceId, "test_barrier", "process_1", 1, now.Add(2*time.Minute))
		barrier = getBarrier(t, core, barrierId)
		require.EqualValues(t, 1, barrier.ArrivedProcesses)
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
		_ = createBarrier(t, core, barrierId, "test_barrier", 3, 10, now)

		// Have multiple processes arrive
		for i := range 3 {
			_ = arriveAtBarrier(t, core, namespaceId, "test_barrier", fmt.Sprintf("process_%d", i), 1, now.Add(time.Duration(i+1)*time.Minute))
		}

		// List participants
		resp := listBarrierParticipants(t, core, namespaceId, "test_barrier")
		require.Len(t, resp.Participants, 3)

		// Verify all participants are present
		participantIds := make([]string, len(resp.Participants))
		for i, participant := range resp.Participants {
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
		_ = createBarrier(t, core, barrierId, "test_barrier", 3, 10, now)

		// List participants before any have arrived
		resp1 := listBarrierParticipants(t, core, namespaceId, "test_barrier")
		require.Empty(t, resp1.Participants)
	})

	t.Run("nonexistent barrier", func(t *testing.T) {
		core := newBarriersCore(t)
		namespaceId := &corepb.NamespaceId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
		}

		// Try to list participants of nonexistent barrier
		appErr := listBarrierParticipantsWithError(t, core, namespaceId, "nonexistent_barrier")
		require.Contains(t, appErr.Message, "barrier not found")
		require.Equal(t, monsterax.NotFound, appErr.Code)
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
	_ = createBarrier(t, core1, barrierId, "test_barrier", 3, 10, now)

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
	resp := listBarrierParticipants(t, core2, namespaceId, "test_barrier")
	require.Len(t, resp.Participants, 1)
	require.Equal(t, "process_1", resp.Participants[0].ProcessId)

	// Verify that the original core has different state (it should have 2 participants)
	resp2 := listBarrierParticipants(t, core1, namespaceId, "test_barrier")
	require.Len(t, resp2.Participants, 2)
}

func TestCore_RunBarriersGarbageCollection(t *testing.T) {
	t.Run("delete_barrier_drains_participants", func(t *testing.T) {
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

		// Barrier with enough participants that a single GC pass cannot drain everything.
		const expectedParticipants = 25
		_ = createBarrier(t, core, barrierId, "barrier_to_delete", expectedParticipants, 10, now)
		for i := range expectedParticipants {
			_ = arriveAtBarrier(t, core, namespaceId, "barrier_to_delete", fmt.Sprintf("process_%d", i), 1, now)
		}

		// Sibling barrier in the same namespace — must survive GC.
		siblingId := &corepb.BarrierId{
			AccountId:   namespaceId.AccountId,
			NamespaceId: namespaceId.NamespaceId,
			BarrierId:   rand.Uint64(),
		}
		_ = createBarrier(t, core, siblingId, "barrier_to_keep", 2, 10, now)
		_ = arriveAtBarrier(t, core, namespaceId, "barrier_to_keep", "alive_process", 1, now)

		// Delete the target barrier — enqueues a barrier-scoped GC record. The barrier row is
		// gone immediately, but participants are left for the GC to drain.
		_ = deleteBarrier(t, core, namespaceId, "barrier_to_delete", rand.Uint64(), now)

		// The barrier row itself is already gone.
		appErr := getBarrierWithError(t, core, barrierId)
		require.Equal(t, monsterax.NotFound, appErr.Code)

		// Run GC with a tight budget (5 visits per pass) — must take multiple iterations to drain.
		const maxVisitedPerPass = int64(5)
		passes := 0
		for ; passes < 100; passes++ {
			runBarriersGarbageCollection(t, core, now, 10, 10, 100, maxVisitedPerPass)

			// Detect completion by checking whether any GC record is left.
			txn := core.badgerStore.View()
			records, err := core.gcRecords.List(txn, 10)
			txn.Discard()
			require.NoError(t, err)
			if len(records) == 0 {
				break
			}
		}
		require.Greater(t, passes, 1, "GC should require more than one pass to drain %d participants with budget %d", expectedParticipants, maxVisitedPerPass)
		require.Less(t, passes, 100, "GC did not converge within 100 passes")

		// The deleted barrier's participants must all be gone. Since the barrier row is also
		// gone, ListBarrierParticipants returns NotFound — drill in via the lower-level helper.
		txn := core.badgerStore.View()
		leftover, err := core.participants.List(txn, barrierId.AccountId, barrierId.NamespaceId, barrierId.BarrierId, nil, 100)
		txn.Discard()
		require.NoError(t, err)
		require.Empty(t, leftover.participants)

		// The sibling barrier and its participant must still be intact.
		sibling := getBarrier(t, core, siblingId)
		require.EqualValues(t, 1, sibling.ArrivedProcesses)
		siblingParticipants := listBarrierParticipants(t, core, namespaceId, "barrier_to_keep")
		require.Len(t, siblingParticipants.Participants, 1)
	})

	t.Run("delete_namespace_drains_everything", func(t *testing.T) {
		core := newBarriersCore(t)
		now := time.Now()

		// Two namespaces: one will be deleted, the other must survive.
		deletedNs := &corepb.NamespaceId{AccountId: rand.Uint64(), NamespaceId: rand.Uint32()}
		keepNs := &corepb.NamespaceId{AccountId: deletedNs.AccountId, NamespaceId: rand.Uint32()}

		// Populate the doomed namespace with two barriers, each with several participants.
		for i := range 2 {
			barrierId := &corepb.BarrierId{
				AccountId:   deletedNs.AccountId,
				NamespaceId: deletedNs.NamespaceId,
				BarrierId:   rand.Uint64(),
			}
			name := fmt.Sprintf("doomed_barrier_%d", i)
			_ = createBarrier(t, core, barrierId, name, 10, 10, now)
			for j := range 6 {
				_ = arriveAtBarrier(t, core, deletedNs, name, fmt.Sprintf("p_%d_%d", i, j), 1, now)
			}
		}

		// Populate the keep namespace with one barrier.
		keepBarrierId := &corepb.BarrierId{
			AccountId:   keepNs.AccountId,
			NamespaceId: keepNs.NamespaceId,
			BarrierId:   rand.Uint64(),
		}
		_ = createBarrier(t, core, keepBarrierId, "keep_barrier", 3, 10, now)
		_ = arriveAtBarrier(t, core, keepNs, "keep_barrier", "alive", 1, now)

		// Mark the doomed namespace for deletion.
		barriersDeleteNamespace(t, core, deletedNs, rand.Uint64(), now)

		// Run GC with a tight budget until everything drains.
		const maxVisitedPerPass = int64(4)
		passes := 0
		for ; passes < 100; passes++ {
			runBarriersGarbageCollection(t, core, now, 10, 10, 100, maxVisitedPerPass)

			txn := core.badgerStore.View()
			records, err := core.gcRecords.List(txn, 10)
			txn.Discard()
			require.NoError(t, err)
			if len(records) == 0 {
				break
			}
		}
		require.Greater(t, passes, 1, "GC should require multiple passes under a tight budget")
		require.Less(t, passes, 100, "GC did not converge within 100 passes")

		// The deleted namespace must contain no barriers and no participants.
		deletedBarriers := listBarriers(t, core, deletedNs)
		require.Empty(t, deletedBarriers.Barriers)

		// The keep namespace must be intact.
		keepBarriers := listBarriers(t, core, keepNs)
		require.Len(t, keepBarriers.Barriers, 1)
		keepParticipants := listBarrierParticipants(t, core, keepNs, "keep_barrier")
		require.Len(t, keepParticipants.Participants, 1)
	})
}

func newBarriersCore(t *testing.T) *Core {
	t.Helper()

	badgerStore, err := store.NewBadgerInMemoryStore()
	require.NoError(t, err)
	return NewCore(badgerStore, []byte{0x1d, 0x36, 0x00, 0x00}, []byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})
}

func createBarrier(t *testing.T, core *Core, barrierId *corepb.BarrierId, name string, expectedProcesses uint64, maxNumberOfBarriersPerNamespace int64, now time.Time) *corepb.Barrier {
	t.Helper()

	resp, err := core.CreateBarrier(&coreapis.CreateBarrierRequest{
		Payload: &corepb.CreateBarrierRequest{
			BarrierId:                       barrierId,
			Name:                            name,
			Description:                     "Test barrier description",
			ExpectedProcesses:               expectedProcesses,
			MaxNumberOfBarriersPerNamespace: maxNumberOfBarriersPerNamespace,
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

func createBarrierWithError(t *testing.T, core *Core, barrierId *corepb.BarrierId, name string, expectedProcesses uint64, maxNumberOfBarriersPerNamespace int64, now time.Time) *monsterax.Error {
	t.Helper()

	resp, err := core.CreateBarrier(&coreapis.CreateBarrierRequest{
		Payload: &corepb.CreateBarrierRequest{
			BarrierId:                       barrierId,
			Name:                            name,
			Description:                     "Test barrier description",
			ExpectedProcesses:               expectedProcesses,
			MaxNumberOfBarriersPerNamespace: maxNumberOfBarriersPerNamespace,
			Now:                             now.UnixNano(),
		},
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Nil(t, resp.Payload)
	require.NotNil(t, resp.ApplicationError)

	return resp.ApplicationError
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

func arriveAtBarrierWithError(t *testing.T, core *Core, namespaceId *corepb.NamespaceId, barrierName string, processId string, generation uint64, now time.Time) *monsterax.Error {
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
	require.NotNil(t, resp.ApplicationError)
	require.Nil(t, resp.Payload)

	return resp.ApplicationError
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

func getBarrierWithError(t *testing.T, core *Core, barrierId *corepb.BarrierId) *monsterax.Error {
	t.Helper()

	resp, err := core.GetBarrier(&coreapis.GetBarrierRequest{
		Payload: &corepb.GetBarrierRequest{
			BarrierId: barrierId,
		},
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.NotNil(t, resp.ApplicationError)
	require.Nil(t, resp.Payload)

	return resp.ApplicationError
}

func listBarriers(t *testing.T, core *Core, namespaceId *corepb.NamespaceId) *corepb.ListBarriersResponse {
	t.Helper()

	resp, err := core.ListBarriers(&coreapis.ListBarriersRequest{
		Payload: &corepb.ListBarriersRequest{
			NamespaceId: namespaceId,
		},
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Nil(t, resp.ApplicationError)
	require.NotNil(t, resp.Payload)

	return resp.Payload
}

func listBarrierParticipants(t *testing.T, core *Core, namespaceId *corepb.NamespaceId, barrierName string) *corepb.ListBarrierParticipantsResponse {
	t.Helper()

	resp, err := core.ListBarrierParticipants(&coreapis.ListBarrierParticipantsRequest{
		Payload: &corepb.ListBarrierParticipantsRequest{
			NamespaceId: namespaceId,
			BarrierName: barrierName,
		},
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Nil(t, resp.ApplicationError)
	require.NotNil(t, resp.Payload)

	return resp.Payload
}

func listBarrierParticipantsWithError(t *testing.T, core *Core, namespaceId *corepb.NamespaceId, barrierName string) *monsterax.Error {
	t.Helper()

	resp, err := core.ListBarrierParticipants(&coreapis.ListBarrierParticipantsRequest{
		Payload: &corepb.ListBarrierParticipantsRequest{
			NamespaceId: namespaceId,
			BarrierName: barrierName,
		},
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Nil(t, resp.Payload)
	require.NotNil(t, resp.ApplicationError)

	return resp.ApplicationError
}

func updateBarrier(t *testing.T, core *Core, barrierId *corepb.BarrierId, description string, expectedProcesses uint64, now time.Time) *corepb.UpdateBarrierResponse {
	t.Helper()

	resp, err := core.UpdateBarrier(&coreapis.UpdateBarrierRequest{
		Payload: &corepb.UpdateBarrierRequest{
			BarrierId:         barrierId,
			Description:       description,
			ExpectedProcesses: expectedProcesses,
			Now:               now.UnixNano(),
		},
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Nil(t, resp.ApplicationError)
	require.NotNil(t, resp.Payload)

	return resp.Payload
}

func updateBarrierWithError(t *testing.T, core *Core, barrierId *corepb.BarrierId, description string, expectedProcesses uint64, now time.Time) *monsterax.Error {
	t.Helper()

	resp, err := core.UpdateBarrier(&coreapis.UpdateBarrierRequest{
		Payload: &corepb.UpdateBarrierRequest{
			BarrierId:         barrierId,
			Description:       description,
			ExpectedProcesses: expectedProcesses,
			Now:               now.UnixNano(),
		},
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Nil(t, resp.Payload)
	require.NotNil(t, resp.ApplicationError)

	return resp.ApplicationError
}

func getBarrierByName(t *testing.T, core *Core, namespaceId *corepb.NamespaceId, barrierName string) *corepb.GetBarrierByNameResponse {
	t.Helper()

	resp, err := core.GetBarrierByName(&coreapis.GetBarrierByNameRequest{
		Payload: &corepb.GetBarrierByNameRequest{
			NamespaceId: namespaceId,
			BarrierName: barrierName,
		},
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Nil(t, resp.ApplicationError)
	require.NotNil(t, resp.Payload)

	return resp.Payload
}

func getBarrierByNameWithError(t *testing.T, core *Core, namespaceId *corepb.NamespaceId, barrierName string) *monsterax.Error {
	t.Helper()

	resp, err := core.GetBarrierByName(&coreapis.GetBarrierByNameRequest{
		Payload: &corepb.GetBarrierByNameRequest{
			NamespaceId: namespaceId,
			BarrierName: barrierName,
		},
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Nil(t, resp.Payload)
	require.NotNil(t, resp.ApplicationError)

	return resp.ApplicationError
}

func deleteBarrier(t *testing.T, core *Core, namespaceId *corepb.NamespaceId, barrierName string, recordId uint64, now time.Time) *corepb.DeleteBarrierResponse {
	t.Helper()

	resp, err := core.DeleteBarrier(&coreapis.DeleteBarrierRequest{
		Payload: &corepb.DeleteBarrierRequest{
			NamespaceId: namespaceId,
			BarrierName: barrierName,
			RecordId:    recordId,
			Now:         now.UnixNano(),
		},
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Nil(t, resp.ApplicationError)
	require.NotNil(t, resp.Payload)

	return resp.Payload
}

func barriersDeleteNamespace(t *testing.T, core *Core, namespaceId *corepb.NamespaceId, recordId uint64, now time.Time) {
	t.Helper()

	resp, err := core.BarriersDeleteNamespace(&coreapis.BarriersDeleteNamespaceRequest{
		Payload: &corepb.BarriersDeleteNamespaceRequest{
			NamespaceId: namespaceId,
			RecordId:    recordId,
			Now:         now.UnixNano(),
		},
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Nil(t, resp.ApplicationError)
	require.NotNil(t, resp.Payload)
}

func runBarriersGarbageCollection(t *testing.T, core *Core, now time.Time, gcRecordsPageSize, gcRecordBarriersPageSize, gcRecordParticipantsPageSize, maxVisited int64) {
	t.Helper()

	resp, err := core.RunBarriersGarbageCollection(&coreapis.RunBarriersGarbageCollectionRequest{
		Payload: &corepb.RunBarriersGarbageCollectionRequest{
			Now:                          now.UnixNano(),
			GcRecordsPageSize:            gcRecordsPageSize,
			GcRecordBarriersPageSize:     gcRecordBarriersPageSize,
			GcRecordParticipantsPageSize: gcRecordParticipantsPageSize,
			MaxVisited:                   maxVisited,
		},
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Nil(t, resp.ApplicationError)
	require.NotNil(t, resp.Payload)
}
