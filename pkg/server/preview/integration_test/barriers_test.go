package integration_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	gracklepb "github.com/evrblk/evrblk-go/grackle/preview"
	"github.com/stretchr/testify/require"
)

func TestListBarriers(t *testing.T) {
	t.Run("validation", func(t *testing.T) {
		server := setupGrackleApiServer(t)
		ctx := context.Background()

		// Create namespace
		_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
			Name: "namespace1",
		})
		require.NoError(t, err)

		// Valid request
		resp, err := server.ListBarriers(ctx, &gracklepb.ListBarriersRequest{
			NamespaceName: "namespace1",
		})
		require.NoError(t, err)
		require.NotNil(t, resp.Barriers)

		// Invalid request - invalid namespace name
		_, err = server.ListBarriers(ctx, &gracklepb.ListBarriersRequest{
			NamespaceName: "invalid@namespace",
		})
		require.Error(t, err)
	})

	t.Run("pagination", func(t *testing.T) {
		server := setupGrackleApiServer(t)
		ctx := context.Background()

		// Create namespace
		_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
			Name: "test-namespace",
		})
		require.NoError(t, err)

		// Create 25 barriers to test pagination (3 pages with limit 10)
		for i := range 25 {
			_, err := server.CreateBarrier(ctx, &gracklepb.CreateBarrierRequest{
				NamespaceName:     "test-namespace",
				BarrierName:       fmt.Sprintf("barrier_%03d", i+1),
				ExpectedProcesses: 3,
				ExpiresAt:         time.Now().Add(10 * time.Minute).UnixNano(),
			})
			require.NoError(t, err)
		}

		// Test forward pagination through 3 pages
		var allBarriers []*gracklepb.Barrier

		// Page 1: Get first 10 barriers
		resp1, err := server.ListBarriers(ctx, &gracklepb.ListBarriersRequest{
			NamespaceName: "test-namespace",
			Limit:         10,
		})
		require.NoError(t, err)
		require.Len(t, resp1.Barriers, 10)
		require.NotEmpty(t, resp1.NextPaginationToken)
		require.Empty(t, resp1.PreviousPaginationToken)

		allBarriers = append(allBarriers, resp1.Barriers...)

		// Page 2: Get next 10 barriers
		resp2, err := server.ListBarriers(ctx, &gracklepb.ListBarriersRequest{
			NamespaceName:   "test-namespace",
			PaginationToken: resp1.NextPaginationToken,
			Limit:           10,
		})
		require.NoError(t, err)
		require.Len(t, resp2.Barriers, 10)
		require.NotEmpty(t, resp2.NextPaginationToken)
		require.NotEmpty(t, resp2.PreviousPaginationToken)

		allBarriers = append(allBarriers, resp2.Barriers...)

		// Page 3: Get remaining 5 barriers
		resp3, err := server.ListBarriers(ctx, &gracklepb.ListBarriersRequest{
			NamespaceName:   "test-namespace",
			PaginationToken: resp2.NextPaginationToken,
			Limit:           10,
		})
		require.NoError(t, err)
		require.Len(t, resp3.Barriers, 5)
		require.Empty(t, resp3.NextPaginationToken)
		require.NotEmpty(t, resp3.PreviousPaginationToken)

		allBarriers = append(allBarriers, resp3.Barriers...)

		// Verify we got all 25 barriers
		require.Len(t, allBarriers, 25)

		// Test backward pagination from the last page
		resp4, err := server.ListBarriers(ctx, &gracklepb.ListBarriersRequest{
			NamespaceName:   "test-namespace",
			PaginationToken: resp3.PreviousPaginationToken,
			Limit:           10,
		})
		require.NoError(t, err)
		require.Len(t, resp4.Barriers, 10)
		require.NotEmpty(t, resp4.NextPaginationToken)
		require.NotEmpty(t, resp4.PreviousPaginationToken)
	})
}

func TestDeleteBarrier(t *testing.T) {
	t.Run("validation", func(t *testing.T) {
		server := setupGrackleApiServer(t)
		ctx := context.Background()

		// Create namespace
		_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
			Name: "namespace1",
		})
		require.NoError(t, err)

		// Valid request
		_, err = server.DeleteBarrier(ctx, &gracklepb.DeleteBarrierRequest{
			NamespaceName: "namespace1",
			BarrierName:   "barrier1",
		})
		require.NoError(t, err)

		// Invalid request - invalid namespace name
		_, err = server.DeleteBarrier(ctx, &gracklepb.DeleteBarrierRequest{
			NamespaceName: "invalid@namespace",
			BarrierName:   "barrier1",
		})
		require.Error(t, err)
	})
}

func TestGetBarrier(t *testing.T) {
	t.Run("validation", func(t *testing.T) {
		server := setupGrackleApiServer(t)
		ctx := context.Background()

		// Create namespace
		_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
			Name: "namespace1",
		})
		require.NoError(t, err)

		// Create barrier
		_, err = server.CreateBarrier(ctx, &gracklepb.CreateBarrierRequest{
			NamespaceName:     "namespace1",
			BarrierName:       "barrier1",
			ExpectedProcesses: 3,
			ExpiresAt:         time.Now().Add(10 * time.Minute).UnixNano(),
		})
		require.NoError(t, err)

		// Valid request
		resp, err := server.GetBarrier(ctx, &gracklepb.GetBarrierRequest{
			NamespaceName: "namespace1",
			BarrierName:   "barrier1",
		})
		require.NoError(t, err)
		require.NotNil(t, resp.Barrier)
		require.Equal(t, "barrier1", resp.Barrier.Name)

		// Invalid request - invalid namespace name
		_, err = server.GetBarrier(ctx, &gracklepb.GetBarrierRequest{
			NamespaceName: "invalid@namespace",
			BarrierName:   "barrier1",
		})
		require.Error(t, err)
	})
}

func TestCreateBarrier(t *testing.T) {
	t.Run("validation", func(t *testing.T) {
		server := setupGrackleApiServer(t)
		ctx := context.Background()

		// Create namespace
		_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
			Name: "namespace1",
		})
		require.NoError(t, err)

		// Valid request
		resp, err := server.CreateBarrier(ctx, &gracklepb.CreateBarrierRequest{
			NamespaceName:     "namespace1",
			BarrierName:       "barrier1",
			ExpectedProcesses: 3,
			ExpiresAt:         time.Now().Add(10 * time.Minute).UnixNano(),
		})
		require.NoError(t, err)
		require.NotNil(t, resp.Barrier)
		require.Equal(t, "barrier1", resp.Barrier.Name)

		// Invalid request - invalid namespace name
		_, err = server.CreateBarrier(ctx, &gracklepb.CreateBarrierRequest{
			NamespaceName:     "invalid@namespace",
			BarrierName:       "barrier2",
			ExpectedProcesses: 3,
			ExpiresAt:         time.Now().Add(10 * time.Minute).UnixNano(),
		})
		require.Error(t, err)

		// Invalid request - expected processes zero
		_, err = server.CreateBarrier(ctx, &gracklepb.CreateBarrierRequest{
			NamespaceName:     "namespace1",
			BarrierName:       "barrier3",
			ExpectedProcesses: 0,
			ExpiresAt:         time.Now().Add(10 * time.Minute).UnixNano(),
		})
		require.Error(t, err)
	})
}

func TestUpdateBarrier(t *testing.T) {
	t.Run("validation", func(t *testing.T) {
		server := setupGrackleApiServer(t)
		ctx := context.Background()

		// Create namespace
		_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
			Name: "namespace1",
		})
		require.NoError(t, err)

		// Create barrier
		createResp, err := server.CreateBarrier(ctx, &gracklepb.CreateBarrierRequest{
			NamespaceName:     "namespace1",
			BarrierName:       "barrier1",
			Description:       "Original description",
			ExpectedProcesses: 3,
			ExpiresAt:         time.Now().Add(10 * time.Minute).UnixNano(),
		})
		require.NoError(t, err)
		require.Equal(t, "Original description", createResp.Barrier.Description)
		require.EqualValues(t, 3, createResp.Barrier.ExpectedProcesses)

		// Valid request - update barrier
		updateResp, err := server.UpdateBarrier(ctx, &gracklepb.UpdateBarrierRequest{
			NamespaceName:     "namespace1",
			BarrierName:       "barrier1",
			Description:       "Updated description",
			ExpectedProcesses: 5,
		})
		require.NoError(t, err)
		require.NotNil(t, updateResp.Barrier)
		require.Equal(t, "Updated description", updateResp.Barrier.Description)
		require.EqualValues(t, 5, updateResp.Barrier.ExpectedProcesses)

		// Invalid request - invalid namespace name
		_, err = server.UpdateBarrier(ctx, &gracklepb.UpdateBarrierRequest{
			NamespaceName:     "invalid@namespace",
			BarrierName:       "barrier1",
			Description:       "desc",
			ExpectedProcesses: 5,
		})
		require.Error(t, err)

		// Invalid request - expected processes zero
		_, err = server.UpdateBarrier(ctx, &gracklepb.UpdateBarrierRequest{
			NamespaceName:     "namespace1",
			BarrierName:       "barrier1",
			Description:       "desc",
			ExpectedProcesses: 0,
		})
		require.Error(t, err)
	})

	t.Run("update successfully", func(t *testing.T) {
		server := setupGrackleApiServer(t)
		ctx := context.Background()

		// Create namespace
		_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
			Name: "test-namespace",
		})
		require.NoError(t, err)

		// Create barrier
		createResp, err := server.CreateBarrier(ctx, &gracklepb.CreateBarrierRequest{
			NamespaceName:     "test-namespace",
			BarrierName:       "test-barrier",
			Description:       "Original description",
			ExpectedProcesses: 3,
			ExpiresAt:         time.Now().Add(10 * time.Minute).UnixNano(),
		})
		require.NoError(t, err)
		originalCreatedAt := createResp.Barrier.CreatedAt
		originalUpdatedAt := createResp.Barrier.UpdatedAt

		// Update barrier
		time.Sleep(10 * time.Millisecond) // Ensure UpdatedAt will be different
		updateResp, err := server.UpdateBarrier(ctx, &gracklepb.UpdateBarrierRequest{
			NamespaceName:     "test-namespace",
			BarrierName:       "test-barrier",
			Description:       "Updated description",
			ExpectedProcesses: 10,
		})
		require.NoError(t, err)
		require.NotNil(t, updateResp.Barrier)
		require.Equal(t, "test-barrier", updateResp.Barrier.Name)
		require.Equal(t, "Updated description", updateResp.Barrier.Description)
		require.EqualValues(t, 10, updateResp.Barrier.ExpectedProcesses)
		require.Equal(t, originalCreatedAt, updateResp.Barrier.CreatedAt)   // CreatedAt should not change
		require.Greater(t, updateResp.Barrier.UpdatedAt, originalUpdatedAt) // UpdatedAt should increase

		// Get barrier to verify changes persisted
		getResp, err := server.GetBarrier(ctx, &gracklepb.GetBarrierRequest{
			NamespaceName: "test-namespace",
			BarrierName:   "test-barrier",
		})
		require.NoError(t, err)
		require.Equal(t, "Updated description", getResp.Barrier.Description)
		require.EqualValues(t, 10, getResp.Barrier.ExpectedProcesses)
	})

	t.Run("cannot reduce expected_processes below arrived_processes", func(t *testing.T) {
		server := setupGrackleApiServer(t)
		ctx := context.Background()

		// Create namespace
		_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
			Name: "test-namespace",
		})
		require.NoError(t, err)

		// Create barrier with expected_processes = 5
		_, err = server.CreateBarrier(ctx, &gracklepb.CreateBarrierRequest{
			NamespaceName:     "test-namespace",
			BarrierName:       "test-barrier",
			Description:       "Test barrier",
			ExpectedProcesses: 5,
			ExpiresAt:         time.Now().Add(10 * time.Minute).UnixNano(),
		})
		require.NoError(t, err)

		// Have 3 processes arrive at the barrier
		for i := range 3 {
			_, err := server.ArriveAtBarrier(ctx, &gracklepb.ArriveAtBarrierRequest{
				NamespaceName:      "test-namespace",
				BarrierName:        "test-barrier",
				ProcessId:          fmt.Sprintf("process-%d", i),
				ExpectedGeneration: 1,
			})
			require.NoError(t, err)
		}

		// Try to update expected_processes to 2 (less than 3 arrived processes) - should fail
		_, err = server.UpdateBarrier(ctx, &gracklepb.UpdateBarrierRequest{
			NamespaceName:     "test-namespace",
			BarrierName:       "test-barrier",
			Description:       "Updated",
			ExpectedProcesses: 2,
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "there are currently more arrived processes than the new expected processes")

		// Update to expected_processes = 3 (equal to arrived_processes) - should succeed
		updateResp, err := server.UpdateBarrier(ctx, &gracklepb.UpdateBarrierRequest{
			NamespaceName:     "test-namespace",
			BarrierName:       "test-barrier",
			Description:       "Updated",
			ExpectedProcesses: 3,
		})
		require.NoError(t, err)
		require.EqualValues(t, 3, updateResp.Barrier.ExpectedProcesses)

		// Update to expected_processes = 10 (greater than arrived_processes) - should succeed
		updateResp2, err := server.UpdateBarrier(ctx, &gracklepb.UpdateBarrierRequest{
			NamespaceName:     "test-namespace",
			BarrierName:       "test-barrier",
			Description:       "Updated again",
			ExpectedProcesses: 10,
		})
		require.NoError(t, err)
		require.EqualValues(t, 10, updateResp2.Barrier.ExpectedProcesses)
	})

	t.Run("update nonexistent barrier", func(t *testing.T) {
		server := setupGrackleApiServer(t)
		ctx := context.Background()

		// Create namespace
		_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
			Name: "test-namespace",
		})
		require.NoError(t, err)

		// Try to update a barrier that doesn't exist
		_, err = server.UpdateBarrier(ctx, &gracklepb.UpdateBarrierRequest{
			NamespaceName:     "test-namespace",
			BarrierName:       "nonexistent-barrier",
			Description:       "desc",
			ExpectedProcesses: 5,
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "barrier not found")
	})
}

func TestArriveAtBarrier(t *testing.T) {
	t.Run("validation", func(t *testing.T) {
		server := setupGrackleApiServer(t)
		ctx := context.Background()

		// Create namespace first
		_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
			Name: "namespace1",
		})
		require.NoError(t, err)

		// Create a barrier
		_, err = server.CreateBarrier(ctx, &gracklepb.CreateBarrierRequest{
			NamespaceName:     "namespace1",
			BarrierName:       "barrier1",
			ExpectedProcesses: 3,
			ExpiresAt:         time.Now().Add(10 * time.Minute).UnixNano(),
		})
		require.NoError(t, err)

		// Valid request
		_, err = server.ArriveAtBarrier(ctx, &gracklepb.ArriveAtBarrierRequest{
			NamespaceName:      "namespace1",
			BarrierName:        "barrier1",
			ProcessId:          "proc1",
			ExpectedGeneration: 1,
		})
		require.NoError(t, err)

		// Invalid request - invalid namespace name
		_, err = server.ArriveAtBarrier(ctx, &gracklepb.ArriveAtBarrierRequest{
			NamespaceName:      "invalid@namespace",
			BarrierName:        "barrier1",
			ProcessId:          "proc2",
			ExpectedGeneration: 1,
		})
		require.Error(t, err)

		// Invalid request - expected generation zero
		_, err = server.ArriveAtBarrier(ctx, &gracklepb.ArriveAtBarrierRequest{
			NamespaceName:      "namespace1",
			BarrierName:        "barrier1",
			ProcessId:          "proc3",
			ExpectedGeneration: 0,
		})
		require.Error(t, err)
	})
}

func TestListBarrierParticipants(t *testing.T) {
	t.Run("validation", func(t *testing.T) {
		server := setupGrackleApiServer(t)
		ctx := context.Background()

		// Create namespace first
		_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
			Name: "namespace1",
		})
		require.NoError(t, err)

		// Create barrier
		_, err = server.CreateBarrier(ctx, &gracklepb.CreateBarrierRequest{
			NamespaceName:     "namespace1",
			BarrierName:       "barrier1",
			ExpectedProcesses: 3,
			ExpiresAt:         time.Now().Add(10 * time.Minute).UnixNano(),
		})
		require.NoError(t, err)

		// Valid request
		resp, err := server.ListBarrierParticipants(ctx, &gracklepb.ListBarrierParticipantsRequest{
			NamespaceName: "namespace1",
			BarrierName:   "barrier1",
			Generation:    1,
		})
		require.NoError(t, err)
		require.NotNil(t, resp.Participants)

		// Invalid request - invalid namespace name
		_, err = server.ListBarrierParticipants(ctx, &gracklepb.ListBarrierParticipantsRequest{
			NamespaceName: "invalid@namespace",
			BarrierName:   "barrier1",
			Generation:    1,
		})
		require.Error(t, err)
	})

	t.Run("pagination", func(t *testing.T) {
		server := setupGrackleApiServer(t)
		ctx := context.Background()

		// Create namespace
		_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
			Name: "test-namespace",
		})
		require.NoError(t, err)

		// Create barrier expecting 25 processes
		_, err = server.CreateBarrier(ctx, &gracklepb.CreateBarrierRequest{
			NamespaceName:     "test-namespace",
			BarrierName:       "test-barrier",
			ExpectedProcesses: 25,
			ExpiresAt:         time.Now().Add(10 * time.Minute).UnixNano(),
		})
		require.NoError(t, err)

		// Have 25 processes arrive at the barrier
		for i := range 25 {
			_, err := server.ArriveAtBarrier(ctx, &gracklepb.ArriveAtBarrierRequest{
				NamespaceName:      "test-namespace",
				BarrierName:        "test-barrier",
				ProcessId:          fmt.Sprintf("process_%03d", i+1),
				ExpectedGeneration: 1,
			})
			require.NoError(t, err)
		}

		// Test forward pagination through 3 pages
		var allParticipants []*gracklepb.BarrierParticipant

		// Page 1: Get first 10 participants
		resp1, err := server.ListBarrierParticipants(ctx, &gracklepb.ListBarrierParticipantsRequest{
			NamespaceName: "test-namespace",
			BarrierName:   "test-barrier",
			Generation:    1,
			Limit:         10,
		})
		require.NoError(t, err)
		require.Len(t, resp1.Participants, 10)
		require.NotEmpty(t, resp1.NextPaginationToken)
		require.Empty(t, resp1.PreviousPaginationToken)

		allParticipants = append(allParticipants, resp1.Participants...)

		// Page 2: Get next 10 participants
		resp2, err := server.ListBarrierParticipants(ctx, &gracklepb.ListBarrierParticipantsRequest{
			NamespaceName:   "test-namespace",
			BarrierName:     "test-barrier",
			Generation:      1,
			PaginationToken: resp1.NextPaginationToken,
			Limit:           10,
		})
		require.NoError(t, err)
		require.Len(t, resp2.Participants, 10)
		require.NotEmpty(t, resp2.NextPaginationToken)
		require.NotEmpty(t, resp2.PreviousPaginationToken)

		allParticipants = append(allParticipants, resp2.Participants...)

		// Page 3: Get remaining 5 participants
		resp3, err := server.ListBarrierParticipants(ctx, &gracklepb.ListBarrierParticipantsRequest{
			NamespaceName:   "test-namespace",
			BarrierName:     "test-barrier",
			Generation:      1,
			PaginationToken: resp2.NextPaginationToken,
			Limit:           10,
		})
		require.NoError(t, err)
		require.Len(t, resp3.Participants, 5)
		require.Empty(t, resp3.NextPaginationToken)
		require.NotEmpty(t, resp3.PreviousPaginationToken)

		allParticipants = append(allParticipants, resp3.Participants...)

		// Verify we got all 25 participants
		require.Len(t, allParticipants, 25)

		// Test backward pagination from the last page
		resp4, err := server.ListBarrierParticipants(ctx, &gracklepb.ListBarrierParticipantsRequest{
			NamespaceName:   "test-namespace",
			BarrierName:     "test-barrier",
			Generation:      1,
			PaginationToken: resp3.PreviousPaginationToken,
			Limit:           10,
		})
		require.NoError(t, err)
		require.Len(t, resp4.Participants, 10)
		require.NotEmpty(t, resp4.NextPaginationToken)
		require.NotEmpty(t, resp4.PreviousPaginationToken)
	})
}

func TestWaitAtBarrier(t *testing.T) {
	t.Run("validation", func(t *testing.T) {
		server := setupGrackleApiServer(t)
		ctx := context.Background()

		// Create namespace
		_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
			Name: "namespace1",
		})
		require.NoError(t, err)

		// Create barrier
		_, err = server.CreateBarrier(ctx, &gracklepb.CreateBarrierRequest{
			NamespaceName:     "namespace1",
			BarrierName:       "barrier1",
			ExpectedProcesses: 3,
			ExpiresAt:         time.Now().Add(10 * time.Minute).UnixNano(),
		})
		require.NoError(t, err)

		// Valid request
		_, err = server.WaitAtBarrier(ctx, &gracklepb.WaitAtBarrierRequest{
			NamespaceName:      "namespace1",
			BarrierName:        "barrier1",
			ExpectedGeneration: 1,
			TimeoutSeconds:     1,
		})
		require.NoError(t, err)

		// Invalid request - invalid namespace name
		_, err = server.WaitAtBarrier(ctx, &gracklepb.WaitAtBarrierRequest{
			NamespaceName:      "invalid@namespace",
			BarrierName:        "barrier1",
			ExpectedGeneration: 1,
			TimeoutSeconds:     1,
		})
		require.Error(t, err)

		// Invalid request - expected generation zero
		_, err = server.WaitAtBarrier(ctx, &gracklepb.WaitAtBarrierRequest{
			NamespaceName:      "namespace1",
			BarrierName:        "barrier1",
			ExpectedGeneration: 0,
			TimeoutSeconds:     1,
		})
		require.Error(t, err)

		// Invalid request - timeout too high
		_, err = server.WaitAtBarrier(ctx, &gracklepb.WaitAtBarrierRequest{
			NamespaceName:      "namespace1",
			BarrierName:        "barrier1",
			ExpectedGeneration: 1,
			TimeoutSeconds:     301,
		})
		require.Error(t, err)
	})

	t.Run("completion", func(t *testing.T) {
		server := setupGrackleApiServer(t)
		ctx := context.Background()

		// Create namespace
		_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
			Name: "test-namespace",
		})
		require.NoError(t, err)

		// Create barrier expecting 3 processes
		_, err = server.CreateBarrier(ctx, &gracklepb.CreateBarrierRequest{
			NamespaceName:     "test-namespace",
			BarrierName:       "test-barrier",
			ExpectedProcesses: 3,
			ExpiresAt:         time.Now().Add(10 * time.Minute).UnixNano(),
		})
		require.NoError(t, err)

		// Test: Wait for a barrier that completes within timeout
		go func() {
			time.Sleep(30 * time.Millisecond)
			// First process arrives
			_, _ = server.ArriveAtBarrier(ctx, &gracklepb.ArriveAtBarrierRequest{
				NamespaceName:      "test-namespace",
				BarrierName:        "test-barrier",
				ProcessId:          "proc1",
				ExpectedGeneration: 1,
			})
			time.Sleep(30 * time.Millisecond)
			// Second process arrives
			_, _ = server.ArriveAtBarrier(ctx, &gracklepb.ArriveAtBarrierRequest{
				NamespaceName:      "test-namespace",
				BarrierName:        "test-barrier",
				ProcessId:          "proc2",
				ExpectedGeneration: 1,
			})
			time.Sleep(30 * time.Millisecond)
			// Third process arrives
			_, _ = server.ArriveAtBarrier(ctx, &gracklepb.ArriveAtBarrierRequest{
				NamespaceName:      "test-namespace",
				BarrierName:        "test-barrier",
				ProcessId:          "proc3",
				ExpectedGeneration: 1,
			})
		}()

		// Wait for barrier with 1 second timeout
		resp, err := server.WaitAtBarrier(ctx, &gracklepb.WaitAtBarrierRequest{
			NamespaceName:      "test-namespace",
			BarrierName:        "test-barrier",
			ExpectedGeneration: 1,
			TimeoutSeconds:     1,
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.True(t, resp.AllArrived)
		require.False(t, resp.TimedOut)
		require.Equal(t, uint64(3), resp.Barrier.ExpectedProcesses)
		require.Equal(t, uint64(3), resp.Barrier.ArrivedProcesses)
		require.Equal(t, uint64(1), resp.Barrier.Generation)
		require.Equal(t, uint64(2), resp.NextGeneration)
	})

	t.Run("timeout", func(t *testing.T) {
		server := setupGrackleApiServer(t)
		ctx := context.Background()

		// Create namespace
		_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
			Name: "test-namespace",
		})
		require.NoError(t, err)

		// Create barrier that won't complete
		_, err = server.CreateBarrier(ctx, &gracklepb.CreateBarrierRequest{
			NamespaceName:     "test-namespace",
			BarrierName:       "test-barrier-timeout",
			ExpectedProcesses: 5,
			ExpiresAt:         time.Now().Add(10 * time.Minute).UnixNano(),
		})
		require.NoError(t, err)

		// Only 2 processes arrive
		_, err = server.ArriveAtBarrier(ctx, &gracklepb.ArriveAtBarrierRequest{
			NamespaceName:      "test-namespace",
			BarrierName:        "test-barrier-timeout",
			ProcessId:          "p1",
			ExpectedGeneration: 1,
		})
		require.NoError(t, err)

		_, err = server.ArriveAtBarrier(ctx, &gracklepb.ArriveAtBarrierRequest{
			NamespaceName:      "test-namespace",
			BarrierName:        "test-barrier-timeout",
			ProcessId:          "p2",
			ExpectedGeneration: 1,
		})
		require.NoError(t, err)

		// Wait with 1 second timeout (should timeout)
		resp, err := server.WaitAtBarrier(ctx, &gracklepb.WaitAtBarrierRequest{
			NamespaceName:      "test-namespace",
			BarrierName:        "test-barrier-timeout",
			ExpectedGeneration: 1,
			TimeoutSeconds:     1,
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.False(t, resp.AllArrived)
		require.True(t, resp.TimedOut)
		require.Equal(t, uint64(5), resp.Barrier.ExpectedProcesses)
		require.Equal(t, uint64(2), resp.Barrier.ArrivedProcesses)
	})
}
