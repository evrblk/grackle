package preview

import (
	"context"
	"fmt"
	"testing"

	gracklepb "github.com/evrblk/evrblk-go/grackle/preview"
	"github.com/evrblk/grackle/pkg/server/preview"
	"github.com/stretchr/testify/require"
)

func TestCreateSemaphore(t *testing.T) {
	t.Run("validation", func(t *testing.T) {
		server := setupGrackleApiServer(t)
		ctx := context.Background()

		// Create namespace
		_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
			Name: "namespace1",
		})
		require.NoError(t, err)

		// Valid request
		resp, err := server.CreateSemaphore(ctx, &gracklepb.CreateSemaphoreRequest{
			NamespaceName: "namespace1",
			SemaphoreName: "semaphore1",
			Permits:       5,
		})
		require.NoError(t, err)
		require.NotNil(t, resp.Semaphore)

		// Invalid request - invalid namespace name
		_, err = server.CreateSemaphore(ctx, &gracklepb.CreateSemaphoreRequest{
			NamespaceName: "invalid@namespace",
			SemaphoreName: "semaphore1",
			Permits:       5,
		})
		require.Error(t, err)
	})

	t.Run("max_size_validation", func(t *testing.T) {
		server := setupGrackleApiServer(t)
		ctx := context.Background()

		// Create namespace
		_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
			Name: "namespace1",
		})
		require.NoError(t, err)

		// Test valid semaphore size (within limits)
		resp, err := server.CreateSemaphore(ctx, &gracklepb.CreateSemaphoreRequest{
			NamespaceName: "namespace1",
			SemaphoreName: "semaphore1",
			Permits:       uint64(preview.DefaultServiceLimits.MaxNumberOfSemaphoreHolders), // Max allowed by account limits
		})
		require.NoError(t, err)
		require.NotNil(t, resp.Semaphore)
		require.EqualValues(t, preview.DefaultServiceLimits.MaxNumberOfSemaphoreHolders, resp.Semaphore.Permits)

		// Test semaphore size exceeding account limits
		_, err = server.CreateSemaphore(ctx, &gracklepb.CreateSemaphoreRequest{
			NamespaceName: "namespace1",
			SemaphoreName: "semaphore2",
			Permits:       uint64(preview.DefaultServiceLimits.MaxNumberOfSemaphoreHolders + 1), // Exceeds MaxNumberOfSemaphoreHolders
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), fmt.Sprintf("semaphore size is too big, max: %d", preview.DefaultServiceLimits.MaxNumberOfSemaphoreHolders))
	})
}

func TestGetSemaphore(t *testing.T) {
	t.Run("validation", func(t *testing.T) {
		server := setupGrackleApiServer(t)
		ctx := context.Background()

		// Create namespace
		_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
			Name: "namespace1",
		})
		require.NoError(t, err)

		// Create semaphore
		_, err = server.CreateSemaphore(ctx, &gracklepb.CreateSemaphoreRequest{
			NamespaceName: "namespace1",
			SemaphoreName: "semaphore1",
			Permits:       5,
		})
		require.NoError(t, err)

		// Valid request
		resp, err := server.GetSemaphore(ctx, &gracklepb.GetSemaphoreRequest{
			NamespaceName: "namespace1",
			SemaphoreName: "semaphore1",
		})
		require.NoError(t, err)
		require.NotNil(t, resp.Semaphore)

		// Invalid request - invalid namespace name
		_, err = server.GetSemaphore(ctx, &gracklepb.GetSemaphoreRequest{
			NamespaceName: "invalid@namespace",
			SemaphoreName: "semaphore1",
		})
		require.Error(t, err)
	})
}

func TestAcquireSemaphore(t *testing.T) {
	t.Run("validation", func(t *testing.T) {
		server := setupGrackleApiServer(t)
		ctx := context.Background()

		// Create namespace
		_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
			Name: "namespace1",
		})
		require.NoError(t, err)

		// Create semaphore
		_, err = server.CreateSemaphore(ctx, &gracklepb.CreateSemaphoreRequest{
			NamespaceName: "namespace1",
			SemaphoreName: "semaphore1",
			Permits:       5,
		})
		require.NoError(t, err)

		// Create lease
		resp1, err := server.CreateSemaphoreLease(ctx, &gracklepb.CreateSemaphoreLeaseRequest{
			NamespaceName: "namespace1",
			TtlSeconds:    30,
			ProcessId:     "process_1",
		})
		require.NoError(t, err)

		// Valid request
		resp2, err := server.AcquireSemaphore(ctx, &gracklepb.AcquireSemaphoreRequest{
			NamespaceName:  "namespace1",
			SemaphoreName:  "semaphore1",
			LeaseId:        resp1.Lease.LeaseId,
			Weight:         1,
			TimeoutSeconds: 60,
		})
		require.NoError(t, err)
		require.NotNil(t, resp2)

		// Invalid request - missing lease id
		_, err = server.AcquireSemaphore(ctx, &gracklepb.AcquireSemaphoreRequest{
			NamespaceName:  "namespace1",
			SemaphoreName:  "semaphore1",
			Weight:         1,
			TimeoutSeconds: 60,
		})
		require.Error(t, err)
	})
}

func TestReleaseSemaphore(t *testing.T) {
	t.Run("validation", func(t *testing.T) {
		server := setupGrackleApiServer(t)
		ctx := context.Background()

		// Create namespace
		_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
			Name: "namespace1",
		})
		require.NoError(t, err)

		// Create semaphore
		_, err = server.CreateSemaphore(ctx, &gracklepb.CreateSemaphoreRequest{
			NamespaceName: "namespace1",
			SemaphoreName: "semaphore1",
			Permits:       5,
		})
		require.NoError(t, err)

		// Create lease
		resp1, err := server.CreateSemaphoreLease(ctx, &gracklepb.CreateSemaphoreLeaseRequest{
			NamespaceName: "namespace1",
			TtlSeconds:    30,
			ProcessId:     "process_1",
		})
		require.NoError(t, err)

		// Valid request
		resp2, err := server.ReleaseSemaphore(ctx, &gracklepb.ReleaseSemaphoreRequest{
			NamespaceName: "namespace1",
			SemaphoreName: "semaphore1",
			LeaseId:       resp1.Lease.LeaseId,
		})
		require.NoError(t, err)
		require.NotNil(t, resp2)

		// Invalid request - missing lease id
		_, err = server.ReleaseSemaphore(ctx, &gracklepb.ReleaseSemaphoreRequest{
			NamespaceName: "namespace1",
			SemaphoreName: "semaphore1",
		})
		require.Error(t, err)
	})
}

func TestUpdateSemaphore(t *testing.T) {
	t.Run("validation", func(t *testing.T) {
		server := setupGrackleApiServer(t)
		ctx := context.Background()

		// Create namespace
		_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
			Name: "namespace1",
		})
		require.NoError(t, err)

		// Create semaphore
		_, err = server.CreateSemaphore(ctx, &gracklepb.CreateSemaphoreRequest{
			NamespaceName: "namespace1",
			SemaphoreName: "semaphore1",
			Permits:       5,
		})
		require.NoError(t, err)

		// Valid request
		resp, err := server.UpdateSemaphore(ctx, &gracklepb.UpdateSemaphoreRequest{
			NamespaceName: "namespace1",
			SemaphoreName: "semaphore1",
			Permits:       10,
		})
		require.NoError(t, err)
		require.NotNil(t, resp.Semaphore)

		// Invalid request - invalid namespace name
		_, err = server.UpdateSemaphore(ctx, &gracklepb.UpdateSemaphoreRequest{
			NamespaceName: "invalid@namespace",
			SemaphoreName: "semaphore1",
			Permits:       10,
		})
		require.Error(t, err)
	})

	t.Run("max_size_validation", func(t *testing.T) {
		server := setupGrackleApiServer(t)
		ctx := context.Background()

		// Create namespace
		_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
			Name: "namespace1",
		})
		require.NoError(t, err)

		// Create semaphore
		_, err = server.CreateSemaphore(ctx, &gracklepb.CreateSemaphoreRequest{
			NamespaceName: "namespace1",
			SemaphoreName: "semaphore1",
			Permits:       10,
		})
		require.NoError(t, err)

		// Test valid update (within limits)
		resp, err := server.UpdateSemaphore(ctx, &gracklepb.UpdateSemaphoreRequest{
			NamespaceName: "namespace1",
			SemaphoreName: "semaphore1",
			Permits:       uint64(preview.DefaultServiceLimits.MaxNumberOfSemaphoreHolders), // Max allowed by account limits
		})
		require.NoError(t, err)
		require.NotNil(t, resp.Semaphore)
		require.EqualValues(t, preview.DefaultServiceLimits.MaxNumberOfSemaphoreHolders, resp.Semaphore.Permits)

		// Test update exceeding account limits
		_, err = server.UpdateSemaphore(ctx, &gracklepb.UpdateSemaphoreRequest{
			NamespaceName: "namespace1",
			SemaphoreName: "semaphore1",
			Permits:       uint64(preview.DefaultServiceLimits.MaxNumberOfSemaphoreHolders + 1), // Exceeds MaxNumberOfSemaphoreHolders
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), fmt.Sprintf("semaphore size is too big, max: %d", preview.DefaultServiceLimits.MaxNumberOfSemaphoreHolders))
	})
}

func TestDeleteSemaphore(t *testing.T) {
	t.Run("validation", func(t *testing.T) {
		server := setupGrackleApiServer(t)
		ctx := context.Background()

		// Create namespace
		_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
			Name: "namespace1",
		})
		require.NoError(t, err)

		// Create semaphore
		_, err = server.CreateSemaphore(ctx, &gracklepb.CreateSemaphoreRequest{
			NamespaceName: "namespace1",
			SemaphoreName: "semaphore1",
			Permits:       5,
		})
		require.NoError(t, err)

		// Valid request
		_, err = server.DeleteSemaphore(ctx, &gracklepb.DeleteSemaphoreRequest{
			NamespaceName: "namespace1",
			SemaphoreName: "semaphore1",
		})
		require.NoError(t, err)

		// Invalid request - invalid namespace name
		_, err = server.DeleteSemaphore(ctx, &gracklepb.DeleteSemaphoreRequest{
			NamespaceName: "invalid@namespace",
			SemaphoreName: "semaphore1",
		})
		require.Error(t, err)
	})
}

func TestListSemaphores(t *testing.T) {
	t.Run("validation", func(t *testing.T) {
		server := setupGrackleApiServer(t)
		ctx := context.Background()

		// Create namespace
		_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
			Name: "namespace1",
		})
		require.NoError(t, err)

		// Valid request
		resp, err := server.ListSemaphores(ctx, &gracklepb.ListSemaphoresRequest{
			NamespaceName: "namespace1",
		})
		require.NoError(t, err)
		require.NotNil(t, resp.Semaphores)

		// Invalid request - invalid namespace name
		_, err = server.ListSemaphores(ctx, &gracklepb.ListSemaphoresRequest{
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

		// Create 25 semaphores to test pagination (3 pages with limit 10)
		for i := range 25 {
			_, err := server.CreateSemaphore(ctx, &gracklepb.CreateSemaphoreRequest{
				NamespaceName: "test-namespace",
				SemaphoreName: fmt.Sprintf("semaphore_%03d", i+1),
				Description:   fmt.Sprintf("Test semaphore %d", i+1),
				Permits:       10,
			})
			require.NoError(t, err)
		}

		// Test forward pagination through 3 pages
		var allSemaphores []*gracklepb.Semaphore

		// Page 1: Get first 10 semaphores
		resp1, err := server.ListSemaphores(ctx, &gracklepb.ListSemaphoresRequest{
			NamespaceName: "test-namespace",
			Limit:         10,
		})
		require.NoError(t, err)
		require.Len(t, resp1.Semaphores, 10)
		require.NotEmpty(t, resp1.NextPaginationToken)
		require.Empty(t, resp1.PreviousPaginationToken)

		allSemaphores = append(allSemaphores, resp1.Semaphores...)

		// Page 2: Get next 10 semaphores
		resp2, err := server.ListSemaphores(ctx, &gracklepb.ListSemaphoresRequest{
			NamespaceName:   "test-namespace",
			PaginationToken: resp1.NextPaginationToken,
			Limit:           10,
		})
		require.NoError(t, err)
		require.Len(t, resp2.Semaphores, 10)
		require.NotEmpty(t, resp2.NextPaginationToken)
		require.NotEmpty(t, resp2.PreviousPaginationToken)

		allSemaphores = append(allSemaphores, resp2.Semaphores...)

		// Page 3: Get remaining 5 semaphores
		resp3, err := server.ListSemaphores(ctx, &gracklepb.ListSemaphoresRequest{
			NamespaceName:   "test-namespace",
			PaginationToken: resp2.NextPaginationToken,
			Limit:           10,
		})
		require.NoError(t, err)
		require.Len(t, resp3.Semaphores, 5)
		require.Empty(t, resp3.NextPaginationToken)
		require.NotEmpty(t, resp3.PreviousPaginationToken)

		allSemaphores = append(allSemaphores, resp3.Semaphores...)

		// Verify we got all 25 semaphores
		require.Len(t, allSemaphores, 25)

		// Test backward pagination from the last page
		resp4, err := server.ListSemaphores(ctx, &gracklepb.ListSemaphoresRequest{
			NamespaceName:   "test-namespace",
			PaginationToken: resp3.PreviousPaginationToken,
			Limit:           10,
		})
		require.NoError(t, err)
		require.Len(t, resp4.Semaphores, 10)
		require.NotEmpty(t, resp4.NextPaginationToken)
		require.NotEmpty(t, resp4.PreviousPaginationToken)
	})
}

func TestListSemaphoreHolders(t *testing.T) {
	t.Run("validation", func(t *testing.T) {
		server := setupGrackleApiServer(t)
		ctx := context.Background()

		// Create namespace
		_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
			Name: "namespace1",
		})
		require.NoError(t, err)

		// Create semaphore
		_, err = server.CreateSemaphore(ctx, &gracklepb.CreateSemaphoreRequest{
			NamespaceName: "namespace1",
			SemaphoreName: "semaphore1",
			Permits:       10,
		})
		require.NoError(t, err)

		// Valid request
		resp, err := server.ListSemaphoreHolders(ctx, &gracklepb.ListSemaphoreHoldersRequest{
			NamespaceName: "namespace1",
			SemaphoreName: "semaphore1",
		})
		require.NoError(t, err)
		require.NotNil(t, resp.Holders)

		// Invalid request - invalid namespace name
		_, err = server.ListSemaphoreHolders(ctx, &gracklepb.ListSemaphoreHoldersRequest{
			NamespaceName: "invalid@namespace",
			SemaphoreName: "semaphore1",
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

		// Create a semaphore with 25 permits
		_, err = server.CreateSemaphore(ctx, &gracklepb.CreateSemaphoreRequest{
			NamespaceName: "test-namespace",
			SemaphoreName: "test-semaphore",
			Permits:       25,
		})
		require.NoError(t, err)

		// Acquire 25 permits from different processes
		for i := range 25 {
			// Create lease
			resp1, err := server.CreateSemaphoreLease(ctx, &gracklepb.CreateSemaphoreLeaseRequest{
				NamespaceName: "test-namespace",
				TtlSeconds:    30,
				ProcessId:     fmt.Sprintf("process_%03d", i+1),
			})
			require.NoError(t, err)

			// Acquire semaphore
			resp2, err := server.AcquireSemaphore(ctx, &gracklepb.AcquireSemaphoreRequest{
				NamespaceName:  "test-namespace",
				SemaphoreName:  "test-semaphore",
				LeaseId:        resp1.Lease.LeaseId,
				Weight:         1,
				TimeoutSeconds: 60,
			})
			require.NoError(t, err)
			require.True(t, resp2.Success)
		}

		// Test forward pagination through 3 pages
		var allHolders []*gracklepb.SemaphoreHolder

		// Page 1: Get first 10 holders
		resp2, err := server.ListSemaphoreHolders(ctx, &gracklepb.ListSemaphoreHoldersRequest{
			NamespaceName: "test-namespace",
			SemaphoreName: "test-semaphore",
			Limit:         10,
		})
		require.NoError(t, err)
		require.Len(t, resp2.Holders, 10)
		require.NotEmpty(t, resp2.NextPaginationToken)
		require.Empty(t, resp2.PreviousPaginationToken)

		allHolders = append(allHolders, resp2.Holders...)

		// Page 2: Get next 10 holders
		resp3, err := server.ListSemaphoreHolders(ctx, &gracklepb.ListSemaphoreHoldersRequest{
			NamespaceName:   "test-namespace",
			SemaphoreName:   "test-semaphore",
			PaginationToken: resp2.NextPaginationToken,
			Limit:           10,
		})
		require.NoError(t, err)
		require.Len(t, resp3.Holders, 10)
		require.NotEmpty(t, resp3.NextPaginationToken)
		require.NotEmpty(t, resp3.PreviousPaginationToken)

		allHolders = append(allHolders, resp3.Holders...)

		// Page 3: Get remaining 5 holders
		resp4, err := server.ListSemaphoreHolders(ctx, &gracklepb.ListSemaphoreHoldersRequest{
			NamespaceName:   "test-namespace",
			SemaphoreName:   "test-semaphore",
			PaginationToken: resp3.NextPaginationToken,
			Limit:           10,
		})
		require.NoError(t, err)
		require.Len(t, resp4.Holders, 5)
		require.Empty(t, resp4.NextPaginationToken)
		require.NotEmpty(t, resp4.PreviousPaginationToken)

		allHolders = append(allHolders, resp4.Holders...)

		// Verify we got all 25 holders
		require.Len(t, allHolders, 25)

		// Test backward pagination from the last page
		resp5, err := server.ListSemaphoreHolders(ctx, &gracklepb.ListSemaphoreHoldersRequest{
			NamespaceName:   "test-namespace",
			SemaphoreName:   "test-semaphore",
			PaginationToken: resp4.PreviousPaginationToken,
			Limit:           10,
		})
		require.NoError(t, err)
		require.Len(t, resp5.Holders, 10)
		require.NotEmpty(t, resp5.NextPaginationToken)
		require.NotEmpty(t, resp5.PreviousPaginationToken)
	})
}

func TestCreateSemaphoreLease(t *testing.T) {
	t.Run("validation", func(t *testing.T) {
		server := setupGrackleApiServer(t)
		ctx := context.Background()

		// Create namespace
		_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
			Name: "namespace1",
		})
		require.NoError(t, err)

		// Valid request
		resp, err := server.CreateSemaphoreLease(ctx, &gracklepb.CreateSemaphoreLeaseRequest{
			NamespaceName: "namespace1",
			ProcessId:     "process1",
			TtlSeconds:    60,
		})
		require.NoError(t, err)
		require.NotNil(t, resp.Lease)
		require.Equal(t, "process1", resp.Lease.ProcessId)
		require.NotEmpty(t, resp.Lease.LeaseId)
		require.Greater(t, resp.Lease.ExpiresAt, resp.Lease.CreatedAt)

		// Invalid request - invalid namespace name
		_, err = server.CreateSemaphoreLease(ctx, &gracklepb.CreateSemaphoreLeaseRequest{
			NamespaceName: "invalid@namespace",
			ProcessId:     "process1",
			TtlSeconds:    60,
		})
		require.Error(t, err)

		// Invalid request - namespace not found
		_, err = server.CreateSemaphoreLease(ctx, &gracklepb.CreateSemaphoreLeaseRequest{
			NamespaceName: "nonexistent",
			ProcessId:     "process1",
			TtlSeconds:    60,
		})
		require.Error(t, err)
	})
}

func TestRevokeSemaphoreLease(t *testing.T) {
	t.Run("validation", func(t *testing.T) {
		server := setupGrackleApiServer(t)
		ctx := context.Background()

		// Create namespace
		_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
			Name: "namespace1",
		})
		require.NoError(t, err)

		// Create lease
		resp1, err := server.CreateSemaphoreLease(ctx, &gracklepb.CreateSemaphoreLeaseRequest{
			NamespaceName: "namespace1",
			ProcessId:     "process1",
			TtlSeconds:    60,
		})
		require.NoError(t, err)

		// Valid request
		_, err = server.RevokeSemaphoreLease(ctx, &gracklepb.RevokeSemaphoreLeaseRequest{
			NamespaceName: "namespace1",
			LeaseId:       resp1.Lease.LeaseId,
		})
		require.NoError(t, err)

		// Invalid request - invalid namespace name
		_, err = server.RevokeSemaphoreLease(ctx, &gracklepb.RevokeSemaphoreLeaseRequest{
			NamespaceName: "invalid@namespace",
			LeaseId:       resp1.Lease.LeaseId,
		})
		require.Error(t, err)

		// Invalid request - invalid lease ID format
		_, err = server.RevokeSemaphoreLease(ctx, &gracklepb.RevokeSemaphoreLeaseRequest{
			NamespaceName: "namespace1",
			LeaseId:       "invalid_lease_id",
		})
		require.Error(t, err)
	})
}

func TestRefreshSemaphoreLease(t *testing.T) {
	t.Run("validation", func(t *testing.T) {
		server := setupGrackleApiServer(t)
		ctx := context.Background()

		// Create namespace
		_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
			Name: "namespace1",
		})
		require.NoError(t, err)

		// Create lease
		resp1, err := server.CreateSemaphoreLease(ctx, &gracklepb.CreateSemaphoreLeaseRequest{
			NamespaceName: "namespace1",
			ProcessId:     "process1",
			TtlSeconds:    60,
		})
		require.NoError(t, err)

		// Valid request
		resp2, err := server.RefreshSemaphoreLease(ctx, &gracklepb.RefreshSemaphoreLeaseRequest{
			NamespaceName: "namespace1",
			LeaseId:       resp1.Lease.LeaseId,
			TtlSeconds:    120,
		})
		require.NoError(t, err)
		require.NotNil(t, resp2.Lease)
		require.Equal(t, resp1.Lease.LeaseId, resp2.Lease.LeaseId)
		require.Equal(t, "process1", resp2.Lease.ProcessId)

		// Invalid request - invalid namespace name
		_, err = server.RefreshSemaphoreLease(ctx, &gracklepb.RefreshSemaphoreLeaseRequest{
			NamespaceName: "invalid@namespace",
			LeaseId:       resp1.Lease.LeaseId,
			TtlSeconds:    120,
		})
		require.Error(t, err)

		// Invalid request - invalid lease ID format
		_, err = server.RefreshSemaphoreLease(ctx, &gracklepb.RefreshSemaphoreLeaseRequest{
			NamespaceName: "namespace1",
			LeaseId:       "invalid_lease_id",
			TtlSeconds:    120,
		})
		require.Error(t, err)
	})
}

func TestListSemaphoreLeases(t *testing.T) {
	t.Run("validation", func(t *testing.T) {
		server := setupGrackleApiServer(t)
		ctx := context.Background()

		// Create namespace
		_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
			Name: "namespace1",
		})
		require.NoError(t, err)

		// Create multiple leases
		for i := range 5 {
			_, err := server.CreateSemaphoreLease(ctx, &gracklepb.CreateSemaphoreLeaseRequest{
				NamespaceName: "namespace1",
				ProcessId:     fmt.Sprintf("process%d", i+1),
				TtlSeconds:    60,
			})
			require.NoError(t, err)
		}

		// Valid request
		resp, err := server.ListSemaphoreLeases(ctx, &gracklepb.ListSemaphoreLeasesRequest{
			NamespaceName: "namespace1",
		})
		require.NoError(t, err)
		require.NotNil(t, resp.Leases)
		require.Len(t, resp.Leases, 5)

		// Invalid request - invalid namespace name
		_, err = server.ListSemaphoreLeases(ctx, &gracklepb.ListSemaphoreLeasesRequest{
			NamespaceName: "invalid@namespace",
		})
		require.Error(t, err)
	})

	t.Run("pagination", func(t *testing.T) {
		server := setupGrackleApiServer(t)
		ctx := context.Background()

		// Create namespace
		_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
			Name: "namespace1",
		})
		require.NoError(t, err)

		// Create 25 leases to test pagination
		for i := range 25 {
			_, err := server.CreateSemaphoreLease(ctx, &gracklepb.CreateSemaphoreLeaseRequest{
				NamespaceName: "namespace1",
				ProcessId:     fmt.Sprintf("process%03d", i+1),
				TtlSeconds:    60,
			})
			require.NoError(t, err)
		}

		// First page
		resp1, err := server.ListSemaphoreLeases(ctx, &gracklepb.ListSemaphoreLeasesRequest{
			NamespaceName: "namespace1",
			Limit:         10,
		})
		require.NoError(t, err)
		require.Len(t, resp1.Leases, 10)
		require.NotEmpty(t, resp1.NextPaginationToken)

		// Second page
		resp2, err := server.ListSemaphoreLeases(ctx, &gracklepb.ListSemaphoreLeasesRequest{
			NamespaceName:   "namespace1",
			PaginationToken: resp1.NextPaginationToken,
			Limit:           10,
		})
		require.NoError(t, err)
		require.Len(t, resp2.Leases, 10)
		require.NotEmpty(t, resp2.NextPaginationToken)

		// Third page
		resp3, err := server.ListSemaphoreLeases(ctx, &gracklepb.ListSemaphoreLeasesRequest{
			NamespaceName:   "namespace1",
			PaginationToken: resp2.NextPaginationToken,
			Limit:           10,
		})
		require.NoError(t, err)
		require.Len(t, resp3.Leases, 5)
		require.Empty(t, resp3.NextPaginationToken)
	})
}

func TestGetSemaphoreLease(t *testing.T) {
	t.Run("validation", func(t *testing.T) {
		server := setupGrackleApiServer(t)
		ctx := context.Background()

		// Create namespace
		_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
			Name: "namespace1",
		})
		require.NoError(t, err)

		// Create lease
		resp1, err := server.CreateSemaphoreLease(ctx, &gracklepb.CreateSemaphoreLeaseRequest{
			NamespaceName: "namespace1",
			ProcessId:     "process1",
			TtlSeconds:    60,
		})
		require.NoError(t, err)

		// Valid request
		resp2, err := server.GetSemaphoreLease(ctx, &gracklepb.GetSemaphoreLeaseRequest{
			NamespaceName: "namespace1",
			LeaseId:       resp1.Lease.LeaseId,
		})
		require.NoError(t, err)
		require.NotNil(t, resp2.Lease)
		require.Equal(t, resp1.Lease.LeaseId, resp2.Lease.LeaseId)
		require.Equal(t, "process1", resp2.Lease.ProcessId)

		// Invalid request - invalid namespace name
		_, err = server.GetSemaphoreLease(ctx, &gracklepb.GetSemaphoreLeaseRequest{
			NamespaceName: "invalid@namespace",
			LeaseId:       resp1.Lease.LeaseId,
		})
		require.Error(t, err)

		// Invalid request - invalid lease ID format
		_, err = server.GetSemaphoreLease(ctx, &gracklepb.GetSemaphoreLeaseRequest{
			NamespaceName: "namespace1",
			LeaseId:       "invalid_lease_id",
		})
		require.Error(t, err)

		// Invalid request - lease not found
		_, err = server.GetSemaphoreLease(ctx, &gracklepb.GetSemaphoreLeaseRequest{
			NamespaceName: "namespace1",
			LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
		})
		require.Error(t, err)
	})
}
