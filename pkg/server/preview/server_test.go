package preview

import (
	"context"
	"fmt"
	"testing"
	"time"

	gracklepb "github.com/evrblk/evrblk-go/grackle/preview"
	"github.com/evrblk/monstera/store"
	"github.com/evrblk/monstera/utils"
	"github.com/stretchr/testify/require"

	"github.com/evrblk/grackle/pkg/barriers"
	"github.com/evrblk/grackle/pkg/locks"
	"github.com/evrblk/grackle/pkg/monsteragen"
	"github.com/evrblk/grackle/pkg/namespaces"
	"github.com/evrblk/grackle/pkg/semaphores"
	"github.com/evrblk/grackle/pkg/sharding"
	"github.com/evrblk/grackle/pkg/waitgroups"
)

func TestCreateNamespace(t *testing.T) {
	t.Run("validation", func(t *testing.T) {
		server := setupGrackleApiServer(t)
		ctx := context.Background()

		// valid request
		resp, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
			Name:        "namespace1",
			Description: "Test namespace",
		})
		require.NoError(t, err)
		require.NotNil(t, resp.Namespace)

		// invalid request - invalid namespace name
		_, err = server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
			Name:        "invalid@namespace",
			Description: "Test namespace",
		})
		require.Error(t, err)
	})
}

func TestGetNamespace(t *testing.T) {
	t.Run("validation", func(t *testing.T) {
		server := setupGrackleApiServer(t)
		ctx := context.Background()

		// Create namespace first
		_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
			Name: "namespace1",
		})
		require.NoError(t, err)

		// valid request
		resp, err := server.GetNamespace(ctx, &gracklepb.GetNamespaceRequest{
			NamespaceName: "namespace1",
		})
		require.NoError(t, err)
		require.NotNil(t, resp.Namespace)

		// invalid request - invalid namespace name
		_, err = server.GetNamespace(ctx, &gracklepb.GetNamespaceRequest{
			NamespaceName: "invalid@namespace",
		})
		require.Error(t, err)
	})
}

func TestUpdateNamespace(t *testing.T) {
	t.Run("validation", func(t *testing.T) {
		server := setupGrackleApiServer(t)
		ctx := context.Background()

		// Create namespace first
		_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
			Name: "namespace1",
		})
		require.NoError(t, err)

		// valid request
		resp, err := server.UpdateNamespace(ctx, &gracklepb.UpdateNamespaceRequest{
			NamespaceName: "namespace1",
			Description:   "Updated description",
		})
		require.NoError(t, err)
		require.NotNil(t, resp.Namespace)

		// invalid request - invalid namespace name
		_, err = server.UpdateNamespace(ctx, &gracklepb.UpdateNamespaceRequest{
			NamespaceName: "invalid@namespace",
			Description:   "Updated description",
		})
		require.Error(t, err)
	})
}

func TestDeleteNamespace(t *testing.T) {
	t.Run("validation", func(t *testing.T) {
		server := setupGrackleApiServer(t)
		ctx := context.Background()

		// Create namespace first
		_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
			Name: "namespace1",
		})
		require.NoError(t, err)

		// valid request
		_, err = server.DeleteNamespace(ctx, &gracklepb.DeleteNamespaceRequest{
			NamespaceName: "namespace1",
		})
		require.NoError(t, err)

		// invalid request - invalid namespace name
		_, err = server.DeleteNamespace(ctx, &gracklepb.DeleteNamespaceRequest{
			NamespaceName: "invalid@namespace",
		})
		require.Error(t, err)
	})
}

func TestListNamespaces(t *testing.T) {
	t.Run("validation", func(t *testing.T) {
		server := setupGrackleApiServer(t)
		ctx := context.Background()

		// valid request
		resp, err := server.ListNamespaces(ctx, &gracklepb.ListNamespacesRequest{})
		require.NoError(t, err)
		require.NotNil(t, resp.Namespaces)
	})

	t.Run("pagination", func(t *testing.T) {
		server := setupGrackleApiServer(t)
		ctx := context.Background()

		// Create 25 namespaces to test pagination (3 pages with limit 10)
		namespaceNames := make([]string, 25)
		for i := 0; i < 25; i++ {
			namespaceNames[i] = fmt.Sprintf("namespace_%03d", i+1)
			_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
				Name:        namespaceNames[i],
				Description: fmt.Sprintf("Test namespace %d", i+1),
			})
			require.NoError(t, err)
		}

		// Test forward pagination through 3 pages
		var allNamespaces []*gracklepb.Namespace

		// Page 1: Get first 10 namespaces
		resp1, err := server.ListNamespaces(ctx, &gracklepb.ListNamespacesRequest{
			Limit: 10,
		})
		require.NoError(t, err)
		require.Len(t, resp1.Namespaces, 10)
		require.NotEmpty(t, resp1.NextPaginationToken)
		require.Empty(t, resp1.PreviousPaginationToken) // First page has no previous token

		allNamespaces = append(allNamespaces, resp1.Namespaces...)

		// Page 2: Get next 10 namespaces
		resp2, err := server.ListNamespaces(ctx, &gracklepb.ListNamespacesRequest{
			PaginationToken: resp1.NextPaginationToken,
			Limit:           10,
		})
		require.NoError(t, err)
		require.Len(t, resp2.Namespaces, 10)
		require.NotEmpty(t, resp2.NextPaginationToken)
		require.NotEmpty(t, resp2.PreviousPaginationToken) // Middle page has both tokens

		allNamespaces = append(allNamespaces, resp2.Namespaces...)

		// Page 3: Get remaining 5 namespaces
		resp3, err := server.ListNamespaces(ctx, &gracklepb.ListNamespacesRequest{
			PaginationToken: resp2.NextPaginationToken,
			Limit:           10,
		})
		require.NoError(t, err)
		require.Len(t, resp3.Namespaces, 5)         // Should be 5 remaining namespaces
		require.Empty(t, resp3.NextPaginationToken) // Last page has no next token
		require.NotEmpty(t, resp3.PreviousPaginationToken)

		allNamespaces = append(allNamespaces, resp3.Namespaces...)

		// Verify we got all 25 namespaces
		require.Len(t, allNamespaces, 25)

		// Test backward pagination from the last page
		var backwardNamespaces = resp3.Namespaces

		// Go back to page 2
		resp4, err := server.ListNamespaces(ctx, &gracklepb.ListNamespacesRequest{
			PaginationToken: resp3.PreviousPaginationToken,
			Limit:           10,
		})
		require.NoError(t, err)
		require.Len(t, resp4.Namespaces, 10)
		require.NotEmpty(t, resp4.NextPaginationToken)
		require.NotEmpty(t, resp4.PreviousPaginationToken)

		backwardNamespaces = append(backwardNamespaces, resp4.Namespaces...)

		// Go back to page 1
		resp5, err := server.ListNamespaces(ctx, &gracklepb.ListNamespacesRequest{
			PaginationToken: resp4.PreviousPaginationToken,
			Limit:           10,
		})
		require.NoError(t, err)
		require.Len(t, resp5.Namespaces, 10)
		require.NotEmpty(t, resp5.NextPaginationToken)
		require.Empty(t, resp5.PreviousPaginationToken) // First page has no previous token

		backwardNamespaces = append(backwardNamespaces, resp5.Namespaces...)

		// Verify we got all 25 namespaces
		require.Len(t, backwardNamespaces, 25)

		// Test pagination with different limits
		resp6, err := server.ListNamespaces(ctx, &gracklepb.ListNamespacesRequest{
			Limit: 5,
		})
		require.NoError(t, err)
		require.Len(t, resp6.Namespaces, 5)
		require.NotEmpty(t, resp6.NextPaginationToken)

		// Test pagination with invalid token
		_, err = server.ListNamespaces(ctx, &gracklepb.ListNamespacesRequest{
			PaginationToken: "invalid_token",
			Limit:           10,
		})
		require.Error(t, err)

		// Test pagination with zero limit (should use default)
		resp7, err := server.ListNamespaces(ctx, &gracklepb.ListNamespacesRequest{
			Limit: 0,
		})
		require.NoError(t, err)
		require.NotEmpty(t, resp7.Namespaces)
		require.Len(t, resp7.Namespaces, 25)
		require.Empty(t, resp7.NextPaginationToken)

		// Test pagination with negative limit (should use default)
		_, err = server.ListNamespaces(ctx, &gracklepb.ListNamespacesRequest{
			Limit: -1,
		})
		require.Error(t, err)
	})
}

func TestCreateWaitGroup(t *testing.T) {
	t.Run("validation", func(t *testing.T) {
		server := setupGrackleApiServer(t)
		ctx := context.Background()

		// Create namespace first
		_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
			Name: "namespace1",
		})
		require.NoError(t, err)

		// valid request
		resp, err := server.CreateWaitGroup(ctx, &gracklepb.CreateWaitGroupRequest{
			NamespaceName: "namespace1",
			WaitGroupName: "waitgroup1",
			Counter:       1,
		})
		require.NoError(t, err)
		require.NotNil(t, resp.WaitGroup)

		// invalid request - invalid namespace name
		_, err = server.CreateWaitGroup(ctx, &gracklepb.CreateWaitGroupRequest{
			NamespaceName: "invalid@namespace",
			WaitGroupName: "waitgroup1",
			Counter:       1,
		})
		require.Error(t, err)
	})

	t.Run("max_size_validation", func(t *testing.T) {
		server := setupGrackleApiServer(t)
		ctx := context.Background()

		// Create namespace first
		_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
			Name: "namespace1",
		})
		require.NoError(t, err)

		// Test valid wait group size (within limits)
		resp, err := server.CreateWaitGroup(ctx, &gracklepb.CreateWaitGroupRequest{
			NamespaceName: "namespace1",
			WaitGroupName: "waitgroup1",
			Counter:       uint64(defaultServiceLimits.MaxWaitGroupSize), // Max allowed by account limits
		})
		require.NoError(t, err)
		require.NotNil(t, resp.WaitGroup)
		require.EqualValues(t, defaultServiceLimits.MaxWaitGroupSize, resp.WaitGroup.Counter)

		// Test wait group size exceeding account limits
		_, err = server.CreateWaitGroup(ctx, &gracklepb.CreateWaitGroupRequest{
			NamespaceName: "namespace1",
			WaitGroupName: "waitgroup2",
			Counter:       uint64(defaultServiceLimits.MaxWaitGroupSize + 1), // Exceeds MaxWaitGroupSize
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), fmt.Sprintf("wait group size is too big, max: %d", uint64(defaultServiceLimits.MaxWaitGroupSize)))
	})
}

func TestGetWaitGroup(t *testing.T) {
	t.Run("validation", func(t *testing.T) {
		server := setupGrackleApiServer(t)
		ctx := context.Background()

		// Create namespace and wait group first
		_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
			Name: "namespace1",
		})
		require.NoError(t, err)

		_, err = server.CreateWaitGroup(ctx, &gracklepb.CreateWaitGroupRequest{
			NamespaceName: "namespace1",
			WaitGroupName: "waitgroup1",
			Counter:       1,
		})
		require.NoError(t, err)

		// valid request
		resp, err := server.GetWaitGroup(ctx, &gracklepb.GetWaitGroupRequest{
			NamespaceName: "namespace1",
			WaitGroupName: "waitgroup1",
		})
		require.NoError(t, err)
		require.NotNil(t, resp.WaitGroup)

		// invalid request - invalid namespace name
		_, err = server.GetWaitGroup(ctx, &gracklepb.GetWaitGroupRequest{
			NamespaceName: "invalid@namespace",
			WaitGroupName: "waitgroup1",
		})
		require.Error(t, err)
	})
}

func TestAddJobsToWaitGroup(t *testing.T) {
	t.Run("validation", func(t *testing.T) {
		server := setupGrackleApiServer(t)
		ctx := context.Background()

		// Create namespace and wait group first
		_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
			Name: "namespace1",
		})
		require.NoError(t, err)

		_, err = server.CreateWaitGroup(ctx, &gracklepb.CreateWaitGroupRequest{
			NamespaceName: "namespace1",
			WaitGroupName: "waitgroup1",
			Counter:       1,
		})
		require.NoError(t, err)

		// valid request
		resp, err := server.AddJobsToWaitGroup(ctx, &gracklepb.AddJobsToWaitGroupRequest{
			NamespaceName: "namespace1",
			WaitGroupName: "waitgroup1",
			Counter:       5,
		})
		require.NoError(t, err)
		require.NotNil(t, resp.WaitGroup)

		// invalid request - invalid namespace name
		_, err = server.AddJobsToWaitGroup(ctx, &gracklepb.AddJobsToWaitGroupRequest{
			NamespaceName: "invalid@namespace",
			WaitGroupName: "waitgroup1",
			Counter:       5,
		})
		require.Error(t, err)
	})
}

func TestCompleteJobsFromWaitGroup(t *testing.T) {
	t.Run("validation", func(t *testing.T) {
		server := setupGrackleApiServer(t)
		ctx := context.Background()

		// Create namespace and wait group first
		_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
			Name: "namespace1",
		})
		require.NoError(t, err)

		_, err = server.CreateWaitGroup(ctx, &gracklepb.CreateWaitGroupRequest{
			NamespaceName: "namespace1",
			WaitGroupName: "waitgroup1",
			Counter:       1,
		})
		require.NoError(t, err)

		// valid request
		resp, err := server.CompleteJobsFromWaitGroup(ctx, &gracklepb.CompleteJobsFromWaitGroupRequest{
			NamespaceName: "namespace1",
			WaitGroupName: "waitgroup1",
			ProcessIds:    []string{"proc1", "proc2"},
		})
		require.NoError(t, err)
		require.NotNil(t, resp)

		// invalid request - invalid namespace name
		_, err = server.CompleteJobsFromWaitGroup(ctx, &gracklepb.CompleteJobsFromWaitGroupRequest{
			NamespaceName: "invalid@namespace",
			WaitGroupName: "waitgroup1",
			ProcessIds:    []string{"proc1"},
		})
		require.Error(t, err)
	})
}

func TestDeleteWaitGroup(t *testing.T) {
	t.Run("validation", func(t *testing.T) {
		server := setupGrackleApiServer(t)
		ctx := context.Background()

		// Create namespace and wait group first
		_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
			Name: "namespace1",
		})
		require.NoError(t, err)

		_, err = server.CreateWaitGroup(ctx, &gracklepb.CreateWaitGroupRequest{
			NamespaceName: "namespace1",
			WaitGroupName: "waitgroup1",
			Counter:       1,
		})
		require.NoError(t, err)

		// valid request
		_, err = server.DeleteWaitGroup(ctx, &gracklepb.DeleteWaitGroupRequest{
			NamespaceName: "namespace1",
			WaitGroupName: "waitgroup1",
		})
		require.NoError(t, err)

		// invalid request - invalid namespace name
		_, err = server.DeleteWaitGroup(ctx, &gracklepb.DeleteWaitGroupRequest{
			NamespaceName: "invalid@namespace",
			WaitGroupName: "waitgroup1",
		})
		require.Error(t, err)
	})
}

func TestListWaitGroups(t *testing.T) {
	t.Run("validation", func(t *testing.T) {
		server := setupGrackleApiServer(t)
		ctx := context.Background()

		// Create namespace first
		_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
			Name: "namespace1",
		})
		require.NoError(t, err)

		// valid request
		resp, err := server.ListWaitGroups(ctx, &gracklepb.ListWaitGroupsRequest{
			NamespaceName: "namespace1",
		})
		require.NoError(t, err)
		require.NotNil(t, resp.WaitGroups)

		// invalid request - invalid namespace name
		_, err = server.ListWaitGroups(ctx, &gracklepb.ListWaitGroupsRequest{
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

		// Create 25 wait groups to test pagination (3 pages with limit 10)
		for i := 0; i < 25; i++ {
			_, err := server.CreateWaitGroup(ctx, &gracklepb.CreateWaitGroupRequest{
				NamespaceName: "test-namespace",
				WaitGroupName: fmt.Sprintf("waitgroup_%03d", i+1),
				Description:   fmt.Sprintf("Test wait group %d", i+1),
				Counter:       10,
			})
			require.NoError(t, err)
		}

		// Test forward pagination through 3 pages
		var allWaitGroups []*gracklepb.WaitGroup

		// Page 1: Get first 10 wait groups
		resp1, err := server.ListWaitGroups(ctx, &gracklepb.ListWaitGroupsRequest{
			NamespaceName: "test-namespace",
			Limit:         10,
		})
		require.NoError(t, err)
		require.Len(t, resp1.WaitGroups, 10)
		require.NotEmpty(t, resp1.NextPaginationToken)
		require.Empty(t, resp1.PreviousPaginationToken)

		allWaitGroups = append(allWaitGroups, resp1.WaitGroups...)

		// Page 2: Get next 10 wait groups
		resp2, err := server.ListWaitGroups(ctx, &gracklepb.ListWaitGroupsRequest{
			NamespaceName:   "test-namespace",
			PaginationToken: resp1.NextPaginationToken,
			Limit:           10,
		})
		require.NoError(t, err)
		require.Len(t, resp2.WaitGroups, 10)
		require.NotEmpty(t, resp2.NextPaginationToken)
		require.NotEmpty(t, resp2.PreviousPaginationToken)

		allWaitGroups = append(allWaitGroups, resp2.WaitGroups...)

		// Page 3: Get remaining 5 wait groups
		resp3, err := server.ListWaitGroups(ctx, &gracklepb.ListWaitGroupsRequest{
			NamespaceName:   "test-namespace",
			PaginationToken: resp2.NextPaginationToken,
			Limit:           10,
		})
		require.NoError(t, err)
		require.Len(t, resp3.WaitGroups, 5)
		require.Empty(t, resp3.NextPaginationToken)
		require.NotEmpty(t, resp3.PreviousPaginationToken)

		allWaitGroups = append(allWaitGroups, resp3.WaitGroups...)

		// Verify we got all 25 wait groups
		require.Len(t, allWaitGroups, 25)

		// Test backward pagination from the last page
		resp4, err := server.ListWaitGroups(ctx, &gracklepb.ListWaitGroupsRequest{
			NamespaceName:   "test-namespace",
			PaginationToken: resp3.PreviousPaginationToken,
			Limit:           10,
		})
		require.NoError(t, err)
		require.Len(t, resp4.WaitGroups, 10)
		require.NotEmpty(t, resp4.NextPaginationToken)
		require.NotEmpty(t, resp4.PreviousPaginationToken)
	})
}

func TestListWaitGroupJobs(t *testing.T) {
	t.Run("validation", func(t *testing.T) {
		server := setupGrackleApiServer(t)
		ctx := context.Background()

		// Create namespace first
		_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
			Name: "namespace1",
		})
		require.NoError(t, err)

		// Create a wait group
		_, err = server.CreateWaitGroup(ctx, &gracklepb.CreateWaitGroupRequest{
			NamespaceName: "namespace1",
			WaitGroupName: "waitgroup1",
			Counter:       10,
		})
		require.NoError(t, err)

		// valid request
		resp, err := server.ListWaitGroupJobs(ctx, &gracklepb.ListWaitGroupJobsRequest{
			NamespaceName: "namespace1",
			WaitGroupName: "waitgroup1",
		})
		require.NoError(t, err)
		require.NotNil(t, resp.Jobs)

		// invalid request - invalid namespace name
		_, err = server.ListWaitGroupJobs(ctx, &gracklepb.ListWaitGroupJobsRequest{
			NamespaceName: "invalid@namespace",
			WaitGroupName: "waitgroup1",
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

		// Create a wait group with 25 jobs
		_, err = server.CreateWaitGroup(ctx, &gracklepb.CreateWaitGroupRequest{
			NamespaceName: "test-namespace",
			WaitGroupName: "test-waitgroup",
			Counter:       25,
		})
		require.NoError(t, err)

		// Complete all 25 jobs
		processIds := make([]string, 25)
		for i := 0; i < 25; i++ {
			processIds[i] = fmt.Sprintf("process-%d", i+1)
		}
		_, err = server.CompleteJobsFromWaitGroup(ctx, &gracklepb.CompleteJobsFromWaitGroupRequest{
			NamespaceName: "test-namespace",
			WaitGroupName: "test-waitgroup",
			ProcessIds:    processIds,
		})
		require.NoError(t, err)

		// First page - get first 10 jobs
		resp1, err := server.ListWaitGroupJobs(ctx, &gracklepb.ListWaitGroupJobsRequest{
			NamespaceName: "test-namespace",
			WaitGroupName: "test-waitgroup",
			Limit:         10,
		})
		require.NoError(t, err)
		require.Len(t, resp1.Jobs, 10)
		require.NotEmpty(t, resp1.NextPaginationToken)
		require.Empty(t, resp1.PreviousPaginationToken)

		// Second page - get next 10 jobs
		resp2, err := server.ListWaitGroupJobs(ctx, &gracklepb.ListWaitGroupJobsRequest{
			NamespaceName:   "test-namespace",
			WaitGroupName:   "test-waitgroup",
			Limit:           10,
			PaginationToken: resp1.NextPaginationToken,
		})
		require.NoError(t, err)
		require.Len(t, resp2.Jobs, 10)
		require.NotEmpty(t, resp2.NextPaginationToken)
		require.NotEmpty(t, resp2.PreviousPaginationToken)

		// Third page - get remaining 5 jobs
		resp3, err := server.ListWaitGroupJobs(ctx, &gracklepb.ListWaitGroupJobsRequest{
			NamespaceName:   "test-namespace",
			WaitGroupName:   "test-waitgroup",
			Limit:           10,
			PaginationToken: resp2.NextPaginationToken,
		})
		require.NoError(t, err)
		require.Len(t, resp3.Jobs, 5)
		require.Empty(t, resp3.NextPaginationToken)
		require.NotEmpty(t, resp3.PreviousPaginationToken)
	})
}

func TestWaitForWaitGroup(t *testing.T) {
	t.Run("validation", func(t *testing.T) {
		server := setupGrackleApiServer(t)
		ctx := context.Background()

		// Create namespace first
		_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
			Name: "namespace1",
		})
		require.NoError(t, err)

		// Create wait group
		_, err = server.CreateWaitGroup(ctx, &gracklepb.CreateWaitGroupRequest{
			NamespaceName: "namespace1",
			WaitGroupName: "waitgroup1",
			Counter:       10,
		})
		require.NoError(t, err)

		// valid request
		_, err = server.WaitForWaitGroup(ctx, &gracklepb.WaitForWaitGroupRequest{
			NamespaceName:  "namespace1",
			WaitGroupName:  "waitgroup1",
			TimeoutSeconds: 1,
		})
		require.NoError(t, err)

		// invalid request - invalid namespace name
		_, err = server.WaitForWaitGroup(ctx, &gracklepb.WaitForWaitGroupRequest{
			NamespaceName:  "invalid@namespace",
			WaitGroupName:  "waitgroup1",
			TimeoutSeconds: 1,
		})
		require.Error(t, err)

		// invalid request - timeout too high
		_, err = server.WaitForWaitGroup(ctx, &gracklepb.WaitForWaitGroupRequest{
			NamespaceName:  "namespace1",
			WaitGroupName:  "waitgroup1",
			TimeoutSeconds: 301,
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

		// Create wait group with counter=2
		_, err = server.CreateWaitGroup(ctx, &gracklepb.CreateWaitGroupRequest{
			NamespaceName: "test-namespace",
			WaitGroupName: "test-wg",
			Counter:       2,
		})
		require.NoError(t, err)

		// Test: Wait for a wait group that completes within timeout
		go func() {
			time.Sleep(50 * time.Millisecond)
			// Complete first job
			_, _ = server.CompleteJobsFromWaitGroup(ctx, &gracklepb.CompleteJobsFromWaitGroupRequest{
				NamespaceName: "test-namespace",
				WaitGroupName: "test-wg",
				ProcessIds:    []string{"proc1"},
			})
			time.Sleep(50 * time.Millisecond)
			// Complete second job
			_, _ = server.CompleteJobsFromWaitGroup(ctx, &gracklepb.CompleteJobsFromWaitGroupRequest{
				NamespaceName: "test-namespace",
				WaitGroupName: "test-wg",
				ProcessIds:    []string{"proc2"},
			})
		}()

		// Wait for completion with 1 second timeout
		resp, err := server.WaitForWaitGroup(ctx, &gracklepb.WaitForWaitGroupRequest{
			NamespaceName:  "test-namespace",
			WaitGroupName:  "test-wg",
			TimeoutSeconds: 1,
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.True(t, resp.Completed)
		require.False(t, resp.TimedOut)
		require.Equal(t, uint64(2), resp.WaitGroup.Counter)
		require.Equal(t, uint64(2), resp.WaitGroup.Completed)
	})

	t.Run("timeout", func(t *testing.T) {
		server := setupGrackleApiServer(t)
		ctx := context.Background()

		// Create namespace
		_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
			Name: "test-namespace",
		})
		require.NoError(t, err)

		// Create wait group that won't complete
		_, err = server.CreateWaitGroup(ctx, &gracklepb.CreateWaitGroupRequest{
			NamespaceName: "test-namespace",
			WaitGroupName: "test-wg-timeout",
			Counter:       10,
		})
		require.NoError(t, err)

		// Complete only 5 jobs
		_, err = server.CompleteJobsFromWaitGroup(ctx, &gracklepb.CompleteJobsFromWaitGroupRequest{
			NamespaceName: "test-namespace",
			WaitGroupName: "test-wg-timeout",
			ProcessIds:    []string{"p1", "p2", "p3", "p4", "p5"},
		})
		require.NoError(t, err)

		// Wait with 1 second timeout (should timeout)
		resp, err := server.WaitForWaitGroup(ctx, &gracklepb.WaitForWaitGroupRequest{
			NamespaceName:  "test-namespace",
			WaitGroupName:  "test-wg-timeout",
			TimeoutSeconds: 1,
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.False(t, resp.Completed)
		require.True(t, resp.TimedOut)
		require.Equal(t, uint64(10), resp.WaitGroup.Counter)
		require.Equal(t, uint64(5), resp.WaitGroup.Completed)
	})
}

func TestAcquireLock(t *testing.T) {
	t.Run("validation", func(t *testing.T) {
		server := setupGrackleApiServer(t)
		ctx := context.Background()

		// create namespace first
		_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
			Name: "namespace1",
		})
		require.NoError(t, err)

		// valid request
		_, err = server.AcquireLock(ctx, &gracklepb.AcquireLockRequest{
			NamespaceName: "namespace1",
			LockName:      "lock1",
			ProcessId:     "proc1",
			ExpiresAt:     time.Now().UnixNano(),
		})
		require.NoError(t, err)

		// invalid request - missing process id
		_, err = server.AcquireLock(ctx, &gracklepb.AcquireLockRequest{
			NamespaceName: "namespace1",
			LockName:      "lock2",
			ExpiresAt:     time.Now().UnixNano(),
		})
		require.Error(t, err)
	})
}

func TestReleaseLock(t *testing.T) {
	t.Run("validation", func(t *testing.T) {
		server := setupGrackleApiServer(t)
		ctx := context.Background()

		// Create namespace first
		_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
			Name: "namespace1",
		})
		require.NoError(t, err)

		// valid request
		_, err = server.ReleaseLock(ctx, &gracklepb.ReleaseLockRequest{
			NamespaceName: "namespace1",
			LockName:      "lock1",
			ProcessId:     "proc1",
		})
		require.NoError(t, err)

		// invalid request - missing process id
		_, err = server.ReleaseLock(ctx, &gracklepb.ReleaseLockRequest{
			NamespaceName: "namespace1",
			LockName:      "lock1",
		})
		require.Error(t, err)
	})
}

func TestGetLock(t *testing.T) {
	t.Run("validation", func(t *testing.T) {
		server := setupGrackleApiServer(t)
		ctx := context.Background()

		// Create namespace first
		_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
			Name: "namespace1",
		})
		require.NoError(t, err)

		// valid request
		resp, err := server.GetLock(ctx, &gracklepb.GetLockRequest{
			NamespaceName: "namespace1",
			LockName:      "lock1",
		})
		require.NoError(t, err)
		require.NotNil(t, resp.Lock)

		// invalid request - invalid namespace name
		_, err = server.GetLock(ctx, &gracklepb.GetLockRequest{
			NamespaceName: "invalid@namespace",
			LockName:      "lock1",
		})
		require.Error(t, err)
	})
}

func TestDeleteLock(t *testing.T) {
	t.Run("validation", func(t *testing.T) {
		server := setupGrackleApiServer(t)
		ctx := context.Background()

		// Create namespace first
		_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
			Name: "namespace1",
		})
		require.NoError(t, err)

		// valid request
		_, err = server.DeleteLock(ctx, &gracklepb.DeleteLockRequest{
			NamespaceName: "namespace1",
			LockName:      "lock1",
		})
		require.NoError(t, err)

		// invalid request - invalid namespace name
		_, err = server.DeleteLock(ctx, &gracklepb.DeleteLockRequest{
			NamespaceName: "invalid@namespace",
			LockName:      "lock1",
		})
		require.Error(t, err)
	})
}

func TestListLocks(t *testing.T) {
	t.Run("validation", func(t *testing.T) {
		server := setupGrackleApiServer(t)
		ctx := context.Background()

		// Create namespace first
		_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
			Name: "namespace1",
		})
		require.NoError(t, err)

		// valid request
		resp, err := server.ListLocks(ctx, &gracklepb.ListLocksRequest{
			NamespaceName: "namespace1",
		})
		require.NoError(t, err)
		require.NotNil(t, resp.Locks)

		// invalid request - invalid namespace name
		_, err = server.ListLocks(ctx, &gracklepb.ListLocksRequest{
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

		// Create 25 locks to test pagination (3 pages with limit 10)
		for i := 0; i < 25; i++ {
			_, err := server.AcquireLock(ctx, &gracklepb.AcquireLockRequest{
				NamespaceName: "test-namespace",
				LockName:      fmt.Sprintf("lock_%03d", i+1),
				ProcessId:     fmt.Sprintf("process_%03d", i+1),
				ExpiresAt:     time.Now().Add(10 * time.Minute).UnixNano(),
			})
			require.NoError(t, err)
		}

		// Test forward pagination through 3 pages
		var allLocks []*gracklepb.Lock

		// Page 1: Get first 10 locks
		resp1, err := server.ListLocks(ctx, &gracklepb.ListLocksRequest{
			NamespaceName: "test-namespace",
			Limit:         10,
		})
		require.NoError(t, err)
		require.Len(t, resp1.Locks, 10)
		require.NotEmpty(t, resp1.NextPaginationToken)
		require.Empty(t, resp1.PreviousPaginationToken)

		allLocks = append(allLocks, resp1.Locks...)

		// Page 2: Get next 10 locks
		resp2, err := server.ListLocks(ctx, &gracklepb.ListLocksRequest{
			NamespaceName:   "test-namespace",
			PaginationToken: resp1.NextPaginationToken,
			Limit:           10,
		})
		require.NoError(t, err)
		require.Len(t, resp2.Locks, 10)
		require.NotEmpty(t, resp2.NextPaginationToken)
		require.NotEmpty(t, resp2.PreviousPaginationToken)

		allLocks = append(allLocks, resp2.Locks...)

		// Page 3: Get remaining 5 locks
		resp3, err := server.ListLocks(ctx, &gracklepb.ListLocksRequest{
			NamespaceName:   "test-namespace",
			PaginationToken: resp2.NextPaginationToken,
			Limit:           10,
		})
		require.NoError(t, err)
		require.Len(t, resp3.Locks, 5)
		require.Empty(t, resp3.NextPaginationToken)
		require.NotEmpty(t, resp3.PreviousPaginationToken)

		allLocks = append(allLocks, resp3.Locks...)

		// Verify we got all 25 locks
		require.Len(t, allLocks, 25)

		// Test backward pagination from the last page
		resp4, err := server.ListLocks(ctx, &gracklepb.ListLocksRequest{
			NamespaceName:   "test-namespace",
			PaginationToken: resp3.PreviousPaginationToken,
			Limit:           10,
		})
		require.NoError(t, err)
		require.Len(t, resp4.Locks, 10)
		require.NotEmpty(t, resp4.NextPaginationToken)
		require.NotEmpty(t, resp4.PreviousPaginationToken)
	})
}

func TestCreateSemaphore(t *testing.T) {
	t.Run("validation", func(t *testing.T) {
		server := setupGrackleApiServer(t)
		ctx := context.Background()

		// Create namespace first
		_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
			Name: "namespace1",
		})
		require.NoError(t, err)

		// valid request
		resp, err := server.CreateSemaphore(ctx, &gracklepb.CreateSemaphoreRequest{
			NamespaceName: "namespace1",
			SemaphoreName: "semaphore1",
			Permits:       5,
		})
		require.NoError(t, err)
		require.NotNil(t, resp.Semaphore)

		// invalid request - invalid namespace name
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

		// Create namespace first
		_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
			Name: "namespace1",
		})
		require.NoError(t, err)

		// Test valid semaphore size (within limits)
		resp, err := server.CreateSemaphore(ctx, &gracklepb.CreateSemaphoreRequest{
			NamespaceName: "namespace1",
			SemaphoreName: "semaphore1",
			Permits:       uint64(defaultServiceLimits.MaxNumberOfSemaphoreHolders), // Max allowed by account limits
		})
		require.NoError(t, err)
		require.NotNil(t, resp.Semaphore)
		require.EqualValues(t, defaultServiceLimits.MaxNumberOfSemaphoreHolders, resp.Semaphore.Permits)

		// Test semaphore size exceeding account limits
		_, err = server.CreateSemaphore(ctx, &gracklepb.CreateSemaphoreRequest{
			NamespaceName: "namespace1",
			SemaphoreName: "semaphore2",
			Permits:       uint64(defaultServiceLimits.MaxNumberOfSemaphoreHolders + 1), // Exceeds MaxNumberOfSemaphoreHolders
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), fmt.Sprintf("semaphore size is too big, max: %d", defaultServiceLimits.MaxNumberOfSemaphoreHolders))
	})
}

func TestGetSemaphore(t *testing.T) {
	t.Run("validation", func(t *testing.T) {
		server := setupGrackleApiServer(t)
		ctx := context.Background()

		// Create namespace and semaphore first
		_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
			Name: "namespace1",
		})
		require.NoError(t, err)

		_, err = server.CreateSemaphore(ctx, &gracklepb.CreateSemaphoreRequest{
			NamespaceName: "namespace1",
			SemaphoreName: "semaphore1",
			Permits:       5,
		})
		require.NoError(t, err)

		// valid request
		resp, err := server.GetSemaphore(ctx, &gracklepb.GetSemaphoreRequest{
			NamespaceName: "namespace1",
			SemaphoreName: "semaphore1",
		})
		require.NoError(t, err)
		require.NotNil(t, resp.Semaphore)

		// invalid request - invalid namespace name
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

		// Create namespace and semaphore first
		_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
			Name: "namespace1",
		})
		require.NoError(t, err)

		_, err = server.CreateSemaphore(ctx, &gracklepb.CreateSemaphoreRequest{
			NamespaceName: "namespace1",
			SemaphoreName: "semaphore1",
			Permits:       5,
		})
		require.NoError(t, err)

		// valid request
		resp, err := server.AcquireSemaphore(ctx, &gracklepb.AcquireSemaphoreRequest{
			NamespaceName: "namespace1",
			SemaphoreName: "semaphore1",
			ProcessId:     "proc1",
			Weight:        1,
			ExpiresAt:     time.Now().UnixNano(),
		})
		require.NoError(t, err)
		require.NotNil(t, resp)

		// invalid request - missing process id
		_, err = server.AcquireSemaphore(ctx, &gracklepb.AcquireSemaphoreRequest{
			NamespaceName: "namespace1",
			SemaphoreName: "semaphore1",
			Weight:        1,
			ExpiresAt:     time.Now().UnixNano(),
		})
		require.Error(t, err)
	})
}

func TestReleaseSemaphore(t *testing.T) {
	t.Run("validation", func(t *testing.T) {
		server := setupGrackleApiServer(t)
		ctx := context.Background()

		// Create namespace and semaphore first
		_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
			Name: "namespace1",
		})
		require.NoError(t, err)

		_, err = server.CreateSemaphore(ctx, &gracklepb.CreateSemaphoreRequest{
			NamespaceName: "namespace1",
			SemaphoreName: "semaphore1",
			Permits:       5,
		})
		require.NoError(t, err)

		// valid request
		resp, err := server.ReleaseSemaphore(ctx, &gracklepb.ReleaseSemaphoreRequest{
			NamespaceName: "namespace1",
			SemaphoreName: "semaphore1",
			ProcessId:     "proc1",
		})
		require.NoError(t, err)
		require.NotNil(t, resp)

		// invalid request - missing process id
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

		// Create namespace and semaphore first
		_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
			Name: "namespace1",
		})
		require.NoError(t, err)

		_, err = server.CreateSemaphore(ctx, &gracklepb.CreateSemaphoreRequest{
			NamespaceName: "namespace1",
			SemaphoreName: "semaphore1",
			Permits:       5,
		})
		require.NoError(t, err)

		// valid request
		resp, err := server.UpdateSemaphore(ctx, &gracklepb.UpdateSemaphoreRequest{
			NamespaceName: "namespace1",
			SemaphoreName: "semaphore1",
			Permits:       10,
		})
		require.NoError(t, err)
		require.NotNil(t, resp.Semaphore)

		// invalid request - invalid namespace name
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

		// Create namespace first
		_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
			Name: "namespace1",
		})
		require.NoError(t, err)

		// Create a semaphore with valid size
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
			Permits:       uint64(defaultServiceLimits.MaxNumberOfSemaphoreHolders), // Max allowed by account limits
		})
		require.NoError(t, err)
		require.NotNil(t, resp.Semaphore)
		require.EqualValues(t, defaultServiceLimits.MaxNumberOfSemaphoreHolders, resp.Semaphore.Permits)

		// Test update exceeding account limits
		_, err = server.UpdateSemaphore(ctx, &gracklepb.UpdateSemaphoreRequest{
			NamespaceName: "namespace1",
			SemaphoreName: "semaphore1",
			Permits:       uint64(defaultServiceLimits.MaxNumberOfSemaphoreHolders + 1), // Exceeds MaxNumberOfSemaphoreHolders
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), fmt.Sprintf("semaphore size is too big, max: %d", defaultServiceLimits.MaxNumberOfSemaphoreHolders))
	})
}

func TestDeleteSemaphore(t *testing.T) {
	t.Run("validation", func(t *testing.T) {
		server := setupGrackleApiServer(t)
		ctx := context.Background()

		// Create namespace and semaphore first
		_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
			Name: "namespace1",
		})
		require.NoError(t, err)

		_, err = server.CreateSemaphore(ctx, &gracklepb.CreateSemaphoreRequest{
			NamespaceName: "namespace1",
			SemaphoreName: "semaphore1",
			Permits:       5,
		})
		require.NoError(t, err)

		// valid request
		_, err = server.DeleteSemaphore(ctx, &gracklepb.DeleteSemaphoreRequest{
			NamespaceName: "namespace1",
			SemaphoreName: "semaphore1",
		})
		require.NoError(t, err)

		// invalid request - invalid namespace name
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

		// Create namespace first
		_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
			Name: "namespace1",
		})
		require.NoError(t, err)

		// valid request
		resp, err := server.ListSemaphores(ctx, &gracklepb.ListSemaphoresRequest{
			NamespaceName: "namespace1",
		})
		require.NoError(t, err)
		require.NotNil(t, resp.Semaphores)

		// invalid request - invalid namespace name
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
		for i := 0; i < 25; i++ {
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

		// Create namespace first
		_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
			Name: "namespace1",
		})
		require.NoError(t, err)

		// Create a semaphore
		_, err = server.CreateSemaphore(ctx, &gracklepb.CreateSemaphoreRequest{
			NamespaceName: "namespace1",
			SemaphoreName: "semaphore1",
			Permits:       10,
		})
		require.NoError(t, err)

		// valid request
		resp, err := server.ListSemaphoreHolders(ctx, &gracklepb.ListSemaphoreHoldersRequest{
			NamespaceName: "namespace1",
			SemaphoreName: "semaphore1",
		})
		require.NoError(t, err)
		require.NotNil(t, resp.Holders)

		// invalid request - invalid namespace name
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
		for i := 0; i < 25; i++ {
			_, err := server.AcquireSemaphore(ctx, &gracklepb.AcquireSemaphoreRequest{
				NamespaceName: "test-namespace",
				SemaphoreName: "test-semaphore",
				ProcessId:     fmt.Sprintf("process_%03d", i+1),
				ExpiresAt:     time.Now().Add(10 * time.Minute).UnixNano(),
				Weight:        1,
			})
			require.NoError(t, err)
		}

		// Test forward pagination through 3 pages
		var allHolders []*gracklepb.SemaphoreHolder

		// Page 1: Get first 10 holders
		resp1, err := server.ListSemaphoreHolders(ctx, &gracklepb.ListSemaphoreHoldersRequest{
			NamespaceName: "test-namespace",
			SemaphoreName: "test-semaphore",
			Limit:         10,
		})
		require.NoError(t, err)
		require.Len(t, resp1.Holders, 10)
		require.NotEmpty(t, resp1.NextPaginationToken)
		require.Empty(t, resp1.PreviousPaginationToken)

		allHolders = append(allHolders, resp1.Holders...)

		// Page 2: Get next 10 holders
		resp2, err := server.ListSemaphoreHolders(ctx, &gracklepb.ListSemaphoreHoldersRequest{
			NamespaceName:   "test-namespace",
			SemaphoreName:   "test-semaphore",
			PaginationToken: resp1.NextPaginationToken,
			Limit:           10,
		})
		require.NoError(t, err)
		require.Len(t, resp2.Holders, 10)
		require.NotEmpty(t, resp2.NextPaginationToken)
		require.NotEmpty(t, resp2.PreviousPaginationToken)

		allHolders = append(allHolders, resp2.Holders...)

		// Page 3: Get remaining 5 holders
		resp3, err := server.ListSemaphoreHolders(ctx, &gracklepb.ListSemaphoreHoldersRequest{
			NamespaceName:   "test-namespace",
			SemaphoreName:   "test-semaphore",
			PaginationToken: resp2.NextPaginationToken,
			Limit:           10,
		})
		require.NoError(t, err)
		require.Len(t, resp3.Holders, 5)
		require.Empty(t, resp3.NextPaginationToken)
		require.NotEmpty(t, resp3.PreviousPaginationToken)

		allHolders = append(allHolders, resp3.Holders...)

		// Verify we got all 25 holders
		require.Len(t, allHolders, 25)

		// Test backward pagination from the last page
		resp4, err := server.ListSemaphoreHolders(ctx, &gracklepb.ListSemaphoreHoldersRequest{
			NamespaceName:   "test-namespace",
			SemaphoreName:   "test-semaphore",
			PaginationToken: resp3.PreviousPaginationToken,
			Limit:           10,
		})
		require.NoError(t, err)
		require.Len(t, resp4.Holders, 10)
		require.NotEmpty(t, resp4.NextPaginationToken)
		require.NotEmpty(t, resp4.PreviousPaginationToken)
	})
}

func TestListBarriers(t *testing.T) {
	t.Run("validation", func(t *testing.T) {
		server := setupGrackleApiServer(t)
		ctx := context.Background()

		// Create namespace first
		_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
			Name: "namespace1",
		})
		require.NoError(t, err)

		// valid request
		resp, err := server.ListBarriers(ctx, &gracklepb.ListBarriersRequest{
			NamespaceName: "namespace1",
		})
		require.NoError(t, err)
		require.NotNil(t, resp.Barriers)

		// invalid request - invalid namespace name
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
		for i := 0; i < 25; i++ {
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

		// Create namespace first
		_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
			Name: "namespace1",
		})
		require.NoError(t, err)

		// valid request
		_, err = server.DeleteBarrier(ctx, &gracklepb.DeleteBarrierRequest{
			NamespaceName: "namespace1",
			BarrierName:   "barrier1",
		})
		require.NoError(t, err)

		// invalid request - invalid namespace name
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

		// valid request
		resp, err := server.GetBarrier(ctx, &gracklepb.GetBarrierRequest{
			NamespaceName: "namespace1",
			BarrierName:   "barrier1",
		})
		require.NoError(t, err)
		require.NotNil(t, resp.Barrier)
		require.Equal(t, "barrier1", resp.Barrier.Name)

		// invalid request - invalid namespace name
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

		// Create namespace first
		_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
			Name: "namespace1",
		})
		require.NoError(t, err)

		// valid request
		resp, err := server.CreateBarrier(ctx, &gracklepb.CreateBarrierRequest{
			NamespaceName:     "namespace1",
			BarrierName:       "barrier1",
			ExpectedProcesses: 3,
			ExpiresAt:         time.Now().Add(10 * time.Minute).UnixNano(),
		})
		require.NoError(t, err)
		require.NotNil(t, resp.Barrier)
		require.Equal(t, "barrier1", resp.Barrier.Name)

		// invalid request - invalid namespace name
		_, err = server.CreateBarrier(ctx, &gracklepb.CreateBarrierRequest{
			NamespaceName:     "invalid@namespace",
			BarrierName:       "barrier2",
			ExpectedProcesses: 3,
			ExpiresAt:         time.Now().Add(10 * time.Minute).UnixNano(),
		})
		require.Error(t, err)

		// invalid request - expected processes zero
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

		// Create namespace first
		_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
			Name: "namespace1",
		})
		require.NoError(t, err)

		// Create a barrier
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

		// valid request - update barrier
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

		// invalid request - invalid namespace name
		_, err = server.UpdateBarrier(ctx, &gracklepb.UpdateBarrierRequest{
			NamespaceName:     "invalid@namespace",
			BarrierName:       "barrier1",
			Description:       "desc",
			ExpectedProcesses: 5,
		})
		require.Error(t, err)

		// invalid request - expected processes zero
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

		// Create a barrier
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

		// Create a barrier with expected_processes = 5
		_, err = server.CreateBarrier(ctx, &gracklepb.CreateBarrierRequest{
			NamespaceName:     "test-namespace",
			BarrierName:       "test-barrier",
			Description:       "Test barrier",
			ExpectedProcesses: 5,
			ExpiresAt:         time.Now().Add(10 * time.Minute).UnixNano(),
		})
		require.NoError(t, err)

		// Have 3 processes arrive at the barrier
		for i := 0; i < 3; i++ {
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

		// valid request
		_, err = server.ArriveAtBarrier(ctx, &gracklepb.ArriveAtBarrierRequest{
			NamespaceName:      "namespace1",
			BarrierName:        "barrier1",
			ProcessId:          "proc1",
			ExpectedGeneration: 1,
		})
		require.NoError(t, err)

		// invalid request - invalid namespace name
		_, err = server.ArriveAtBarrier(ctx, &gracklepb.ArriveAtBarrierRequest{
			NamespaceName:      "invalid@namespace",
			BarrierName:        "barrier1",
			ProcessId:          "proc2",
			ExpectedGeneration: 1,
		})
		require.Error(t, err)

		// invalid request - expected generation zero
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

		// Create a barrier
		_, err = server.CreateBarrier(ctx, &gracklepb.CreateBarrierRequest{
			NamespaceName:     "namespace1",
			BarrierName:       "barrier1",
			ExpectedProcesses: 3,
			ExpiresAt:         time.Now().Add(10 * time.Minute).UnixNano(),
		})
		require.NoError(t, err)

		// valid request
		resp, err := server.ListBarrierParticipants(ctx, &gracklepb.ListBarrierParticipantsRequest{
			NamespaceName: "namespace1",
			BarrierName:   "barrier1",
			Generation:    1,
		})
		require.NoError(t, err)
		require.NotNil(t, resp.Participants)

		// invalid request - invalid namespace name
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

		// Create a barrier expecting 25 processes
		_, err = server.CreateBarrier(ctx, &gracklepb.CreateBarrierRequest{
			NamespaceName:     "test-namespace",
			BarrierName:       "test-barrier",
			ExpectedProcesses: 25,
			ExpiresAt:         time.Now().Add(10 * time.Minute).UnixNano(),
		})
		require.NoError(t, err)

		// Have 25 processes arrive at the barrier
		for i := 0; i < 25; i++ {
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

		// valid request
		_, err = server.WaitAtBarrier(ctx, &gracklepb.WaitAtBarrierRequest{
			NamespaceName:      "namespace1",
			BarrierName:        "barrier1",
			ExpectedGeneration: 1,
			TimeoutSeconds:     1,
		})
		require.NoError(t, err)

		// invalid request - invalid namespace name
		_, err = server.WaitAtBarrier(ctx, &gracklepb.WaitAtBarrierRequest{
			NamespaceName:      "invalid@namespace",
			BarrierName:        "barrier1",
			ExpectedGeneration: 1,
			TimeoutSeconds:     1,
		})
		require.Error(t, err)

		// invalid request - expected generation zero
		_, err = server.WaitAtBarrier(ctx, &gracklepb.WaitAtBarrierRequest{
			NamespaceName:      "namespace1",
			BarrierName:        "barrier1",
			ExpectedGeneration: 0,
			TimeoutSeconds:     1,
		})
		require.Error(t, err)

		// invalid request - timeout too high
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

func setupGrackleApiServer(t *testing.T) *GrackleApiServer {
	dataStore, err := store.NewBadgerInMemoryStore()
	require.NoError(t, err)

	coresFactory := &monsteragen.GrackleNonclusteredApplicationCoresFactory{
		GrackleWaitGroupsCoreFactoryFunc: func(shardId string, lowerBound []byte, upperBound []byte) monsteragen.GrackleWaitGroupsCoreApi {
			return waitgroups.NewCore(dataStore, utils.GetTruncatedHash([]byte(shardId), 4), lowerBound, upperBound)
		},
		GrackleSemaphoresCoreFactoryFunc: func(shardId string, lowerBound []byte, upperBound []byte) monsteragen.GrackleSemaphoresCoreApi {
			return semaphores.NewCore(dataStore, utils.GetTruncatedHash([]byte(shardId), 4), lowerBound, upperBound)
		},
		GrackleNamespacesCoreFactoryFunc: func(shardId string, lowerBound []byte, upperBound []byte) monsteragen.GrackleNamespacesCoreApi {
			return namespaces.NewCore(dataStore, lowerBound, upperBound)
		},
		GrackleLocksCoreFactoryFunc: func(shardId string, lowerBound []byte, upperBound []byte) monsteragen.GrackleLocksCoreApi {
			return locks.NewCore(dataStore, utils.GetTruncatedHash([]byte(shardId), 4), lowerBound, upperBound)
		},
		GrackleBarriersCoreFactoryFunc: func(shardId string, lowerBound []byte, upperBound []byte) monsteragen.GrackleBarriersCoreApi {
			return barriers.NewCore(dataStore, utils.GetTruncatedHash([]byte(shardId), 4), lowerBound, upperBound)
		},
	}
	grackleCoreApiClient := monsteragen.NewGrackleCoreApiNonclusteredStub(8, coresFactory, &sharding.GrackleShardKeyCalculator{})

	grackleApiGatewayServer := NewGrackleApiServer(grackleCoreApiClient)

	return grackleApiGatewayServer
}
