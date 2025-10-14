package preview

import (
	"context"
	"fmt"
	"testing"
	"time"

	gracklepb "github.com/evrblk/evrblk-go/grackle/preview"
	"github.com/evrblk/grackle/pkg/grackle"
	"github.com/evrblk/monstera"
	"github.com/stretchr/testify/require"
)

func TestAcquireLockValidation(t *testing.T) {
	server := setupGrackleApiServer()
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
}

func TestCreateNamespaceValidation(t *testing.T) {
	server := setupGrackleApiServer()
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
}

func TestGetNamespaceValidation(t *testing.T) {
	server := setupGrackleApiServer()
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
}

func TestUpdateNamespaceValidation(t *testing.T) {
	server := setupGrackleApiServer()
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
}

func TestDeleteNamespaceValidation(t *testing.T) {
	server := setupGrackleApiServer()
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
}

func TestListNamespacesValidation(t *testing.T) {
	server := setupGrackleApiServer()
	ctx := context.Background()

	// valid request
	resp, err := server.ListNamespaces(ctx, &gracklepb.ListNamespacesRequest{})
	require.NoError(t, err)
	require.NotNil(t, resp.Namespaces)
}

func TestListNamespacesPagination(t *testing.T) {
	server := setupGrackleApiServer()
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
	var backwardNamespaces []*gracklepb.Namespace

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
}

func TestCreateWaitGroupValidation(t *testing.T) {
	server := setupGrackleApiServer()
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
}

func TestGetWaitGroupValidation(t *testing.T) {
	server := setupGrackleApiServer()
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
}

func TestAddJobsToWaitGroupValidation(t *testing.T) {
	server := setupGrackleApiServer()
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
}

func TestCompleteJobsFromWaitGroupValidation(t *testing.T) {
	server := setupGrackleApiServer()
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
}

func TestDeleteWaitGroupValidation(t *testing.T) {
	server := setupGrackleApiServer()
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
}

func TestListWaitGroupsValidation(t *testing.T) {
	server := setupGrackleApiServer()
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
}

func TestListLocksValidation(t *testing.T) {
	server := setupGrackleApiServer()
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
}

func TestReleaseLockValidation(t *testing.T) {
	server := setupGrackleApiServer()
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
}

func TestGetLockValidation(t *testing.T) {
	server := setupGrackleApiServer()
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
}

func TestDeleteLockValidation(t *testing.T) {
	server := setupGrackleApiServer()
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
}

func TestCreateSemaphoreValidation(t *testing.T) {
	server := setupGrackleApiServer()
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
}

func TestGetSemaphoreValidation(t *testing.T) {
	server := setupGrackleApiServer()
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
}

func TestAcquireSemaphoreValidation(t *testing.T) {
	server := setupGrackleApiServer()
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
		ExpiresAt:     time.Now().UnixNano(),
	})
	require.NoError(t, err)
	require.NotNil(t, resp)

	// invalid request - missing process id
	_, err = server.AcquireSemaphore(ctx, &gracklepb.AcquireSemaphoreRequest{
		NamespaceName: "namespace1",
		SemaphoreName: "semaphore1",
		ExpiresAt:     time.Now().UnixNano(),
	})
	require.Error(t, err)
}

func TestReleaseSemaphoreValidation(t *testing.T) {
	server := setupGrackleApiServer()
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
}

func TestUpdateSemaphoreValidation(t *testing.T) {
	server := setupGrackleApiServer()
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
}

func TestDeleteSemaphoreValidation(t *testing.T) {
	server := setupGrackleApiServer()
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
}

func TestListSemaphoresValidation(t *testing.T) {
	server := setupGrackleApiServer()
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
}

func TestCreateSemaphoreMaxSizeValidation(t *testing.T) {
	server := setupGrackleApiServer()
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
		Permits:       100, // Max allowed by account limits
	})
	require.NoError(t, err)
	require.NotNil(t, resp.Semaphore)
	require.Equal(t, uint64(100), resp.Semaphore.Permits)

	// Test semaphore size exceeding account limits
	_, err = server.CreateSemaphore(ctx, &gracklepb.CreateSemaphoreRequest{
		NamespaceName: "namespace1",
		SemaphoreName: "semaphore2",
		Permits:       101, // Exceeds MaxNumberOfSemaphoreHolders (100)
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "semaphore size is too big, max: 100")
}

func TestUpdateSemaphoreMaxSizeValidation(t *testing.T) {
	server := setupGrackleApiServer()
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
		Permits:       100, // Max allowed by account limits
	})
	require.NoError(t, err)
	require.NotNil(t, resp.Semaphore)
	require.Equal(t, uint64(100), resp.Semaphore.Permits)

	// Test update exceeding account limits
	_, err = server.UpdateSemaphore(ctx, &gracklepb.UpdateSemaphoreRequest{
		NamespaceName: "namespace1",
		SemaphoreName: "semaphore1",
		Permits:       101, // Exceeds MaxNumberOfSemaphoreHolders (100)
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "semaphore size is too big, max: 100")
}

func TestCreateWaitGroupMaxSizeValidation(t *testing.T) {
	server := setupGrackleApiServer()
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
	require.Equal(t, uint64(defaultServiceLimits.MaxWaitGroupSize), resp.WaitGroup.Counter)

	// Test wait group size exceeding account limits
	_, err = server.CreateWaitGroup(ctx, &gracklepb.CreateWaitGroupRequest{
		NamespaceName: "namespace1",
		WaitGroupName: "waitgroup2",
		Counter:       uint64(defaultServiceLimits.MaxWaitGroupSize + 1), // Exceeds MaxWaitGroupSize
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), fmt.Sprintf("wait group size is too big, max: %d", uint64(defaultServiceLimits.MaxWaitGroupSize)))
}

func setupGrackleApiServer() *GrackleApiServer {
	dataStore := monstera.NewBadgerInMemoryStore()

	coresFactory := &grackle.GrackleNonclusteredApplicationCoresFactory{
		GrackleWaitGroupsCoreFactoryFunc: func(shardId string, lowerBound []byte, upperBound []byte) grackle.GrackleWaitGroupsCoreApi {
			return grackle.NewWaitGroupsCore(dataStore, monstera.GetTruncatedHash([]byte(shardId), 4), lowerBound, upperBound)
		},
		GrackleSemaphoresCoreFactoryFunc: func(shardId string, lowerBound []byte, upperBound []byte) grackle.GrackleSemaphoresCoreApi {
			return grackle.NewSemaphoresCore(dataStore, monstera.GetTruncatedHash([]byte(shardId), 4), lowerBound, upperBound)
		},
		GrackleNamespacesCoreFactoryFunc: func(shardId string, lowerBound []byte, upperBound []byte) grackle.GrackleNamespacesCoreApi {
			return grackle.NewNamespacesCore(dataStore, monstera.GetTruncatedHash([]byte(shardId), 4), lowerBound, upperBound)
		},
		GrackleLocksCoreFactoryFunc: func(shardId string, lowerBound []byte, upperBound []byte) grackle.GrackleLocksCoreApi {
			return grackle.NewLocksCore(dataStore, monstera.GetTruncatedHash([]byte(shardId), 4), lowerBound, upperBound)
		},
	}
	grackleCoreApiClient := grackle.NewGrackleCoreApiNonclusteredStub(8, coresFactory, &grackle.GrackleShardKeyCalculator{})

	grackleApiGatewayServer := NewGrackleApiServer(grackleCoreApiClient)

	return grackleApiGatewayServer
}
