package preview

import (
	"context"
	"fmt"
	gracklepb "github.com/evrblk/evrblk-go/grackle/preview"
	"github.com/evrblk/grackle/pkg/grackle"
	"github.com/evrblk/monstera"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestAcquireLockValidation(t *testing.T) {
	require := require.New(t)

	server := setupGrackleApiServer()
	ctx := authenticatedContext()

	// create namespace first
	_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
		Name: "namespace1",
	})
	require.NoError(err)

	// valid request
	_, err = server.AcquireLock(ctx, &gracklepb.AcquireLockRequest{
		NamespaceName: "namespace1",
		LockName:      "lock1",
		ProcessId:     "proc1",
	})
	require.NoError(err)

	// invalid request - missing process id
	_, err = server.AcquireLock(ctx, &gracklepb.AcquireLockRequest{
		NamespaceName: "namespace1",
		LockName:      "lock2",
	})
	require.Error(err)
}

func TestAcquireLockAuthentication(t *testing.T) {
	require := require.New(t)

	server := setupGrackleApiServer()
	ctx := context.Background()

	_, err := server.AcquireLock(ctx, &gracklepb.AcquireLockRequest{
		NamespaceName: "namespace1",
		LockName:      "lock1",
		ProcessId:     "proc1",
	})
	require.Error(err)
}

func TestCreateNamespaceValidation(t *testing.T) {
	require := require.New(t)

	server := setupGrackleApiServer()
	ctx := authenticatedContext()

	// valid request
	resp, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
		Name:        "namespace1",
		Description: "Test namespace",
	})
	require.NoError(err)
	require.NotNil(resp.Namespace)

	// invalid request - invalid namespace name
	_, err = server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
		Name:        "invalid@namespace",
		Description: "Test namespace",
	})
	require.Error(err)
}

func TestCreateNamespaceAuthentication(t *testing.T) {
	require := require.New(t)

	server := setupGrackleApiServer()
	ctx := context.Background()

	_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
		Name:        "namespace1",
		Description: "Test namespace",
	})
	require.Error(err)
}

func TestGetNamespaceValidation(t *testing.T) {
	require := require.New(t)

	server := setupGrackleApiServer()
	ctx := authenticatedContext()

	// Create namespace first
	_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
		Name: "namespace1",
	})
	require.NoError(err)

	// valid request
	resp, err := server.GetNamespace(ctx, &gracklepb.GetNamespaceRequest{
		NamespaceName: "namespace1",
	})
	require.NoError(err)
	require.NotNil(resp.Namespace)

	// invalid request - invalid namespace name
	_, err = server.GetNamespace(ctx, &gracklepb.GetNamespaceRequest{
		NamespaceName: "invalid@namespace",
	})
	require.Error(err)
}

func TestGetNamespaceAuthentication(t *testing.T) {
	require := require.New(t)

	server := setupGrackleApiServer()
	ctx := context.Background()

	_, err := server.GetNamespace(ctx, &gracklepb.GetNamespaceRequest{
		NamespaceName: "namespace1",
	})
	require.Error(err)
}

func TestUpdateNamespaceValidation(t *testing.T) {
	require := require.New(t)

	server := setupGrackleApiServer()
	ctx := authenticatedContext()

	// Create namespace first
	_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
		Name: "namespace1",
	})
	require.NoError(err)

	// valid request
	resp, err := server.UpdateNamespace(ctx, &gracklepb.UpdateNamespaceRequest{
		NamespaceName: "namespace1",
		Description:   "Updated description",
	})
	require.NoError(err)
	require.NotNil(resp.Namespace)

	// invalid request - invalid namespace name
	_, err = server.UpdateNamespace(ctx, &gracklepb.UpdateNamespaceRequest{
		NamespaceName: "invalid@namespace",
		Description:   "Updated description",
	})
	require.Error(err)
}

func TestUpdateNamespaceAuthentication(t *testing.T) {
	require := require.New(t)

	server := setupGrackleApiServer()
	ctx := context.Background()

	_, err := server.UpdateNamespace(ctx, &gracklepb.UpdateNamespaceRequest{
		NamespaceName: "namespace1",
		Description:   "Updated description",
	})
	require.Error(err)
}

func TestDeleteNamespaceValidation(t *testing.T) {
	require := require.New(t)

	server := setupGrackleApiServer()
	ctx := authenticatedContext()

	// Create namespace first
	_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
		Name: "namespace1",
	})
	require.NoError(err)

	// valid request
	_, err = server.DeleteNamespace(ctx, &gracklepb.DeleteNamespaceRequest{
		NamespaceName: "namespace1",
	})
	require.NoError(err)

	// invalid request - invalid namespace name
	_, err = server.DeleteNamespace(ctx, &gracklepb.DeleteNamespaceRequest{
		NamespaceName: "invalid@namespace",
	})
	require.Error(err)
}

func TestDeleteNamespaceAuthentication(t *testing.T) {
	require := require.New(t)

	server := setupGrackleApiServer()
	ctx := context.Background()

	_, err := server.DeleteNamespace(ctx, &gracklepb.DeleteNamespaceRequest{
		NamespaceName: "namespace1",
	})
	require.Error(err)
}

func TestListNamespacesValidation(t *testing.T) {
	require := require.New(t)

	server := setupGrackleApiServer()
	ctx := authenticatedContext()

	// valid request
	resp, err := server.ListNamespaces(ctx, &gracklepb.ListNamespacesRequest{})
	require.NoError(err)
	require.NotNil(resp.Namespaces)
}

func TestListNamespacesAuthentication(t *testing.T) {
	require := require.New(t)

	server := setupGrackleApiServer()
	ctx := context.Background()

	_, err := server.ListNamespaces(ctx, &gracklepb.ListNamespacesRequest{})
	require.Error(err)
}

func TestListNamespacesPagination(t *testing.T) {
	require := require.New(t)

	server := setupGrackleApiServer()
	ctx := authenticatedContext()

	// Create 25 namespaces to test pagination (3 pages with limit 10)
	namespaceNames := make([]string, 25)
	for i := 0; i < 25; i++ {
		namespaceNames[i] = fmt.Sprintf("namespace_%03d", i+1)
		_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
			Name:        namespaceNames[i],
			Description: fmt.Sprintf("Test namespace %d", i+1),
		})
		require.NoError(err)
	}

	// Test forward pagination through 3 pages
	var allNamespaces []*gracklepb.Namespace

	// Page 1: Get first 10 namespaces
	resp1, err := server.ListNamespaces(ctx, &gracklepb.ListNamespacesRequest{
		Limit: 10,
	})
	require.NoError(err)
	require.Len(resp1.Namespaces, 10)
	require.NotEmpty(resp1.NextPaginationToken)
	require.Empty(resp1.PreviousPaginationToken) // First page has no previous token

	allNamespaces = append(allNamespaces, resp1.Namespaces...)

	// Page 2: Get next 10 namespaces
	resp2, err := server.ListNamespaces(ctx, &gracklepb.ListNamespacesRequest{
		PaginationToken: resp1.NextPaginationToken,
		Limit:           10,
	})
	require.NoError(err)
	require.Len(resp2.Namespaces, 10)
	require.NotEmpty(resp2.NextPaginationToken)
	require.NotEmpty(resp2.PreviousPaginationToken) // Middle page has both tokens

	allNamespaces = append(allNamespaces, resp2.Namespaces...)

	// Page 3: Get remaining 5 namespaces
	resp3, err := server.ListNamespaces(ctx, &gracklepb.ListNamespacesRequest{
		PaginationToken: resp2.NextPaginationToken,
		Limit:           10,
	})
	require.NoError(err)
	require.Len(resp3.Namespaces, 5)         // Should be 5 remaining namespaces
	require.Empty(resp3.NextPaginationToken) // Last page has no next token
	require.NotEmpty(resp3.PreviousPaginationToken)

	allNamespaces = append(allNamespaces, resp3.Namespaces...)

	// Verify we got all 25 namespaces
	require.Len(allNamespaces, 25)

	// Test backward pagination from the last page
	var backwardNamespaces []*gracklepb.Namespace

	// Go back to page 2
	resp4, err := server.ListNamespaces(ctx, &gracklepb.ListNamespacesRequest{
		PaginationToken: resp3.PreviousPaginationToken,
		Limit:           10,
	})
	require.NoError(err)
	require.Len(resp4.Namespaces, 10)
	require.NotEmpty(resp4.NextPaginationToken)
	require.NotEmpty(resp4.PreviousPaginationToken)

	backwardNamespaces = append(backwardNamespaces, resp4.Namespaces...)

	// Go back to page 1
	resp5, err := server.ListNamespaces(ctx, &gracklepb.ListNamespacesRequest{
		PaginationToken: resp4.PreviousPaginationToken,
		Limit:           10,
	})
	require.NoError(err)
	require.Len(resp5.Namespaces, 10)
	require.NotEmpty(resp5.NextPaginationToken)
	require.Empty(resp5.PreviousPaginationToken) // First page has no previous token

	backwardNamespaces = append(backwardNamespaces, resp5.Namespaces...)

	// Test pagination with different limits
	resp6, err := server.ListNamespaces(ctx, &gracklepb.ListNamespacesRequest{
		Limit: 5,
	})
	require.NoError(err)
	require.Len(resp6.Namespaces, 5)
	require.NotEmpty(resp6.NextPaginationToken)

	// Test pagination with invalid token
	_, err = server.ListNamespaces(ctx, &gracklepb.ListNamespacesRequest{
		PaginationToken: "invalid_token",
		Limit:           10,
	})
	require.Error(err)

	// Test pagination with zero limit (should use default)
	resp7, err := server.ListNamespaces(ctx, &gracklepb.ListNamespacesRequest{
		Limit: 0,
	})
	require.NoError(err)
	require.NotEmpty(resp7.Namespaces)
	require.Len(resp7.Namespaces, 25)
	require.Empty(resp7.NextPaginationToken)

	// Test pagination with negative limit (should use default)
	_, err = server.ListNamespaces(ctx, &gracklepb.ListNamespacesRequest{
		Limit: -1,
	})
	require.Error(err)
}

func TestCreateWaitGroupValidation(t *testing.T) {
	require := require.New(t)

	server := setupGrackleApiServer()
	ctx := authenticatedContext()

	// Create namespace first
	_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
		Name: "namespace1",
	})
	require.NoError(err)

	// valid request
	resp, err := server.CreateWaitGroup(ctx, &gracklepb.CreateWaitGroupRequest{
		NamespaceName: "namespace1",
		WaitGroupName: "waitgroup1",
		Counter:       1,
	})
	require.NoError(err)
	require.NotNil(resp.WaitGroup)

	// invalid request - invalid namespace name
	_, err = server.CreateWaitGroup(ctx, &gracklepb.CreateWaitGroupRequest{
		NamespaceName: "invalid@namespace",
		WaitGroupName: "waitgroup1",
		Counter:       1,
	})
	require.Error(err)
}

func TestCreateWaitGroupAuthentication(t *testing.T) {
	require := require.New(t)

	server := setupGrackleApiServer()
	ctx := context.Background()

	_, err := server.CreateWaitGroup(ctx, &gracklepb.CreateWaitGroupRequest{
		NamespaceName: "namespace1",
		WaitGroupName: "waitgroup1",
	})
	require.Error(err)
}

func TestGetWaitGroupValidation(t *testing.T) {
	require := require.New(t)

	server := setupGrackleApiServer()
	ctx := authenticatedContext()

	// Create namespace and wait group first
	_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
		Name: "namespace1",
	})
	require.NoError(err)

	_, err = server.CreateWaitGroup(ctx, &gracklepb.CreateWaitGroupRequest{
		NamespaceName: "namespace1",
		WaitGroupName: "waitgroup1",
		Counter:       1,
	})
	require.NoError(err)

	// valid request
	resp, err := server.GetWaitGroup(ctx, &gracklepb.GetWaitGroupRequest{
		NamespaceName: "namespace1",
		WaitGroupName: "waitgroup1",
	})
	require.NoError(err)
	require.NotNil(resp.WaitGroup)

	// invalid request - invalid namespace name
	_, err = server.GetWaitGroup(ctx, &gracklepb.GetWaitGroupRequest{
		NamespaceName: "invalid@namespace",
		WaitGroupName: "waitgroup1",
	})
	require.Error(err)
}

func TestGetWaitGroupAuthentication(t *testing.T) {
	require := require.New(t)

	server := setupGrackleApiServer()
	ctx := context.Background()

	_, err := server.GetWaitGroup(ctx, &gracklepb.GetWaitGroupRequest{
		NamespaceName: "namespace1",
		WaitGroupName: "waitgroup1",
	})
	require.Error(err)
}

func TestAddJobsToWaitGroupValidation(t *testing.T) {
	require := require.New(t)

	server := setupGrackleApiServer()
	ctx := authenticatedContext()

	// Create namespace and wait group first
	_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
		Name: "namespace1",
	})
	require.NoError(err)

	_, err = server.CreateWaitGroup(ctx, &gracklepb.CreateWaitGroupRequest{
		NamespaceName: "namespace1",
		WaitGroupName: "waitgroup1",
		Counter:       1,
	})
	require.NoError(err)

	// valid request
	resp, err := server.AddJobsToWaitGroup(ctx, &gracklepb.AddJobsToWaitGroupRequest{
		NamespaceName: "namespace1",
		WaitGroupName: "waitgroup1",
		Counter:       5,
	})
	require.NoError(err)
	require.NotNil(resp.WaitGroup)

	// invalid request - invalid namespace name
	_, err = server.AddJobsToWaitGroup(ctx, &gracklepb.AddJobsToWaitGroupRequest{
		NamespaceName: "invalid@namespace",
		WaitGroupName: "waitgroup1",
		Counter:       5,
	})
	require.Error(err)
}

func TestAddJobsToWaitGroupAuthentication(t *testing.T) {
	require := require.New(t)

	server := setupGrackleApiServer()
	ctx := context.Background()

	_, err := server.AddJobsToWaitGroup(ctx, &gracklepb.AddJobsToWaitGroupRequest{
		NamespaceName: "namespace1",
		WaitGroupName: "waitgroup1",
		Counter:       5,
	})
	require.Error(err)
}

func TestCompleteJobsFromWaitGroupValidation(t *testing.T) {
	require := require.New(t)

	server := setupGrackleApiServer()
	ctx := authenticatedContext()

	// Create namespace and wait group first
	_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
		Name: "namespace1",
	})
	require.NoError(err)

	_, err = server.CreateWaitGroup(ctx, &gracklepb.CreateWaitGroupRequest{
		NamespaceName: "namespace1",
		WaitGroupName: "waitgroup1",
		Counter:       1,
	})
	require.NoError(err)

	// valid request
	resp, err := server.CompleteJobsFromWaitGroup(ctx, &gracklepb.CompleteJobsFromWaitGroupRequest{
		NamespaceName: "namespace1",
		WaitGroupName: "waitgroup1",
		ProcessIds:    []string{"proc1", "proc2"},
	})
	require.NoError(err)
	require.NotNil(resp)

	// invalid request - invalid namespace name
	_, err = server.CompleteJobsFromWaitGroup(ctx, &gracklepb.CompleteJobsFromWaitGroupRequest{
		NamespaceName: "invalid@namespace",
		WaitGroupName: "waitgroup1",
		ProcessIds:    []string{"proc1"},
	})
	require.Error(err)
}

func TestCompleteJobsFromWaitGroupAuthentication(t *testing.T) {
	require := require.New(t)

	server := setupGrackleApiServer()
	ctx := context.Background()

	_, err := server.CompleteJobsFromWaitGroup(ctx, &gracklepb.CompleteJobsFromWaitGroupRequest{
		NamespaceName: "namespace1",
		WaitGroupName: "waitgroup1",
		ProcessIds:    []string{"proc1"},
	})
	require.Error(err)
}

func TestDeleteWaitGroupValidation(t *testing.T) {
	require := require.New(t)

	server := setupGrackleApiServer()
	ctx := authenticatedContext()

	// Create namespace and wait group first
	_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
		Name: "namespace1",
	})
	require.NoError(err)

	_, err = server.CreateWaitGroup(ctx, &gracklepb.CreateWaitGroupRequest{
		NamespaceName: "namespace1",
		WaitGroupName: "waitgroup1",
		Counter:       1,
	})
	require.NoError(err)

	// valid request
	_, err = server.DeleteWaitGroup(ctx, &gracklepb.DeleteWaitGroupRequest{
		NamespaceName: "namespace1",
		WaitGroupName: "waitgroup1",
	})
	require.NoError(err)

	// invalid request - invalid namespace name
	_, err = server.DeleteWaitGroup(ctx, &gracklepb.DeleteWaitGroupRequest{
		NamespaceName: "invalid@namespace",
		WaitGroupName: "waitgroup1",
	})
	require.Error(err)
}

func TestDeleteWaitGroupAuthentication(t *testing.T) {
	require := require.New(t)

	server := setupGrackleApiServer()
	ctx := context.Background()

	_, err := server.DeleteWaitGroup(ctx, &gracklepb.DeleteWaitGroupRequest{
		NamespaceName: "namespace1",
		WaitGroupName: "waitgroup1",
	})
	require.Error(err)
}

func TestListWaitGroupsValidation(t *testing.T) {
	require := require.New(t)

	server := setupGrackleApiServer()
	ctx := authenticatedContext()

	// Create namespace first
	_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
		Name: "namespace1",
	})
	require.NoError(err)

	// valid request
	resp, err := server.ListWaitGroups(ctx, &gracklepb.ListWaitGroupsRequest{
		NamespaceName: "namespace1",
	})
	require.NoError(err)
	require.NotNil(resp.WaitGroups)

	// invalid request - invalid namespace name
	_, err = server.ListWaitGroups(ctx, &gracklepb.ListWaitGroupsRequest{
		NamespaceName: "invalid@namespace",
	})
	require.Error(err)
}

func TestListWaitGroupsAuthentication(t *testing.T) {
	require := require.New(t)

	server := setupGrackleApiServer()
	ctx := context.Background()

	_, err := server.ListWaitGroups(ctx, &gracklepb.ListWaitGroupsRequest{
		NamespaceName: "namespace1",
	})
	require.Error(err)
}

func TestListLocksValidation(t *testing.T) {
	require := require.New(t)

	server := setupGrackleApiServer()
	ctx := authenticatedContext()

	// Create namespace first
	_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
		Name: "namespace1",
	})
	require.NoError(err)

	// valid request
	resp, err := server.ListLocks(ctx, &gracklepb.ListLocksRequest{
		NamespaceName: "namespace1",
	})
	require.NoError(err)
	require.NotNil(resp.Locks)

	// invalid request - invalid namespace name
	_, err = server.ListLocks(ctx, &gracklepb.ListLocksRequest{
		NamespaceName: "invalid@namespace",
	})
	require.Error(err)
}

func TestListLocksAuthentication(t *testing.T) {
	require := require.New(t)

	server := setupGrackleApiServer()
	ctx := context.Background()

	_, err := server.ListLocks(ctx, &gracklepb.ListLocksRequest{
		NamespaceName: "namespace1",
	})
	require.Error(err)
}

func TestReleaseLockValidation(t *testing.T) {
	require := require.New(t)

	server := setupGrackleApiServer()
	ctx := authenticatedContext()

	// Create namespace first
	_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
		Name: "namespace1",
	})
	require.NoError(err)

	// valid request
	_, err = server.ReleaseLock(ctx, &gracklepb.ReleaseLockRequest{
		NamespaceName: "namespace1",
		LockName:      "lock1",
		ProcessId:     "proc1",
	})
	require.NoError(err)

	// invalid request - missing process id
	_, err = server.ReleaseLock(ctx, &gracklepb.ReleaseLockRequest{
		NamespaceName: "namespace1",
		LockName:      "lock1",
	})
	require.Error(err)
}

func TestReleaseLockAuthentication(t *testing.T) {
	require := require.New(t)

	server := setupGrackleApiServer()
	ctx := context.Background()

	_, err := server.ReleaseLock(ctx, &gracklepb.ReleaseLockRequest{
		NamespaceName: "namespace1",
		LockName:      "lock1",
		ProcessId:     "proc1",
	})
	require.Error(err)
}

func TestGetLockValidation(t *testing.T) {
	require := require.New(t)

	server := setupGrackleApiServer()
	ctx := authenticatedContext()

	// Create namespace first
	_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
		Name: "namespace1",
	})
	require.NoError(err)

	// valid request
	resp, err := server.GetLock(ctx, &gracklepb.GetLockRequest{
		NamespaceName: "namespace1",
		LockName:      "lock1",
	})
	require.NoError(err)
	require.NotNil(resp.Lock)

	// invalid request - invalid namespace name
	_, err = server.GetLock(ctx, &gracklepb.GetLockRequest{
		NamespaceName: "invalid@namespace",
		LockName:      "lock1",
	})
	require.Error(err)
}

func TestGetLockAuthentication(t *testing.T) {
	require := require.New(t)

	server := setupGrackleApiServer()
	ctx := context.Background()

	_, err := server.GetLock(ctx, &gracklepb.GetLockRequest{
		NamespaceName: "namespace1",
		LockName:      "lock1",
	})
	require.Error(err)
}

func TestDeleteLockValidation(t *testing.T) {
	require := require.New(t)

	server := setupGrackleApiServer()
	ctx := authenticatedContext()

	// Create namespace first
	_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
		Name: "namespace1",
	})
	require.NoError(err)

	// valid request
	_, err = server.DeleteLock(ctx, &gracklepb.DeleteLockRequest{
		NamespaceName: "namespace1",
		LockName:      "lock1",
	})
	require.NoError(err)

	// invalid request - invalid namespace name
	_, err = server.DeleteLock(ctx, &gracklepb.DeleteLockRequest{
		NamespaceName: "invalid@namespace",
		LockName:      "lock1",
	})
	require.Error(err)
}

func TestDeleteLockAuthentication(t *testing.T) {
	require := require.New(t)

	server := setupGrackleApiServer()
	ctx := context.Background()

	_, err := server.DeleteLock(ctx, &gracklepb.DeleteLockRequest{
		NamespaceName: "namespace1",
		LockName:      "lock1",
	})
	require.Error(err)
}

func TestCreateSemaphoreValidation(t *testing.T) {
	require := require.New(t)

	server := setupGrackleApiServer()
	ctx := authenticatedContext()

	// Create namespace first
	_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
		Name: "namespace1",
	})
	require.NoError(err)

	// valid request
	resp, err := server.CreateSemaphore(ctx, &gracklepb.CreateSemaphoreRequest{
		NamespaceName: "namespace1",
		SemaphoreName: "semaphore1",
		Permits:       5,
	})
	require.NoError(err)
	require.NotNil(resp.Semaphore)

	// invalid request - invalid namespace name
	_, err = server.CreateSemaphore(ctx, &gracklepb.CreateSemaphoreRequest{
		NamespaceName: "invalid@namespace",
		SemaphoreName: "semaphore1",
		Permits:       5,
	})
	require.Error(err)
}

func TestCreateSemaphoreAuthentication(t *testing.T) {
	require := require.New(t)

	server := setupGrackleApiServer()
	ctx := context.Background()

	_, err := server.CreateSemaphore(ctx, &gracklepb.CreateSemaphoreRequest{
		NamespaceName: "namespace1",
		SemaphoreName: "semaphore1",
		Permits:       5,
	})
	require.Error(err)
}

func TestGetSemaphoreValidation(t *testing.T) {
	require := require.New(t)

	server := setupGrackleApiServer()
	ctx := authenticatedContext()

	// Create namespace and semaphore first
	_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
		Name: "namespace1",
	})
	require.NoError(err)

	_, err = server.CreateSemaphore(ctx, &gracklepb.CreateSemaphoreRequest{
		NamespaceName: "namespace1",
		SemaphoreName: "semaphore1",
		Permits:       5,
	})
	require.NoError(err)

	// valid request
	resp, err := server.GetSemaphore(ctx, &gracklepb.GetSemaphoreRequest{
		NamespaceName: "namespace1",
		SemaphoreName: "semaphore1",
	})
	require.NoError(err)
	require.NotNil(resp.Semaphore)

	// invalid request - invalid namespace name
	_, err = server.GetSemaphore(ctx, &gracklepb.GetSemaphoreRequest{
		NamespaceName: "invalid@namespace",
		SemaphoreName: "semaphore1",
	})
	require.Error(err)
}

func TestGetSemaphoreAuthentication(t *testing.T) {
	require := require.New(t)

	server := setupGrackleApiServer()
	ctx := context.Background()

	_, err := server.GetSemaphore(ctx, &gracklepb.GetSemaphoreRequest{
		NamespaceName: "namespace1",
		SemaphoreName: "semaphore1",
	})
	require.Error(err)
}

func TestAcquireSemaphoreValidation(t *testing.T) {
	require := require.New(t)

	server := setupGrackleApiServer()
	ctx := authenticatedContext()

	// Create namespace and semaphore first
	_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
		Name: "namespace1",
	})
	require.NoError(err)

	_, err = server.CreateSemaphore(ctx, &gracklepb.CreateSemaphoreRequest{
		NamespaceName: "namespace1",
		SemaphoreName: "semaphore1",
		Permits:       5,
	})
	require.NoError(err)

	// valid request
	resp, err := server.AcquireSemaphore(ctx, &gracklepb.AcquireSemaphoreRequest{
		NamespaceName: "namespace1",
		SemaphoreName: "semaphore1",
		ProcessId:     "proc1",
	})
	require.NoError(err)
	require.NotNil(resp)

	// invalid request - missing process id
	_, err = server.AcquireSemaphore(ctx, &gracklepb.AcquireSemaphoreRequest{
		NamespaceName: "namespace1",
		SemaphoreName: "semaphore1",
	})
	require.Error(err)
}

func TestAcquireSemaphoreAuthentication(t *testing.T) {
	require := require.New(t)

	server := setupGrackleApiServer()
	ctx := context.Background()

	_, err := server.AcquireSemaphore(ctx, &gracklepb.AcquireSemaphoreRequest{
		NamespaceName: "namespace1",
		SemaphoreName: "semaphore1",
		ProcessId:     "proc1",
	})
	require.Error(err)
}

func TestReleaseSemaphoreValidation(t *testing.T) {
	require := require.New(t)

	server := setupGrackleApiServer()
	ctx := authenticatedContext()

	// Create namespace and semaphore first
	_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
		Name: "namespace1",
	})
	require.NoError(err)

	_, err = server.CreateSemaphore(ctx, &gracklepb.CreateSemaphoreRequest{
		NamespaceName: "namespace1",
		SemaphoreName: "semaphore1",
		Permits:       5,
	})
	require.NoError(err)

	// valid request
	resp, err := server.ReleaseSemaphore(ctx, &gracklepb.ReleaseSemaphoreRequest{
		NamespaceName: "namespace1",
		SemaphoreName: "semaphore1",
		ProcessId:     "proc1",
	})
	require.NoError(err)
	require.NotNil(resp)

	// invalid request - missing process id
	_, err = server.ReleaseSemaphore(ctx, &gracklepb.ReleaseSemaphoreRequest{
		NamespaceName: "namespace1",
		SemaphoreName: "semaphore1",
	})
	require.Error(err)
}

func TestReleaseSemaphoreAuthentication(t *testing.T) {
	require := require.New(t)

	server := setupGrackleApiServer()
	ctx := context.Background()

	_, err := server.ReleaseSemaphore(ctx, &gracklepb.ReleaseSemaphoreRequest{
		NamespaceName: "namespace1",
		SemaphoreName: "semaphore1",
		ProcessId:     "proc1",
	})
	require.Error(err)
}

func TestUpdateSemaphoreValidation(t *testing.T) {
	require := require.New(t)

	server := setupGrackleApiServer()
	ctx := authenticatedContext()

	// Create namespace and semaphore first
	_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
		Name: "namespace1",
	})
	require.NoError(err)

	_, err = server.CreateSemaphore(ctx, &gracklepb.CreateSemaphoreRequest{
		NamespaceName: "namespace1",
		SemaphoreName: "semaphore1",
		Permits:       5,
	})
	require.NoError(err)

	// valid request
	resp, err := server.UpdateSemaphore(ctx, &gracklepb.UpdateSemaphoreRequest{
		NamespaceName: "namespace1",
		SemaphoreName: "semaphore1",
		Permits:       10,
	})
	require.NoError(err)
	require.NotNil(resp.Semaphore)

	// invalid request - invalid namespace name
	_, err = server.UpdateSemaphore(ctx, &gracklepb.UpdateSemaphoreRequest{
		NamespaceName: "invalid@namespace",
		SemaphoreName: "semaphore1",
		Permits:       10,
	})
	require.Error(err)
}

func TestUpdateSemaphoreAuthentication(t *testing.T) {
	require := require.New(t)

	server := setupGrackleApiServer()
	ctx := context.Background()

	_, err := server.UpdateSemaphore(ctx, &gracklepb.UpdateSemaphoreRequest{
		NamespaceName: "namespace1",
		SemaphoreName: "semaphore1",
		Permits:       10,
	})
	require.Error(err)
}

func TestDeleteSemaphoreValidation(t *testing.T) {
	require := require.New(t)

	server := setupGrackleApiServer()
	ctx := authenticatedContext()

	// Create namespace and semaphore first
	_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
		Name: "namespace1",
	})
	require.NoError(err)

	_, err = server.CreateSemaphore(ctx, &gracklepb.CreateSemaphoreRequest{
		NamespaceName: "namespace1",
		SemaphoreName: "semaphore1",
		Permits:       5,
	})
	require.NoError(err)

	// valid request
	_, err = server.DeleteSemaphore(ctx, &gracklepb.DeleteSemaphoreRequest{
		NamespaceName: "namespace1",
		SemaphoreName: "semaphore1",
	})
	require.NoError(err)

	// invalid request - invalid namespace name
	_, err = server.DeleteSemaphore(ctx, &gracklepb.DeleteSemaphoreRequest{
		NamespaceName: "invalid@namespace",
		SemaphoreName: "semaphore1",
	})
	require.Error(err)
}

func TestDeleteSemaphoreAuthentication(t *testing.T) {
	require := require.New(t)

	server := setupGrackleApiServer()
	ctx := context.Background()

	_, err := server.DeleteSemaphore(ctx, &gracklepb.DeleteSemaphoreRequest{
		NamespaceName: "namespace1",
		SemaphoreName: "semaphore1",
	})
	require.Error(err)
}

func TestListSemaphoresValidation(t *testing.T) {
	require := require.New(t)

	server := setupGrackleApiServer()
	ctx := authenticatedContext()

	// Create namespace first
	_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
		Name: "namespace1",
	})
	require.NoError(err)

	// valid request
	resp, err := server.ListSemaphores(ctx, &gracklepb.ListSemaphoresRequest{
		NamespaceName: "namespace1",
	})
	require.NoError(err)
	require.NotNil(resp.Semaphores)

	// invalid request - invalid namespace name
	_, err = server.ListSemaphores(ctx, &gracklepb.ListSemaphoresRequest{
		NamespaceName: "invalid@namespace",
	})
	require.Error(err)
}

func TestListSemaphoresAuthentication(t *testing.T) {
	require := require.New(t)

	server := setupGrackleApiServer()
	ctx := context.Background()

	_, err := server.ListSemaphores(ctx, &gracklepb.ListSemaphoresRequest{
		NamespaceName: "namespace1",
	})
	require.Error(err)
}

func TestCreateSemaphoreMaxSizeValidation(t *testing.T) {
	require := require.New(t)

	server := setupGrackleApiServer()
	ctx := authenticatedContext()

	// Create namespace first
	_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
		Name: "namespace1",
	})
	require.NoError(err)

	// Test valid semaphore size (within limits)
	resp, err := server.CreateSemaphore(ctx, &gracklepb.CreateSemaphoreRequest{
		NamespaceName: "namespace1",
		SemaphoreName: "semaphore1",
		Permits:       50, // Max allowed by account limits
	})
	require.NoError(err)
	require.NotNil(resp.Semaphore)
	require.Equal(uint64(50), resp.Semaphore.Permits)

	// Test semaphore size exceeding account limits
	_, err = server.CreateSemaphore(ctx, &gracklepb.CreateSemaphoreRequest{
		NamespaceName: "namespace1",
		SemaphoreName: "semaphore2",
		Permits:       51, // Exceeds MaxNumberOfSemaphoreHolders (50)
	})
	require.Error(err)
	require.Contains(err.Error(), "semaphore size is too big, max: 50")
}

func TestUpdateSemaphoreMaxSizeValidation(t *testing.T) {
	require := require.New(t)

	server := setupGrackleApiServer()
	ctx := authenticatedContext()

	// Create namespace first
	_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
		Name: "namespace1",
	})
	require.NoError(err)

	// Create a semaphore with valid size
	_, err = server.CreateSemaphore(ctx, &gracklepb.CreateSemaphoreRequest{
		NamespaceName: "namespace1",
		SemaphoreName: "semaphore1",
		Permits:       10,
	})
	require.NoError(err)

	// Test valid update (within limits)
	resp, err := server.UpdateSemaphore(ctx, &gracklepb.UpdateSemaphoreRequest{
		NamespaceName: "namespace1",
		SemaphoreName: "semaphore1",
		Permits:       50, // Max allowed by account limits
	})
	require.NoError(err)
	require.NotNil(resp.Semaphore)
	require.Equal(uint64(50), resp.Semaphore.Permits)

	// Test update exceeding account limits
	_, err = server.UpdateSemaphore(ctx, &gracklepb.UpdateSemaphoreRequest{
		NamespaceName: "namespace1",
		SemaphoreName: "semaphore1",
		Permits:       51, // Exceeds MaxNumberOfSemaphoreHolders (50)
	})
	require.Error(err)
	require.Contains(err.Error(), "semaphore size is too big, max: 50")
}

func TestCreateWaitGroupMaxSizeValidation(t *testing.T) {
	require := require.New(t)

	server := setupGrackleApiServer()
	ctx := authenticatedContext()

	// Create namespace first
	_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
		Name: "namespace1",
	})
	require.NoError(err)

	// Test valid wait group size (within limits)
	resp, err := server.CreateWaitGroup(ctx, &gracklepb.CreateWaitGroupRequest{
		NamespaceName: "namespace1",
		WaitGroupName: "waitgroup1",
		Counter:       1000, // Max allowed by account limits
	})
	require.NoError(err)
	require.NotNil(resp.WaitGroup)
	require.Equal(uint64(1000), resp.WaitGroup.Counter)

	// Test wait group size exceeding account limits
	_, err = server.CreateWaitGroup(ctx, &gracklepb.CreateWaitGroupRequest{
		NamespaceName: "namespace1",
		WaitGroupName: "waitgroup2",
		Counter:       1001, // Exceeds MaxWaitGroupSize (1000)
	})
	require.Error(err)
	require.Contains(err.Error(), "wait group size is too big, max: 1000")
}

func setupGrackleApiServer() *GrackleApiServer {
	dataStore := monstera.NewBadgerInMemoryStore()

	namespacesCore := grackle.NewNamespacesCore(dataStore, []byte{0x02, 0x00}, []byte{0x00, 0x00}, []byte{0xff, 0xff})
	locksCore := grackle.NewLocksCore(dataStore, []byte{0x03, 0x00}, []byte{0x00, 0x00}, []byte{0xff, 0xff})
	semaphoresCore := grackle.NewSemaphoresCore(dataStore, []byte{0x04, 0x00}, []byte{0x00, 0x00}, []byte{0xff, 0xff})
	waitGroupsCore := grackle.NewWaitGroupsCore(dataStore, []byte{0x05, 0x00}, []byte{0x00, 0x00}, []byte{0xff, 0xff})
	grackleCoreApiClient := grackle.NewGrackleCoreApiStandaloneStub(locksCore, semaphoresCore, namespacesCore, waitGroupsCore)

	grackleApiGatewayServer := NewGrackleApiServer(grackleCoreApiClient)

	return grackleApiGatewayServer
}

func authenticatedContext() context.Context {
	ctx := context.Background()
	ctx = context.WithValue(ctx, "account", uint64(123))
	return ctx
}
