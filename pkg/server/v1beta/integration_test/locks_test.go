package integration_test

import (
	"context"
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	gracklepb "github.com/evrblk/evrblk-go/grackle/v1beta"
	"github.com/stretchr/testify/require"
)

func TestAcquireLock(t *testing.T) {
	t.Run("validation", func(t *testing.T) {
		server := setupGrackleApiServer(t)
		ctx := context.Background()

		// Create namespace
		_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
			Name: "namespace1",
		})
		require.NoError(t, err)

		// Create lease
		resp1, err := server.CreateLockLease(ctx, &gracklepb.CreateLockLeaseRequest{
			NamespaceName: "namespace1",
			TtlSeconds:    30,
			ProcessId:     "process_1",
		})
		require.NoError(t, err)

		// Valid request
		_, err = server.AcquireLock(ctx, &gracklepb.AcquireLockRequest{
			NamespaceName: "namespace1",
			LockName:      "lock1",
			LeaseId:       resp1.Lease.LeaseId,
		})
		require.NoError(t, err)

		// Invalid request - missing lease id
		_, err = server.AcquireLock(ctx, &gracklepb.AcquireLockRequest{
			NamespaceName: "namespace1",
			LockName:      "lock2",
		})
		require.Error(t, err)
	})

	t.Run("blocking", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			server, closeServer := newGrackleApiServer(t)
			defer closeServer()
			ctx := context.Background()

			// Create namespace
			_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
				Name: "test-namespace",
			})
			require.NoError(t, err)

			// Create first lease and acquire the lock exclusively, making it busy
			holderLease, err := server.CreateLockLease(ctx, &gracklepb.CreateLockLeaseRequest{
				NamespaceName: "test-namespace",
				ProcessId:     "holder",
				TtlSeconds:    30,
			})
			require.NoError(t, err)

			acqResp, err := server.AcquireLock(ctx, &gracklepb.AcquireLockRequest{
				NamespaceName:  "test-namespace",
				LockName:       "test-lock",
				LeaseId:        holderLease.Lease.LeaseId,
				Exclusive:      true,
				TimeoutSeconds: 5,
			})
			require.NoError(t, err)
			require.Equal(t, gracklepb.AcquireOutcome_ACQUIRE_OUTCOME_ACQUIRED, acqResp.Outcome)

			// Create second lease for the waiter
			waiterLease, err := server.CreateLockLease(ctx, &gracklepb.CreateLockLeaseRequest{
				NamespaceName: "test-namespace",
				ProcessId:     "waiter",
				TtlSeconds:    30,
			})
			require.NoError(t, err)

			// Release the holder's lock after a delay from another goroutine
			go func() {
				time.Sleep(500 * time.Millisecond)
				_, _ = server.ReleaseLock(ctx, &gracklepb.ReleaseLockRequest{
					NamespaceName: "test-namespace",
					LockName:      "test-lock",
					LeaseId:       holderLease.Lease.LeaseId,
				})
			}()

			// AcquireLock should block on the busy lock and succeed once released
			resp, err := server.AcquireLock(ctx, &gracklepb.AcquireLockRequest{
				NamespaceName:  "test-namespace",
				LockName:       "test-lock",
				LeaseId:        waiterLease.Lease.LeaseId,
				Exclusive:      true,
				TimeoutSeconds: 10,
			})
			require.NoError(t, err)
			require.NotNil(t, resp)
			require.Equal(t, gracklepb.AcquireOutcome_ACQUIRE_OUTCOME_ACQUIRED, resp.Outcome)
		})
	})

	t.Run("timeout", func(t *testing.T) {
		server := setupGrackleApiServer(t)
		ctx := context.Background()

		// Create namespace
		_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
			Name: "test-namespace",
		})
		require.NoError(t, err)

		// Create first lease and acquire the lock exclusively, making it busy
		holderLease, err := server.CreateLockLease(ctx, &gracklepb.CreateLockLeaseRequest{
			NamespaceName: "test-namespace",
			ProcessId:     "holder",
			TtlSeconds:    30,
		})
		require.NoError(t, err)

		acqResp, err := server.AcquireLock(ctx, &gracklepb.AcquireLockRequest{
			NamespaceName:  "test-namespace",
			LockName:       "test-lock",
			LeaseId:        holderLease.Lease.LeaseId,
			Exclusive:      true,
			TimeoutSeconds: 5,
		})
		require.NoError(t, err)
		require.Equal(t, gracklepb.AcquireOutcome_ACQUIRE_OUTCOME_ACQUIRED, acqResp.Outcome)

		// Create second lease for the waiter
		waiterLease, err := server.CreateLockLease(ctx, &gracklepb.CreateLockLeaseRequest{
			NamespaceName: "test-namespace",
			ProcessId:     "waiter",
			TtlSeconds:    30,
		})
		require.NoError(t, err)

		// AcquireLock should block until timeout and return TIMED_OUT since the holder never releases
		resp, err := server.AcquireLock(ctx, &gracklepb.AcquireLockRequest{
			NamespaceName:  "test-namespace",
			LockName:       "test-lock",
			LeaseId:        waiterLease.Lease.LeaseId,
			Exclusive:      true,
			TimeoutSeconds: 2,
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Equal(t, gracklepb.AcquireOutcome_ACQUIRE_OUTCOME_TIMED_OUT, resp.Outcome)
	})
}

func TestReleaseLock(t *testing.T) {
	t.Run("validation", func(t *testing.T) {
		server := setupGrackleApiServer(t)
		ctx := context.Background()

		// Create namespace
		_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
			Name: "namespace1",
		})
		require.NoError(t, err)

		// Create lease
		resp1, err := server.CreateLockLease(ctx, &gracklepb.CreateLockLeaseRequest{
			NamespaceName: "namespace1",
			TtlSeconds:    30,
			ProcessId:     "process_1",
		})
		require.NoError(t, err)

		// Valid request
		_, err = server.ReleaseLock(ctx, &gracklepb.ReleaseLockRequest{
			NamespaceName: "namespace1",
			LockName:      "lock1",
			LeaseId:       resp1.Lease.LeaseId,
		})
		require.NoError(t, err)

		// Invalid request - missing lease id
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

		// Create namespace
		_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
			Name: "namespace1",
		})
		require.NoError(t, err)

		// Valid request
		resp, err := server.GetLock(ctx, &gracklepb.GetLockRequest{
			NamespaceName: "namespace1",
			LockName:      "lock1",
		})
		require.NoError(t, err)
		require.NotNil(t, resp.Lock)

		// Invalid request - invalid namespace name
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

		// Create namespace
		_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
			Name: "namespace1",
		})
		require.NoError(t, err)

		// Valid request
		_, err = server.DeleteLock(ctx, &gracklepb.DeleteLockRequest{
			NamespaceName: "namespace1",
			LockName:      "lock1",
		})
		require.NoError(t, err)

		// Invalid request - invalid namespace name
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

		// Create namespace
		_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
			Name: "namespace1",
		})
		require.NoError(t, err)

		// Valid request
		resp, err := server.ListLocks(ctx, &gracklepb.ListLocksRequest{
			NamespaceName: "namespace1",
		})
		require.NoError(t, err)
		require.NotNil(t, resp.Locks)

		// Invalid request - invalid namespace name
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

		// Create lease
		resp1, err := server.CreateLockLease(ctx, &gracklepb.CreateLockLeaseRequest{
			NamespaceName: "test-namespace",
			TtlSeconds:    30,
			ProcessId:     "process_1",
		})
		require.NoError(t, err)

		// Create 25 locks to test pagination (3 pages with limit 10)
		for i := range 25 {
			_, err := server.AcquireLock(ctx, &gracklepb.AcquireLockRequest{
				NamespaceName: "test-namespace",
				LockName:      fmt.Sprintf("lock_%03d", i+1),
				LeaseId:       resp1.Lease.LeaseId,
			})
			require.NoError(t, err)
		}

		// Test forward pagination through 3 pages
		var allLocks []*gracklepb.Lock

		// Page 1: Get first 10 locks
		resp2, err := server.ListLocks(ctx, &gracklepb.ListLocksRequest{
			NamespaceName: "test-namespace",
			Limit:         10,
		})
		require.NoError(t, err)
		require.Len(t, resp2.Locks, 10)
		require.NotEmpty(t, resp2.NextPaginationToken)
		require.Empty(t, resp2.PreviousPaginationToken)

		allLocks = append(allLocks, resp2.Locks...)

		// Page 2: Get next 10 locks
		resp3, err := server.ListLocks(ctx, &gracklepb.ListLocksRequest{
			NamespaceName:   "test-namespace",
			PaginationToken: resp2.NextPaginationToken,
			Limit:           10,
		})
		require.NoError(t, err)
		require.Len(t, resp3.Locks, 10)
		require.NotEmpty(t, resp3.NextPaginationToken)
		require.NotEmpty(t, resp3.PreviousPaginationToken)

		allLocks = append(allLocks, resp3.Locks...)

		// Page 3: Get remaining 5 locks
		resp4, err := server.ListLocks(ctx, &gracklepb.ListLocksRequest{
			NamespaceName:   "test-namespace",
			PaginationToken: resp3.NextPaginationToken,
			Limit:           10,
		})
		require.NoError(t, err)
		require.Len(t, resp4.Locks, 5)
		require.Empty(t, resp4.NextPaginationToken)
		require.NotEmpty(t, resp4.PreviousPaginationToken)

		allLocks = append(allLocks, resp4.Locks...)

		// Verify we got all 25 locks
		require.Len(t, allLocks, 25)

		// Test backward pagination from the last page
		resp5, err := server.ListLocks(ctx, &gracklepb.ListLocksRequest{
			NamespaceName:   "test-namespace",
			PaginationToken: resp4.PreviousPaginationToken,
			Limit:           10,
		})
		require.NoError(t, err)
		require.Len(t, resp5.Locks, 10)
		require.NotEmpty(t, resp5.NextPaginationToken)
		require.NotEmpty(t, resp5.PreviousPaginationToken)
	})
}

func TestCreateLockLease(t *testing.T) {
	t.Run("validation", func(t *testing.T) {
		server := setupGrackleApiServer(t)
		ctx := context.Background()

		// Create namespace
		_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
			Name: "namespace1",
		})
		require.NoError(t, err)

		// Valid request
		resp, err := server.CreateLockLease(ctx, &gracklepb.CreateLockLeaseRequest{
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
		_, err = server.CreateLockLease(ctx, &gracklepb.CreateLockLeaseRequest{
			NamespaceName: "invalid@namespace",
			ProcessId:     "process1",
			TtlSeconds:    60,
		})
		require.Error(t, err)

		// Invalid request - namespace not found
		_, err = server.CreateLockLease(ctx, &gracklepb.CreateLockLeaseRequest{
			NamespaceName: "nonexistent",
			ProcessId:     "process1",
			TtlSeconds:    60,
		})
		require.Error(t, err)
	})
}

func TestRevokeLockLease(t *testing.T) {
	t.Run("validation", func(t *testing.T) {
		server := setupGrackleApiServer(t)
		ctx := context.Background()

		// Create namespace
		_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
			Name: "namespace1",
		})
		require.NoError(t, err)

		// Create lease
		resp1, err := server.CreateLockLease(ctx, &gracklepb.CreateLockLeaseRequest{
			NamespaceName: "namespace1",
			ProcessId:     "process1",
			TtlSeconds:    60,
		})
		require.NoError(t, err)

		// Valid request
		_, err = server.RevokeLockLease(ctx, &gracklepb.RevokeLockLeaseRequest{
			NamespaceName: "namespace1",
			LeaseId:       resp1.Lease.LeaseId,
		})
		require.NoError(t, err)

		// Invalid request - invalid namespace name
		_, err = server.RevokeLockLease(ctx, &gracklepb.RevokeLockLeaseRequest{
			NamespaceName: "invalid@namespace",
			LeaseId:       resp1.Lease.LeaseId,
		})
		require.Error(t, err)

		// Invalid request - invalid lease ID format
		_, err = server.RevokeLockLease(ctx, &gracklepb.RevokeLockLeaseRequest{
			NamespaceName: "namespace1",
			LeaseId:       "invalid_lease_id",
		})
		require.Error(t, err)
	})
}

func TestRefreshLockLease(t *testing.T) {
	t.Run("validation", func(t *testing.T) {
		server := setupGrackleApiServer(t)
		ctx := context.Background()

		// Create namespace
		_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
			Name: "namespace1",
		})
		require.NoError(t, err)

		// Create lease
		resp1, err := server.CreateLockLease(ctx, &gracklepb.CreateLockLeaseRequest{
			NamespaceName: "namespace1",
			ProcessId:     "process1",
			TtlSeconds:    60,
		})
		require.NoError(t, err)

		// Valid request
		resp2, err := server.RefreshLockLease(ctx, &gracklepb.RefreshLockLeaseRequest{
			NamespaceName: "namespace1",
			LeaseId:       resp1.Lease.LeaseId,
			TtlSeconds:    120,
		})
		require.NoError(t, err)
		require.NotNil(t, resp2.Lease)
		require.Equal(t, resp1.Lease.LeaseId, resp2.Lease.LeaseId)
		require.Equal(t, "process1", resp2.Lease.ProcessId)

		// Invalid request - invalid namespace name
		_, err = server.RefreshLockLease(ctx, &gracklepb.RefreshLockLeaseRequest{
			NamespaceName: "invalid@namespace",
			LeaseId:       resp1.Lease.LeaseId,
			TtlSeconds:    120,
		})
		require.Error(t, err)

		// Invalid request - invalid lease ID format
		_, err = server.RefreshLockLease(ctx, &gracklepb.RefreshLockLeaseRequest{
			NamespaceName: "namespace1",
			LeaseId:       "invalid_lease_id",
			TtlSeconds:    120,
		})
		require.Error(t, err)
	})
}

func TestListLockLeases(t *testing.T) {
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
			_, err := server.CreateLockLease(ctx, &gracklepb.CreateLockLeaseRequest{
				NamespaceName: "namespace1",
				ProcessId:     fmt.Sprintf("process%d", i+1),
				TtlSeconds:    60,
			})
			require.NoError(t, err)
		}

		// Valid request
		resp, err := server.ListLockLeases(ctx, &gracklepb.ListLockLeasesRequest{
			NamespaceName: "namespace1",
		})
		require.NoError(t, err)
		require.NotNil(t, resp.Leases)
		require.Len(t, resp.Leases, 5)

		// Invalid request - invalid namespace name
		_, err = server.ListLockLeases(ctx, &gracklepb.ListLockLeasesRequest{
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
			_, err := server.CreateLockLease(ctx, &gracklepb.CreateLockLeaseRequest{
				NamespaceName: "namespace1",
				ProcessId:     fmt.Sprintf("process%03d", i+1),
				TtlSeconds:    60,
			})
			require.NoError(t, err)
		}

		// First page
		resp1, err := server.ListLockLeases(ctx, &gracklepb.ListLockLeasesRequest{
			NamespaceName: "namespace1",
			Limit:         10,
		})
		require.NoError(t, err)
		require.Len(t, resp1.Leases, 10)
		require.NotEmpty(t, resp1.NextPaginationToken)

		// Second page
		resp2, err := server.ListLockLeases(ctx, &gracklepb.ListLockLeasesRequest{
			NamespaceName:   "namespace1",
			PaginationToken: resp1.NextPaginationToken,
			Limit:           10,
		})
		require.NoError(t, err)
		require.Len(t, resp2.Leases, 10)
		require.NotEmpty(t, resp2.NextPaginationToken)

		// Third page
		resp3, err := server.ListLockLeases(ctx, &gracklepb.ListLockLeasesRequest{
			NamespaceName:   "namespace1",
			PaginationToken: resp2.NextPaginationToken,
			Limit:           10,
		})
		require.NoError(t, err)
		require.Len(t, resp3.Leases, 5)
		require.Empty(t, resp3.NextPaginationToken)
	})
}

func TestGetLockLease(t *testing.T) {
	t.Run("validation", func(t *testing.T) {
		server := setupGrackleApiServer(t)
		ctx := context.Background()

		// Create namespace
		_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
			Name: "namespace1",
		})
		require.NoError(t, err)

		// Create lease
		resp1, err := server.CreateLockLease(ctx, &gracklepb.CreateLockLeaseRequest{
			NamespaceName: "namespace1",
			ProcessId:     "process1",
			TtlSeconds:    60,
		})
		require.NoError(t, err)

		// Valid request
		resp2, err := server.GetLockLease(ctx, &gracklepb.GetLockLeaseRequest{
			NamespaceName: "namespace1",
			LeaseId:       resp1.Lease.LeaseId,
		})
		require.NoError(t, err)
		require.NotNil(t, resp2.Lease)
		require.Equal(t, resp1.Lease.LeaseId, resp2.Lease.LeaseId)
		require.Equal(t, "process1", resp2.Lease.ProcessId)

		// Invalid request - invalid namespace name
		_, err = server.GetLockLease(ctx, &gracklepb.GetLockLeaseRequest{
			NamespaceName: "invalid@namespace",
			LeaseId:       resp1.Lease.LeaseId,
		})
		require.Error(t, err)

		// Invalid request - invalid lease ID format
		_, err = server.GetLockLease(ctx, &gracklepb.GetLockLeaseRequest{
			NamespaceName: "namespace1",
			LeaseId:       "invalid_lease_id",
		})
		require.Error(t, err)

		// Invalid request - lease not found
		_, err = server.GetLockLease(ctx, &gracklepb.GetLockLeaseRequest{
			NamespaceName: "namespace1",
			LeaseId:       "ls_1fM5oldgzaB3TfUzFNzQfMP8ek3XbnFQE",
		})
		require.Error(t, err)
	})
}
