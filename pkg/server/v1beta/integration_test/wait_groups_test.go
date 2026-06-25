package integration_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	gracklepb "github.com/evrblk/evrblk-go/grackle/v1beta"
	"github.com/evrblk/grackle/pkg/grackle"
	"github.com/stretchr/testify/require"
)

// completeJobs builds a slice of CompleteJobRequest from plain job ids
// (without metadata), which is the common case in these tests.
func completeJobs(jobIds []string) []*gracklepb.CompleteJobRequest {
	jobs := make([]*gracklepb.CompleteJobRequest, len(jobIds))
	for i, jobId := range jobIds {
		jobs[i] = &gracklepb.CompleteJobRequest{JobId: jobId}
	}
	return jobs
}

func TestCreateWaitGroup(t *testing.T) {
	t.Run("validation", func(t *testing.T) {
		server := setupGrackleApiServer(t)
		ctx := context.Background()

		// Create namespace
		_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
			Name: "namespace1",
		})
		require.NoError(t, err)

		// Valid request
		resp, err := server.CreateWaitGroup(ctx, &gracklepb.CreateWaitGroupRequest{
			NamespaceName:              "namespace1",
			WaitGroupName:              "waitgroup1",
			Counter:                    1,
			DeleteAfterFinishedSeconds: 60,
		})
		require.NoError(t, err)
		require.NotNil(t, resp.WaitGroup)

		// Invalid request - invalid namespace name
		_, err = server.CreateWaitGroup(ctx, &gracklepb.CreateWaitGroupRequest{
			NamespaceName:              "invalid@namespace",
			WaitGroupName:              "waitgroup1",
			Counter:                    1,
			DeleteAfterFinishedSeconds: 60,
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

		// Test valid wait group size (within limits)
		resp, err := server.CreateWaitGroup(ctx, &gracklepb.CreateWaitGroupRequest{
			NamespaceName:              "namespace1",
			WaitGroupName:              "waitgroup1",
			Counter:                    grackle.DefaultServiceLimits.MaxWaitGroupSize, // Max allowed by account limits
			DeleteAfterFinishedSeconds: 60,
		})
		require.NoError(t, err)
		require.NotNil(t, resp.WaitGroup)
		require.EqualValues(t, grackle.DefaultServiceLimits.MaxWaitGroupSize, resp.WaitGroup.Counter)

		// Test wait group size exceeding account limits
		_, err = server.CreateWaitGroup(ctx, &gracklepb.CreateWaitGroupRequest{
			NamespaceName:              "namespace1",
			WaitGroupName:              "waitgroup2",
			Counter:                    grackle.DefaultServiceLimits.MaxWaitGroupSize + 1, // Exceeds MaxWaitGroupSize
			DeleteAfterFinishedSeconds: 60,
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), fmt.Sprintf("wait group size is too big, max: %d", uint64(grackle.DefaultServiceLimits.MaxWaitGroupSize)))
	})
}

func TestGetWaitGroup(t *testing.T) {
	t.Run("validation", func(t *testing.T) {
		server := setupGrackleApiServer(t)
		ctx := context.Background()

		// Create namespace
		_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
			Name: "namespace1",
		})
		require.NoError(t, err)

		// Create wait group
		_, err = server.CreateWaitGroup(ctx, &gracklepb.CreateWaitGroupRequest{
			NamespaceName:              "namespace1",
			WaitGroupName:              "waitgroup1",
			Counter:                    1,
			DeleteAfterFinishedSeconds: 60,
		})
		require.NoError(t, err)

		// Valid request
		resp, err := server.GetWaitGroup(ctx, &gracklepb.GetWaitGroupRequest{
			NamespaceName: "namespace1",
			WaitGroupName: "waitgroup1",
		})
		require.NoError(t, err)
		require.NotNil(t, resp.WaitGroup)

		// Invalid request - invalid namespace name
		_, err = server.GetWaitGroup(ctx, &gracklepb.GetWaitGroupRequest{
			NamespaceName: "invalid@namespace",
			WaitGroupName: "waitgroup1",
		})
		require.Error(t, err)
	})
}

func TestCompleteJobsFromWaitGroup(t *testing.T) {
	t.Run("validation", func(t *testing.T) {
		server := setupGrackleApiServer(t)
		ctx := context.Background()

		// Create namespace
		_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
			Name: "namespace1",
		})
		require.NoError(t, err)

		// Create wait group
		_, err = server.CreateWaitGroup(ctx, &gracklepb.CreateWaitGroupRequest{
			NamespaceName:              "namespace1",
			WaitGroupName:              "waitgroup1",
			Counter:                    2,
			DeleteAfterFinishedSeconds: 60,
		})
		require.NoError(t, err)

		// Valid request
		resp, err := server.CompleteJobsFromWaitGroup(ctx, &gracklepb.CompleteJobsFromWaitGroupRequest{
			NamespaceName: "namespace1",
			WaitGroupName: "waitgroup1",
			Jobs:          completeJobs([]string{"job1", "job2"}),
		})
		require.NoError(t, err)
		require.NotNil(t, resp)

		// Invalid request - invalid namespace name
		_, err = server.CompleteJobsFromWaitGroup(ctx, &gracklepb.CompleteJobsFromWaitGroupRequest{
			NamespaceName: "invalid@namespace",
			WaitGroupName: "waitgroup1",
			Jobs:          completeJobs([]string{"job1"}),
		})
		require.Error(t, err)
	})

	t.Run("overflow", func(t *testing.T) {
		server := setupGrackleApiServer(t)
		ctx := context.Background()

		// Create namespace
		_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
			Name: "namespace1",
		})
		require.NoError(t, err)

		// Create wait group with a small counter
		_, err = server.CreateWaitGroup(ctx, &gracklepb.CreateWaitGroupRequest{
			NamespaceName:              "namespace1",
			WaitGroupName:              "waitgroup1",
			Counter:                    2,
			DeleteAfterFinishedSeconds: 60,
		})
		require.NoError(t, err)

		// Completing more jobs than the counter allows must fail
		_, err = server.CompleteJobsFromWaitGroup(ctx, &gracklepb.CompleteJobsFromWaitGroupRequest{
			NamespaceName: "namespace1",
			WaitGroupName: "waitgroup1",
			Jobs:          completeJobs([]string{"job1", "job2", "job3"}),
		})
		require.Error(t, err)

		// The failed request must not have persisted any jobs
		resp, err := server.GetWaitGroup(ctx, &gracklepb.GetWaitGroupRequest{
			NamespaceName: "namespace1",
			WaitGroupName: "waitgroup1",
		})
		require.NoError(t, err)
		require.EqualValues(t, 0, resp.WaitGroup.CompletedJobs)

		// Completing exactly Counter jobs succeeds
		_, err = server.CompleteJobsFromWaitGroup(ctx, &gracklepb.CompleteJobsFromWaitGroupRequest{
			NamespaceName: "namespace1",
			WaitGroupName: "waitgroup1",
			Jobs:          completeJobs([]string{"job1", "job2"}),
		})
		require.NoError(t, err)

		// Adding more new jobs on top now overflows and must fail
		_, err = server.CompleteJobsFromWaitGroup(ctx, &gracklepb.CompleteJobsFromWaitGroupRequest{
			NamespaceName: "namespace1",
			WaitGroupName: "waitgroup1",
			Jobs:          completeJobs([]string{"job3"}),
		})
		require.Error(t, err)
	})
}

func TestDeleteWaitGroup(t *testing.T) {
	t.Run("validation", func(t *testing.T) {
		server := setupGrackleApiServer(t)
		ctx := context.Background()

		// Create namespace
		_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
			Name: "namespace1",
		})
		require.NoError(t, err)

		// Create wait group
		_, err = server.CreateWaitGroup(ctx, &gracklepb.CreateWaitGroupRequest{
			NamespaceName:              "namespace1",
			WaitGroupName:              "waitgroup1",
			Counter:                    1,
			DeleteAfterFinishedSeconds: 60,
		})
		require.NoError(t, err)

		// Valid request
		_, err = server.DeleteWaitGroup(ctx, &gracklepb.DeleteWaitGroupRequest{
			NamespaceName: "namespace1",
			WaitGroupName: "waitgroup1",
		})
		require.NoError(t, err)

		// Invalid request - invalid namespace name
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

		// Create namespace
		_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
			Name: "namespace1",
		})
		require.NoError(t, err)

		// Valid request
		resp, err := server.ListWaitGroups(ctx, &gracklepb.ListWaitGroupsRequest{
			NamespaceName: "namespace1",
		})
		require.NoError(t, err)
		require.NotNil(t, resp.WaitGroups)

		// Invalid request - invalid namespace name
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
		for i := range 25 {
			_, err := server.CreateWaitGroup(ctx, &gracklepb.CreateWaitGroupRequest{
				NamespaceName:              "test-namespace",
				WaitGroupName:              fmt.Sprintf("waitgroup_%03d", i+1),
				Description:                fmt.Sprintf("Test wait group %d", i+1),
				Counter:                    10,
				DeleteAfterFinishedSeconds: 60,
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

func TestListWaitGroupCompletedJobs(t *testing.T) {
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
			NamespaceName:              "namespace1",
			WaitGroupName:              "waitgroup1",
			Counter:                    10,
			DeleteAfterFinishedSeconds: 60,
		})
		require.NoError(t, err)

		// Valid request
		resp, err := server.ListWaitGroupCompletedJobs(ctx, &gracklepb.ListWaitGroupCompletedJobsRequest{
			NamespaceName: "namespace1",
			WaitGroupName: "waitgroup1",
		})
		require.NoError(t, err)
		require.NotNil(t, resp.Jobs)

		// Invalid request - invalid namespace name
		_, err = server.ListWaitGroupCompletedJobs(ctx, &gracklepb.ListWaitGroupCompletedJobsRequest{
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
			NamespaceName:              "test-namespace",
			WaitGroupName:              "test-waitgroup",
			Counter:                    25,
			DeleteAfterFinishedSeconds: 60,
		})
		require.NoError(t, err)

		// Complete all 25 jobs
		jobIds := make([]string, 25)
		for i := range 25 {
			jobIds[i] = fmt.Sprintf("job-%d", i+1)
		}
		_, err = server.CompleteJobsFromWaitGroup(ctx, &gracklepb.CompleteJobsFromWaitGroupRequest{
			NamespaceName: "test-namespace",
			WaitGroupName: "test-waitgroup",
			Jobs:          completeJobs(jobIds),
		})
		require.NoError(t, err)

		// First page - get first 10 jobs
		resp1, err := server.ListWaitGroupCompletedJobs(ctx, &gracklepb.ListWaitGroupCompletedJobsRequest{
			NamespaceName: "test-namespace",
			WaitGroupName: "test-waitgroup",
			Limit:         10,
		})
		require.NoError(t, err)
		require.Len(t, resp1.Jobs, 10)
		require.NotEmpty(t, resp1.NextPaginationToken)
		require.Empty(t, resp1.PreviousPaginationToken)

		// Second page - get next 10 jobs
		resp2, err := server.ListWaitGroupCompletedJobs(ctx, &gracklepb.ListWaitGroupCompletedJobsRequest{
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
		resp3, err := server.ListWaitGroupCompletedJobs(ctx, &gracklepb.ListWaitGroupCompletedJobsRequest{
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
			NamespaceName:              "namespace1",
			WaitGroupName:              "waitgroup1",
			Counter:                    10,
			DeleteAfterFinishedSeconds: 60,
		})
		require.NoError(t, err)

		// Valid request
		_, err = server.WaitForWaitGroup(ctx, &gracklepb.WaitForWaitGroupRequest{
			NamespaceName:  "namespace1",
			WaitGroupName:  "waitgroup1",
			TimeoutSeconds: 1,
		})
		require.NoError(t, err)

		// Invalid request - invalid namespace name
		_, err = server.WaitForWaitGroup(ctx, &gracklepb.WaitForWaitGroupRequest{
			NamespaceName:  "invalid@namespace",
			WaitGroupName:  "waitgroup1",
			TimeoutSeconds: 1,
		})
		require.Error(t, err)

		// Invalid request - timeout too high
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
			NamespaceName:              "test-namespace",
			WaitGroupName:              "test-wg",
			Counter:                    2,
			DeleteAfterFinishedSeconds: 60,
		})
		require.NoError(t, err)

		// Test: Wait for a wait group that completes within timeout
		go func() {
			time.Sleep(500 * time.Millisecond)
			// Complete first job
			_, _ = server.CompleteJobsFromWaitGroup(ctx, &gracklepb.CompleteJobsFromWaitGroupRequest{
				NamespaceName: "test-namespace",
				WaitGroupName: "test-wg",
				Jobs:          completeJobs([]string{"job1"}),
			})
			time.Sleep(500 * time.Millisecond)
			// Complete second job
			_, _ = server.CompleteJobsFromWaitGroup(ctx, &gracklepb.CompleteJobsFromWaitGroupRequest{
				NamespaceName: "test-namespace",
				WaitGroupName: "test-wg",
				Jobs:          completeJobs([]string{"job2"}),
			})
		}()

		// Wait for completion with 1 second timeout
		resp, err := server.WaitForWaitGroup(ctx, &gracklepb.WaitForWaitGroupRequest{
			NamespaceName:  "test-namespace",
			WaitGroupName:  "test-wg",
			TimeoutSeconds: 10,
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Equal(t, gracklepb.WaitGroupWaitOutcome_WAIT_GROUP_WAIT_OUTCOME_COMPLETED, resp.Outcome)
		require.Equal(t, gracklepb.WaitGroupStatus_WAIT_GROUP_STATUS_COMPLETED, resp.WaitGroup.Status)
		require.EqualValues(t, 2, resp.WaitGroup.Counter)
		require.EqualValues(t, 2, resp.WaitGroup.CompletedJobs)
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
			NamespaceName:              "test-namespace",
			WaitGroupName:              "test-wg-timeout",
			Counter:                    10,
			DeleteAfterFinishedSeconds: 60,
		})
		require.NoError(t, err)

		go func() {
			// Complete only 5 jobs
			_, _ = server.CompleteJobsFromWaitGroup(ctx, &gracklepb.CompleteJobsFromWaitGroupRequest{
				NamespaceName: "test-namespace",
				WaitGroupName: "test-wg-timeout",
				Jobs:          completeJobs([]string{"p1", "p2", "p3", "p4", "p5"}),
			})
		}()

		// Wait with 1 second timeout (should timeout)
		resp, err := server.WaitForWaitGroup(ctx, &gracklepb.WaitForWaitGroupRequest{
			NamespaceName:  "test-namespace",
			WaitGroupName:  "test-wg-timeout",
			TimeoutSeconds: 2,
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Equal(t, gracklepb.WaitGroupWaitOutcome_WAIT_GROUP_WAIT_OUTCOME_TIMED_OUT, resp.Outcome)
		require.Equal(t, gracklepb.WaitGroupStatus_WAIT_GROUP_STATUS_ACTIVE, resp.WaitGroup.Status)
		require.EqualValues(t, 10, resp.WaitGroup.Counter)
		require.EqualValues(t, 5, resp.WaitGroup.CompletedJobs)
	})
}
