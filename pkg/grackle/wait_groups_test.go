package grackle

import (
	"bytes"
	"fmt"
	"io"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/evrblk/grackle/pkg/corepb"
	"github.com/evrblk/monstera"
)

func TestCreateAndGetWaitGroup(t *testing.T) {
	waitGroupsCore := newWaitGroupsCore()

	now := time.Now()
	expiresAt := now.Add(time.Hour).UnixNano()

	accountId := rand.Uint64()
	waitGroupId := &corepb.WaitGroupId{
		AccountId:          accountId,
		NamespaceName:      "test_namespace",
		NamespaceCreatedAt: now.UnixNano(),
		WaitGroupName:      "test_wait_group",
	}

	// Create wait group
	createResponse, err := waitGroupsCore.CreateWaitGroup(&corepb.CreateWaitGroupRequest{
		NamespaceTimestampedId: &corepb.NamespaceTimestampedId{
			AccountId:          accountId,
			NamespaceName:      "test_namespace",
			NamespaceCreatedAt: now.UnixNano(),
		},
		Name:                              "test_wait_group",
		Description:                       "test description",
		Counter:                           10,
		Now:                               now.UnixNano(),
		ExpiresAt:                         expiresAt,
		MaxNumberOfWaitGroupsPerNamespace: 100,
	})

	require.NoError(t, err)
	require.NotNil(t, createResponse.WaitGroup)
	require.Equal(t, "test description", createResponse.WaitGroup.Description)
	require.EqualValues(t, 10, createResponse.WaitGroup.Counter)
	require.EqualValues(t, 0, createResponse.WaitGroup.Completed)
	require.Equal(t, expiresAt, createResponse.WaitGroup.ExpiresAt)

	// Get wait group
	getResponse, err := waitGroupsCore.GetWaitGroup(&corepb.GetWaitGroupRequest{
		WaitGroupId: waitGroupId,
	})

	require.NoError(t, err)
	require.NotNil(t, getResponse.WaitGroup)
	require.Equal(t, "test description", getResponse.WaitGroup.Description)
	require.EqualValues(t, 10, getResponse.WaitGroup.Counter)
	require.EqualValues(t, 0, getResponse.WaitGroup.Completed)
}

func TestCreateWaitGroupWithDuplicateName(t *testing.T) {
	waitGroupsCore := newWaitGroupsCore()

	now := time.Now()
	expiresAt := now.Add(time.Hour).UnixNano()

	accountId := rand.Uint64()
	namespaceTimestampedId := &corepb.NamespaceTimestampedId{
		AccountId:          accountId,
		NamespaceName:      "test_namespace",
		NamespaceCreatedAt: now.UnixNano(),
	}

	// Create first wait group
	_, err := waitGroupsCore.CreateWaitGroup(&corepb.CreateWaitGroupRequest{
		NamespaceTimestampedId:            namespaceTimestampedId,
		Name:                              "test_wait_group",
		Description:                       "test description",
		Counter:                           10,
		Now:                               now.UnixNano(),
		ExpiresAt:                         expiresAt,
		MaxNumberOfWaitGroupsPerNamespace: 100,
	})
	require.NoError(t, err)

	// Try to create a second wait group with the same name in the same namespace
	_, err = waitGroupsCore.CreateWaitGroup(&corepb.CreateWaitGroupRequest{
		NamespaceTimestampedId:            namespaceTimestampedId,
		Name:                              "test_wait_group",
		Description:                       "duplicate description",
		Counter:                           20,
		Now:                               now.UnixNano(),
		ExpiresAt:                         expiresAt,
		MaxNumberOfWaitGroupsPerNamespace: 100,
	})

	require.Error(t, err)
}

func TestAddJobToWaitGroup(t *testing.T) {
	waitGroupsCore := newWaitGroupsCore()

	now := time.Now()
	expiresAt := now.Add(time.Hour).UnixNano()

	accountId := rand.Uint64()
	waitGroupId := &corepb.WaitGroupId{
		AccountId:          accountId,
		NamespaceName:      "test_namespace",
		NamespaceCreatedAt: now.UnixNano(),
		WaitGroupName:      "test_wait_group",
	}

	// T+0: Create wait group
	_, err := waitGroupsCore.CreateWaitGroup(&corepb.CreateWaitGroupRequest{
		NamespaceTimestampedId: &corepb.NamespaceTimestampedId{
			AccountId:          accountId,
			NamespaceName:      "test_namespace",
			NamespaceCreatedAt: now.UnixNano(),
		},
		Name:                              "test_wait_group",
		Description:                       "test description",
		Counter:                           10,
		Now:                               now.UnixNano(),
		ExpiresAt:                         expiresAt,
		MaxNumberOfWaitGroupsPerNamespace: 100,
	})
	require.NoError(t, err)

	// T+1m: Add jobs to wait group
	addResponse, err := waitGroupsCore.AddJobsToWaitGroup(&corepb.AddJobsToWaitGroupRequest{
		WaitGroupId:      waitGroupId,
		Counter:          5,
		Now:              now.Add(time.Minute).UnixNano(),
		MaxWaitGroupSize: 100,
	})

	require.NoError(t, err)
	require.NotNil(t, addResponse.WaitGroup)
	require.EqualValues(t, 15, addResponse.WaitGroup.Counter)
	require.EqualValues(t, 0, addResponse.WaitGroup.Completed)
}

func TestAddJobsToNonExistingWaitGroup(t *testing.T) {
	waitGroupsCore := newWaitGroupsCore()

	now := time.Now()

	accountId := rand.Uint64()
	nonExistingWaitGroupId := &corepb.WaitGroupId{
		AccountId:          accountId,
		NamespaceName:      "test_namespace",
		NamespaceCreatedAt: now.UnixNano(),
		WaitGroupName:      "non_existing_wait_group",
	}

	// Try to add jobs to a non-existing wait group
	_, err := waitGroupsCore.AddJobsToWaitGroup(&corepb.AddJobsToWaitGroupRequest{
		WaitGroupId:      nonExistingWaitGroupId,
		Counter:          5,
		Now:              now.UnixNano(),
		MaxWaitGroupSize: 100,
	})

	require.Error(t, err)
}

func TestCompleteJobFromWaitGroup(t *testing.T) {
	waitGroupsCore := newWaitGroupsCore()

	now := time.Now()

	accountId := rand.Uint64()
	waitGroupId := &corepb.WaitGroupId{
		AccountId:          accountId,
		NamespaceName:      "test_namespace",
		NamespaceCreatedAt: now.UnixNano(),
		WaitGroupName:      "test_wait_group",
	}

	// T+0: Create wait group
	_, err := waitGroupsCore.CreateWaitGroup(&corepb.CreateWaitGroupRequest{
		NamespaceTimestampedId: &corepb.NamespaceTimestampedId{
			AccountId:          accountId,
			NamespaceName:      "test_namespace",
			NamespaceCreatedAt: now.UnixNano(),
		},
		Name:                              "test_wait_group",
		Description:                       "test description",
		Counter:                           10,
		Now:                               now.UnixNano(),
		ExpiresAt:                         now.Add(time.Hour).UnixNano(),
		MaxNumberOfWaitGroupsPerNamespace: 100,
	})
	require.NoError(t, err)

	// T+1m: Complete jobs
	completeResponse, err := waitGroupsCore.CompleteJobsFromWaitGroup(&corepb.CompleteJobsFromWaitGroupRequest{
		WaitGroupId: waitGroupId,
		ProcessIds:  []string{"process_1", "process_2", "process_3"},
		Now:         now.Add(time.Minute).UnixNano(),
	})

	require.NoError(t, err)
	require.NotNil(t, completeResponse.WaitGroup)
	require.EqualValues(t, 10, completeResponse.WaitGroup.Counter)
	require.EqualValues(t, 3, completeResponse.WaitGroup.Completed)

	// T+2m: Complete same jobs again (should not increase completed counter)
	completeResponse, err = waitGroupsCore.CompleteJobsFromWaitGroup(&corepb.CompleteJobsFromWaitGroupRequest{
		WaitGroupId: waitGroupId,
		ProcessIds:  []string{"process_1", "process_2"},
		Now:         now.Add(2 * time.Minute).UnixNano(),
	})

	require.NoError(t, err)
	require.NotNil(t, completeResponse.WaitGroup)
	require.EqualValues(t, 10, completeResponse.WaitGroup.Counter)
	require.EqualValues(t, 3, completeResponse.WaitGroup.Completed)
}

func TestCompleteJobsFromNonExistingWaitGroup(t *testing.T) {
	waitGroupsCore := newWaitGroupsCore()

	now := time.Now()

	accountId := rand.Uint64()
	nonExistingWaitGroupId := &corepb.WaitGroupId{
		AccountId:          accountId,
		NamespaceName:      "test_namespace",
		NamespaceCreatedAt: now.UnixNano(),
		WaitGroupName:      "non_existing_wait_group",
	}

	// Try to complete jobs from a non-existing wait group
	_, err := waitGroupsCore.CompleteJobsFromWaitGroup(&corepb.CompleteJobsFromWaitGroupRequest{
		WaitGroupId: nonExistingWaitGroupId,
		ProcessIds:  []string{"process_1", "process_2", "process_3"},
		Now:         now.UnixNano(),
	})

	require.Error(t, err)
}

func TestListWaitGroups(t *testing.T) {
	waitGroupsCore := newWaitGroupsCore()

	now := time.Now()

	accountId := rand.Uint64()
	namespaceTimestampedId := &corepb.NamespaceTimestampedId{
		AccountId:          accountId,
		NamespaceName:      "test_namespace",
		NamespaceCreatedAt: now.UnixNano(),
	}

	// T+0: Create first wait group
	_, err := waitGroupsCore.CreateWaitGroup(&corepb.CreateWaitGroupRequest{
		NamespaceTimestampedId:            namespaceTimestampedId,
		Name:                              "test_wait_group_1",
		Description:                       "test description 1",
		Counter:                           10,
		Now:                               now.UnixNano(),
		ExpiresAt:                         now.Add(time.Hour).UnixNano(),
		MaxNumberOfWaitGroupsPerNamespace: 100,
	})
	require.NoError(t, err)

	// T+1m: Create second wait group
	_, err = waitGroupsCore.CreateWaitGroup(&corepb.CreateWaitGroupRequest{
		NamespaceTimestampedId:            namespaceTimestampedId,
		Name:                              "test_wait_group_2",
		Description:                       "test description 2",
		Counter:                           20,
		Now:                               now.Add(time.Minute).UnixNano(),
		ExpiresAt:                         now.Add(time.Minute).Add(time.Hour).UnixNano(),
		MaxNumberOfWaitGroupsPerNamespace: 100,
	})
	require.NoError(t, err)

	// List wait groups
	response, err := waitGroupsCore.ListWaitGroups(&corepb.ListWaitGroupsRequest{
		NamespaceTimestampedId: namespaceTimestampedId,
	})

	require.NoError(t, err)
	require.Len(t, response.WaitGroups, 2)
}

func TestDeleteWaitGroup(t *testing.T) {
	waitGroupsCore := newWaitGroupsCore()

	now := time.Now()
	expiresAt := now.Add(time.Hour).UnixNano()

	accountId := rand.Uint64()
	waitGroupId := &corepb.WaitGroupId{
		AccountId:          accountId,
		NamespaceName:      "test_namespace",
		NamespaceCreatedAt: now.UnixNano(),
		WaitGroupName:      "test_wait_group",
	}

	// Create wait group
	_, err := waitGroupsCore.CreateWaitGroup(&corepb.CreateWaitGroupRequest{
		NamespaceTimestampedId: &corepb.NamespaceTimestampedId{
			AccountId:          accountId,
			NamespaceName:      "test_namespace",
			NamespaceCreatedAt: now.UnixNano(),
		},
		Name:                              "test_wait_group",
		Description:                       "test description",
		Counter:                           10,
		Now:                               now.UnixNano(),
		ExpiresAt:                         expiresAt,
		MaxNumberOfWaitGroupsPerNamespace: 100,
	})
	require.NoError(t, err)

	// Delete wait group
	_, err = waitGroupsCore.DeleteWaitGroup(&corepb.DeleteWaitGroupRequest{
		WaitGroupId: waitGroupId,
	})
	require.NoError(t, err)

	// Try to get deleted wait group
	_, err = waitGroupsCore.GetWaitGroup(&corepb.GetWaitGroupRequest{
		WaitGroupId: waitGroupId,
	})
	require.Error(t, err)
}

func TestDeleteNonExistingWaitGroup(t *testing.T) {
	waitGroupsCore := newWaitGroupsCore()

	accountId := rand.Uint64()
	nonExistingWaitGroupId := &corepb.WaitGroupId{
		AccountId:          accountId,
		NamespaceName:      "test_namespace",
		NamespaceCreatedAt: time.Now().UnixNano(),
		WaitGroupName:      "non_existing_wait_group",
	}

	// Try to delete a non-existing wait group
	_, err := waitGroupsCore.DeleteWaitGroup(&corepb.DeleteWaitGroupRequest{
		WaitGroupId: nonExistingWaitGroupId,
	})

	// Deleting a non-exising wait group does not return errors
	require.NoError(t, err)
}

func TestLargeWaitGroup(t *testing.T) {
	waitGroupsCore := newWaitGroupsCore()

	now := time.Now()
	groupSize := uint64(1_000_000)

	accountId := rand.Uint64()
	waitGroupId := &corepb.WaitGroupId{
		AccountId:          accountId,
		NamespaceName:      "test_namespace",
		NamespaceCreatedAt: now.UnixNano(),
		WaitGroupName:      "test_large_wait_group",
	}

	// Create a large wait group
	createResponse, err := waitGroupsCore.CreateWaitGroup(&corepb.CreateWaitGroupRequest{
		NamespaceTimestampedId: &corepb.NamespaceTimestampedId{
			AccountId:          accountId,
			NamespaceName:      "test_namespace",
			NamespaceCreatedAt: now.UnixNano(),
		},
		Name:                              "test_large_wait_group",
		Description:                       "test description",
		Counter:                           groupSize,
		Now:                               now.UnixNano(),
		ExpiresAt:                         now.Add(time.Hour).UnixNano(),
		MaxNumberOfWaitGroupsPerNamespace: 100,
	})

	require.NoError(t, err)
	require.NotNil(t, createResponse.WaitGroup)
	require.EqualValues(t, groupSize, createResponse.WaitGroup.Counter)
	require.EqualValues(t, 0, createResponse.WaitGroup.Completed)

	// Complete all jobs in batches
	batchSize := 10
	processIds := make([]string, batchSize)
	completedJobs := uint64(0)

	for completedJobs < groupSize {
		for i := 0; i < batchSize; i++ {
			processIds[i] = fmt.Sprintf("process_%d", completedJobs+uint64(i))
		}

		completeResponse, err := waitGroupsCore.CompleteJobsFromWaitGroup(&corepb.CompleteJobsFromWaitGroupRequest{
			WaitGroupId: waitGroupId,
			ProcessIds:  processIds,
			Now:         now.Add(time.Duration(completedJobs) * time.Millisecond).UnixNano(),
		})

		require.NoError(t, err)
		require.NotNil(t, completeResponse.WaitGroup)
		completedJobs += uint64(batchSize)
	}

	// Verify final state
	getResponse, err := waitGroupsCore.GetWaitGroup(&corepb.GetWaitGroupRequest{
		WaitGroupId: waitGroupId,
	})

	require.NoError(t, err)
	require.NotNil(t, getResponse.WaitGroup)
	require.EqualValues(t, groupSize, getResponse.WaitGroup.Counter)
	require.EqualValues(t, groupSize, getResponse.WaitGroup.Completed)

	// Delete wait group
	_, err = waitGroupsCore.DeleteWaitGroup(&corepb.DeleteWaitGroupRequest{
		WaitGroupId: waitGroupId,
	})
	require.NoError(t, err)
}

func TestSnapshotAndRestoreWaitGroups(t *testing.T) {
	now := time.Now()

	accountId := rand.Uint64()
	waitGroupId := &corepb.WaitGroupId{
		AccountId:          accountId,
		NamespaceName:      "test_namespace",
		NamespaceCreatedAt: now.UnixNano(),
		WaitGroupName:      "test_wait_group",
	}

	// Create two wait group cores for testing snapshot and restore
	waitGroupsCore1 := newWaitGroupsCore()
	waitGroupsCore2 := newWaitGroupsCore()

	// T+0: Create wait group
	_, err := waitGroupsCore1.CreateWaitGroup(&corepb.CreateWaitGroupRequest{
		NamespaceTimestampedId: &corepb.NamespaceTimestampedId{
			AccountId:          accountId,
			NamespaceName:      "test_namespace",
			NamespaceCreatedAt: now.UnixNano(),
		},
		Name:                              "test_wait_group",
		Description:                       "test description",
		Counter:                           10,
		Now:                               now.UnixNano(),
		ExpiresAt:                         now.Add(time.Hour).UnixNano(),
		MaxNumberOfWaitGroupsPerNamespace: 100,
	})
	require.NoError(t, err)

	// T+1m: Complete some jobs
	_, err = waitGroupsCore1.CompleteJobsFromWaitGroup(&corepb.CompleteJobsFromWaitGroupRequest{
		WaitGroupId: waitGroupId,
		ProcessIds:  []string{"process_1", "process_2", "process_3"},
		Now:         now.Add(time.Minute).UnixNano(),
	})
	require.NoError(t, err)

	// Take snapshot at this point
	snapshot := waitGroupsCore1.Snapshot()

	// T+2m: Complete more jobs (after snapshot)
	_, err = waitGroupsCore1.CompleteJobsFromWaitGroup(&corepb.CompleteJobsFromWaitGroupRequest{
		WaitGroupId: waitGroupId,
		ProcessIds:  []string{"process_4", "process_5"},
		Now:         now.Add(2 * time.Minute).UnixNano(),
	})
	require.NoError(t, err)

	// T+3m: Add more jobs to wait group (after snapshot)
	_, err = waitGroupsCore1.AddJobsToWaitGroup(&corepb.AddJobsToWaitGroupRequest{
		WaitGroupId:      waitGroupId,
		Counter:          5,
		Now:              now.Add(3 * time.Minute).UnixNano(),
		MaxWaitGroupSize: 100,
	})
	require.NoError(t, err)

	// Write snapshot to buffer
	buf := bytes.NewBuffer(nil)
	err = snapshot.Write(buf)
	require.NoError(t, err)

	// Restore snapshot to second core
	err = waitGroupsCore2.Restore(io.NopCloser(buf))
	require.NoError(t, err)

	// T+4m: Check that the restored state matches the snapshot state
	// The wait group should exist with 3 completed jobs out of 10 total
	response1, err := waitGroupsCore2.GetWaitGroup(&corepb.GetWaitGroupRequest{
		WaitGroupId: waitGroupId,
	})
	require.NoError(t, err)
	require.NotNil(t, response1.WaitGroup)
	require.EqualValues(t, 10, response1.WaitGroup.Counter)
	require.EqualValues(t, 3, response1.WaitGroup.Completed)

	// T+5m: Complete more jobs in restored state
	_, err = waitGroupsCore2.CompleteJobsFromWaitGroup(&corepb.CompleteJobsFromWaitGroupRequest{
		WaitGroupId: waitGroupId,
		ProcessIds:  []string{"process_6", "process_7"},
		Now:         now.Add(5 * time.Minute).UnixNano(),
	})
	require.NoError(t, err)

	// T+6m: Verify state in restored core
	response2, err := waitGroupsCore2.GetWaitGroup(&corepb.GetWaitGroupRequest{
		WaitGroupId: waitGroupId,
	})
	require.NoError(t, err)
	require.EqualValues(t, 10, response2.WaitGroup.Counter)
	require.EqualValues(t, 5, response2.WaitGroup.Completed)

	// T+7m: Add more jobs in restored state
	_, err = waitGroupsCore2.AddJobsToWaitGroup(&corepb.AddJobsToWaitGroupRequest{
		WaitGroupId:      waitGroupId,
		Counter:          3,
		Now:              now.Add(7 * time.Minute).UnixNano(),
		MaxWaitGroupSize: 100,
	})
	require.NoError(t, err)

	// T+8m: Verify final state in restored core
	response3, err := waitGroupsCore2.GetWaitGroup(&corepb.GetWaitGroupRequest{
		WaitGroupId: waitGroupId,
	})
	require.NoError(t, err)
	require.EqualValues(t, 13, response3.WaitGroup.Counter)
	require.EqualValues(t, 5, response3.WaitGroup.Completed)

	// Verify that the original core has different state (it should have more completed jobs)
	response4, err := waitGroupsCore1.GetWaitGroup(&corepb.GetWaitGroupRequest{
		WaitGroupId: waitGroupId,
	})
	require.NoError(t, err)
	require.EqualValues(t, 15, response4.WaitGroup.Counter)  // 10 + 5 added after snapshot
	require.EqualValues(t, 5, response4.WaitGroup.Completed) // 3 + 2 completed after snapshot
}

func TestCreateWaitGroupMaxNumberOfWaitGroupsPerNamespace(t *testing.T) {
	waitGroupsCore := newWaitGroupsCore()

	now := time.Now()
	expiresAt := now.Add(time.Hour).UnixNano()
	accountId := rand.Uint64()
	maxWaitGroups := int64(3) // Use a small number for testing

	namespaceTimestampedId := &corepb.NamespaceTimestampedId{
		AccountId:          accountId,
		NamespaceName:      "test_namespace",
		NamespaceCreatedAt: now.UnixNano(),
	}

	// Create wait groups up to the limit
	for i := int64(0); i < maxWaitGroups; i++ {
		_, err := waitGroupsCore.CreateWaitGroup(&corepb.CreateWaitGroupRequest{
			NamespaceTimestampedId:            namespaceTimestampedId,
			Name:                              fmt.Sprintf("test_wait_group_%d", i),
			Description:                       fmt.Sprintf("test description %d", i),
			Counter:                           10,
			Now:                               now.UnixNano(),
			ExpiresAt:                         expiresAt,
			MaxNumberOfWaitGroupsPerNamespace: maxWaitGroups,
		})
		require.NoError(t, err, "Failed to create wait group %d", i)
	}

	// Try to create one more wait group, which should fail
	_, err := waitGroupsCore.CreateWaitGroup(&corepb.CreateWaitGroupRequest{
		NamespaceTimestampedId:            namespaceTimestampedId,
		Name:                              "one_too_many",
		Description:                       "this should fail",
		Counter:                           10,
		Now:                               now.UnixNano(),
		ExpiresAt:                         expiresAt,
		MaxNumberOfWaitGroupsPerNamespace: maxWaitGroups,
	})

	require.Error(t, err, "Expected error when exceeding MaxNumberOfWaitGroupsPerNamespace")
}

func TestAddJobsToWaitGroupMaxWaitGroupSize(t *testing.T) {
	waitGroupsCore := newWaitGroupsCore()

	now := time.Now()
	expiresAt := now.Add(time.Hour).UnixNano()
	accountId := rand.Uint64()
	maxWaitGroupSize := int64(10) // Use a small number for testing

	waitGroupId := &corepb.WaitGroupId{
		AccountId:          accountId,
		NamespaceName:      "test_namespace",
		NamespaceCreatedAt: now.UnixNano(),
		WaitGroupName:      "test_wait_group",
	}

	// Create wait group with initial counter of 5
	_, err := waitGroupsCore.CreateWaitGroup(&corepb.CreateWaitGroupRequest{
		NamespaceTimestampedId: &corepb.NamespaceTimestampedId{
			AccountId:          accountId,
			NamespaceName:      "test_namespace",
			NamespaceCreatedAt: now.UnixNano(),
		},
		Name:                              "test_wait_group",
		Description:                       "test description",
		Counter:                           5,
		Now:                               now.UnixNano(),
		ExpiresAt:                         expiresAt,
		MaxNumberOfWaitGroupsPerNamespace: 100,
	})
	require.NoError(t, err)

	// Add jobs up to the limit (5 + 5 = 10, which equals maxWaitGroupSize)
	_, err = waitGroupsCore.AddJobsToWaitGroup(&corepb.AddJobsToWaitGroupRequest{
		WaitGroupId:      waitGroupId,
		Counter:          5,
		Now:              now.Add(time.Minute).UnixNano(),
		MaxWaitGroupSize: maxWaitGroupSize,
	})
	require.NoError(t, err)

	// Try to add one more job, which should fail (5 + 5 + 1 = 11 > 10)
	_, err = waitGroupsCore.AddJobsToWaitGroup(&corepb.AddJobsToWaitGroupRequest{
		WaitGroupId:      waitGroupId,
		Counter:          1,
		Now:              now.Add(2 * time.Minute).UnixNano(),
		MaxWaitGroupSize: maxWaitGroupSize,
	})

	require.Error(t, err, "Expected error when exceeding MaxWaitGroupSize")
}

func TestRunWaitGroupsGarbageCollection(t *testing.T) {
	waitGroupsCore := newWaitGroupsCore()
	now := time.Now()

	// Test parameters - use smaller limits to force multiple runs
	maxDeletedObjects := int64(1000)
	gcRecordsPageSize := int64(100)
	gcRecordWaitGroupsPageSize := int64(1000)

	// Create test data for account 1
	account1 := rand.Uint64()
	namespace1 := &corepb.NamespaceTimestampedId{
		AccountId:          account1,
		NamespaceName:      "test_namespace_1",
		NamespaceCreatedAt: now.UnixNano(),
	}

	// Create multiple wait groups for account 1 with many jobs each
	numWaitGroups := 3
	jobsPerGroup := 500 // 1500 total jobs, exceeding MaxDeletedObjects
	waitGroupIds := make([]*corepb.WaitGroupId, numWaitGroups)

	for i := 0; i < numWaitGroups; i++ {
		waitGroupName := fmt.Sprintf("test_wait_group_%d", i)
		waitGroupIds[i] = &corepb.WaitGroupId{
			AccountId:          account1,
			NamespaceName:      "test_namespace_1",
			NamespaceCreatedAt: now.UnixNano(),
			WaitGroupName:      waitGroupName,
		}

		// Create wait group with many jobs
		_, err := waitGroupsCore.CreateWaitGroup(&corepb.CreateWaitGroupRequest{
			NamespaceTimestampedId:            namespace1,
			Name:                              waitGroupName,
			Description:                       fmt.Sprintf("test description %d", i),
			Counter:                           uint64(jobsPerGroup),
			Now:                               now.UnixNano(),
			ExpiresAt:                         now.Add(time.Hour).UnixNano(),
			MaxNumberOfWaitGroupsPerNamespace: 100,
		})
		require.NoError(t, err)

		// Complete all jobs to create job records
		processIds := make([]string, jobsPerGroup)
		for j := 0; j < jobsPerGroup; j++ {
			processIds[j] = fmt.Sprintf("process_%d_%d", i, j)
		}

		_, err = waitGroupsCore.CompleteJobsFromWaitGroup(&corepb.CompleteJobsFromWaitGroupRequest{
			WaitGroupId: waitGroupIds[i],
			ProcessIds:  processIds,
			Now:         now.Add(time.Minute).UnixNano(),
		})
		require.NoError(t, err)
	}

	// Create test data for account 2 with multiple namespaces
	account2 := rand.Uint64()
	numNamespaces := 2
	waitGroupsPerNamespace := 3
	jobsPerWaitGroup := 400 // 1200 jobs per namespace

	for ns := 0; ns < numNamespaces; ns++ {
		namespaceName := fmt.Sprintf("test_namespace_%d", ns+2)
		namespaceTimestampedId := &corepb.NamespaceTimestampedId{
			AccountId:          account2,
			NamespaceName:      namespaceName,
			NamespaceCreatedAt: now.UnixNano(),
		}

		// Create multiple wait groups for this namespace
		for wg := 0; wg < waitGroupsPerNamespace; wg++ {
			waitGroupName := fmt.Sprintf("test_wait_group_%d", wg)
			waitGroupId := &corepb.WaitGroupId{
				AccountId:          namespaceTimestampedId.AccountId,
				NamespaceName:      namespaceTimestampedId.NamespaceName,
				NamespaceCreatedAt: namespaceTimestampedId.NamespaceCreatedAt,
				WaitGroupName:      waitGroupName,
			}

			// Create wait group with many jobs
			_, err := waitGroupsCore.CreateWaitGroup(&corepb.CreateWaitGroupRequest{
				NamespaceTimestampedId:            namespaceTimestampedId,
				Name:                              waitGroupName,
				Description:                       fmt.Sprintf("test description namespace %d wg %d", ns, wg),
				Counter:                           uint64(jobsPerWaitGroup),
				Now:                               now.UnixNano(),
				ExpiresAt:                         now.Add(time.Hour).UnixNano(),
				MaxNumberOfWaitGroupsPerNamespace: 100,
			})
			require.NoError(t, err)

			// Complete all jobs to create job records
			processIds := make([]string, jobsPerWaitGroup)
			for j := 0; j < jobsPerWaitGroup; j++ {
				processIds[j] = fmt.Sprintf("process_ns%d_wg%d_%d", ns, wg, j)
			}

			_, err = waitGroupsCore.CompleteJobsFromWaitGroup(&corepb.CompleteJobsFromWaitGroupRequest{
				WaitGroupId: waitGroupId,
				ProcessIds:  processIds,
				Now:         now.Add(time.Minute).UnixNano(),
			})
			require.NoError(t, err)
		}
	}

	// Delete one wait group from account 1 (this will create a GC record for individual wait group)
	_, err := waitGroupsCore.DeleteWaitGroup(&corepb.DeleteWaitGroupRequest{
		RecordId:    rand.Uint64(),
		WaitGroupId: waitGroupIds[0],
	})
	require.NoError(t, err)

	// Delete one namespace from account 2 (this will create a GC record for entire namespace)
	namespaceToDelete := &corepb.NamespaceTimestampedId{
		AccountId:          account2,
		NamespaceName:      "test_namespace_2",
		NamespaceCreatedAt: now.UnixNano(),
	}
	_, err = waitGroupsCore.WaitGroupsDeleteNamespace(&corepb.WaitGroupsDeleteNamespaceRequest{
		RecordId:               rand.Uint64(),
		NamespaceTimestampedId: namespaceToDelete,
		Now:                    now.UnixNano(),
	})
	require.NoError(t, err)

	// Run garbage collection multiple times to process all objects
	// The first run should process some objects but not all due to MaxDeletedObjects limit
	runCount := 0
	maxRuns := 10 // Prevent infinite loop

	for runCount < maxRuns {
		runCount++
		t.Logf("Running garbage collection iteration %d", runCount)

		_, err := waitGroupsCore.RunWaitGroupsGarbageCollection(&corepb.RunWaitGroupsGarbageCollectionRequest{
			Now:                        now.UnixNano(),
			GcRecordsPageSize:          gcRecordsPageSize,
			GcRecordWaitGroupsPageSize: gcRecordWaitGroupsPageSize,
			MaxDeletedObjects:          maxDeletedObjects,
		})
		require.NoError(t, err)

		// Check if both expected deletions have been completed
		deletedWaitGroupAccessible := true
		deletedNamespaceAccessible := true

		// Check if the deleted wait group is still accessible
		_, err = waitGroupsCore.GetWaitGroup(&corepb.GetWaitGroupRequest{
			WaitGroupId: waitGroupIds[0],
		})
		if err != nil {
			deletedWaitGroupAccessible = false
			t.Logf("Deleted wait group is no longer accessible after run %d", runCount)
		}

		// Check if the deleted namespace's wait groups are still accessible
		deletedNamespaceWaitGroupId := &corepb.WaitGroupId{
			AccountId:          account2,
			NamespaceName:      "test_namespace_2",
			NamespaceCreatedAt: now.UnixNano(),
			WaitGroupName:      "test_wait_group_0",
		}
		_, err = waitGroupsCore.GetWaitGroup(&corepb.GetWaitGroupRequest{
			WaitGroupId: deletedNamespaceWaitGroupId,
		})
		if err != nil {
			deletedNamespaceAccessible = false
			t.Logf("Deleted namespace is no longer accessible after run %d", runCount)
		}

		// If both deletions are complete, we can stop
		if !deletedWaitGroupAccessible && !deletedNamespaceAccessible {
			t.Logf("Both deletions completed after %d runs", runCount)
			break
		}
	}

	require.Less(t, runCount, maxRuns, "Garbage collection did not complete within expected number of runs")

	// Verify that the deleted wait group is no longer accessible
	_, err = waitGroupsCore.GetWaitGroup(&corepb.GetWaitGroupRequest{
		WaitGroupId: waitGroupIds[0],
	})
	require.Error(t, err, "Deleted wait group should not be accessible")

	// Verify that the deleted namespace's wait groups are no longer accessible
	_, err = waitGroupsCore.GetWaitGroup(&corepb.GetWaitGroupRequest{
		WaitGroupId: &corepb.WaitGroupId{
			AccountId:          account2,
			NamespaceName:      "test_namespace_2",
			NamespaceCreatedAt: now.UnixNano(),
			WaitGroupName:      "test_wait_group_0",
		},
	})
	require.Error(t, err, "Deleted namespace's wait groups should not be accessible")

	// Verify that other wait groups and namespaces are still accessible
	_, err = waitGroupsCore.GetWaitGroup(&corepb.GetWaitGroupRequest{
		WaitGroupId: waitGroupIds[1],
	})
	require.NoError(t, err, "Other wait groups should still be accessible")

	// Debug: Check what namespaces actually exist
	t.Logf("Checking if namespace 3 exists...")
	_, err = waitGroupsCore.GetWaitGroup(&corepb.GetWaitGroupRequest{
		WaitGroupId: &corepb.WaitGroupId{
			AccountId:          account2,
			NamespaceName:      "test_namespace_3",
			NamespaceCreatedAt: now.UnixNano(),
			WaitGroupName:      "test_wait_group_0",
		},
	})
	if err != nil {
		t.Logf("Namespace 3 is not accessible: %v", err)
		// Let's check if namespace 2 is also not accessible (it should be deleted)
		_, err2 := waitGroupsCore.GetWaitGroup(&corepb.GetWaitGroupRequest{
			WaitGroupId: &corepb.WaitGroupId{
				AccountId:          account2,
				NamespaceName:      "test_namespace_2",
				NamespaceCreatedAt: now.UnixNano(),
				WaitGroupName:      "test_wait_group_0",
			},
		})
		if err2 != nil {
			t.Logf("Namespace 2 is also not accessible (expected): %v", err2)
		} else {
			t.Logf("Namespace 2 is still accessible (unexpected)")
		}
	} else {
		t.Logf("Namespace 3 is accessible (expected)")
	}
	require.NoError(t, err, "Other namespaces should still be accessible")

	// Also verify that namespace 4 (if it exists) is still accessible
	_, err = waitGroupsCore.GetWaitGroup(&corepb.GetWaitGroupRequest{
		WaitGroupId: &corepb.WaitGroupId{
			AccountId:          account2,
			NamespaceName:      "test_namespace_4",
			NamespaceCreatedAt: now.UnixNano(),
			WaitGroupName:      "test_wait_group_0",
		},
	})
	// This might fail if we only created 2 namespaces, which is fine
	if err != nil {
		t.Logf("Namespace 4 not accessible (expected if only 2 namespaces were created): %v", err)
	}

	t.Logf("Garbage collection completed successfully in %d runs", runCount)
}

func newWaitGroupsCore() *WaitGroupsCore {
	return NewWaitGroupsCore(monstera.NewBadgerInMemoryStore(), []byte{0x1d, 0x36, 0x00, 0x00}, []byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})
}
