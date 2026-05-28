package waitgroups

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

	"github.com/evrblk/grackle/pkg/corepb"
	"github.com/evrblk/grackle/pkg/tables"
)

func init() {
	registry := monsterax.NewBaseTableRegistry(1)
	tables.RegisterGracklePrefixes(registry)
}

func TestCore_Create(t *testing.T) {
	t.Run("create wait group", func(t *testing.T) {
		waitGroupsCore := newWaitGroupsCore(t)

		now := time.Now()
		expiresAt := now.Add(time.Hour).UnixNano()

		waitGroupId := &corepb.WaitGroupId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
			WaitGroupId: rand.Uint64(),
		}

		// Create wait group
		createResponse, err := waitGroupsCore.CreateWaitGroup(&corepb.CreateWaitGroupRequest{
			WaitGroupId:                       waitGroupId,
			Name:                              "test_wait_group",
			Description:                       "test description",
			Counter:                           10,
			Now:                               now.UnixNano(),
			ExpiresAt:                         expiresAt,
			MaxNumberOfWaitGroupsPerNamespace: 100,
		})

		require.NoError(t, err)
		require.NotNil(t, createResponse.WaitGroup)
		require.Equal(t, "test_wait_group", createResponse.WaitGroup.Name)
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
		require.Equal(t, "test_wait_group", getResponse.WaitGroup.Name)
		require.Equal(t, "test description", getResponse.WaitGroup.Description)
		require.EqualValues(t, 10, getResponse.WaitGroup.Counter)
		require.EqualValues(t, 0, getResponse.WaitGroup.Completed)

		// Get wait group by name
		getByNameResponse, err := waitGroupsCore.GetWaitGroupByName(&corepb.GetWaitGroupByNameRequest{
			NamespaceId: &corepb.NamespaceId{
				AccountId:   waitGroupId.AccountId,
				NamespaceId: waitGroupId.NamespaceId,
			},
			WaitGroupName: "test_wait_group",
		})

		require.NoError(t, err)
		require.NotNil(t, getByNameResponse.WaitGroup)
		require.Equal(t, "test_wait_group", getByNameResponse.WaitGroup.Name)
		require.Equal(t, "test description", getByNameResponse.WaitGroup.Description)
		require.EqualValues(t, 10, getByNameResponse.WaitGroup.Counter)
		require.EqualValues(t, 0, getByNameResponse.WaitGroup.Completed)
	})

	t.Run("create wait group with duplicate name", func(t *testing.T) {
		waitGroupsCore := newWaitGroupsCore(t)

		now := time.Now()
		expiresAt := now.Add(time.Hour).UnixNano()

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		// Create first wait group
		_, err := waitGroupsCore.CreateWaitGroup(&corepb.CreateWaitGroupRequest{
			WaitGroupId: &corepb.WaitGroupId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
				WaitGroupId: rand.Uint64(),
			},
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
			WaitGroupId: &corepb.WaitGroupId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
				WaitGroupId: rand.Uint64(),
			},
			Name:                              "test_wait_group",
			Description:                       "duplicate description",
			Counter:                           20,
			Now:                               now.UnixNano(),
			ExpiresAt:                         expiresAt,
			MaxNumberOfWaitGroupsPerNamespace: 100,
		})

		require.Error(t, err)
	})

	t.Run("maximum number of wait groups per namespace", func(t *testing.T) {
		waitGroupsCore := newWaitGroupsCore(t)

		now := time.Now()
		expiresAt := now.Add(time.Hour).UnixNano()
		maxWaitGroups := int64(3) // Use a small number for testing

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		// Create wait groups up to the limit
		for i := int64(0); i < maxWaitGroups; i++ {
			_, err := waitGroupsCore.CreateWaitGroup(&corepb.CreateWaitGroupRequest{
				WaitGroupId: &corepb.WaitGroupId{
					AccountId:   accountId,
					NamespaceId: namespaceId,
					WaitGroupId: rand.Uint64(),
				},
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
			WaitGroupId: &corepb.WaitGroupId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
				WaitGroupId: rand.Uint64(),
			},
			Name:                              "one_too_many",
			Description:                       "this should fail",
			Counter:                           10,
			Now:                               now.UnixNano(),
			ExpiresAt:                         expiresAt,
			MaxNumberOfWaitGroupsPerNamespace: maxWaitGroups,
		})

		require.Error(t, err, "Expected error when exceeding MaxNumberOfWaitGroupsPerNamespace")
	})
}

func TestCore_AddJobsToWaitGroup(t *testing.T) {
	t.Run("add jobs to existing wait group", func(t *testing.T) {
		waitGroupsCore := newWaitGroupsCore(t)

		now := time.Now()
		expiresAt := now.Add(time.Hour).UnixNano()

		namespaceId := &corepb.NamespaceId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
		}

		// T+0: Create wait group
		_, err := waitGroupsCore.CreateWaitGroup(&corepb.CreateWaitGroupRequest{
			WaitGroupId: &corepb.WaitGroupId{
				AccountId:   namespaceId.AccountId,
				NamespaceId: namespaceId.NamespaceId,
				WaitGroupId: rand.Uint64(),
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
			NamespaceId:      namespaceId,
			WaitGroupName:    "test_wait_group",
			Counter:          5,
			Now:              now.Add(time.Minute).UnixNano(),
			MaxWaitGroupSize: 100,
		})

		require.NoError(t, err)
		require.NotNil(t, addResponse.WaitGroup)
		require.EqualValues(t, 15, addResponse.WaitGroup.Counter)
		require.EqualValues(t, 0, addResponse.WaitGroup.Completed)
	})

	t.Run("add jobs to nonexistent wait group", func(t *testing.T) {
		waitGroupsCore := newWaitGroupsCore(t)

		now := time.Now()

		// Try to add jobs to a nonexistent wait group
		_, err := waitGroupsCore.AddJobsToWaitGroup(&corepb.AddJobsToWaitGroupRequest{
			NamespaceId: &corepb.NamespaceId{
				AccountId:   rand.Uint64(),
				NamespaceId: rand.Uint32(),
			},
			WaitGroupName:    "nonexistent_wait_group",
			Counter:          5,
			Now:              now.UnixNano(),
			MaxWaitGroupSize: 100,
		})

		require.Error(t, err)
	})

	t.Run("maximum group size", func(t *testing.T) {
		waitGroupsCore := newWaitGroupsCore(t)

		now := time.Now()
		expiresAt := now.Add(time.Hour).UnixNano()
		maxWaitGroupSize := int64(10) // Use a small number for testing

		waitGroupId := &corepb.WaitGroupId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
			WaitGroupId: rand.Uint64(),
		}

		// Create wait group with initial counter of 5
		_, err := waitGroupsCore.CreateWaitGroup(&corepb.CreateWaitGroupRequest{
			WaitGroupId:                       waitGroupId,
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
			NamespaceId: &corepb.NamespaceId{
				AccountId:   waitGroupId.AccountId,
				NamespaceId: waitGroupId.NamespaceId,
			},
			WaitGroupName:    "test_wait_group",
			Counter:          5,
			Now:              now.Add(time.Minute).UnixNano(),
			MaxWaitGroupSize: maxWaitGroupSize,
		})
		require.NoError(t, err)

		// Try to add one more job, which should fail (5 + 5 + 1 = 11 > 10)
		_, err = waitGroupsCore.AddJobsToWaitGroup(&corepb.AddJobsToWaitGroupRequest{
			NamespaceId: &corepb.NamespaceId{
				AccountId:   waitGroupId.AccountId,
				NamespaceId: waitGroupId.NamespaceId,
			},
			WaitGroupName:    "test_wait_group",
			Counter:          1,
			Now:              now.Add(2 * time.Minute).UnixNano(),
			MaxWaitGroupSize: maxWaitGroupSize,
		})

		require.Error(t, err, "Expected error when exceeding MaxWaitGroupSize")
	})
}

func TestCore_CompleteJobsFromWaitGroup(t *testing.T) {
	t.Run("complete jobs from existing wait group", func(t *testing.T) {
		waitGroupsCore := newWaitGroupsCore(t)

		now := time.Now()

		namespaceId := &corepb.NamespaceId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
		}

		// T+0: Create wait group
		_, err := waitGroupsCore.CreateWaitGroup(&corepb.CreateWaitGroupRequest{
			WaitGroupId: &corepb.WaitGroupId{
				AccountId:   namespaceId.AccountId,
				NamespaceId: namespaceId.NamespaceId,
				WaitGroupId: rand.Uint64(),
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
			NamespaceId:   namespaceId,
			WaitGroupName: "test_wait_group",
			ProcessIds:    []string{"process_1", "process_2", "process_3"},
			Now:           now.Add(time.Minute).UnixNano(),
		})

		require.NoError(t, err)
		require.NotNil(t, completeResponse.WaitGroup)
		require.EqualValues(t, 10, completeResponse.WaitGroup.Counter)
		require.EqualValues(t, 3, completeResponse.WaitGroup.Completed)

		// T+2m: Complete same jobs again (should not increase completed counter)
		completeResponse, err = waitGroupsCore.CompleteJobsFromWaitGroup(&corepb.CompleteJobsFromWaitGroupRequest{
			NamespaceId:   namespaceId,
			WaitGroupName: "test_wait_group",
			ProcessIds:    []string{"process_1", "process_2"},
			Now:           now.Add(2 * time.Minute).UnixNano(),
		})

		require.NoError(t, err)
		require.NotNil(t, completeResponse.WaitGroup)
		require.EqualValues(t, 10, completeResponse.WaitGroup.Counter)
		require.EqualValues(t, 3, completeResponse.WaitGroup.Completed)
	})

	t.Run("complete jobs from nonexistent wait group", func(t *testing.T) {
		waitGroupsCore := newWaitGroupsCore(t)

		now := time.Now()

		// Try to complete jobs from a nonexistent wait group
		_, err := waitGroupsCore.CompleteJobsFromWaitGroup(&corepb.CompleteJobsFromWaitGroupRequest{
			NamespaceId: &corepb.NamespaceId{
				AccountId:   rand.Uint64(),
				NamespaceId: rand.Uint32(),
			},
			WaitGroupName: "nonexistent_wait_group",
			ProcessIds:    []string{"process_1", "process_2", "process_3"},
			Now:           now.UnixNano(),
		})

		require.Error(t, err)
	})
}

func TestCore_ListWaitGroups(t *testing.T) {
	waitGroupsCore := newWaitGroupsCore(t)

	now := time.Now()

	accountId := rand.Uint64()
	namespaceId := rand.Uint32()

	// T+0: Create first wait group
	_, err := waitGroupsCore.CreateWaitGroup(&corepb.CreateWaitGroupRequest{
		WaitGroupId: &corepb.WaitGroupId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			WaitGroupId: rand.Uint64(),
		},
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
		WaitGroupId: &corepb.WaitGroupId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			WaitGroupId: rand.Uint64(),
		},
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
		NamespaceId: &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
		},
	})

	require.NoError(t, err)
	require.Len(t, response.WaitGroups, 2)
}

func TestCore_ListWaitGroupJobs(t *testing.T) {
	t.Run("list jobs from wait group with multiple jobs", func(t *testing.T) {
		waitGroupsCore := newWaitGroupsCore(t)

		now := time.Now()

		namespaceId := &corepb.NamespaceId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
		}

		// Create wait group
		_, err := waitGroupsCore.CreateWaitGroup(&corepb.CreateWaitGroupRequest{
			WaitGroupId: &corepb.WaitGroupId{
				AccountId:   namespaceId.AccountId,
				NamespaceId: namespaceId.NamespaceId,
				WaitGroupId: rand.Uint64(),
			},
			Name:                              "test_wait_group",
			Description:                       "test description",
			Counter:                           10,
			Now:                               now.UnixNano(),
			ExpiresAt:                         now.Add(time.Hour).UnixNano(),
			MaxNumberOfWaitGroupsPerNamespace: 100,
		})
		require.NoError(t, err)

		// Complete some jobs
		_, err = waitGroupsCore.CompleteJobsFromWaitGroup(&corepb.CompleteJobsFromWaitGroupRequest{
			NamespaceId:   namespaceId,
			WaitGroupName: "test_wait_group",
			ProcessIds:    []string{"process_1", "process_2", "process_3"},
			Now:           now.Add(time.Minute).UnixNano(),
		})
		require.NoError(t, err)

		// List wait group jobs
		response, err := waitGroupsCore.ListWaitGroupJobs(&corepb.ListWaitGroupJobsRequest{
			NamespaceId:   namespaceId,
			WaitGroupName: "test_wait_group",
		})

		require.NoError(t, err)
		require.Len(t, response.Jobs, 3)
		require.Nil(t, response.NextPaginationToken)
		require.Nil(t, response.PreviousPaginationToken)
	})

	t.Run("list jobs from wait group with pagination", func(t *testing.T) {
		waitGroupsCore := newWaitGroupsCore(t)

		now := time.Now()

		namespaceId := &corepb.NamespaceId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
		}

		// Create wait group
		_, err := waitGroupsCore.CreateWaitGroup(&corepb.CreateWaitGroupRequest{
			WaitGroupId: &corepb.WaitGroupId{
				AccountId:   namespaceId.AccountId,
				NamespaceId: namespaceId.NamespaceId,
				WaitGroupId: rand.Uint64(),
			},
			Name:                              "test_wait_group",
			Description:                       "test description",
			Counter:                           100,
			Now:                               now.UnixNano(),
			ExpiresAt:                         now.Add(time.Hour).UnixNano(),
			MaxNumberOfWaitGroupsPerNamespace: 100,
		})
		require.NoError(t, err)

		// Complete many jobs
		processIds := make([]string, 50)
		for i := 0; i < 50; i++ {
			processIds[i] = fmt.Sprintf("process_%d", i)
		}
		_, err = waitGroupsCore.CompleteJobsFromWaitGroup(&corepb.CompleteJobsFromWaitGroupRequest{
			NamespaceId:   namespaceId,
			WaitGroupName: "test_wait_group",
			ProcessIds:    processIds,
			Now:           now.Add(time.Minute).UnixNano(),
		})
		require.NoError(t, err)

		// List first page with limit
		response1, err := waitGroupsCore.ListWaitGroupJobs(&corepb.ListWaitGroupJobsRequest{
			NamespaceId:   namespaceId,
			WaitGroupName: "test_wait_group",
			Limit:         20,
		})

		require.NoError(t, err)
		require.Len(t, response1.Jobs, 20)
		require.NotNil(t, response1.NextPaginationToken)
		require.Nil(t, response1.PreviousPaginationToken)

		// List second page
		response2, err := waitGroupsCore.ListWaitGroupJobs(&corepb.ListWaitGroupJobsRequest{
			NamespaceId:     namespaceId,
			WaitGroupName:   "test_wait_group",
			Limit:           20,
			PaginationToken: response1.NextPaginationToken,
		})

		require.NoError(t, err)
		require.Len(t, response2.Jobs, 20)
		require.NotNil(t, response2.NextPaginationToken)
		require.NotNil(t, response2.PreviousPaginationToken)

		// List third page
		response3, err := waitGroupsCore.ListWaitGroupJobs(&corepb.ListWaitGroupJobsRequest{
			NamespaceId:     namespaceId,
			WaitGroupName:   "test_wait_group",
			Limit:           20,
			PaginationToken: response2.NextPaginationToken,
		})

		require.NoError(t, err)
		require.Len(t, response3.Jobs, 10)
		require.Nil(t, response3.NextPaginationToken)
		require.NotNil(t, response3.PreviousPaginationToken)
	})

	t.Run("list jobs from empty wait group", func(t *testing.T) {
		waitGroupsCore := newWaitGroupsCore(t)

		now := time.Now()

		namespaceId := &corepb.NamespaceId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
		}

		// Create wait group
		_, err := waitGroupsCore.CreateWaitGroup(&corepb.CreateWaitGroupRequest{
			WaitGroupId: &corepb.WaitGroupId{
				AccountId:   namespaceId.AccountId,
				NamespaceId: namespaceId.NamespaceId,
				WaitGroupId: rand.Uint64(),
			},
			Name:                              "test_wait_group",
			Description:                       "test description",
			Counter:                           10,
			Now:                               now.UnixNano(),
			ExpiresAt:                         now.Add(time.Hour).UnixNano(),
			MaxNumberOfWaitGroupsPerNamespace: 100,
		})
		require.NoError(t, err)

		// List wait group jobs (no jobs completed yet)
		response, err := waitGroupsCore.ListWaitGroupJobs(&corepb.ListWaitGroupJobsRequest{
			NamespaceId:   namespaceId,
			WaitGroupName: "test_wait_group",
		})

		require.NoError(t, err)
		require.Len(t, response.Jobs, 0)
		require.Nil(t, response.NextPaginationToken)
		require.Nil(t, response.PreviousPaginationToken)
	})

	t.Run("list jobs from nonexistent wait group", func(t *testing.T) {
		waitGroupsCore := newWaitGroupsCore(t)

		// Try to list jobs from a nonexistent wait group
		_, err := waitGroupsCore.ListWaitGroupJobs(&corepb.ListWaitGroupJobsRequest{
			NamespaceId: &corepb.NamespaceId{
				AccountId:   rand.Uint64(),
				NamespaceId: rand.Uint32(),
			},
			WaitGroupName: "nonexistent_wait_group",
		})

		require.Error(t, err)
	})
}

func TestCore_DeleteWaitGroup(t *testing.T) {
	t.Run("delete existing wait group", func(t *testing.T) {
		waitGroupsCore := newWaitGroupsCore(t)

		now := time.Now()
		expiresAt := now.Add(time.Hour).UnixNano()

		namespaceId := &corepb.NamespaceId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
		}

		// Create wait group
		_, err := waitGroupsCore.CreateWaitGroup(&corepb.CreateWaitGroupRequest{
			WaitGroupId: &corepb.WaitGroupId{
				AccountId:   namespaceId.AccountId,
				NamespaceId: namespaceId.NamespaceId,
				WaitGroupId: rand.Uint64(),
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
			NamespaceId:   namespaceId,
			WaitGroupName: "test_wait_group",
		})
		require.NoError(t, err)

		// Try to get deleted wait group
		_, err = waitGroupsCore.GetWaitGroupByName(&corepb.GetWaitGroupByNameRequest{
			NamespaceId:   namespaceId,
			WaitGroupName: "test_wait_group",
		})
		require.Error(t, err)
	})

	t.Run("delete nonexistent wait group", func(t *testing.T) {
		waitGroupsCore := newWaitGroupsCore(t)

		// Try to delete a nonexistent wait group
		_, err := waitGroupsCore.DeleteWaitGroup(&corepb.DeleteWaitGroupRequest{
			NamespaceId: &corepb.NamespaceId{
				AccountId:   rand.Uint64(),
				NamespaceId: rand.Uint32(),
			},
			WaitGroupName: "nonexistent_wait_group",
		})

		// Deleting a nonexistent wait group does not return errors
		require.NoError(t, err)
	})

	t.Run("large wait group", func(t *testing.T) {
		waitGroupsCore := newWaitGroupsCore(t)

		now := time.Now()
		groupSize := uint64(1_000_000)

		waitGroupId := &corepb.WaitGroupId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
			WaitGroupId: rand.Uint64(),
		}

		// Create a large wait group
		createResponse, err := waitGroupsCore.CreateWaitGroup(&corepb.CreateWaitGroupRequest{
			WaitGroupId:                       waitGroupId,
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
				NamespaceId: &corepb.NamespaceId{
					AccountId:   waitGroupId.AccountId,
					NamespaceId: waitGroupId.NamespaceId,
				},
				WaitGroupName: "test_large_wait_group",
				ProcessIds:    processIds,
				Now:           now.Add(time.Duration(completedJobs) * time.Millisecond).UnixNano(),
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
			RecordId: rand.Uint64(),
			NamespaceId: &corepb.NamespaceId{
				AccountId:   waitGroupId.AccountId,
				NamespaceId: waitGroupId.NamespaceId,
			},
			WaitGroupName: "test_large_wait_group",
		})
		require.NoError(t, err)
	})
}

func TestCore_SnapshotAndRestore(t *testing.T) {
	now := time.Now()

	waitGroupId := &corepb.WaitGroupId{
		AccountId:   rand.Uint64(),
		NamespaceId: rand.Uint32(),
		WaitGroupId: rand.Uint64(),
	}

	// Create two wait group cores for testing snapshot and restore
	waitGroupsCore1 := newWaitGroupsCore(t)
	waitGroupsCore2 := newWaitGroupsCore(t)

	// T+0: Create wait group
	_, err := waitGroupsCore1.CreateWaitGroup(&corepb.CreateWaitGroupRequest{
		WaitGroupId:                       waitGroupId,
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
		NamespaceId: &corepb.NamespaceId{
			AccountId:   waitGroupId.AccountId,
			NamespaceId: waitGroupId.NamespaceId,
		},
		WaitGroupName: "test_wait_group",
		ProcessIds:    []string{"process_1", "process_2", "process_3"},
		Now:           now.Add(time.Minute).UnixNano(),
	})
	require.NoError(t, err)

	// Take snapshot at this point
	snapshot := waitGroupsCore1.Snapshot()

	// T+2m: Complete more jobs (after snapshot)
	_, err = waitGroupsCore1.CompleteJobsFromWaitGroup(&corepb.CompleteJobsFromWaitGroupRequest{
		NamespaceId: &corepb.NamespaceId{
			AccountId:   waitGroupId.AccountId,
			NamespaceId: waitGroupId.NamespaceId,
		},
		WaitGroupName: "test_wait_group",
		ProcessIds:    []string{"process_4", "process_5"},
		Now:           now.Add(2 * time.Minute).UnixNano(),
	})
	require.NoError(t, err)

	// T+3m: Add more jobs to wait group (after snapshot)
	_, err = waitGroupsCore1.AddJobsToWaitGroup(&corepb.AddJobsToWaitGroupRequest{
		NamespaceId: &corepb.NamespaceId{
			AccountId:   waitGroupId.AccountId,
			NamespaceId: waitGroupId.NamespaceId,
		},
		WaitGroupName:    "test_wait_group",
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
		NamespaceId: &corepb.NamespaceId{
			AccountId:   waitGroupId.AccountId,
			NamespaceId: waitGroupId.NamespaceId,
		},
		WaitGroupName: "test_wait_group",
		ProcessIds:    []string{"process_6", "process_7"},
		Now:           now.Add(5 * time.Minute).UnixNano(),
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
		NamespaceId: &corepb.NamespaceId{
			AccountId:   waitGroupId.AccountId,
			NamespaceId: waitGroupId.NamespaceId,
		},
		WaitGroupName:    "test_wait_group",
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

func TestCore_RunWaitGroupsGarbageCollection(t *testing.T) {
	waitGroupsCore := newWaitGroupsCore(t)
	now := time.Now()

	// Test parameters - use smaller limits to force multiple runs
	maxDeletedObjects := int64(1000)
	gcRecordsPageSize := int64(100)
	gcRecordWaitGroupsPageSize := int64(1000)

	// Create test data for account 1
	accountId1 := rand.Uint64()
	namespaceId1 := &corepb.NamespaceId{
		AccountId:   accountId1,
		NamespaceId: rand.Uint32(),
	}

	// Create multiple wait groups for account 1 with many jobs each
	numWaitGroups := 3
	jobsPerGroup := 500 // 1500 total jobs, exceeding MaxDeletedObjects
	waitGroupIds := make([]*corepb.WaitGroupId, numWaitGroups)

	for i := 0; i < numWaitGroups; i++ {
		waitGroupName := fmt.Sprintf("test_wait_group_%d", i)
		waitGroupIds[i] = &corepb.WaitGroupId{
			AccountId:   namespaceId1.AccountId,
			NamespaceId: namespaceId1.NamespaceId,
			WaitGroupId: rand.Uint64(),
		}

		// Create wait group with many jobs
		_, err := waitGroupsCore.CreateWaitGroup(&corepb.CreateWaitGroupRequest{
			WaitGroupId:                       waitGroupIds[i],
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
			NamespaceId: &corepb.NamespaceId{
				AccountId:   waitGroupIds[i].AccountId,
				NamespaceId: waitGroupIds[i].NamespaceId,
			},
			WaitGroupName: waitGroupName,
			ProcessIds:    processIds,
			Now:           now.Add(time.Minute).UnixNano(),
		})
		require.NoError(t, err)
	}

	// Create test data for account 2 with multiple namespaces
	accountId2 := rand.Uint64()
	numNamespaces := 2
	waitGroupsPerNamespace := 3
	jobsPerWaitGroup := 400 // 1200 jobs per namespace
	namespaceIds := make([]*corepb.NamespaceId, numNamespaces)

	for ns := 0; ns < numNamespaces; ns++ {
		namespaceIds[ns] = &corepb.NamespaceId{
			AccountId:   accountId2,
			NamespaceId: rand.Uint32(),
		}

		// Create multiple wait groups for this namespace
		for wg := 0; wg < waitGroupsPerNamespace; wg++ {
			waitGroupName := fmt.Sprintf("test_wait_group_%d", wg)
			waitGroupId := &corepb.WaitGroupId{
				AccountId:   namespaceIds[ns].AccountId,
				NamespaceId: namespaceIds[ns].NamespaceId,
				WaitGroupId: uint64(wg),
			}

			// Create wait group with many jobs
			_, err := waitGroupsCore.CreateWaitGroup(&corepb.CreateWaitGroupRequest{
				WaitGroupId:                       waitGroupId,
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
				NamespaceId: &corepb.NamespaceId{
					AccountId:   waitGroupId.AccountId,
					NamespaceId: waitGroupId.NamespaceId,
				},
				WaitGroupName: waitGroupName,
				ProcessIds:    processIds,
				Now:           now.Add(time.Minute).UnixNano(),
			})
			require.NoError(t, err)
		}
	}

	// Delete one wait group from account 1 (this will create a GC record for individual wait group)
	_, err := waitGroupsCore.DeleteWaitGroup(&corepb.DeleteWaitGroupRequest{
		RecordId: rand.Uint64(),
		NamespaceId: &corepb.NamespaceId{
			AccountId:   waitGroupIds[0].AccountId,
			NamespaceId: waitGroupIds[0].NamespaceId,
		},
		WaitGroupName: "test_wait_group_0",
	})
	require.NoError(t, err)

	// Delete one namespace from account 2 (this will create a GC record for entire namespace)
	namespaceToDelete := namespaceIds[1]

	_, err = waitGroupsCore.WaitGroupsDeleteNamespace(&corepb.WaitGroupsDeleteNamespaceRequest{
		RecordId:    rand.Uint64(),
		NamespaceId: namespaceToDelete,
		Now:         now.UnixNano(),
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
			AccountId:   namespaceToDelete.AccountId,
			NamespaceId: namespaceToDelete.NamespaceId,
			WaitGroupId: 0,
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
			AccountId:   namespaceToDelete.AccountId,
			NamespaceId: namespaceToDelete.NamespaceId,
			WaitGroupId: 0,
		},
	})
	require.Error(t, err, "Deleted namespace's wait groups should not be accessible")

	// Verify that other wait groups from account 1 are still accessible
	_, err = waitGroupsCore.GetWaitGroup(&corepb.GetWaitGroupRequest{
		WaitGroupId: waitGroupIds[1],
	})
	require.NoError(t, err, "Other wait groups should still be accessible")

	// Verify that the non-deleted namespace from account 2 is still accessible
	// namespaceIds[0] should still be accessible (only namespaceIds[1] was deleted)
	remainingNamespaceWaitGroupId := &corepb.WaitGroupId{
		AccountId:   namespaceIds[0].AccountId,
		NamespaceId: namespaceIds[0].NamespaceId,
		WaitGroupId: 0, // First wait group in that namespace
	}
	_, err = waitGroupsCore.GetWaitGroup(&corepb.GetWaitGroupRequest{
		WaitGroupId: remainingNamespaceWaitGroupId,
	})
	require.NoError(t, err, "Other namespaces should still be accessible")

	t.Logf("Garbage collection completed successfully in %d runs", runCount)
}

func newWaitGroupsCore(t *testing.T) *Core {
	store, err := store.NewBadgerInMemoryStore()
	require.NoError(t, err)
	return NewCore(store, []byte{0x1d, 0x36, 0x00, 0x00}, []byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})
}
