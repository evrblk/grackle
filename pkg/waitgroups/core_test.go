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

	"github.com/evrblk/grackle/pkg/coreapis"
	"github.com/evrblk/grackle/pkg/corepb"
	"github.com/evrblk/grackle/pkg/tables"
)

func init() {
	registry := monsterax.NewBaseTableRegistry(1)
	tables.RegisterGracklePrefixes(registry)
}

func TestCore_Create(t *testing.T) {
	t.Run("create wait group", func(t *testing.T) {
		core := newWaitGroupsCore(t)
		now := time.Now()
		expiresAt := now.Add(time.Hour).UnixNano()
		waitGroupId := &corepb.WaitGroupId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint64(),
			WaitGroupId: rand.Uint64(),
		}

		// Create wait group
		resp1, err := core.CreateWaitGroup(&coreapis.CreateWaitGroupRequest{
			Payload: &corepb.CreateWaitGroupRequest{
				WaitGroupId:                       waitGroupId,
				Name:                              "test_wait_group",
				Description:                       "test description",
				Counter:                           10,
				Now:                               now.UnixNano(),
				ExpiresAt:                         expiresAt,
				MaxNumberOfWaitGroupsPerNamespace: 100,
				DeleteAfterFinishedSeconds:        3600,
			},
		})

		require.NoError(t, err)
		require.NotNil(t, resp1)
		require.Nil(t, resp1.ApplicationError)
		require.NotNil(t, resp1.Payload)
		require.NotNil(t, resp1.Payload.WaitGroup)
		require.Equal(t, "test_wait_group", resp1.Payload.WaitGroup.Name)
		require.Equal(t, "test description", resp1.Payload.WaitGroup.Description)
		require.EqualValues(t, 10, resp1.Payload.WaitGroup.Counter)
		require.EqualValues(t, 0, resp1.Payload.WaitGroup.CompletedJobs)
		require.Equal(t, expiresAt, resp1.Payload.WaitGroup.ExpiresAt)
		// A newly created wait group is active, has no finish time, and carries
		// the configured retention period.
		require.Equal(t, corepb.WaitGroupStatus_WAIT_GROUP_STATUS_ACTIVE, resp1.Payload.WaitGroup.Status)
		require.EqualValues(t, 0, resp1.Payload.WaitGroup.FinishedAt)
		require.EqualValues(t, 3600, resp1.Payload.WaitGroup.DeleteAfterFinishedSeconds)

		// Get wait group
		resp2, err := core.GetWaitGroup(&coreapis.GetWaitGroupRequest{
			Payload: &corepb.GetWaitGroupRequest{
				WaitGroupId: waitGroupId,
			},
		})

		require.NoError(t, err)
		require.NotNil(t, resp2)
		require.Nil(t, resp2.ApplicationError)
		require.NotNil(t, resp2.Payload)
		require.NotNil(t, resp2.Payload.WaitGroup)
		require.Equal(t, "test_wait_group", resp2.Payload.WaitGroup.Name)
		require.Equal(t, "test description", resp2.Payload.WaitGroup.Description)
		require.EqualValues(t, 10, resp2.Payload.WaitGroup.Counter)
		require.EqualValues(t, 0, resp2.Payload.WaitGroup.CompletedJobs)

		// Get wait group by name
		resp3, err := core.GetWaitGroupByName(&coreapis.GetWaitGroupByNameRequest{
			Payload: &corepb.GetWaitGroupByNameRequest{
				NamespaceId: &corepb.NamespaceId{
					AccountId:   waitGroupId.AccountId,
					NamespaceId: waitGroupId.NamespaceId,
				},
				WaitGroupName: "test_wait_group",
			},
		})

		require.NoError(t, err)
		require.NotNil(t, resp3)
		require.Nil(t, resp3.ApplicationError)
		require.NotNil(t, resp3.Payload)
		require.NotNil(t, resp3.Payload.WaitGroup)
		require.Equal(t, "test_wait_group", resp3.Payload.WaitGroup.Name)
		require.Equal(t, "test description", resp3.Payload.WaitGroup.Description)
		require.EqualValues(t, 10, resp3.Payload.WaitGroup.Counter)
		require.EqualValues(t, 0, resp3.Payload.WaitGroup.CompletedJobs)
	})

	t.Run("create wait group with duplicate name", func(t *testing.T) {
		core := newWaitGroupsCore(t)
		now := time.Now()
		expiresAt := now.Add(time.Hour).UnixNano()
		accountId := rand.Uint64()
		namespaceId := rand.Uint64()

		// Create first wait group
		resp1, err := core.CreateWaitGroup(&coreapis.CreateWaitGroupRequest{
			Payload: &corepb.CreateWaitGroupRequest{
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
			},
		})
		require.NoError(t, err)
		require.NotNil(t, resp1)
		require.Nil(t, resp1.ApplicationError)
		require.NotNil(t, resp1.Payload)

		// Try to create a second wait group with the same name in the same namespace
		resp2, err := core.CreateWaitGroup(&coreapis.CreateWaitGroupRequest{
			Payload: &corepb.CreateWaitGroupRequest{
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
			},
		})

		require.NoError(t, err)
		require.NotNil(t, resp2)
		require.Nil(t, resp2.Payload)
		require.NotNil(t, resp2.ApplicationError)
		require.Equal(t, monsterax.AlreadyExists, resp2.ApplicationError.Code)
	})

	t.Run("maximum number of wait groups per namespace", func(t *testing.T) {
		core := newWaitGroupsCore(t)
		now := time.Now()
		expiresAt := now.Add(time.Hour).UnixNano()
		maxWaitGroups := int64(3) // Use a small number for testing
		accountId := rand.Uint64()
		namespaceId := rand.Uint64()

		// Create wait groups up to the limit
		for i := range maxWaitGroups {
			resp, err := core.CreateWaitGroup(&coreapis.CreateWaitGroupRequest{
				Payload: &corepb.CreateWaitGroupRequest{
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
				},
			})
			require.NoError(t, err, "Failed to create wait group %d", i)
			require.NotNil(t, resp)
			require.Nil(t, resp.ApplicationError, "Failed to create wait group %d", i)
			require.NotNil(t, resp.Payload)
			require.NotNil(t, resp.Payload.WaitGroup)
		}

		// Try to create one more wait group, which should fail
		resp, err := core.CreateWaitGroup(&coreapis.CreateWaitGroupRequest{
			Payload: &corepb.CreateWaitGroupRequest{
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
			},
		})

		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Nil(t, resp.Payload)
		require.NotNil(t, resp.ApplicationError)
		require.Equal(t, monsterax.ResourceExhausted, resp.ApplicationError.Code)
	})
}

func TestCore_CompleteJobsFromWaitGroup(t *testing.T) {
	t.Run("complete jobs from existing wait group", func(t *testing.T) {
		core := newWaitGroupsCore(t)
		now := time.Now()
		namespaceId := &corepb.NamespaceId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint64(),
		}

		// T+0: Create wait group
		resp1, err := core.CreateWaitGroup(&coreapis.CreateWaitGroupRequest{
			Payload: &corepb.CreateWaitGroupRequest{
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
			},
		})
		require.NoError(t, err)
		require.NotNil(t, resp1)
		require.Nil(t, resp1.ApplicationError)
		require.NotNil(t, resp1.Payload)

		// T+1m: Complete jobs
		resp2, err := core.CompleteJobsFromWaitGroup(&coreapis.CompleteJobsFromWaitGroupRequest{
			Payload: &corepb.CompleteJobsFromWaitGroupRequest{
				NamespaceId:   namespaceId,
				WaitGroupName: "test_wait_group",
				Jobs:          completeJobRequests([]string{"job_1", "job_2", "job_3"}),
				Now:           now.Add(time.Minute).UnixNano(),
			},
		})

		require.NoError(t, err)
		require.NotNil(t, resp2)
		require.Nil(t, resp2.ApplicationError)
		require.NotNil(t, resp2.Payload.WaitGroup)
		require.EqualValues(t, 10, resp2.Payload.WaitGroup.Counter)
		require.EqualValues(t, 3, resp2.Payload.WaitGroup.CompletedJobs)

		// T+2m: Complete same jobs again (should not increase completed counter)
		resp3, err := core.CompleteJobsFromWaitGroup(&coreapis.CompleteJobsFromWaitGroupRequest{
			Payload: &corepb.CompleteJobsFromWaitGroupRequest{
				NamespaceId:   namespaceId,
				WaitGroupName: "test_wait_group",
				Jobs:          completeJobRequests([]string{"job_1", "job_2"}),
				Now:           now.Add(2 * time.Minute).UnixNano(),
			},
		})

		require.NoError(t, err)
		require.NotNil(t, resp3)
		require.Nil(t, resp3.ApplicationError)
		require.NotNil(t, resp3.Payload.WaitGroup)
		require.EqualValues(t, 10, resp3.Payload.WaitGroup.Counter)
		require.EqualValues(t, 3, resp3.Payload.WaitGroup.CompletedJobs)
	})

	t.Run("complete jobs from nonexistent wait group", func(t *testing.T) {
		core := newWaitGroupsCore(t)
		now := time.Now()

		// Try to complete jobs from a nonexistent wait group
		resp1, err := core.CompleteJobsFromWaitGroup(&coreapis.CompleteJobsFromWaitGroupRequest{
			Payload: &corepb.CompleteJobsFromWaitGroupRequest{
				NamespaceId: &corepb.NamespaceId{
					AccountId:   rand.Uint64(),
					NamespaceId: rand.Uint64(),
				},
				WaitGroupName: "nonexistent_wait_group",
				Jobs:          completeJobRequests([]string{"job_1", "job_2", "job_3"}),
				Now:           now.UnixNano(),
			},
		})

		require.NoError(t, err)
		require.NotNil(t, resp1)
		require.Nil(t, resp1.Payload)
		require.NotNil(t, resp1.ApplicationError)
		require.Equal(t, monsterax.NotFound, resp1.ApplicationError.Code)
	})

	t.Run("counter overflow completes wait group", func(t *testing.T) {
		core := newWaitGroupsCore(t)
		now := time.Now()
		namespaceId := &corepb.NamespaceId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint64(),
		}
		waitGroupId := &corepb.WaitGroupId{
			AccountId:   namespaceId.AccountId,
			NamespaceId: namespaceId.NamespaceId,
			WaitGroupId: rand.Uint64(),
		}

		// T+0: Create wait group with a small counter
		_ = createWaitGroup(t, core, waitGroupId, "test_wait_group", 2, now)

		// Completing more jobs than the counter allows must be rejected with InvalidArgument
		appErr := completeJobsFromWaitGroupWithError(t, core, namespaceId, "test_wait_group", []string{"job_1", "job_2", "job_3"}, now.Add(time.Minute))
		require.Equal(t, monsterax.InvalidArgument, appErr.Code)

		// The failed request must not have persisted any state: Completed stays at 0
		// and no job rows were written.
		wg := getWaitGroup(t, core, waitGroupId)
		require.EqualValues(t, 2, wg.Counter)
		require.EqualValues(t, 0, wg.CompletedJobs)

		jobsList := ListWaitGroupCompletedJobs(t, core, namespaceId, "test_wait_group")
		require.Empty(t, jobsList.Jobs)

		// Completing exactly Counter jobs succeeds
		wg = completeJobsFromWaitGroup(t, core, namespaceId, "test_wait_group", []string{"job_1", "job_2"}, now.Add(2*time.Minute))
		require.EqualValues(t, 2, wg.CompletedJobs)

		// Adding more new jobs on top now overflows and must be rejected
		appErr = completeJobsFromWaitGroupWithError(t, core, namespaceId, "test_wait_group", []string{"job_3"}, now.Add(3*time.Minute))
		require.Equal(t, monsterax.InvalidArgument, appErr.Code)
	})
}

func TestCore_ListWaitGroups(t *testing.T) {
	core := newWaitGroupsCore(t)
	now := time.Now()
	accountId := rand.Uint64()
	namespaceId := rand.Uint64()

	// T+0: Create first wait group
	resp1, err := core.CreateWaitGroup(&coreapis.CreateWaitGroupRequest{
		Payload: &corepb.CreateWaitGroupRequest{
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
		},
	})
	require.NoError(t, err)
	require.NotNil(t, resp1)
	require.Nil(t, resp1.ApplicationError)
	require.NotNil(t, resp1.Payload)

	// T+1m: Create second wait group
	resp2, err := core.CreateWaitGroup(&coreapis.CreateWaitGroupRequest{
		Payload: &corepb.CreateWaitGroupRequest{
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
		},
	})
	require.NoError(t, err)
	require.NotNil(t, resp2)
	require.Nil(t, resp2.ApplicationError)
	require.NotNil(t, resp2.Payload)

	// List wait groups
	resp3, err := core.ListWaitGroups(&coreapis.ListWaitGroupsRequest{
		Payload: &corepb.ListWaitGroupsRequest{
			NamespaceId: &corepb.NamespaceId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
			},
		},
	})

	require.NoError(t, err)
	require.NotNil(t, resp3)
	require.Nil(t, resp3.ApplicationError)
	require.NotNil(t, resp3.Payload)
	require.Len(t, resp3.Payload.WaitGroups, 2)
}

func TestCore_ListWaitGroupCompletedJobs(t *testing.T) {
	t.Run("list jobs from wait group with multiple jobs", func(t *testing.T) {
		core := newWaitGroupsCore(t)
		now := time.Now()
		namespaceId := &corepb.NamespaceId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint64(),
		}

		// Create wait group
		resp1, err := core.CreateWaitGroup(&coreapis.CreateWaitGroupRequest{
			Payload: &corepb.CreateWaitGroupRequest{
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
			},
		})
		require.NoError(t, err)
		require.NotNil(t, resp1)
		require.Nil(t, resp1.ApplicationError)
		require.NotNil(t, resp1.Payload)

		// Complete some jobs
		resp2, err := core.CompleteJobsFromWaitGroup(&coreapis.CompleteJobsFromWaitGroupRequest{
			Payload: &corepb.CompleteJobsFromWaitGroupRequest{
				NamespaceId:   namespaceId,
				WaitGroupName: "test_wait_group",
				Jobs:          completeJobRequests([]string{"job_1", "job_2", "job_3"}),
				Now:           now.Add(time.Minute).UnixNano(),
			},
		})
		require.NoError(t, err)
		require.NotNil(t, resp2)
		require.Nil(t, resp2.ApplicationError)
		require.NotNil(t, resp2.Payload)

		// List wait group jobs
		resp3, err := core.ListWaitGroupCompletedJobs(&coreapis.ListWaitGroupCompletedJobsRequest{
			Payload: &corepb.ListWaitGroupCompletedJobsRequest{
				NamespaceId:   namespaceId,
				WaitGroupName: "test_wait_group",
			},
		})

		require.NoError(t, err)
		require.NotNil(t, resp3)
		require.Nil(t, resp3.ApplicationError)
		require.NotNil(t, resp3.Payload)
		require.Len(t, resp3.Payload.Jobs, 3)
		require.Nil(t, resp3.Payload.NextPaginationToken)
		require.Nil(t, resp3.Payload.PreviousPaginationToken)
	})

	t.Run("list jobs from wait group with pagination", func(t *testing.T) {
		core := newWaitGroupsCore(t)
		now := time.Now()
		namespaceId := &corepb.NamespaceId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint64(),
		}

		// Create wait group
		resp1, err := core.CreateWaitGroup(&coreapis.CreateWaitGroupRequest{
			Payload: &corepb.CreateWaitGroupRequest{
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
			},
		})
		require.NoError(t, err)
		require.NotNil(t, resp1)
		require.Nil(t, resp1.ApplicationError)
		require.NotNil(t, resp1.Payload)

		// Complete many jobs
		jobIds := make([]string, 50)
		for i := range 50 {
			jobIds[i] = fmt.Sprintf("job_%d", i)
		}
		resp2, err := core.CompleteJobsFromWaitGroup(&coreapis.CompleteJobsFromWaitGroupRequest{
			Payload: &corepb.CompleteJobsFromWaitGroupRequest{
				NamespaceId:   namespaceId,
				WaitGroupName: "test_wait_group",
				Jobs:          completeJobRequests(jobIds),
				Now:           now.Add(time.Minute).UnixNano(),
			},
		})
		require.NoError(t, err)
		require.NotNil(t, resp2)
		require.Nil(t, resp2.ApplicationError)
		require.NotNil(t, resp2.Payload)

		// List first page with limit
		resp3, err := core.ListWaitGroupCompletedJobs(&coreapis.ListWaitGroupCompletedJobsRequest{
			Payload: &corepb.ListWaitGroupCompletedJobsRequest{
				NamespaceId:   namespaceId,
				WaitGroupName: "test_wait_group",
				Limit:         20,
			},
		})

		require.NoError(t, err)
		require.NotNil(t, resp3)
		require.NotNil(t, resp3.Payload)
		require.Len(t, resp3.Payload.Jobs, 20)
		require.NotNil(t, resp3.Payload.NextPaginationToken)
		require.Nil(t, resp3.Payload.PreviousPaginationToken)

		// List second page
		resp4, err := core.ListWaitGroupCompletedJobs(&coreapis.ListWaitGroupCompletedJobsRequest{
			Payload: &corepb.ListWaitGroupCompletedJobsRequest{
				NamespaceId:     namespaceId,
				WaitGroupName:   "test_wait_group",
				Limit:           20,
				PaginationToken: resp3.Payload.NextPaginationToken,
			},
		})

		require.NoError(t, err)
		require.NotNil(t, resp4)
		require.NotNil(t, resp4.Payload)
		require.Len(t, resp4.Payload.Jobs, 20)
		require.NotNil(t, resp4.Payload.NextPaginationToken)
		require.NotNil(t, resp4.Payload.PreviousPaginationToken)

		// List third page
		resp5, err := core.ListWaitGroupCompletedJobs(&coreapis.ListWaitGroupCompletedJobsRequest{
			Payload: &corepb.ListWaitGroupCompletedJobsRequest{
				NamespaceId:     namespaceId,
				WaitGroupName:   "test_wait_group",
				Limit:           20,
				PaginationToken: resp4.Payload.NextPaginationToken,
			},
		})

		require.NoError(t, err)
		require.NotNil(t, resp5)
		require.NotNil(t, resp5.Payload)
		require.Len(t, resp5.Payload.Jobs, 10)
		require.Nil(t, resp5.Payload.NextPaginationToken)
		require.NotNil(t, resp5.Payload.PreviousPaginationToken)
	})

	t.Run("list jobs from empty wait group", func(t *testing.T) {
		core := newWaitGroupsCore(t)
		now := time.Now()
		namespaceId := &corepb.NamespaceId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint64(),
		}

		// Create wait group
		resp1, err := core.CreateWaitGroup(&coreapis.CreateWaitGroupRequest{
			Payload: &corepb.CreateWaitGroupRequest{
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
			},
		})
		require.NoError(t, err)
		require.NotNil(t, resp1)
		require.NotNil(t, resp1.Payload)

		// List wait group jobs (no jobs completed yet)
		resp2, err := core.ListWaitGroupCompletedJobs(&coreapis.ListWaitGroupCompletedJobsRequest{
			Payload: &corepb.ListWaitGroupCompletedJobsRequest{
				NamespaceId:   namespaceId,
				WaitGroupName: "test_wait_group",
			},
		})

		require.NoError(t, err)
		require.Len(t, resp2.Payload.Jobs, 0)
		require.Nil(t, resp2.Payload.NextPaginationToken)
		require.Nil(t, resp2.Payload.PreviousPaginationToken)
	})

	t.Run("list jobs from nonexistent wait group", func(t *testing.T) {
		core := newWaitGroupsCore(t)

		// Try to list jobs from a nonexistent wait group
		resp1, err := core.ListWaitGroupCompletedJobs(&coreapis.ListWaitGroupCompletedJobsRequest{
			Payload: &corepb.ListWaitGroupCompletedJobsRequest{
				NamespaceId: &corepb.NamespaceId{
					AccountId:   rand.Uint64(),
					NamespaceId: rand.Uint64(),
				},
				WaitGroupName: "nonexistent_wait_group",
			},
		})

		require.NoError(t, err)
		require.NotNil(t, resp1)
		require.Nil(t, resp1.Payload)
		require.NotNil(t, resp1.ApplicationError)
		require.Equal(t, monsterax.NotFound, resp1.ApplicationError.Code)
	})
}

func TestCore_DeleteWaitGroup(t *testing.T) {
	t.Run("delete existing wait group", func(t *testing.T) {
		core := newWaitGroupsCore(t)
		now := time.Now()
		expiresAt := now.Add(time.Hour).UnixNano()
		namespaceId := &corepb.NamespaceId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint64(),
		}

		// Create wait group
		resp1, err := core.CreateWaitGroup(&coreapis.CreateWaitGroupRequest{
			Payload: &corepb.CreateWaitGroupRequest{
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
			},
		})
		require.NoError(t, err)
		require.NotNil(t, resp1)
		require.NotNil(t, resp1.Payload)

		// Delete wait group
		resp2, err := core.DeleteWaitGroup(&coreapis.DeleteWaitGroupRequest{
			Payload: &corepb.DeleteWaitGroupRequest{
				NamespaceId:   namespaceId,
				WaitGroupName: "test_wait_group",
			},
		})
		require.NoError(t, err)
		require.NotNil(t, resp2)
		require.NotNil(t, resp2.Payload)

		// Try to get deleted wait group
		resp3, err := core.GetWaitGroupByName(&coreapis.GetWaitGroupByNameRequest{
			Payload: &corepb.GetWaitGroupByNameRequest{
				NamespaceId:   namespaceId,
				WaitGroupName: "test_wait_group",
			},
		})
		require.NoError(t, err)
		require.NotNil(t, resp3)
		require.Nil(t, resp3.Payload)
		require.NotNil(t, resp3.ApplicationError)
		require.Equal(t, monsterax.NotFound, resp3.ApplicationError.Code)
	})

	t.Run("delete nonexistent wait group", func(t *testing.T) {
		core := newWaitGroupsCore(t)

		// Try to delete a nonexistent wait group
		resp1, err := core.DeleteWaitGroup(&coreapis.DeleteWaitGroupRequest{
			Payload: &corepb.DeleteWaitGroupRequest{
				NamespaceId: &corepb.NamespaceId{
					AccountId:   rand.Uint64(),
					NamespaceId: rand.Uint64(),
				},
				WaitGroupName: "nonexistent_wait_group",
			},
		})

		// Deleting a nonexistent wait group does not return errors
		require.NoError(t, err)
		require.NotNil(t, resp1)
		require.NotNil(t, resp1.Payload)
		require.Nil(t, resp1.ApplicationError)
	})

	t.Run("large wait group", func(t *testing.T) {
		core := newWaitGroupsCore(t)
		now := time.Now()
		groupSize := int64(1_000_000)
		waitGroupId := &corepb.WaitGroupId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint64(),
			WaitGroupId: rand.Uint64(),
		}

		// Create a large wait group
		resp1, err := core.CreateWaitGroup(&coreapis.CreateWaitGroupRequest{
			Payload: &corepb.CreateWaitGroupRequest{
				WaitGroupId:                       waitGroupId,
				Name:                              "test_large_wait_group",
				Description:                       "test description",
				Counter:                           groupSize,
				Now:                               now.UnixNano(),
				ExpiresAt:                         now.Add(time.Hour).UnixNano(),
				MaxNumberOfWaitGroupsPerNamespace: 100,
			},
		})

		require.NoError(t, err)
		require.NotNil(t, resp1)
		require.NotNil(t, resp1.Payload)
		require.NotNil(t, resp1.Payload.WaitGroup)
		require.EqualValues(t, groupSize, resp1.Payload.WaitGroup.Counter)
		require.EqualValues(t, 0, resp1.Payload.WaitGroup.CompletedJobs)

		// Complete all jobs in batches
		batchSize := 10
		jobIds := make([]string, batchSize)
		completedJobs := int64(0)

		for completedJobs < groupSize {
			for i := range batchSize {
				jobIds[i] = fmt.Sprintf("job_%d", completedJobs+int64(i))
			}

			resp2, err := core.CompleteJobsFromWaitGroup(&coreapis.CompleteJobsFromWaitGroupRequest{
				Payload: &corepb.CompleteJobsFromWaitGroupRequest{
					NamespaceId: &corepb.NamespaceId{
						AccountId:   waitGroupId.AccountId,
						NamespaceId: waitGroupId.NamespaceId,
					},
					WaitGroupName: "test_large_wait_group",
					Jobs:          completeJobRequests(jobIds),
					Now:           now.Add(time.Duration(completedJobs) * time.Millisecond).UnixNano(),
				},
			})

			require.NoError(t, err)
			require.NotNil(t, resp2)
			require.Nil(t, resp2.ApplicationError)
			require.NotNil(t, resp2.Payload)
			require.NotNil(t, resp2.Payload.WaitGroup)
			completedJobs += int64(batchSize)
		}

		// Verify final state
		resp3, err := core.GetWaitGroup(&coreapis.GetWaitGroupRequest{
			Payload: &corepb.GetWaitGroupRequest{
				WaitGroupId: waitGroupId,
			},
		})

		require.NoError(t, err)
		require.NotNil(t, resp3)
		require.Nil(t, resp3.ApplicationError)
		require.NotNil(t, resp3.Payload)
		require.NotNil(t, resp3.Payload.WaitGroup)
		require.EqualValues(t, groupSize, resp3.Payload.WaitGroup.Counter)
		require.EqualValues(t, groupSize, resp3.Payload.WaitGroup.CompletedJobs)

		// Delete wait group
		resp4, err := core.DeleteWaitGroup(&coreapis.DeleteWaitGroupRequest{
			Payload: &corepb.DeleteWaitGroupRequest{
				RecordId: rand.Uint64(),
				NamespaceId: &corepb.NamespaceId{
					AccountId:   waitGroupId.AccountId,
					NamespaceId: waitGroupId.NamespaceId,
				},
				WaitGroupName: "test_large_wait_group",
			},
		})
		require.NoError(t, err)
		require.NotNil(t, resp4)
		require.Nil(t, resp4.ApplicationError)
		require.NotNil(t, resp4.Payload)
	})
}

func TestCore_UpdateWaitGroup(t *testing.T) {
	t.Run("update description, expiration, and metadata", func(t *testing.T) {
		core := newWaitGroupsCore(t)
		now := time.Now()
		expiresAt := now.Add(time.Hour).UnixNano()
		namespaceId := &corepb.NamespaceId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint64(),
		}
		waitGroupId := &corepb.WaitGroupId{
			AccountId:   namespaceId.AccountId,
			NamespaceId: namespaceId.NamespaceId,
			WaitGroupId: rand.Uint64(),
		}

		// Create wait group with initial metadata
		resp1, err := core.CreateWaitGroup(&coreapis.CreateWaitGroupRequest{
			Payload: &corepb.CreateWaitGroupRequest{
				WaitGroupId:                       waitGroupId,
				Name:                              "test_wait_group",
				Description:                       "initial description",
				Counter:                           10,
				Now:                               now.UnixNano(),
				ExpiresAt:                         expiresAt,
				Metadata:                          map[string]string{"team": "search", "env": "staging"},
				MaxNumberOfWaitGroupsPerNamespace: 100,
			},
		})
		require.NoError(t, err)
		require.Nil(t, resp1.ApplicationError)
		require.Equal(t, map[string]string{"team": "search", "env": "staging"}, resp1.Payload.WaitGroup.Metadata)

		// Update description, expiration, and metadata
		newExpiresAt := now.Add(2 * time.Hour).UnixNano()
		resp2, err := core.UpdateWaitGroup(&coreapis.UpdateWaitGroupRequest{
			Payload: &corepb.UpdateWaitGroupRequest{
				NamespaceId:     namespaceId,
				WaitGroupName:   "test_wait_group",
				Description:     "updated description",
				ExpiresAt:       newExpiresAt,
				Metadata:        map[string]string{"team": "search", "env": "production"},
				Now:             now.Add(time.Minute).UnixNano(),
				ExpectedVersion: 1,
				Counter:         10,
			},
		})
		require.NoError(t, err)
		require.NotNil(t, resp2)
		require.Nil(t, resp2.ApplicationError)
		require.NotNil(t, resp2.Payload.WaitGroup)
		require.Equal(t, "updated description", resp2.Payload.WaitGroup.Description)
		require.Equal(t, newExpiresAt, resp2.Payload.WaitGroup.ExpiresAt)
		require.Equal(t, map[string]string{"team": "search", "env": "production"}, resp2.Payload.WaitGroup.Metadata)
		require.Equal(t, now.Add(time.Minute).UnixNano(), resp2.Payload.WaitGroup.UpdatedAt)

		// Name, counter, and completed are immutable through Update
		require.Equal(t, "test_wait_group", resp2.Payload.WaitGroup.Name)
		require.EqualValues(t, 10, resp2.Payload.WaitGroup.Counter)
		require.EqualValues(t, 0, resp2.Payload.WaitGroup.CompletedJobs)

		// Reload and confirm persisted
		wg := getWaitGroup(t, core, waitGroupId)
		require.Equal(t, "updated description", wg.Description)
		require.Equal(t, newExpiresAt, wg.ExpiresAt)
		require.Equal(t, map[string]string{"team": "search", "env": "production"}, wg.Metadata)
	})

	t.Run("clearing metadata", func(t *testing.T) {
		core := newWaitGroupsCore(t)
		now := time.Now()
		namespaceId := &corepb.NamespaceId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint64(),
		}
		waitGroupId := &corepb.WaitGroupId{
			AccountId:   namespaceId.AccountId,
			NamespaceId: namespaceId.NamespaceId,
			WaitGroupId: rand.Uint64(),
		}

		_, err := core.CreateWaitGroup(&coreapis.CreateWaitGroupRequest{
			Payload: &corepb.CreateWaitGroupRequest{
				WaitGroupId:                       waitGroupId,
				Name:                              "test_wait_group",
				Description:                       "desc",
				Counter:                           1,
				Now:                               now.UnixNano(),
				ExpiresAt:                         now.Add(time.Hour).UnixNano(),
				Metadata:                          map[string]string{"key": "value"},
				MaxNumberOfWaitGroupsPerNamespace: 100,
			},
		})
		require.NoError(t, err)

		// Update with no metadata clears it
		resp, err := core.UpdateWaitGroup(&coreapis.UpdateWaitGroupRequest{
			Payload: &corepb.UpdateWaitGroupRequest{
				NamespaceId:     namespaceId,
				WaitGroupName:   "test_wait_group",
				Description:     "desc",
				ExpiresAt:       now.Add(time.Hour).UnixNano(),
				Now:             now.Add(time.Minute).UnixNano(),
				ExpectedVersion: 1,
			},
		})
		require.NoError(t, err)
		require.Nil(t, resp.ApplicationError)
		require.Empty(t, resp.Payload.WaitGroup.Metadata)
	})

	t.Run("update nonexistent wait group", func(t *testing.T) {
		core := newWaitGroupsCore(t)
		now := time.Now()

		resp, err := core.UpdateWaitGroup(&coreapis.UpdateWaitGroupRequest{
			Payload: &corepb.UpdateWaitGroupRequest{
				NamespaceId: &corepb.NamespaceId{
					AccountId:   rand.Uint64(),
					NamespaceId: rand.Uint64(),
				},
				WaitGroupName: "nonexistent_wait_group",
				Description:   "updated description",
				ExpiresAt:     now.Add(time.Hour).UnixNano(),
				Now:           now.UnixNano(),
			},
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Nil(t, resp.Payload)
		require.NotNil(t, resp.ApplicationError)
		require.Equal(t, monsterax.NotFound, resp.ApplicationError.Code)
	})

	t.Run("changing expiration reschedules garbage collection", func(t *testing.T) {
		core := newWaitGroupsCore(t)
		now := time.Now()
		oldExpiresAt := now.Add(time.Hour).UnixNano()
		newExpiresAt := now.Add(48 * time.Hour).UnixNano()
		namespaceId := &corepb.NamespaceId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint64(),
		}
		waitGroupId := &corepb.WaitGroupId{
			AccountId:   namespaceId.AccountId,
			NamespaceId: namespaceId.NamespaceId,
			WaitGroupId: rand.Uint64(),
		}

		_, err := core.CreateWaitGroup(&coreapis.CreateWaitGroupRequest{
			Payload: &corepb.CreateWaitGroupRequest{
				WaitGroupId:                       waitGroupId,
				Name:                              "test_wait_group",
				Description:                       "desc",
				Counter:                           1,
				Now:                               now.UnixNano(),
				ExpiresAt:                         oldExpiresAt,
				MaxNumberOfWaitGroupsPerNamespace: 100,
			},
		})
		require.NoError(t, err)

		// Push expiration far into the future
		_, err = core.UpdateWaitGroup(&coreapis.UpdateWaitGroupRequest{
			Payload: &corepb.UpdateWaitGroupRequest{
				NamespaceId:     namespaceId,
				WaitGroupName:   "test_wait_group",
				Description:     "desc",
				ExpiresAt:       newExpiresAt,
				Now:             now.Add(time.Minute).UnixNano(),
				ExpectedVersion: 1,
				Counter:         1,
			},
		})
		require.NoError(t, err)

		// Running GC at a time after the OLD expiration but before the NEW one
		// must not mark the wait group expired, because the expiration index was
		// reconciled to the new timestamp.
		_, err = core.RunWaitGroupsGarbageCollection(&coreapis.RunWaitGroupsGarbageCollectionRequest{
			Payload: &corepb.RunWaitGroupsGarbageCollectionRequest{
				Now:                        now.Add(24 * time.Hour).UnixNano(),
				GcRecordsPageSize:          100,
				GcRecordWaitGroupsPageSize: 1000,
				MaxDeletedObjects:          1000,
			},
		})
		require.NoError(t, err)

		// Wait group is still present
		wg := getWaitGroup(t, core, waitGroupId)
		require.Equal(t, newExpiresAt, wg.ExpiresAt)
	})

	t.Run("version increments on each successful update", func(t *testing.T) {
		core := newWaitGroupsCore(t)
		now := time.Now()
		namespaceId := &corepb.NamespaceId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint64(),
		}
		waitGroupId := &corepb.WaitGroupId{
			AccountId:   namespaceId.AccountId,
			NamespaceId: namespaceId.NamespaceId,
			WaitGroupId: rand.Uint64(),
		}

		// A freshly created wait group starts at version 1
		resp1, err := core.CreateWaitGroup(&coreapis.CreateWaitGroupRequest{
			Payload: &corepb.CreateWaitGroupRequest{
				WaitGroupId:                       waitGroupId,
				Name:                              "test_wait_group",
				Description:                       "desc",
				Counter:                           10,
				Now:                               now.UnixNano(),
				ExpiresAt:                         now.Add(time.Hour).UnixNano(),
				MaxNumberOfWaitGroupsPerNamespace: 100,
			},
		})
		require.NoError(t, err)
		require.Nil(t, resp1.ApplicationError)
		require.EqualValues(t, 1, resp1.Payload.WaitGroup.Version)

		// Updating with the matching current version succeeds and bumps the version
		resp2, err := core.UpdateWaitGroup(&coreapis.UpdateWaitGroupRequest{
			Payload: &corepb.UpdateWaitGroupRequest{
				NamespaceId:     namespaceId,
				WaitGroupName:   "test_wait_group",
				Description:     "desc v2",
				ExpiresAt:       now.Add(time.Hour).UnixNano(),
				Now:             now.Add(time.Minute).UnixNano(),
				ExpectedVersion: 1,
				Counter:         10,
			},
		})
		require.NoError(t, err)
		require.Nil(t, resp2.ApplicationError)
		require.EqualValues(t, 2, resp2.Payload.WaitGroup.Version)

		// The next update must use the new version
		resp3, err := core.UpdateWaitGroup(&coreapis.UpdateWaitGroupRequest{
			Payload: &corepb.UpdateWaitGroupRequest{
				NamespaceId:     namespaceId,
				WaitGroupName:   "test_wait_group",
				Description:     "desc v3",
				ExpiresAt:       now.Add(time.Hour).UnixNano(),
				Now:             now.Add(2 * time.Minute).UnixNano(),
				ExpectedVersion: 2,
				Counter:         10,
			},
		})
		require.NoError(t, err)
		require.Nil(t, resp3.ApplicationError)
		require.EqualValues(t, 3, resp3.Payload.WaitGroup.Version)
	})

	t.Run("update with stale version", func(t *testing.T) {
		core := newWaitGroupsCore(t)
		now := time.Now()
		namespaceId := &corepb.NamespaceId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint64(),
		}
		waitGroupId := &corepb.WaitGroupId{
			AccountId:   namespaceId.AccountId,
			NamespaceId: namespaceId.NamespaceId,
			WaitGroupId: rand.Uint64(),
		}

		_, err := core.CreateWaitGroup(&coreapis.CreateWaitGroupRequest{
			Payload: &corepb.CreateWaitGroupRequest{
				WaitGroupId:                       waitGroupId,
				Name:                              "test_wait_group",
				Description:                       "desc",
				Counter:                           10,
				Now:                               now.UnixNano(),
				ExpiresAt:                         now.Add(time.Hour).UnixNano(),
				MaxNumberOfWaitGroupsPerNamespace: 100,
			},
		})
		require.NoError(t, err)

		// First update with version 1 succeeds (wait group is now at version 2)
		resp, err := core.UpdateWaitGroup(&coreapis.UpdateWaitGroupRequest{
			Payload: &corepb.UpdateWaitGroupRequest{
				NamespaceId:     namespaceId,
				WaitGroupName:   "test_wait_group",
				Description:     "desc v2",
				ExpiresAt:       now.Add(time.Hour).UnixNano(),
				Now:             now.Add(time.Minute).UnixNano(),
				ExpectedVersion: 1,
				Counter:         10,
			},
		})
		require.NoError(t, err)
		require.Nil(t, resp.ApplicationError)

		// Reusing the stale version 1 is rejected with a version mismatch
		resp, err = core.UpdateWaitGroup(&coreapis.UpdateWaitGroupRequest{
			Payload: &corepb.UpdateWaitGroupRequest{
				NamespaceId:     namespaceId,
				WaitGroupName:   "test_wait_group",
				Description:     "should not apply",
				ExpiresAt:       now.Add(time.Hour).UnixNano(),
				Now:             now.Add(2 * time.Minute).UnixNano(),
				ExpectedVersion: 1,
				Counter:         10,
			},
		})
		require.NoError(t, err)
		require.Nil(t, resp.Payload)
		require.NotNil(t, resp.ApplicationError)
		require.Equal(t, monsterax.InvalidArgument, resp.ApplicationError.Code)
		require.Contains(t, resp.ApplicationError.Message, "version mismatch")

		// The rejected update did not change anything
		wg := getWaitGroup(t, core, waitGroupId)
		require.Equal(t, "desc v2", wg.Description)
		require.EqualValues(t, 2, wg.Version)
	})

	t.Run("update with future version", func(t *testing.T) {
		core := newWaitGroupsCore(t)
		now := time.Now()
		namespaceId := &corepb.NamespaceId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint64(),
		}
		waitGroupId := &corepb.WaitGroupId{
			AccountId:   namespaceId.AccountId,
			NamespaceId: namespaceId.NamespaceId,
			WaitGroupId: rand.Uint64(),
		}

		_, err := core.CreateWaitGroup(&coreapis.CreateWaitGroupRequest{
			Payload: &corepb.CreateWaitGroupRequest{
				WaitGroupId:                       waitGroupId,
				Name:                              "test_wait_group",
				Description:                       "desc",
				Counter:                           10,
				Now:                               now.UnixNano(),
				ExpiresAt:                         now.Add(time.Hour).UnixNano(),
				MaxNumberOfWaitGroupsPerNamespace: 100,
			},
		})
		require.NoError(t, err)

		// Passing a version the wait group has never reached is rejected
		resp, err := core.UpdateWaitGroup(&coreapis.UpdateWaitGroupRequest{
			Payload: &corepb.UpdateWaitGroupRequest{
				NamespaceId:     namespaceId,
				WaitGroupName:   "test_wait_group",
				Description:     "desc",
				ExpiresAt:       now.Add(time.Hour).UnixNano(),
				Now:             now.Add(time.Minute).UnixNano(),
				ExpectedVersion: 99,
				Counter:         10,
			},
		})
		require.NoError(t, err)
		require.Nil(t, resp.Payload)
		require.NotNil(t, resp.ApplicationError)
		require.Equal(t, monsterax.InvalidArgument, resp.ApplicationError.Code)
		require.Contains(t, resp.ApplicationError.Message, "version mismatch")
	})

	t.Run("cannot update a finished wait group", func(t *testing.T) {
		core := newWaitGroupsCore(t)
		now := time.Now()
		namespaceId := &corepb.NamespaceId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint64(),
		}
		waitGroupId := &corepb.WaitGroupId{
			AccountId:   namespaceId.AccountId,
			NamespaceId: namespaceId.NamespaceId,
			WaitGroupId: rand.Uint64(),
		}

		_, err := core.CreateWaitGroup(&coreapis.CreateWaitGroupRequest{
			Payload: &corepb.CreateWaitGroupRequest{
				WaitGroupId:                       waitGroupId,
				Name:                              "test_wait_group",
				Description:                       "desc",
				Counter:                           2,
				Now:                               now.UnixNano(),
				ExpiresAt:                         now.Add(time.Hour).UnixNano(),
				MaxNumberOfWaitGroupsPerNamespace: 100,
			},
		})
		require.NoError(t, err)

		// Complete all jobs so the wait group becomes COMPLETED.
		wg := completeJobsFromWaitGroup(t, core, namespaceId, "test_wait_group", []string{"job_1", "job_2"}, now.Add(time.Minute))
		require.Equal(t, corepb.WaitGroupStatus_WAIT_GROUP_STATUS_COMPLETED, wg.Status)

		// Updating a finished wait group is rejected.
		resp, err := core.UpdateWaitGroup(&coreapis.UpdateWaitGroupRequest{
			Payload: &corepb.UpdateWaitGroupRequest{
				NamespaceId:     namespaceId,
				WaitGroupName:   "test_wait_group",
				Description:     "new desc",
				ExpiresAt:       now.Add(2 * time.Hour).UnixNano(),
				Now:             now.Add(2 * time.Minute).UnixNano(),
				ExpectedVersion: wg.Version,
				Counter:         2,
			},
		})
		require.NoError(t, err)
		require.Nil(t, resp.Payload)
		require.NotNil(t, resp.ApplicationError)
		require.Equal(t, monsterax.InvalidArgument, resp.ApplicationError.Code)
		require.Contains(t, resp.ApplicationError.Message, "only active wait groups can be updated")
	})
}

func TestCore_CompleteJobsWithMetadata(t *testing.T) {
	core := newWaitGroupsCore(t)
	now := time.Now()
	namespaceId := &corepb.NamespaceId{
		AccountId:   rand.Uint64(),
		NamespaceId: rand.Uint64(),
	}

	_, err := core.CreateWaitGroup(&coreapis.CreateWaitGroupRequest{
		Payload: &corepb.CreateWaitGroupRequest{
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
		},
	})
	require.NoError(t, err)

	// Complete jobs, each carrying its own metadata
	resp, err := core.CompleteJobsFromWaitGroup(&coreapis.CompleteJobsFromWaitGroupRequest{
		Payload: &corepb.CompleteJobsFromWaitGroupRequest{
			NamespaceId:   namespaceId,
			WaitGroupName: "test_wait_group",
			Jobs: []*corepb.CompleteJobRequest{
				{JobId: "job_1", Metadata: map[string]string{"worker": "w1"}},
				{JobId: "job_2", Metadata: map[string]string{"worker": "w2"}},
			},
			Now: now.Add(time.Minute).UnixNano(),
		},
	})
	require.NoError(t, err)
	require.Nil(t, resp.ApplicationError)
	require.EqualValues(t, 2, resp.Payload.WaitGroup.CompletedJobs)

	// Metadata is persisted on the completed job records
	jobsList := ListWaitGroupCompletedJobs(t, core, namespaceId, "test_wait_group")
	require.Len(t, jobsList.Jobs, 2)
	byId := make(map[string]map[string]string)
	for _, job := range jobsList.Jobs {
		byId[job.Id.JobId] = job.Metadata
	}
	require.Equal(t, map[string]string{"worker": "w1"}, byId["job_1"])
	require.Equal(t, map[string]string{"worker": "w2"}, byId["job_2"])
}

func TestCore_SnapshotAndRestore(t *testing.T) {
	now := time.Now()
	waitGroupId := &corepb.WaitGroupId{
		AccountId:   rand.Uint64(),
		NamespaceId: rand.Uint64(),
		WaitGroupId: rand.Uint64(),
	}

	// Create two wait group cores for testing snapshot and restore
	core1 := newWaitGroupsCore(t)
	core2 := newWaitGroupsCore(t)

	// T+0: Create wait group
	resp1, err := core1.CreateWaitGroup(&coreapis.CreateWaitGroupRequest{
		Payload: &corepb.CreateWaitGroupRequest{
			WaitGroupId:                       waitGroupId,
			Name:                              "test_wait_group",
			Description:                       "test description",
			Counter:                           10,
			Now:                               now.UnixNano(),
			ExpiresAt:                         now.Add(time.Hour).UnixNano(),
			MaxNumberOfWaitGroupsPerNamespace: 100,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, resp1)
	require.Nil(t, resp1.ApplicationError)
	require.NotNil(t, resp1.Payload)

	// T+1m: Complete some jobs
	resp2, err := core1.CompleteJobsFromWaitGroup(&coreapis.CompleteJobsFromWaitGroupRequest{
		Payload: &corepb.CompleteJobsFromWaitGroupRequest{
			NamespaceId: &corepb.NamespaceId{
				AccountId:   waitGroupId.AccountId,
				NamespaceId: waitGroupId.NamespaceId,
			},
			WaitGroupName: "test_wait_group",
			Jobs:          completeJobRequests([]string{"job_1", "job_2", "job_3"}),
			Now:           now.Add(time.Minute).UnixNano(),
		},
	})
	require.NoError(t, err)
	require.NotNil(t, resp2)
	require.Nil(t, resp2.ApplicationError)
	require.NotNil(t, resp2.Payload)

	// Take snapshot at this point
	snapshot := core1.Snapshot()

	// T+2m: Complete more jobs (after snapshot)
	resp3, err := core1.CompleteJobsFromWaitGroup(&coreapis.CompleteJobsFromWaitGroupRequest{
		Payload: &corepb.CompleteJobsFromWaitGroupRequest{
			NamespaceId: &corepb.NamespaceId{
				AccountId:   waitGroupId.AccountId,
				NamespaceId: waitGroupId.NamespaceId,
			},
			WaitGroupName: "test_wait_group",
			Jobs:          completeJobRequests([]string{"job_4", "job_5"}),
			Now:           now.Add(2 * time.Minute).UnixNano(),
		},
	})
	require.NoError(t, err)
	require.NotNil(t, resp3)
	require.Nil(t, resp3.ApplicationError)
	require.NotNil(t, resp3.Payload)

	// Write snapshot to buffer
	buf := bytes.NewBuffer(nil)
	err = snapshot.Write(buf)
	require.NoError(t, err)

	// Restore snapshot to second core
	err = core2.Restore(io.NopCloser(buf))
	require.NoError(t, err)

	// T+4m: Check that the restored state matches the snapshot state
	// The wait group should exist with 3 completed jobs out of 10 total
	resp5, err := core2.GetWaitGroup(&coreapis.GetWaitGroupRequest{
		Payload: &corepb.GetWaitGroupRequest{
			WaitGroupId: waitGroupId,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, resp5)
	require.Nil(t, resp5.ApplicationError)
	require.NotNil(t, resp5.Payload)
	require.NotNil(t, resp5.Payload.WaitGroup)
	require.EqualValues(t, 10, resp5.Payload.WaitGroup.Counter)
	require.EqualValues(t, 3, resp5.Payload.WaitGroup.CompletedJobs)

	// T+5m: Complete more jobs in restored state
	resp6, err := core2.CompleteJobsFromWaitGroup(&coreapis.CompleteJobsFromWaitGroupRequest{
		Payload: &corepb.CompleteJobsFromWaitGroupRequest{
			NamespaceId: &corepb.NamespaceId{
				AccountId:   waitGroupId.AccountId,
				NamespaceId: waitGroupId.NamespaceId,
			},
			WaitGroupName: "test_wait_group",
			Jobs:          completeJobRequests([]string{"job_6", "job_7"}),
			Now:           now.Add(5 * time.Minute).UnixNano(),
		},
	})
	require.NoError(t, err)
	require.NotNil(t, resp6)
	require.Nil(t, resp6.ApplicationError)
	require.NotNil(t, resp6.Payload)

	// T+6m: Verify state in restored core
	resp7, err := core2.GetWaitGroup(&coreapis.GetWaitGroupRequest{
		Payload: &corepb.GetWaitGroupRequest{
			WaitGroupId: waitGroupId,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, resp7)
	require.Nil(t, resp7.ApplicationError)
	require.NotNil(t, resp7.Payload)
	require.EqualValues(t, 10, resp7.Payload.WaitGroup.Counter)
	require.EqualValues(t, 5, resp7.Payload.WaitGroup.CompletedJobs)

	// T+7m: Complete more jobs in restored state
	resp8, err := core2.CompleteJobsFromWaitGroup(&coreapis.CompleteJobsFromWaitGroupRequest{
		Payload: &corepb.CompleteJobsFromWaitGroupRequest{
			NamespaceId: &corepb.NamespaceId{
				AccountId:   waitGroupId.AccountId,
				NamespaceId: waitGroupId.NamespaceId,
			},
			WaitGroupName: "test_wait_group",
			Jobs:          completeJobRequests([]string{"job_8", "job_9"}),
			Now:           now.Add(8 * time.Minute).UnixNano(),
		},
	})
	require.NoError(t, err)
	require.NotNil(t, resp8)
	require.Nil(t, resp8.ApplicationError)
	require.NotNil(t, resp8.Payload)

	// T+8m: Verify final state in restored core
	resp9, err := core2.GetWaitGroup(&coreapis.GetWaitGroupRequest{
		Payload: &corepb.GetWaitGroupRequest{
			WaitGroupId: waitGroupId,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, resp9)
	require.Nil(t, resp9.ApplicationError)
	require.NotNil(t, resp9.Payload)
	require.EqualValues(t, 10, resp9.Payload.WaitGroup.Counter)
	require.EqualValues(t, 7, resp9.Payload.WaitGroup.CompletedJobs)

	// Verify that the original core has different state (it should have more completed jobs)
	resp10, err := core1.GetWaitGroup(&coreapis.GetWaitGroupRequest{
		Payload: &corepb.GetWaitGroupRequest{
			WaitGroupId: waitGroupId,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, resp10)
	require.Nil(t, resp10.ApplicationError)
	require.NotNil(t, resp10.Payload)
	require.EqualValues(t, 10, resp10.Payload.WaitGroup.Counter)      // 10 + 5 added after snapshot
	require.EqualValues(t, 5, resp10.Payload.WaitGroup.CompletedJobs) // 3 + 2 completed after snapshot
}

func TestCore_RunWaitGroupsGarbageCollection(t *testing.T) {
	core := newWaitGroupsCore(t)
	now := time.Now()

	// Test parameters - use smaller limits to force multiple runs
	maxDeletedObjects := int64(1000)
	gcRecordsPageSize := int64(100)
	gcRecordWaitGroupsPageSize := int64(1000)

	// Create test data for account 1
	accountId1 := rand.Uint64()
	namespaceId1 := &corepb.NamespaceId{
		AccountId:   accountId1,
		NamespaceId: rand.Uint64(),
	}

	// Create multiple wait groups for account 1 with many jobs each
	numWaitGroups := 3
	jobsPerGroup := 500 // 1500 total jobs, exceeding MaxDeletedObjects
	waitGroupIds := make([]*corepb.WaitGroupId, numWaitGroups)

	for i := range numWaitGroups {
		waitGroupName := fmt.Sprintf("test_wait_group_%d", i)
		waitGroupIds[i] = &corepb.WaitGroupId{
			AccountId:   namespaceId1.AccountId,
			NamespaceId: namespaceId1.NamespaceId,
			WaitGroupId: rand.Uint64(),
		}

		// Create wait group with many jobs
		resp1, err := core.CreateWaitGroup(&coreapis.CreateWaitGroupRequest{
			Payload: &corepb.CreateWaitGroupRequest{
				WaitGroupId:                       waitGroupIds[i],
				Name:                              waitGroupName,
				Description:                       fmt.Sprintf("test description %d", i),
				Counter:                           int64(jobsPerGroup),
				Now:                               now.UnixNano(),
				ExpiresAt:                         now.Add(time.Hour).UnixNano(),
				MaxNumberOfWaitGroupsPerNamespace: 100,
			},
		})
		require.NoError(t, err)
		require.NotNil(t, resp1)
		require.Nil(t, resp1.ApplicationError)
		require.NotNil(t, resp1.Payload)

		// Complete all jobs to create job records
		jobIds := make([]string, jobsPerGroup)
		for j := range jobsPerGroup {
			jobIds[j] = fmt.Sprintf("job_%d_%d", i, j)
		}

		resp2, err := core.CompleteJobsFromWaitGroup(&coreapis.CompleteJobsFromWaitGroupRequest{
			Payload: &corepb.CompleteJobsFromWaitGroupRequest{
				NamespaceId: &corepb.NamespaceId{
					AccountId:   waitGroupIds[i].AccountId,
					NamespaceId: waitGroupIds[i].NamespaceId,
				},
				WaitGroupName: waitGroupName,
				Jobs:          completeJobRequests(jobIds),
				Now:           now.Add(time.Minute).UnixNano(),
			},
		})
		require.NoError(t, err)
		require.NotNil(t, resp2)
		require.Nil(t, resp2.ApplicationError)
		require.NotNil(t, resp2.Payload)
	}

	// Create test data for account 2 with multiple namespaces
	accountId2 := rand.Uint64()
	numNamespaces := 2
	waitGroupsPerNamespace := 3
	jobsPerWaitGroup := 400 // 1200 jobs per namespace
	namespaceIds := make([]*corepb.NamespaceId, numNamespaces)

	for ns := range numNamespaces {
		namespaceIds[ns] = &corepb.NamespaceId{
			AccountId:   accountId2,
			NamespaceId: rand.Uint64(),
		}

		// Create multiple wait groups for this namespace
		for wg := range waitGroupsPerNamespace {
			waitGroupName := fmt.Sprintf("test_wait_group_%d", wg)
			waitGroupId := &corepb.WaitGroupId{
				AccountId:   namespaceIds[ns].AccountId,
				NamespaceId: namespaceIds[ns].NamespaceId,
				WaitGroupId: uint64(wg),
			}

			// Create wait group with many jobs
			resp3, err := core.CreateWaitGroup(&coreapis.CreateWaitGroupRequest{
				Payload: &corepb.CreateWaitGroupRequest{
					WaitGroupId:                       waitGroupId,
					Name:                              waitGroupName,
					Description:                       fmt.Sprintf("test description namespace %d wg %d", ns, wg),
					Counter:                           int64(jobsPerWaitGroup),
					Now:                               now.UnixNano(),
					ExpiresAt:                         now.Add(time.Hour).UnixNano(),
					MaxNumberOfWaitGroupsPerNamespace: 100,
				},
			})
			require.NoError(t, err)
			require.NotNil(t, resp3)
			require.Nil(t, resp3.ApplicationError)
			require.NotNil(t, resp3.Payload)

			// Complete all jobs to create job records
			jobIds := make([]string, jobsPerWaitGroup)
			for j := range jobsPerWaitGroup {
				jobIds[j] = fmt.Sprintf("job_ns%d_wg%d_%d", ns, wg, j)
			}

			resp4, err := core.CompleteJobsFromWaitGroup(&coreapis.CompleteJobsFromWaitGroupRequest{
				Payload: &corepb.CompleteJobsFromWaitGroupRequest{
					NamespaceId: &corepb.NamespaceId{
						AccountId:   waitGroupId.AccountId,
						NamespaceId: waitGroupId.NamespaceId,
					},
					WaitGroupName: waitGroupName,
					Jobs:          completeJobRequests(jobIds),
					Now:           now.Add(time.Minute).UnixNano(),
				},
			})
			require.NoError(t, err)
			require.NotNil(t, resp4)
			require.Nil(t, resp4.ApplicationError)
			require.NotNil(t, resp4.Payload)
		}
	}

	// Delete one wait group from account 1 (this will create a GC record for individual wait group)
	resp5, err := core.DeleteWaitGroup(&coreapis.DeleteWaitGroupRequest{
		Payload: &corepb.DeleteWaitGroupRequest{
			RecordId: rand.Uint64(),
			NamespaceId: &corepb.NamespaceId{
				AccountId:   waitGroupIds[0].AccountId,
				NamespaceId: waitGroupIds[0].NamespaceId,
			},
			WaitGroupName: "test_wait_group_0",
		},
	})
	require.NoError(t, err)
	require.NotNil(t, resp5)
	require.Nil(t, resp5.ApplicationError)
	require.NotNil(t, resp5.Payload)

	// Delete one namespace from account 2 (this will create a GC record for entire namespace)
	namespaceToDelete := namespaceIds[1]

	resp6, err := core.WaitGroupsDeleteNamespace(&coreapis.WaitGroupsDeleteNamespaceRequest{
		Payload: &corepb.WaitGroupsDeleteNamespaceRequest{
			RecordId:    rand.Uint64(),
			NamespaceId: namespaceToDelete,
			Now:         now.UnixNano(),
		},
	})
	require.NoError(t, err)
	require.NotNil(t, resp6)
	require.Nil(t, resp6.ApplicationError)
	require.NotNil(t, resp6.Payload)

	// Run garbage collection multiple times to process all objects
	// The first run should process some objects but not all due to MaxDeletedObjects limit
	runCount := 0
	maxRuns := 10 // Prevent infinite loop

	for runCount < maxRuns {
		runCount++
		t.Logf("Running garbage collection iteration %d", runCount)

		resp7, err := core.RunWaitGroupsGarbageCollection(&coreapis.RunWaitGroupsGarbageCollectionRequest{
			Payload: &corepb.RunWaitGroupsGarbageCollectionRequest{
				Now:                        now.UnixNano(),
				GcRecordsPageSize:          gcRecordsPageSize,
				GcRecordWaitGroupsPageSize: gcRecordWaitGroupsPageSize,
				MaxDeletedObjects:          maxDeletedObjects,
			},
		})
		require.NoError(t, err)
		require.NotNil(t, resp7)
		require.Nil(t, resp7.ApplicationError)
		require.NotNil(t, resp7.Payload)

		// Check if both expected deletions have been completed
		deletedWaitGroupAccessible := true
		deletedNamespaceAccessible := true

		// Check if the deleted wait group is still accessible
		resp8, err := core.GetWaitGroup(&coreapis.GetWaitGroupRequest{
			Payload: &corepb.GetWaitGroupRequest{
				WaitGroupId: waitGroupIds[0],
			},
		})
		require.NoError(t, err)
		require.NotNil(t, resp8)

		if resp8.ApplicationError != nil && resp8.ApplicationError.Code == monsterax.NotFound {
			deletedWaitGroupAccessible = false
			t.Logf("Deleted wait group is no longer accessible after run %d", runCount)
		}

		// Check if the deleted namespace's wait groups are still accessible
		deletedNamespaceWaitGroupId := &corepb.WaitGroupId{
			AccountId:   namespaceToDelete.AccountId,
			NamespaceId: namespaceToDelete.NamespaceId,
			WaitGroupId: 0,
		}
		resp9, err := core.GetWaitGroup(&coreapis.GetWaitGroupRequest{
			Payload: &corepb.GetWaitGroupRequest{
				WaitGroupId: deletedNamespaceWaitGroupId,
			},
		})
		require.NoError(t, err)
		require.NotNil(t, resp8)

		if resp9.ApplicationError != nil && resp9.ApplicationError.Code == monsterax.NotFound {
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
	resp10, err := core.GetWaitGroup(&coreapis.GetWaitGroupRequest{
		Payload: &corepb.GetWaitGroupRequest{
			WaitGroupId: waitGroupIds[0],
		},
	})
	require.NoError(t, err)
	require.NotNil(t, resp10)
	require.NotNil(t, resp10.ApplicationError)
	require.Equal(t, monsterax.NotFound, resp10.ApplicationError.Code)

	// Verify that the deleted namespace's wait groups are no longer accessible
	resp11, err := core.GetWaitGroup(&coreapis.GetWaitGroupRequest{
		Payload: &corepb.GetWaitGroupRequest{
			WaitGroupId: &corepb.WaitGroupId{
				AccountId:   namespaceToDelete.AccountId,
				NamespaceId: namespaceToDelete.NamespaceId,
				WaitGroupId: 0,
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, resp11)
	require.NotNil(t, resp11.ApplicationError)
	require.Equal(t, monsterax.NotFound, resp11.ApplicationError.Code)

	// Verify that other wait groups from account 1 are still accessible
	resp12, err := core.GetWaitGroup(&coreapis.GetWaitGroupRequest{
		Payload: &corepb.GetWaitGroupRequest{
			WaitGroupId: waitGroupIds[1],
		},
	})
	require.NoError(t, err)
	require.NotNil(t, resp12)
	require.Nil(t, resp12.ApplicationError)
	require.NotNil(t, resp12.Payload)

	// Verify that the non-deleted namespace from account 2 is still accessible
	// namespaceIds[0] should still be accessible (only namespaceIds[1] was deleted)
	remainingNamespaceWaitGroupId := &corepb.WaitGroupId{
		AccountId:   namespaceIds[0].AccountId,
		NamespaceId: namespaceIds[0].NamespaceId,
		WaitGroupId: 0, // First wait group in that namespace
	}
	resp13, err := core.GetWaitGroup(&coreapis.GetWaitGroupRequest{
		Payload: &corepb.GetWaitGroupRequest{
			WaitGroupId: remainingNamespaceWaitGroupId,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, resp13)
	require.Nil(t, resp13.ApplicationError)
	require.NotNil(t, resp13.Payload)

	t.Logf("Garbage collection completed successfully in %d runs", runCount)
}

func TestCore_DeleteAfterFinished(t *testing.T) {
	t.Run("completed wait group is marked completed and deleted after retention", func(t *testing.T) {
		core := newWaitGroupsCore(t)
		now := time.Now()
		deleteAfterSeconds := int64(600) // 10 minutes
		namespaceId := &corepb.NamespaceId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint64(),
		}
		waitGroupId := &corepb.WaitGroupId{
			AccountId:   namespaceId.AccountId,
			NamespaceId: namespaceId.NamespaceId,
			WaitGroupId: rand.Uint64(),
		}

		_, err := core.CreateWaitGroup(&coreapis.CreateWaitGroupRequest{
			Payload: &corepb.CreateWaitGroupRequest{
				WaitGroupId:                       waitGroupId,
				Name:                              "test_wait_group",
				Description:                       "desc",
				Counter:                           2,
				Now:                               now.UnixNano(),
				ExpiresAt:                         now.Add(time.Hour).UnixNano(),
				MaxNumberOfWaitGroupsPerNamespace: 100,
				DeleteAfterFinishedSeconds:        deleteAfterSeconds,
			},
		})
		require.NoError(t, err)

		// Completing all jobs finishes the wait group as COMPLETED.
		finishedAt := now.Add(time.Minute)
		wg := completeJobsFromWaitGroup(t, core, namespaceId, "test_wait_group", []string{"job_1", "job_2"}, finishedAt)
		require.Equal(t, corepb.WaitGroupStatus_WAIT_GROUP_STATUS_COMPLETED, wg.Status)
		require.Equal(t, finishedAt.UnixNano(), wg.FinishedAt)
		require.EqualValues(t, 2, wg.CompletedJobs)

		// GC before the retention period elapses must not delete the wait group.
		runWaitGroupsGC(t, core, finishedAt.Add(5*time.Minute))
		wg = getWaitGroup(t, core, waitGroupId)
		require.Equal(t, corepb.WaitGroupStatus_WAIT_GROUP_STATUS_COMPLETED, wg.Status)

		// GC after the retention period deletes the wait group and its jobs.
		runWaitGroupsGC(t, core, finishedAt.Add(time.Duration(deleteAfterSeconds)*time.Second).Add(time.Minute))
		requireWaitGroupNotFound(t, core, waitGroupId)
	})

	t.Run("expired wait group is marked expired then deleted after retention", func(t *testing.T) {
		core := newWaitGroupsCore(t)
		now := time.Now()
		deleteAfterSeconds := int64(600) // 10 minutes
		expiresAt := now.Add(time.Hour)
		namespaceId := &corepb.NamespaceId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint64(),
		}
		waitGroupId := &corepb.WaitGroupId{
			AccountId:   namespaceId.AccountId,
			NamespaceId: namespaceId.NamespaceId,
			WaitGroupId: rand.Uint64(),
		}

		_, err := core.CreateWaitGroup(&coreapis.CreateWaitGroupRequest{
			Payload: &corepb.CreateWaitGroupRequest{
				WaitGroupId:                       waitGroupId,
				Name:                              "test_wait_group",
				Description:                       "desc",
				Counter:                           5,
				Now:                               now.UnixNano(),
				ExpiresAt:                         expiresAt.UnixNano(),
				MaxNumberOfWaitGroupsPerNamespace: 100,
				DeleteAfterFinishedSeconds:        deleteAfterSeconds,
			},
		})
		require.NoError(t, err)

		// Only partially complete it so it never reaches its counter.
		completeJobsFromWaitGroup(t, core, namespaceId, "test_wait_group", []string{"job_1"}, now.Add(time.Minute))

		// GC after expires_at but before the retention period marks the wait group
		// EXPIRED without deleting it.
		runWaitGroupsGC(t, core, expiresAt.Add(5*time.Minute))
		wg := getWaitGroup(t, core, waitGroupId)
		require.Equal(t, corepb.WaitGroupStatus_WAIT_GROUP_STATUS_EXPIRED, wg.Status)
		require.Equal(t, expiresAt.UnixNano(), wg.FinishedAt)

		// GC after the retention period (finished_at + delete_after) deletes it.
		runWaitGroupsGC(t, core, expiresAt.Add(time.Duration(deleteAfterSeconds)*time.Second).Add(time.Minute))
		requireWaitGroupNotFound(t, core, waitGroupId)
	})

	t.Run("expires_at no longer deletes directly", func(t *testing.T) {
		core := newWaitGroupsCore(t)
		now := time.Now()
		expiresAt := now.Add(time.Hour)
		namespaceId := &corepb.NamespaceId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint64(),
		}
		waitGroupId := &corepb.WaitGroupId{
			AccountId:   namespaceId.AccountId,
			NamespaceId: namespaceId.NamespaceId,
			WaitGroupId: rand.Uint64(),
		}

		_, err := core.CreateWaitGroup(&coreapis.CreateWaitGroupRequest{
			Payload: &corepb.CreateWaitGroupRequest{
				WaitGroupId:                       waitGroupId,
				Name:                              "test_wait_group",
				Description:                       "desc",
				Counter:                           5,
				Now:                               now.UnixNano(),
				ExpiresAt:                         expiresAt.UnixNano(),
				MaxNumberOfWaitGroupsPerNamespace: 100,
				// Large retention: even though the wait group expires, it stays
				// around long after expires_at.
				DeleteAfterFinishedSeconds: int64((24 * time.Hour).Seconds()),
			},
		})
		require.NoError(t, err)

		// Way past expires_at, but well within the retention window.
		runWaitGroupsGC(t, core, expiresAt.Add(time.Hour))
		wg := getWaitGroup(t, core, waitGroupId)
		require.Equal(t, corepb.WaitGroupStatus_WAIT_GROUP_STATUS_EXPIRED, wg.Status)
	})
}

// runWaitGroupsGC runs a garbage collection pass at the given wall-clock time
// with generous page sizes.
func runWaitGroupsGC(t *testing.T, core *Core, now time.Time) {
	t.Helper()

	resp, err := core.RunWaitGroupsGarbageCollection(&coreapis.RunWaitGroupsGarbageCollectionRequest{
		Payload: &corepb.RunWaitGroupsGarbageCollectionRequest{
			Now:                        now.UnixNano(),
			GcRecordsPageSize:          100,
			GcRecordWaitGroupsPageSize: 1000,
			MaxDeletedObjects:          1000,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Nil(t, resp.ApplicationError)
}

// requireWaitGroupNotFound asserts that the wait group no longer exists.
func requireWaitGroupNotFound(t *testing.T, core *Core, waitGroupId *corepb.WaitGroupId) {
	t.Helper()

	resp, err := core.GetWaitGroup(&coreapis.GetWaitGroupRequest{
		Payload: &corepb.GetWaitGroupRequest{
			WaitGroupId: waitGroupId,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.NotNil(t, resp.ApplicationError)
	require.Equal(t, monsterax.NotFound, resp.ApplicationError.Code)
}

func TestCore_LastActivityAt(t *testing.T) {
	t.Run("create sets it, completing jobs updates it, update does not", func(t *testing.T) {
		core := newWaitGroupsCore(t)
		now := time.Now()
		namespaceId := &corepb.NamespaceId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint64(),
		}
		waitGroupId := &corepb.WaitGroupId{
			AccountId:   namespaceId.AccountId,
			NamespaceId: namespaceId.NamespaceId,
			WaitGroupId: rand.Uint64(),
		}

		// Create records the creation time as the initial activity.
		wg := createWaitGroup(t, core, waitGroupId, "test_wait_group", 10, now)
		require.Equal(t, now.UnixNano(), wg.LastActivityAt)

		// Update* must NOT touch last_activity_at.
		updateResp, err := core.UpdateWaitGroup(&coreapis.UpdateWaitGroupRequest{
			Payload: &corepb.UpdateWaitGroupRequest{
				NamespaceId:     namespaceId,
				WaitGroupName:   "test_wait_group",
				Description:     "new description",
				Counter:         10,
				ExpiresAt:       now.Add(2 * time.Hour).UnixNano(),
				Now:             now.Add(time.Minute).UnixNano(),
				ExpectedVersion: 1,
			},
		})
		require.NoError(t, err)
		require.Nil(t, updateResp.ApplicationError)
		require.Equal(t, now.UnixNano(), updateResp.Payload.WaitGroup.LastActivityAt)

		// Reporting a completed job is activity and updates the timestamp.
		completeTime := now.Add(2 * time.Minute)
		wg2 := completeJobsFromWaitGroup(t, core, namespaceId, "test_wait_group", []string{"job_1"}, completeTime)
		require.Equal(t, completeTime.UnixNano(), wg2.LastActivityAt)
	})
}

// completeJobRequests builds a slice of CompleteJobRequest from plain job ids
// (without metadata), which is the common case in these tests.
func completeJobRequests(jobIds []string) []*corepb.CompleteJobRequest {
	jobs := make([]*corepb.CompleteJobRequest, len(jobIds))
	for i, jobId := range jobIds {
		jobs[i] = &corepb.CompleteJobRequest{JobId: jobId}
	}
	return jobs
}

func newWaitGroupsCore(t *testing.T) *Core {
	store, err := store.NewBadgerInMemoryStore()
	require.NoError(t, err)
	return NewCore(store, []byte{0x1d, 0x36, 0x00, 0x00}, []byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})
}

func createWaitGroup(t *testing.T, core *Core, waitGroupId *corepb.WaitGroupId, name string, counter int64, now time.Time) *corepb.WaitGroup {
	t.Helper()

	resp, err := core.CreateWaitGroup(&coreapis.CreateWaitGroupRequest{
		Payload: &corepb.CreateWaitGroupRequest{
			WaitGroupId:                       waitGroupId,
			Name:                              name,
			Description:                       "test description",
			Counter:                           counter,
			Now:                               now.UnixNano(),
			ExpiresAt:                         now.Add(time.Hour).UnixNano(),
			MaxNumberOfWaitGroupsPerNamespace: 100,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Nil(t, resp.ApplicationError)
	require.NotNil(t, resp.Payload)
	require.NotNil(t, resp.Payload.WaitGroup)
	return resp.Payload.WaitGroup
}

func completeJobsFromWaitGroup(t *testing.T, core *Core, namespaceId *corepb.NamespaceId, waitGroupName string, jobIds []string, now time.Time) *corepb.WaitGroup {
	t.Helper()

	resp, err := core.CompleteJobsFromWaitGroup(&coreapis.CompleteJobsFromWaitGroupRequest{
		Payload: &corepb.CompleteJobsFromWaitGroupRequest{
			NamespaceId:   namespaceId,
			WaitGroupName: waitGroupName,
			Jobs:          completeJobRequests(jobIds),
			Now:           now.UnixNano(),
		},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Nil(t, resp.ApplicationError)
	require.NotNil(t, resp.Payload)
	require.NotNil(t, resp.Payload.WaitGroup)
	return resp.Payload.WaitGroup
}

func completeJobsFromWaitGroupWithError(t *testing.T, core *Core, namespaceId *corepb.NamespaceId, waitGroupName string, jobIds []string, now time.Time) *monsterax.Error {
	t.Helper()

	resp, err := core.CompleteJobsFromWaitGroup(&coreapis.CompleteJobsFromWaitGroupRequest{
		Payload: &corepb.CompleteJobsFromWaitGroupRequest{
			NamespaceId:   namespaceId,
			WaitGroupName: waitGroupName,
			Jobs:          completeJobRequests(jobIds),
			Now:           now.UnixNano(),
		},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Nil(t, resp.Payload)
	require.NotNil(t, resp.ApplicationError)
	return resp.ApplicationError
}

func getWaitGroup(t *testing.T, core *Core, waitGroupId *corepb.WaitGroupId) *corepb.WaitGroup {
	t.Helper()

	resp, err := core.GetWaitGroup(&coreapis.GetWaitGroupRequest{
		Payload: &corepb.GetWaitGroupRequest{
			WaitGroupId: waitGroupId,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Nil(t, resp.ApplicationError)
	require.NotNil(t, resp.Payload)
	require.NotNil(t, resp.Payload.WaitGroup)
	return resp.Payload.WaitGroup
}

func ListWaitGroupCompletedJobs(t *testing.T, core *Core, namespaceId *corepb.NamespaceId, waitGroupName string) *corepb.ListWaitGroupCompletedJobsResponse {
	t.Helper()

	resp, err := core.ListWaitGroupCompletedJobs(&coreapis.ListWaitGroupCompletedJobsRequest{
		Payload: &corepb.ListWaitGroupCompletedJobsRequest{
			NamespaceId:   namespaceId,
			WaitGroupName: waitGroupName,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Nil(t, resp.ApplicationError)
	require.NotNil(t, resp.Payload)
	return resp.Payload
}
