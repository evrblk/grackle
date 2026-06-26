package integration_test

import (
	"context"
	"testing"
	"time"

	gracklepb "github.com/evrblk/evrblk-go/grackle/v1beta"
	"github.com/stretchr/testify/require"
)

// TestMetadataRoundTrip exercises the metadata field end-to-end through the
// public API for every entity that supports it: it is accepted on the
// create/acquire/arrive request, stored by the core, and returned on the
// corresponding read.
func TestMetadataRoundTrip(t *testing.T) {
	t.Run("namespace", func(t *testing.T) {
		server := setupGrackleApiServer(t)
		ctx := context.Background()

		md := map[string]string{"team": "search", "cost-center": "1234"}
		createResp, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
			Name:     "namespace1",
			Metadata: md,
		})
		require.NoError(t, err)
		require.Equal(t, md, createResp.Namespace.Metadata)

		getResp, err := server.GetNamespace(ctx, &gracklepb.GetNamespaceRequest{
			NamespaceName: "namespace1",
		})
		require.NoError(t, err)
		require.Equal(t, md, getResp.Namespace.Metadata)

		// Update replaces the metadata
		updMd := map[string]string{"team": "search", "cost-center": "5678"}
		updResp, err := server.UpdateNamespace(ctx, &gracklepb.UpdateNamespaceRequest{
			NamespaceName:   "namespace1",
			Metadata:        updMd,
			ExpectedVersion: 1,
		})
		require.NoError(t, err)
		require.Equal(t, updMd, updResp.Namespace.Metadata)

		getResp, err = server.GetNamespace(ctx, &gracklepb.GetNamespaceRequest{
			NamespaceName: "namespace1",
		})
		require.NoError(t, err)
		require.Equal(t, updMd, getResp.Namespace.Metadata)
	})

	t.Run("wait group", func(t *testing.T) {
		server := setupGrackleApiServer(t)
		ctx := context.Background()

		_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{Name: "namespace1"})
		require.NoError(t, err)

		md := map[string]string{"pipeline": "etl", "owner": "data"}
		createResp, err := server.CreateWaitGroup(ctx, &gracklepb.CreateWaitGroupRequest{
			NamespaceName:              "namespace1",
			WaitGroupName:              "waitgroup1",
			Counter:                    3,
			Metadata:                   md,
			DeleteAfterFinishedSeconds: 60,
			ExpiresAt:                  time.Now().Add(time.Hour).UnixNano(),
		})
		require.NoError(t, err)
		require.Equal(t, md, createResp.WaitGroup.Metadata)

		getResp, err := server.GetWaitGroup(ctx, &gracklepb.GetWaitGroupRequest{
			NamespaceName: "namespace1",
			WaitGroupName: "waitgroup1",
		})
		require.NoError(t, err)
		require.Equal(t, md, getResp.WaitGroup.Metadata)
	})

	t.Run("completed job", func(t *testing.T) {
		server := setupGrackleApiServer(t)
		ctx := context.Background()

		_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{Name: "namespace1"})
		require.NoError(t, err)

		_, err = server.CreateWaitGroup(ctx, &gracklepb.CreateWaitGroupRequest{
			NamespaceName:              "namespace1",
			WaitGroupName:              "waitgroup1",
			Counter:                    2,
			DeleteAfterFinishedSeconds: 60,
			ExpiresAt:                  time.Now().Add(time.Hour).UnixNano(),
		})
		require.NoError(t, err)

		_, err = server.CompleteJobsFromWaitGroup(ctx, &gracklepb.CompleteJobsFromWaitGroupRequest{
			NamespaceName: "namespace1",
			WaitGroupName: "waitgroup1",
			Jobs: []*gracklepb.CompleteJobRequest{
				{JobId: "job1", Metadata: map[string]string{"worker": "w1"}},
				{JobId: "job2", Metadata: map[string]string{"worker": "w2"}},
			},
		})
		require.NoError(t, err)

		listResp, err := server.ListWaitGroupCompletedJobs(ctx, &gracklepb.ListWaitGroupCompletedJobsRequest{
			NamespaceName: "namespace1",
			WaitGroupName: "waitgroup1",
		})
		require.NoError(t, err)
		require.Len(t, listResp.Jobs, 2)

		byId := make(map[string]map[string]string)
		for _, job := range listResp.Jobs {
			byId[job.JobId] = job.Metadata
		}
		require.Equal(t, map[string]string{"worker": "w1"}, byId["job1"])
		require.Equal(t, map[string]string{"worker": "w2"}, byId["job2"])
	})

	t.Run("lock holder", func(t *testing.T) {
		server := setupGrackleApiServer(t)
		ctx := context.Background()

		_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{Name: "namespace1"})
		require.NoError(t, err)

		lease, err := server.CreateLockLease(ctx, &gracklepb.CreateLockLeaseRequest{
			NamespaceName: "namespace1",
			ProcessId:     "process_1",
			TtlSeconds:    30,
		})
		require.NoError(t, err)

		md := map[string]string{"host": "node-1", "pid": "1234"}
		acqResp, err := server.AcquireLock(ctx, &gracklepb.AcquireLockRequest{
			NamespaceName: "namespace1",
			LockName:      "lock1",
			LeaseId:       lease.Lease.LeaseId,
			Exclusive:     true,
			Metadata:      md,
		})
		require.NoError(t, err)
		require.Equal(t, gracklepb.AcquireOutcome_ACQUIRE_OUTCOME_ACQUIRED, acqResp.Outcome)
		require.Len(t, acqResp.Lock.LockHolders, 1)
		require.Equal(t, md, acqResp.Lock.LockHolders[0].Metadata)

		getResp, err := server.GetLock(ctx, &gracklepb.GetLockRequest{
			NamespaceName: "namespace1",
			LockName:      "lock1",
		})
		require.NoError(t, err)
		require.Len(t, getResp.Lock.LockHolders, 1)
		require.Equal(t, md, getResp.Lock.LockHolders[0].Metadata)
	})

	t.Run("semaphore and holder", func(t *testing.T) {
		server := setupGrackleApiServer(t)
		ctx := context.Background()

		_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{Name: "namespace1"})
		require.NoError(t, err)

		semMd := map[string]string{"team": "search"}
		createResp, err := server.CreateSemaphore(ctx, &gracklepb.CreateSemaphoreRequest{
			NamespaceName: "namespace1",
			SemaphoreName: "semaphore1",
			Permits:       5,
			Metadata:      semMd,
		})
		require.NoError(t, err)
		require.Equal(t, semMd, createResp.Semaphore.Metadata)

		getResp, err := server.GetSemaphore(ctx, &gracklepb.GetSemaphoreRequest{
			NamespaceName: "namespace1",
			SemaphoreName: "semaphore1",
		})
		require.NoError(t, err)
		require.Equal(t, semMd, getResp.Semaphore.Metadata)

		// Update metadata
		updMd := map[string]string{"team": "search", "env": "prod"}
		updResp, err := server.UpdateSemaphore(ctx, &gracklepb.UpdateSemaphoreRequest{
			NamespaceName:   "namespace1",
			SemaphoreName:   "semaphore1",
			Permits:         5,
			Metadata:        updMd,
			ExpectedVersion: 1,
		})
		require.NoError(t, err)
		require.Equal(t, updMd, updResp.Semaphore.Metadata)

		// Acquire with holder metadata
		lease, err := server.CreateSemaphoreLease(ctx, &gracklepb.CreateSemaphoreLeaseRequest{
			NamespaceName: "namespace1",
			ProcessId:     "process_1",
			TtlSeconds:    30,
		})
		require.NoError(t, err)

		holderMd := map[string]string{"host": "node-2"}
		acqResp, err := server.AcquireSemaphore(ctx, &gracklepb.AcquireSemaphoreRequest{
			NamespaceName:  "namespace1",
			SemaphoreName:  "semaphore1",
			LeaseId:        lease.Lease.LeaseId,
			Weight:         1,
			TimeoutSeconds: 60,
			Metadata:       holderMd,
		})
		require.NoError(t, err)
		require.Equal(t, gracklepb.AcquireOutcome_ACQUIRE_OUTCOME_ACQUIRED, acqResp.Outcome)

		holdersResp, err := server.ListSemaphoreHolders(ctx, &gracklepb.ListSemaphoreHoldersRequest{
			NamespaceName: "namespace1",
			SemaphoreName: "semaphore1",
		})
		require.NoError(t, err)
		require.Len(t, holdersResp.Holders, 1)
		require.Equal(t, holderMd, holdersResp.Holders[0].Metadata)
	})

	t.Run("barrier and participant", func(t *testing.T) {
		server := setupGrackleApiServer(t)
		ctx := context.Background()

		_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{Name: "namespace1"})
		require.NoError(t, err)

		barrierMd := map[string]string{"job": "rollout", "tier": "gold"}
		createResp, err := server.CreateBarrier(ctx, &gracklepb.CreateBarrierRequest{
			NamespaceName:              "namespace1",
			BarrierName:                "barrier1",
			ExpectedProcesses:          2,
			DeleteInactiveAfterSeconds: int64((10 * time.Minute).Seconds()),
			Metadata:                   barrierMd,
		})
		require.NoError(t, err)
		require.Equal(t, barrierMd, createResp.Barrier.Metadata)

		getResp, err := server.GetBarrier(ctx, &gracklepb.GetBarrierRequest{
			NamespaceName: "namespace1",
			BarrierName:   "barrier1",
		})
		require.NoError(t, err)
		require.Equal(t, barrierMd, getResp.Barrier.Metadata)

		// Update barrier metadata
		updMd := map[string]string{"job": "rollout", "tier": "platinum"}
		updResp, err := server.UpdateBarrier(ctx, &gracklepb.UpdateBarrierRequest{
			NamespaceName:              "namespace1",
			BarrierName:                "barrier1",
			ExpectedProcesses:          2,
			Metadata:                   updMd,
			ExpectedVersion:            1,
			DeleteInactiveAfterSeconds: int64((10 * time.Minute).Seconds()),
		})
		require.NoError(t, err)
		require.Equal(t, updMd, updResp.Barrier.Metadata)

		// A single process arrives (ExpectedProcesses is 2, so the barrier does
		// not trip and the participant row remains queryable at generation 1).
		participantMd := map[string]string{"host": "node-1"}
		_, err = server.ArriveAtBarrier(ctx, &gracklepb.ArriveAtBarrierRequest{
			NamespaceName:      "namespace1",
			BarrierName:        "barrier1",
			ProcessId:          "process_1",
			ExpectedGeneration: 1,
			Metadata:           participantMd,
		})
		require.NoError(t, err)

		participantsResp, err := server.ListBarrierParticipants(ctx, &gracklepb.ListBarrierParticipantsRequest{
			NamespaceName: "namespace1",
			BarrierName:   "barrier1",
			Generation:    1,
		})
		require.NoError(t, err)
		require.Len(t, participantsResp.Participants, 1)
		require.Equal(t, "process_1", participantsResp.Participants[0].ProcessId)
		require.Equal(t, participantMd, participantsResp.Participants[0].Metadata)
	})
}
