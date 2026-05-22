package preview

import (
	"context"
	"math/rand/v2"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	gracklepb "github.com/evrblk/evrblk-go/grackle/preview"
	monsterax "github.com/evrblk/monstera/x"

	"github.com/evrblk/grackle/pkg/corepb"
	"github.com/evrblk/grackle/pkg/grackle"
	"github.com/evrblk/grackle/pkg/monsteragen"
)

type GrackleApiServerHandler struct {
	grackleCoreApiClient monsteragen.GrackleCoreApi
}

func (s *GrackleApiServerHandler) Stop() {

}

func (s *GrackleApiServerHandler) CreateNamespace(ctx context.Context, request *gracklepb.CreateNamespaceRequest, accountId uint64, limits grackle.GrackleServiceLimits) (*gracklepb.CreateNamespaceResponse, error) {
	now := time.Now()

	resp, err := s.grackleCoreApiClient.CreateNamespace(ctx, &corepb.CreateNamespaceRequest{
		NamespaceId: &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: rand.Uint32(),
		},
		Name:                  request.Name,
		Description:           request.Description,
		Now:                   now.UnixNano(),
		MaxNumberOfNamespaces: limits.MaxNumberOfNamespaces,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	return &gracklepb.CreateNamespaceResponse{
		Namespace: namespaceToFront(resp.Namespace),
	}, nil
}

func (s *GrackleApiServerHandler) GetNamespace(ctx context.Context, request *gracklepb.GetNamespaceRequest, accountId uint64, limits grackle.GrackleServiceLimits) (*gracklepb.GetNamespaceResponse, error) {
	resp1, err := s.grackleCoreApiClient.GetNamespaceByName(ctx, &corepb.GetNamespaceByNameRequest{
		AccountId:     accountId,
		NamespaceName: request.NamespaceName,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	return &gracklepb.GetNamespaceResponse{
		Namespace: namespaceToFront(resp1.Namespace),
	}, nil
}

func (s *GrackleApiServerHandler) UpdateNamespace(ctx context.Context, request *gracklepb.UpdateNamespaceRequest, accountId uint64, limits grackle.GrackleServiceLimits) (*gracklepb.UpdateNamespaceResponse, error) {
	now := time.Now()

	resp1, err := s.grackleCoreApiClient.GetNamespaceByName(ctx, &corepb.GetNamespaceByNameRequest{
		AccountId:     accountId,
		NamespaceName: request.NamespaceName,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	resp2, err := s.grackleCoreApiClient.UpdateNamespace(ctx, &corepb.UpdateNamespaceRequest{
		NamespaceId: resp1.Namespace.Id,
		Description: request.Description,
		Now:         now.UnixNano(),
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	return &gracklepb.UpdateNamespaceResponse{
		Namespace: namespaceToFront(resp2.Namespace),
	}, nil
}

func (s *GrackleApiServerHandler) DeleteNamespace(ctx context.Context, request *gracklepb.DeleteNamespaceRequest, accountId uint64, limits grackle.GrackleServiceLimits) (*gracklepb.DeleteNamespaceResponse, error) {
	now := time.Now()

	gcRecordId := rand.Uint64()

	resp1, err := s.grackleCoreApiClient.GetNamespaceByName(ctx, &corepb.GetNamespaceByNameRequest{
		AccountId:     accountId,
		NamespaceName: request.NamespaceName,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	_, err = s.grackleCoreApiClient.LocksDeleteNamespace(ctx, &corepb.LocksDeleteNamespaceRequest{
		RecordId:    gcRecordId,
		NamespaceId: resp1.Namespace.Id,
		Now:         now.UnixNano(),
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	_, err = s.grackleCoreApiClient.WaitGroupsDeleteNamespace(ctx, &corepb.WaitGroupsDeleteNamespaceRequest{
		RecordId:    gcRecordId,
		NamespaceId: resp1.Namespace.Id,
		Now:         now.UnixNano(),
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	_, err = s.grackleCoreApiClient.SemaphoresDeleteNamespace(ctx, &corepb.SemaphoresDeleteNamespaceRequest{
		RecordId:    gcRecordId,
		NamespaceId: resp1.Namespace.Id,
		Now:         now.UnixNano(),
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	_, err = s.grackleCoreApiClient.DeleteNamespace(ctx, &corepb.DeleteNamespaceRequest{
		NamespaceId: resp1.Namespace.Id,
		Now:         now.UnixNano(),
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	return &gracklepb.DeleteNamespaceResponse{}, nil
}

func (s *GrackleApiServerHandler) ListNamespaces(ctx context.Context, request *gracklepb.ListNamespacesRequest, accountId uint64, limits grackle.GrackleServiceLimits) (*gracklepb.ListNamespacesResponse, error) {
	paginationToken, err := paginationTokenFromFront(request.PaginationToken)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	resp1, err := s.grackleCoreApiClient.ListNamespaces(ctx, &corepb.ListNamespacesRequest{
		AccountId:       accountId,
		PaginationToken: paginationToken,
		Limit:           request.Limit,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	nextPaginationToken, err := paginationTokenToFront(resp1.NextPaginationToken)
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}
	previousPaginationToken, err := paginationTokenToFront(resp1.PreviousPaginationToken)
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	return &gracklepb.ListNamespacesResponse{
		Namespaces:              namespacesToFront(resp1.Namespaces),
		NextPaginationToken:     nextPaginationToken,
		PreviousPaginationToken: previousPaginationToken,
	}, nil
}

func (s *GrackleApiServerHandler) CreateWaitGroup(ctx context.Context, request *gracklepb.CreateWaitGroupRequest, accountId uint64, limits grackle.GrackleServiceLimits) (*gracklepb.CreateWaitGroupResponse, error) {
	now := time.Now()

	// Check if wait group size is too big
	if request.Counter > uint64(limits.MaxWaitGroupSize) {
		return nil, status.Errorf(codes.InvalidArgument, "wait group size is too big, max: %d", limits.MaxWaitGroupSize)
	}

	resp1, err := s.grackleCoreApiClient.GetNamespaceByName(ctx, &corepb.GetNamespaceByNameRequest{
		AccountId:     accountId,
		NamespaceName: request.NamespaceName,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	resp2, err := s.grackleCoreApiClient.CreateWaitGroup(ctx, &corepb.CreateWaitGroupRequest{
		WaitGroupId: &corepb.WaitGroupId{
			AccountId:   accountId,
			NamespaceId: resp1.Namespace.Id.NamespaceId,
			WaitGroupId: rand.Uint64(),
		},
		Name:                              request.WaitGroupName,
		Description:                       request.Description,
		Now:                               now.UnixNano(),
		Counter:                           request.Counter,
		ExpiresAt:                         request.ExpiresAt,
		MaxNumberOfWaitGroupsPerNamespace: limits.MaxNumberOfWaitGroupsPerNamespace,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	return &gracklepb.CreateWaitGroupResponse{
		WaitGroup: waitGroupToFront(resp2.WaitGroup),
	}, nil
}

func (s *GrackleApiServerHandler) GetWaitGroup(ctx context.Context, request *gracklepb.GetWaitGroupRequest, accountId uint64, limits grackle.GrackleServiceLimits) (*gracklepb.GetWaitGroupResponse, error) {
	resp1, err := s.grackleCoreApiClient.GetNamespaceByName(ctx, &corepb.GetNamespaceByNameRequest{
		AccountId:     accountId,
		NamespaceName: request.NamespaceName,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	resp2, err := s.grackleCoreApiClient.GetWaitGroupByName(ctx, &corepb.GetWaitGroupByNameRequest{
		NamespaceId:   resp1.Namespace.Id,
		WaitGroupName: request.WaitGroupName,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	return &gracklepb.GetWaitGroupResponse{
		WaitGroup: waitGroupToFront(resp2.WaitGroup),
	}, nil
}

func (s *GrackleApiServerHandler) WaitForWaitGroup(ctx context.Context, request *gracklepb.WaitForWaitGroupRequest, accountId uint64, limits grackle.GrackleServiceLimits) (*gracklepb.WaitForWaitGroupResponse, error) {
	// Get namespace ID once
	resp1, err := s.grackleCoreApiClient.GetNamespaceByName(ctx, &corepb.GetNamespaceByNameRequest{
		AccountId:     accountId,
		NamespaceName: request.NamespaceName,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	// Calculate deadline
	deadline := time.Now().Add(time.Duration(request.TimeoutSeconds) * time.Second)

	// Initial poll interval of 100ms
	pollInterval := 100 * time.Millisecond
	maxPollInterval := 1 * time.Second

	for {
		// Check if context is cancelled
		if ctx.Err() != nil {
			return nil, status.Errorf(codes.Canceled, "request cancelled")
		}

		// Get current wait group state
		resp2, err := s.grackleCoreApiClient.GetWaitGroupByName(ctx, &corepb.GetWaitGroupByNameRequest{
			NamespaceId:   resp1.Namespace.Id,
			WaitGroupName: request.WaitGroupName,
		})
		if err != nil {
			return nil, monsterax.ErrorToGRPC(err)
		}

		// Check if timeout has been reached
		timedOut := time.Now().After(deadline)

		// Check if wait group is completed
		completed := resp2.WaitGroup.Counter == resp2.WaitGroup.Completed

		if timedOut || completed {
			return &gracklepb.WaitForWaitGroupResponse{
				WaitGroup: waitGroupToFront(resp2.WaitGroup),
				Completed: completed,
				TimedOut:  timedOut,
			}, nil
		}

		// Sleep before next poll, respecting both timeout and context cancellation
		sleepDuration := pollInterval
		if timeUntilDeadline := time.Until(deadline); timeUntilDeadline < sleepDuration {
			sleepDuration = timeUntilDeadline
		}

		select {
		case <-time.After(sleepDuration):
			// Exponential backoff with maximum
			pollInterval = pollInterval * 2
			if pollInterval > maxPollInterval {
				pollInterval = maxPollInterval
			}
		case <-ctx.Done():
			return nil, status.Errorf(codes.Canceled, "request cancelled")
		}
	}
}

func (s *GrackleApiServerHandler) AddJobsToWaitGroup(ctx context.Context, request *gracklepb.AddJobsToWaitGroupRequest, accountId uint64, limits grackle.GrackleServiceLimits) (*gracklepb.AddJobsToWaitGroupResponse, error) {
	now := time.Now()

	resp1, err := s.grackleCoreApiClient.GetNamespaceByName(ctx, &corepb.GetNamespaceByNameRequest{
		AccountId:     accountId,
		NamespaceName: request.NamespaceName,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	resp2, err := s.grackleCoreApiClient.AddJobsToWaitGroup(ctx, &corepb.AddJobsToWaitGroupRequest{
		NamespaceId:      resp1.Namespace.Id,
		WaitGroupName:    request.WaitGroupName,
		Counter:          request.Counter,
		Now:              now.UnixNano(),
		MaxWaitGroupSize: limits.MaxWaitGroupSize,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	return &gracklepb.AddJobsToWaitGroupResponse{
		WaitGroup: waitGroupToFront(resp2.WaitGroup),
	}, nil
}

func (s *GrackleApiServerHandler) CompleteJobsFromWaitGroup(ctx context.Context, request *gracklepb.CompleteJobsFromWaitGroupRequest, accountId uint64, limits grackle.GrackleServiceLimits) (*gracklepb.CompleteJobsFromWaitGroupResponse, error) {
	now := time.Now()

	resp1, err := s.grackleCoreApiClient.GetNamespaceByName(ctx, &corepb.GetNamespaceByNameRequest{
		AccountId:     accountId,
		NamespaceName: request.NamespaceName,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	resp2, err := s.grackleCoreApiClient.CompleteJobsFromWaitGroup(ctx, &corepb.CompleteJobsFromWaitGroupRequest{
		NamespaceId:   resp1.Namespace.Id,
		WaitGroupName: request.WaitGroupName,
		ProcessIds:    request.ProcessIds,
		Now:           now.UnixNano(),
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	return &gracklepb.CompleteJobsFromWaitGroupResponse{
		WaitGroup: waitGroupToFront(resp2.WaitGroup),
	}, nil
}

func (s *GrackleApiServerHandler) DeleteWaitGroup(ctx context.Context, request *gracklepb.DeleteWaitGroupRequest, accountId uint64, limits grackle.GrackleServiceLimits) (*gracklepb.DeleteWaitGroupResponse, error) {
	resp1, err := s.grackleCoreApiClient.GetNamespaceByName(ctx, &corepb.GetNamespaceByNameRequest{
		AccountId:     accountId,
		NamespaceName: request.NamespaceName,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	_, err = s.grackleCoreApiClient.DeleteWaitGroup(ctx, &corepb.DeleteWaitGroupRequest{
		NamespaceId:   resp1.Namespace.Id,
		WaitGroupName: request.WaitGroupName,
		RecordId:      rand.Uint64(),
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	return &gracklepb.DeleteWaitGroupResponse{}, nil
}

func (s *GrackleApiServerHandler) ListWaitGroups(ctx context.Context, request *gracklepb.ListWaitGroupsRequest, accountId uint64, limits grackle.GrackleServiceLimits) (*gracklepb.ListWaitGroupsResponse, error) {
	resp1, err := s.grackleCoreApiClient.GetNamespaceByName(ctx, &corepb.GetNamespaceByNameRequest{
		AccountId:     accountId,
		NamespaceName: request.NamespaceName,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	paginationToken, err := paginationTokenFromFront(request.PaginationToken)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	resp2, err := s.grackleCoreApiClient.ListWaitGroups(ctx, &corepb.ListWaitGroupsRequest{
		NamespaceId:     resp1.Namespace.Id,
		PaginationToken: paginationToken,
		Limit:           request.Limit,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	nextPaginationToken, err := paginationTokenToFront(resp2.NextPaginationToken)
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}
	previousPaginationToken, err := paginationTokenToFront(resp2.PreviousPaginationToken)
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	return &gracklepb.ListWaitGroupsResponse{
		WaitGroups:              waitGroupsToFront(resp2.WaitGroups),
		NextPaginationToken:     nextPaginationToken,
		PreviousPaginationToken: previousPaginationToken,
	}, nil
}

func (s *GrackleApiServerHandler) ListWaitGroupJobs(ctx context.Context, request *gracklepb.ListWaitGroupJobsRequest, accountId uint64, limits grackle.GrackleServiceLimits) (*gracklepb.ListWaitGroupJobsResponse, error) {
	resp1, err := s.grackleCoreApiClient.GetNamespaceByName(ctx, &corepb.GetNamespaceByNameRequest{
		AccountId:     accountId,
		NamespaceName: request.NamespaceName,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	paginationToken, err := paginationTokenFromFront(request.PaginationToken)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	resp2, err := s.grackleCoreApiClient.ListWaitGroupJobs(ctx, &corepb.ListWaitGroupJobsRequest{
		NamespaceId:     resp1.Namespace.Id,
		WaitGroupName:   request.WaitGroupName,
		PaginationToken: paginationToken,
		Limit:           request.Limit,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	nextPaginationToken, err := paginationTokenToFront(resp2.NextPaginationToken)
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}
	previousPaginationToken, err := paginationTokenToFront(resp2.PreviousPaginationToken)
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	return &gracklepb.ListWaitGroupJobsResponse{
		Jobs:                    waitGroupJobsToFront(resp2.Jobs),
		NextPaginationToken:     nextPaginationToken,
		PreviousPaginationToken: previousPaginationToken,
	}, nil
}

func (s *GrackleApiServerHandler) AcquireLock(ctx context.Context, request *gracklepb.AcquireLockRequest, accountId uint64, limits grackle.GrackleServiceLimits) (*gracklepb.AcquireLockResponse, error) {
	now := time.Now()

	resp1, err := s.grackleCoreApiClient.GetNamespaceByName(ctx, &corepb.GetNamespaceByNameRequest{
		AccountId:     accountId,
		NamespaceName: request.NamespaceName,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	resp2, err := s.grackleCoreApiClient.AcquireLock(ctx, &corepb.AcquireLockRequest{
		LockId: &corepb.LockId{
			AccountId:   accountId,
			NamespaceId: resp1.Namespace.Id.NamespaceId,
			LockName:    request.LockName,
		},
		Now:                          now.UnixNano(),
		ProcessId:                    request.ProcessId,
		ExpiresAt:                    request.ExpiresAt,
		Exclusive:                    request.Exclusive,
		MaxNumberOfLocksPerNamespace: limits.MaxNumberOfLocksPerNamespace,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	return &gracklepb.AcquireLockResponse{
		Lock:    lockToFront(resp2.Lock),
		Success: resp2.Success,
	}, nil
}

func (s *GrackleApiServerHandler) ReleaseLock(ctx context.Context, request *gracklepb.ReleaseLockRequest, accountId uint64, limits grackle.GrackleServiceLimits) (*gracklepb.ReleaseLockResponse, error) {
	now := time.Now()

	resp1, err := s.grackleCoreApiClient.GetNamespaceByName(ctx, &corepb.GetNamespaceByNameRequest{
		AccountId:     accountId,
		NamespaceName: request.NamespaceName,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	resp2, err := s.grackleCoreApiClient.ReleaseLock(ctx, &corepb.ReleaseLockRequest{
		LockId: &corepb.LockId{
			AccountId:   accountId,
			NamespaceId: resp1.Namespace.Id.NamespaceId,
			LockName:    request.LockName,
		},
		Now:       now.UnixNano(),
		ProcessId: request.ProcessId,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	return &gracklepb.ReleaseLockResponse{
		Lock: lockToFront(resp2.Lock),
	}, nil
}

func (s *GrackleApiServerHandler) GetLock(ctx context.Context, request *gracklepb.GetLockRequest, accountId uint64, limits grackle.GrackleServiceLimits) (*gracklepb.GetLockResponse, error) {
	now := time.Now()

	resp1, err := s.grackleCoreApiClient.GetNamespaceByName(ctx, &corepb.GetNamespaceByNameRequest{
		AccountId:     accountId,
		NamespaceName: request.NamespaceName,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	resp2, err := s.grackleCoreApiClient.GetLock(ctx, &corepb.GetLockRequest{
		LockId: &corepb.LockId{
			AccountId:   accountId,
			NamespaceId: resp1.Namespace.Id.NamespaceId,
			LockName:    request.LockName,
		},
		Now: now.UnixNano(),
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	return &gracklepb.GetLockResponse{
		Lock: lockToFront(resp2.Lock),
	}, nil
}

func (s *GrackleApiServerHandler) DeleteLock(ctx context.Context, request *gracklepb.DeleteLockRequest, accountId uint64, limits grackle.GrackleServiceLimits) (*gracklepb.DeleteLockResponse, error) {
	now := time.Now()

	resp1, err := s.grackleCoreApiClient.GetNamespaceByName(ctx, &corepb.GetNamespaceByNameRequest{
		AccountId:     accountId,
		NamespaceName: request.NamespaceName,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	_, err = s.grackleCoreApiClient.DeleteLock(ctx, &corepb.DeleteLockRequest{
		LockId: &corepb.LockId{
			AccountId:   accountId,
			NamespaceId: resp1.Namespace.Id.NamespaceId,
			LockName:    request.LockName,
		},
		Now: now.UnixNano(),
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	return &gracklepb.DeleteLockResponse{}, nil
}

func (s *GrackleApiServerHandler) ListLocks(ctx context.Context, request *gracklepb.ListLocksRequest, accountId uint64, limits grackle.GrackleServiceLimits) (*gracklepb.ListLocksResponse, error) {
	now := time.Now()

	resp1, err := s.grackleCoreApiClient.GetNamespaceByName(ctx, &corepb.GetNamespaceByNameRequest{
		AccountId:     accountId,
		NamespaceName: request.NamespaceName,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	paginationToken, err := paginationTokenFromFront(request.PaginationToken)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	resp2, err := s.grackleCoreApiClient.ListLocks(ctx, &corepb.ListLocksRequest{
		NamespaceId:     resp1.Namespace.Id,
		Now:             now.UnixNano(),
		PaginationToken: paginationToken,
		Limit:           request.Limit,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	nextPaginationToken, err := paginationTokenToFront(resp2.NextPaginationToken)
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}
	previousPaginationToken, err := paginationTokenToFront(resp2.PreviousPaginationToken)
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	return &gracklepb.ListLocksResponse{
		Locks:                   locksToFront(resp2.Locks),
		NextPaginationToken:     nextPaginationToken,
		PreviousPaginationToken: previousPaginationToken,
	}, nil
}

func (s *GrackleApiServerHandler) CreateSemaphore(ctx context.Context, request *gracklepb.CreateSemaphoreRequest, accountId uint64, limits grackle.GrackleServiceLimits) (*gracklepb.CreateSemaphoreResponse, error) {
	now := time.Now()

	// Check if semaphore size is too big
	if request.Permits > uint64(limits.MaxNumberOfSemaphoreHolders) {
		return nil, status.Errorf(codes.InvalidArgument, "semaphore size is too big, max: %d", limits.MaxNumberOfSemaphoreHolders)
	}

	resp1, err := s.grackleCoreApiClient.GetNamespaceByName(ctx, &corepb.GetNamespaceByNameRequest{
		AccountId:     accountId,
		NamespaceName: request.NamespaceName,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	resp2, err := s.grackleCoreApiClient.CreateSemaphore(ctx, &corepb.CreateSemaphoreRequest{
		SemaphoreId: &corepb.SemaphoreId{
			AccountId:   accountId,
			NamespaceId: resp1.Namespace.Id.NamespaceId,
			SemaphoreId: rand.Uint64(),
		},
		Name:                              request.SemaphoreName,
		Description:                       request.Description,
		Now:                               now.UnixNano(),
		Permits:                           request.Permits,
		MaxNumberOfSemaphoresPerNamespace: limits.MaxNumberOfSemaphoresPerNamespace,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	return &gracklepb.CreateSemaphoreResponse{
		Semaphore: semaphoreToFront(resp2.Semaphore),
	}, nil
}

func (s *GrackleApiServerHandler) ListSemaphores(ctx context.Context, request *gracklepb.ListSemaphoresRequest, accountId uint64, limits grackle.GrackleServiceLimits) (*gracklepb.ListSemaphoresResponse, error) {
	resp1, err := s.grackleCoreApiClient.GetNamespaceByName(ctx, &corepb.GetNamespaceByNameRequest{
		AccountId:     accountId,
		NamespaceName: request.NamespaceName,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	paginationToken, err := paginationTokenFromFront(request.PaginationToken)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	resp2, err := s.grackleCoreApiClient.ListSemaphores(ctx, &corepb.ListSemaphoresRequest{
		NamespaceId:     resp1.Namespace.Id,
		PaginationToken: paginationToken,
		Limit:           request.Limit,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	nextPaginationToken, err := paginationTokenToFront(resp2.NextPaginationToken)
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}
	previousPaginationToken, err := paginationTokenToFront(resp2.PreviousPaginationToken)
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	return &gracklepb.ListSemaphoresResponse{
		Semaphores:              semaphoresToFront(resp2.Semaphores),
		NextPaginationToken:     nextPaginationToken,
		PreviousPaginationToken: previousPaginationToken,
	}, nil
}

func (s *GrackleApiServerHandler) ListSemaphoreHolders(ctx context.Context, request *gracklepb.ListSemaphoreHoldersRequest, accountId uint64, limits grackle.GrackleServiceLimits) (*gracklepb.ListSemaphoreHoldersResponse, error) {
	resp1, err := s.grackleCoreApiClient.GetNamespaceByName(ctx, &corepb.GetNamespaceByNameRequest{
		AccountId:     accountId,
		NamespaceName: request.NamespaceName,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	paginationToken, err := paginationTokenFromFront(request.PaginationToken)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	resp2, err := s.grackleCoreApiClient.ListSemaphoreHolders(ctx, &corepb.ListSemaphoreHoldersRequest{
		NamespaceId:     resp1.Namespace.Id,
		SemaphoreName:   request.SemaphoreName,
		PaginationToken: paginationToken,
		Limit:           request.Limit,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	nextPaginationToken, err := paginationTokenToFront(resp2.NextPaginationToken)
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}
	previousPaginationToken, err := paginationTokenToFront(resp2.PreviousPaginationToken)
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	return &gracklepb.ListSemaphoreHoldersResponse{
		Holders:                 semaphoreHoldersToFront(resp2.Holders),
		NextPaginationToken:     nextPaginationToken,
		PreviousPaginationToken: previousPaginationToken,
	}, nil
}

func (s *GrackleApiServerHandler) GetSemaphore(ctx context.Context, request *gracklepb.GetSemaphoreRequest, accountId uint64, limits grackle.GrackleServiceLimits) (*gracklepb.GetSemaphoreResponse, error) {
	now := time.Now()

	resp1, err := s.grackleCoreApiClient.GetNamespaceByName(ctx, &corepb.GetNamespaceByNameRequest{
		AccountId:     accountId,
		NamespaceName: request.NamespaceName,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	resp2, err := s.grackleCoreApiClient.GetSemaphoreByName(ctx, &corepb.GetSemaphoreByNameRequest{
		NamespaceId:   resp1.Namespace.Id,
		SemaphoreName: request.SemaphoreName,
		Now:           now.UnixNano(),
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	return &gracklepb.GetSemaphoreResponse{
		Semaphore: semaphoreToFront(resp2.Semaphore),
	}, nil
}

func (s *GrackleApiServerHandler) AcquireSemaphore(ctx context.Context, request *gracklepb.AcquireSemaphoreRequest, accountId uint64, limits grackle.GrackleServiceLimits) (*gracklepb.AcquireSemaphoreResponse, error) {
	now := time.Now()

	resp1, err := s.grackleCoreApiClient.GetNamespaceByName(ctx, &corepb.GetNamespaceByNameRequest{
		AccountId:     accountId,
		NamespaceName: request.NamespaceName,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	resp2, err := s.grackleCoreApiClient.AcquireSemaphore(ctx, &corepb.AcquireSemaphoreRequest{
		NamespaceId:   resp1.Namespace.Id,
		SemaphoreName: request.SemaphoreName,
		Weight:        request.Weight,
		Now:           now.UnixNano(),
		ProcessId:     request.ProcessId,
		ExpiresAt:     request.ExpiresAt,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	return &gracklepb.AcquireSemaphoreResponse{
		Semaphore: semaphoreToFront(resp2.Semaphore),
		Success:   resp2.Success,
	}, nil
}

func (s *GrackleApiServerHandler) ReleaseSemaphore(ctx context.Context, request *gracklepb.ReleaseSemaphoreRequest, accountId uint64, limits grackle.GrackleServiceLimits) (*gracklepb.ReleaseSemaphoreResponse, error) {
	now := time.Now()

	resp1, err := s.grackleCoreApiClient.GetNamespaceByName(ctx, &corepb.GetNamespaceByNameRequest{
		AccountId:     accountId,
		NamespaceName: request.NamespaceName,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	resp2, err := s.grackleCoreApiClient.ReleaseSemaphore(ctx, &corepb.ReleaseSemaphoreRequest{
		NamespaceId:   resp1.Namespace.Id,
		SemaphoreName: request.SemaphoreName,
		Now:           now.UnixNano(),
		ProcessId:     request.ProcessId,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	return &gracklepb.ReleaseSemaphoreResponse{
		Semaphore: semaphoreToFront(resp2.Semaphore),
	}, nil
}

func (s *GrackleApiServerHandler) UpdateSemaphore(ctx context.Context, request *gracklepb.UpdateSemaphoreRequest, accountId uint64, limits grackle.GrackleServiceLimits) (*gracklepb.UpdateSemaphoreResponse, error) {
	now := time.Now()

	// Check if semaphore size is too big
	if request.Permits > uint64(limits.MaxNumberOfSemaphoreHolders) {
		return nil, status.Errorf(codes.InvalidArgument, "semaphore size is too big, max: %d", limits.MaxNumberOfSemaphoreHolders)
	}

	resp1, err := s.grackleCoreApiClient.GetNamespaceByName(ctx, &corepb.GetNamespaceByNameRequest{
		AccountId:     accountId,
		NamespaceName: request.NamespaceName,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	resp2, err := s.grackleCoreApiClient.UpdateSemaphore(ctx, &corepb.UpdateSemaphoreRequest{
		NamespaceId:   resp1.Namespace.Id,
		SemaphoreName: request.SemaphoreName,
		Description:   request.Description,
		Now:           now.UnixNano(),
		Permits:       request.Permits,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	return &gracklepb.UpdateSemaphoreResponse{
		Semaphore: semaphoreToFront(resp2.Semaphore),
	}, nil
}

func (s *GrackleApiServerHandler) DeleteSemaphore(ctx context.Context, request *gracklepb.DeleteSemaphoreRequest, accountId uint64, limits grackle.GrackleServiceLimits) (*gracklepb.DeleteSemaphoreResponse, error) {
	resp1, err := s.grackleCoreApiClient.GetNamespaceByName(ctx, &corepb.GetNamespaceByNameRequest{
		AccountId:     accountId,
		NamespaceName: request.NamespaceName,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	_, err = s.grackleCoreApiClient.DeleteSemaphore(ctx, &corepb.DeleteSemaphoreRequest{
		NamespaceId:   resp1.Namespace.Id,
		SemaphoreName: request.SemaphoreName,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	return &gracklepb.DeleteSemaphoreResponse{}, nil
}

func (s *GrackleApiServerHandler) CreateBarrier(ctx context.Context, request *gracklepb.CreateBarrierRequest, accountId uint64, limits grackle.GrackleServiceLimits) (*gracklepb.CreateBarrierResponse, error) {
	now := time.Now()

	resp1, err := s.grackleCoreApiClient.GetNamespaceByName(ctx, &corepb.GetNamespaceByNameRequest{
		AccountId:     accountId,
		NamespaceName: request.NamespaceName,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	resp2, err := s.grackleCoreApiClient.CreateBarrier(ctx, &corepb.CreateBarrierRequest{
		BarrierId: &corepb.BarrierId{
			AccountId:   accountId,
			NamespaceId: resp1.Namespace.Id.NamespaceId,
			BarrierId:   rand.Uint64(),
		},
		Name:                            request.BarrierName,
		Description:                     request.Description,
		ExpectedProcesses:               request.ExpectedProcesses,
		Now:                             now.UnixNano(),
		MaxNumberOfBarriersPerNamespace: limits.MaxNumberOfBarriersPerNamespace,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	return &gracklepb.CreateBarrierResponse{
		Barrier: barrierToFront(resp2.Barrier),
	}, nil
}

func (s *GrackleApiServerHandler) ListBarriers(ctx context.Context, request *gracklepb.ListBarriersRequest, accountId uint64, limits grackle.GrackleServiceLimits) (*gracklepb.ListBarriersResponse, error) {
	resp1, err := s.grackleCoreApiClient.GetNamespaceByName(ctx, &corepb.GetNamespaceByNameRequest{
		AccountId:     accountId,
		NamespaceName: request.NamespaceName,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	paginationToken, err := paginationTokenFromFront(request.PaginationToken)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	resp2, err := s.grackleCoreApiClient.ListBarriers(ctx, &corepb.ListBarriersRequest{
		NamespaceId:     resp1.Namespace.Id,
		PaginationToken: paginationToken,
		Limit:           request.Limit,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	nextPaginationToken, err := paginationTokenToFront(resp2.NextPaginationToken)
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}
	previousPaginationToken, err := paginationTokenToFront(resp2.PreviousPaginationToken)
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	return &gracklepb.ListBarriersResponse{
		Barriers:                barriersToFront(resp2.Barriers),
		NextPaginationToken:     nextPaginationToken,
		PreviousPaginationToken: previousPaginationToken,
	}, nil
}

func (s *GrackleApiServerHandler) GetBarrier(ctx context.Context, request *gracklepb.GetBarrierRequest, accountId uint64, limits grackle.GrackleServiceLimits) (*gracklepb.GetBarrierResponse, error) {
	resp1, err := s.grackleCoreApiClient.GetNamespaceByName(ctx, &corepb.GetNamespaceByNameRequest{
		AccountId:     accountId,
		NamespaceName: request.NamespaceName,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	resp2, err := s.grackleCoreApiClient.GetBarrierByName(ctx, &corepb.GetBarrierByNameRequest{
		NamespaceId: resp1.Namespace.Id,
		BarrierName: request.BarrierName,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	return &gracklepb.GetBarrierResponse{
		Barrier: barrierToFront(resp2.Barrier),
	}, nil
}

func (s *GrackleApiServerHandler) DeleteBarrier(ctx context.Context, request *gracklepb.DeleteBarrierRequest, accountId uint64, limits grackle.GrackleServiceLimits) (*gracklepb.DeleteBarrierResponse, error) {
	now := time.Now()

	resp1, err := s.grackleCoreApiClient.GetNamespaceByName(ctx, &corepb.GetNamespaceByNameRequest{
		AccountId:     accountId,
		NamespaceName: request.NamespaceName,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	_, err = s.grackleCoreApiClient.DeleteBarrier(ctx, &corepb.DeleteBarrierRequest{
		NamespaceId: resp1.Namespace.Id,
		BarrierName: request.BarrierName,
		Now:         now.UnixNano(),
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	return &gracklepb.DeleteBarrierResponse{}, nil
}

func (s *GrackleApiServerHandler) UpdateBarrier(ctx context.Context, request *gracklepb.UpdateBarrierRequest, accountId uint64, limits grackle.GrackleServiceLimits) (*gracklepb.UpdateBarrierResponse, error) {
	now := time.Now()

	resp1, err := s.grackleCoreApiClient.GetNamespaceByName(ctx, &corepb.GetNamespaceByNameRequest{
		AccountId:     accountId,
		NamespaceName: request.NamespaceName,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	// Get the barrier to retrieve its ID
	resp2, err := s.grackleCoreApiClient.GetBarrierByName(ctx, &corepb.GetBarrierByNameRequest{
		NamespaceId: resp1.Namespace.Id,
		BarrierName: request.BarrierName,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	resp3, err := s.grackleCoreApiClient.UpdateBarrier(ctx, &corepb.UpdateBarrierRequest{
		BarrierId:         resp2.Barrier.Id,
		Description:       request.Description,
		ExpectedProcesses: request.ExpectedProcesses,
		Now:               now.UnixNano(),
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	return &gracklepb.UpdateBarrierResponse{
		Barrier: barrierToFront(resp3.Barrier),
	}, nil
}

func (s *GrackleApiServerHandler) ArriveAtBarrier(ctx context.Context, request *gracklepb.ArriveAtBarrierRequest, accountId uint64, limits grackle.GrackleServiceLimits) (*gracklepb.ArriveAtBarrierResponse, error) {
	now := time.Now()

	resp1, err := s.grackleCoreApiClient.GetNamespaceByName(ctx, &corepb.GetNamespaceByNameRequest{
		AccountId:     accountId,
		NamespaceName: request.NamespaceName,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	_, err = s.grackleCoreApiClient.ArriveAtBarrier(ctx, &corepb.ArriveAtBarrierRequest{
		NamespaceId: resp1.Namespace.Id,
		BarrierName: request.BarrierName,
		ProcessId:   request.ProcessId,
		Generation:  request.ExpectedGeneration,
		Now:         now.UnixNano(),
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	// Get the updated barrier state
	resp2, err := s.grackleCoreApiClient.GetBarrierByName(ctx, &corepb.GetBarrierByNameRequest{
		NamespaceId: resp1.Namespace.Id,
		BarrierName: request.BarrierName,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	allArrived := resp2.Barrier.ArrivedProcesses >= resp2.Barrier.ExpectedProcesses &&
		resp2.Barrier.Generation == request.ExpectedGeneration

	return &gracklepb.ArriveAtBarrierResponse{
		Barrier:        barrierToFront(resp2.Barrier),
		AllArrived:     allArrived,
		NextGeneration: resp2.Barrier.Generation + 1,
	}, nil
}

func (s *GrackleApiServerHandler) WaitAtBarrier(ctx context.Context, request *gracklepb.WaitAtBarrierRequest, accountId uint64, limits grackle.GrackleServiceLimits) (*gracklepb.WaitAtBarrierResponse, error) {
	// Get namespace ID once
	resp1, err := s.grackleCoreApiClient.GetNamespaceByName(ctx, &corepb.GetNamespaceByNameRequest{
		AccountId:     accountId,
		NamespaceName: request.NamespaceName,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	// Calculate deadline
	deadline := time.Now().Add(time.Duration(request.TimeoutSeconds) * time.Second)

	// Initial poll interval of 100ms
	pollInterval := 100 * time.Millisecond
	maxPollInterval := 1 * time.Second

	for {
		// Check if context is cancelled
		if ctx.Err() != nil {
			return nil, status.Errorf(codes.Canceled, "request cancelled")
		}

		// Get current barrier state
		resp2, err := s.grackleCoreApiClient.GetBarrierByName(ctx, &corepb.GetBarrierByNameRequest{
			NamespaceId: resp1.Namespace.Id,
			BarrierName: request.BarrierName,
		})
		if err != nil {
			return nil, monsterax.ErrorToGRPC(err)
		}

		// Check if all processes have arrived at the expected generation
		allArrived := resp2.Barrier.ArrivedProcesses >= resp2.Barrier.ExpectedProcesses &&
			resp2.Barrier.Generation == request.ExpectedGeneration

		// Check if timeout has been reached
		timedOut := time.Now().After(deadline)

		if timedOut || allArrived {
			// Return the last known state
			return &gracklepb.WaitAtBarrierResponse{
				Barrier:        barrierToFront(resp2.Barrier),
				AllArrived:     allArrived,
				NextGeneration: resp2.Barrier.Generation + 1,
				TimedOut:       timedOut,
			}, nil
		}

		// Sleep before next poll, respecting both timeout and context cancellation
		sleepDuration := pollInterval
		if timeUntilDeadline := time.Until(deadline); timeUntilDeadline < sleepDuration {
			sleepDuration = timeUntilDeadline
		}

		select {
		case <-time.After(sleepDuration):
			// Exponential backoff with maximum
			pollInterval = pollInterval * 2
			if pollInterval > maxPollInterval {
				pollInterval = maxPollInterval
			}
		case <-ctx.Done():
			return nil, status.Errorf(codes.Canceled, "request cancelled")
		}
	}
}

func (s *GrackleApiServerHandler) ListBarrierParticipants(ctx context.Context, request *gracklepb.ListBarrierParticipantsRequest, accountId uint64, limits grackle.GrackleServiceLimits) (*gracklepb.ListBarrierParticipantsResponse, error) {
	resp1, err := s.grackleCoreApiClient.GetNamespaceByName(ctx, &corepb.GetNamespaceByNameRequest{
		AccountId:     accountId,
		NamespaceName: request.NamespaceName,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	paginationToken, err := paginationTokenFromFront(request.PaginationToken)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	resp2, err := s.grackleCoreApiClient.ListBarrierParticipants(ctx, &corepb.ListBarrierParticipantsRequest{
		NamespaceId:     resp1.Namespace.Id,
		BarrierName:     request.BarrierName,
		Generation:      request.Generation,
		PaginationToken: paginationToken,
		Limit:           request.Limit,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	nextPaginationToken, err := paginationTokenToFront(resp2.NextPaginationToken)
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}
	previousPaginationToken, err := paginationTokenToFront(resp2.PreviousPaginationToken)
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	return &gracklepb.ListBarrierParticipantsResponse{
		Participants:            barrierParticipantsToFront(resp2.Participants),
		NextPaginationToken:     nextPaginationToken,
		PreviousPaginationToken: previousPaginationToken,
	}, nil
}

func NewGrackleApiServerHandler(grackleCoreApiClient monsteragen.GrackleCoreApi) *GrackleApiServerHandler {
	return &GrackleApiServerHandler{
		grackleCoreApiClient: grackleCoreApiClient,
	}
}
