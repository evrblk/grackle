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
	"github.com/evrblk/grackle/pkg/ids"
	"github.com/evrblk/grackle/pkg/monsteragen"
)

type GrackleApiServerHandler struct {
	grackleCoreApiClient monsteragen.GrackleCoreApi
}

func (s *GrackleApiServerHandler) Stop() {

}

func (s *GrackleApiServerHandler) CreateNamespace(ctx context.Context, request *gracklepb.CreateNamespaceRequest, accountId uint64, limits grackle.GrackleServiceLimits) (*gracklepb.CreateNamespaceResponse, error) {
	now := time.Now()

	// Create namespace with generated ID and enforce account limits
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
	// Retrieve namespace by name for the given account
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

	// Resolve namespace by name to get its ID
	resp1, err := s.grackleCoreApiClient.GetNamespaceByName(ctx, &corepb.GetNamespaceByNameRequest{
		AccountId:     accountId,
		NamespaceName: request.NamespaceName,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	// Update namespace description
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

	// Resolve namespace by name to get its ID
	resp1, err := s.grackleCoreApiClient.GetNamespaceByName(ctx, &corepb.GetNamespaceByNameRequest{
		AccountId:     accountId,
		NamespaceName: request.NamespaceName,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	// Mark locks for garbage collection
	_, err = s.grackleCoreApiClient.LocksDeleteNamespace(ctx, &corepb.LocksDeleteNamespaceRequest{
		RecordId:    gcRecordId,
		NamespaceId: resp1.Namespace.Id,
		Now:         now.UnixNano(),
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	// Mark wait groups for garbage collection
	_, err = s.grackleCoreApiClient.WaitGroupsDeleteNamespace(ctx, &corepb.WaitGroupsDeleteNamespaceRequest{
		RecordId:    gcRecordId,
		NamespaceId: resp1.Namespace.Id,
		Now:         now.UnixNano(),
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	// Mark semaphores for garbage collection
	_, err = s.grackleCoreApiClient.SemaphoresDeleteNamespace(ctx, &corepb.SemaphoresDeleteNamespaceRequest{
		RecordId:    gcRecordId,
		NamespaceId: resp1.Namespace.Id,
		Now:         now.UnixNano(),
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	// Mark barriers for garbage collection
	_, err = s.grackleCoreApiClient.BarriersDeleteNamespace(ctx, &corepb.BarriersDeleteNamespaceRequest{
		RecordId:    gcRecordId,
		NamespaceId: resp1.Namespace.Id,
		Now:         now.UnixNano(),
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	// Delete the namespace itself
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
	// Decode pagination token from base64-encoded format
	paginationToken, err := paginationTokenFromFront(request.PaginationToken)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	// List all namespaces for the account with pagination
	resp1, err := s.grackleCoreApiClient.ListNamespaces(ctx, &corepb.ListNamespacesRequest{
		AccountId:       accountId,
		PaginationToken: paginationToken,
		Limit:           request.Limit,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	// Encode pagination tokens for response
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

	// Validate wait group size doesn't exceed account limits
	if request.Counter > uint64(limits.MaxWaitGroupSize) {
		return nil, status.Errorf(codes.InvalidArgument, "wait group size is too big, max: %d", limits.MaxWaitGroupSize)
	}

	// Resolve namespace by name to get its ID
	resp1, err := s.grackleCoreApiClient.GetNamespaceByName(ctx, &corepb.GetNamespaceByNameRequest{
		AccountId:     accountId,
		NamespaceName: request.NamespaceName,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	// Create wait group with generated ID
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
	// Resolve namespace by name to get its ID
	resp1, err := s.grackleCoreApiClient.GetNamespaceByName(ctx, &corepb.GetNamespaceByNameRequest{
		AccountId:     accountId,
		NamespaceName: request.NamespaceName,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	// Retrieve wait group by name within the namespace
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
	// Resolve namespace by name once to avoid repeated lookups
	resp1, err := s.grackleCoreApiClient.GetNamespaceByName(ctx, &corepb.GetNamespaceByNameRequest{
		AccountId:     accountId,
		NamespaceName: request.NamespaceName,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	// Calculate absolute deadline for timeout
	deadline := time.Now().Add(time.Duration(request.TimeoutSeconds) * time.Second)

	// Initialize polling with exponential backoff
	pollInterval := 100 * time.Millisecond
	maxPollInterval := 1 * time.Second

	for {
		// Check if context is cancelled
		if ctx.Err() != nil {
			return nil, status.Errorf(codes.Canceled, "request cancelled")
		}

		// Poll wait group state
		resp2, err := s.grackleCoreApiClient.GetWaitGroupByName(ctx, &corepb.GetWaitGroupByNameRequest{
			NamespaceId:   resp1.Namespace.Id,
			WaitGroupName: request.WaitGroupName,
		})
		if err != nil {
			return nil, monsterax.ErrorToGRPC(err)
		}

		// Check completion and timeout conditions
		timedOut := time.Now().After(deadline)
		completed := resp2.WaitGroup.Counter == resp2.WaitGroup.Completed

		if timedOut || completed {
			return &gracklepb.WaitForWaitGroupResponse{
				WaitGroup: waitGroupToFront(resp2.WaitGroup),
				Completed: completed,
				TimedOut:  timedOut,
			}, nil
		}

		// Sleep with exponential backoff, respecting deadline
		sleepDuration := pollInterval
		if timeUntilDeadline := time.Until(deadline); timeUntilDeadline < sleepDuration {
			sleepDuration = timeUntilDeadline
		}

		select {
		case <-time.After(sleepDuration):
			// Increase poll interval with exponential backoff
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

	// Resolve namespace by name to get its ID
	resp1, err := s.grackleCoreApiClient.GetNamespaceByName(ctx, &corepb.GetNamespaceByNameRequest{
		AccountId:     accountId,
		NamespaceName: request.NamespaceName,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	// Increment wait group counter with size validation
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

	// Resolve namespace by name to get its ID
	resp1, err := s.grackleCoreApiClient.GetNamespaceByName(ctx, &corepb.GetNamespaceByNameRequest{
		AccountId:     accountId,
		NamespaceName: request.NamespaceName,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	// Mark jobs as completed in the wait group
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
	// Resolve namespace by name to get its ID
	resp1, err := s.grackleCoreApiClient.GetNamespaceByName(ctx, &corepb.GetNamespaceByNameRequest{
		AccountId:     accountId,
		NamespaceName: request.NamespaceName,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	// Delete wait group and mark for garbage collection
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
	// Resolve namespace by name to get its ID
	resp1, err := s.grackleCoreApiClient.GetNamespaceByName(ctx, &corepb.GetNamespaceByNameRequest{
		AccountId:     accountId,
		NamespaceName: request.NamespaceName,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	// Decode pagination token from base64-encoded format
	paginationToken, err := paginationTokenFromFront(request.PaginationToken)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	// List wait groups in namespace with pagination
	resp2, err := s.grackleCoreApiClient.ListWaitGroups(ctx, &corepb.ListWaitGroupsRequest{
		NamespaceId:     resp1.Namespace.Id,
		PaginationToken: paginationToken,
		Limit:           request.Limit,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	// Encode pagination tokens for response
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
	// Resolve namespace by name to get its ID
	resp1, err := s.grackleCoreApiClient.GetNamespaceByName(ctx, &corepb.GetNamespaceByNameRequest{
		AccountId:     accountId,
		NamespaceName: request.NamespaceName,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	// Decode pagination token from base64-encoded format
	paginationToken, err := paginationTokenFromFront(request.PaginationToken)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	// List jobs associated with the wait group
	resp2, err := s.grackleCoreApiClient.ListWaitGroupJobs(ctx, &corepb.ListWaitGroupJobsRequest{
		NamespaceId:     resp1.Namespace.Id,
		WaitGroupName:   request.WaitGroupName,
		PaginationToken: paginationToken,
		Limit:           request.Limit,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	// Encode pagination tokens for response
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

	// Resolve namespace by name to get its ID
	resp1, err := s.grackleCoreApiClient.GetNamespaceByName(ctx, &corepb.GetNamespaceByNameRequest{
		AccountId:     accountId,
		NamespaceName: request.NamespaceName,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	// Decode and validate lease ID
	leaseId, err := ids.DecodeLeaseId(request.LeaseId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid AcquireLockRequest.LeaseId: %v", err)
	}

	if leaseId.AccountId != accountId || leaseId.NamespaceId != resp1.Namespace.Id.NamespaceId {
		return nil, status.Errorf(codes.NotFound, "lease not found")
	}

	// Attempt to acquire lock (shared or exclusive)
	resp2, err := s.grackleCoreApiClient.AcquireLock(ctx, &corepb.AcquireLockRequest{
		LockId: &corepb.LockId{
			AccountId:   accountId,
			NamespaceId: resp1.Namespace.Id.NamespaceId,
			LockName:    request.LockName,
		},
		LeaseId:                      leaseId.LeaseId,
		Now:                          now.UnixNano(),
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

	// Resolve namespace by name to get its ID
	resp1, err := s.grackleCoreApiClient.GetNamespaceByName(ctx, &corepb.GetNamespaceByNameRequest{
		AccountId:     accountId,
		NamespaceName: request.NamespaceName,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	// Decode and validate lease ID
	leaseId, err := ids.DecodeLeaseId(request.LeaseId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid ReleaseLockRequest.LeaseId: %v", err)
	}

	if leaseId.AccountId != accountId || leaseId.NamespaceId != resp1.Namespace.Id.NamespaceId {
		return nil, status.Errorf(codes.NotFound, "lease not found")
	}

	// Release the lock held by this lease
	resp2, err := s.grackleCoreApiClient.ReleaseLock(ctx, &corepb.ReleaseLockRequest{
		LockId: &corepb.LockId{
			AccountId:   accountId,
			NamespaceId: resp1.Namespace.Id.NamespaceId,
			LockName:    request.LockName,
		},
		LeaseId: leaseId.LeaseId,
		Now:     now.UnixNano(),
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

	// Resolve namespace by name to get its ID
	resp1, err := s.grackleCoreApiClient.GetNamespaceByName(ctx, &corepb.GetNamespaceByNameRequest{
		AccountId:     accountId,
		NamespaceName: request.NamespaceName,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	// Retrieve lock state
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

	// Resolve namespace by name to get its ID
	resp1, err := s.grackleCoreApiClient.GetNamespaceByName(ctx, &corepb.GetNamespaceByNameRequest{
		AccountId:     accountId,
		NamespaceName: request.NamespaceName,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	// Delete the lock
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

	// Resolve namespace by name to get its ID
	resp1, err := s.grackleCoreApiClient.GetNamespaceByName(ctx, &corepb.GetNamespaceByNameRequest{
		AccountId:     accountId,
		NamespaceName: request.NamespaceName,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	// Decode pagination token from base64-encoded format
	paginationToken, err := paginationTokenFromFront(request.PaginationToken)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	// List all locks in namespace with pagination
	resp2, err := s.grackleCoreApiClient.ListLocks(ctx, &corepb.ListLocksRequest{
		NamespaceId:     resp1.Namespace.Id,
		Now:             now.UnixNano(),
		PaginationToken: paginationToken,
		Limit:           request.Limit,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	// Encode pagination tokens for response
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

	// Validate semaphore size doesn't exceed account limits
	if request.Permits > uint64(limits.MaxNumberOfSemaphoreHolders) {
		return nil, status.Errorf(codes.InvalidArgument, "semaphore size is too big, max: %d", limits.MaxNumberOfSemaphoreHolders)
	}

	// Resolve namespace by name to get its ID
	resp1, err := s.grackleCoreApiClient.GetNamespaceByName(ctx, &corepb.GetNamespaceByNameRequest{
		AccountId:     accountId,
		NamespaceName: request.NamespaceName,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	// Create semaphore with generated ID
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
	// Resolve namespace by name to get its ID
	resp1, err := s.grackleCoreApiClient.GetNamespaceByName(ctx, &corepb.GetNamespaceByNameRequest{
		AccountId:     accountId,
		NamespaceName: request.NamespaceName,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	// Decode pagination token from base64-encoded format
	paginationToken, err := paginationTokenFromFront(request.PaginationToken)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	// List all semaphores in namespace with pagination
	resp2, err := s.grackleCoreApiClient.ListSemaphores(ctx, &corepb.ListSemaphoresRequest{
		NamespaceId:     resp1.Namespace.Id,
		PaginationToken: paginationToken,
		Limit:           request.Limit,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	// Encode pagination tokens for response
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
	// Resolve namespace by name to get its ID
	resp1, err := s.grackleCoreApiClient.GetNamespaceByName(ctx, &corepb.GetNamespaceByNameRequest{
		AccountId:     accountId,
		NamespaceName: request.NamespaceName,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	// Decode pagination token from base64-encoded format
	paginationToken, err := paginationTokenFromFront(request.PaginationToken)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	// List holders for the semaphore with pagination
	resp2, err := s.grackleCoreApiClient.ListSemaphoreHolders(ctx, &corepb.ListSemaphoreHoldersRequest{
		NamespaceId:     resp1.Namespace.Id,
		SemaphoreName:   request.SemaphoreName,
		PaginationToken: paginationToken,
		Limit:           request.Limit,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	// Encode pagination tokens for response
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

	// Resolve namespace by name to get its ID
	resp1, err := s.grackleCoreApiClient.GetNamespaceByName(ctx, &corepb.GetNamespaceByNameRequest{
		AccountId:     accountId,
		NamespaceName: request.NamespaceName,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	// Retrieve semaphore by name within the namespace
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

	// Resolve namespace by name to get its ID
	resp1, err := s.grackleCoreApiClient.GetNamespaceByName(ctx, &corepb.GetNamespaceByNameRequest{
		AccountId:     accountId,
		NamespaceName: request.NamespaceName,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	// Decode and validate lease ID
	leaseId, err := ids.DecodeLeaseId(request.LeaseId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid AcquireSemaphoreRequest.LeaseId: %v", err)
	}

	if leaseId.AccountId != accountId || leaseId.NamespaceId != resp1.Namespace.Id.NamespaceId {
		return nil, status.Errorf(codes.NotFound, "lease not found")
	}

	// Attempt to acquire semaphore with specified weight
	resp2, err := s.grackleCoreApiClient.AcquireSemaphore(ctx, &corepb.AcquireSemaphoreRequest{
		NamespaceId:   resp1.Namespace.Id,
		SemaphoreName: request.SemaphoreName,
		LeaseId:       leaseId.LeaseId,
		Weight:        request.Weight,
		Now:           now.UnixNano(),
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

	// Resolve namespace by name to get its ID
	resp1, err := s.grackleCoreApiClient.GetNamespaceByName(ctx, &corepb.GetNamespaceByNameRequest{
		AccountId:     accountId,
		NamespaceName: request.NamespaceName,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	// Decode and validate lease ID
	leaseId, err := ids.DecodeLeaseId(request.LeaseId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid ReleaseSemaphoreRequest.LeaseId: %v", err)
	}

	// Validate lease ID belongs to the account and namespace
	if leaseId.AccountId != accountId || leaseId.NamespaceId != resp1.Namespace.Id.NamespaceId {
		return nil, status.Errorf(codes.NotFound, "lease not found")
	}

	// Release the semaphore held by this lease
	resp2, err := s.grackleCoreApiClient.ReleaseSemaphore(ctx, &corepb.ReleaseSemaphoreRequest{
		NamespaceId:   resp1.Namespace.Id,
		SemaphoreName: request.SemaphoreName,
		LeaseId:       leaseId.LeaseId,
		Now:           now.UnixNano(),
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

	// Validate semaphore size doesn't exceed account limits
	if request.Permits > uint64(limits.MaxNumberOfSemaphoreHolders) {
		return nil, status.Errorf(codes.InvalidArgument, "semaphore size is too big, max: %d", limits.MaxNumberOfSemaphoreHolders)
	}

	// Resolve namespace by name to get its ID
	resp1, err := s.grackleCoreApiClient.GetNamespaceByName(ctx, &corepb.GetNamespaceByNameRequest{
		AccountId:     accountId,
		NamespaceName: request.NamespaceName,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	// Update the semaphore
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
	// Resolve namespace by name to get its ID
	resp1, err := s.grackleCoreApiClient.GetNamespaceByName(ctx, &corepb.GetNamespaceByNameRequest{
		AccountId:     accountId,
		NamespaceName: request.NamespaceName,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	// Delete the semaphore
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

	// Resolve namespace by name to get its ID
	resp1, err := s.grackleCoreApiClient.GetNamespaceByName(ctx, &corepb.GetNamespaceByNameRequest{
		AccountId:     accountId,
		NamespaceName: request.NamespaceName,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	// Create barrier with generated ID
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
	// Resolve namespace by name to get its ID
	resp1, err := s.grackleCoreApiClient.GetNamespaceByName(ctx, &corepb.GetNamespaceByNameRequest{
		AccountId:     accountId,
		NamespaceName: request.NamespaceName,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	// Decode pagination token from base64-encoded format
	paginationToken, err := paginationTokenFromFront(request.PaginationToken)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	// List barriers in namespace with pagination
	resp2, err := s.grackleCoreApiClient.ListBarriers(ctx, &corepb.ListBarriersRequest{
		NamespaceId:     resp1.Namespace.Id,
		PaginationToken: paginationToken,
		Limit:           request.Limit,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	// Encode pagination tokens for response
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
	// Resolve namespace by name to get its ID
	resp1, err := s.grackleCoreApiClient.GetNamespaceByName(ctx, &corepb.GetNamespaceByNameRequest{
		AccountId:     accountId,
		NamespaceName: request.NamespaceName,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	// Retrieve barrier by name
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

	// Resolve namespace by name to get its ID
	resp1, err := s.grackleCoreApiClient.GetNamespaceByName(ctx, &corepb.GetNamespaceByNameRequest{
		AccountId:     accountId,
		NamespaceName: request.NamespaceName,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	// Delete the barrier
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

	// Resolve namespace by name to get its ID
	resp1, err := s.grackleCoreApiClient.GetNamespaceByName(ctx, &corepb.GetNamespaceByNameRequest{
		AccountId:     accountId,
		NamespaceName: request.NamespaceName,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	// Retrieve barrier to get its ID
	resp2, err := s.grackleCoreApiClient.GetBarrierByName(ctx, &corepb.GetBarrierByNameRequest{
		NamespaceId: resp1.Namespace.Id,
		BarrierName: request.BarrierName,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	// Update barrier
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

	// Resolve namespace by name to get its ID
	resp1, err := s.grackleCoreApiClient.GetNamespaceByName(ctx, &corepb.GetNamespaceByNameRequest{
		AccountId:     accountId,
		NamespaceName: request.NamespaceName,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	// Mark process as arrived at barrier
	resp2, err := s.grackleCoreApiClient.ArriveAtBarrier(ctx, &corepb.ArriveAtBarrierRequest{
		NamespaceId: resp1.Namespace.Id,
		BarrierName: request.BarrierName,
		ProcessId:   request.ProcessId,
		Generation:  request.ExpectedGeneration,
		Now:         now.UnixNano(),
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	// Check if all processes have arrived at the expected generation
	allArrived := resp2.Barrier.ArrivedProcesses >= resp2.Barrier.ExpectedProcesses &&
		resp2.Barrier.Generation == request.ExpectedGeneration

	return &gracklepb.ArriveAtBarrierResponse{
		Barrier:        barrierToFront(resp2.Barrier),
		AllArrived:     allArrived,
		NextGeneration: resp2.Barrier.Generation + 1,
	}, nil
}

func (s *GrackleApiServerHandler) WaitAtBarrier(ctx context.Context, request *gracklepb.WaitAtBarrierRequest, accountId uint64, limits grackle.GrackleServiceLimits) (*gracklepb.WaitAtBarrierResponse, error) {
	// Resolve namespace by name once to avoid repeated lookups
	resp1, err := s.grackleCoreApiClient.GetNamespaceByName(ctx, &corepb.GetNamespaceByNameRequest{
		AccountId:     accountId,
		NamespaceName: request.NamespaceName,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	// Calculate absolute deadline for timeout
	deadline := time.Now().Add(time.Duration(request.TimeoutSeconds) * time.Second)

	// Initialize polling with exponential backoff
	pollInterval := 100 * time.Millisecond
	maxPollInterval := 1 * time.Second

	for {
		// Check if context is cancelled
		if ctx.Err() != nil {
			return nil, status.Errorf(codes.Canceled, "request cancelled")
		}

		// Poll barrier state
		resp2, err := s.grackleCoreApiClient.GetBarrierByName(ctx, &corepb.GetBarrierByNameRequest{
			NamespaceId: resp1.Namespace.Id,
			BarrierName: request.BarrierName,
		})
		if err != nil {
			return nil, monsterax.ErrorToGRPC(err)
		}

		// Check completion and timeout conditions
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
			// Increase poll interval with exponential backoff
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
	// Resolve namespace by name to get its ID
	resp1, err := s.grackleCoreApiClient.GetNamespaceByName(ctx, &corepb.GetNamespaceByNameRequest{
		AccountId:     accountId,
		NamespaceName: request.NamespaceName,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	// Decode pagination token from base64-encoded format
	paginationToken, err := paginationTokenFromFront(request.PaginationToken)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	// List participants for the specific barrier generation
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

	// Encode pagination tokens for response
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

func (s *GrackleApiServerHandler) CreateSemaphoreLease(ctx context.Context, request *gracklepb.CreateSemaphoreLeaseRequest, accountId uint64, limits grackle.GrackleServiceLimits) (*gracklepb.CreateSemaphoreLeaseResponse, error) {
	now := time.Now()

	// Resolve namespace by name to get its ID
	resp1, err := s.grackleCoreApiClient.GetNamespaceByName(ctx, &corepb.GetNamespaceByNameRequest{
		AccountId:     accountId,
		NamespaceName: request.NamespaceName,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	// Create semaphore lease with generated ID
	resp2, err := s.grackleCoreApiClient.CreateSemaphoreLease(ctx, &corepb.CreateSemaphoreLeaseRequest{
		LeaseId: &corepb.LeaseId{
			AccountId:   accountId,
			NamespaceId: resp1.Namespace.Id.NamespaceId,
			LeaseId:     rand.Uint64(),
		},
		ProcessId:                  request.ProcessId,
		TtlSeconds:                 request.TtlSeconds,
		Now:                        now.UnixNano(),
		MaxNumberOfSemaphoreLeases: limits.MaxNumberOfSemaphoreLeases,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	return &gracklepb.CreateSemaphoreLeaseResponse{
		Lease: leaseToFront(resp2.Lease),
	}, nil
}

func (s *GrackleApiServerHandler) RevokeSemaphoreLease(ctx context.Context, request *gracklepb.RevokeSemaphoreLeaseRequest, accountId uint64, limits grackle.GrackleServiceLimits) (*gracklepb.RevokeSemaphoreLeaseResponse, error) {
	now := time.Now()

	// Resolve namespace by name to get its ID
	resp1, err := s.grackleCoreApiClient.GetNamespaceByName(ctx, &corepb.GetNamespaceByNameRequest{
		AccountId:     accountId,
		NamespaceName: request.NamespaceName,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	// Decode and validate lease ID
	leaseId, err := ids.DecodeLeaseId(request.LeaseId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid RevokeSemaphoreLeaseRequest.LeaseId: %v", err)
	}

	// Validate lease ID belongs to the account and namespace
	if leaseId.AccountId != accountId || leaseId.NamespaceId != resp1.Namespace.Id.NamespaceId {
		return nil, status.Errorf(codes.NotFound, "lease not found")
	}

	// Revoke the semaphore lease
	_, err = s.grackleCoreApiClient.RevokeSemaphoreLease(ctx, &corepb.RevokeSemaphoreLeaseRequest{
		LeaseId: leaseId,
		Now:     now.UnixNano(),
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	return &gracklepb.RevokeSemaphoreLeaseResponse{}, nil
}

func (s *GrackleApiServerHandler) RefreshSemaphoreLease(ctx context.Context, request *gracklepb.RefreshSemaphoreLeaseRequest, accountId uint64, limits grackle.GrackleServiceLimits) (*gracklepb.RefreshSemaphoreLeaseResponse, error) {
	now := time.Now()

	// Resolve namespace by name to get its ID
	resp1, err := s.grackleCoreApiClient.GetNamespaceByName(ctx, &corepb.GetNamespaceByNameRequest{
		AccountId:     accountId,
		NamespaceName: request.NamespaceName,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	// Decode and validate lease ID
	leaseId, err := ids.DecodeLeaseId(request.LeaseId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid RefreshSemaphoreLeaseRequest.LeaseId: %v", err)
	}

	// Validate lease ID belongs to the account and namespace
	if leaseId.AccountId != accountId || leaseId.NamespaceId != resp1.Namespace.Id.NamespaceId {
		return nil, status.Errorf(codes.NotFound, "lease not found")
	}

	// Refresh the semaphore lease TTL
	resp2, err := s.grackleCoreApiClient.RefreshSemaphoreLease(ctx, &corepb.RefreshSemaphoreLeaseRequest{
		LeaseId:    leaseId,
		TtlSeconds: request.TtlSeconds,
		Now:        now.UnixNano(),
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	return &gracklepb.RefreshSemaphoreLeaseResponse{
		Lease: leaseToFront(resp2.Lease),
	}, nil
}

func (s *GrackleApiServerHandler) ListSemaphoreLeases(ctx context.Context, request *gracklepb.ListSemaphoreLeasesRequest, accountId uint64, limits grackle.GrackleServiceLimits) (*gracklepb.ListSemaphoreLeasesResponse, error) {
	// Resolve namespace by name to get its ID
	resp1, err := s.grackleCoreApiClient.GetNamespaceByName(ctx, &corepb.GetNamespaceByNameRequest{
		AccountId:     accountId,
		NamespaceName: request.NamespaceName,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	// Decode pagination token from base64-encoded format
	paginationToken, err := paginationTokenFromFront(request.PaginationToken)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	// List semaphore leases with pagination
	resp2, err := s.grackleCoreApiClient.ListSemaphoreLeases(ctx, &corepb.ListSemaphoreLeasesRequest{
		NamespaceId:     resp1.Namespace.Id,
		PaginationToken: paginationToken,
		Limit:           request.Limit,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	// Encode pagination tokens for response
	nextPaginationToken, err := paginationTokenToFront(resp2.NextPaginationToken)
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}
	previousPaginationToken, err := paginationTokenToFront(resp2.PreviousPaginationToken)
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	return &gracklepb.ListSemaphoreLeasesResponse{
		Leases:                  leasesToFront(resp2.Leases),
		NextPaginationToken:     nextPaginationToken,
		PreviousPaginationToken: previousPaginationToken,
	}, nil
}

func (s *GrackleApiServerHandler) GetSemaphoreLease(ctx context.Context, request *gracklepb.GetSemaphoreLeaseRequest, accountId uint64, limits grackle.GrackleServiceLimits) (*gracklepb.GetSemaphoreLeaseResponse, error) {
	// Resolve namespace by name to get its ID
	resp1, err := s.grackleCoreApiClient.GetNamespaceByName(ctx, &corepb.GetNamespaceByNameRequest{
		AccountId:     accountId,
		NamespaceName: request.NamespaceName,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	// Decode and validate lease ID
	leaseId, err := ids.DecodeLeaseId(request.LeaseId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid GetSemaphoreLeaseRequest.LeaseId: %v", err)
	}

	// Validate lease ID belongs to the account and namespace
	if leaseId.AccountId != accountId || leaseId.NamespaceId != resp1.Namespace.Id.NamespaceId {
		return nil, status.Errorf(codes.NotFound, "lease not found")
	}

	// Retrieve semaphore lease by ID
	resp2, err := s.grackleCoreApiClient.GetSemaphoreLease(ctx, &corepb.GetSemaphoreLeaseRequest{
		LeaseId: leaseId,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	return &gracklepb.GetSemaphoreLeaseResponse{
		Lease: leaseToFront(resp2.Lease),
	}, nil
}

func (s *GrackleApiServerHandler) CreateLockLease(ctx context.Context, request *gracklepb.CreateLockLeaseRequest, accountId uint64, limits grackle.GrackleServiceLimits) (*gracklepb.CreateLockLeaseResponse, error) {
	now := time.Now()

	// Resolve namespace by name to get its ID
	resp1, err := s.grackleCoreApiClient.GetNamespaceByName(ctx, &corepb.GetNamespaceByNameRequest{
		AccountId:     accountId,
		NamespaceName: request.NamespaceName,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	// Create lock lease with generated ID
	resp2, err := s.grackleCoreApiClient.CreateLockLease(ctx, &corepb.CreateLockLeaseRequest{
		LeaseId: &corepb.LeaseId{
			AccountId:   accountId,
			NamespaceId: resp1.Namespace.Id.NamespaceId,
			LeaseId:     rand.Uint64(),
		},
		ProcessId:             request.ProcessId,
		TtlSeconds:            request.TtlSeconds,
		Now:                   now.UnixNano(),
		MaxNumberOfLockLeases: limits.MaxNumberOfLockLeases,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	return &gracklepb.CreateLockLeaseResponse{
		Lease: leaseToFront(resp2.Lease),
	}, nil
}

func (s *GrackleApiServerHandler) RevokeLockLease(ctx context.Context, request *gracklepb.RevokeLockLeaseRequest, accountId uint64, limits grackle.GrackleServiceLimits) (*gracklepb.RevokeLockLeaseResponse, error) {
	now := time.Now()

	// Resolve namespace by name to get its ID
	resp1, err := s.grackleCoreApiClient.GetNamespaceByName(ctx, &corepb.GetNamespaceByNameRequest{
		AccountId:     accountId,
		NamespaceName: request.NamespaceName,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	// Decode and validate lease ID
	leaseId, err := ids.DecodeLeaseId(request.LeaseId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid RevokeLockLeaseRequest.LeaseId: %v", err)
	}

	// Validate lease ID belongs to the account and namespace
	if leaseId.AccountId != accountId || leaseId.NamespaceId != resp1.Namespace.Id.NamespaceId {
		return nil, status.Errorf(codes.NotFound, "lease not found")
	}

	// Revoke the lock lease
	_, err = s.grackleCoreApiClient.RevokeLockLease(ctx, &corepb.RevokeLockLeaseRequest{
		LeaseId: leaseId,
		Now:     now.UnixNano(),
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	return &gracklepb.RevokeLockLeaseResponse{}, nil
}

func (s *GrackleApiServerHandler) RefreshLockLease(ctx context.Context, request *gracklepb.RefreshLockLeaseRequest, accountId uint64, limits grackle.GrackleServiceLimits) (*gracklepb.RefreshLockLeaseResponse, error) {
	now := time.Now()

	// Resolve namespace by name to get its ID
	resp1, err := s.grackleCoreApiClient.GetNamespaceByName(ctx, &corepb.GetNamespaceByNameRequest{
		AccountId:     accountId,
		NamespaceName: request.NamespaceName,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	// Decode and validate lease ID
	leaseId, err := ids.DecodeLeaseId(request.LeaseId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid RefreshLockLeaseRequest.LeaseId: %v", err)
	}

	// Validate lease ID belongs to the account and namespace
	if leaseId.AccountId != accountId || leaseId.NamespaceId != resp1.Namespace.Id.NamespaceId {
		return nil, status.Errorf(codes.NotFound, "lease not found")
	}

	// Refresh the lock lease TTL
	resp2, err := s.grackleCoreApiClient.RefreshLockLease(ctx, &corepb.RefreshLockLeaseRequest{
		LeaseId:    leaseId,
		TtlSeconds: request.TtlSeconds,
		Now:        now.UnixNano(),
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	return &gracklepb.RefreshLockLeaseResponse{
		Lease: leaseToFront(resp2.Lease),
	}, nil
}

func (s *GrackleApiServerHandler) ListLockLeases(ctx context.Context, request *gracklepb.ListLockLeasesRequest, accountId uint64, limits grackle.GrackleServiceLimits) (*gracklepb.ListLockLeasesResponse, error) {
	now := time.Now()

	// Resolve namespace by name to get its ID
	resp1, err := s.grackleCoreApiClient.GetNamespaceByName(ctx, &corepb.GetNamespaceByNameRequest{
		AccountId:     accountId,
		NamespaceName: request.NamespaceName,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	// Decode pagination token from base64-encoded format
	paginationToken, err := paginationTokenFromFront(request.PaginationToken)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	// List lock leases with pagination
	resp2, err := s.grackleCoreApiClient.ListLockLeases(ctx, &corepb.ListLockLeasesRequest{
		NamespaceId:     resp1.Namespace.Id,
		Now:             now.UnixNano(),
		PaginationToken: paginationToken,
		Limit:           request.Limit,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	// Encode pagination tokens for response
	nextPaginationToken, err := paginationTokenToFront(resp2.NextPaginationToken)
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}
	previousPaginationToken, err := paginationTokenToFront(resp2.PreviousPaginationToken)
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	return &gracklepb.ListLockLeasesResponse{
		Leases:                  leasesToFront(resp2.Leases),
		NextPaginationToken:     nextPaginationToken,
		PreviousPaginationToken: previousPaginationToken,
	}, nil
}

func (s *GrackleApiServerHandler) GetLockLease(ctx context.Context, request *gracklepb.GetLockLeaseRequest, accountId uint64, limits grackle.GrackleServiceLimits) (*gracklepb.GetLockLeaseResponse, error) {
	now := time.Now()

	// Resolve namespace by name to get its ID
	resp1, err := s.grackleCoreApiClient.GetNamespaceByName(ctx, &corepb.GetNamespaceByNameRequest{
		AccountId:     accountId,
		NamespaceName: request.NamespaceName,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	// Decode and validate lease ID
	leaseId, err := ids.DecodeLeaseId(request.LeaseId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid GetLockLeaseRequest.LeaseId: %v", err)
	}

	// Validate lease ID belongs to the account and namespace
	if leaseId.AccountId != accountId || leaseId.NamespaceId != resp1.Namespace.Id.NamespaceId {
		return nil, status.Errorf(codes.NotFound, "lease not found")
	}

	// Retrieve lock lease by ID
	resp2, err := s.grackleCoreApiClient.GetLockLease(ctx, &corepb.GetLockLeaseRequest{
		LeaseId: leaseId,
		Now:     now.UnixNano(),
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	return &gracklepb.GetLockLeaseResponse{
		Lease: leaseToFront(resp2.Lease),
	}, nil
}

func NewGrackleApiServerHandler(grackleCoreApiClient monsteragen.GrackleCoreApi) *GrackleApiServerHandler {
	return &GrackleApiServerHandler{
		grackleCoreApiClient: grackleCoreApiClient,
	}
}
