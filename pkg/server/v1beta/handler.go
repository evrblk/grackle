package v1beta

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	gracklepb "github.com/evrblk/evrblk-go/grackle/v1beta"
	mrpc "github.com/evrblk/monstera/rpc"
	"github.com/evrblk/yellowstone-common/cache"

	"github.com/evrblk/grackle/pkg/coreapis"
	"github.com/evrblk/grackle/pkg/corepb"
	"github.com/evrblk/grackle/pkg/grackle"
	"github.com/evrblk/grackle/pkg/ids"
)

const (
	// minWaitGroupExpiresInFuture is the minimum lead time required between now and
	// a wait group's expires_at deadline.
	minWaitGroupExpiresInFuture = 1 * time.Minute

	// maxIDGenerationAttempts bounds how many times a handler regenerates a random
	// entity ID and retries when the core reports an ID collision.
	maxIDGenerationAttempts = 5

	namespacesCacheTTL         = 5 * time.Second
	namespacesCacheNegativeTTL = 1 * time.Second
)

type GrackleApiServerHandler struct {
	grackleClient coreapis.GrackleClientApi

	namespacesCache *cache.Cache[string, *corepb.Namespace]
}

func (s *GrackleApiServerHandler) Stop() {
	s.namespacesCache.Close()
}

func (s *GrackleApiServerHandler) CreateNamespace(ctx context.Context, req *gracklepb.CreateNamespaceRequest, accountId uint64, limits grackle.ServiceLimits) (*gracklepb.CreateNamespaceResponse, error) {
	// The namespace ID is randomly generated here. On the rare ID collision
	// the core returns IDCollision; we regenerate the ID and retry.
	for range maxIDGenerationAttempts {
		// Create namespace with generated ID and enforce account limits
		resp, err := s.grackleClient.CreateNamespace(ctx, &corepb.CreateNamespaceRequest{
			NamespaceId: &corepb.NamespaceId{
				AccountId:   accountId,
				NamespaceId: rand.Uint64(),
			},
			Name:                  req.Name,
			Description:           req.Description,
			Metadata:              req.Metadata,
			MaxNumberOfNamespaces: limits.MaxNumberOfNamespaces,
		})
		if err != nil {
			if isIDCollision(err) {
				continue
			}
			return nil, mrpc.ErrorToGRPC(err)
		}

		return &gracklepb.CreateNamespaceResponse{
			Namespace: namespaceToFront(resp.Namespace),
		}, nil
	}

	return nil, status.Error(codes.Internal, "failed to generate a unique namespace id")
}

func (s *GrackleApiServerHandler) GetNamespace(ctx context.Context, req *gracklepb.GetNamespaceRequest, accountId uint64, limits grackle.ServiceLimits) (*gracklepb.GetNamespaceResponse, error) {
	// Retrieve namespace by name for the given account
	resp1, err := s.grackleClient.GetNamespaceByName(ctx, &corepb.GetNamespaceByNameRequest{
		AccountId:     accountId,
		NamespaceName: req.NamespaceName,
	})
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	return &gracklepb.GetNamespaceResponse{
		Namespace: namespaceToFront(resp1.Namespace),
	}, nil
}

func (s *GrackleApiServerHandler) UpdateNamespace(ctx context.Context, req *gracklepb.UpdateNamespaceRequest, accountId uint64, limits grackle.ServiceLimits) (*gracklepb.UpdateNamespaceResponse, error) {
	// Update namespace
	resp1, err := s.grackleClient.UpdateNamespace(ctx, &corepb.UpdateNamespaceRequest{
		AccountId:       accountId,
		NamespaceName:   req.NamespaceName,
		Description:     req.Description,
		Metadata:        req.Metadata,
		ExpectedVersion: req.ExpectedVersion,
	})
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	return &gracklepb.UpdateNamespaceResponse{
		Namespace: namespaceToFront(resp1.Namespace),
	}, nil
}

// TODO what to do with active AcquireLock, WaitForWaitGroup, etc requests?
func (s *GrackleApiServerHandler) DeleteNamespace(ctx context.Context, req *gracklepb.DeleteNamespaceRequest, accountId uint64, limits grackle.ServiceLimits) (*gracklepb.DeleteNamespaceResponse, error) {
	gcRecordId := rand.Uint64()

	// Resolve namespace by name to get its ID
	resp1, err := s.grackleClient.GetNamespaceByName(ctx, &corepb.GetNamespaceByNameRequest{
		AccountId:     accountId,
		NamespaceName: req.NamespaceName,
	})
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	// Mark locks for garbage collection
	_, err = s.grackleClient.LocksDeleteNamespace(ctx, &corepb.LocksDeleteNamespaceRequest{
		RecordId:    gcRecordId,
		NamespaceId: resp1.Namespace.Id,
	})
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	// Mark wait groups for garbage collection
	_, err = s.grackleClient.WaitGroupsDeleteNamespace(ctx, &corepb.WaitGroupsDeleteNamespaceRequest{
		RecordId:    gcRecordId,
		NamespaceId: resp1.Namespace.Id,
	})
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	// Mark semaphores for garbage collection
	_, err = s.grackleClient.SemaphoresDeleteNamespace(ctx, &corepb.SemaphoresDeleteNamespaceRequest{
		RecordId:    gcRecordId,
		NamespaceId: resp1.Namespace.Id,
	})
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	// Mark barriers for garbage collection
	_, err = s.grackleClient.BarriersDeleteNamespace(ctx, &corepb.BarriersDeleteNamespaceRequest{
		RecordId:    gcRecordId,
		NamespaceId: resp1.Namespace.Id,
	})
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	// Delete the namespace itself
	_, err = s.grackleClient.DeleteNamespace(ctx, &corepb.DeleteNamespaceRequest{
		NamespaceName: req.NamespaceName,
	})
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	return &gracklepb.DeleteNamespaceResponse{}, nil
}

func (s *GrackleApiServerHandler) ListNamespaces(ctx context.Context, req *gracklepb.ListNamespacesRequest, accountId uint64, limits grackle.ServiceLimits) (*gracklepb.ListNamespacesResponse, error) {
	// Decode pagination token from base64-encoded format
	paginationToken, err := paginationTokenToCore(req.PaginationToken)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	// List all namespaces for the account with pagination
	resp1, err := s.grackleClient.ListNamespaces(ctx, &corepb.ListNamespacesRequest{
		AccountId:       accountId,
		PaginationToken: paginationToken,
		Limit:           req.Limit,
	})
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	// Encode pagination tokens for response
	nextPaginationToken, err := paginationTokenToFront(resp1.NextPaginationToken)
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}
	previousPaginationToken, err := paginationTokenToFront(resp1.PreviousPaginationToken)
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	return &gracklepb.ListNamespacesResponse{
		Namespaces:              namespacesToFront(resp1.Namespaces),
		NextPaginationToken:     nextPaginationToken,
		PreviousPaginationToken: previousPaginationToken,
	}, nil
}

func (s *GrackleApiServerHandler) CreateWaitGroup(ctx context.Context, req *gracklepb.CreateWaitGroupRequest, accountId uint64, limits grackle.ServiceLimits) (*gracklepb.CreateWaitGroupResponse, error) {
	now := time.Now()

	// Validate wait group size doesn't exceed account limits
	if req.Counter > limits.MaxWaitGroupSize {
		return nil, status.Errorf(codes.InvalidArgument, "wait group size is too big, max: %d", limits.MaxWaitGroupSize)
	}

	// A deadline is mandatory and must be far enough into the future.
	if req.ExpiresAt < now.Add(minWaitGroupExpiresInFuture).UnixNano() {
		return nil, status.Errorf(codes.InvalidArgument, "expires_at must be at least %s into the future", minWaitGroupExpiresInFuture)
	}

	// Resolve namespace by name to get its ID
	namespace, err := s.getNamespace(accountId, req.NamespaceName)
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	// Create wait group with generated ID. On the rare ID collision the core
	// returns IDCollision; regenerate the ID and retry rather than surfacing it.
	for range maxIDGenerationAttempts {
		resp1, err := s.grackleClient.CreateWaitGroup(ctx, &corepb.CreateWaitGroupRequest{
			WaitGroupId: &corepb.WaitGroupId{
				AccountId:   accountId,
				NamespaceId: namespace.Id.NamespaceId,
				WaitGroupId: rand.Uint64(),
			},
			Name:                              req.WaitGroupName,
			Description:                       req.Description,
			Counter:                           req.Counter,
			ExpiresAt:                         req.ExpiresAt,
			Metadata:                          req.Metadata,
			MaxNumberOfWaitGroupsPerNamespace: limits.MaxNumberOfWaitGroupsPerNamespace,
			DeleteAfterFinishedSeconds:        req.DeleteAfterFinishedSeconds,
		})
		if err != nil {
			if isIDCollision(err) {
				continue
			}
			return nil, mrpc.ErrorToGRPC(err)
		}

		return &gracklepb.CreateWaitGroupResponse{
			WaitGroup: waitGroupToFront(resp1.WaitGroup),
		}, nil
	}

	return nil, status.Error(codes.Internal, "failed to generate a unique wait group id")
}

func (s *GrackleApiServerHandler) UpdateWaitGroup(ctx context.Context, req *gracklepb.UpdateWaitGroupRequest, accountId uint64, limits grackle.ServiceLimits) (*gracklepb.UpdateWaitGroupResponse, error) {
	now := time.Now()

	// Validate wait group size doesn't exceed account limits
	// TODO: consistent error format with Validate* methods
	if req.Counter > limits.MaxWaitGroupSize {
		return nil, status.Errorf(codes.InvalidArgument, "wait group size is too big, max: %d", limits.MaxWaitGroupSize)
	}

	// A deadline is mandatory and must be into the future.
	if req.ExpiresAt < now.UnixNano() {
		return nil, status.Errorf(codes.InvalidArgument, "expires_at must be into the future")
	}

	// Resolve namespace by name to get its ID
	namespace, err := s.getNamespace(accountId, req.NamespaceName)
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	// Create wait group with generated ID
	resp1, err := s.grackleClient.UpdateWaitGroup(ctx, &corepb.UpdateWaitGroupRequest{
		NamespaceId:                namespace.Id,
		WaitGroupName:              req.WaitGroupName,
		Description:                req.Description,
		Counter:                    req.Counter,
		ExpiresAt:                  req.ExpiresAt,
		Metadata:                   req.Metadata,
		ExpectedVersion:            req.ExpectedVersion,
		DeleteAfterFinishedSeconds: req.DeleteAfterFinishedSeconds,
	})
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	return &gracklepb.UpdateWaitGroupResponse{
		WaitGroup: waitGroupToFront(resp1.WaitGroup),
	}, nil
}

func (s *GrackleApiServerHandler) GetWaitGroup(ctx context.Context, req *gracklepb.GetWaitGroupRequest, accountId uint64, limits grackle.ServiceLimits) (*gracklepb.GetWaitGroupResponse, error) {
	// Resolve namespace by name to get its ID
	namespace, err := s.getNamespace(accountId, req.NamespaceName)
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	// Retrieve wait group by name within the namespace
	resp1, err := s.grackleClient.GetWaitGroupByName(ctx, &corepb.GetWaitGroupByNameRequest{
		NamespaceId:   namespace.Id,
		WaitGroupName: req.WaitGroupName,
	})
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	return &gracklepb.GetWaitGroupResponse{
		WaitGroup: waitGroupToFront(resp1.WaitGroup),
	}, nil
}

func (s *GrackleApiServerHandler) WaitForWaitGroup(ctx context.Context, req *gracklepb.WaitForWaitGroupRequest, accountId uint64, limits grackle.ServiceLimits) (*gracklepb.WaitForWaitGroupResponse, error) {
	// Resolve namespace by name once to avoid repeated lookups
	namespace, err := s.getNamespace(accountId, req.NamespaceName)
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	// Calculate absolute deadline for timeout
	deadline := time.Now().Add(time.Duration(req.TimeoutSeconds) * time.Second)

	// Initialize polling with exponential backoff
	pollInterval := 100 * time.Millisecond
	maxPollInterval := 1 * time.Second

	for {
		// Check if context is cancelled
		if ctx.Err() != nil {
			return nil, status.Errorf(codes.Canceled, "req cancelled")
		}

		// Poll wait group state
		resp1, err := s.grackleClient.GetWaitGroupByName(ctx, &corepb.GetWaitGroupByNameRequest{
			NamespaceId:   namespace.Id,
			WaitGroupName: req.WaitGroupName,
		})
		if err != nil {
			return nil, mrpc.ErrorToGRPC(err)
		}

		// Return as soon as the wait group has finished (completed or expired),
		// or once the deadline passes while it is still active.
		switch resp1.WaitGroup.Status {
		case corepb.WaitGroupStatus_WAIT_GROUP_STATUS_COMPLETED:
			return &gracklepb.WaitForWaitGroupResponse{
				WaitGroup: waitGroupToFront(resp1.WaitGroup),
				Outcome:   gracklepb.WaitGroupWaitOutcome_WAIT_GROUP_WAIT_OUTCOME_COMPLETED,
			}, nil
		case corepb.WaitGroupStatus_WAIT_GROUP_STATUS_EXPIRED:
			return &gracklepb.WaitForWaitGroupResponse{
				WaitGroup: waitGroupToFront(resp1.WaitGroup),
				Outcome:   gracklepb.WaitGroupWaitOutcome_WAIT_GROUP_WAIT_OUTCOME_EXPIRED,
			}, nil
		}

		if time.Now().After(deadline) {
			return &gracklepb.WaitForWaitGroupResponse{
				WaitGroup: waitGroupToFront(resp1.WaitGroup),
				Outcome:   gracklepb.WaitGroupWaitOutcome_WAIT_GROUP_WAIT_OUTCOME_TIMED_OUT,
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
			pollInterval = min(pollInterval*2, maxPollInterval)
		case <-ctx.Done():
			return nil, status.Errorf(codes.Canceled, "req cancelled")
		}
	}
}

func (s *GrackleApiServerHandler) CompleteJobsFromWaitGroup(ctx context.Context, req *gracklepb.CompleteJobsFromWaitGroupRequest, accountId uint64, limits grackle.ServiceLimits) (*gracklepb.CompleteJobsFromWaitGroupResponse, error) {
	// Resolve namespace by name to get its ID
	namespace, err := s.getNamespace(accountId, req.NamespaceName)
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	// Mark jobs as completed in the wait group
	resp1, err := s.grackleClient.CompleteJobsFromWaitGroup(ctx, &corepb.CompleteJobsFromWaitGroupRequest{
		NamespaceId:   namespace.Id,
		WaitGroupName: req.WaitGroupName,
		Jobs:          completeJobsToCore(req.Jobs),
	})
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	return &gracklepb.CompleteJobsFromWaitGroupResponse{
		WaitGroup: waitGroupToFront(resp1.WaitGroup),
	}, nil
}

func (s *GrackleApiServerHandler) DeleteWaitGroup(ctx context.Context, req *gracklepb.DeleteWaitGroupRequest, accountId uint64, limits grackle.ServiceLimits) (*gracklepb.DeleteWaitGroupResponse, error) {
	// Resolve namespace by name to get its ID
	namespace, err := s.getNamespace(accountId, req.NamespaceName)
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	// Delete wait group and mark for garbage collection
	_, err = s.grackleClient.DeleteWaitGroup(ctx, &corepb.DeleteWaitGroupRequest{
		NamespaceId:   namespace.Id,
		WaitGroupName: req.WaitGroupName,
		RecordId:      rand.Uint64(),
	})
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	return &gracklepb.DeleteWaitGroupResponse{}, nil
}

func (s *GrackleApiServerHandler) ListWaitGroups(ctx context.Context, req *gracklepb.ListWaitGroupsRequest, accountId uint64, limits grackle.ServiceLimits) (*gracklepb.ListWaitGroupsResponse, error) {
	// Resolve namespace by name to get its ID
	namespace, err := s.getNamespace(accountId, req.NamespaceName)
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	// Decode pagination token from base64-encoded format
	paginationToken, err := paginationTokenToCore(req.PaginationToken)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	// List wait groups in namespace with pagination
	resp1, err := s.grackleClient.ListWaitGroups(ctx, &corepb.ListWaitGroupsRequest{
		NamespaceId:     namespace.Id,
		PaginationToken: paginationToken,
		Limit:           req.Limit,
	})
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	// Encode pagination tokens for response
	nextPaginationToken, err := paginationTokenToFront(resp1.NextPaginationToken)
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}
	previousPaginationToken, err := paginationTokenToFront(resp1.PreviousPaginationToken)
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	return &gracklepb.ListWaitGroupsResponse{
		WaitGroups:              waitGroupsToFront(resp1.WaitGroups),
		NextPaginationToken:     nextPaginationToken,
		PreviousPaginationToken: previousPaginationToken,
	}, nil
}

func (s *GrackleApiServerHandler) ListWaitGroupCompletedJobs(ctx context.Context, req *gracklepb.ListWaitGroupCompletedJobsRequest, accountId uint64, limits grackle.ServiceLimits) (*gracklepb.ListWaitGroupCompletedJobsResponse, error) {
	// Resolve namespace by name to get its ID
	namespace, err := s.getNamespace(accountId, req.NamespaceName)
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	// Decode pagination token from base64-encoded format
	paginationToken, err := paginationTokenToCore(req.PaginationToken)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	// List jobs associated with the wait group
	resp1, err := s.grackleClient.ListWaitGroupCompletedJobs(ctx, &corepb.ListWaitGroupCompletedJobsRequest{
		NamespaceId:     namespace.Id,
		WaitGroupName:   req.WaitGroupName,
		PaginationToken: paginationToken,
		Limit:           req.Limit,
	})
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	// Encode pagination tokens for response
	nextPaginationToken, err := paginationTokenToFront(resp1.NextPaginationToken)
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}
	previousPaginationToken, err := paginationTokenToFront(resp1.PreviousPaginationToken)
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	return &gracklepb.ListWaitGroupCompletedJobsResponse{
		Jobs:                    waitGroupJobsToFront(resp1.Jobs),
		NextPaginationToken:     nextPaginationToken,
		PreviousPaginationToken: previousPaginationToken,
	}, nil
}

func (s *GrackleApiServerHandler) AcquireLock(ctx context.Context, req *gracklepb.AcquireLockRequest, accountId uint64, limits grackle.ServiceLimits) (*gracklepb.AcquireLockResponse, error) {
	// Resolve namespace by name to get its ID
	namespace, err := s.getNamespace(accountId, req.NamespaceName)
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	// Decode and validate lease ID
	leaseId, err := ids.DecodeLeaseId(req.LeaseId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid AcquireLockRequest.LeaseId: %v", err)
	}

	if leaseId.AccountId != accountId || leaseId.NamespaceId != namespace.Id.NamespaceId {
		return nil, status.Errorf(codes.NotFound, "lease not found")
	}

	// Calculate absolute deadline for timeout
	deadline := time.Now().Add(time.Duration(req.TimeoutSeconds) * time.Second)

	// Initialize polling with exponential backoff
	pollInterval := 100 * time.Millisecond
	maxPollInterval := 1 * time.Second

	for {
		// Check if context is cancelled
		if ctx.Err() != nil {
			return nil, status.Errorf(codes.Canceled, "req cancelled")
		}

		// Attempt to acquire lock (shared or exclusive)
		resp1, err := s.grackleClient.AcquireLock(ctx, &corepb.AcquireLockRequest{
			LockId: &corepb.LockId{
				AccountId:   accountId,
				NamespaceId: namespace.Id.NamespaceId,
				LockName:    req.LockName,
			},
			LeaseId:                      leaseId.LeaseId,
			Exclusive:                    req.Exclusive,
			Metadata:                     req.Metadata,
			MaxNumberOfLocksPerNamespace: limits.MaxNumberOfLocksPerNamespace,
		})
		if err != nil {
			return nil, mrpc.ErrorToGRPC(err)
		}

		// Return as soon as the lock is acquired, or once the deadline passes.
		if resp1.Success {
			return &gracklepb.AcquireLockResponse{
				Lock:    lockToFront(resp1.Lock),
				Outcome: gracklepb.AcquireOutcome_ACQUIRE_OUTCOME_ACQUIRED,
			}, nil
		}
		if time.Now().After(deadline) {
			return &gracklepb.AcquireLockResponse{
				Lock:          lockToFront(resp1.Lock),
				Outcome:       acquireFailureOutcome(req.TimeoutSeconds),
				Reason:        contentionReasonToFront(resp1.Reason),
				BlockingLocks: locksToFront(resp1.BlockingLocks),
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
			pollInterval = min(pollInterval*2, maxPollInterval)
		case <-ctx.Done():
			return nil, status.Errorf(codes.Canceled, "req cancelled")
		}
	}
}

func (s *GrackleApiServerHandler) ReleaseLock(ctx context.Context, req *gracklepb.ReleaseLockRequest, accountId uint64, limits grackle.ServiceLimits) (*gracklepb.ReleaseLockResponse, error) {
	// Resolve namespace by name to get its ID
	namespace, err := s.getNamespace(accountId, req.NamespaceName)
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	// Decode and validate lease ID
	leaseId, err := ids.DecodeLeaseId(req.LeaseId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid ReleaseLockRequest.LeaseId: %v", err)
	}

	if leaseId.AccountId != accountId || leaseId.NamespaceId != namespace.Id.NamespaceId {
		return nil, status.Errorf(codes.NotFound, "lease not found")
	}

	// Release the lock held by this lease
	resp1, err := s.grackleClient.ReleaseLock(ctx, &corepb.ReleaseLockRequest{
		LockId: &corepb.LockId{
			AccountId:   accountId,
			NamespaceId: namespace.Id.NamespaceId,
			LockName:    req.LockName,
		},
		LeaseId: leaseId.LeaseId,
	})
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	return &gracklepb.ReleaseLockResponse{
		Lock: lockToFront(resp1.Lock),
	}, nil
}

func (s *GrackleApiServerHandler) GetLock(ctx context.Context, req *gracklepb.GetLockRequest, accountId uint64, limits grackle.ServiceLimits) (*gracklepb.GetLockResponse, error) {
	// Resolve namespace by name to get its ID
	namespace, err := s.getNamespace(accountId, req.NamespaceName)
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	// Retrieve lock state
	resp1, err := s.grackleClient.GetLock(ctx, &corepb.GetLockRequest{
		LockId: &corepb.LockId{
			AccountId:   accountId,
			NamespaceId: namespace.Id.NamespaceId,
			LockName:    req.LockName,
		},
	})
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	return &gracklepb.GetLockResponse{
		Lock: lockToFront(resp1.Lock),
	}, nil
}

func (s *GrackleApiServerHandler) DeleteLock(ctx context.Context, req *gracklepb.DeleteLockRequest, accountId uint64, limits grackle.ServiceLimits) (*gracklepb.DeleteLockResponse, error) {
	// Resolve namespace by name to get its ID
	namespace, err := s.getNamespace(accountId, req.NamespaceName)
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	// Delete the lock
	_, err = s.grackleClient.DeleteLock(ctx, &corepb.DeleteLockRequest{
		LockId: &corepb.LockId{
			AccountId:   accountId,
			NamespaceId: namespace.Id.NamespaceId,
			LockName:    req.LockName,
		},
	})
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	return &gracklepb.DeleteLockResponse{}, nil
}

func (s *GrackleApiServerHandler) ListLocks(ctx context.Context, req *gracklepb.ListLocksRequest, accountId uint64, limits grackle.ServiceLimits) (*gracklepb.ListLocksResponse, error) {
	// Resolve namespace by name to get its ID
	namespace, err := s.getNamespace(accountId, req.NamespaceName)
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	// Decode pagination token from base64-encoded format
	paginationToken, err := paginationTokenToCore(req.PaginationToken)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	// List all locks in namespace with pagination
	resp1, err := s.grackleClient.ListLocks(ctx, &corepb.ListLocksRequest{
		NamespaceId:     namespace.Id,
		PaginationToken: paginationToken,
		Limit:           req.Limit,
	})
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	// Encode pagination tokens for response
	nextPaginationToken, err := paginationTokenToFront(resp1.NextPaginationToken)
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}
	previousPaginationToken, err := paginationTokenToFront(resp1.PreviousPaginationToken)
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	return &gracklepb.ListLocksResponse{
		Locks:                   locksToFront(resp1.Locks),
		NextPaginationToken:     nextPaginationToken,
		PreviousPaginationToken: previousPaginationToken,
	}, nil
}

func (s *GrackleApiServerHandler) CreateSemaphore(ctx context.Context, req *gracklepb.CreateSemaphoreRequest, accountId uint64, limits grackle.ServiceLimits) (*gracklepb.CreateSemaphoreResponse, error) {
	// Validate semaphore size doesn't exceed account limits
	if req.Permits > limits.MaxNumberOfSemaphoreHolders {
		return nil, status.Errorf(codes.InvalidArgument, "semaphore size is too big, max: %d", limits.MaxNumberOfSemaphoreHolders)
	}

	// Resolve namespace by name to get its ID
	namespace, err := s.getNamespace(accountId, req.NamespaceName)
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	// Create semaphore with generated ID. On the rare ID collision the core
	// returns IDCollision; regenerate the ID and retry.
	for range maxIDGenerationAttempts {
		resp1, err := s.grackleClient.CreateSemaphore(ctx, &corepb.CreateSemaphoreRequest{
			SemaphoreId: &corepb.SemaphoreId{
				AccountId:   accountId,
				NamespaceId: namespace.Id.NamespaceId,
				SemaphoreId: rand.Uint64(),
			},
			Name:                              req.SemaphoreName,
			Description:                       req.Description,
			Permits:                           req.Permits,
			Metadata:                          req.Metadata,
			MaxNumberOfSemaphoresPerNamespace: limits.MaxNumberOfSemaphoresPerNamespace,
		})
		if err != nil {
			if isIDCollision(err) {
				continue
			}
			return nil, mrpc.ErrorToGRPC(err)
		}

		return &gracklepb.CreateSemaphoreResponse{
			Semaphore: semaphoreToFront(resp1.Semaphore),
		}, nil
	}

	return nil, status.Error(codes.Internal, "failed to generate a unique semaphore id")
}

func (s *GrackleApiServerHandler) ListSemaphores(ctx context.Context, req *gracklepb.ListSemaphoresRequest, accountId uint64, limits grackle.ServiceLimits) (*gracklepb.ListSemaphoresResponse, error) {
	// Resolve namespace by name to get its ID
	namespace, err := s.getNamespace(accountId, req.NamespaceName)
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	// Decode pagination token from base64-encoded format
	paginationToken, err := paginationTokenToCore(req.PaginationToken)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	// List all semaphores in namespace with pagination
	resp1, err := s.grackleClient.ListSemaphores(ctx, &corepb.ListSemaphoresRequest{
		NamespaceId:     namespace.Id,
		PaginationToken: paginationToken,
		Limit:           req.Limit,
	})
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	// Encode pagination tokens for response
	nextPaginationToken, err := paginationTokenToFront(resp1.NextPaginationToken)
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}
	previousPaginationToken, err := paginationTokenToFront(resp1.PreviousPaginationToken)
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	return &gracklepb.ListSemaphoresResponse{
		Semaphores:              semaphoresToFront(resp1.Semaphores),
		NextPaginationToken:     nextPaginationToken,
		PreviousPaginationToken: previousPaginationToken,
	}, nil
}

func (s *GrackleApiServerHandler) ListSemaphoreHolders(ctx context.Context, req *gracklepb.ListSemaphoreHoldersRequest, accountId uint64, limits grackle.ServiceLimits) (*gracklepb.ListSemaphoreHoldersResponse, error) {
	// Resolve namespace by name to get its ID
	namespace, err := s.getNamespace(accountId, req.NamespaceName)
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	// Decode pagination token from base64-encoded format
	paginationToken, err := paginationTokenToCore(req.PaginationToken)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	// List holders for the semaphore with pagination
	resp1, err := s.grackleClient.ListSemaphoreHolders(ctx, &corepb.ListSemaphoreHoldersRequest{
		NamespaceId:     namespace.Id,
		SemaphoreName:   req.SemaphoreName,
		PaginationToken: paginationToken,
		Limit:           req.Limit,
	})
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	// Encode pagination tokens for response
	nextPaginationToken, err := paginationTokenToFront(resp1.NextPaginationToken)
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}
	previousPaginationToken, err := paginationTokenToFront(resp1.PreviousPaginationToken)
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	return &gracklepb.ListSemaphoreHoldersResponse{
		Holders:                 semaphoreHoldersToFront(resp1.Holders),
		NextPaginationToken:     nextPaginationToken,
		PreviousPaginationToken: previousPaginationToken,
	}, nil
}

func (s *GrackleApiServerHandler) GetSemaphore(ctx context.Context, req *gracklepb.GetSemaphoreRequest, accountId uint64, limits grackle.ServiceLimits) (*gracklepb.GetSemaphoreResponse, error) {
	// Resolve namespace by name to get its ID
	namespace, err := s.getNamespace(accountId, req.NamespaceName)
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	// Retrieve semaphore by name within the namespace
	resp1, err := s.grackleClient.GetSemaphoreByName(ctx, &corepb.GetSemaphoreByNameRequest{
		NamespaceId:   namespace.Id,
		SemaphoreName: req.SemaphoreName,
	})
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	return &gracklepb.GetSemaphoreResponse{
		Semaphore: semaphoreToFront(resp1.Semaphore),
	}, nil
}

func (s *GrackleApiServerHandler) AcquireSemaphore(ctx context.Context, req *gracklepb.AcquireSemaphoreRequest, accountId uint64, limits grackle.ServiceLimits) (*gracklepb.AcquireSemaphoreResponse, error) {
	// Resolve namespace by name to get its ID
	namespace, err := s.getNamespace(accountId, req.NamespaceName)
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	// Decode and validate lease ID
	leaseId, err := ids.DecodeLeaseId(req.LeaseId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid AcquireSemaphoreRequest.LeaseId: %v", err)
	}

	if leaseId.AccountId != accountId || leaseId.NamespaceId != namespace.Id.NamespaceId {
		return nil, status.Errorf(codes.NotFound, "lease not found")
	}

	// Calculate absolute deadline for timeout
	deadline := time.Now().Add(time.Duration(req.TimeoutSeconds) * time.Second)

	// Initialize polling with exponential backoff
	pollInterval := 100 * time.Millisecond
	maxPollInterval := 1 * time.Second

	for {
		// Check if context is cancelled
		if ctx.Err() != nil {
			return nil, status.Errorf(codes.Canceled, "req cancelled")
		}

		// Attempt to acquire semaphore with specified weight
		resp1, err := s.grackleClient.AcquireSemaphore(ctx, &corepb.AcquireSemaphoreRequest{
			NamespaceId:   namespace.Id,
			SemaphoreName: req.SemaphoreName,
			LeaseId:       leaseId.LeaseId,
			Weight:        req.Weight,
			Metadata:      req.Metadata,
		})
		if err != nil {
			return nil, mrpc.ErrorToGRPC(err)
		}

		// Return as soon as the semaphore is acquired, or once the deadline passes.
		if resp1.Success {
			return &gracklepb.AcquireSemaphoreResponse{
				Semaphore: semaphoreToFront(resp1.Semaphore),
				Outcome:   gracklepb.AcquireOutcome_ACQUIRE_OUTCOME_ACQUIRED,
			}, nil
		}
		if time.Now().After(deadline) {
			return &gracklepb.AcquireSemaphoreResponse{
				Semaphore: semaphoreToFront(resp1.Semaphore),
				Outcome:   acquireFailureOutcome(req.TimeoutSeconds),
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
			pollInterval = min(pollInterval*2, maxPollInterval)
		case <-ctx.Done():
			return nil, status.Errorf(codes.Canceled, "req cancelled")
		}
	}
}

func (s *GrackleApiServerHandler) ReleaseSemaphore(ctx context.Context, req *gracklepb.ReleaseSemaphoreRequest, accountId uint64, limits grackle.ServiceLimits) (*gracklepb.ReleaseSemaphoreResponse, error) {
	// Resolve namespace by name to get its ID
	namespace, err := s.getNamespace(accountId, req.NamespaceName)
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	// Decode and validate lease ID
	leaseId, err := ids.DecodeLeaseId(req.LeaseId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid ReleaseSemaphoreRequest.LeaseId: %v", err)
	}

	// Validate lease ID belongs to the account and namespace
	if leaseId.AccountId != accountId || leaseId.NamespaceId != namespace.Id.NamespaceId {
		return nil, status.Errorf(codes.NotFound, "lease not found")
	}

	// Release the semaphore held by this lease
	resp1, err := s.grackleClient.ReleaseSemaphore(ctx, &corepb.ReleaseSemaphoreRequest{
		NamespaceId:   namespace.Id,
		SemaphoreName: req.SemaphoreName,
		LeaseId:       leaseId.LeaseId,
	})
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	return &gracklepb.ReleaseSemaphoreResponse{
		Semaphore: semaphoreToFront(resp1.Semaphore),
	}, nil
}

func (s *GrackleApiServerHandler) UpdateSemaphore(ctx context.Context, req *gracklepb.UpdateSemaphoreRequest, accountId uint64, limits grackle.ServiceLimits) (*gracklepb.UpdateSemaphoreResponse, error) {
	// Validate semaphore size doesn't exceed account limits
	if req.Permits > limits.MaxNumberOfSemaphoreHolders {
		return nil, status.Errorf(codes.InvalidArgument, "semaphore size is too big, max: %d", limits.MaxNumberOfSemaphoreHolders)
	}

	// Resolve namespace by name to get its ID
	namespace, err := s.getNamespace(accountId, req.NamespaceName)
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	// Update the semaphore
	resp1, err := s.grackleClient.UpdateSemaphore(ctx, &corepb.UpdateSemaphoreRequest{
		NamespaceId:     namespace.Id,
		SemaphoreName:   req.SemaphoreName,
		Description:     req.Description,
		Permits:         req.Permits,
		Metadata:        req.Metadata,
		ExpectedVersion: req.ExpectedVersion,
	})
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	return &gracklepb.UpdateSemaphoreResponse{
		Semaphore: semaphoreToFront(resp1.Semaphore),
	}, nil
}

func (s *GrackleApiServerHandler) DeleteSemaphore(ctx context.Context, req *gracklepb.DeleteSemaphoreRequest, accountId uint64, limits grackle.ServiceLimits) (*gracklepb.DeleteSemaphoreResponse, error) {
	// Resolve namespace by name to get its ID
	namespace, err := s.getNamespace(accountId, req.NamespaceName)
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	// Delete the semaphore
	_, err = s.grackleClient.DeleteSemaphore(ctx, &corepb.DeleteSemaphoreRequest{
		NamespaceId:   namespace.Id,
		SemaphoreName: req.SemaphoreName,
		RecordId:      rand.Uint64(),
	})
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	return &gracklepb.DeleteSemaphoreResponse{}, nil
}

func (s *GrackleApiServerHandler) CreateBarrier(ctx context.Context, req *gracklepb.CreateBarrierRequest, accountId uint64, limits grackle.ServiceLimits) (*gracklepb.CreateBarrierResponse, error) {
	// Resolve namespace by name to get its ID
	namespace, err := s.getNamespace(accountId, req.NamespaceName)
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	// Create barrier with generated ID. On the rare ID collision the core returns
	// IDCollision; regenerate the ID and retry.
	for range maxIDGenerationAttempts {
		resp1, err := s.grackleClient.CreateBarrier(ctx, &corepb.CreateBarrierRequest{
			BarrierId: &corepb.BarrierId{
				AccountId:   accountId,
				NamespaceId: namespace.Id.NamespaceId,
				BarrierId:   rand.Uint64(),
			},
			Name:                            req.BarrierName,
			Description:                     req.Description,
			ExpectedProcesses:               req.ExpectedProcesses,
			Metadata:                        req.Metadata,
			MaxNumberOfBarriersPerNamespace: limits.MaxNumberOfBarriersPerNamespace,
			DeleteInactiveAfterSeconds:      req.DeleteInactiveAfterSeconds,
		})
		if err != nil {
			if isIDCollision(err) {
				continue
			}
			return nil, mrpc.ErrorToGRPC(err)
		}

		return &gracklepb.CreateBarrierResponse{
			Barrier: barrierToFront(resp1.Barrier),
		}, nil
	}

	return nil, status.Error(codes.Internal, "failed to generate a unique barrier id")
}

func (s *GrackleApiServerHandler) ListBarriers(ctx context.Context, req *gracklepb.ListBarriersRequest, accountId uint64, limits grackle.ServiceLimits) (*gracklepb.ListBarriersResponse, error) {
	// Resolve namespace by name to get its ID
	namespace, err := s.getNamespace(accountId, req.NamespaceName)
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	// Decode pagination token from base64-encoded format
	paginationToken, err := paginationTokenToCore(req.PaginationToken)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	// List barriers in namespace with pagination
	resp1, err := s.grackleClient.ListBarriers(ctx, &corepb.ListBarriersRequest{
		NamespaceId:     namespace.Id,
		PaginationToken: paginationToken,
		Limit:           req.Limit,
	})
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	// Encode pagination tokens for response
	nextPaginationToken, err := paginationTokenToFront(resp1.NextPaginationToken)
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}
	previousPaginationToken, err := paginationTokenToFront(resp1.PreviousPaginationToken)
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	return &gracklepb.ListBarriersResponse{
		Barriers:                barriersToFront(resp1.Barriers),
		NextPaginationToken:     nextPaginationToken,
		PreviousPaginationToken: previousPaginationToken,
	}, nil
}

func (s *GrackleApiServerHandler) GetBarrier(ctx context.Context, req *gracklepb.GetBarrierRequest, accountId uint64, limits grackle.ServiceLimits) (*gracklepb.GetBarrierResponse, error) {
	// Resolve namespace by name to get its ID
	namespace, err := s.getNamespace(accountId, req.NamespaceName)
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	// Retrieve barrier by name
	resp1, err := s.grackleClient.GetBarrierByName(ctx, &corepb.GetBarrierByNameRequest{
		NamespaceId: namespace.Id,
		BarrierName: req.BarrierName,
	})
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	return &gracklepb.GetBarrierResponse{
		Barrier: barrierToFront(resp1.Barrier),
	}, nil
}

func (s *GrackleApiServerHandler) DeleteBarrier(ctx context.Context, req *gracklepb.DeleteBarrierRequest, accountId uint64, limits grackle.ServiceLimits) (*gracklepb.DeleteBarrierResponse, error) {
	// Resolve namespace by name to get its ID
	namespace, err := s.getNamespace(accountId, req.NamespaceName)
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	// Delete the barrier
	_, err = s.grackleClient.DeleteBarrier(ctx, &corepb.DeleteBarrierRequest{
		NamespaceId: namespace.Id,
		BarrierName: req.BarrierName,
	})
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	return &gracklepb.DeleteBarrierResponse{}, nil
}

func (s *GrackleApiServerHandler) UpdateBarrier(ctx context.Context, req *gracklepb.UpdateBarrierRequest, accountId uint64, limits grackle.ServiceLimits) (*gracklepb.UpdateBarrierResponse, error) {
	// Resolve namespace by name to get its ID
	namespace, err := s.getNamespace(accountId, req.NamespaceName)
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	// Retrieve barrier to get its ID
	resp1, err := s.grackleClient.GetBarrierByName(ctx, &corepb.GetBarrierByNameRequest{
		NamespaceId: namespace.Id,
		BarrierName: req.BarrierName,
	})
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	// Update barrier
	resp2, err := s.grackleClient.UpdateBarrier(ctx, &corepb.UpdateBarrierRequest{
		BarrierId:                  resp1.Barrier.Id,
		Description:                req.Description,
		ExpectedProcesses:          req.ExpectedProcesses,
		Metadata:                   req.Metadata,
		ExpectedVersion:            req.ExpectedVersion,
		DeleteInactiveAfterSeconds: req.DeleteInactiveAfterSeconds,
	})
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	return &gracklepb.UpdateBarrierResponse{
		Barrier: barrierToFront(resp2.Barrier),
	}, nil
}

func (s *GrackleApiServerHandler) ArriveAtBarrier(ctx context.Context, req *gracklepb.ArriveAtBarrierRequest, accountId uint64, limits grackle.ServiceLimits) (*gracklepb.ArriveAtBarrierResponse, error) {
	// Resolve namespace by name to get its ID
	namespace, err := s.getNamespace(accountId, req.NamespaceName)
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	// Mark process as arrived at barrier
	resp1, err := s.grackleClient.ArriveAtBarrier(ctx, &corepb.ArriveAtBarrierRequest{
		NamespaceId: namespace.Id,
		BarrierName: req.BarrierName,
		ProcessId:   req.ProcessId,
		Generation:  req.ExpectedGeneration,
		Metadata:    req.Metadata,
	})
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	return &gracklepb.ArriveAtBarrierResponse{
		Barrier:    barrierToFront(resp1.Barrier),
		AllArrived: resp1.AllArrived,
	}, nil
}

func (s *GrackleApiServerHandler) WaitAtBarrier(ctx context.Context, req *gracklepb.WaitAtBarrierRequest, accountId uint64, limits grackle.ServiceLimits) (*gracklepb.WaitAtBarrierResponse, error) {
	// Resolve namespace by name once to avoid repeated lookups
	namespace, err := s.getNamespace(accountId, req.NamespaceName)
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	// Calculate absolute deadline for timeout
	deadline := time.Now().Add(time.Duration(req.TimeoutSeconds) * time.Second)

	// Initialize polling with exponential backoff
	pollInterval := 100 * time.Millisecond
	maxPollInterval := 1 * time.Second

	for {
		// Check if context is cancelled
		if ctx.Err() != nil {
			return nil, status.Errorf(codes.Canceled, "req cancelled")
		}

		// Poll barrier state
		resp1, err := s.grackleClient.GetBarrierByName(ctx, &corepb.GetBarrierByNameRequest{
			NamespaceId: namespace.Id,
			BarrierName: req.BarrierName,
		})
		if err != nil {
			return nil, mrpc.ErrorToGRPC(err)
		}

		// The barrier auto-trips inside ArriveAtBarrier by advancing Generation. From the
		// waiter's perspective, the trip has happened iff the barrier's current Generation
		// is strictly greater than the one we were registered to wait at.
		tripped := resp1.Barrier.Generation > req.ExpectedGeneration

		if tripped {
			// The barrier tripped. The caller's next round is deterministically
			// ExpectedGeneration+1 (generations advance by exactly one per trip), so
			// no next-generation value is returned. barrier.Generation reflects where
			// the barrier actually is now — it may already be further ahead if a later
			// cohort tripped it again while this waiter was between polls.
			return &gracklepb.WaitAtBarrierResponse{
				Barrier: barrierToFront(resp1.Barrier),
				Outcome: gracklepb.BarrierWaitOutcome_BARRIER_WAIT_OUTCOME_TRIPPED,
			}, nil
		}

		if time.Now().After(deadline) {
			// Deadline passed without the barrier tripping.
			return &gracklepb.WaitAtBarrierResponse{
				Barrier: barrierToFront(resp1.Barrier),
				Outcome: gracklepb.BarrierWaitOutcome_BARRIER_WAIT_OUTCOME_TIMED_OUT,
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
			pollInterval = min(pollInterval*2, maxPollInterval)
		case <-ctx.Done():
			return nil, status.Errorf(codes.Canceled, "req cancelled")
		}
	}
}

func (s *GrackleApiServerHandler) ListBarrierParticipants(ctx context.Context, req *gracklepb.ListBarrierParticipantsRequest, accountId uint64, limits grackle.ServiceLimits) (*gracklepb.ListBarrierParticipantsResponse, error) {
	// Resolve namespace by name to get its ID
	namespace, err := s.getNamespace(accountId, req.NamespaceName)
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	// Decode pagination token from base64-encoded format
	paginationToken, err := paginationTokenToCore(req.PaginationToken)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	// List participants for the specific barrier generation
	resp1, err := s.grackleClient.ListBarrierParticipants(ctx, &corepb.ListBarrierParticipantsRequest{
		NamespaceId:     namespace.Id,
		BarrierName:     req.BarrierName,
		Generation:      req.Generation,
		PaginationToken: paginationToken,
		Limit:           req.Limit,
	})
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	// Encode pagination tokens for response
	nextPaginationToken, err := paginationTokenToFront(resp1.NextPaginationToken)
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}
	previousPaginationToken, err := paginationTokenToFront(resp1.PreviousPaginationToken)
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	return &gracklepb.ListBarrierParticipantsResponse{
		Participants:            barrierParticipantsToFront(resp1.Participants),
		NextPaginationToken:     nextPaginationToken,
		PreviousPaginationToken: previousPaginationToken,
	}, nil
}

func (s *GrackleApiServerHandler) CreateSemaphoreLease(ctx context.Context, req *gracklepb.CreateSemaphoreLeaseRequest, accountId uint64, limits grackle.ServiceLimits) (*gracklepb.CreateSemaphoreLeaseResponse, error) {
	// Resolve namespace by name to get its ID
	namespace, err := s.getNamespace(accountId, req.NamespaceName)
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	// Create semaphore lease with generated ID. On the rare ID collision the core
	// returns IDCollision; regenerate the ID and retry.
	for range maxIDGenerationAttempts {
		resp1, err := s.grackleClient.CreateSemaphoreLease(ctx, &corepb.CreateSemaphoreLeaseRequest{
			LeaseId: &corepb.LeaseId{
				AccountId:   accountId,
				NamespaceId: namespace.Id.NamespaceId,
				LeaseId:     rand.Uint64(),
			},
			ProcessId:                  req.ProcessId,
			TtlSeconds:                 req.TtlSeconds,
			Metadata:                   req.Metadata,
			MaxNumberOfSemaphoreLeases: limits.MaxNumberOfSemaphoreLeases,
		})
		if err != nil {
			if isIDCollision(err) {
				continue
			}
			return nil, mrpc.ErrorToGRPC(err)
		}

		return &gracklepb.CreateSemaphoreLeaseResponse{
			Lease: leaseToFront(resp1.Lease),
		}, nil
	}

	return nil, status.Error(codes.Internal, "failed to generate a unique lease id")
}

func (s *GrackleApiServerHandler) RevokeSemaphoreLease(ctx context.Context, req *gracklepb.RevokeSemaphoreLeaseRequest, accountId uint64, limits grackle.ServiceLimits) (*gracklepb.RevokeSemaphoreLeaseResponse, error) {
	// Resolve namespace by name to get its ID
	namespace, err := s.getNamespace(accountId, req.NamespaceName)
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	// Decode and validate lease ID
	leaseId, err := ids.DecodeLeaseId(req.LeaseId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid RevokeSemaphoreLeaseRequest.LeaseId: %v", err)
	}

	// Validate lease ID belongs to the account and namespace
	if leaseId.AccountId != accountId || leaseId.NamespaceId != namespace.Id.NamespaceId {
		return nil, status.Errorf(codes.NotFound, "lease not found")
	}

	// Revoke the semaphore lease
	_, err = s.grackleClient.RevokeSemaphoreLease(ctx, &corepb.RevokeSemaphoreLeaseRequest{
		LeaseId: leaseId,
	})
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	return &gracklepb.RevokeSemaphoreLeaseResponse{}, nil
}

func (s *GrackleApiServerHandler) RefreshSemaphoreLease(ctx context.Context, req *gracklepb.RefreshSemaphoreLeaseRequest, accountId uint64, limits grackle.ServiceLimits) (*gracklepb.RefreshSemaphoreLeaseResponse, error) {
	// Resolve namespace by name to get its ID
	namespace, err := s.getNamespace(accountId, req.NamespaceName)
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	// Decode and validate lease ID
	leaseId, err := ids.DecodeLeaseId(req.LeaseId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid RefreshSemaphoreLeaseRequest.LeaseId: %v", err)
	}

	// Validate lease ID belongs to the account and namespace
	if leaseId.AccountId != accountId || leaseId.NamespaceId != namespace.Id.NamespaceId {
		return nil, status.Errorf(codes.NotFound, "lease not found")
	}

	// Refresh the semaphore lease TTL
	resp1, err := s.grackleClient.RefreshSemaphoreLease(ctx, &corepb.RefreshSemaphoreLeaseRequest{
		LeaseId:    leaseId,
		TtlSeconds: req.TtlSeconds,
	})
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	return &gracklepb.RefreshSemaphoreLeaseResponse{
		Lease: leaseToFront(resp1.Lease),
	}, nil
}

func (s *GrackleApiServerHandler) ListSemaphoreLeases(ctx context.Context, req *gracklepb.ListSemaphoreLeasesRequest, accountId uint64, limits grackle.ServiceLimits) (*gracklepb.ListSemaphoreLeasesResponse, error) {
	// Resolve namespace by name to get its ID
	namespace, err := s.getNamespace(accountId, req.NamespaceName)
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	// Decode pagination token from base64-encoded format
	paginationToken, err := paginationTokenToCore(req.PaginationToken)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	// List semaphore leases with pagination
	resp1, err := s.grackleClient.ListSemaphoreLeases(ctx, &corepb.ListSemaphoreLeasesRequest{
		NamespaceId:     namespace.Id,
		PaginationToken: paginationToken,
		Limit:           req.Limit,
	})
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	// Encode pagination tokens for response
	nextPaginationToken, err := paginationTokenToFront(resp1.NextPaginationToken)
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}
	previousPaginationToken, err := paginationTokenToFront(resp1.PreviousPaginationToken)
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	return &gracklepb.ListSemaphoreLeasesResponse{
		Leases:                  leasesToFront(resp1.Leases),
		NextPaginationToken:     nextPaginationToken,
		PreviousPaginationToken: previousPaginationToken,
	}, nil
}

func (s *GrackleApiServerHandler) GetSemaphoreLease(ctx context.Context, req *gracklepb.GetSemaphoreLeaseRequest, accountId uint64, limits grackle.ServiceLimits) (*gracklepb.GetSemaphoreLeaseResponse, error) {
	// Resolve namespace by name to get its ID
	namespace, err := s.getNamespace(accountId, req.NamespaceName)
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	// Decode and validate lease ID
	leaseId, err := ids.DecodeLeaseId(req.LeaseId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid GetSemaphoreLeaseRequest.LeaseId: %v", err)
	}

	// Validate lease ID belongs to the account and namespace
	if leaseId.AccountId != accountId || leaseId.NamespaceId != namespace.Id.NamespaceId {
		return nil, status.Errorf(codes.NotFound, "lease not found")
	}

	// Retrieve semaphore lease by ID
	resp1, err := s.grackleClient.GetSemaphoreLease(ctx, &corepb.GetSemaphoreLeaseRequest{
		LeaseId: leaseId,
	})
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	return &gracklepb.GetSemaphoreLeaseResponse{
		Lease: leaseToFront(resp1.Lease),
	}, nil
}

func (s *GrackleApiServerHandler) CreateLockLease(ctx context.Context, req *gracklepb.CreateLockLeaseRequest, accountId uint64, limits grackle.ServiceLimits) (*gracklepb.CreateLockLeaseResponse, error) {
	// Resolve namespace by name to get its ID
	namespace, err := s.getNamespace(accountId, req.NamespaceName)
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	// Create lock lease with generated ID. On the rare ID collision the core
	// returns IDCollision; regenerate the ID and retry.
	for range maxIDGenerationAttempts {
		resp1, err := s.grackleClient.CreateLockLease(ctx, &corepb.CreateLockLeaseRequest{
			LeaseId: &corepb.LeaseId{
				AccountId:   accountId,
				NamespaceId: namespace.Id.NamespaceId,
				LeaseId:     rand.Uint64(),
			},
			ProcessId:             req.ProcessId,
			TtlSeconds:            req.TtlSeconds,
			Metadata:              req.Metadata,
			MaxNumberOfLockLeases: limits.MaxNumberOfLockLeases,
		})
		if err != nil {
			if isIDCollision(err) {
				continue
			}
			return nil, mrpc.ErrorToGRPC(err)
		}

		return &gracklepb.CreateLockLeaseResponse{
			Lease: leaseToFront(resp1.Lease),
		}, nil
	}

	return nil, status.Error(codes.Internal, "failed to generate a unique lease id")
}

func (s *GrackleApiServerHandler) RevokeLockLease(ctx context.Context, req *gracklepb.RevokeLockLeaseRequest, accountId uint64, limits grackle.ServiceLimits) (*gracklepb.RevokeLockLeaseResponse, error) {
	// Resolve namespace by name to get its ID
	namespace, err := s.getNamespace(accountId, req.NamespaceName)
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	// Decode and validate lease ID
	leaseId, err := ids.DecodeLeaseId(req.LeaseId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid RevokeLockLeaseRequest.LeaseId: %v", err)
	}

	// Validate lease ID belongs to the account and namespace
	if leaseId.AccountId != accountId || leaseId.NamespaceId != namespace.Id.NamespaceId {
		return nil, status.Errorf(codes.NotFound, "lease not found")
	}

	// Revoke the lock lease
	_, err = s.grackleClient.RevokeLockLease(ctx, &corepb.RevokeLockLeaseRequest{
		LeaseId: leaseId,
	})
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	return &gracklepb.RevokeLockLeaseResponse{}, nil
}

func (s *GrackleApiServerHandler) RefreshLockLease(ctx context.Context, req *gracklepb.RefreshLockLeaseRequest, accountId uint64, limits grackle.ServiceLimits) (*gracklepb.RefreshLockLeaseResponse, error) {
	// Resolve namespace by name to get its ID
	namespace, err := s.getNamespace(accountId, req.NamespaceName)
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	// Decode and validate lease ID
	leaseId, err := ids.DecodeLeaseId(req.LeaseId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid RefreshLockLeaseRequest.LeaseId: %v", err)
	}

	// Validate lease ID belongs to the account and namespace
	if leaseId.AccountId != accountId || leaseId.NamespaceId != namespace.Id.NamespaceId {
		return nil, status.Errorf(codes.NotFound, "lease not found")
	}

	// Refresh the lock lease TTL
	resp1, err := s.grackleClient.RefreshLockLease(ctx, &corepb.RefreshLockLeaseRequest{
		LeaseId:    leaseId,
		TtlSeconds: req.TtlSeconds,
	})
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	return &gracklepb.RefreshLockLeaseResponse{
		Lease: leaseToFront(resp1.Lease),
	}, nil
}

func (s *GrackleApiServerHandler) ListLockLeases(ctx context.Context, req *gracklepb.ListLockLeasesRequest, accountId uint64, limits grackle.ServiceLimits) (*gracklepb.ListLockLeasesResponse, error) {
	// Resolve namespace by name to get its ID
	namespace, err := s.getNamespace(accountId, req.NamespaceName)
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	// Decode pagination token from base64-encoded format
	paginationToken, err := paginationTokenToCore(req.PaginationToken)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	// List lock leases with pagination
	resp1, err := s.grackleClient.ListLockLeases(ctx, &corepb.ListLockLeasesRequest{
		NamespaceId:     namespace.Id,
		PaginationToken: paginationToken,
		Limit:           req.Limit,
	})
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	// Encode pagination tokens for response
	nextPaginationToken, err := paginationTokenToFront(resp1.NextPaginationToken)
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}
	previousPaginationToken, err := paginationTokenToFront(resp1.PreviousPaginationToken)
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	return &gracklepb.ListLockLeasesResponse{
		Leases:                  leasesToFront(resp1.Leases),
		NextPaginationToken:     nextPaginationToken,
		PreviousPaginationToken: previousPaginationToken,
	}, nil
}

func (s *GrackleApiServerHandler) GetLockLease(ctx context.Context, req *gracklepb.GetLockLeaseRequest, accountId uint64, limits grackle.ServiceLimits) (*gracklepb.GetLockLeaseResponse, error) {
	// Resolve namespace by name to get its ID
	namespace, err := s.getNamespace(accountId, req.NamespaceName)
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	// Decode and validate lease ID
	leaseId, err := ids.DecodeLeaseId(req.LeaseId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid GetLockLeaseRequest.LeaseId: %v", err)
	}

	// Validate lease ID belongs to the account and namespace
	if leaseId.AccountId != accountId || leaseId.NamespaceId != namespace.Id.NamespaceId {
		return nil, status.Errorf(codes.NotFound, "lease not found")
	}

	// Retrieve lock lease by ID
	resp1, err := s.grackleClient.GetLockLease(ctx, &corepb.GetLockLeaseRequest{
		LeaseId: leaseId,
	})
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	return &gracklepb.GetLockLeaseResponse{
		Lease: leaseToFront(resp1.Lease),
	}, nil
}

// getNamespace resolves a namespace by (account, name), serving it from the
// namespaces cache when possible and otherwise fetching it from the core via
// GetNamespaceByName. Concurrent lookups for the same key are deduplicated by
// the cache, so a cache miss triggers at most one core RPC. NotFound errors
// are negatively cached (see NewGrackleApiServerHandler), so repeated lookups
// of a missing namespace do not repeatedly hit the core.
func (s *GrackleApiServerHandler) getNamespace(accountId uint64, namespaceName string) (*corepb.Namespace, error) {
	key := fmt.Sprintf("%d/%s", accountId, namespaceName)

	return s.namespacesCache.GetOrLoad(key, func() (*corepb.Namespace, error) {
		resp, err := s.grackleClient.GetNamespaceByName(context.TODO(), &corepb.GetNamespaceByNameRequest{
			AccountId:     accountId,
			NamespaceName: namespaceName,
		})
		if err != nil {
			return nil, err
		}
		return resp.Namespace, nil
	})
}

// NewGrackleApiServerHandler builds a handler backed by the given core client.
func NewGrackleApiServerHandler(grackleClient coreapis.GrackleClientApi) *GrackleApiServerHandler {
	return &GrackleApiServerHandler{
		grackleClient: grackleClient,

		// The namespaces cache holds positive entries to keep hot namespaces out
		// of the core's path while staying fresh enough to pick up changes, and
		// negatively caches NotFound errors so lookups of a missing namespace
		// don't repeatedly hit the core. Expired entries are swept every 5m.
		namespacesCache: cache.New[string, *corepb.Namespace](
			cache.WithTTL(namespacesCacheTTL),
			cache.WithNegativeTTL(namespacesCacheNegativeTTL),
			cache.WithCleaningInterval(5*time.Minute),
			cache.WithCacheableError(isNotFound),
		),
	}
}

// acquireFailureOutcome maps a failed acquisition to a terminal outcome: a
// non-blocking attempt (timeout_seconds == 0) reports UNAVAILABLE, while a
// blocking attempt that ran out the clock reports TIMED_OUT.
func acquireFailureOutcome(timeoutSeconds int32) gracklepb.AcquireOutcome {
	if timeoutSeconds <= 0 {
		return gracklepb.AcquireOutcome_ACQUIRE_OUTCOME_UNAVAILABLE
	}
	return gracklepb.AcquireOutcome_ACQUIRE_OUTCOME_TIMED_OUT
}

func isIDCollision(err error) bool {
	var appErr *mrpc.Error
	return errors.As(err, &appErr) && appErr.Code == mrpc.IDCollision
}

func isNotFound(err error) bool {
	var appErr *mrpc.Error
	return errors.As(err, &appErr) && appErr.Code == mrpc.NotFound
}
