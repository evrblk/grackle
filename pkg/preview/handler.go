package preview

import (
	"context"
	"math/rand/v2"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	gracklepb "github.com/evrblk/evrblk-go/grackle/preview"
	"github.com/evrblk/grackle/pkg/corepb"
	"github.com/evrblk/grackle/pkg/grackle"
	monsterax "github.com/evrblk/monstera/x"
)

type GrackleApiServerHandler struct {
	grackleCoreApiClient grackle.GrackleCoreApi
}

func (s *GrackleApiServerHandler) Stop() {

}

func (s *GrackleApiServerHandler) CreateNamespace(ctx context.Context, request *gracklepb.CreateNamespaceRequest, accountId uint64, limits grackle.GrackleServiceLimits) (*gracklepb.CreateNamespaceResponse, error) {
	now := time.Now()

	res, err := s.grackleCoreApiClient.CreateNamespace(ctx, &corepb.CreateNamespaceRequest{
		AccountId:             accountId,
		Name:                  request.Name,
		Description:           request.Description,
		Now:                   now.UnixNano(),
		MaxNumberOfNamespaces: limits.MaxNumberOfNamespaces,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	return &gracklepb.CreateNamespaceResponse{
		Namespace: namespaceToFront(res.Namespace),
	}, nil
}

func (s *GrackleApiServerHandler) GetNamespace(ctx context.Context, request *gracklepb.GetNamespaceRequest, accountId uint64, limits grackle.GrackleServiceLimits) (*gracklepb.GetNamespaceResponse, error) {
	namespace, err := s.grackleCoreApiClient.GetNamespace(ctx, &corepb.GetNamespaceRequest{
		NamespaceId: &corepb.NamespaceId{
			AccountId:     accountId,
			NamespaceName: request.NamespaceName,
		},
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	return &gracklepb.GetNamespaceResponse{
		Namespace: namespaceToFront(namespace.Namespace),
	}, nil
}

func (s *GrackleApiServerHandler) UpdateNamespace(ctx context.Context, request *gracklepb.UpdateNamespaceRequest, accountId uint64, limits grackle.GrackleServiceLimits) (*gracklepb.UpdateNamespaceResponse, error) {
	now := time.Now()

	res, err := s.grackleCoreApiClient.UpdateNamespace(ctx, &corepb.UpdateNamespaceRequest{
		NamespaceId: &corepb.NamespaceId{
			AccountId:     accountId,
			NamespaceName: request.NamespaceName,
		},
		Description: request.Description,
		Now:         now.UnixNano(),
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	return &gracklepb.UpdateNamespaceResponse{
		Namespace: namespaceToFront(res.Namespace),
	}, nil
}

func (s *GrackleApiServerHandler) DeleteNamespace(ctx context.Context, request *gracklepb.DeleteNamespaceRequest, accountId uint64, limits grackle.GrackleServiceLimits) (*gracklepb.DeleteNamespaceResponse, error) {
	now := time.Now()

	namespaceId := &corepb.NamespaceId{
		AccountId:     accountId,
		NamespaceName: request.NamespaceName,
	}
	gcRecordId := rand.Uint64()

	res1, err := s.grackleCoreApiClient.GetNamespace(ctx, &corepb.GetNamespaceRequest{
		NamespaceId: &corepb.NamespaceId{
			AccountId:     accountId,
			NamespaceName: request.NamespaceName,
		},
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	namespaceTimestampedId := &corepb.NamespaceTimestampedId{
		AccountId:          accountId,
		NamespaceName:      request.NamespaceName,
		NamespaceCreatedAt: res1.Namespace.CreatedAt,
	}

	_, err = s.grackleCoreApiClient.LocksDeleteNamespace(ctx, &corepb.LocksDeleteNamespaceRequest{
		RecordId:               gcRecordId,
		Now:                    now.UnixNano(),
		NamespaceTimestampedId: namespaceTimestampedId,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	_, err = s.grackleCoreApiClient.WaitGroupsDeleteNamespace(ctx, &corepb.WaitGroupsDeleteNamespaceRequest{
		RecordId:               gcRecordId,
		Now:                    now.UnixNano(),
		NamespaceTimestampedId: namespaceTimestampedId,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	_, err = s.grackleCoreApiClient.SemaphoresDeleteNamespace(ctx, &corepb.SemaphoresDeleteNamespaceRequest{
		RecordId:               gcRecordId,
		Now:                    now.UnixNano(),
		NamespaceTimestampedId: namespaceTimestampedId,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	_, err = s.grackleCoreApiClient.DeleteNamespace(ctx, &corepb.DeleteNamespaceRequest{
		Now:         now.UnixNano(),
		NamespaceId: namespaceId,
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

	res, err := s.grackleCoreApiClient.ListNamespaces(ctx, &corepb.ListNamespacesRequest{
		AccountId:       accountId,
		PaginationToken: paginationToken,
		Limit:           request.Limit,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	nextPaginationToken, err := paginationTokenToFront(res.NextPaginationToken)
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}
	previousPaginationToken, err := paginationTokenToFront(res.PreviousPaginationToken)
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	return &gracklepb.ListNamespacesResponse{
		Namespaces:              namespacesToFront(res.Namespaces),
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

	res1, err := s.grackleCoreApiClient.GetNamespace(ctx, &corepb.GetNamespaceRequest{
		NamespaceId: &corepb.NamespaceId{
			AccountId:     accountId,
			NamespaceName: request.NamespaceName,
		},
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	res2, err := s.grackleCoreApiClient.CreateWaitGroup(ctx, &corepb.CreateWaitGroupRequest{
		NamespaceTimestampedId: &corepb.NamespaceTimestampedId{
			AccountId:          accountId,
			NamespaceName:      request.NamespaceName,
			NamespaceCreatedAt: res1.Namespace.CreatedAt,
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
		WaitGroup: waitGroupToFront(res2.WaitGroup),
	}, nil
}

func (s *GrackleApiServerHandler) GetWaitGroup(ctx context.Context, request *gracklepb.GetWaitGroupRequest, accountId uint64, limits grackle.GrackleServiceLimits) (*gracklepb.GetWaitGroupResponse, error) {
	res1, err := s.grackleCoreApiClient.GetNamespace(ctx, &corepb.GetNamespaceRequest{
		NamespaceId: &corepb.NamespaceId{
			AccountId:     accountId,
			NamespaceName: request.NamespaceName,
		},
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	res2, err := s.grackleCoreApiClient.GetWaitGroup(ctx, &corepb.GetWaitGroupRequest{
		WaitGroupId: &corepb.WaitGroupId{
			AccountId:          accountId,
			NamespaceName:      request.NamespaceName,
			NamespaceCreatedAt: res1.Namespace.CreatedAt,
			WaitGroupName:      request.WaitGroupName,
		},
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	return &gracklepb.GetWaitGroupResponse{
		WaitGroup: waitGroupToFront(res2.WaitGroup),
	}, nil
}

func (s *GrackleApiServerHandler) AddJobsToWaitGroup(ctx context.Context, request *gracklepb.AddJobsToWaitGroupRequest, accountId uint64, limits grackle.GrackleServiceLimits) (*gracklepb.AddJobsToWaitGroupResponse, error) {
	now := time.Now()

	res1, err := s.grackleCoreApiClient.GetNamespace(ctx, &corepb.GetNamespaceRequest{
		NamespaceId: &corepb.NamespaceId{
			AccountId:     accountId,
			NamespaceName: request.NamespaceName,
		},
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	res2, err := s.grackleCoreApiClient.AddJobsToWaitGroup(ctx, &corepb.AddJobsToWaitGroupRequest{
		WaitGroupId: &corepb.WaitGroupId{
			AccountId:          accountId,
			NamespaceName:      request.NamespaceName,
			NamespaceCreatedAt: res1.Namespace.CreatedAt,
			WaitGroupName:      request.WaitGroupName,
		},
		Counter:          request.Counter,
		Now:              now.UnixNano(),
		MaxWaitGroupSize: limits.MaxWaitGroupSize,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	return &gracklepb.AddJobsToWaitGroupResponse{
		WaitGroup: waitGroupToFront(res2.WaitGroup),
	}, nil
}

func (s *GrackleApiServerHandler) CompleteJobsFromWaitGroup(ctx context.Context, request *gracklepb.CompleteJobsFromWaitGroupRequest, accountId uint64, limits grackle.GrackleServiceLimits) (*gracklepb.CompleteJobsFromWaitGroupResponse, error) {
	now := time.Now()

	res1, err := s.grackleCoreApiClient.GetNamespace(ctx, &corepb.GetNamespaceRequest{
		NamespaceId: &corepb.NamespaceId{
			AccountId:     accountId,
			NamespaceName: request.NamespaceName,
		},
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	res2, err := s.grackleCoreApiClient.CompleteJobsFromWaitGroup(ctx, &corepb.CompleteJobsFromWaitGroupRequest{
		WaitGroupId: &corepb.WaitGroupId{
			AccountId:          accountId,
			NamespaceName:      request.NamespaceName,
			NamespaceCreatedAt: res1.Namespace.CreatedAt,
			WaitGroupName:      request.WaitGroupName,
		},
		ProcessIds: request.ProcessIds,
		Now:        now.UnixNano(),
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	return &gracklepb.CompleteJobsFromWaitGroupResponse{
		WaitGroup: waitGroupToFront(res2.WaitGroup),
	}, nil
}

func (s *GrackleApiServerHandler) DeleteWaitGroup(ctx context.Context, request *gracklepb.DeleteWaitGroupRequest, accountId uint64, limits grackle.GrackleServiceLimits) (*gracklepb.DeleteWaitGroupResponse, error) {
	res1, err := s.grackleCoreApiClient.GetNamespace(ctx, &corepb.GetNamespaceRequest{
		NamespaceId: &corepb.NamespaceId{
			AccountId:     accountId,
			NamespaceName: request.NamespaceName,
		},
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	_, err = s.grackleCoreApiClient.DeleteWaitGroup(ctx, &corepb.DeleteWaitGroupRequest{
		WaitGroupId: &corepb.WaitGroupId{
			AccountId:          accountId,
			NamespaceName:      request.NamespaceName,
			NamespaceCreatedAt: res1.Namespace.CreatedAt,
			WaitGroupName:      request.WaitGroupName,
		},
		RecordId: rand.Uint64(),
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	return &gracklepb.DeleteWaitGroupResponse{}, nil
}

func (s *GrackleApiServerHandler) ListWaitGroups(ctx context.Context, request *gracklepb.ListWaitGroupsRequest, accountId uint64, limits grackle.GrackleServiceLimits) (*gracklepb.ListWaitGroupsResponse, error) {
	res1, err := s.grackleCoreApiClient.GetNamespace(ctx, &corepb.GetNamespaceRequest{
		NamespaceId: &corepb.NamespaceId{
			AccountId:     accountId,
			NamespaceName: request.NamespaceName,
		},
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	paginationToken, err := paginationTokenFromFront(request.PaginationToken)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	res, err := s.grackleCoreApiClient.ListWaitGroups(ctx, &corepb.ListWaitGroupsRequest{
		NamespaceTimestampedId: &corepb.NamespaceTimestampedId{
			AccountId:          accountId,
			NamespaceName:      request.NamespaceName,
			NamespaceCreatedAt: res1.Namespace.CreatedAt,
		},
		PaginationToken: paginationToken,
		Limit:           request.Limit,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	nextPaginationToken, err := paginationTokenToFront(res.NextPaginationToken)
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}
	previousPaginationToken, err := paginationTokenToFront(res.PreviousPaginationToken)
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	return &gracklepb.ListWaitGroupsResponse{
		WaitGroups:              waitGroupsToFront(res.WaitGroups),
		NextPaginationToken:     nextPaginationToken,
		PreviousPaginationToken: previousPaginationToken,
	}, nil
}

func (s *GrackleApiServerHandler) AcquireLock(ctx context.Context, request *gracklepb.AcquireLockRequest, accountId uint64, limits grackle.GrackleServiceLimits) (*gracklepb.AcquireLockResponse, error) {
	now := time.Now()

	res1, err := s.grackleCoreApiClient.GetNamespace(ctx, &corepb.GetNamespaceRequest{
		NamespaceId: &corepb.NamespaceId{
			AccountId:     accountId,
			NamespaceName: request.NamespaceName,
		},
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	res2, err := s.grackleCoreApiClient.AcquireLock(ctx, &corepb.AcquireLockRequest{
		LockId: &corepb.LockId{
			AccountId:          accountId,
			NamespaceName:      request.NamespaceName,
			NamespaceCreatedAt: res1.Namespace.CreatedAt,
			LockName:           request.LockName,
		},
		Now:                          now.UnixNano(),
		ProcessId:                    request.ProcessId,
		ExpiresAt:                    request.ExpiresAt,
		WriteLock:                    request.WriteLock,
		MaxNumberOfLocksPerNamespace: limits.MaxNumberOfLocksPerNamespace,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	return &gracklepb.AcquireLockResponse{
		Lock:    lockToFront(res2.Lock),
		Success: res2.Success,
	}, nil
}

func (s *GrackleApiServerHandler) ReleaseLock(ctx context.Context, request *gracklepb.ReleaseLockRequest, accountId uint64, limits grackle.GrackleServiceLimits) (*gracklepb.ReleaseLockResponse, error) {
	now := time.Now()

	res1, err := s.grackleCoreApiClient.GetNamespace(ctx, &corepb.GetNamespaceRequest{
		NamespaceId: &corepb.NamespaceId{
			AccountId:     accountId,
			NamespaceName: request.NamespaceName,
		},
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	res2, err := s.grackleCoreApiClient.ReleaseLock(ctx, &corepb.ReleaseLockRequest{
		LockId: &corepb.LockId{
			AccountId:          accountId,
			NamespaceName:      request.NamespaceName,
			NamespaceCreatedAt: res1.Namespace.CreatedAt,
			LockName:           request.LockName,
		},
		Now:       now.UnixNano(),
		ProcessId: request.ProcessId,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	return &gracklepb.ReleaseLockResponse{
		Lock: lockToFront(res2.Lock),
	}, nil
}

func (s *GrackleApiServerHandler) GetLock(ctx context.Context, request *gracklepb.GetLockRequest, accountId uint64, limits grackle.GrackleServiceLimits) (*gracklepb.GetLockResponse, error) {
	now := time.Now()

	res1, err := s.grackleCoreApiClient.GetNamespace(ctx, &corepb.GetNamespaceRequest{
		NamespaceId: &corepb.NamespaceId{
			AccountId:     accountId,
			NamespaceName: request.NamespaceName,
		},
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	res2, err := s.grackleCoreApiClient.GetLock(ctx, &corepb.GetLockRequest{
		LockId: &corepb.LockId{
			AccountId:          accountId,
			NamespaceName:      request.NamespaceName,
			NamespaceCreatedAt: res1.Namespace.CreatedAt,
			LockName:           request.LockName,
		},
		Now: now.UnixNano(),
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	return &gracklepb.GetLockResponse{
		Lock: lockToFront(res2.Lock),
	}, nil
}

func (s *GrackleApiServerHandler) DeleteLock(ctx context.Context, request *gracklepb.DeleteLockRequest, accountId uint64, limits grackle.GrackleServiceLimits) (*gracklepb.DeleteLockResponse, error) {
	now := time.Now()

	res1, err := s.grackleCoreApiClient.GetNamespace(ctx, &corepb.GetNamespaceRequest{
		NamespaceId: &corepb.NamespaceId{
			AccountId:     accountId,
			NamespaceName: request.NamespaceName,
		},
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	_, err = s.grackleCoreApiClient.DeleteLock(ctx, &corepb.DeleteLockRequest{
		LockId: &corepb.LockId{
			AccountId:          accountId,
			NamespaceName:      request.NamespaceName,
			NamespaceCreatedAt: res1.Namespace.CreatedAt,
			LockName:           request.LockName,
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

	res1, err := s.grackleCoreApiClient.GetNamespace(ctx, &corepb.GetNamespaceRequest{
		NamespaceId: &corepb.NamespaceId{
			AccountId:     accountId,
			NamespaceName: request.NamespaceName,
		},
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	paginationToken, err := paginationTokenFromFront(request.PaginationToken)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	res2, err := s.grackleCoreApiClient.ListLocks(ctx, &corepb.ListLocksRequest{
		NamespaceTimestampedId: &corepb.NamespaceTimestampedId{
			AccountId:          accountId,
			NamespaceName:      request.NamespaceName,
			NamespaceCreatedAt: res1.Namespace.CreatedAt,
		},
		Now:             now.UnixNano(),
		PaginationToken: paginationToken,
		Limit:           request.Limit,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	nextPaginationToken, err := paginationTokenToFront(res2.NextPaginationToken)
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}
	previousPaginationToken, err := paginationTokenToFront(res2.PreviousPaginationToken)
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	return &gracklepb.ListLocksResponse{
		Locks:                   locksToFront(res2.Locks),
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

	res1, err := s.grackleCoreApiClient.GetNamespace(ctx, &corepb.GetNamespaceRequest{
		NamespaceId: &corepb.NamespaceId{
			AccountId:     accountId,
			NamespaceName: request.NamespaceName,
		},
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	res2, err := s.grackleCoreApiClient.CreateSemaphore(ctx, &corepb.CreateSemaphoreRequest{
		NamespaceTimestampedId: &corepb.NamespaceTimestampedId{
			AccountId:          accountId,
			NamespaceName:      request.NamespaceName,
			NamespaceCreatedAt: res1.Namespace.CreatedAt,
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
		Semaphore: semaphoreToFront(res2.Semaphore),
	}, nil
}

func (s *GrackleApiServerHandler) ListSemaphores(ctx context.Context, request *gracklepb.ListSemaphoresRequest, accountId uint64, limits grackle.GrackleServiceLimits) (*gracklepb.ListSemaphoresResponse, error) {
	res1, err := s.grackleCoreApiClient.GetNamespace(ctx, &corepb.GetNamespaceRequest{
		NamespaceId: &corepb.NamespaceId{
			AccountId:     accountId,
			NamespaceName: request.NamespaceName,
		},
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	paginationToken, err := paginationTokenFromFront(request.PaginationToken)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	res2, err := s.grackleCoreApiClient.ListSemaphores(ctx, &corepb.ListSemaphoresRequest{
		NamespaceTimestampedId: &corepb.NamespaceTimestampedId{
			AccountId:          accountId,
			NamespaceName:      request.NamespaceName,
			NamespaceCreatedAt: res1.Namespace.CreatedAt,
		},
		PaginationToken: paginationToken,
		Limit:           request.Limit,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	nextPaginationToken, err := paginationTokenToFront(res2.NextPaginationToken)
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}
	previousPaginationToken, err := paginationTokenToFront(res2.PreviousPaginationToken)
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	return &gracklepb.ListSemaphoresResponse{
		Semaphores:              semaphoresToFront(res2.Semaphores),
		NextPaginationToken:     nextPaginationToken,
		PreviousPaginationToken: previousPaginationToken,
	}, nil
}

func (s *GrackleApiServerHandler) GetSemaphore(ctx context.Context, request *gracklepb.GetSemaphoreRequest, accountId uint64, limits grackle.GrackleServiceLimits) (*gracklepb.GetSemaphoreResponse, error) {
	now := time.Now()

	res1, err := s.grackleCoreApiClient.GetNamespace(ctx, &corepb.GetNamespaceRequest{
		NamespaceId: &corepb.NamespaceId{
			AccountId:     accountId,
			NamespaceName: request.NamespaceName,
		},
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	res2, err := s.grackleCoreApiClient.GetSemaphore(ctx, &corepb.GetSemaphoreRequest{
		SemaphoreId: &corepb.SemaphoreId{
			AccountId:          accountId,
			NamespaceName:      request.NamespaceName,
			NamespaceCreatedAt: res1.Namespace.CreatedAt,
			SemaphoreName:      request.SemaphoreName,
		},
		Now: now.UnixNano(),
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	return &gracklepb.GetSemaphoreResponse{
		Semaphore: semaphoreToFront(res2.Semaphore),
	}, nil
}

func (s *GrackleApiServerHandler) AcquireSemaphore(ctx context.Context, request *gracklepb.AcquireSemaphoreRequest, accountId uint64, limits grackle.GrackleServiceLimits) (*gracklepb.AcquireSemaphoreResponse, error) {
	now := time.Now()

	res1, err := s.grackleCoreApiClient.GetNamespace(ctx, &corepb.GetNamespaceRequest{
		NamespaceId: &corepb.NamespaceId{
			AccountId:     accountId,
			NamespaceName: request.NamespaceName,
		},
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	res2, err := s.grackleCoreApiClient.AcquireSemaphore(ctx, &corepb.AcquireSemaphoreRequest{
		SemaphoreId: &corepb.SemaphoreId{
			AccountId:          accountId,
			NamespaceName:      request.NamespaceName,
			NamespaceCreatedAt: res1.Namespace.CreatedAt,
			SemaphoreName:      request.SemaphoreName,
		},
		Now:       now.UnixNano(),
		ProcessId: request.ProcessId,
		ExpiresAt: request.ExpiresAt,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	return &gracklepb.AcquireSemaphoreResponse{
		Semaphore: semaphoreToFront(res2.Semaphore),
		Success:   res2.Success,
	}, nil
}

func (s *GrackleApiServerHandler) ReleaseSemaphore(ctx context.Context, request *gracklepb.ReleaseSemaphoreRequest, accountId uint64, limits grackle.GrackleServiceLimits) (*gracklepb.ReleaseSemaphoreResponse, error) {
	now := time.Now()

	res1, err := s.grackleCoreApiClient.GetNamespace(ctx, &corepb.GetNamespaceRequest{
		NamespaceId: &corepb.NamespaceId{
			AccountId:     accountId,
			NamespaceName: request.NamespaceName,
		},
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	res2, err := s.grackleCoreApiClient.ReleaseSemaphore(ctx, &corepb.ReleaseSemaphoreRequest{
		SemaphoreId: &corepb.SemaphoreId{
			AccountId:          accountId,
			NamespaceName:      request.NamespaceName,
			NamespaceCreatedAt: res1.Namespace.CreatedAt,
			SemaphoreName:      request.SemaphoreName,
		},
		Now:       now.UnixNano(),
		ProcessId: request.ProcessId,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	return &gracklepb.ReleaseSemaphoreResponse{
		Semaphore: semaphoreToFront(res2.Semaphore),
	}, nil
}

func (s *GrackleApiServerHandler) UpdateSemaphore(ctx context.Context, request *gracklepb.UpdateSemaphoreRequest, accountId uint64, limits grackle.GrackleServiceLimits) (*gracklepb.UpdateSemaphoreResponse, error) {
	now := time.Now()

	// Check if semaphore size is too big
	if request.Permits > uint64(limits.MaxNumberOfSemaphoreHolders) {
		return nil, status.Errorf(codes.InvalidArgument, "semaphore size is too big, max: %d", limits.MaxNumberOfSemaphoreHolders)
	}

	res1, err := s.grackleCoreApiClient.GetNamespace(ctx, &corepb.GetNamespaceRequest{
		NamespaceId: &corepb.NamespaceId{
			AccountId:     accountId,
			NamespaceName: request.NamespaceName,
		},
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	res2, err := s.grackleCoreApiClient.UpdateSemaphore(ctx, &corepb.UpdateSemaphoreRequest{
		SemaphoreId: &corepb.SemaphoreId{
			AccountId:          accountId,
			NamespaceName:      request.NamespaceName,
			NamespaceCreatedAt: res1.Namespace.CreatedAt,
			SemaphoreName:      request.SemaphoreName,
		},
		Description: request.Description,
		Now:         now.UnixNano(),
		Permits:     request.Permits,
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	return &gracklepb.UpdateSemaphoreResponse{
		Semaphore: semaphoreToFront(res2.Semaphore),
	}, nil
}

func (s *GrackleApiServerHandler) DeleteSemaphore(ctx context.Context, request *gracklepb.DeleteSemaphoreRequest, accountId uint64, limits grackle.GrackleServiceLimits) (*gracklepb.DeleteSemaphoreResponse, error) {
	res1, err := s.grackleCoreApiClient.GetNamespace(ctx, &corepb.GetNamespaceRequest{
		NamespaceId: &corepb.NamespaceId{
			AccountId:     accountId,
			NamespaceName: request.NamespaceName,
		},
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	_, err = s.grackleCoreApiClient.DeleteSemaphore(ctx, &corepb.DeleteSemaphoreRequest{
		SemaphoreId: &corepb.SemaphoreId{
			AccountId:          accountId,
			NamespaceName:      request.NamespaceName,
			NamespaceCreatedAt: res1.Namespace.CreatedAt,
			SemaphoreName:      request.SemaphoreName,
		},
	})
	if err != nil {
		return nil, monsterax.ErrorToGRPC(err)
	}

	return &gracklepb.DeleteSemaphoreResponse{}, nil
}

func NewGrackleApiServerHandler(grackleCoreApiClient grackle.GrackleCoreApi) *GrackleApiServerHandler {
	return &GrackleApiServerHandler{
		grackleCoreApiClient: grackleCoreApiClient,
	}
}
