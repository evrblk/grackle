package v1beta

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	gracklepb "github.com/evrblk/evrblk-go/grackle/v1beta"
	mrpc "github.com/evrblk/monstera/rpc"

	"github.com/evrblk/grackle/pkg/coreapis"
	"github.com/evrblk/grackle/pkg/corepb"
	"github.com/evrblk/grackle/pkg/grackle"
)

func TestHandler_CreateNamespace(t *testing.T) {
	t.Run("retries on IDCollision", func(t *testing.T) {
		calls := 0
		seenIDs := make(map[uint64]struct{})
		handler := &GrackleApiServerHandler{grackleClient: &fakeGrackleClient{
			createNamespace: func(ctx context.Context, req *corepb.CreateNamespaceRequest) (*corepb.CreateNamespaceResponse, error) {
				calls++
				seenIDs[req.NamespaceId.NamespaceId] = struct{}{}
				// Collide on the first two attempts, then succeed.
				if calls < 3 {
					return nil, mrpc.NewError(mrpc.IDCollision, "namespace with this id already exists")
				}
				return &corepb.CreateNamespaceResponse{
					Namespace: &corepb.Namespace{Id: req.NamespaceId, Name: req.Name},
				}, nil
			},
		}}

		resp, err := handler.CreateNamespace(
			context.Background(),
			&gracklepb.CreateNamespaceRequest{Name: "ns"},
			42,
			grackle.ServiceLimits{MaxNumberOfNamespaces: 10},
		)

		require.NoError(t, err)
		require.Equal(t, "ns", resp.Namespace.Name)
		require.Equal(t, 3, calls)
		// Each retry regenerates different ID.
		require.Len(t, seenIDs, 3)
	})

	t.Run("exhausts retries", func(t *testing.T) {
		calls := 0
		handler := &GrackleApiServerHandler{grackleClient: &fakeGrackleClient{
			createNamespace: func(ctx context.Context, req *corepb.CreateNamespaceRequest) (*corepb.CreateNamespaceResponse, error) {
				calls++
				return nil, mrpc.NewError(mrpc.IDCollision, "namespace with this id already exists")
			},
		}}

		_, err := handler.CreateNamespace(
			context.Background(),
			&gracklepb.CreateNamespaceRequest{Name: "ns"},
			42,
			grackle.ServiceLimits{MaxNumberOfNamespaces: 10},
		)

		require.Error(t, err)
		// IDCollision is never surfaced to the client; exhausting retries is Internal.
		require.Equal(t, codes.Internal, status.Code(err))
		require.Equal(t, maxIDGenerationAttempts, calls)
	})

	t.Run("non-collision error not retried", func(t *testing.T) {
		calls := 0
		handler := &GrackleApiServerHandler{grackleClient: &fakeGrackleClient{
			createNamespace: func(ctx context.Context, req *corepb.CreateNamespaceRequest) (*corepb.CreateNamespaceResponse, error) {
				calls++
				return nil, mrpc.NewErrorWithContext(mrpc.AlreadyExists, "namespace with this name already exists",
					map[string]string{"namespace_name": req.Name})
			},
		}}

		_, err := handler.CreateNamespace(
			context.Background(),
			&gracklepb.CreateNamespaceRequest{Name: "ns"},
			42,
			grackle.ServiceLimits{MaxNumberOfNamespaces: 10},
		)

		require.Error(t, err)
		// A name conflict is a user-facing error returned immediately, without retry.
		require.Equal(t, codes.AlreadyExists, status.Code(err))
		require.Equal(t, 1, calls)
	})
}

func TestHandler_CreateWaitGroup(t *testing.T) {
	t.Run("retries on IDCollision", func(t *testing.T) {
		now := time.Now()
		calls := 0
		seenIDs := make(map[uint64]struct{})
		handler := &GrackleApiServerHandler{grackleClient: &fakeGrackleClient{
			createWaitGroup: func(ctx context.Context, req *corepb.CreateWaitGroupRequest) (*corepb.CreateWaitGroupResponse, error) {
				calls++
				seenIDs[req.WaitGroupId.WaitGroupId] = struct{}{}
				if calls < 3 {
					return nil, mrpc.NewError(mrpc.IDCollision, "wait group with this id already exists")
				}
				return &corepb.CreateWaitGroupResponse{WaitGroup: &corepb.WaitGroup{Id: req.WaitGroupId, Name: req.Name}}, nil
			},
		}}

		resp, err := handler.CreateWaitGroup(
			context.Background(),
			&gracklepb.CreateWaitGroupRequest{WaitGroupName: "wg", ExpiresAt: now.Add(time.Hour).UnixNano()},
			42,
			grackle.ServiceLimits{MaxWaitGroupSize: 100, MaxNumberOfWaitGroupsPerNamespace: 10},
		)

		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Equal(t, 3, calls)
		// Each retry regenerates different ID.
		require.Len(t, seenIDs, 3)
	})

	t.Run("exhausts retries", func(t *testing.T) {
		now := time.Now()
		calls := 0
		handler := &GrackleApiServerHandler{grackleClient: &fakeGrackleClient{
			createWaitGroup: func(ctx context.Context, req *corepb.CreateWaitGroupRequest) (*corepb.CreateWaitGroupResponse, error) {
				calls++
				return nil, mrpc.NewError(mrpc.IDCollision, "wait group with this id already exists")
			},
		}}

		_, err := handler.CreateWaitGroup(
			context.Background(),
			&gracklepb.CreateWaitGroupRequest{WaitGroupName: "wg", ExpiresAt: now.Add(time.Hour).UnixNano()},
			42,
			grackle.ServiceLimits{MaxWaitGroupSize: 100, MaxNumberOfWaitGroupsPerNamespace: 10},
		)

		require.Error(t, err)
		require.Equal(t, codes.Internal, status.Code(err))
		require.Equal(t, maxIDGenerationAttempts, calls)
	})
}

func TestHandler_CreateSemaphore(t *testing.T) {
	t.Run("retries on IDCollision", func(t *testing.T) {
		calls := 0
		seenIDs := make(map[uint64]struct{})
		handler := &GrackleApiServerHandler{grackleClient: &fakeGrackleClient{
			createSemaphore: func(ctx context.Context, req *corepb.CreateSemaphoreRequest) (*corepb.CreateSemaphoreResponse, error) {
				calls++
				seenIDs[req.SemaphoreId.SemaphoreId] = struct{}{}
				if calls < 3 {
					return nil, mrpc.NewError(mrpc.IDCollision, "semaphore with this id already exists")
				}
				return &corepb.CreateSemaphoreResponse{Semaphore: &corepb.Semaphore{Id: req.SemaphoreId, Name: req.Name}}, nil
			},
		}}

		resp, err := handler.CreateSemaphore(
			context.Background(),
			&gracklepb.CreateSemaphoreRequest{SemaphoreName: "sem", Permits: 1},
			42,
			grackle.ServiceLimits{MaxNumberOfSemaphoreHolders: 100, MaxNumberOfSemaphoresPerNamespace: 10},
		)

		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Equal(t, 3, calls)
		// Each retry regenerates different ID.
		require.Len(t, seenIDs, 3)
	})

	t.Run("exhausts retries", func(t *testing.T) {
		calls := 0
		handler := &GrackleApiServerHandler{grackleClient: &fakeGrackleClient{
			createSemaphore: func(ctx context.Context, req *corepb.CreateSemaphoreRequest) (*corepb.CreateSemaphoreResponse, error) {
				calls++
				return nil, mrpc.NewError(mrpc.IDCollision, "semaphore with this id already exists")
			},
		}}

		_, err := handler.CreateSemaphore(
			context.Background(),
			&gracklepb.CreateSemaphoreRequest{SemaphoreName: "sem", Permits: 1},
			42,
			grackle.ServiceLimits{MaxNumberOfSemaphoreHolders: 100, MaxNumberOfSemaphoresPerNamespace: 10},
		)

		require.Error(t, err)
		require.Equal(t, codes.Internal, status.Code(err))
		require.Equal(t, maxIDGenerationAttempts, calls)
	})
}

func TestHandler_CreateBarrier(t *testing.T) {
	t.Run("retries on IDCollision", func(t *testing.T) {
		calls := 0
		seenIDs := make(map[uint64]struct{})
		handler := &GrackleApiServerHandler{grackleClient: &fakeGrackleClient{
			createBarrier: func(ctx context.Context, req *corepb.CreateBarrierRequest) (*corepb.CreateBarrierResponse, error) {
				calls++
				seenIDs[req.BarrierId.BarrierId] = struct{}{}
				if calls < 3 {
					return nil, mrpc.NewError(mrpc.IDCollision, "barrier with this id already exists")
				}
				return &corepb.CreateBarrierResponse{Barrier: &corepb.Barrier{Id: req.BarrierId, Name: req.Name}}, nil
			},
		}}

		resp, err := handler.CreateBarrier(
			context.Background(),
			&gracklepb.CreateBarrierRequest{BarrierName: "bar", ExpectedProcesses: 3},
			42,
			grackle.ServiceLimits{MaxNumberOfBarriersPerNamespace: 10},
		)

		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Equal(t, 3, calls)
		// Each retry regenerates different ID.
		require.Len(t, seenIDs, 3)
	})

	t.Run("exhausts retries", func(t *testing.T) {
		calls := 0
		handler := &GrackleApiServerHandler{grackleClient: &fakeGrackleClient{
			createBarrier: func(ctx context.Context, req *corepb.CreateBarrierRequest) (*corepb.CreateBarrierResponse, error) {
				calls++
				return nil, mrpc.NewError(mrpc.IDCollision, "barrier with this id already exists")
			},
		}}

		_, err := handler.CreateBarrier(
			context.Background(),
			&gracklepb.CreateBarrierRequest{BarrierName: "bar", ExpectedProcesses: 3},
			42,
			grackle.ServiceLimits{MaxNumberOfBarriersPerNamespace: 10},
		)

		require.Error(t, err)
		require.Equal(t, codes.Internal, status.Code(err))
		require.Equal(t, maxIDGenerationAttempts, calls)
	})
}

func TestHandler_CreateSemaphoreLease(t *testing.T) {
	t.Run("retries on IDCollision", func(t *testing.T) {
		calls := 0
		seenIDs := make(map[uint64]struct{})
		handler := &GrackleApiServerHandler{grackleClient: &fakeGrackleClient{
			createSemaphoreLease: func(ctx context.Context, req *corepb.CreateSemaphoreLeaseRequest) (*corepb.CreateSemaphoreLeaseResponse, error) {
				calls++
				seenIDs[req.LeaseId.LeaseId] = struct{}{}
				if calls < 3 {
					return nil, mrpc.NewError(mrpc.IDCollision, "lease with this id already exists")
				}
				return &corepb.CreateSemaphoreLeaseResponse{Lease: &corepb.Lease{Id: req.LeaseId, ProcessId: req.ProcessId}}, nil
			},
		}}

		resp, err := handler.CreateSemaphoreLease(
			context.Background(),
			&gracklepb.CreateSemaphoreLeaseRequest{ProcessId: "p", TtlSeconds: 60},
			42,
			grackle.ServiceLimits{MaxNumberOfSemaphoreLeases: 10},
		)

		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Equal(t, 3, calls)
		// Each retry regenerates different ID.
		require.Len(t, seenIDs, 3)
	})

	t.Run("exhausts retries", func(t *testing.T) {
		calls := 0
		handler := &GrackleApiServerHandler{grackleClient: &fakeGrackleClient{
			createSemaphoreLease: func(ctx context.Context, req *corepb.CreateSemaphoreLeaseRequest) (*corepb.CreateSemaphoreLeaseResponse, error) {
				calls++
				return nil, mrpc.NewError(mrpc.IDCollision, "lease with this id already exists")
			},
		}}

		_, err := handler.CreateSemaphoreLease(
			context.Background(),
			&gracklepb.CreateSemaphoreLeaseRequest{ProcessId: "p", TtlSeconds: 60},
			42,
			grackle.ServiceLimits{MaxNumberOfSemaphoreLeases: 10},
		)

		require.Error(t, err)
		require.Equal(t, codes.Internal, status.Code(err))
		require.Equal(t, maxIDGenerationAttempts, calls)
	})
}

func TestHandler_CreateLockLease(t *testing.T) {
	t.Run("retries on IDCollision", func(t *testing.T) {
		calls := 0
		seenIDs := make(map[uint64]struct{})
		handler := &GrackleApiServerHandler{grackleClient: &fakeGrackleClient{
			createLockLease: func(ctx context.Context, req *corepb.CreateLockLeaseRequest) (*corepb.CreateLockLeaseResponse, error) {
				calls++
				seenIDs[req.LeaseId.LeaseId] = struct{}{}
				if calls < 3 {
					return nil, mrpc.NewError(mrpc.IDCollision, "lease with this id already exists")
				}
				return &corepb.CreateLockLeaseResponse{Lease: &corepb.Lease{Id: req.LeaseId, ProcessId: req.ProcessId}}, nil
			},
		}}

		resp, err := handler.CreateLockLease(
			context.Background(),
			&gracklepb.CreateLockLeaseRequest{ProcessId: "p", TtlSeconds: 60},
			42,
			grackle.ServiceLimits{MaxNumberOfLockLeases: 10},
		)

		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Equal(t, 3, calls)
		// Each retry regenerates different ID.
		require.Len(t, seenIDs, 3)
	})

	t.Run("exhausts retries", func(t *testing.T) {
		calls := 0
		handler := &GrackleApiServerHandler{grackleClient: &fakeGrackleClient{
			createLockLease: func(ctx context.Context, req *corepb.CreateLockLeaseRequest) (*corepb.CreateLockLeaseResponse, error) {
				calls++
				return nil, mrpc.NewError(mrpc.IDCollision, "lease with this id already exists")
			},
		}}

		_, err := handler.CreateLockLease(
			context.Background(),
			&gracklepb.CreateLockLeaseRequest{ProcessId: "p", TtlSeconds: 60},
			42,
			grackle.ServiceLimits{MaxNumberOfLockLeases: 10},
		)

		require.Error(t, err)
		require.Equal(t, codes.Internal, status.Code(err))
		require.Equal(t, maxIDGenerationAttempts, calls)
	})
}

type fakeGrackleClient struct {
	coreapis.GrackleClientApi

	createNamespace      func(ctx context.Context, req *corepb.CreateNamespaceRequest) (*corepb.CreateNamespaceResponse, error)
	createWaitGroup      func(ctx context.Context, req *corepb.CreateWaitGroupRequest) (*corepb.CreateWaitGroupResponse, error)
	createSemaphore      func(ctx context.Context, req *corepb.CreateSemaphoreRequest) (*corepb.CreateSemaphoreResponse, error)
	createBarrier        func(ctx context.Context, req *corepb.CreateBarrierRequest) (*corepb.CreateBarrierResponse, error)
	createSemaphoreLease func(ctx context.Context, req *corepb.CreateSemaphoreLeaseRequest) (*corepb.CreateSemaphoreLeaseResponse, error)
	createLockLease      func(ctx context.Context, req *corepb.CreateLockLeaseRequest) (*corepb.CreateLockLeaseResponse, error)
}

func (f *fakeGrackleClient) CreateNamespace(ctx context.Context, req *corepb.CreateNamespaceRequest) (*corepb.CreateNamespaceResponse, error) {
	return f.createNamespace(ctx, req)
}

func (f *fakeGrackleClient) CreateWaitGroup(ctx context.Context, req *corepb.CreateWaitGroupRequest) (*corepb.CreateWaitGroupResponse, error) {
	return f.createWaitGroup(ctx, req)
}

func (f *fakeGrackleClient) CreateSemaphore(ctx context.Context, req *corepb.CreateSemaphoreRequest) (*corepb.CreateSemaphoreResponse, error) {
	return f.createSemaphore(ctx, req)
}

func (f *fakeGrackleClient) CreateBarrier(ctx context.Context, req *corepb.CreateBarrierRequest) (*corepb.CreateBarrierResponse, error) {
	return f.createBarrier(ctx, req)
}

func (f *fakeGrackleClient) CreateSemaphoreLease(ctx context.Context, req *corepb.CreateSemaphoreLeaseRequest) (*corepb.CreateSemaphoreLeaseResponse, error) {
	return f.createSemaphoreLease(ctx, req)
}

func (f *fakeGrackleClient) CreateLockLease(ctx context.Context, req *corepb.CreateLockLeaseRequest) (*corepb.CreateLockLeaseResponse, error) {
	return f.createLockLease(ctx, req)
}

// The wait group / semaphore / barrier handlers first resolve the namespace by
// name; return a fixed namespace so the create path is reached.
func (f *fakeGrackleClient) GetNamespaceByName(ctx context.Context, req *corepb.GetNamespaceByNameRequest) (*corepb.GetNamespaceByNameResponse, error) {
	return &corepb.GetNamespaceByNameResponse{
		Namespace: &corepb.Namespace{Id: &corepb.NamespaceId{AccountId: req.AccountId, NamespaceId: 7}},
	}, nil
}
