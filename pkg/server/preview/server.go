package preview

import (
	"context"
	"log"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	gracklepb "github.com/evrblk/evrblk-go/grackle/preview"

	"github.com/evrblk/grackle/pkg/grackle"
	"github.com/evrblk/grackle/pkg/monsteragen"
)

var (
	locksOperationsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "grackle_locks_operations_total",
		Help: "Grackle Locks operations total",
	})
	semaphoresOperationsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "grackle_semaphores_operations_total",
		Help: "Grackle Semaphores operations total",
	})
	waitGroupsOperationsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "grackle_wait_groups_operations_total",
		Help: "Grackle Wait Groups operations total",
	})
	barriersOperationsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "grackle_barriers_operations_total",
		Help: "Grackle Barriers operations total",
	})
)

func RegisterMetrics() {
	prometheus.MustRegister(locksOperationsTotal)
	prometheus.MustRegister(semaphoresOperationsTotal)
	prometheus.MustRegister(waitGroupsOperationsTotal)
	prometheus.MustRegister(barriersOperationsTotal)
}

var (
	defaultServiceLimits = grackle.GrackleServiceLimits{
		MaxNumberOfNamespaces:             10_000,
		MaxNumberOfWaitGroupsPerNamespace: 100_000,
		MaxNumberOfLocksPerNamespace:      100_000,
		MaxNumberOfSemaphoresPerNamespace: 100_000,
		MaxNumberOfBarriersPerNamespace:   100_000,
		MaxNumberOfSharedLockHolders:      100,
		MaxNumberOfSemaphoreHolders:       1_000,
		MaxWaitGroupSize:                  100_000_000,
		MaxNumberOfBarrierParticipants:    1_000_000,
	}
)

type GrackleApiServer struct {
	gracklepb.UnimplementedGracklePreviewApiServer

	handler *GrackleApiServerHandler
}

func (s *GrackleApiServer) Close() {
	log.Println("Stopping GrackleApiServer...")
	s.handler.Stop()
}

func (s *GrackleApiServer) CreateNamespace(ctx context.Context, request *gracklepb.CreateNamespaceRequest) (*gracklepb.CreateNamespaceResponse, error) {
	if err := ValidateCreateNamespaceRequest(request); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	return s.handler.CreateNamespace(ctx, request, 0, defaultServiceLimits)
}

func (s *GrackleApiServer) GetNamespace(ctx context.Context, request *gracklepb.GetNamespaceRequest) (*gracklepb.GetNamespaceResponse, error) {
	if err := ValidateGetNamespaceRequest(request); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	return s.handler.GetNamespace(ctx, request, 0, defaultServiceLimits)
}

func (s *GrackleApiServer) UpdateNamespace(ctx context.Context, request *gracklepb.UpdateNamespaceRequest) (*gracklepb.UpdateNamespaceResponse, error) {
	if err := ValidateUpdateNamespaceRequest(request); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	return s.handler.UpdateNamespace(ctx, request, 0, defaultServiceLimits)
}

func (s *GrackleApiServer) DeleteNamespace(ctx context.Context, request *gracklepb.DeleteNamespaceRequest) (*gracklepb.DeleteNamespaceResponse, error) {
	if err := ValidateDeleteNamespaceRequest(request); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	return s.handler.DeleteNamespace(ctx, request, 0, defaultServiceLimits)
}

func (s *GrackleApiServer) ListNamespaces(ctx context.Context, request *gracklepb.ListNamespacesRequest) (*gracklepb.ListNamespacesResponse, error) {
	if err := ValidateListNamespacesRequest(request); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	return s.handler.ListNamespaces(ctx, request, 0, defaultServiceLimits)
}

func (s *GrackleApiServer) CreateWaitGroup(ctx context.Context, request *gracklepb.CreateWaitGroupRequest) (*gracklepb.CreateWaitGroupResponse, error) {
	if err := ValidateCreateWaitGroupRequest(request); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	// Increment counter of total wait groups operations
	waitGroupsOperationsTotal.Inc()

	return s.handler.CreateWaitGroup(ctx, request, 0, defaultServiceLimits)
}

func (s *GrackleApiServer) GetWaitGroup(ctx context.Context, request *gracklepb.GetWaitGroupRequest) (*gracklepb.GetWaitGroupResponse, error) {
	if err := ValidateGetWaitGroupRequest(request); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	// Increment counter of total wait groups operations
	waitGroupsOperationsTotal.Inc()

	return s.handler.GetWaitGroup(ctx, request, 0, defaultServiceLimits)
}

func (s *GrackleApiServer) AddJobsToWaitGroup(ctx context.Context, request *gracklepb.AddJobsToWaitGroupRequest) (*gracklepb.AddJobsToWaitGroupResponse, error) {
	if err := ValidateAddJobsToWaitGroupRequest(request); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	// Increment counter of total wait groups operations
	waitGroupsOperationsTotal.Inc()

	return s.handler.AddJobsToWaitGroup(ctx, request, 0, defaultServiceLimits)
}

func (s *GrackleApiServer) CompleteJobsFromWaitGroup(ctx context.Context, request *gracklepb.CompleteJobsFromWaitGroupRequest) (*gracklepb.CompleteJobsFromWaitGroupResponse, error) {
	if err := ValidateCompleteJobsFromWaitGroupRequest(request); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	// Increment counter of total wait groups operations
	waitGroupsOperationsTotal.Inc()

	return s.handler.CompleteJobsFromWaitGroup(ctx, request, 0, defaultServiceLimits)
}

func (s *GrackleApiServer) DeleteWaitGroup(ctx context.Context, request *gracklepb.DeleteWaitGroupRequest) (*gracklepb.DeleteWaitGroupResponse, error) {
	if err := ValidateDeleteWaitGroupRequest(request); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	// Increment counter of total wait groups operations
	waitGroupsOperationsTotal.Inc()

	return s.handler.DeleteWaitGroup(ctx, request, 0, defaultServiceLimits)
}

func (s *GrackleApiServer) ListWaitGroups(ctx context.Context, request *gracklepb.ListWaitGroupsRequest) (*gracklepb.ListWaitGroupsResponse, error) {
	if err := ValidateListWaitGroupsRequest(request); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	return s.handler.ListWaitGroups(ctx, request, 0, defaultServiceLimits)
}

func (s *GrackleApiServer) ListWaitGroupJobs(ctx context.Context, request *gracklepb.ListWaitGroupJobsRequest) (*gracklepb.ListWaitGroupJobsResponse, error) {
	if err := ValidateListWaitGroupJobsRequest(request); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	return s.handler.ListWaitGroupJobs(ctx, request, 0, defaultServiceLimits)
}

func (s *GrackleApiServer) AcquireLock(ctx context.Context, request *gracklepb.AcquireLockRequest) (*gracklepb.AcquireLockResponse, error) {
	if err := ValidateAcquireLockRequest(request); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	// Increment counter of total locks operations
	locksOperationsTotal.Inc()

	return s.handler.AcquireLock(ctx, request, 0, defaultServiceLimits)
}

func (s *GrackleApiServer) ReleaseLock(ctx context.Context, request *gracklepb.ReleaseLockRequest) (*gracklepb.ReleaseLockResponse, error) {
	if err := ValidateReleaseLockRequest(request); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	// Increment counter of total locks operations
	locksOperationsTotal.Inc()

	return s.handler.ReleaseLock(ctx, request, 0, defaultServiceLimits)
}

func (s *GrackleApiServer) GetLock(ctx context.Context, request *gracklepb.GetLockRequest) (*gracklepb.GetLockResponse, error) {
	if err := ValidateGetLockRequest(request); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	// Increment counter of total locks operations
	locksOperationsTotal.Inc()

	return s.handler.GetLock(ctx, request, 0, defaultServiceLimits)
}

func (s *GrackleApiServer) DeleteLock(ctx context.Context, request *gracklepb.DeleteLockRequest) (*gracklepb.DeleteLockResponse, error) {
	if err := ValidateDeleteLockRequest(request); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	// Increment counter of total locks operations
	locksOperationsTotal.Inc()

	return s.handler.DeleteLock(ctx, request, 0, defaultServiceLimits)
}

func (s *GrackleApiServer) ListLocks(ctx context.Context, request *gracklepb.ListLocksRequest) (*gracklepb.ListLocksResponse, error) {
	if err := ValidateListLocksRequest(request); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	return s.handler.ListLocks(ctx, request, 0, defaultServiceLimits)
}

func (s *GrackleApiServer) CreateSemaphore(ctx context.Context, request *gracklepb.CreateSemaphoreRequest) (*gracklepb.CreateSemaphoreResponse, error) {
	if err := ValidateCreateSemaphoreRequest(request); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	return s.handler.CreateSemaphore(ctx, request, 0, defaultServiceLimits)
}

func (s *GrackleApiServer) ListSemaphores(ctx context.Context, request *gracklepb.ListSemaphoresRequest) (*gracklepb.ListSemaphoresResponse, error) {
	if err := ValidateListSemaphoresRequest(request); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	return s.handler.ListSemaphores(ctx, request, 0, defaultServiceLimits)
}

func (s *GrackleApiServer) ListSemaphoreHolders(ctx context.Context, request *gracklepb.ListSemaphoreHoldersRequest) (*gracklepb.ListSemaphoreHoldersResponse, error) {
	if err := ValidateListSemaphoreHoldersRequest(request); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	return s.handler.ListSemaphoreHolders(ctx, request, 0, defaultServiceLimits)
}

func (s *GrackleApiServer) GetSemaphore(ctx context.Context, request *gracklepb.GetSemaphoreRequest) (*gracklepb.GetSemaphoreResponse, error) {
	if err := ValidateGetSemaphoreRequest(request); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	// Increment counter of total semaphore operations
	semaphoresOperationsTotal.Inc()

	return s.handler.GetSemaphore(ctx, request, 0, defaultServiceLimits)
}

func (s *GrackleApiServer) AcquireSemaphore(ctx context.Context, request *gracklepb.AcquireSemaphoreRequest) (*gracklepb.AcquireSemaphoreResponse, error) {
	if err := ValidateAcquireSemaphoreRequest(request); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	// Increment counter of total semaphore operations
	semaphoresOperationsTotal.Inc()

	return s.handler.AcquireSemaphore(ctx, request, 0, defaultServiceLimits)
}

func (s *GrackleApiServer) ReleaseSemaphore(ctx context.Context, request *gracklepb.ReleaseSemaphoreRequest) (*gracklepb.ReleaseSemaphoreResponse, error) {
	if err := ValidateReleaseSemaphoreRequest(request); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	// Increment counter of total semaphore operations
	semaphoresOperationsTotal.Inc()

	return s.handler.ReleaseSemaphore(ctx, request, 0, defaultServiceLimits)
}

func (s *GrackleApiServer) UpdateSemaphore(ctx context.Context, request *gracklepb.UpdateSemaphoreRequest) (*gracklepb.UpdateSemaphoreResponse, error) {
	if err := ValidateUpdateSemaphoreRequest(request); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	return s.handler.UpdateSemaphore(ctx, request, 0, defaultServiceLimits)
}

func (s *GrackleApiServer) DeleteSemaphore(ctx context.Context, request *gracklepb.DeleteSemaphoreRequest) (*gracklepb.DeleteSemaphoreResponse, error) {
	if err := ValidateDeleteSemaphoreRequest(request); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	return s.handler.DeleteSemaphore(ctx, request, 0, defaultServiceLimits)
}

func (s *GrackleApiServer) CreateBarrier(ctx context.Context, request *gracklepb.CreateBarrierRequest) (*gracklepb.CreateBarrierResponse, error) {
	if err := ValidateCreateBarrierRequest(request); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	return s.handler.CreateBarrier(ctx, request, 0, defaultServiceLimits)
}

func (s *GrackleApiServer) ListBarriers(ctx context.Context, request *gracklepb.ListBarriersRequest) (*gracklepb.ListBarriersResponse, error) {
	if err := ValidateListBarriersRequest(request); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	return s.handler.ListBarriers(ctx, request, 0, defaultServiceLimits)
}

func (s *GrackleApiServer) GetBarrier(ctx context.Context, request *gracklepb.GetBarrierRequest) (*gracklepb.GetBarrierResponse, error) {
	if err := ValidateGetBarrierRequest(request); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	return s.handler.GetBarrier(ctx, request, 0, defaultServiceLimits)
}

func (s *GrackleApiServer) DeleteBarrier(ctx context.Context, request *gracklepb.DeleteBarrierRequest) (*gracklepb.DeleteBarrierResponse, error) {
	if err := ValidateDeleteBarrierRequest(request); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	return s.handler.DeleteBarrier(ctx, request, 0, defaultServiceLimits)
}

func (s *GrackleApiServer) UpdateBarrier(ctx context.Context, request *gracklepb.UpdateBarrierRequest) (*gracklepb.UpdateBarrierResponse, error) {
	if err := ValidateUpdateBarrierRequest(request); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	return s.handler.UpdateBarrier(ctx, request, 0, defaultServiceLimits)
}

func (s *GrackleApiServer) ArriveAtBarrier(ctx context.Context, request *gracklepb.ArriveAtBarrierRequest) (*gracklepb.ArriveAtBarrierResponse, error) {
	if err := ValidateArriveAtBarrierRequest(request); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	return s.handler.ArriveAtBarrier(ctx, request, 0, defaultServiceLimits)
}

func (s *GrackleApiServer) WaitAtBarrier(ctx context.Context, request *gracklepb.WaitAtBarrierRequest) (*gracklepb.WaitAtBarrierResponse, error) {
	if err := ValidateWaitAtBarrierRequest(request); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	return s.handler.WaitAtBarrier(ctx, request, 0, defaultServiceLimits)
}

func (s *GrackleApiServer) ListBarrierParticipants(ctx context.Context, request *gracklepb.ListBarrierParticipantsRequest) (*gracklepb.ListBarrierParticipantsResponse, error) {
	if err := ValidateListBarrierParticipantsRequest(request); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	return s.handler.ListBarrierParticipants(ctx, request, 0, defaultServiceLimits)
}

func NewGrackleApiServer(grackleCoreApiClient monsteragen.GrackleCoreApi) *GrackleApiServer {
	return &GrackleApiServer{
		handler: NewGrackleApiServerHandler(grackleCoreApiClient),
	}
}
