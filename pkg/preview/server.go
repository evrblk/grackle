package preview

import (
	"context"
	"log"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	gracklepb "github.com/evrblk/evrblk-go/grackle/preview"
	"github.com/evrblk/grackle/pkg/grackle"
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

	errGrpcUnauthenticated = status.Error(codes.Unauthenticated, "unauthenticated")
)

func init() {
	prometheus.MustRegister(locksOperationsTotal)
	prometheus.MustRegister(semaphoresOperationsTotal)
	prometheus.MustRegister(waitGroupsOperationsTotal)
}

var (
	defaultServiceLimits = grackle.GrackleServiceLimits{
		MaxNumberOfNamespaces:             100,
		MaxNumberOfWaitGroupsPerNamespace: 100,
		MaxNumberOfLocksPerNamespace:      100,
		MaxNumberOfSemaphoresPerNamespace: 100,
		MaxNumberOfReadLockHolders:        100,
		MaxNumberOfSemaphoreHolders:       100,
		MaxWaitGroupSize:                  1000000,
		ControlPlaneReadRequestRate:       1000,
		ControlPlaneUpdateRequestRate:     100,
		DataPlaneRequestRate:              1000,
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
	accountId, ok := ctx.Value("account").(uint64)
	if !ok {
		return nil, errGrpcUnauthenticated
	}

	if err := ValidateCreateNamespaceRequest(request); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	return s.handler.CreateNamespace(ctx, request, accountId, defaultServiceLimits)
}

func (s *GrackleApiServer) GetNamespace(ctx context.Context, request *gracklepb.GetNamespaceRequest) (*gracklepb.GetNamespaceResponse, error) {
	accountId, ok := ctx.Value("account").(uint64)
	if !ok {
		return nil, errGrpcUnauthenticated
	}

	if err := ValidateGetNamespaceRequest(request); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	return s.handler.GetNamespace(ctx, request, accountId, defaultServiceLimits)
}

func (s *GrackleApiServer) UpdateNamespace(ctx context.Context, request *gracklepb.UpdateNamespaceRequest) (*gracklepb.UpdateNamespaceResponse, error) {
	accountId, ok := ctx.Value("account").(uint64)
	if !ok {
		return nil, errGrpcUnauthenticated
	}

	if err := ValidateUpdateNamespaceRequest(request); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	return s.handler.UpdateNamespace(ctx, request, accountId, defaultServiceLimits)
}

func (s *GrackleApiServer) DeleteNamespace(ctx context.Context, request *gracklepb.DeleteNamespaceRequest) (*gracklepb.DeleteNamespaceResponse, error) {
	accountId, ok := ctx.Value("account").(uint64)
	if !ok {
		return nil, errGrpcUnauthenticated
	}

	if err := ValidateDeleteNamespaceRequest(request); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	return s.handler.DeleteNamespace(ctx, request, accountId, defaultServiceLimits)
}

func (s *GrackleApiServer) ListNamespaces(ctx context.Context, request *gracklepb.ListNamespacesRequest) (*gracklepb.ListNamespacesResponse, error) {
	accountId, ok := ctx.Value("account").(uint64)
	if !ok {
		return nil, errGrpcUnauthenticated
	}

	if err := ValidateListNamespacesRequest(request); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	return s.handler.ListNamespaces(ctx, request, accountId, defaultServiceLimits)
}

func (s *GrackleApiServer) CreateWaitGroup(ctx context.Context, request *gracklepb.CreateWaitGroupRequest) (*gracklepb.CreateWaitGroupResponse, error) {
	accountId, ok := ctx.Value("account").(uint64)
	if !ok {
		return nil, errGrpcUnauthenticated
	}

	if err := ValidateCreateWaitGroupRequest(request); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	// Increment counter of total wait groups operations
	waitGroupsOperationsTotal.Inc()

	return s.handler.CreateWaitGroup(ctx, request, accountId, defaultServiceLimits)
}

func (s *GrackleApiServer) GetWaitGroup(ctx context.Context, request *gracklepb.GetWaitGroupRequest) (*gracklepb.GetWaitGroupResponse, error) {
	accountId, ok := ctx.Value("account").(uint64)
	if !ok {
		return nil, errGrpcUnauthenticated
	}

	if err := ValidateGetWaitGroupRequest(request); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	// Increment counter of total wait groups operations
	waitGroupsOperationsTotal.Inc()

	return s.handler.GetWaitGroup(ctx, request, accountId, defaultServiceLimits)
}

func (s *GrackleApiServer) AddJobsToWaitGroup(ctx context.Context, request *gracklepb.AddJobsToWaitGroupRequest) (*gracklepb.AddJobsToWaitGroupResponse, error) {
	accountId, ok := ctx.Value("account").(uint64)
	if !ok {
		return nil, errGrpcUnauthenticated
	}

	if err := ValidateAddJobsToWaitGroupRequest(request); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	// Increment counter of total wait groups operations
	waitGroupsOperationsTotal.Inc()

	return s.handler.AddJobsToWaitGroup(ctx, request, accountId, defaultServiceLimits)
}

func (s *GrackleApiServer) CompleteJobsFromWaitGroup(ctx context.Context, request *gracklepb.CompleteJobsFromWaitGroupRequest) (*gracklepb.CompleteJobsFromWaitGroupResponse, error) {
	accountId, ok := ctx.Value("account").(uint64)
	if !ok {
		return nil, errGrpcUnauthenticated
	}

	if err := ValidateCompleteJobsFromWaitGroupRequest(request); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	// Increment counter of total wait groups operations
	waitGroupsOperationsTotal.Inc()

	return s.handler.CompleteJobsFromWaitGroup(ctx, request, accountId, defaultServiceLimits)
}

func (s *GrackleApiServer) DeleteWaitGroup(ctx context.Context, request *gracklepb.DeleteWaitGroupRequest) (*gracklepb.DeleteWaitGroupResponse, error) {
	accountId, ok := ctx.Value("account").(uint64)
	if !ok {
		return nil, errGrpcUnauthenticated
	}

	if err := ValidateDeleteWaitGroupRequest(request); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	// Increment counter of total wait groups operations
	waitGroupsOperationsTotal.Inc()

	return s.handler.DeleteWaitGroup(ctx, request, accountId, defaultServiceLimits)
}

func (s *GrackleApiServer) ListWaitGroups(ctx context.Context, request *gracklepb.ListWaitGroupsRequest) (*gracklepb.ListWaitGroupsResponse, error) {
	accountId, ok := ctx.Value("account").(uint64)
	if !ok {
		return nil, errGrpcUnauthenticated
	}

	if err := ValidateListWaitGroupsRequest(request); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	return s.handler.ListWaitGroups(ctx, request, accountId, defaultServiceLimits)
}

func (s *GrackleApiServer) AcquireLock(ctx context.Context, request *gracklepb.AcquireLockRequest) (*gracklepb.AcquireLockResponse, error) {
	accountId, ok := ctx.Value("account").(uint64)
	if !ok {
		return nil, errGrpcUnauthenticated
	}

	if err := ValidateAcquireLockRequest(request); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	// Increment counter of total locks operations
	locksOperationsTotal.Inc()

	return s.handler.AcquireLock(ctx, request, accountId, defaultServiceLimits)
}

func (s *GrackleApiServer) ReleaseLock(ctx context.Context, request *gracklepb.ReleaseLockRequest) (*gracklepb.ReleaseLockResponse, error) {
	accountId, ok := ctx.Value("account").(uint64)
	if !ok {
		return nil, errGrpcUnauthenticated
	}

	if err := ValidateReleaseLockRequest(request); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	// Increment counter of total locks operations
	locksOperationsTotal.Inc()

	return s.handler.ReleaseLock(ctx, request, accountId, defaultServiceLimits)
}

func (s *GrackleApiServer) GetLock(ctx context.Context, request *gracklepb.GetLockRequest) (*gracklepb.GetLockResponse, error) {
	accountId, ok := ctx.Value("account").(uint64)
	if !ok {
		return nil, errGrpcUnauthenticated
	}

	if err := ValidateGetLockRequest(request); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	// Increment counter of total locks operations
	locksOperationsTotal.Inc()

	return s.handler.GetLock(ctx, request, accountId, defaultServiceLimits)
}

func (s *GrackleApiServer) DeleteLock(ctx context.Context, request *gracklepb.DeleteLockRequest) (*gracklepb.DeleteLockResponse, error) {
	accountId, ok := ctx.Value("account").(uint64)
	if !ok {
		return nil, errGrpcUnauthenticated
	}

	if err := ValidateDeleteLockRequest(request); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	// Increment counter of total locks operations
	locksOperationsTotal.Inc()

	return s.handler.DeleteLock(ctx, request, accountId, defaultServiceLimits)
}

func (s *GrackleApiServer) ListLocks(ctx context.Context, request *gracklepb.ListLocksRequest) (*gracklepb.ListLocksResponse, error) {
	accountId, ok := ctx.Value("account").(uint64)
	if !ok {
		return nil, errGrpcUnauthenticated
	}

	if err := ValidateListLocksRequest(request); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	return s.handler.ListLocks(ctx, request, accountId, defaultServiceLimits)
}

func (s *GrackleApiServer) CreateSemaphore(ctx context.Context, request *gracklepb.CreateSemaphoreRequest) (*gracklepb.CreateSemaphoreResponse, error) {
	accountId, ok := ctx.Value("account").(uint64)
	if !ok {
		return nil, errGrpcUnauthenticated
	}

	if err := ValidateCreateSemaphoreRequest(request); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	return s.handler.CreateSemaphore(ctx, request, accountId, defaultServiceLimits)
}

func (s *GrackleApiServer) ListSemaphores(ctx context.Context, request *gracklepb.ListSemaphoresRequest) (*gracklepb.ListSemaphoresResponse, error) {
	accountId, ok := ctx.Value("account").(uint64)
	if !ok {
		return nil, errGrpcUnauthenticated
	}

	if err := ValidateListSemaphoresRequest(request); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	return s.handler.ListSemaphores(ctx, request, accountId, defaultServiceLimits)
}

func (s *GrackleApiServer) GetSemaphore(ctx context.Context, request *gracklepb.GetSemaphoreRequest) (*gracklepb.GetSemaphoreResponse, error) {
	accountId, ok := ctx.Value("account").(uint64)
	if !ok {
		return nil, errGrpcUnauthenticated
	}

	if err := ValidateGetSemaphoreRequest(request); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	// Increment counter of total semaphore operations
	semaphoresOperationsTotal.Inc()

	return s.handler.GetSemaphore(ctx, request, accountId, defaultServiceLimits)
}

func (s *GrackleApiServer) AcquireSemaphore(ctx context.Context, request *gracklepb.AcquireSemaphoreRequest) (*gracklepb.AcquireSemaphoreResponse, error) {
	accountId, ok := ctx.Value("account").(uint64)
	if !ok {
		return nil, errGrpcUnauthenticated
	}

	if err := ValidateAcquireSemaphoreRequest(request); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	// Increment counter of total semaphore operations
	semaphoresOperationsTotal.Inc()

	return s.handler.AcquireSemaphore(ctx, request, accountId, defaultServiceLimits)
}

func (s *GrackleApiServer) ReleaseSemaphore(ctx context.Context, request *gracklepb.ReleaseSemaphoreRequest) (*gracklepb.ReleaseSemaphoreResponse, error) {
	accountId, ok := ctx.Value("account").(uint64)
	if !ok {
		return nil, errGrpcUnauthenticated
	}

	if err := ValidateReleaseSemaphoreRequest(request); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	// Increment counter of total semaphore operations
	semaphoresOperationsTotal.Inc()

	return s.handler.ReleaseSemaphore(ctx, request, accountId, defaultServiceLimits)
}

func (s *GrackleApiServer) UpdateSemaphore(ctx context.Context, request *gracklepb.UpdateSemaphoreRequest) (*gracklepb.UpdateSemaphoreResponse, error) {
	accountId, ok := ctx.Value("account").(uint64)
	if !ok {
		return nil, errGrpcUnauthenticated
	}

	if err := ValidateUpdateSemaphoreRequest(request); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	return s.handler.UpdateSemaphore(ctx, request, accountId, defaultServiceLimits)
}

func (s *GrackleApiServer) DeleteSemaphore(ctx context.Context, request *gracklepb.DeleteSemaphoreRequest) (*gracklepb.DeleteSemaphoreResponse, error) {
	accountId, ok := ctx.Value("account").(uint64)
	if !ok {
		return nil, errGrpcUnauthenticated
	}

	if err := ValidateDeleteSemaphoreRequest(request); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	return s.handler.DeleteSemaphore(ctx, request, accountId, defaultServiceLimits)
}

func NewGrackleApiServer(grackleCoreApiClient grackle.GrackleCoreApi) *GrackleApiServer {
	return &GrackleApiServer{
		handler: NewGrackleApiServerHandler(grackleCoreApiClient),
	}
}
