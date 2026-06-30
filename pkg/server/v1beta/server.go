package v1beta

import (
	"context"
	"log"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	gracklepb "github.com/evrblk/evrblk-go/grackle/v1beta"

	"github.com/evrblk/grackle/pkg/coreapis"
	"github.com/evrblk/grackle/pkg/grackle"
)

type GrackleApiServer struct {
	gracklepb.UnimplementedGrackleApiServer

	handler *GrackleApiServerHandler
}

func (s *GrackleApiServer) Close() {
	log.Println("Stopping GrackleApiServer...")
	s.handler.Stop()
}

func (s *GrackleApiServer) CreateNamespace(ctx context.Context, req *gracklepb.CreateNamespaceRequest) (*gracklepb.CreateNamespaceResponse, error) {
	if err := ValidateCreateNamespaceRequest(req); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	return s.handler.CreateNamespace(ctx, req, 0, grackle.DefaultServiceLimits)
}

func (s *GrackleApiServer) GetNamespace(ctx context.Context, req *gracklepb.GetNamespaceRequest) (*gracklepb.GetNamespaceResponse, error) {
	if err := ValidateGetNamespaceRequest(req); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	return s.handler.GetNamespace(ctx, req, 0, grackle.DefaultServiceLimits)
}

func (s *GrackleApiServer) UpdateNamespace(ctx context.Context, req *gracklepb.UpdateNamespaceRequest) (*gracklepb.UpdateNamespaceResponse, error) {
	if err := ValidateUpdateNamespaceRequest(req); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	return s.handler.UpdateNamespace(ctx, req, 0, grackle.DefaultServiceLimits)
}

func (s *GrackleApiServer) DeleteNamespace(ctx context.Context, req *gracklepb.DeleteNamespaceRequest) (*gracklepb.DeleteNamespaceResponse, error) {
	if err := ValidateDeleteNamespaceRequest(req); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	return s.handler.DeleteNamespace(ctx, req, 0, grackle.DefaultServiceLimits)
}

func (s *GrackleApiServer) ListNamespaces(ctx context.Context, req *gracklepb.ListNamespacesRequest) (*gracklepb.ListNamespacesResponse, error) {
	if err := ValidateListNamespacesRequest(req); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	return s.handler.ListNamespaces(ctx, req, 0, grackle.DefaultServiceLimits)
}

func (s *GrackleApiServer) CreateWaitGroup(ctx context.Context, req *gracklepb.CreateWaitGroupRequest) (*gracklepb.CreateWaitGroupResponse, error) {
	if err := ValidateCreateWaitGroupRequest(req); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	return s.handler.CreateWaitGroup(ctx, req, 0, grackle.DefaultServiceLimits)
}

func (s *GrackleApiServer) UpdateWaitGroup(ctx context.Context, req *gracklepb.UpdateWaitGroupRequest) (*gracklepb.UpdateWaitGroupResponse, error) {
	if err := ValidateUpdateWaitGroupRequest(req); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	return s.handler.UpdateWaitGroup(ctx, req, 0, grackle.DefaultServiceLimits)
}

func (s *GrackleApiServer) GetWaitGroup(ctx context.Context, req *gracklepb.GetWaitGroupRequest) (*gracklepb.GetWaitGroupResponse, error) {
	if err := ValidateGetWaitGroupRequest(req); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	return s.handler.GetWaitGroup(ctx, req, 0, grackle.DefaultServiceLimits)
}

func (s *GrackleApiServer) WaitForWaitGroup(ctx context.Context, req *gracklepb.WaitForWaitGroupRequest) (*gracklepb.WaitForWaitGroupResponse, error) {
	if err := ValidateWaitForWaitGroupRequest(req); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	return s.handler.WaitForWaitGroup(ctx, req, 0, grackle.DefaultServiceLimits)
}

func (s *GrackleApiServer) CompleteJobsFromWaitGroup(ctx context.Context, req *gracklepb.CompleteJobsFromWaitGroupRequest) (*gracklepb.CompleteJobsFromWaitGroupResponse, error) {
	if err := ValidateCompleteJobsFromWaitGroupRequest(req); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	return s.handler.CompleteJobsFromWaitGroup(ctx, req, 0, grackle.DefaultServiceLimits)
}

func (s *GrackleApiServer) DeleteWaitGroup(ctx context.Context, req *gracklepb.DeleteWaitGroupRequest) (*gracklepb.DeleteWaitGroupResponse, error) {
	if err := ValidateDeleteWaitGroupRequest(req); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	return s.handler.DeleteWaitGroup(ctx, req, 0, grackle.DefaultServiceLimits)
}

func (s *GrackleApiServer) ListWaitGroups(ctx context.Context, req *gracklepb.ListWaitGroupsRequest) (*gracklepb.ListWaitGroupsResponse, error) {
	if err := ValidateListWaitGroupsRequest(req); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	return s.handler.ListWaitGroups(ctx, req, 0, grackle.DefaultServiceLimits)
}

func (s *GrackleApiServer) ListWaitGroupCompletedJobs(ctx context.Context, req *gracklepb.ListWaitGroupCompletedJobsRequest) (*gracklepb.ListWaitGroupCompletedJobsResponse, error) {
	if err := ValidateListWaitGroupCompletedJobsRequest(req); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	return s.handler.ListWaitGroupCompletedJobs(ctx, req, 0, grackle.DefaultServiceLimits)
}

func (s *GrackleApiServer) AcquireLock(ctx context.Context, req *gracklepb.AcquireLockRequest) (*gracklepb.AcquireLockResponse, error) {
	if err := ValidateAcquireLockRequest(req); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	return s.handler.AcquireLock(ctx, req, 0, grackle.DefaultServiceLimits)
}

func (s *GrackleApiServer) ReleaseLock(ctx context.Context, req *gracklepb.ReleaseLockRequest) (*gracklepb.ReleaseLockResponse, error) {
	if err := ValidateReleaseLockRequest(req); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	return s.handler.ReleaseLock(ctx, req, 0, grackle.DefaultServiceLimits)
}

func (s *GrackleApiServer) GetLock(ctx context.Context, req *gracklepb.GetLockRequest) (*gracklepb.GetLockResponse, error) {
	if err := ValidateGetLockRequest(req); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	return s.handler.GetLock(ctx, req, 0, grackle.DefaultServiceLimits)
}

func (s *GrackleApiServer) DeleteLock(ctx context.Context, req *gracklepb.DeleteLockRequest) (*gracklepb.DeleteLockResponse, error) {
	if err := ValidateDeleteLockRequest(req); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	return s.handler.DeleteLock(ctx, req, 0, grackle.DefaultServiceLimits)
}

func (s *GrackleApiServer) ListLocks(ctx context.Context, req *gracklepb.ListLocksRequest) (*gracklepb.ListLocksResponse, error) {
	if err := ValidateListLocksRequest(req); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	return s.handler.ListLocks(ctx, req, 0, grackle.DefaultServiceLimits)
}

func (s *GrackleApiServer) CreateSemaphore(ctx context.Context, req *gracklepb.CreateSemaphoreRequest) (*gracklepb.CreateSemaphoreResponse, error) {
	if err := ValidateCreateSemaphoreRequest(req); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	return s.handler.CreateSemaphore(ctx, req, 0, grackle.DefaultServiceLimits)
}

func (s *GrackleApiServer) ListSemaphores(ctx context.Context, req *gracklepb.ListSemaphoresRequest) (*gracklepb.ListSemaphoresResponse, error) {
	if err := ValidateListSemaphoresRequest(req); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	return s.handler.ListSemaphores(ctx, req, 0, grackle.DefaultServiceLimits)
}

func (s *GrackleApiServer) ListSemaphoreHolders(ctx context.Context, req *gracklepb.ListSemaphoreHoldersRequest) (*gracklepb.ListSemaphoreHoldersResponse, error) {
	if err := ValidateListSemaphoreHoldersRequest(req); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	return s.handler.ListSemaphoreHolders(ctx, req, 0, grackle.DefaultServiceLimits)
}

func (s *GrackleApiServer) GetSemaphore(ctx context.Context, req *gracklepb.GetSemaphoreRequest) (*gracklepb.GetSemaphoreResponse, error) {
	if err := ValidateGetSemaphoreRequest(req); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	return s.handler.GetSemaphore(ctx, req, 0, grackle.DefaultServiceLimits)
}

func (s *GrackleApiServer) AcquireSemaphore(ctx context.Context, req *gracklepb.AcquireSemaphoreRequest) (*gracklepb.AcquireSemaphoreResponse, error) {
	if err := ValidateAcquireSemaphoreRequest(req); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	return s.handler.AcquireSemaphore(ctx, req, 0, grackle.DefaultServiceLimits)
}

func (s *GrackleApiServer) ReleaseSemaphore(ctx context.Context, req *gracklepb.ReleaseSemaphoreRequest) (*gracklepb.ReleaseSemaphoreResponse, error) {
	if err := ValidateReleaseSemaphoreRequest(req); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	return s.handler.ReleaseSemaphore(ctx, req, 0, grackle.DefaultServiceLimits)
}

func (s *GrackleApiServer) UpdateSemaphore(ctx context.Context, req *gracklepb.UpdateSemaphoreRequest) (*gracklepb.UpdateSemaphoreResponse, error) {
	if err := ValidateUpdateSemaphoreRequest(req); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	return s.handler.UpdateSemaphore(ctx, req, 0, grackle.DefaultServiceLimits)
}

func (s *GrackleApiServer) DeleteSemaphore(ctx context.Context, req *gracklepb.DeleteSemaphoreRequest) (*gracklepb.DeleteSemaphoreResponse, error) {
	if err := ValidateDeleteSemaphoreRequest(req); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	return s.handler.DeleteSemaphore(ctx, req, 0, grackle.DefaultServiceLimits)
}

func (s *GrackleApiServer) CreateBarrier(ctx context.Context, req *gracklepb.CreateBarrierRequest) (*gracklepb.CreateBarrierResponse, error) {
	if err := ValidateCreateBarrierRequest(req); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	return s.handler.CreateBarrier(ctx, req, 0, grackle.DefaultServiceLimits)
}

func (s *GrackleApiServer) ListBarriers(ctx context.Context, req *gracklepb.ListBarriersRequest) (*gracklepb.ListBarriersResponse, error) {
	if err := ValidateListBarriersRequest(req); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	return s.handler.ListBarriers(ctx, req, 0, grackle.DefaultServiceLimits)
}

func (s *GrackleApiServer) GetBarrier(ctx context.Context, req *gracklepb.GetBarrierRequest) (*gracklepb.GetBarrierResponse, error) {
	if err := ValidateGetBarrierRequest(req); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	return s.handler.GetBarrier(ctx, req, 0, grackle.DefaultServiceLimits)
}

func (s *GrackleApiServer) DeleteBarrier(ctx context.Context, req *gracklepb.DeleteBarrierRequest) (*gracklepb.DeleteBarrierResponse, error) {
	if err := ValidateDeleteBarrierRequest(req); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	return s.handler.DeleteBarrier(ctx, req, 0, grackle.DefaultServiceLimits)
}

func (s *GrackleApiServer) UpdateBarrier(ctx context.Context, req *gracklepb.UpdateBarrierRequest) (*gracklepb.UpdateBarrierResponse, error) {
	if err := ValidateUpdateBarrierRequest(req); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	return s.handler.UpdateBarrier(ctx, req, 0, grackle.DefaultServiceLimits)
}

func (s *GrackleApiServer) ArriveAtBarrier(ctx context.Context, req *gracklepb.ArriveAtBarrierRequest) (*gracklepb.ArriveAtBarrierResponse, error) {
	if err := ValidateArriveAtBarrierRequest(req); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	return s.handler.ArriveAtBarrier(ctx, req, 0, grackle.DefaultServiceLimits)
}

func (s *GrackleApiServer) WaitAtBarrier(ctx context.Context, req *gracklepb.WaitAtBarrierRequest) (*gracklepb.WaitAtBarrierResponse, error) {
	if err := ValidateWaitAtBarrierRequest(req); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	return s.handler.WaitAtBarrier(ctx, req, 0, grackle.DefaultServiceLimits)
}

func (s *GrackleApiServer) ListBarrierParticipants(ctx context.Context, req *gracklepb.ListBarrierParticipantsRequest) (*gracklepb.ListBarrierParticipantsResponse, error) {
	if err := ValidateListBarrierParticipantsRequest(req); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	return s.handler.ListBarrierParticipants(ctx, req, 0, grackle.DefaultServiceLimits)
}

func (s *GrackleApiServer) CreateSemaphoreLease(ctx context.Context, req *gracklepb.CreateSemaphoreLeaseRequest) (*gracklepb.CreateSemaphoreLeaseResponse, error) {
	if err := ValidateCreateSemaphoreLeaseRequest(req); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	return s.handler.CreateSemaphoreLease(ctx, req, 0, grackle.DefaultServiceLimits)
}

func (s *GrackleApiServer) RevokeSemaphoreLease(ctx context.Context, req *gracklepb.RevokeSemaphoreLeaseRequest) (*gracklepb.RevokeSemaphoreLeaseResponse, error) {
	if err := ValidateRevokeSemaphoreLeaseRequest(req); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	return s.handler.RevokeSemaphoreLease(ctx, req, 0, grackle.DefaultServiceLimits)
}

func (s *GrackleApiServer) RefreshSemaphoreLease(ctx context.Context, req *gracklepb.RefreshSemaphoreLeaseRequest) (*gracklepb.RefreshSemaphoreLeaseResponse, error) {
	if err := ValidateRefreshSemaphoreLeaseRequest(req); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	return s.handler.RefreshSemaphoreLease(ctx, req, 0, grackle.DefaultServiceLimits)
}

func (s *GrackleApiServer) ListSemaphoreLeases(ctx context.Context, req *gracklepb.ListSemaphoreLeasesRequest) (*gracklepb.ListSemaphoreLeasesResponse, error) {
	if err := ValidateListSemaphoreLeasesRequest(req); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	return s.handler.ListSemaphoreLeases(ctx, req, 0, grackle.DefaultServiceLimits)
}

func (s *GrackleApiServer) GetSemaphoreLease(ctx context.Context, req *gracklepb.GetSemaphoreLeaseRequest) (*gracklepb.GetSemaphoreLeaseResponse, error) {
	if err := ValidateGetSemaphoreLeaseRequest(req); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	return s.handler.GetSemaphoreLease(ctx, req, 0, grackle.DefaultServiceLimits)
}

func (s *GrackleApiServer) CreateLockLease(ctx context.Context, req *gracklepb.CreateLockLeaseRequest) (*gracklepb.CreateLockLeaseResponse, error) {
	if err := ValidateCreateLockLeaseRequest(req); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	return s.handler.CreateLockLease(ctx, req, 0, grackle.DefaultServiceLimits)
}

func (s *GrackleApiServer) RevokeLockLease(ctx context.Context, req *gracklepb.RevokeLockLeaseRequest) (*gracklepb.RevokeLockLeaseResponse, error) {
	if err := ValidateRevokeLockLeaseRequest(req); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	return s.handler.RevokeLockLease(ctx, req, 0, grackle.DefaultServiceLimits)
}

func (s *GrackleApiServer) RefreshLockLease(ctx context.Context, req *gracklepb.RefreshLockLeaseRequest) (*gracklepb.RefreshLockLeaseResponse, error) {
	if err := ValidateRefreshLockLeaseRequest(req); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	return s.handler.RefreshLockLease(ctx, req, 0, grackle.DefaultServiceLimits)
}

func (s *GrackleApiServer) ListLockLeases(ctx context.Context, req *gracklepb.ListLockLeasesRequest) (*gracklepb.ListLockLeasesResponse, error) {
	if err := ValidateListLockLeasesRequest(req); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	return s.handler.ListLockLeases(ctx, req, 0, grackle.DefaultServiceLimits)
}

func (s *GrackleApiServer) GetLockLease(ctx context.Context, req *gracklepb.GetLockLeaseRequest) (*gracklepb.GetLockLeaseResponse, error) {
	if err := ValidateGetLockLeaseRequest(req); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	return s.handler.GetLockLease(ctx, req, 0, grackle.DefaultServiceLimits)
}

func NewGrackleApiServer(grackleClient coreapis.GrackleClientApi) *GrackleApiServer {
	return &GrackleApiServer{
		handler: NewGrackleApiServerHandler(grackleClient),
	}
}
