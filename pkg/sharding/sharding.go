package sharding

import (
	"github.com/evrblk/monstera/utils"

	"github.com/evrblk/grackle/pkg/corepb"
	"github.com/evrblk/grackle/pkg/monsteragen"
)

func ByAccount(accountId uint64) []byte {
	return utils.GetTruncatedHash(utils.ConcatBytes(accountId), 4)
}

func ByAccountAndNamespace(accountId uint64, namespaceId uint32) []byte {
	return utils.GetTruncatedHash(utils.ConcatBytes(accountId, namespaceId), 4)
}

type GrackleShardKeyCalculator struct{}

var _ monsteragen.GrackleMonsteraShardKeyCalculator = &GrackleShardKeyCalculator{}

func (g *GrackleShardKeyCalculator) ListLocksShardKey(request *corepb.ListLocksRequest) []byte {
	return ByAccountAndNamespace(request.NamespaceId.AccountId, request.NamespaceId.NamespaceId)
}

func (g *GrackleShardKeyCalculator) AcquireLockShardKey(request *corepb.AcquireLockRequest) []byte {
	return ByAccountAndNamespace(request.LockId.AccountId, request.LockId.NamespaceId)
}

func (g *GrackleShardKeyCalculator) AcquireSemaphoreShardKey(request *corepb.AcquireSemaphoreRequest) []byte {
	return ByAccountAndNamespace(request.NamespaceId.AccountId, request.NamespaceId.NamespaceId)
}

func (g *GrackleShardKeyCalculator) AddJobsToWaitGroupShardKey(request *corepb.AddJobsToWaitGroupRequest) []byte {
	return ByAccountAndNamespace(request.NamespaceId.AccountId, request.NamespaceId.NamespaceId)
}

func (g *GrackleShardKeyCalculator) CompleteJobsFromWaitGroupShardKey(request *corepb.CompleteJobsFromWaitGroupRequest) []byte {
	return ByAccountAndNamespace(request.NamespaceId.AccountId, request.NamespaceId.NamespaceId)
}

func (g *GrackleShardKeyCalculator) CreateNamespaceShardKey(request *corepb.CreateNamespaceRequest) []byte {
	return ByAccount(request.NamespaceId.AccountId)
}

func (g *GrackleShardKeyCalculator) CreateSemaphoreShardKey(request *corepb.CreateSemaphoreRequest) []byte {
	return ByAccountAndNamespace(request.SemaphoreId.AccountId, request.SemaphoreId.NamespaceId)
}

func (g *GrackleShardKeyCalculator) CreateWaitGroupShardKey(request *corepb.CreateWaitGroupRequest) []byte {
	return ByAccountAndNamespace(request.WaitGroupId.AccountId, request.WaitGroupId.NamespaceId)
}

func (g *GrackleShardKeyCalculator) DeleteLockShardKey(request *corepb.DeleteLockRequest) []byte {
	return ByAccountAndNamespace(request.LockId.AccountId, request.LockId.NamespaceId)
}

func (g *GrackleShardKeyCalculator) DeleteNamespaceShardKey(request *corepb.DeleteNamespaceRequest) []byte {
	return ByAccount(request.NamespaceId.AccountId)
}

func (g *GrackleShardKeyCalculator) DeleteSemaphoreShardKey(request *corepb.DeleteSemaphoreRequest) []byte {
	return ByAccountAndNamespace(request.NamespaceId.AccountId, request.NamespaceId.NamespaceId)
}

func (g *GrackleShardKeyCalculator) DeleteWaitGroupShardKey(request *corepb.DeleteWaitGroupRequest) []byte {
	return ByAccountAndNamespace(request.NamespaceId.AccountId, request.NamespaceId.NamespaceId)
}

func (g *GrackleShardKeyCalculator) GetLockShardKey(request *corepb.GetLockRequest) []byte {
	return ByAccountAndNamespace(request.LockId.AccountId, request.LockId.NamespaceId)
}

func (g *GrackleShardKeyCalculator) GetNamespaceShardKey(request *corepb.GetNamespaceRequest) []byte {
	return ByAccount(request.NamespaceId.AccountId)
}

func (g *GrackleShardKeyCalculator) GetNamespaceByNameShardKey(request *corepb.GetNamespaceByNameRequest) []byte {
	return ByAccount(request.AccountId)
}

func (g *GrackleShardKeyCalculator) GetSemaphoreShardKey(request *corepb.GetSemaphoreRequest) []byte {
	return ByAccountAndNamespace(request.SemaphoreId.AccountId, request.SemaphoreId.NamespaceId)
}

func (g *GrackleShardKeyCalculator) GetSemaphoreByNameShardKey(request *corepb.GetSemaphoreByNameRequest) []byte {
	return ByAccountAndNamespace(request.NamespaceId.AccountId, request.NamespaceId.NamespaceId)
}

func (g *GrackleShardKeyCalculator) GetWaitGroupShardKey(request *corepb.GetWaitGroupRequest) []byte {
	return ByAccountAndNamespace(request.WaitGroupId.AccountId, request.WaitGroupId.NamespaceId)
}

func (g *GrackleShardKeyCalculator) GetWaitGroupByNameShardKey(request *corepb.GetWaitGroupByNameRequest) []byte {
	return ByAccountAndNamespace(request.NamespaceId.AccountId, request.NamespaceId.NamespaceId)
}

func (g *GrackleShardKeyCalculator) ListNamespacesShardKey(request *corepb.ListNamespacesRequest) []byte {
	return ByAccount(request.AccountId)
}

func (g *GrackleShardKeyCalculator) ListSemaphoresShardKey(request *corepb.ListSemaphoresRequest) []byte {
	return ByAccountAndNamespace(request.NamespaceId.AccountId, request.NamespaceId.NamespaceId)
}

func (g *GrackleShardKeyCalculator) ListSemaphoreHoldersShardKey(request *corepb.ListSemaphoreHoldersRequest) []byte {
	return ByAccountAndNamespace(request.NamespaceId.AccountId, request.NamespaceId.NamespaceId)
}

func (g *GrackleShardKeyCalculator) ListWaitGroupsShardKey(request *corepb.ListWaitGroupsRequest) []byte {
	return ByAccountAndNamespace(request.NamespaceId.AccountId, request.NamespaceId.NamespaceId)
}

func (g *GrackleShardKeyCalculator) ListWaitGroupJobsShardKey(request *corepb.ListWaitGroupJobsRequest) []byte {
	return ByAccountAndNamespace(request.NamespaceId.AccountId, request.NamespaceId.NamespaceId)
}

func (g *GrackleShardKeyCalculator) ReleaseLockShardKey(request *corepb.ReleaseLockRequest) []byte {
	return ByAccountAndNamespace(request.LockId.AccountId, request.LockId.NamespaceId)
}

func (g *GrackleShardKeyCalculator) ReleaseSemaphoreShardKey(request *corepb.ReleaseSemaphoreRequest) []byte {
	return ByAccountAndNamespace(request.NamespaceId.AccountId, request.NamespaceId.NamespaceId)
}

func (g *GrackleShardKeyCalculator) UpdateNamespaceShardKey(request *corepb.UpdateNamespaceRequest) []byte {
	return ByAccount(request.NamespaceId.AccountId)
}

func (g *GrackleShardKeyCalculator) UpdateSemaphoreShardKey(request *corepb.UpdateSemaphoreRequest) []byte {
	return ByAccountAndNamespace(request.NamespaceId.AccountId, request.NamespaceId.NamespaceId)
}

func (g *GrackleShardKeyCalculator) LocksDeleteNamespaceShardKey(request *corepb.LocksDeleteNamespaceRequest) []byte {
	return ByAccountAndNamespace(request.NamespaceId.AccountId, request.NamespaceId.NamespaceId)
}

func (g *GrackleShardKeyCalculator) SemaphoresDeleteNamespaceShardKey(request *corepb.SemaphoresDeleteNamespaceRequest) []byte {
	return ByAccountAndNamespace(request.NamespaceId.AccountId, request.NamespaceId.NamespaceId)
}

func (g *GrackleShardKeyCalculator) WaitGroupsDeleteNamespaceShardKey(request *corepb.WaitGroupsDeleteNamespaceRequest) []byte {
	return ByAccountAndNamespace(request.NamespaceId.AccountId, request.NamespaceId.NamespaceId)
}

func (g *GrackleShardKeyCalculator) GetBarrierShardKey(request *corepb.GetBarrierRequest) []byte {
	return ByAccountAndNamespace(request.BarrierId.AccountId, request.BarrierId.NamespaceId)
}

func (g *GrackleShardKeyCalculator) GetBarrierByNameShardKey(request *corepb.GetBarrierByNameRequest) []byte {
	return ByAccountAndNamespace(request.NamespaceId.AccountId, request.NamespaceId.NamespaceId)
}

func (g *GrackleShardKeyCalculator) ListBarriersShardKey(request *corepb.ListBarriersRequest) []byte {
	return ByAccountAndNamespace(request.NamespaceId.AccountId, request.NamespaceId.NamespaceId)
}

func (g *GrackleShardKeyCalculator) ListBarrierParticipantsShardKey(request *corepb.ListBarrierParticipantsRequest) []byte {
	return ByAccountAndNamespace(request.NamespaceId.AccountId, request.NamespaceId.NamespaceId)
}

func (g *GrackleShardKeyCalculator) CreateBarrierShardKey(request *corepb.CreateBarrierRequest) []byte {
	return ByAccountAndNamespace(request.BarrierId.AccountId, request.BarrierId.NamespaceId)
}

func (g *GrackleShardKeyCalculator) UpdateBarrierShardKey(request *corepb.UpdateBarrierRequest) []byte {
	return ByAccountAndNamespace(request.BarrierId.AccountId, request.BarrierId.NamespaceId)
}

func (g *GrackleShardKeyCalculator) DeleteBarrierShardKey(request *corepb.DeleteBarrierRequest) []byte {
	return ByAccountAndNamespace(request.NamespaceId.AccountId, request.NamespaceId.NamespaceId)
}

func (g *GrackleShardKeyCalculator) ArriveAtBarrierShardKey(request *corepb.ArriveAtBarrierRequest) []byte {
	return ByAccountAndNamespace(request.NamespaceId.AccountId, request.NamespaceId.NamespaceId)
}

func (g *GrackleShardKeyCalculator) WaitAtBarrierShardKey(request *corepb.WaitAtBarrierRequest) []byte {
	return ByAccountAndNamespace(request.NamespaceId.AccountId, request.NamespaceId.NamespaceId)
}

func (g *GrackleShardKeyCalculator) BarriersDeleteNamespaceShardKey(request *corepb.BarriersDeleteNamespaceRequest) []byte {
	return ByAccountAndNamespace(request.NamespaceId.AccountId, request.NamespaceId.NamespaceId)
}
