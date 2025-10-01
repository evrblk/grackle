package grackle

import (
	"github.com/evrblk/grackle/pkg/corepb"
	"github.com/evrblk/monstera"
)

func shardByAccount(accountId uint64) []byte {
	return monstera.GetShardKey(monstera.ConcatBytes(accountId), 4)
}

func shardByAccountAndNamespace(accountId uint64, namespaceName string) []byte {
	return monstera.GetShardKey(monstera.ConcatBytes(accountId, namespaceName), 4)
}

type GrackleShardKeyCalculator struct{}

var _ GrackleMonsteraShardKeyCalculator = &GrackleShardKeyCalculator{}

func (g *GrackleShardKeyCalculator) ListLocksShardKey(request *corepb.ListLocksRequest) []byte {
	return shardByAccountAndNamespace(request.NamespaceTimestampedId.AccountId, request.NamespaceTimestampedId.NamespaceName)
}

func (g *GrackleShardKeyCalculator) AcquireLockShardKey(request *corepb.AcquireLockRequest) []byte {
	return shardByAccountAndNamespace(request.LockId.AccountId, request.LockId.NamespaceName)
}

func (g *GrackleShardKeyCalculator) AcquireSemaphoreShardKey(request *corepb.AcquireSemaphoreRequest) []byte {
	return shardByAccountAndNamespace(request.SemaphoreId.AccountId, request.SemaphoreId.NamespaceName)
}

func (g *GrackleShardKeyCalculator) AddJobsToWaitGroupShardKey(request *corepb.AddJobsToWaitGroupRequest) []byte {
	return shardByAccountAndNamespace(request.WaitGroupId.AccountId, request.WaitGroupId.NamespaceName)
}

func (g *GrackleShardKeyCalculator) CompleteJobsFromWaitGroupShardKey(request *corepb.CompleteJobsFromWaitGroupRequest) []byte {
	return shardByAccountAndNamespace(request.WaitGroupId.AccountId, request.WaitGroupId.NamespaceName)
}

func (g *GrackleShardKeyCalculator) CreateNamespaceShardKey(request *corepb.CreateNamespaceRequest) []byte {
	return shardByAccount(request.AccountId)
}

func (g *GrackleShardKeyCalculator) CreateSemaphoreShardKey(request *corepb.CreateSemaphoreRequest) []byte {
	return shardByAccountAndNamespace(request.NamespaceTimestampedId.AccountId, request.NamespaceTimestampedId.NamespaceName)
}

func (g *GrackleShardKeyCalculator) CreateWaitGroupShardKey(request *corepb.CreateWaitGroupRequest) []byte {
	return shardByAccountAndNamespace(request.NamespaceTimestampedId.AccountId, request.NamespaceTimestampedId.NamespaceName)
}

func (g *GrackleShardKeyCalculator) DeleteLockShardKey(request *corepb.DeleteLockRequest) []byte {
	return shardByAccountAndNamespace(request.LockId.AccountId, request.LockId.NamespaceName)
}

func (g *GrackleShardKeyCalculator) DeleteNamespaceShardKey(request *corepb.DeleteNamespaceRequest) []byte {
	return shardByAccount(request.NamespaceId.AccountId)
}

func (g *GrackleShardKeyCalculator) DeleteSemaphoreShardKey(request *corepb.DeleteSemaphoreRequest) []byte {
	return shardByAccountAndNamespace(request.SemaphoreId.AccountId, request.SemaphoreId.NamespaceName)
}

func (g *GrackleShardKeyCalculator) DeleteWaitGroupShardKey(request *corepb.DeleteWaitGroupRequest) []byte {
	return shardByAccountAndNamespace(request.WaitGroupId.AccountId, request.WaitGroupId.NamespaceName)
}

func (g *GrackleShardKeyCalculator) GetLockShardKey(request *corepb.GetLockRequest) []byte {
	return shardByAccountAndNamespace(request.LockId.AccountId, request.LockId.NamespaceName)
}

func (g *GrackleShardKeyCalculator) GetNamespaceShardKey(request *corepb.GetNamespaceRequest) []byte {
	return shardByAccount(request.NamespaceId.AccountId)
}

func (g *GrackleShardKeyCalculator) GetSemaphoreShardKey(request *corepb.GetSemaphoreRequest) []byte {
	return shardByAccountAndNamespace(request.SemaphoreId.AccountId, request.SemaphoreId.NamespaceName)
}

func (g *GrackleShardKeyCalculator) GetWaitGroupShardKey(request *corepb.GetWaitGroupRequest) []byte {
	return shardByAccountAndNamespace(request.WaitGroupId.AccountId, request.WaitGroupId.NamespaceName)
}

func (g *GrackleShardKeyCalculator) ListNamespacesShardKey(request *corepb.ListNamespacesRequest) []byte {
	return shardByAccount(request.AccountId)
}

func (g *GrackleShardKeyCalculator) ListSemaphoresShardKey(request *corepb.ListSemaphoresRequest) []byte {
	return shardByAccountAndNamespace(request.NamespaceTimestampedId.AccountId, request.NamespaceTimestampedId.NamespaceName)
}

func (g *GrackleShardKeyCalculator) ListWaitGroupsShardKey(request *corepb.ListWaitGroupsRequest) []byte {
	return shardByAccountAndNamespace(request.NamespaceTimestampedId.AccountId, request.NamespaceTimestampedId.NamespaceName)
}

func (g *GrackleShardKeyCalculator) ReleaseLockShardKey(request *corepb.ReleaseLockRequest) []byte {
	return shardByAccountAndNamespace(request.LockId.AccountId, request.LockId.NamespaceName)
}

func (g *GrackleShardKeyCalculator) ReleaseSemaphoreShardKey(request *corepb.ReleaseSemaphoreRequest) []byte {
	return shardByAccountAndNamespace(request.SemaphoreId.AccountId, request.SemaphoreId.NamespaceName)
}

func (g *GrackleShardKeyCalculator) UpdateNamespaceShardKey(request *corepb.UpdateNamespaceRequest) []byte {
	return shardByAccount(request.NamespaceId.AccountId)
}

func (g *GrackleShardKeyCalculator) UpdateSemaphoreShardKey(request *corepb.UpdateSemaphoreRequest) []byte {
	return shardByAccountAndNamespace(request.SemaphoreId.AccountId, request.SemaphoreId.NamespaceName)
}

func (g *GrackleShardKeyCalculator) LocksDeleteNamespaceShardKey(request *corepb.LocksDeleteNamespaceRequest) []byte {
	return shardByAccountAndNamespace(request.NamespaceTimestampedId.AccountId, request.NamespaceTimestampedId.NamespaceName)
}

func (g *GrackleShardKeyCalculator) SemaphoresDeleteNamespaceShardKey(request *corepb.SemaphoresDeleteNamespaceRequest) []byte {
	return shardByAccountAndNamespace(request.NamespaceTimestampedId.AccountId, request.NamespaceTimestampedId.NamespaceName)
}

func (g *GrackleShardKeyCalculator) WaitGroupsDeleteNamespaceShardKey(request *corepb.WaitGroupsDeleteNamespaceRequest) []byte {
	return shardByAccountAndNamespace(request.NamespaceTimestampedId.AccountId, request.NamespaceTimestampedId.NamespaceName)
}
