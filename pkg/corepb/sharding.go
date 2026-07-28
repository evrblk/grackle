package corepb

import (
	"github.com/evrblk/grackle/pkg/sharding"
	"github.com/evrblk/monstera/cluster"
)

// ListLocksRequest

func (r *ListLocksRequest) ShardKey() cluster.ShardKey {
	return sharding.ByAccountAndNamespace(r.NamespaceId.AccountId, r.NamespaceId.NamespaceId)
}

// ListLocksByLeaseIdRequest

func (r *ListLocksByLeaseIdRequest) ShardKey() cluster.ShardKey {
	return sharding.ByAccountAndNamespace(r.LeaseId.AccountId, r.LeaseId.NamespaceId)
}

// GetLockLeaseRequest

func (r *GetLockLeaseRequest) ShardKey() cluster.ShardKey {
	return sharding.ByAccountAndNamespace(r.LeaseId.AccountId, r.LeaseId.NamespaceId)
}

// ListLockLeasesRequest

func (r *ListLockLeasesRequest) ShardKey() cluster.ShardKey {
	return sharding.ByAccountAndNamespace(r.NamespaceId.AccountId, r.NamespaceId.NamespaceId)
}

// ListLockLeasesByProcessIdRequest

func (r *ListLockLeasesByProcessIdRequest) ShardKey() cluster.ShardKey {
	return sharding.ByAccountAndNamespace(r.NamespaceId.AccountId, r.NamespaceId.NamespaceId)
}

// CreateLockLeaseRequest

func (r *CreateLockLeaseRequest) ShardKey() cluster.ShardKey {
	return sharding.ByAccountAndNamespace(r.LeaseId.AccountId, r.LeaseId.NamespaceId)
}

// RefreshLockLeaseRequest

func (r *RefreshLockLeaseRequest) ShardKey() cluster.ShardKey {
	return sharding.ByAccountAndNamespace(r.LeaseId.AccountId, r.LeaseId.NamespaceId)
}

// RevokeLockLeaseRequest

func (r *RevokeLockLeaseRequest) ShardKey() cluster.ShardKey {
	return sharding.ByAccountAndNamespace(r.LeaseId.AccountId, r.LeaseId.NamespaceId)
}

// AcquireLockRequest

func (r *AcquireLockRequest) ShardKey() cluster.ShardKey {
	return sharding.ByAccountAndNamespace(r.LockId.AccountId, r.LockId.NamespaceId)
}

// AcquireSemaphoreRequest

func (r *AcquireSemaphoreRequest) ShardKey() cluster.ShardKey {
	return sharding.ByAccountAndNamespace(r.NamespaceId.AccountId, r.NamespaceId.NamespaceId)
}

// CompleteJobsFromWaitGroupRequest

func (r *CompleteJobsFromWaitGroupRequest) ShardKey() cluster.ShardKey {
	return sharding.ByAccountAndNamespace(r.NamespaceId.AccountId, r.NamespaceId.NamespaceId)
}

// CreateNamespaceRequest

func (r *CreateNamespaceRequest) ShardKey() cluster.ShardKey {
	return sharding.ByAccount(r.NamespaceId.AccountId)
}

// CreateSemaphoreRequest

func (r *CreateSemaphoreRequest) ShardKey() cluster.ShardKey {
	return sharding.ByAccountAndNamespace(r.SemaphoreId.AccountId, r.SemaphoreId.NamespaceId)
}

// CreateWaitGroupRequest

func (r *CreateWaitGroupRequest) ShardKey() cluster.ShardKey {
	return sharding.ByAccountAndNamespace(r.WaitGroupId.AccountId, r.WaitGroupId.NamespaceId)
}

// DeleteLockRequest

func (r *DeleteLockRequest) ShardKey() cluster.ShardKey {
	return sharding.ByAccountAndNamespace(r.LockId.AccountId, r.LockId.NamespaceId)
}

// DeleteNamespaceRequest

func (r *DeleteNamespaceRequest) ShardKey() cluster.ShardKey {
	return sharding.ByAccount(r.AccountId)
}

// DeleteSemaphoreRequest

func (r *DeleteSemaphoreRequest) ShardKey() cluster.ShardKey {
	return sharding.ByAccountAndNamespace(r.NamespaceId.AccountId, r.NamespaceId.NamespaceId)
}

// DeleteWaitGroupRequest

func (r *DeleteWaitGroupRequest) ShardKey() cluster.ShardKey {
	return sharding.ByAccountAndNamespace(r.NamespaceId.AccountId, r.NamespaceId.NamespaceId)
}

// GetLockRequest

func (r *GetLockRequest) ShardKey() cluster.ShardKey {
	return sharding.ByAccountAndNamespace(r.LockId.AccountId, r.LockId.NamespaceId)
}

// GetNamespaceRequest

func (r *GetNamespaceRequest) ShardKey() cluster.ShardKey {
	return sharding.ByAccount(r.NamespaceId.AccountId)
}

// GetNamespaceByNameRequest

func (r *GetNamespaceByNameRequest) ShardKey() cluster.ShardKey {
	return sharding.ByAccount(r.AccountId)
}

// GetSemaphoreRequest

func (r *GetSemaphoreRequest) ShardKey() cluster.ShardKey {
	return sharding.ByAccountAndNamespace(r.SemaphoreId.AccountId, r.SemaphoreId.NamespaceId)
}

// GetSemaphoreByNameRequest

func (r *GetSemaphoreByNameRequest) ShardKey() cluster.ShardKey {
	return sharding.ByAccountAndNamespace(r.NamespaceId.AccountId, r.NamespaceId.NamespaceId)
}

// GetWaitGroupRequest

func (r *GetWaitGroupRequest) ShardKey() cluster.ShardKey {
	return sharding.ByAccountAndNamespace(r.WaitGroupId.AccountId, r.WaitGroupId.NamespaceId)
}

// GetWaitGroupByNameRequest

func (r *GetWaitGroupByNameRequest) ShardKey() cluster.ShardKey {
	return sharding.ByAccountAndNamespace(r.NamespaceId.AccountId, r.NamespaceId.NamespaceId)
}

// ListNamespacesRequest

func (r *ListNamespacesRequest) ShardKey() cluster.ShardKey {
	return sharding.ByAccount(r.AccountId)
}

// ListSemaphoresRequest

func (r *ListSemaphoresRequest) ShardKey() cluster.ShardKey {
	return sharding.ByAccountAndNamespace(r.NamespaceId.AccountId, r.NamespaceId.NamespaceId)
}

// ListSemaphoresByLeaseIdRequest

func (r *ListSemaphoresByLeaseIdRequest) ShardKey() cluster.ShardKey {
	return sharding.ByAccountAndNamespace(r.LeaseId.AccountId, r.LeaseId.NamespaceId)
}

// ListSemaphoreLeasesRequest

func (r *ListSemaphoreLeasesRequest) ShardKey() cluster.ShardKey {
	return sharding.ByAccountAndNamespace(r.NamespaceId.AccountId, r.NamespaceId.NamespaceId)
}

// ListSemaphoreLeasesByProcessIdRequest

func (r *ListSemaphoreLeasesByProcessIdRequest) ShardKey() cluster.ShardKey {
	return sharding.ByAccountAndNamespace(r.NamespaceId.AccountId, r.NamespaceId.NamespaceId)
}

// GetSemaphoreLeaseRequest

func (r *GetSemaphoreLeaseRequest) ShardKey() cluster.ShardKey {
	return sharding.ByAccountAndNamespace(r.LeaseId.AccountId, r.LeaseId.NamespaceId)
}

// ListSemaphoreHoldersRequest

func (r *ListSemaphoreHoldersRequest) ShardKey() cluster.ShardKey {
	return sharding.ByAccountAndNamespace(r.NamespaceId.AccountId, r.NamespaceId.NamespaceId)
}

// CreateSemaphoreLeaseRequest

func (r *CreateSemaphoreLeaseRequest) ShardKey() cluster.ShardKey {
	return sharding.ByAccountAndNamespace(r.LeaseId.AccountId, r.LeaseId.NamespaceId)
}

// RevokeSemaphoreLeaseRequest

func (r *RevokeSemaphoreLeaseRequest) ShardKey() cluster.ShardKey {
	return sharding.ByAccountAndNamespace(r.LeaseId.AccountId, r.LeaseId.NamespaceId)
}

// RefreshSemaphoreLeaseRequest

func (r *RefreshSemaphoreLeaseRequest) ShardKey() cluster.ShardKey {
	return sharding.ByAccountAndNamespace(r.LeaseId.AccountId, r.LeaseId.NamespaceId)
}

// ListWaitGroupsRequest

func (r *ListWaitGroupsRequest) ShardKey() cluster.ShardKey {
	return sharding.ByAccountAndNamespace(r.NamespaceId.AccountId, r.NamespaceId.NamespaceId)
}

// ListWaitGroupCompletedJobsRequest

func (r *ListWaitGroupCompletedJobsRequest) ShardKey() cluster.ShardKey {
	return sharding.ByAccountAndNamespace(r.NamespaceId.AccountId, r.NamespaceId.NamespaceId)
}

// UpdateWaitGroupRequest

func (r *UpdateWaitGroupRequest) ShardKey() cluster.ShardKey {
	return sharding.ByAccountAndNamespace(r.NamespaceId.AccountId, r.NamespaceId.NamespaceId)
}

// ReleaseLockRequest

func (r *ReleaseLockRequest) ShardKey() cluster.ShardKey {
	return sharding.ByAccountAndNamespace(r.LockId.AccountId, r.LockId.NamespaceId)
}

// ReleaseSemaphoreRequest

func (r *ReleaseSemaphoreRequest) ShardKey() cluster.ShardKey {
	return sharding.ByAccountAndNamespace(r.NamespaceId.AccountId, r.NamespaceId.NamespaceId)
}

// UpdateNamespaceRequest

func (r *UpdateNamespaceRequest) ShardKey() cluster.ShardKey {
	return sharding.ByAccount(r.AccountId)
}

// UpdateSemaphoreRequest

func (r *UpdateSemaphoreRequest) ShardKey() cluster.ShardKey {
	return sharding.ByAccountAndNamespace(r.NamespaceId.AccountId, r.NamespaceId.NamespaceId)
}

// LocksDeleteNamespaceRequest

func (r *LocksDeleteNamespaceRequest) ShardKey() cluster.ShardKey {
	return sharding.ByAccountAndNamespace(r.NamespaceId.AccountId, r.NamespaceId.NamespaceId)
}

// SemaphoresDeleteNamespaceRequest

func (r *SemaphoresDeleteNamespaceRequest) ShardKey() cluster.ShardKey {
	return sharding.ByAccountAndNamespace(r.NamespaceId.AccountId, r.NamespaceId.NamespaceId)
}

// WaitGroupsDeleteNamespaceRequest

func (r *WaitGroupsDeleteNamespaceRequest) ShardKey() cluster.ShardKey {
	return sharding.ByAccountAndNamespace(r.NamespaceId.AccountId, r.NamespaceId.NamespaceId)
}

// GetBarrierRequest

func (r *GetBarrierRequest) ShardKey() cluster.ShardKey {
	return sharding.ByAccountAndNamespace(r.BarrierId.AccountId, r.BarrierId.NamespaceId)
}

// GetBarrierByNameRequest

func (r *GetBarrierByNameRequest) ShardKey() cluster.ShardKey {
	return sharding.ByAccountAndNamespace(r.NamespaceId.AccountId, r.NamespaceId.NamespaceId)
}

// ListBarriersRequest

func (r *ListBarriersRequest) ShardKey() cluster.ShardKey {
	return sharding.ByAccountAndNamespace(r.NamespaceId.AccountId, r.NamespaceId.NamespaceId)
}

// ListBarrierParticipantsRequest

func (r *ListBarrierParticipantsRequest) ShardKey() cluster.ShardKey {
	return sharding.ByAccountAndNamespace(r.NamespaceId.AccountId, r.NamespaceId.NamespaceId)
}

// CreateBarrierRequest

func (r *CreateBarrierRequest) ShardKey() cluster.ShardKey {
	return sharding.ByAccountAndNamespace(r.BarrierId.AccountId, r.BarrierId.NamespaceId)
}

// UpdateBarrierRequest

func (r *UpdateBarrierRequest) ShardKey() cluster.ShardKey {
	return sharding.ByAccountAndNamespace(r.BarrierId.AccountId, r.BarrierId.NamespaceId)
}

// DeleteBarrierRequest

func (r *DeleteBarrierRequest) ShardKey() cluster.ShardKey {
	return sharding.ByAccountAndNamespace(r.NamespaceId.AccountId, r.NamespaceId.NamespaceId)
}

// ArriveAtBarrierRequest

func (r *ArriveAtBarrierRequest) ShardKey() cluster.ShardKey {
	return sharding.ByAccountAndNamespace(r.NamespaceId.AccountId, r.NamespaceId.NamespaceId)
}

// BarriersDeleteNamespaceRequest

func (r *BarriersDeleteNamespaceRequest) ShardKey() cluster.ShardKey {
	return sharding.ByAccountAndNamespace(r.NamespaceId.AccountId, r.NamespaceId.NamespaceId)
}
