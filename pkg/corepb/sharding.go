package corepb

import (
	"github.com/evrblk/grackle/pkg/sharding"
)

// ListLocksRequest

func (r *ListLocksRequest) ShardKey() []byte {
	return sharding.ByAccountAndNamespace(r.NamespaceId.AccountId, r.NamespaceId.NamespaceId)
}

// ListLocksByLeaseIdRequest

func (r *ListLocksByLeaseIdRequest) ShardKey() []byte {
	return sharding.ByAccountAndNamespace(r.LeaseId.AccountId, r.LeaseId.NamespaceId)
}

// GetLockLeaseRequest

func (r *GetLockLeaseRequest) ShardKey() []byte {
	return sharding.ByAccountAndNamespace(r.LeaseId.AccountId, r.LeaseId.NamespaceId)
}

// ListLockLeasesRequest

func (r *ListLockLeasesRequest) ShardKey() []byte {
	return sharding.ByAccountAndNamespace(r.NamespaceId.AccountId, r.NamespaceId.NamespaceId)
}

// ListLockLeasesByProcessIdRequest

func (r *ListLockLeasesByProcessIdRequest) ShardKey() []byte {
	return sharding.ByAccountAndNamespace(r.NamespaceId.AccountId, r.NamespaceId.NamespaceId)
}

// CreateLockLeaseRequest

func (r *CreateLockLeaseRequest) ShardKey() []byte {
	return sharding.ByAccountAndNamespace(r.LeaseId.AccountId, r.LeaseId.NamespaceId)
}

// RefreshLockLeaseRequest

func (r *RefreshLockLeaseRequest) ShardKey() []byte {
	return sharding.ByAccountAndNamespace(r.LeaseId.AccountId, r.LeaseId.NamespaceId)
}

// RevokeLockLeaseRequest

func (r *RevokeLockLeaseRequest) ShardKey() []byte {
	return sharding.ByAccountAndNamespace(r.LeaseId.AccountId, r.LeaseId.NamespaceId)
}

// AcquireLockRequest

func (r *AcquireLockRequest) ShardKey() []byte {
	return sharding.ByAccountAndNamespace(r.LockId.AccountId, r.LockId.NamespaceId)
}

// AcquireSemaphoreRequest

func (r *AcquireSemaphoreRequest) ShardKey() []byte {
	return sharding.ByAccountAndNamespace(r.NamespaceId.AccountId, r.NamespaceId.NamespaceId)
}

// AddJobsToWaitGroupRequest

func (r *AddJobsToWaitGroupRequest) ShardKey() []byte {
	return sharding.ByAccountAndNamespace(r.NamespaceId.AccountId, r.NamespaceId.NamespaceId)
}

// CompleteJobsFromWaitGroupRequest

func (r *CompleteJobsFromWaitGroupRequest) ShardKey() []byte {
	return sharding.ByAccountAndNamespace(r.NamespaceId.AccountId, r.NamespaceId.NamespaceId)
}

// CreateNamespaceRequest

func (r *CreateNamespaceRequest) ShardKey() []byte {
	return sharding.ByAccount(r.NamespaceId.AccountId)
}

// CreateSemaphoreRequest

func (r *CreateSemaphoreRequest) ShardKey() []byte {
	return sharding.ByAccountAndNamespace(r.SemaphoreId.AccountId, r.SemaphoreId.NamespaceId)
}

// CreateWaitGroupRequest

func (r *CreateWaitGroupRequest) ShardKey() []byte {
	return sharding.ByAccountAndNamespace(r.WaitGroupId.AccountId, r.WaitGroupId.NamespaceId)
}

// DeleteLockRequest

func (r *DeleteLockRequest) ShardKey() []byte {
	return sharding.ByAccountAndNamespace(r.LockId.AccountId, r.LockId.NamespaceId)
}

// DeleteNamespaceRequest

func (r *DeleteNamespaceRequest) ShardKey() []byte {
	return sharding.ByAccount(r.NamespaceId.AccountId)
}

// DeleteSemaphoreRequest

func (r *DeleteSemaphoreRequest) ShardKey() []byte {
	return sharding.ByAccountAndNamespace(r.NamespaceId.AccountId, r.NamespaceId.NamespaceId)
}

// DeleteWaitGroupRequest

func (r *DeleteWaitGroupRequest) ShardKey() []byte {
	return sharding.ByAccountAndNamespace(r.NamespaceId.AccountId, r.NamespaceId.NamespaceId)
}

// GetLockRequest

func (r *GetLockRequest) ShardKey() []byte {
	return sharding.ByAccountAndNamespace(r.LockId.AccountId, r.LockId.NamespaceId)
}

// GetNamespaceRequest

func (r *GetNamespaceRequest) ShardKey() []byte {
	return sharding.ByAccount(r.NamespaceId.AccountId)
}

// GetNamespaceByNameRequest

func (r *GetNamespaceByNameRequest) ShardKey() []byte {
	return sharding.ByAccount(r.AccountId)
}

// GetSemaphoreRequest

func (r *GetSemaphoreRequest) ShardKey() []byte {
	return sharding.ByAccountAndNamespace(r.SemaphoreId.AccountId, r.SemaphoreId.NamespaceId)
}

// GetSemaphoreByNameRequest

func (r *GetSemaphoreByNameRequest) ShardKey() []byte {
	return sharding.ByAccountAndNamespace(r.NamespaceId.AccountId, r.NamespaceId.NamespaceId)
}

// GetWaitGroupRequest

func (r *GetWaitGroupRequest) ShardKey() []byte {
	return sharding.ByAccountAndNamespace(r.WaitGroupId.AccountId, r.WaitGroupId.NamespaceId)
}

// GetWaitGroupByNameRequest

func (r *GetWaitGroupByNameRequest) ShardKey() []byte {
	return sharding.ByAccountAndNamespace(r.NamespaceId.AccountId, r.NamespaceId.NamespaceId)
}

// ListNamespacesRequest

func (r *ListNamespacesRequest) ShardKey() []byte {
	return sharding.ByAccount(r.AccountId)
}

// ListSemaphoresRequest

func (r *ListSemaphoresRequest) ShardKey() []byte {
	return sharding.ByAccountAndNamespace(r.NamespaceId.AccountId, r.NamespaceId.NamespaceId)
}

// ListSemaphoresByLeaseIdRequest

func (r *ListSemaphoresByLeaseIdRequest) ShardKey() []byte {
	return sharding.ByAccountAndNamespace(r.LeaseId.AccountId, r.LeaseId.NamespaceId)
}

// ListSemaphoreLeasesRequest

func (r *ListSemaphoreLeasesRequest) ShardKey() []byte {
	return sharding.ByAccountAndNamespace(r.NamespaceId.AccountId, r.NamespaceId.NamespaceId)
}

// ListSemaphoreLeasesByProcessIdRequest

func (r *ListSemaphoreLeasesByProcessIdRequest) ShardKey() []byte {
	return sharding.ByAccountAndNamespace(r.NamespaceId.AccountId, r.NamespaceId.NamespaceId)
}

// GetSemaphoreLeaseRequest

func (r *GetSemaphoreLeaseRequest) ShardKey() []byte {
	return sharding.ByAccountAndNamespace(r.LeaseId.AccountId, r.LeaseId.NamespaceId)
}

// ListSemaphoreHoldersRequest

func (r *ListSemaphoreHoldersRequest) ShardKey() []byte {
	return sharding.ByAccountAndNamespace(r.NamespaceId.AccountId, r.NamespaceId.NamespaceId)
}

// CreateSemaphoreLeaseRequest

func (r *CreateSemaphoreLeaseRequest) ShardKey() []byte {
	return sharding.ByAccountAndNamespace(r.LeaseId.AccountId, r.LeaseId.NamespaceId)
}

// RevokeSemaphoreLeaseRequest

func (r *RevokeSemaphoreLeaseRequest) ShardKey() []byte {
	return sharding.ByAccountAndNamespace(r.LeaseId.AccountId, r.LeaseId.NamespaceId)
}

// RefreshSemaphoreLeaseRequest

func (r *RefreshSemaphoreLeaseRequest) ShardKey() []byte {
	return sharding.ByAccountAndNamespace(r.LeaseId.AccountId, r.LeaseId.NamespaceId)
}

// ListWaitGroupsRequest

func (r *ListWaitGroupsRequest) ShardKey() []byte {
	return sharding.ByAccountAndNamespace(r.NamespaceId.AccountId, r.NamespaceId.NamespaceId)
}

// ListWaitGroupJobsRequest

func (r *ListWaitGroupJobsRequest) ShardKey() []byte {
	return sharding.ByAccountAndNamespace(r.NamespaceId.AccountId, r.NamespaceId.NamespaceId)
}

// UpdateWaitGroupRequest

func (r *UpdateWaitGroupRequest) ShardKey() []byte {
	return sharding.ByAccountAndNamespace(r.WaitGroupId.AccountId, r.WaitGroupId.NamespaceId)
}

// ReleaseLockRequest

func (r *ReleaseLockRequest) ShardKey() []byte {
	return sharding.ByAccountAndNamespace(r.LockId.AccountId, r.LockId.NamespaceId)
}

// ReleaseSemaphoreRequest

func (r *ReleaseSemaphoreRequest) ShardKey() []byte {
	return sharding.ByAccountAndNamespace(r.NamespaceId.AccountId, r.NamespaceId.NamespaceId)
}

// UpdateNamespaceRequest

func (r *UpdateNamespaceRequest) ShardKey() []byte {
	return sharding.ByAccount(r.NamespaceId.AccountId)
}

// UpdateSemaphoreRequest

func (r *UpdateSemaphoreRequest) ShardKey() []byte {
	return sharding.ByAccountAndNamespace(r.NamespaceId.AccountId, r.NamespaceId.NamespaceId)
}

// LocksDeleteNamespaceRequest

func (r *LocksDeleteNamespaceRequest) ShardKey() []byte {
	return sharding.ByAccountAndNamespace(r.NamespaceId.AccountId, r.NamespaceId.NamespaceId)
}

// SemaphoresDeleteNamespaceRequest

func (r *SemaphoresDeleteNamespaceRequest) ShardKey() []byte {
	return sharding.ByAccountAndNamespace(r.NamespaceId.AccountId, r.NamespaceId.NamespaceId)
}

// WaitGroupsDeleteNamespaceRequest

func (r *WaitGroupsDeleteNamespaceRequest) ShardKey() []byte {
	return sharding.ByAccountAndNamespace(r.NamespaceId.AccountId, r.NamespaceId.NamespaceId)
}

// GetBarrierRequest

func (r *GetBarrierRequest) ShardKey() []byte {
	return sharding.ByAccountAndNamespace(r.BarrierId.AccountId, r.BarrierId.NamespaceId)
}

// GetBarrierByNameRequest

func (r *GetBarrierByNameRequest) ShardKey() []byte {
	return sharding.ByAccountAndNamespace(r.NamespaceId.AccountId, r.NamespaceId.NamespaceId)
}

// ListBarriersRequest

func (r *ListBarriersRequest) ShardKey() []byte {
	return sharding.ByAccountAndNamespace(r.NamespaceId.AccountId, r.NamespaceId.NamespaceId)
}

// ListBarrierParticipantsRequest

func (r *ListBarrierParticipantsRequest) ShardKey() []byte {
	return sharding.ByAccountAndNamespace(r.NamespaceId.AccountId, r.NamespaceId.NamespaceId)
}

// CreateBarrierRequest

func (r *CreateBarrierRequest) ShardKey() []byte {
	return sharding.ByAccountAndNamespace(r.BarrierId.AccountId, r.BarrierId.NamespaceId)
}

// UpdateBarrierRequest

func (r *UpdateBarrierRequest) ShardKey() []byte {
	return sharding.ByAccountAndNamespace(r.BarrierId.AccountId, r.BarrierId.NamespaceId)
}

// DeleteBarrierRequest

func (r *DeleteBarrierRequest) ShardKey() []byte {
	return sharding.ByAccountAndNamespace(r.NamespaceId.AccountId, r.NamespaceId.NamespaceId)
}

// ArriveAtBarrierRequest

func (r *ArriveAtBarrierRequest) ShardKey() []byte {
	return sharding.ByAccountAndNamespace(r.NamespaceId.AccountId, r.NamespaceId.NamespaceId)
}

// WaitAtBarrierRequest

func (r *WaitAtBarrierRequest) ShardKey() []byte {
	return sharding.ByAccountAndNamespace(r.NamespaceId.AccountId, r.NamespaceId.NamespaceId)
}

// BarriersDeleteNamespaceRequest

func (r *BarriersDeleteNamespaceRequest) ShardKey() []byte {
	return sharding.ByAccountAndNamespace(r.NamespaceId.AccountId, r.NamespaceId.NamespaceId)
}
