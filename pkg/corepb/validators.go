//lint:file-ignore ST1005 field names are capitalized

package corepb

import "fmt"

// Bounds mirror the front-end validators in pkg/server/v1beta/validators.go.
// They are duplicated rather than imported (server/v1beta depends on corepb,
// not the other way around) and are deliberately limited to fields the
// cores actually dereference or use in arithmetic.
const (
	// maxLeaseTtlSeconds is an overflow guard, not the product's TTL policy:
	// pkg/server/v1beta/validators.go caps TtlSeconds at 300s (5 minutes) as a
	// business decision, but Core itself (and its own test suite, which
	// exercises leases up to several hours long) has no such requirement —
	// only that req.Now + TtlSeconds*1e9 not overflow int64 nanoseconds.
	maxLeaseTtlSeconds = 10 * 365 * 86400 // 10 years

	minWaitGroupAutoDeletionTime = 60 // 1 minute
	minBarrierAutoDeletionTime   = 60 // 1 minute

	maxNumberOfCompleteJobsEntries = 50
)

// AcquireLockRequest

func (r *AcquireLockRequest) Validate() error {
	return validateLockId(r.LockId)
}

// AcquireSemaphoreRequest

func (r *AcquireSemaphoreRequest) Validate() error {
	if err := validateNamespaceId(r.NamespaceId); err != nil {
		return err
	}

	if r.Weight <= 0 {
		return fmt.Errorf("Weight must be positive")
	}

	return nil
}

// ArriveAtBarrierRequest

func (r *ArriveAtBarrierRequest) Validate() error {
	return validateNamespaceId(r.NamespaceId)
}

// BarriersDeleteNamespaceRequest

func (r *BarriersDeleteNamespaceRequest) Validate() error {
	return validateNamespaceId(r.NamespaceId)
}

// CompleteJobsFromWaitGroupRequest

func (r *CompleteJobsFromWaitGroupRequest) Validate() error {
	if err := validateNamespaceId(r.NamespaceId); err != nil {
		return err
	}

	if len(r.Jobs) > maxNumberOfCompleteJobsEntries {
		return fmt.Errorf("Jobs exceeds max number of entries (%d)", maxNumberOfCompleteJobsEntries)
	}

	return nil
}

// CreateBarrierRequest

func (r *CreateBarrierRequest) Validate() error {
	if err := validateBarrierId(r.BarrierId); err != nil {
		return err
	}

	if r.ExpectedProcesses <= 0 {
		return fmt.Errorf("ExpectedProcesses must be positive")
	}

	if r.DeleteInactiveAfterSeconds < minBarrierAutoDeletionTime {
		return fmt.Errorf("DeleteInactiveAfterSeconds must be at least %d seconds", minBarrierAutoDeletionTime)
	}

	return nil
}

// CreateLockLeaseRequest

func (r *CreateLockLeaseRequest) Validate() error {
	if err := validateLeaseId(r.LeaseId); err != nil {
		return err
	}

	return validateLeaseTtlSeconds(r.TtlSeconds)
}

// CreateNamespaceRequest

func (r *CreateNamespaceRequest) Validate() error {
	return validateNamespaceId(r.NamespaceId)
}

// CreateSemaphoreRequest

func (r *CreateSemaphoreRequest) Validate() error {
	if err := validateSemaphoreId(r.SemaphoreId); err != nil {
		return err
	}

	if r.Permits <= 0 {
		return fmt.Errorf("Permits must be positive")
	}

	return nil
}

// CreateSemaphoreLeaseRequest

func (r *CreateSemaphoreLeaseRequest) Validate() error {
	if err := validateLeaseId(r.LeaseId); err != nil {
		return err
	}

	return validateLeaseTtlSeconds(r.TtlSeconds)
}

// CreateWaitGroupRequest

func (r *CreateWaitGroupRequest) Validate() error {
	if err := validateWaitGroupId(r.WaitGroupId); err != nil {
		return err
	}

	if r.Counter <= 0 {
		return fmt.Errorf("Counter must be positive")
	}

	if r.ExpiresAt <= 0 {
		return fmt.Errorf("ExpiresAt must be positive")
	}

	if r.DeleteAfterFinishedSeconds < minWaitGroupAutoDeletionTime {
		return fmt.Errorf("DeleteAfterFinishedSeconds must be at least %d seconds", minWaitGroupAutoDeletionTime)
	}

	return nil
}

// DeleteBarrierRequest

func (r *DeleteBarrierRequest) Validate() error {
	return validateNamespaceId(r.NamespaceId)
}

// DeleteLockRequest

func (r *DeleteLockRequest) Validate() error {
	return validateLockId(r.LockId)
}

// DeleteNamespaceRequest

// AccountId/NamespaceName are resolved by a lookup in Core; there is no
// nested id here for a malformed value to corrupt.
func (r *DeleteNamespaceRequest) Validate() error {
	return nil
}

// DeleteSemaphoreRequest

func (r *DeleteSemaphoreRequest) Validate() error {
	return validateNamespaceId(r.NamespaceId)
}

// DeleteWaitGroupRequest

func (r *DeleteWaitGroupRequest) Validate() error {
	return validateNamespaceId(r.NamespaceId)
}

// GetBarrierRequest

func (r *GetBarrierRequest) Validate() error {
	return validateBarrierId(r.BarrierId)
}

// GetBarrierByNameRequest

func (r *GetBarrierByNameRequest) Validate() error {
	return validateNamespaceId(r.NamespaceId)
}

// GetLockRequest

func (r *GetLockRequest) Validate() error {
	return validateLockId(r.LockId)
}

// GetLockLeaseRequest

func (r *GetLockLeaseRequest) Validate() error {
	return validateLeaseId(r.LeaseId)
}

// GetNamespaceRequest

func (r *GetNamespaceRequest) Validate() error {
	return validateNamespaceId(r.NamespaceId)
}

// GetNamespaceByNameRequest

func (r *GetNamespaceByNameRequest) Validate() error {
	return nil
}

// GetSemaphoreRequest

func (r *GetSemaphoreRequest) Validate() error {
	return validateSemaphoreId(r.SemaphoreId)
}

// GetSemaphoreByNameRequest

func (r *GetSemaphoreByNameRequest) Validate() error {
	return validateNamespaceId(r.NamespaceId)
}

// GetSemaphoreLeaseRequest

func (r *GetSemaphoreLeaseRequest) Validate() error {
	return validateLeaseId(r.LeaseId)
}

// GetWaitGroupRequest

func (r *GetWaitGroupRequest) Validate() error {
	return validateWaitGroupId(r.WaitGroupId)
}

// GetWaitGroupByNameRequest

func (r *GetWaitGroupByNameRequest) Validate() error {
	return validateNamespaceId(r.NamespaceId)
}

// ListBarrierParticipantsRequest

func (r *ListBarrierParticipantsRequest) Validate() error {
	return validateNamespaceId(r.NamespaceId)
}

// ListBarriersRequest

func (r *ListBarriersRequest) Validate() error {
	return validateNamespaceId(r.NamespaceId)
}

// ListLockLeasesRequest

func (r *ListLockLeasesRequest) Validate() error {
	return validateNamespaceId(r.NamespaceId)
}

// ListLockLeasesByProcessIdRequest

func (r *ListLockLeasesByProcessIdRequest) Validate() error {
	return validateNamespaceId(r.NamespaceId)
}

// ListLocksRequest

func (r *ListLocksRequest) Validate() error {
	return validateNamespaceId(r.NamespaceId)
}

// ListLocksByLeaseIdRequest

func (r *ListLocksByLeaseIdRequest) Validate() error {
	return validateLeaseId(r.LeaseId)
}

// ListNamespacesRequest

func (r *ListNamespacesRequest) Validate() error {
	return nil
}

// ListSemaphoreHoldersRequest

func (r *ListSemaphoreHoldersRequest) Validate() error {
	return validateNamespaceId(r.NamespaceId)
}

// ListSemaphoreLeasesRequest

func (r *ListSemaphoreLeasesRequest) Validate() error {
	return validateNamespaceId(r.NamespaceId)
}

// ListSemaphoreLeasesByProcessIdRequest

func (r *ListSemaphoreLeasesByProcessIdRequest) Validate() error {
	return validateNamespaceId(r.NamespaceId)
}

// ListSemaphoresRequest

func (r *ListSemaphoresRequest) Validate() error {
	return validateNamespaceId(r.NamespaceId)
}

// ListSemaphoresByLeaseIdRequest

func (r *ListSemaphoresByLeaseIdRequest) Validate() error {
	return validateLeaseId(r.LeaseId)
}

// ListWaitGroupCompletedJobsRequest

func (r *ListWaitGroupCompletedJobsRequest) Validate() error {
	return validateNamespaceId(r.NamespaceId)
}

// ListWaitGroupsRequest

func (r *ListWaitGroupsRequest) Validate() error {
	return validateNamespaceId(r.NamespaceId)
}

// LocksDeleteNamespaceRequest

func (r *LocksDeleteNamespaceRequest) Validate() error {
	return validateNamespaceId(r.NamespaceId)
}

// RefreshLockLeaseRequest

func (r *RefreshLockLeaseRequest) Validate() error {
	if err := validateLeaseId(r.LeaseId); err != nil {
		return err
	}

	return validateLeaseTtlSeconds(r.TtlSeconds)
}

// RefreshSemaphoreLeaseRequest

func (r *RefreshSemaphoreLeaseRequest) Validate() error {
	if err := validateLeaseId(r.LeaseId); err != nil {
		return err
	}

	return validateLeaseTtlSeconds(r.TtlSeconds)
}

// ReleaseLockRequest

func (r *ReleaseLockRequest) Validate() error {
	return validateLockId(r.LockId)
}

// ReleaseSemaphoreRequest

func (r *ReleaseSemaphoreRequest) Validate() error {
	return validateNamespaceId(r.NamespaceId)
}

// RevokeLockLeaseRequest

func (r *RevokeLockLeaseRequest) Validate() error {
	return validateLeaseId(r.LeaseId)
}

// RevokeSemaphoreLeaseRequest

func (r *RevokeSemaphoreLeaseRequest) Validate() error {
	return validateLeaseId(r.LeaseId)
}

// RunBarriersGarbageCollectionRequest

func (r *RunBarriersGarbageCollectionRequest) Validate() error {
	return nil
}

// RunLocksGarbageCollectionRequest

func (r *RunLocksGarbageCollectionRequest) Validate() error {
	return nil
}

// RunSemaphoresGarbageCollectionRequest

func (r *RunSemaphoresGarbageCollectionRequest) Validate() error {
	return nil
}

// RunWaitGroupsGarbageCollectionRequest

func (r *RunWaitGroupsGarbageCollectionRequest) Validate() error {
	return nil
}

// SemaphoresDeleteNamespaceRequest

func (r *SemaphoresDeleteNamespaceRequest) Validate() error {
	return validateNamespaceId(r.NamespaceId)
}

// UpdateBarrierRequest

func (r *UpdateBarrierRequest) Validate() error {
	if err := validateBarrierId(r.BarrierId); err != nil {
		return err
	}

	if r.ExpectedProcesses <= 0 {
		return fmt.Errorf("ExpectedProcesses must be positive")
	}

	if r.DeleteInactiveAfterSeconds < minBarrierAutoDeletionTime {
		return fmt.Errorf("DeleteInactiveAfterSeconds must be at least %d seconds", minBarrierAutoDeletionTime)
	}

	return nil
}

// UpdateNamespaceRequest

func (r *UpdateNamespaceRequest) Validate() error {
	return nil
}

// UpdateSemaphoreRequest

func (r *UpdateSemaphoreRequest) Validate() error {
	if err := validateNamespaceId(r.NamespaceId); err != nil {
		return err
	}

	if r.Permits <= 0 {
		return fmt.Errorf("Permits must be positive")
	}

	return nil
}

// UpdateWaitGroupRequest

func (r *UpdateWaitGroupRequest) Validate() error {
	if err := validateNamespaceId(r.NamespaceId); err != nil {
		return err
	}

	if r.ExpiresAt <= 0 {
		return fmt.Errorf("ExpiresAt must be positive")
	}

	// Same deletion-index risk as CreateWaitGroupRequest.DeleteAfterFinishedSeconds.
	if r.DeleteAfterFinishedSeconds < minWaitGroupAutoDeletionTime {
		return fmt.Errorf("DeleteAfterFinishedSeconds must be at least %d seconds", minWaitGroupAutoDeletionTime)
	}

	return nil
}

// WaitGroupsDeleteNamespaceRequest

func (r *WaitGroupsDeleteNamespaceRequest) Validate() error {
	return validateNamespaceId(r.NamespaceId)
}

// validateNamespaceId, validateLockId, validateLeaseId, validateSemaphoreId,
// validateWaitGroupId, and validateBarrierId require the id to be set and
// every field but AccountId to be positive. AccountId is legitimately zero
// under the single-tenant OSS id encoder (pkg/ids/single_tenant.go always
// decodes AccountId: 0), but NamespaceId/LeaseId/SemaphoreId/WaitGroupId/
// BarrierId are always assigned by their corresponding Create call — zero
// here can only mean a field that was never set, not a real id.

func validateNamespaceId(id *NamespaceId) error {
	if id == nil {
		return fmt.Errorf("NamespaceId must be set")
	}

	if id.NamespaceId == 0 {
		return fmt.Errorf("NamespaceId.NamespaceId must be positive")
	}

	return nil
}

// validateLockId only checks the components every AcquireLock/ReleaseLock/
// GetLock/DeleteLock dereferences unconditionally to build the storage key.
// LockName is the hierarchical path component; an empty or unusual value is
// a legal, if pointless, key — never dereferenced unsafely.
func validateLockId(id *LockId) error {
	if id == nil {
		return fmt.Errorf("LockId must be set")
	}

	if id.NamespaceId == 0 {
		return fmt.Errorf("LockId.NamespaceId must be positive")
	}

	return nil
}

func validateLeaseId(id *LeaseId) error {
	if id == nil {
		return fmt.Errorf("LeaseId must be set")
	}

	if id.NamespaceId == 0 {
		return fmt.Errorf("LeaseId.NamespaceId must be positive")
	}

	if id.LeaseId == 0 {
		return fmt.Errorf("LeaseId.LeaseId must be positive")
	}

	return nil
}

func validateSemaphoreId(id *SemaphoreId) error {
	if id == nil {
		return fmt.Errorf("SemaphoreId must be set")
	}

	if id.NamespaceId == 0 {
		return fmt.Errorf("SemaphoreId.NamespaceId must be positive")
	}

	if id.SemaphoreId == 0 {
		return fmt.Errorf("SemaphoreId.SemaphoreId must be positive")
	}

	return nil
}

func validateWaitGroupId(id *WaitGroupId) error {
	if id == nil {
		return fmt.Errorf("WaitGroupId must be set")
	}

	if id.NamespaceId == 0 {
		return fmt.Errorf("WaitGroupId.NamespaceId must be positive")
	}

	if id.WaitGroupId == 0 {
		return fmt.Errorf("WaitGroupId.WaitGroupId must be positive")
	}

	return nil
}

func validateBarrierId(id *BarrierId) error {
	if id == nil {
		return fmt.Errorf("BarrierId must be set")
	}

	if id.NamespaceId == 0 {
		return fmt.Errorf("BarrierId.NamespaceId must be positive")
	}

	if id.BarrierId == 0 {
		return fmt.Errorf("BarrierId.BarrierId must be positive")
	}

	return nil
}

// validateLeaseTtlSeconds bounds a lease's requested TTL: Create/Refresh
// {Lock,Semaphore}Lease compute expiresAt := req.Now + TtlSeconds*1e9 with no
// clamping downstream, so a non-positive value would silently carry through
// as a wrong, deterministically replicated expiration rather than a caught
// error, and an astronomically large one could overflow the int64 result.
func validateLeaseTtlSeconds(value int64) error {
	if value <= 0 || value > maxLeaseTtlSeconds {
		return fmt.Errorf("TtlSeconds must be between 1 and %d seconds", maxLeaseTtlSeconds)
	}

	return nil
}
