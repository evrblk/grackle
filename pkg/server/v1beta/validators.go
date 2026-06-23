package v1beta

import (
	"encoding/base64"
	"fmt"
	"regexp"

	gracklepb "github.com/evrblk/evrblk-go/grackle/v1beta"
	"github.com/evrblk/grackle/pkg/ids"
	"github.com/evrblk/grackle/pkg/pagination"
)

const (
	maxNamespaceNameLength   = 128
	maxWaitGroupNameLength   = 128
	maxLockNameLength        = 256
	maxSemaphoreNameLength   = 128
	maxBarrierNameLength     = 128
	maxProcessIdLength       = 128
	maxJobIdLength           = 128
	maxDescriptionLength     = 1024
	maxCompleteJobBatchSize  = 50
	maxPaginationTokenLength = 1024
	maxTimeoutSeconds        = 300 // 5 minutes
	maxLeaseIdLength         = 64
	maxLeaseTtlSeconds       = 300 // 5 minutes

	maxMetadataEntries     = 32
	maxMetadataKeyLength   = 128
	maxMetadataValueLength = 256

	nameRegex     = "^[-_0-9a-zA-Z]+$"
	lockNameRegex = "^[-_0-9a-zA-Z]+(/[-_0-9a-zA-Z]+)*$"
)

func ValidateCreateNamespaceRequest(req *gracklepb.CreateNamespaceRequest) error {
	if err := validateNamespaceName(req.Name, "CreateNamespaceRequest.Name"); err != nil {
		return err
	}

	if err := validateDescription(req.Description, "CreateNamespaceRequest.Description"); err != nil {
		return err
	}

	if err := validateMetadata(req.Metadata, "CreateNamespaceRequest.Metadata"); err != nil {
		return err
	}

	return nil
}

func ValidateGetNamespaceRequest(req *gracklepb.GetNamespaceRequest) error {
	if err := validateNamespaceName(req.NamespaceName, "GetNamespaceRequest.NamespaceName"); err != nil {
		return err
	}

	return nil
}

func ValidateUpdateNamespaceRequest(req *gracklepb.UpdateNamespaceRequest) error {
	if err := validateNamespaceName(req.NamespaceName, "UpdateNamespaceRequest.NamespaceName"); err != nil {
		return err
	}

	if err := validateDescription(req.Description, "UpdateNamespaceRequest.Description"); err != nil {
		return err
	}

	if err := validateMetadata(req.Metadata, "UpdateNamespaceRequest.Metadata"); err != nil {
		return err
	}

	return nil
}

func ValidateDeleteNamespaceRequest(req *gracklepb.DeleteNamespaceRequest) error {
	if err := validateNamespaceName(req.NamespaceName, "DeleteNamespaceRequest.NamespaceName"); err != nil {
		return err
	}

	return nil
}

func ValidateListNamespacesRequest(req *gracklepb.ListNamespacesRequest) error {
	if err := validatePaginationToken(req.PaginationToken, "ListNamespacesRequest.PaginationToken"); err != nil {
		return err
	}

	if err := validateLimit(req.Limit, "ListNamespacesRequest.Limit"); err != nil {
		return err
	}

	return nil
}

func ValidateCreateWaitGroupRequest(req *gracklepb.CreateWaitGroupRequest) error {
	if err := validateNamespaceName(req.NamespaceName, "CreateWaitGroupRequest.NamespaceName"); err != nil {
		return err
	}

	if err := validateWaitGroupName(req.WaitGroupName, "CreateWaitGroupRequest.WaitGroupName"); err != nil {
		return err
	}

	if err := validateDescription(req.Description, "CreateWaitGroupRequest.Description"); err != nil {
		return err
	}

	if req.Counter <= 0 {
		return invalid("CreateWaitGroupRequest.Counter", "must be greater than 0")
	}

	if req.DeleteAfterFinishedSeconds < 0 {
		return invalid("CreateWaitGroupRequest.DeleteAfterFinishedSeconds", "must not be negative")
	}

	if err := validateMetadata(req.Metadata, "CreateWaitGroupRequest.Metadata"); err != nil {
		return err
	}

	return nil
}

func ValidateUpdateWaitGroupRequest(req *gracklepb.UpdateWaitGroupRequest) error {
	if err := validateNamespaceName(req.NamespaceName, "UpdateWaitGroupRequest.NamespaceName"); err != nil {
		return err
	}

	if err := validateWaitGroupName(req.WaitGroupName, "UpdateWaitGroupRequest.WaitGroupName"); err != nil {
		return err
	}

	if err := validateDescription(req.Description, "UpdateWaitGroupRequest.Description"); err != nil {
		return err
	}

	if req.Counter <= 0 {
		return invalid("UpdateWaitGroupRequest.Counter", "must be greater than 0")
	}

	if req.DeleteAfterFinishedSeconds < 0 {
		return invalid("UpdateWaitGroupRequest.DeleteAfterFinishedSeconds", "must not be negative")
	}

	if err := validateMetadata(req.Metadata, "UpdateWaitGroupRequest.Metadata"); err != nil {
		return err
	}

	return nil
}

func ValidateGetWaitGroupRequest(req *gracklepb.GetWaitGroupRequest) error {
	if err := validateNamespaceName(req.NamespaceName, "GetWaitGroupRequest.NamespaceName"); err != nil {
		return err
	}

	if err := validateWaitGroupName(req.WaitGroupName, "GetWaitGroupRequest.WaitGroupName"); err != nil {
		return err
	}

	return nil
}

func ValidateWaitForWaitGroupRequest(req *gracklepb.WaitForWaitGroupRequest) error {
	if err := validateNamespaceName(req.NamespaceName, "WaitForWaitGroupRequest.NamespaceName"); err != nil {
		return err
	}

	if err := validateWaitGroupName(req.WaitGroupName, "WaitForWaitGroupRequest.WaitGroupName"); err != nil {
		return err
	}

	if err := validateTimeOutSeconds(req.TimeoutSeconds, "WaitForWaitGroupRequest.TimeoutSeconds"); err != nil {
		return err
	}

	return nil
}

func ValidateDeleteWaitGroupRequest(req *gracklepb.DeleteWaitGroupRequest) error {
	if err := validateNamespaceName(req.NamespaceName, "DeleteWaitGroupRequest.NamespaceName"); err != nil {
		return err
	}

	if err := validateWaitGroupName(req.WaitGroupName, "DeleteWaitGroupRequest.WaitGroupName"); err != nil {
		return err
	}

	return nil
}

func ValidateListWaitGroupsRequest(req *gracklepb.ListWaitGroupsRequest) error {
	if err := validateNamespaceName(req.NamespaceName, "ListWaitGroupsRequest.NamespaceName"); err != nil {
		return err
	}

	if err := validatePaginationToken(req.PaginationToken, "ListWaitGroupsRequest.PaginationToken"); err != nil {
		return err
	}

	if err := validateLimit(req.Limit, "ListWaitGroupsRequest.Limit"); err != nil {
		return err
	}

	return nil
}

func ValidateListWaitGroupCompletedJobsRequest(req *gracklepb.ListWaitGroupCompletedJobsRequest) error {
	if err := validateNamespaceName(req.NamespaceName, "ListWaitGroupCompletedJobsRequest.NamespaceName"); err != nil {
		return err
	}

	if err := validateWaitGroupName(req.WaitGroupName, "ListWaitGroupCompletedJobsRequest.WaitGroupName"); err != nil {
		return err
	}

	if err := validatePaginationToken(req.PaginationToken, "ListWaitGroupCompletedJobsRequest.PaginationToken"); err != nil {
		return err
	}

	if err := validateLimit(req.Limit, "ListWaitGroupCompletedJobsRequest.Limit"); err != nil {
		return err
	}

	return nil
}

func ValidateCompleteJobsFromWaitGroupRequest(req *gracklepb.CompleteJobsFromWaitGroupRequest) error {
	if err := validateNamespaceName(req.NamespaceName, "CompleteJobsFromWaitGroupRequest.NamespaceName"); err != nil {
		return err
	}

	if err := validateWaitGroupName(req.WaitGroupName, "CompleteJobsFromWaitGroupRequest.WaitGroupName"); err != nil {
		return err
	}

	if len(req.Jobs) > maxCompleteJobBatchSize {
		return invalid("CompleteJobsFromWaitGroupRequest.Jobs", fmt.Sprintf("exceeds max batch size (%d)", maxCompleteJobBatchSize))
	}
	for i, job := range req.Jobs {
		if job == nil {
			return invalid(fmt.Sprintf("CompleteJobsFromWaitGroupRequest.Jobs[%d]", i), "must not be nil")
		}
		if err := validateJobId(job.JobId, fmt.Sprintf("CompleteJobsFromWaitGroupRequest.Jobs[%d].JobId", i)); err != nil {
			return err
		}
		if err := validateMetadata(job.Metadata, fmt.Sprintf("CompleteJobsFromWaitGroupRequest.Jobs[%d].Metadata", i)); err != nil {
			return err
		}
	}

	return nil
}

func ValidateListLocksRequest(req *gracklepb.ListLocksRequest) error {
	if err := validateNamespaceName(req.NamespaceName, "ListLocksRequest.NamespaceName"); err != nil {
		return err
	}

	if err := validatePaginationToken(req.PaginationToken, "ListLocksRequest.PaginationToken"); err != nil {
		return err
	}

	if err := validateLimit(req.Limit, "ListLocksRequest.Limit"); err != nil {
		return err
	}

	return nil
}

func ValidateDeleteLockRequest(req *gracklepb.DeleteLockRequest) error {
	if err := validateNamespaceName(req.NamespaceName, "DeleteLockRequest.NamespaceName"); err != nil {
		return err
	}

	if err := validateLockName(req.LockName, "DeleteLockRequest.LockName"); err != nil {
		return err
	}

	return nil
}

func ValidateGetLockRequest(req *gracklepb.GetLockRequest) error {
	if err := validateNamespaceName(req.NamespaceName, "GetLockRequest.NamespaceName"); err != nil {
		return err
	}

	if err := validateLockName(req.LockName, "GetLockRequest.LockName"); err != nil {
		return err
	}

	return nil
}

func ValidateReleaseLockRequest(req *gracklepb.ReleaseLockRequest) error {
	if err := validateNamespaceName(req.NamespaceName, "ReleaseLockRequest.NamespaceName"); err != nil {
		return err
	}

	if err := validateLockName(req.LockName, "ReleaseLockRequest.LockName"); err != nil {
		return err
	}

	if err := validateLeaseId(req.LeaseId, "ReleaseLockRequest.LeaseId"); err != nil {
		return err
	}

	return nil
}

func ValidateAcquireLockRequest(req *gracklepb.AcquireLockRequest) error {
	if err := validateNamespaceName(req.NamespaceName, "AcquireLockRequest.NamespaceName"); err != nil {
		return err
	}

	if err := validateLockName(req.LockName, "AcquireLockRequest.LockName"); err != nil {
		return err
	}

	if err := validateLeaseId(req.LeaseId, "AcquireLockRequest.LeaseId"); err != nil {
		return err
	}

	if err := validateMetadata(req.Metadata, "AcquireLockRequest.Metadata"); err != nil {
		return err
	}

	return nil
}

func ValidateCreateSemaphoreRequest(req *gracklepb.CreateSemaphoreRequest) error {
	if err := validateNamespaceName(req.NamespaceName, "CreateSemaphoreRequest.NamespaceName"); err != nil {
		return err
	}

	if err := validateSemaphoreName(req.SemaphoreName, "CreateSemaphoreRequest.SemaphoreName"); err != nil {
		return err
	}

	if err := validateDescription(req.Description, "CreateSemaphoreRequest.Description"); err != nil {
		return err
	}

	if req.Permits <= 0 {
		return invalid("CreateSemaphoreRequest.Permits", "must be greater than 0")
	}

	if err := validateMetadata(req.Metadata, "CreateSemaphoreRequest.Metadata"); err != nil {
		return err
	}

	return nil
}

func ValidateGetSemaphoreRequest(req *gracklepb.GetSemaphoreRequest) error {
	if err := validateNamespaceName(req.NamespaceName, "GetSemaphoreRequest.NamespaceName"); err != nil {
		return err
	}

	if err := validateSemaphoreName(req.SemaphoreName, "GetSemaphoreRequest.SemaphoreName"); err != nil {
		return err
	}

	return nil
}

func ValidateReleaseSemaphoreRequest(req *gracklepb.ReleaseSemaphoreRequest) error {
	if err := validateNamespaceName(req.NamespaceName, "ReleaseSemaphoreRequest.NamespaceName"); err != nil {
		return err
	}

	if err := validateSemaphoreName(req.SemaphoreName, "ReleaseSemaphoreRequest.SemaphoreName"); err != nil {
		return err
	}

	if err := validateLeaseId(req.LeaseId, "ReleaseSemaphoreRequest.LeaseId"); err != nil {
		return err
	}

	return nil
}

func ValidateUpdateSemaphoreRequest(req *gracklepb.UpdateSemaphoreRequest) error {
	if err := validateNamespaceName(req.NamespaceName, "UpdateSemaphoreRequest.NamespaceName"); err != nil {
		return err
	}

	if err := validateSemaphoreName(req.SemaphoreName, "UpdateSemaphoreRequest.SemaphoreName"); err != nil {
		return err
	}

	if err := validateDescription(req.Description, "UpdateSemaphoreRequest.Description"); err != nil {
		return err
	}

	if req.Permits <= 0 {
		return invalid("UpdateSemaphoreRequest.Permits", "must be greater than 0")
	}

	if err := validateMetadata(req.Metadata, "UpdateSemaphoreRequest.Metadata"); err != nil {
		return err
	}

	return nil
}

func ValidateDeleteSemaphoreRequest(req *gracklepb.DeleteSemaphoreRequest) error {
	if err := validateNamespaceName(req.NamespaceName, "DeleteSemaphoreRequest.NamespaceName"); err != nil {
		return err
	}

	if err := validateSemaphoreName(req.SemaphoreName, "DeleteSemaphoreRequest.SemaphoreName"); err != nil {
		return err
	}

	return nil
}

func ValidateListSemaphoresRequest(req *gracklepb.ListSemaphoresRequest) error {
	if err := validateNamespaceName(req.NamespaceName, "ListSemaphoresRequest.NamespaceName"); err != nil {
		return err
	}

	if err := validatePaginationToken(req.PaginationToken, "ListSemaphoresRequest.PaginationToken"); err != nil {
		return err
	}

	if err := validateLimit(req.Limit, "ListSemaphoresRequest.Limit"); err != nil {
		return err
	}

	return nil
}

func ValidateListSemaphoreHoldersRequest(req *gracklepb.ListSemaphoreHoldersRequest) error {
	if err := validateNamespaceName(req.NamespaceName, "ListSemaphoreHoldersRequest.NamespaceName"); err != nil {
		return err
	}

	if err := validateSemaphoreName(req.SemaphoreName, "ListSemaphoreHoldersRequest.SemaphoreName"); err != nil {
		return err
	}

	if err := validatePaginationToken(req.PaginationToken, "ListSemaphoreHoldersRequest.PaginationToken"); err != nil {
		return err
	}

	if err := validateLimit(req.Limit, "ListSemaphoreHoldersRequest.Limit"); err != nil {
		return err
	}

	return nil
}

func ValidateAcquireSemaphoreRequest(req *gracklepb.AcquireSemaphoreRequest) error {
	if err := validateNamespaceName(req.NamespaceName, "AcquireSemaphoreRequest.NamespaceName"); err != nil {
		return err
	}

	if err := validateSemaphoreName(req.SemaphoreName, "AcquireSemaphoreRequest.SemaphoreName"); err != nil {
		return err
	}

	if err := validateLeaseId(req.LeaseId, "AcquireSemaphoreRequest.LeaseId"); err != nil {
		return err
	}

	if err := validateTimeOutSeconds(req.TimeoutSeconds, "AcquireSemaphoreRequest.TimeoutSeconds"); err != nil {
		return err
	}

	if req.Weight <= 0 {
		return invalid("AcquireSemaphoreRequest.Weight", "must be greater than 0")
	}

	if err := validateMetadata(req.Metadata, "AcquireSemaphoreRequest.Metadata"); err != nil {
		return err
	}

	return nil
}

func ValidateCreateBarrierRequest(req *gracklepb.CreateBarrierRequest) error {
	if err := validateNamespaceName(req.NamespaceName, "CreateBarrierRequest.NamespaceName"); err != nil {
		return err
	}

	if err := validateBarrierName(req.BarrierName, "CreateBarrierRequest.BarrierName"); err != nil {
		return err
	}

	if err := validateDescription(req.Description, "CreateBarrierRequest.Description"); err != nil {
		return err
	}

	if req.ExpectedProcesses <= 0 {
		return invalid("CreateBarrierRequest.ExpectedProcesses", "must be greater than 0")
	}

	if req.DeleteInactiveAfterSeconds <= 0 {
		return invalid("CreateBarrierRequest.DeleteInactiveAfterSeconds", "must be greater than 0")
	}

	if err := validateMetadata(req.Metadata, "CreateBarrierRequest.Metadata"); err != nil {
		return err
	}

	return nil
}

func ValidateListBarriersRequest(req *gracklepb.ListBarriersRequest) error {
	if err := validateNamespaceName(req.NamespaceName, "ListBarriersRequest.NamespaceName"); err != nil {
		return err
	}

	if err := validatePaginationToken(req.PaginationToken, "ListBarriersRequest.PaginationToken"); err != nil {
		return err
	}

	if err := validateLimit(req.Limit, "ListBarriersRequest.Limit"); err != nil {
		return err
	}

	return nil
}

func ValidateGetBarrierRequest(req *gracklepb.GetBarrierRequest) error {
	if err := validateNamespaceName(req.NamespaceName, "GetBarrierRequest.NamespaceName"); err != nil {
		return err
	}

	if err := validateBarrierName(req.BarrierName, "GetBarrierRequest.BarrierName"); err != nil {
		return err
	}

	return nil
}

func ValidateDeleteBarrierRequest(req *gracklepb.DeleteBarrierRequest) error {
	if err := validateNamespaceName(req.NamespaceName, "DeleteBarrierRequest.NamespaceName"); err != nil {
		return err
	}

	if err := validateBarrierName(req.BarrierName, "DeleteBarrierRequest.BarrierName"); err != nil {
		return err
	}

	return nil
}

func ValidateUpdateBarrierRequest(req *gracklepb.UpdateBarrierRequest) error {
	if err := validateNamespaceName(req.NamespaceName, "UpdateBarrierRequest.NamespaceName"); err != nil {
		return err
	}

	if err := validateBarrierName(req.BarrierName, "UpdateBarrierRequest.BarrierName"); err != nil {
		return err
	}

	if err := validateDescription(req.Description, "UpdateBarrierRequest.Description"); err != nil {
		return err
	}

	if req.ExpectedProcesses <= 0 {
		return invalid("UpdateBarrierRequest.ExpectedProcesses", "must be greater than 0")
	}

	if req.DeleteInactiveAfterSeconds <= 0 {
		return invalid("UpdateBarrierRequest.DeleteInactiveAfterSeconds", "must be greater than 0")
	}

	if err := validateMetadata(req.Metadata, "UpdateBarrierRequest.Metadata"); err != nil {
		return err
	}

	return nil
}

func ValidateArriveAtBarrierRequest(req *gracklepb.ArriveAtBarrierRequest) error {
	if err := validateNamespaceName(req.NamespaceName, "ArriveAtBarrierRequest.NamespaceName"); err != nil {
		return err
	}

	if err := validateBarrierName(req.BarrierName, "ArriveAtBarrierRequest.BarrierName"); err != nil {
		return err
	}

	if err := validateProcessId(req.ProcessId, "ArriveAtBarrierRequest.ProcessId"); err != nil {
		return err
	}

	if req.ExpectedGeneration <= 0 {
		return invalid("ArriveAtBarrierRequest.ExpectedGeneration", "must be greater than 0")
	}

	if err := validateMetadata(req.Metadata, "ArriveAtBarrierRequest.Metadata"); err != nil {
		return err
	}

	return nil
}

func ValidateWaitAtBarrierRequest(req *gracklepb.WaitAtBarrierRequest) error {
	if err := validateNamespaceName(req.NamespaceName, "WaitAtBarrierRequest.NamespaceName"); err != nil {
		return err
	}

	if err := validateBarrierName(req.BarrierName, "WaitAtBarrierRequest.BarrierName"); err != nil {
		return err
	}

	if req.ExpectedGeneration <= 0 {
		return invalid("WaitAtBarrierRequest.ExpectedGeneration", "must be greater than 0")
	}

	if err := validateTimeOutSeconds(req.TimeoutSeconds, "WaitAtBarrierRequest.TimeoutSeconds"); err != nil {
		return err
	}

	return nil
}

func ValidateListBarrierParticipantsRequest(req *gracklepb.ListBarrierParticipantsRequest) error {
	if err := validateNamespaceName(req.NamespaceName, "ListBarrierParticipantsRequest.NamespaceName"); err != nil {
		return err
	}

	if err := validateBarrierName(req.BarrierName, "ListBarrierParticipantsRequest.BarrierName"); err != nil {
		return err
	}

	if err := validatePaginationToken(req.PaginationToken, "ListBarrierParticipantsRequest.PaginationToken"); err != nil {
		return err
	}

	if err := validateLimit(req.Limit, "ListBarrierParticipantsRequest.Limit"); err != nil {
		return err
	}

	return nil
}

func ValidateCreateSemaphoreLeaseRequest(req *gracklepb.CreateSemaphoreLeaseRequest) error {
	if err := validateNamespaceName(req.NamespaceName, "CreateSemaphoreLeaseRequest.NamespaceName"); err != nil {
		return err
	}

	if err := validateProcessId(req.ProcessId, "CreateSemaphoreLeaseRequest.ProcessId"); err != nil {
		return err
	}

	if err := validateLeaseTtlSeconds(req.TtlSeconds, "CreateSemaphoreLeaseRequest.TtlSeconds"); err != nil {
		return err
	}

	return nil
}

func ValidateRevokeSemaphoreLeaseRequest(req *gracklepb.RevokeSemaphoreLeaseRequest) error {
	if err := validateNamespaceName(req.NamespaceName, "RevokeSemaphoreLeaseRequest.NamespaceName"); err != nil {
		return err
	}

	if err := validateLeaseId(req.LeaseId, "RevokeSemaphoreLeaseRequest.LeaseId"); err != nil {
		return err
	}

	return nil
}

func ValidateRefreshSemaphoreLeaseRequest(req *gracklepb.RefreshSemaphoreLeaseRequest) error {
	if err := validateNamespaceName(req.NamespaceName, "RefreshSemaphoreLeaseRequest.NamespaceName"); err != nil {
		return err
	}

	if err := validateLeaseId(req.LeaseId, "RefreshSemaphoreLeaseRequest.LeaseId"); err != nil {
		return err
	}

	if err := validateLeaseTtlSeconds(req.TtlSeconds, "RefreshSemaphoreLeaseRequest.TtlSeconds"); err != nil {
		return err
	}

	return nil
}

func ValidateListSemaphoreLeasesRequest(req *gracklepb.ListSemaphoreLeasesRequest) error {
	if err := validateNamespaceName(req.NamespaceName, "ListSemaphoreLeasesRequest.NamespaceName"); err != nil {
		return err
	}

	if err := validatePaginationToken(req.PaginationToken, "ListSemaphoreLeasesRequest.PaginationToken"); err != nil {
		return err
	}

	if err := validateLimit(req.Limit, "ListSemaphoreLeasesRequest.Limit"); err != nil {
		return err
	}

	return nil
}

func ValidateGetSemaphoreLeaseRequest(req *gracklepb.GetSemaphoreLeaseRequest) error {
	if err := validateNamespaceName(req.NamespaceName, "GetSemaphoreLeaseRequest.NamespaceName"); err != nil {
		return err
	}

	if err := validateLeaseId(req.LeaseId, "GetSemaphoreLeaseRequest.LeaseId"); err != nil {
		return err
	}

	return nil
}

func ValidateCreateLockLeaseRequest(req *gracklepb.CreateLockLeaseRequest) error {
	if err := validateNamespaceName(req.NamespaceName, "CreateLockLeaseRequest.NamespaceName"); err != nil {
		return err
	}

	if err := validateProcessId(req.ProcessId, "CreateLockLeaseRequest.ProcessId"); err != nil {
		return err
	}

	if err := validateLeaseTtlSeconds(req.TtlSeconds, "CreateLockLeaseRequest.TtlSeconds"); err != nil {
		return err
	}

	return nil
}

func ValidateRevokeLockLeaseRequest(req *gracklepb.RevokeLockLeaseRequest) error {
	if err := validateNamespaceName(req.NamespaceName, "RevokeLockLeaseRequest.NamespaceName"); err != nil {
		return err
	}

	if err := validateLeaseId(req.LeaseId, "RevokeLockLeaseRequest.LeaseId"); err != nil {
		return err
	}

	return nil
}

func ValidateRefreshLockLeaseRequest(req *gracklepb.RefreshLockLeaseRequest) error {
	if err := validateNamespaceName(req.NamespaceName, "RefreshLockLeaseRequest.NamespaceName"); err != nil {
		return err
	}

	if err := validateLeaseId(req.LeaseId, "RefreshLockLeaseRequest.LeaseId"); err != nil {
		return err
	}

	if err := validateLeaseTtlSeconds(req.TtlSeconds, "RefreshLockLeaseRequest.TtlSeconds"); err != nil {
		return err
	}

	return nil
}

func ValidateListLockLeasesRequest(req *gracklepb.ListLockLeasesRequest) error {
	if err := validateNamespaceName(req.NamespaceName, "ListLockLeasesRequest.NamespaceName"); err != nil {
		return err
	}

	if err := validatePaginationToken(req.PaginationToken, "ListLockLeasesRequest.PaginationToken"); err != nil {
		return err
	}

	if err := validateLimit(req.Limit, "ListLockLeasesRequest.Limit"); err != nil {
		return err
	}

	return nil
}

func ValidateGetLockLeaseRequest(req *gracklepb.GetLockLeaseRequest) error {
	if err := validateNamespaceName(req.NamespaceName, "GetLockLeaseRequest.NamespaceName"); err != nil {
		return err
	}

	if err := validateLeaseId(req.LeaseId, "GetLockLeaseRequest.LeaseId"); err != nil {
		return err
	}

	return nil
}

func validateProcessId(value string, fieldName string) error {
	return validateString(value, 1, maxProcessIdLength, nameRegex, fieldName)
}

func validateJobId(value string, fieldName string) error {
	return validateString(value, 1, maxJobIdLength, nameRegex, fieldName)
}

func validateNamespaceName(value string, fieldName string) error {
	return validateString(value, 1, maxNamespaceNameLength, nameRegex, fieldName)
}

func validateSemaphoreName(value string, fieldName string) error {
	return validateString(value, 1, maxSemaphoreNameLength, nameRegex, fieldName)
}

func validateBarrierName(value string, fieldName string) error {
	return validateString(value, 1, maxBarrierNameLength, nameRegex, fieldName)
}

func validateLockName(value string, fieldName string) error {
	return validateString(value, 1, maxLockNameLength, lockNameRegex, fieldName)
}

func validateWaitGroupName(value string, fieldName string) error {
	return validateString(value, 1, maxWaitGroupNameLength, nameRegex, fieldName)
}

func validateLeaseId(value string, fieldName string) error {
	if len(value) > maxLeaseIdLength || len(value) <= 0 {
		return invalid(fieldName, fmt.Sprintf("length must be between %d and %d characters", 1, maxLeaseIdLength))
	}

	_, err := ids.DecodeLeaseId(value)
	if err != nil {
		return invalid(fieldName, "must be a valid lease ID")
	}

	return nil
}

// validateMetadata enforces the limits on a user-supplied metadata map: the
// number of entries, and the length of each key and value. An empty or nil
// map is always valid.
func validateMetadata(metadata map[string]string, fieldName string) error {
	if len(metadata) > maxMetadataEntries {
		return invalid(fieldName, fmt.Sprintf("exceeds max number of entries (%d)", maxMetadataEntries))
	}

	for key, value := range metadata {
		if len(key) == 0 || len(key) > maxMetadataKeyLength {
			return invalid(fieldName, fmt.Sprintf("key length must be between 1 and %d characters", maxMetadataKeyLength))
		}
		if len(value) > maxMetadataValueLength {
			return invalid(fieldName, fmt.Sprintf("value for key %q exceeds max length (%d)", key, maxMetadataValueLength))
		}
	}

	return nil
}

func validateDescription(value string, fieldName string) error {
	if len(value) > maxDescriptionLength {
		return invalid(fieldName, fmt.Sprintf("exceeds max length (%d)", maxDescriptionLength))
	}

	return nil
}

func validatePaginationToken(value string, fieldName string) error {
	if len(value) > maxPaginationTokenLength {
		return invalid(fieldName, fmt.Sprintf("exceeds max length (%d)", maxPaginationTokenLength))
	}

	_, err := base64.StdEncoding.DecodeString(value)
	if err != nil {
		return invalid(fieldName, "must be a valid base64 string")
	}

	return nil
}

func validateLeaseTtlSeconds(value uint64, fieldName string) error {
	if value <= 0 || value > maxLeaseTtlSeconds {
		return invalid(fieldName, fmt.Sprintf("must be between 1 and %d", maxLeaseTtlSeconds))
	}

	return nil
}

func validateTimeOutSeconds(value int32, fieldName string) error {
	if value <= 0 || value > maxTimeoutSeconds {
		return invalid(fieldName, fmt.Sprintf("must be between 1 and %d", maxTimeoutSeconds))
	}

	return nil
}

func validateLimit(value int32, fieldName string) error {
	if value < 0 || value > pagination.MaxPaginationLimit {
		return invalid(fieldName, fmt.Sprintf("must be between 0 and %d", pagination.MaxPaginationLimit))
	}

	return nil
}

func validateString(value string, minLength int, maxLength int, regex string, fieldName string) error {
	if len(value) > maxLength || len(value) < minLength {
		return invalid(fieldName, fmt.Sprintf("length must be between %d and %d characters", minLength, maxLength))
	}

	if m, err := regexp.MatchString(regex, value); err != nil || !m {
		return invalid(fieldName, "must match regex pattern "+regex)
	}

	return nil
}

func invalid(fieldName string, details string) error {
	if details == "" {
		return fmt.Errorf("invalid %s", fieldName)
	} else {
		return fmt.Errorf("invalid %s: %s", fieldName, details)
	}
}
