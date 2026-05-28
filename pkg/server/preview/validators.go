package preview

import (
	"encoding/base64"
	"errors"
	"fmt"
	"regexp"

	gracklepb "github.com/evrblk/evrblk-go/grackle/preview"
)

const (
	maxNamespaceNameLength   = 128
	maxWaitGroupNameLength   = 128
	maxLockNameLength        = 256
	maxSemaphoreNameLength   = 128
	maxBarrierNameLength     = 128
	maxProcessIdLength       = 128
	maxDescriptionLength     = 1024
	maxCompleteJobBatchSize  = 50
	maxPaginationTokenLength = 1024
	maxPaginationLimit       = 100
	maxTimeoutSeconds        = 300
	maxLeaseIdLength         = 64

	nameRegex     = "^[-_0-9a-zA-Z]*$"
	lockNameRegex = "^[-_0-9a-zA-Z//]*$"
	leaseIdRegex  = "^ls_[0-9a-zA-Z]+$"
)

func ValidateCreateNamespaceRequest(request *gracklepb.CreateNamespaceRequest) error {
	if err := validateNamespaceName(request.Name, "CreateNamespaceRequest.Name"); err != nil {
		return err
	}

	if err := validateDescription(request.Description, "CreateNamespaceRequest.Description"); err != nil {
		return err
	}

	return nil
}

func ValidateGetNamespaceRequest(request *gracklepb.GetNamespaceRequest) error {
	if err := validateNamespaceName(request.NamespaceName, "GetNamespaceRequest.NamespaceName"); err != nil {
		return err
	}

	return nil
}

func ValidateUpdateNamespaceRequest(request *gracklepb.UpdateNamespaceRequest) error {
	if err := validateNamespaceName(request.NamespaceName, "UpdateNamespaceRequest.NamespaceName"); err != nil {
		return err
	}

	if err := validateDescription(request.Description, "UpdateNamespaceRequest.Description"); err != nil {
		return err
	}

	return nil
}

func ValidateDeleteNamespaceRequest(request *gracklepb.DeleteNamespaceRequest) error {
	if err := validateNamespaceName(request.NamespaceName, "DeleteNamespaceRequest.NamespaceName"); err != nil {
		return err
	}

	return nil
}

func ValidateListNamespacesRequest(request *gracklepb.ListNamespacesRequest) error {
	if err := validatePaginationToken(request.PaginationToken, "ListNamespacesRequest.PaginationToken"); err != nil {
		return err
	}

	if err := validateLimit(request.Limit, "ListNamespacesRequest.Limit"); err != nil {
		return err
	}

	return nil
}

func ValidateCreateWaitGroupRequest(request *gracklepb.CreateWaitGroupRequest) error {
	if err := validateNamespaceName(request.NamespaceName, "CreateWaitGroupRequest.NamespaceName"); err != nil {
		return err
	}

	if err := validateWaitGroupName(request.WaitGroupName, "CreateWaitGroupRequest.WaitGroupName"); err != nil {
		return err
	}

	if request.Counter <= 0 {
		return invalid("CreateWaitGroupRequest.Counter", "must be greater than 0")
	}

	return nil
}

func ValidateGetWaitGroupRequest(request *gracklepb.GetWaitGroupRequest) error {
	if err := validateNamespaceName(request.NamespaceName, "GetWaitGroupRequest.NamespaceName"); err != nil {
		return err
	}

	if err := validateWaitGroupName(request.WaitGroupName, "GetWaitGroupRequest.WaitGroupName"); err != nil {
		return err
	}

	return nil
}

func ValidateWaitForWaitGroupRequest(request *gracklepb.WaitForWaitGroupRequest) error {
	if err := validateNamespaceName(request.NamespaceName, "WaitForWaitGroupRequest.NamespaceName"); err != nil {
		return err
	}

	if err := validateWaitGroupName(request.WaitGroupName, "WaitForWaitGroupRequest.WaitGroupName"); err != nil {
		return err
	}

	if request.TimeoutSeconds <= 0 {
		return errors.New("WaitForWaitGroupRequest.TimeoutSeconds must be greater than 0")
	}

	if request.TimeoutSeconds > maxTimeoutSeconds {
		return fmt.Errorf("WaitForWaitGroupRequest.TimeoutSeconds must be less than or equal to %d", maxTimeoutSeconds)
	}

	return nil
}

func ValidateAddJobsToWaitGroupRequest(request *gracklepb.AddJobsToWaitGroupRequest) error {
	if err := validateNamespaceName(request.NamespaceName, "AddJobsToWaitGroupRequest.NamespaceName"); err != nil {
		return err
	}

	if err := validateWaitGroupName(request.WaitGroupName, "AddJobsToWaitGroupRequest.WaitGroupName"); err != nil {
		return err
	}

	return nil
}

func ValidateDeleteWaitGroupRequest(request *gracklepb.DeleteWaitGroupRequest) error {
	if err := validateNamespaceName(request.NamespaceName, "DeleteWaitGroupRequest.NamespaceName"); err != nil {
		return err
	}

	if err := validateWaitGroupName(request.WaitGroupName, "DeleteWaitGroupRequest.WaitGroupName"); err != nil {
		return err
	}

	return nil
}

func ValidateListWaitGroupsRequest(request *gracklepb.ListWaitGroupsRequest) error {
	if err := validateNamespaceName(request.NamespaceName, "ListWaitGroupsRequest.NamespaceName"); err != nil {
		return err
	}

	if err := validatePaginationToken(request.PaginationToken, "ListWaitGroupsRequest.PaginationToken"); err != nil {
		return err
	}

	if err := validateLimit(request.Limit, "ListWaitGroupsRequest.Limit"); err != nil {
		return err
	}

	return nil
}

func ValidateListWaitGroupJobsRequest(request *gracklepb.ListWaitGroupJobsRequest) error {
	if err := validateNamespaceName(request.NamespaceName, "ListWaitGroupJobsRequest.NamespaceName"); err != nil {
		return err
	}

	if err := validateWaitGroupName(request.WaitGroupName, "ListWaitGroupJobsRequest.WaitGroupName"); err != nil {
		return err
	}

	if err := validatePaginationToken(request.PaginationToken, "ListWaitGroupJobsRequest.PaginationToken"); err != nil {
		return err
	}

	if err := validateLimit(request.Limit, "ListWaitGroupJobsRequest.Limit"); err != nil {
		return err
	}

	return nil
}

func ValidateCompleteJobsFromWaitGroupRequest(request *gracklepb.CompleteJobsFromWaitGroupRequest) error {
	if err := validateNamespaceName(request.NamespaceName, "CompleteJobsFromWaitGroupRequest.NamespaceName"); err != nil {
		return err
	}

	if err := validateWaitGroupName(request.WaitGroupName, "CompleteJobsFromWaitGroupRequest.WaitGroupName"); err != nil {
		return err
	}

	if len(request.ProcessIds) > maxCompleteJobBatchSize {
		return invalid("CompleteJobFromWaitGroupRequest.ProcessIds", fmt.Sprintf("exceeds max batch size (%d)", maxCompleteJobBatchSize))
	}
	for i, processId := range request.ProcessIds {
		if err := validateProcessId(processId, fmt.Sprintf("CompleteJobFromWaitGroupRequest.ProcessIds[%d]", i)); err != nil {
			return err
		}
	}

	return nil
}

func ValidateListLocksRequest(request *gracklepb.ListLocksRequest) error {
	if err := validateNamespaceName(request.NamespaceName, "ListLocksRequest.NamespaceName"); err != nil {
		return err
	}

	if err := validatePaginationToken(request.PaginationToken, "ListLocksRequest.PaginationToken"); err != nil {
		return err
	}

	if err := validateLimit(request.Limit, "ListLocksRequest.Limit"); err != nil {
		return err
	}

	return nil
}

func ValidateDeleteLockRequest(request *gracklepb.DeleteLockRequest) error {
	if err := validateNamespaceName(request.NamespaceName, "DeleteLockRequest.NamespaceName"); err != nil {
		return err
	}

	if err := validateLockName(request.LockName, "DeleteLockRequest.LockName"); err != nil {
		return err
	}

	return nil
}

func ValidateGetLockRequest(request *gracklepb.GetLockRequest) error {
	if err := validateNamespaceName(request.NamespaceName, "GetLockRequest.NamespaceName"); err != nil {
		return err
	}

	if err := validateLockName(request.LockName, "GetLockRequest.LockName"); err != nil {
		return err
	}

	return nil
}

func ValidateReleaseLockRequest(request *gracklepb.ReleaseLockRequest) error {
	if err := validateNamespaceName(request.NamespaceName, "ReleaseLockRequest.NamespaceName"); err != nil {
		return err
	}

	if err := validateLockName(request.LockName, "ReleaseLockRequest.LockName"); err != nil {
		return err
	}

	if err := validateLeaseId(request.LeaseId, "ReleaseLockRequest.LeaseId"); err != nil {
		return err
	}

	return nil
}

func ValidateAcquireLockRequest(request *gracklepb.AcquireLockRequest) error {
	if err := validateNamespaceName(request.NamespaceName, "AcquireLockRequest.NamespaceName"); err != nil {
		return err
	}

	if err := validateLockName(request.LockName, "AcquireLockRequest.LockName"); err != nil {
		return err
	}

	if err := validateLeaseId(request.LeaseId, "AcquireLockRequest.LeaseId"); err != nil {
		return err
	}

	return nil
}

func ValidateCreateSemaphoreRequest(request *gracklepb.CreateSemaphoreRequest) error {
	if err := validateNamespaceName(request.NamespaceName, "CreateSemaphoreRequest.NamespaceName"); err != nil {
		return err
	}

	if err := validateSemaphoreName(request.SemaphoreName, "CreateSemaphoreRequest.SemaphoreName"); err != nil {
		return err
	}

	if request.Permits <= 0 {
		return invalid("CreateSemaphoreRequest.Permits", "must be greater than 0")
	}

	return nil
}

func ValidateGetSemaphoreRequest(request *gracklepb.GetSemaphoreRequest) error {
	if err := validateNamespaceName(request.NamespaceName, "GetSemaphoreRequest.NamespaceName"); err != nil {
		return err
	}

	if err := validateSemaphoreName(request.SemaphoreName, "GetSemaphoreRequest.SemaphoreName"); err != nil {
		return err
	}

	return nil
}

func ValidateReleaseSemaphoreRequest(request *gracklepb.ReleaseSemaphoreRequest) error {
	if err := validateNamespaceName(request.NamespaceName, "ReleaseSemaphoreRequest.NamespaceName"); err != nil {
		return err
	}

	if err := validateSemaphoreName(request.SemaphoreName, "ReleaseSemaphoreRequest.SemaphoreName"); err != nil {
		return err
	}

	if err := validateLeaseId(request.LeaseId, "ReleaseSemaphoreRequest.LeaseId"); err != nil {
		return err
	}

	return nil
}

func ValidateUpdateSemaphoreRequest(request *gracklepb.UpdateSemaphoreRequest) error {
	if err := validateNamespaceName(request.NamespaceName, "UpdateSemaphoreRequest.NamespaceName"); err != nil {
		return err
	}

	if err := validateSemaphoreName(request.SemaphoreName, "UpdateSemaphoreRequest.SemaphoreName"); err != nil {
		return err
	}

	if request.Permits <= 0 {
		return invalid("UpdateSemaphoreRequest.Permits", "must be greater than 0")
	}

	return nil
}

func ValidateDeleteSemaphoreRequest(request *gracklepb.DeleteSemaphoreRequest) error {
	if err := validateNamespaceName(request.NamespaceName, "DeleteSemaphoreRequest.NamespaceName"); err != nil {
		return err
	}

	if err := validateSemaphoreName(request.SemaphoreName, "DeleteSemaphoreRequest.SemaphoreName"); err != nil {
		return err
	}

	return nil
}

func ValidateListSemaphoresRequest(request *gracklepb.ListSemaphoresRequest) error {
	if err := validateNamespaceName(request.NamespaceName, "ListSemaphoresRequest.NamespaceName"); err != nil {
		return err
	}

	if err := validatePaginationToken(request.PaginationToken, "ListSemaphoresRequest.PaginationToken"); err != nil {
		return err
	}

	if err := validateLimit(request.Limit, "ListSemaphoresRequest.Limit"); err != nil {
		return err
	}

	return nil
}

func ValidateListSemaphoreHoldersRequest(request *gracklepb.ListSemaphoreHoldersRequest) error {
	if err := validateNamespaceName(request.NamespaceName, "ListSemaphoreHoldersRequest.NamespaceName"); err != nil {
		return err
	}

	if err := validateSemaphoreName(request.SemaphoreName, "ListSemaphoreHoldersRequest.SemaphoreName"); err != nil {
		return err
	}

	if err := validatePaginationToken(request.PaginationToken, "ListSemaphoreHoldersRequest.PaginationToken"); err != nil {
		return err
	}

	if err := validateLimit(request.Limit, "ListSemaphoreHoldersRequest.Limit"); err != nil {
		return err
	}

	return nil
}

func ValidateAcquireSemaphoreRequest(request *gracklepb.AcquireSemaphoreRequest) error {
	if err := validateNamespaceName(request.NamespaceName, "AcquireSemaphoreRequest.NamespaceName"); err != nil {
		return err
	}

	if err := validateSemaphoreName(request.SemaphoreName, "AcquireSemaphoreRequest.SemaphoreName"); err != nil {
		return err
	}

	if err := validateLeaseId(request.LeaseId, "AcquireSemaphoreRequest.LeaseId"); err != nil {
		return err
	}

	return nil
}

func ValidateCreateBarrierRequest(request *gracklepb.CreateBarrierRequest) error {
	if err := validateNamespaceName(request.NamespaceName, "CreateBarrierRequest.NamespaceName"); err != nil {
		return err
	}

	if err := validateBarrierName(request.BarrierName, "CreateBarrierRequest.BarrierName"); err != nil {
		return err
	}

	if err := validateDescription(request.Description, "CreateBarrierRequest.Description"); err != nil {
		return err
	}

	if request.ExpectedProcesses <= 0 {
		return invalid("CreateBarrierRequest.ExpectedProcesses", "must be greater than 0")
	}

	if request.ExpiresAt <= 0 {
		return invalid("CreateBarrierRequest.ExpiresAt", "must be greater than 0")
	}

	return nil
}

func ValidateListBarriersRequest(request *gracklepb.ListBarriersRequest) error {
	if err := validateNamespaceName(request.NamespaceName, "ListBarriersRequest.NamespaceName"); err != nil {
		return err
	}

	if err := validatePaginationToken(request.PaginationToken, "ListBarriersRequest.PaginationToken"); err != nil {
		return err
	}

	if err := validateLimit(request.Limit, "ListBarriersRequest.Limit"); err != nil {
		return err
	}

	return nil
}

func ValidateGetBarrierRequest(request *gracklepb.GetBarrierRequest) error {
	if err := validateNamespaceName(request.NamespaceName, "GetBarrierRequest.NamespaceName"); err != nil {
		return err
	}

	if err := validateBarrierName(request.BarrierName, "GetBarrierRequest.BarrierName"); err != nil {
		return err
	}

	return nil
}

func ValidateDeleteBarrierRequest(request *gracklepb.DeleteBarrierRequest) error {
	if err := validateNamespaceName(request.NamespaceName, "DeleteBarrierRequest.NamespaceName"); err != nil {
		return err
	}

	if err := validateBarrierName(request.BarrierName, "DeleteBarrierRequest.BarrierName"); err != nil {
		return err
	}

	return nil
}

func ValidateUpdateBarrierRequest(request *gracklepb.UpdateBarrierRequest) error {
	if err := validateNamespaceName(request.NamespaceName, "UpdateBarrierRequest.NamespaceName"); err != nil {
		return err
	}

	if err := validateBarrierName(request.BarrierName, "UpdateBarrierRequest.BarrierName"); err != nil {
		return err
	}

	if err := validateDescription(request.Description, "UpdateBarrierRequest.Description"); err != nil {
		return err
	}

	if request.ExpectedProcesses <= 0 {
		return invalid("UpdateBarrierRequest.ExpectedProcesses", "must be greater than 0")
	}

	return nil
}

func ValidateArriveAtBarrierRequest(request *gracklepb.ArriveAtBarrierRequest) error {
	if err := validateNamespaceName(request.NamespaceName, "ArriveAtBarrierRequest.NamespaceName"); err != nil {
		return err
	}

	if err := validateBarrierName(request.BarrierName, "ArriveAtBarrierRequest.BarrierName"); err != nil {
		return err
	}

	if err := validateProcessId(request.ProcessId, "ArriveAtBarrierRequest.ProcessId"); err != nil {
		return err
	}

	if request.ExpectedGeneration <= 0 {
		return invalid("ArriveAtBarrierRequest.ExpectedGeneration", "must be greater than 0")
	}

	return nil
}

func ValidateWaitAtBarrierRequest(request *gracklepb.WaitAtBarrierRequest) error {
	if err := validateNamespaceName(request.NamespaceName, "WaitAtBarrierRequest.NamespaceName"); err != nil {
		return err
	}

	if err := validateBarrierName(request.BarrierName, "WaitAtBarrierRequest.BarrierName"); err != nil {
		return err
	}

	if request.ExpectedGeneration <= 0 {
		return invalid("WaitAtBarrierRequest.ExpectedGeneration", "must be greater than 0")
	}

	if request.TimeoutSeconds <= 0 {
		return errors.New("WaitAtBarrierRequest.TimeoutSeconds must be greater than 0")
	}

	if request.TimeoutSeconds > maxTimeoutSeconds {
		return fmt.Errorf("WaitAtBarrierRequest.TimeoutSeconds must be less than or equal to %d", maxTimeoutSeconds)
	}

	return nil
}

func ValidateListBarrierParticipantsRequest(request *gracklepb.ListBarrierParticipantsRequest) error {
	if err := validateNamespaceName(request.NamespaceName, "ListBarrierParticipantsRequest.NamespaceName"); err != nil {
		return err
	}

	if err := validateBarrierName(request.BarrierName, "ListBarrierParticipantsRequest.BarrierName"); err != nil {
		return err
	}

	if err := validatePaginationToken(request.PaginationToken, "ListBarrierParticipantsRequest.PaginationToken"); err != nil {
		return err
	}

	if err := validateLimit(request.Limit, "ListBarrierParticipantsRequest.Limit"); err != nil {
		return err
	}

	return nil
}

func ValidateCreateSemaphoreLeaseRequest(request *gracklepb.CreateSemaphoreLeaseRequest) error {

}

func ValidateRevokeSemaphoreLeaseRequest(request *gracklepb.RevokeSemaphoreLeaseRequest) error {

}

func ValidateRefreshSemaphoreLeaseRequest(request *gracklepb.RefreshSemaphoreLeaseRequest) error {

}

func ValidateListSemaphoreLeasesRequest(request *gracklepb.ListSemaphoreLeasesRequest) error {

}

func ValidateGetSemaphoreLeaseRequest(request *gracklepb.GetSemaphoreLeaseRequest) error {

}

func ValidateCreateLockLeaseRequest(request *gracklepb.CreateLockLeaseRequest) error {

}

func ValidateRevokeLockLeaseRequest(request *gracklepb.RevokeLockLeaseRequest) error {

}

func ValidateRefreshLockLeaseRequest(request *gracklepb.RefreshLockLeaseRequest) error {

}

func ValidateListLockLeasesRequest(request *gracklepb.ListLockLeasesRequest) error {

}

func ValidateGetLockLeaseRequest(request *gracklepb.GetLockLeaseRequest) error {

}

func validateProcessId(value string, fieldName string) error {
	return validateString(value, 1, maxProcessIdLength, nameRegex, fieldName)
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
	return validateString(value, 1, maxLeaseIdLength, leaseIdRegex, fieldName)
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

func validateLimit(value int32, fieldName string) error {
	if value < 0 || value > maxPaginationLimit {
		return invalid(fieldName, fmt.Sprintf("must be between 0 and %d", maxPaginationLimit))
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
		return fmt.Errorf("Invalid %s", fieldName)
	} else {
		return fmt.Errorf("Invalid %s: %s", fieldName, details)
	}
}
