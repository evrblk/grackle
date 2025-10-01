package preview

import (
	"encoding/base64"
	"fmt"
	"regexp"

	gracklepb "github.com/evrblk/evrblk-go/grackle/preview"
)

const (
	maxNamespaceNameLength   = 128
	maxWaitGroupNameLength   = 128
	maxLockNameLength        = 128
	maxSemaphoreNameLength   = 128
	maxProcessIdLength       = 128
	maxDescriptionLength     = 1024
	maxCompleteJobBatchSize  = 50
	maxPaginationTokenLength = 1024
	maxPaginationLimit       = 100

	nameRegex = "^[-_0-9a-zA-Z]*$"
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

	if request.Counter == 0 {
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

	if err := validateProcessId(request.ProcessId, "ReleaseLockRequest.ProcessId"); err != nil {
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

	if err := validateProcessId(request.ProcessId, "AcquireLockRequest.ProcessId"); err != nil {
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

	if request.Permits == 0 {
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

	if err := validateProcessId(request.ProcessId, "ReleaseSemaphoreRequest.ProcessId"); err != nil {
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

	if request.Permits == 0 {
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

func ValidateAcquireSemaphoreRequest(request *gracklepb.AcquireSemaphoreRequest) error {
	if err := validateNamespaceName(request.NamespaceName, "AcquireSemaphoreRequest.NamespaceName"); err != nil {
		return err
	}

	if err := validateSemaphoreName(request.SemaphoreName, "AcquireSemaphoreRequest.SemaphoreName"); err != nil {
		return err
	}

	if err := validateProcessId(request.ProcessId, "AcquireSemaphoreRequest.ProcessId"); err != nil {
		return err
	}

	return nil
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

func validateLockName(value string, fieldName string) error {
	return validateString(value, 1, maxLockNameLength, nameRegex, fieldName)
}

func validateWaitGroupName(value string, fieldName string) error {
	return validateString(value, 1, maxWaitGroupNameLength, nameRegex, fieldName)
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
