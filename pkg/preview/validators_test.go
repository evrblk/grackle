package preview

import (
	"encoding/base64"
	"testing"

	"github.com/stretchr/testify/assert"

	gracklepb "github.com/evrblk/evrblk-go/grackle/preview"
)

func TestValidateCreateNamespaceRequest(t *testing.T) {
	assert := assert.New(t)

	// empty request
	assert.Error(ValidateCreateNamespaceRequest(&gracklepb.CreateNamespaceRequest{}))

	// empty name
	assert.Error(ValidateCreateNamespaceRequest(&gracklepb.CreateNamespaceRequest{
		Name: "",
	}))

	// name too long
	assert.Error(ValidateCreateNamespaceRequest(&gracklepb.CreateNamespaceRequest{
		Name: string(make([]byte, 129)),
	}))

	// invalid name characters
	assert.Error(ValidateCreateNamespaceRequest(&gracklepb.CreateNamespaceRequest{
		Name: "invalid name",
	}))

	// description too long
	assert.Error(ValidateCreateNamespaceRequest(&gracklepb.CreateNamespaceRequest{
		Name:        "validname",
		Description: string(make([]byte, 1025)),
	}))

	// valid request
	assert.NoError(ValidateCreateNamespaceRequest(&gracklepb.CreateNamespaceRequest{
		Name:        "validname",
		Description: "Valid description",
	}))
}

func TestValidateGetNamespaceRequest(t *testing.T) {
	assert := assert.New(t)

	// empty request
	assert.Error(ValidateGetNamespaceRequest(&gracklepb.GetNamespaceRequest{}))

	// empty namespace name
	assert.Error(ValidateGetNamespaceRequest(&gracklepb.GetNamespaceRequest{
		NamespaceName: "",
	}))

	// namespace name too long
	assert.Error(ValidateGetNamespaceRequest(&gracklepb.GetNamespaceRequest{
		NamespaceName: string(make([]byte, 129)),
	}))

	// invalid namespace name characters
	assert.Error(ValidateGetNamespaceRequest(&gracklepb.GetNamespaceRequest{
		NamespaceName: "invalid name",
	}))

	// valid request
	assert.NoError(ValidateGetNamespaceRequest(&gracklepb.GetNamespaceRequest{
		NamespaceName: "validname",
	}))
}

func TestValidateUpdateNamespaceRequest(t *testing.T) {
	assert := assert.New(t)

	// empty request
	assert.Error(ValidateUpdateNamespaceRequest(&gracklepb.UpdateNamespaceRequest{}))

	// empty namespace name
	assert.Error(ValidateUpdateNamespaceRequest(&gracklepb.UpdateNamespaceRequest{
		NamespaceName: "",
	}))

	// namespace name too long
	assert.Error(ValidateUpdateNamespaceRequest(&gracklepb.UpdateNamespaceRequest{
		NamespaceName: string(make([]byte, 129)),
	}))

	// invalid namespace name characters
	assert.Error(ValidateUpdateNamespaceRequest(&gracklepb.UpdateNamespaceRequest{
		NamespaceName: "invalid name",
	}))

	// description too long
	assert.Error(ValidateUpdateNamespaceRequest(&gracklepb.UpdateNamespaceRequest{
		NamespaceName: "validname",
		Description:   string(make([]byte, 1025)),
	}))

	// valid request
	assert.NoError(ValidateUpdateNamespaceRequest(&gracklepb.UpdateNamespaceRequest{
		NamespaceName: "validname",
		Description:   "Valid description",
	}))
}

func TestValidateDeleteNamespaceRequest(t *testing.T) {
	assert := assert.New(t)

	// empty request
	assert.Error(ValidateDeleteNamespaceRequest(&gracklepb.DeleteNamespaceRequest{}))

	// empty namespace name
	assert.Error(ValidateDeleteNamespaceRequest(&gracklepb.DeleteNamespaceRequest{
		NamespaceName: "",
	}))

	// namespace name too long
	assert.Error(ValidateDeleteNamespaceRequest(&gracklepb.DeleteNamespaceRequest{
		NamespaceName: string(make([]byte, 129)),
	}))

	// invalid namespace name characters
	assert.Error(ValidateDeleteNamespaceRequest(&gracklepb.DeleteNamespaceRequest{
		NamespaceName: "invalid name",
	}))

	// valid request
	assert.NoError(ValidateDeleteNamespaceRequest(&gracklepb.DeleteNamespaceRequest{
		NamespaceName: "validname",
	}))
}

func TestValidateListNamespacesRequest(t *testing.T) {
	assert := assert.New(t)

	// Test empty request - should pass as pagination fields are optional
	assert.NoError(ValidateListNamespacesRequest(&gracklepb.ListNamespacesRequest{}))

	// Test valid request with no pagination
	assert.NoError(ValidateListNamespacesRequest(&gracklepb.ListNamespacesRequest{
		PaginationToken: "",
		Limit:           0,
	}))

	// Test valid request with pagination token
	assert.NoError(ValidateListNamespacesRequest(&gracklepb.ListNamespacesRequest{
		PaginationToken: "dGVzdA==",
		Limit:           0,
	}))

	// Test valid request with limit
	assert.NoError(ValidateListNamespacesRequest(&gracklepb.ListNamespacesRequest{
		PaginationToken: "",
		Limit:           50,
	}))

	// Test valid request with both pagination token and limit
	assert.NoError(ValidateListNamespacesRequest(&gracklepb.ListNamespacesRequest{
		PaginationToken: "dGVzdA==",
		Limit:           100,
	}))

	// Test invalid pagination token (not base64)
	assert.Error(ValidateListNamespacesRequest(&gracklepb.ListNamespacesRequest{
		PaginationToken: "invalid-base64!@#",
		Limit:           50,
	}))

	// Test pagination token too long
	longToken := string(make([]byte, 1025))
	assert.Error(ValidateListNamespacesRequest(&gracklepb.ListNamespacesRequest{
		PaginationToken: longToken,
		Limit:           50,
	}))

	// Test limit too high
	assert.Error(ValidateListNamespacesRequest(&gracklepb.ListNamespacesRequest{
		PaginationToken: "",
		Limit:           101,
	}))

	// Test negative limit
	assert.Error(ValidateListNamespacesRequest(&gracklepb.ListNamespacesRequest{
		PaginationToken: "",
		Limit:           -1,
	}))

	// Test edge case: limit at maximum
	assert.NoError(ValidateListNamespacesRequest(&gracklepb.ListNamespacesRequest{
		PaginationToken: "",
		Limit:           100,
	}))

	// Test edge case: pagination token at maximum length
	maxToken := string(make([]byte, 1024)) // maxPaginationTokenLength
	assert.Error(ValidateListNamespacesRequest(&gracklepb.ListNamespacesRequest{
		PaginationToken: maxToken, // This will fail base64 validation
		Limit:           50,
	}))

	// Test valid base64 token at maximum length
	validMaxToken := base64.StdEncoding.EncodeToString(make([]byte, 768)) // 768 bytes = 1024 base64 chars
	assert.NoError(ValidateListNamespacesRequest(&gracklepb.ListNamespacesRequest{
		PaginationToken: validMaxToken,
		Limit:           50,
	}))
}

func TestValidateCreateWaitGroupRequest(t *testing.T) {
	assert := assert.New(t)

	// empty request
	assert.Error(ValidateCreateWaitGroupRequest(&gracklepb.CreateWaitGroupRequest{}))

	// empty namespace name
	assert.Error(ValidateCreateWaitGroupRequest(&gracklepb.CreateWaitGroupRequest{
		WaitGroupName: "validname",
		Counter:       1,
	}))

	// empty wait group name
	assert.Error(ValidateCreateWaitGroupRequest(&gracklepb.CreateWaitGroupRequest{
		NamespaceName: "validname",
		Counter:       1,
	}))

	// namespace name too long
	assert.Error(ValidateCreateWaitGroupRequest(&gracklepb.CreateWaitGroupRequest{
		NamespaceName: string(make([]byte, 129)),
		WaitGroupName: "validname",
		Counter:       1,
	}))

	// wait group name too long
	assert.Error(ValidateCreateWaitGroupRequest(&gracklepb.CreateWaitGroupRequest{
		NamespaceName: "validname",
		WaitGroupName: string(make([]byte, 129)),
		Counter:       1,
	}))

	// invalid namespace name characters
	assert.Error(ValidateCreateWaitGroupRequest(&gracklepb.CreateWaitGroupRequest{
		NamespaceName: "invalid name",
		WaitGroupName: "validname",
		Counter:       1,
	}))

	// invalid wait group name characters
	assert.Error(ValidateCreateWaitGroupRequest(&gracklepb.CreateWaitGroupRequest{
		NamespaceName: "validname",
		WaitGroupName: "invalid name",
		Counter:       1,
	}))

	// counter must be greater than 0
	assert.Error(ValidateCreateWaitGroupRequest(&gracklepb.CreateWaitGroupRequest{
		NamespaceName: "validname",
		WaitGroupName: "validwaitgroup",
		Counter:       0,
	}))

	// valid request
	assert.NoError(ValidateCreateWaitGroupRequest(&gracklepb.CreateWaitGroupRequest{
		NamespaceName: "validname",
		WaitGroupName: "validwaitgroup",
		Counter:       1,
	}))
}

func TestValidateGetWaitGroupRequest(t *testing.T) {
	assert := assert.New(t)

	// empty request
	assert.Error(ValidateGetWaitGroupRequest(&gracklepb.GetWaitGroupRequest{}))

	// empty namespace name
	assert.Error(ValidateGetWaitGroupRequest(&gracklepb.GetWaitGroupRequest{
		WaitGroupName: "validname",
	}))

	// empty wait group name
	assert.Error(ValidateGetWaitGroupRequest(&gracklepb.GetWaitGroupRequest{
		NamespaceName: "validname",
	}))

	// namespace name too long
	assert.Error(ValidateGetWaitGroupRequest(&gracklepb.GetWaitGroupRequest{
		NamespaceName: string(make([]byte, 129)),
		WaitGroupName: "validname",
	}))

	// wait group name too long
	assert.Error(ValidateGetWaitGroupRequest(&gracklepb.GetWaitGroupRequest{
		NamespaceName: "validname",
		WaitGroupName: string(make([]byte, 129)),
	}))

	// invalid namespace name characters
	assert.Error(ValidateGetWaitGroupRequest(&gracklepb.GetWaitGroupRequest{
		NamespaceName: "invalid name",
		WaitGroupName: "validname",
	}))

	// invalid wait group name characters
	assert.Error(ValidateGetWaitGroupRequest(&gracklepb.GetWaitGroupRequest{
		NamespaceName: "validname",
		WaitGroupName: "invalid name",
	}))

	// valid request
	assert.NoError(ValidateGetWaitGroupRequest(&gracklepb.GetWaitGroupRequest{
		NamespaceName: "validname",
		WaitGroupName: "validwaitgroup",
	}))
}

func TestValidateAddJobsToWaitGroupRequest(t *testing.T) {
	assert := assert.New(t)

	// empty request
	assert.Error(ValidateAddJobsToWaitGroupRequest(&gracklepb.AddJobsToWaitGroupRequest{}))

	// empty namespace name
	assert.Error(ValidateAddJobsToWaitGroupRequest(&gracklepb.AddJobsToWaitGroupRequest{
		WaitGroupName: "validname",
	}))

	// empty wait group name
	assert.Error(ValidateAddJobsToWaitGroupRequest(&gracklepb.AddJobsToWaitGroupRequest{
		NamespaceName: "validname",
	}))

	// namespace name too long
	assert.Error(ValidateAddJobsToWaitGroupRequest(&gracklepb.AddJobsToWaitGroupRequest{
		NamespaceName: string(make([]byte, 129)),
		WaitGroupName: "validname",
	}))

	// wait group name too long
	assert.Error(ValidateAddJobsToWaitGroupRequest(&gracklepb.AddJobsToWaitGroupRequest{
		NamespaceName: "validname",
		WaitGroupName: string(make([]byte, 129)),
	}))

	// invalid namespace name characters
	assert.Error(ValidateAddJobsToWaitGroupRequest(&gracklepb.AddJobsToWaitGroupRequest{
		NamespaceName: "invalid name",
		WaitGroupName: "validname",
	}))

	// invalid wait group name characters
	assert.Error(ValidateAddJobsToWaitGroupRequest(&gracklepb.AddJobsToWaitGroupRequest{
		NamespaceName: "validname",
		WaitGroupName: "invalid name",
	}))

	// valid request
	assert.NoError(ValidateAddJobsToWaitGroupRequest(&gracklepb.AddJobsToWaitGroupRequest{
		NamespaceName: "validname",
		WaitGroupName: "validwaitgroup",
	}))
}

func TestValidateDeleteWaitGroupRequest(t *testing.T) {
	assert := assert.New(t)

	// empty request
	assert.Error(ValidateDeleteWaitGroupRequest(&gracklepb.DeleteWaitGroupRequest{}))

	// empty namespace name
	assert.Error(ValidateDeleteWaitGroupRequest(&gracklepb.DeleteWaitGroupRequest{
		WaitGroupName: "validname",
	}))

	// empty wait group name
	assert.Error(ValidateDeleteWaitGroupRequest(&gracklepb.DeleteWaitGroupRequest{
		NamespaceName: "validname",
	}))

	// namespace name too long
	assert.Error(ValidateDeleteWaitGroupRequest(&gracklepb.DeleteWaitGroupRequest{
		NamespaceName: string(make([]byte, 129)),
		WaitGroupName: "validname",
	}))

	// wait group name too long
	assert.Error(ValidateDeleteWaitGroupRequest(&gracklepb.DeleteWaitGroupRequest{
		NamespaceName: "validname",
		WaitGroupName: string(make([]byte, 129)),
	}))

	// invalid namespace name characters
	assert.Error(ValidateDeleteWaitGroupRequest(&gracklepb.DeleteWaitGroupRequest{
		NamespaceName: "invalid name",
		WaitGroupName: "validname",
	}))

	// invalid wait group name characters
	assert.Error(ValidateDeleteWaitGroupRequest(&gracklepb.DeleteWaitGroupRequest{
		NamespaceName: "validname",
		WaitGroupName: "invalid name",
	}))

	// valid request
	assert.NoError(ValidateDeleteWaitGroupRequest(&gracklepb.DeleteWaitGroupRequest{
		NamespaceName: "validname",
		WaitGroupName: "validwaitgroup",
	}))
}

func TestValidateListWaitGroupsRequest(t *testing.T) {
	assert := assert.New(t)

	// Test empty request - should fail due to missing namespace name
	assert.Error(ValidateListWaitGroupsRequest(&gracklepb.ListWaitGroupsRequest{}))

	// Test empty namespace name
	assert.Error(ValidateListWaitGroupsRequest(&gracklepb.ListWaitGroupsRequest{
		NamespaceName: "",
	}))

	// Test namespace name too long
	assert.Error(ValidateListWaitGroupsRequest(&gracklepb.ListWaitGroupsRequest{
		NamespaceName: string(make([]byte, 129)),
	}))

	// Test invalid namespace name characters
	assert.Error(ValidateListWaitGroupsRequest(&gracklepb.ListWaitGroupsRequest{
		NamespaceName: "invalid name",
	}))

	// Test valid request with no pagination
	assert.NoError(ValidateListWaitGroupsRequest(&gracklepb.ListWaitGroupsRequest{
		NamespaceName:   "validname",
		PaginationToken: "",
		Limit:           0,
	}))

	// Test valid request with pagination token
	assert.NoError(ValidateListWaitGroupsRequest(&gracklepb.ListWaitGroupsRequest{
		NamespaceName:   "validname",
		PaginationToken: "dGVzdA==",
		Limit:           0,
	}))

	// Test valid request with limit
	assert.NoError(ValidateListWaitGroupsRequest(&gracklepb.ListWaitGroupsRequest{
		NamespaceName:   "validname",
		PaginationToken: "",
		Limit:           50,
	}))

	// Test valid request with both pagination token and limit
	assert.NoError(ValidateListWaitGroupsRequest(&gracklepb.ListWaitGroupsRequest{
		NamespaceName:   "validname",
		PaginationToken: "dGVzdA==",
		Limit:           100,
	}))

	// Test invalid pagination token (not base64)
	assert.Error(ValidateListWaitGroupsRequest(&gracklepb.ListWaitGroupsRequest{
		NamespaceName:   "validname",
		PaginationToken: "invalid-base64!@#",
		Limit:           50,
	}))

	// Test pagination token too long
	longToken := string(make([]byte, 1025))
	assert.Error(ValidateListWaitGroupsRequest(&gracklepb.ListWaitGroupsRequest{
		NamespaceName:   "validname",
		PaginationToken: longToken,
		Limit:           50,
	}))

	// Test limit too high
	assert.Error(ValidateListWaitGroupsRequest(&gracklepb.ListWaitGroupsRequest{
		NamespaceName:   "validname",
		PaginationToken: "",
		Limit:           101,
	}))

	// Test negative limit
	assert.Error(ValidateListWaitGroupsRequest(&gracklepb.ListWaitGroupsRequest{
		NamespaceName:   "validname",
		PaginationToken: "",
		Limit:           -1,
	}))

	// Test edge case: limit at maximum
	assert.NoError(ValidateListWaitGroupsRequest(&gracklepb.ListWaitGroupsRequest{
		NamespaceName:   "validname",
		PaginationToken: "",
		Limit:           100,
	}))

	// Test valid base64 token at maximum length
	validMaxToken := base64.StdEncoding.EncodeToString(make([]byte, 768)) // 768 bytes = 1024 base64 chars
	assert.NoError(ValidateListWaitGroupsRequest(&gracklepb.ListWaitGroupsRequest{
		NamespaceName:   "validname",
		PaginationToken: validMaxToken,
		Limit:           50,
	}))
}

func TestValidateCompleteJobsFromWaitGroupRequest(t *testing.T) {
	assert := assert.New(t)

	// empty request
	assert.Error(ValidateCompleteJobsFromWaitGroupRequest(&gracklepb.CompleteJobsFromWaitGroupRequest{}))

	// empty namespace name
	assert.Error(ValidateCompleteJobsFromWaitGroupRequest(&gracklepb.CompleteJobsFromWaitGroupRequest{
		WaitGroupName: "validname",
		ProcessIds:    []string{"proc1"},
	}))

	// empty wait group name
	assert.Error(ValidateCompleteJobsFromWaitGroupRequest(&gracklepb.CompleteJobsFromWaitGroupRequest{
		NamespaceName: "validname",
		ProcessIds:    []string{"proc1"},
	}))

	// namespace name too long
	assert.Error(ValidateCompleteJobsFromWaitGroupRequest(&gracklepb.CompleteJobsFromWaitGroupRequest{
		NamespaceName: string(make([]byte, 129)),
		WaitGroupName: "validname",
		ProcessIds:    []string{"proc1"},
	}))

	// wait group name too long
	assert.Error(ValidateCompleteJobsFromWaitGroupRequest(&gracklepb.CompleteJobsFromWaitGroupRequest{
		NamespaceName: "validname",
		WaitGroupName: string(make([]byte, 129)),
		ProcessIds:    []string{"proc1"},
	}))

	// invalid namespace name characters
	assert.Error(ValidateCompleteJobsFromWaitGroupRequest(&gracklepb.CompleteJobsFromWaitGroupRequest{
		NamespaceName: "invalid name",
		WaitGroupName: "validname",
		ProcessIds:    []string{"proc1"},
	}))

	// invalid wait group name characters
	assert.Error(ValidateCompleteJobsFromWaitGroupRequest(&gracklepb.CompleteJobsFromWaitGroupRequest{
		NamespaceName: "validname",
		WaitGroupName: "invalid name",
		ProcessIds:    []string{"proc1"},
	}))

	// too many process ids
	processIds := make([]string, 51)
	for i := 0; i < 51; i++ {
		processIds[i] = "proc1"
	}
	assert.Error(ValidateCompleteJobsFromWaitGroupRequest(&gracklepb.CompleteJobsFromWaitGroupRequest{
		NamespaceName: "validname",
		WaitGroupName: "validwaitgroup",
		ProcessIds:    processIds,
	}))

	// invalid process id
	assert.Error(ValidateCompleteJobsFromWaitGroupRequest(&gracklepb.CompleteJobsFromWaitGroupRequest{
		NamespaceName: "validname",
		WaitGroupName: "validwaitgroup",
		ProcessIds:    []string{"invalid process id"},
	}))

	// valid request
	assert.NoError(ValidateCompleteJobsFromWaitGroupRequest(&gracklepb.CompleteJobsFromWaitGroupRequest{
		NamespaceName: "validname",
		WaitGroupName: "validwaitgroup",
		ProcessIds:    []string{"proc1", "proc2"},
	}))
}

func TestValidateDeleteLockRequest(t *testing.T) {
	assert := assert.New(t)

	// empty request
	assert.Error(ValidateDeleteLockRequest(&gracklepb.DeleteLockRequest{}))

	// empty namespace name
	assert.Error(ValidateDeleteLockRequest(&gracklepb.DeleteLockRequest{
		LockName: "validname",
	}))

	// empty lock name
	assert.Error(ValidateDeleteLockRequest(&gracklepb.DeleteLockRequest{
		NamespaceName: "validname",
	}))

	// namespace name too long
	assert.Error(ValidateDeleteLockRequest(&gracklepb.DeleteLockRequest{
		NamespaceName: string(make([]byte, 129)),
		LockName:      "validname",
	}))

	// lock name too long
	assert.Error(ValidateDeleteLockRequest(&gracklepb.DeleteLockRequest{
		NamespaceName: "validname",
		LockName:      string(make([]byte, 129)),
	}))

	// invalid namespace name characters
	assert.Error(ValidateDeleteLockRequest(&gracklepb.DeleteLockRequest{
		NamespaceName: "invalid name",
		LockName:      "validname",
	}))

	// invalid lock name characters
	assert.Error(ValidateDeleteLockRequest(&gracklepb.DeleteLockRequest{
		NamespaceName: "validname",
		LockName:      "invalid name",
	}))

	// valid request
	assert.NoError(ValidateDeleteLockRequest(&gracklepb.DeleteLockRequest{
		NamespaceName: "validname",
		LockName:      "validlock",
	}))
}

func TestValidateGetLockRequest(t *testing.T) {
	assert := assert.New(t)

	// empty request
	assert.Error(ValidateGetLockRequest(&gracklepb.GetLockRequest{}))

	// empty namespace name
	assert.Error(ValidateGetLockRequest(&gracklepb.GetLockRequest{
		LockName: "validname",
	}))

	// empty lock name
	assert.Error(ValidateGetLockRequest(&gracklepb.GetLockRequest{
		NamespaceName: "validname",
	}))

	// namespace name too long
	assert.Error(ValidateGetLockRequest(&gracklepb.GetLockRequest{
		NamespaceName: string(make([]byte, 129)),
		LockName:      "validname",
	}))

	// lock name too long
	assert.Error(ValidateGetLockRequest(&gracklepb.GetLockRequest{
		NamespaceName: "validname",
		LockName:      string(make([]byte, 129)),
	}))

	// invalid namespace name characters
	assert.Error(ValidateGetLockRequest(&gracklepb.GetLockRequest{
		NamespaceName: "invalid name",
		LockName:      "validname",
	}))

	// invalid lock name characters
	assert.Error(ValidateGetLockRequest(&gracklepb.GetLockRequest{
		NamespaceName: "validname",
		LockName:      "invalid name",
	}))

	// valid request
	assert.NoError(ValidateGetLockRequest(&gracklepb.GetLockRequest{
		NamespaceName: "validname",
		LockName:      "validlock",
	}))
}

func TestValidateReleaseLockRequest(t *testing.T) {
	assert := assert.New(t)

	// empty request
	assert.Error(ValidateReleaseLockRequest(&gracklepb.ReleaseLockRequest{}))

	// empty namespace name
	assert.Error(ValidateReleaseLockRequest(&gracklepb.ReleaseLockRequest{
		LockName:  "validname",
		ProcessId: "proc1",
	}))

	// empty lock name
	assert.Error(ValidateReleaseLockRequest(&gracklepb.ReleaseLockRequest{
		NamespaceName: "validname",
		ProcessId:     "proc1",
	}))

	// empty process id
	assert.Error(ValidateReleaseLockRequest(&gracklepb.ReleaseLockRequest{
		NamespaceName: "validname",
		LockName:      "validname",
	}))

	// namespace name too long
	assert.Error(ValidateReleaseLockRequest(&gracklepb.ReleaseLockRequest{
		NamespaceName: string(make([]byte, 129)),
		LockName:      "validname",
		ProcessId:     "proc1",
	}))

	// lock name too long
	assert.Error(ValidateReleaseLockRequest(&gracklepb.ReleaseLockRequest{
		NamespaceName: "validname",
		LockName:      string(make([]byte, 129)),
		ProcessId:     "proc1",
	}))

	// process id too long
	assert.Error(ValidateReleaseLockRequest(&gracklepb.ReleaseLockRequest{
		NamespaceName: "validname",
		LockName:      "validname",
		ProcessId:     string(make([]byte, 129)),
	}))

	// invalid namespace name characters
	assert.Error(ValidateReleaseLockRequest(&gracklepb.ReleaseLockRequest{
		NamespaceName: "invalid name",
		LockName:      "validname",
		ProcessId:     "proc1",
	}))

	// invalid lock name characters
	assert.Error(ValidateReleaseLockRequest(&gracklepb.ReleaseLockRequest{
		NamespaceName: "validname",
		LockName:      "invalid name",
		ProcessId:     "proc1",
	}))

	// invalid process id characters
	assert.Error(ValidateReleaseLockRequest(&gracklepb.ReleaseLockRequest{
		NamespaceName: "validname",
		LockName:      "validname",
		ProcessId:     "invalid process id",
	}))

	// valid request
	assert.NoError(ValidateReleaseLockRequest(&gracklepb.ReleaseLockRequest{
		NamespaceName: "validname",
		LockName:      "validlock",
		ProcessId:     "proc1",
	}))
}

func TestValidateAcquireLockRequest(t *testing.T) {
	assert := assert.New(t)

	// empty request
	assert.Error(ValidateAcquireLockRequest(&gracklepb.AcquireLockRequest{}))

	// empty namespace name
	assert.Error(ValidateAcquireLockRequest(&gracklepb.AcquireLockRequest{
		LockName:  "validname",
		ProcessId: "proc1",
	}))

	// empty lock name
	assert.Error(ValidateAcquireLockRequest(&gracklepb.AcquireLockRequest{
		NamespaceName: "validname",
		ProcessId:     "proc1",
	}))

	// empty process id
	assert.Error(ValidateAcquireLockRequest(&gracklepb.AcquireLockRequest{
		NamespaceName: "validname",
		LockName:      "validname",
	}))

	// namespace name too long
	assert.Error(ValidateAcquireLockRequest(&gracklepb.AcquireLockRequest{
		NamespaceName: string(make([]byte, 129)),
		LockName:      "validname",
		ProcessId:     "proc1",
	}))

	// lock name too long
	assert.Error(ValidateAcquireLockRequest(&gracklepb.AcquireLockRequest{
		NamespaceName: "validname",
		LockName:      string(make([]byte, 129)),
		ProcessId:     "proc1",
	}))

	// process id too long
	assert.Error(ValidateAcquireLockRequest(&gracklepb.AcquireLockRequest{
		NamespaceName: "validname",
		LockName:      "validname",
		ProcessId:     string(make([]byte, 129)),
	}))

	// invalid namespace name characters
	assert.Error(ValidateAcquireLockRequest(&gracklepb.AcquireLockRequest{
		NamespaceName: "invalid name",
		LockName:      "validname",
		ProcessId:     "proc1",
	}))

	// invalid lock name characters
	assert.Error(ValidateAcquireLockRequest(&gracklepb.AcquireLockRequest{
		NamespaceName: "validname",
		LockName:      "invalid name",
		ProcessId:     "proc1",
	}))

	// invalid process id characters
	assert.Error(ValidateAcquireLockRequest(&gracklepb.AcquireLockRequest{
		NamespaceName: "validname",
		LockName:      "validname",
		ProcessId:     "invalid process id",
	}))

	// valid request
	assert.NoError(ValidateAcquireLockRequest(&gracklepb.AcquireLockRequest{
		NamespaceName: "validname",
		LockName:      "validlock",
		ProcessId:     "proc1",
	}))
}

func TestValidateCreateSemaphoreRequest(t *testing.T) {
	assert := assert.New(t)

	// empty request
	assert.Error(ValidateCreateSemaphoreRequest(&gracklepb.CreateSemaphoreRequest{}))

	// empty namespace name
	assert.Error(ValidateCreateSemaphoreRequest(&gracklepb.CreateSemaphoreRequest{
		SemaphoreName: "validname",
		Description:   "validdescription",
		Permits:       1,
	}))

	// empty semaphore name
	assert.Error(ValidateCreateSemaphoreRequest(&gracklepb.CreateSemaphoreRequest{
		NamespaceName: "validname",
		Description:   "validdescription",
		Permits:       1,
	}))

	// namespace name too long
	assert.Error(ValidateCreateSemaphoreRequest(&gracklepb.CreateSemaphoreRequest{
		NamespaceName: string(make([]byte, 129)),
		SemaphoreName: "validname",
		Description:   "validdescription",
		Permits:       1,
	}))

	// semaphore name too long
	assert.Error(ValidateCreateSemaphoreRequest(&gracklepb.CreateSemaphoreRequest{
		NamespaceName: "validname",
		SemaphoreName: string(make([]byte, 129)),
		Description:   "validdescription",
		Permits:       1,
	}))

	// invalid namespace name characters
	assert.Error(ValidateCreateSemaphoreRequest(&gracklepb.CreateSemaphoreRequest{
		NamespaceName: "invalid name",
		SemaphoreName: "validname",
		Description:   "validdescription",
		Permits:       1,
	}))

	// invalid semaphore name characters
	assert.Error(ValidateCreateSemaphoreRequest(&gracklepb.CreateSemaphoreRequest{
		NamespaceName: "validname",
		SemaphoreName: "invalid name",
		Description:   "validdescription",
		Permits:       1,
	}))

	// permits must be greater than 0
	assert.Error(ValidateCreateSemaphoreRequest(&gracklepb.CreateSemaphoreRequest{
		NamespaceName: "validname",
		SemaphoreName: "validsemaphore",
		Description:   "validdescription",
		Permits:       0,
	}))

	// valid request
	assert.NoError(ValidateCreateSemaphoreRequest(&gracklepb.CreateSemaphoreRequest{
		NamespaceName: "validname",
		SemaphoreName: "validsemaphore",
		Description:   "validdescription",
		Permits:       1,
	}))
}

func TestValidateGetSemaphoreRequest(t *testing.T) {
	assert := assert.New(t)

	// empty request
	assert.Error(ValidateGetSemaphoreRequest(&gracklepb.GetSemaphoreRequest{}))

	// empty namespace name
	assert.Error(ValidateGetSemaphoreRequest(&gracklepb.GetSemaphoreRequest{
		SemaphoreName: "validname",
	}))

	// empty semaphore name
	assert.Error(ValidateGetSemaphoreRequest(&gracklepb.GetSemaphoreRequest{
		NamespaceName: "validname",
	}))

	// namespace name too long
	assert.Error(ValidateGetSemaphoreRequest(&gracklepb.GetSemaphoreRequest{
		NamespaceName: string(make([]byte, 129)),
		SemaphoreName: "validname",
	}))

	// semaphore name too long
	assert.Error(ValidateGetSemaphoreRequest(&gracklepb.GetSemaphoreRequest{
		NamespaceName: "validname",
		SemaphoreName: string(make([]byte, 129)),
	}))

	// invalid namespace name characters
	assert.Error(ValidateGetSemaphoreRequest(&gracklepb.GetSemaphoreRequest{
		NamespaceName: "invalid name",
		SemaphoreName: "validname",
	}))

	// invalid semaphore name characters
	assert.Error(ValidateGetSemaphoreRequest(&gracklepb.GetSemaphoreRequest{
		NamespaceName: "validname",
		SemaphoreName: "invalid name",
	}))

	// valid request
	assert.NoError(ValidateGetSemaphoreRequest(&gracklepb.GetSemaphoreRequest{
		NamespaceName: "validname",
		SemaphoreName: "validsemaphore",
	}))
}

func TestValidateReleaseSemaphoreRequest(t *testing.T) {
	assert := assert.New(t)

	// empty request
	assert.Error(ValidateReleaseSemaphoreRequest(&gracklepb.ReleaseSemaphoreRequest{}))

	// empty namespace name
	assert.Error(ValidateReleaseSemaphoreRequest(&gracklepb.ReleaseSemaphoreRequest{
		SemaphoreName: "validname",
		ProcessId:     "proc1",
	}))

	// empty semaphore name
	assert.Error(ValidateReleaseSemaphoreRequest(&gracklepb.ReleaseSemaphoreRequest{
		NamespaceName: "validname",
		ProcessId:     "proc1",
	}))

	// empty process id
	assert.Error(ValidateReleaseSemaphoreRequest(&gracklepb.ReleaseSemaphoreRequest{
		NamespaceName: "validname",
		SemaphoreName: "validname",
	}))

	// namespace name too long
	assert.Error(ValidateReleaseSemaphoreRequest(&gracklepb.ReleaseSemaphoreRequest{
		NamespaceName: string(make([]byte, 129)),
		SemaphoreName: "validname",
		ProcessId:     "proc1",
	}))

	// semaphore name too long
	assert.Error(ValidateReleaseSemaphoreRequest(&gracklepb.ReleaseSemaphoreRequest{
		NamespaceName: "validname",
		SemaphoreName: string(make([]byte, 129)),
		ProcessId:     "proc1",
	}))

	// process id too long
	assert.Error(ValidateReleaseSemaphoreRequest(&gracklepb.ReleaseSemaphoreRequest{
		NamespaceName: "validname",
		SemaphoreName: "validname",
		ProcessId:     string(make([]byte, 129)),
	}))

	// invalid namespace name characters
	assert.Error(ValidateReleaseSemaphoreRequest(&gracklepb.ReleaseSemaphoreRequest{
		NamespaceName: "invalid name",
		SemaphoreName: "validname",
		ProcessId:     "proc1",
	}))

	// invalid semaphore name characters
	assert.Error(ValidateReleaseSemaphoreRequest(&gracklepb.ReleaseSemaphoreRequest{
		NamespaceName: "validname",
		SemaphoreName: "invalid name",
		ProcessId:     "proc1",
	}))

	// invalid process id characters
	assert.Error(ValidateReleaseSemaphoreRequest(&gracklepb.ReleaseSemaphoreRequest{
		NamespaceName: "validname",
		SemaphoreName: "validname",
		ProcessId:     "invalid process id",
	}))

	// valid request
	assert.NoError(ValidateReleaseSemaphoreRequest(&gracklepb.ReleaseSemaphoreRequest{
		NamespaceName: "validname",
		SemaphoreName: "validsemaphore",
		ProcessId:     "proc1",
	}))
}

func TestValidateUpdateSemaphoreRequest(t *testing.T) {
	assert := assert.New(t)

	// empty request
	assert.Error(ValidateUpdateSemaphoreRequest(&gracklepb.UpdateSemaphoreRequest{}))

	// empty namespace name
	assert.Error(ValidateUpdateSemaphoreRequest(&gracklepb.UpdateSemaphoreRequest{
		SemaphoreName: "validname",
		Description:   "validdescription",
		Permits:       1,
	}))

	// empty semaphore name
	assert.Error(ValidateUpdateSemaphoreRequest(&gracklepb.UpdateSemaphoreRequest{
		NamespaceName: "validname",
		Description:   "validdescription",
		Permits:       1,
	}))

	// namespace name too long
	assert.Error(ValidateUpdateSemaphoreRequest(&gracklepb.UpdateSemaphoreRequest{
		NamespaceName: string(make([]byte, 129)),
		SemaphoreName: "validname",
		Description:   "validdescription",
		Permits:       1,
	}))

	// semaphore name too long
	assert.Error(ValidateUpdateSemaphoreRequest(&gracklepb.UpdateSemaphoreRequest{
		NamespaceName: "validname",
		SemaphoreName: string(make([]byte, 129)),
		Description:   "validdescription",
		Permits:       1,
	}))

	// invalid namespace name characters
	assert.Error(ValidateUpdateSemaphoreRequest(&gracklepb.UpdateSemaphoreRequest{
		NamespaceName: "invalid name",
		SemaphoreName: "validname",
		Description:   "validdescription",
		Permits:       1,
	}))

	// invalid semaphore name characters
	assert.Error(ValidateUpdateSemaphoreRequest(&gracklepb.UpdateSemaphoreRequest{
		NamespaceName: "validname",
		SemaphoreName: "invalid name",
		Description:   "validdescription",
		Permits:       1,
	}))

	// permits must be greater than 0
	assert.Error(ValidateUpdateSemaphoreRequest(&gracklepb.UpdateSemaphoreRequest{
		NamespaceName: "validname",
		SemaphoreName: "validsemaphore",
		Description:   "validdescription",
		Permits:       0,
	}))

	// valid request
	assert.NoError(ValidateUpdateSemaphoreRequest(&gracklepb.UpdateSemaphoreRequest{
		NamespaceName: "validname",
		SemaphoreName: "validsemaphore",
		Description:   "validdescription",
		Permits:       1,
	}))
}

func TestValidateDeleteSemaphoreRequest(t *testing.T) {
	assert := assert.New(t)

	// empty request
	assert.Error(ValidateDeleteSemaphoreRequest(&gracklepb.DeleteSemaphoreRequest{}))

	// empty namespace name
	assert.Error(ValidateDeleteSemaphoreRequest(&gracklepb.DeleteSemaphoreRequest{
		SemaphoreName: "validname",
	}))

	// empty semaphore name
	assert.Error(ValidateDeleteSemaphoreRequest(&gracklepb.DeleteSemaphoreRequest{
		NamespaceName: "validname",
	}))

	// namespace name too long
	assert.Error(ValidateDeleteSemaphoreRequest(&gracklepb.DeleteSemaphoreRequest{
		NamespaceName: string(make([]byte, 129)),
		SemaphoreName: "validname",
	}))

	// semaphore name too long
	assert.Error(ValidateDeleteSemaphoreRequest(&gracklepb.DeleteSemaphoreRequest{
		NamespaceName: "validname",
		SemaphoreName: string(make([]byte, 129)),
	}))

	// invalid namespace name characters
	assert.Error(ValidateDeleteSemaphoreRequest(&gracklepb.DeleteSemaphoreRequest{
		NamespaceName: "invalid name",
		SemaphoreName: "validname",
	}))

	// invalid semaphore name characters
	assert.Error(ValidateDeleteSemaphoreRequest(&gracklepb.DeleteSemaphoreRequest{
		NamespaceName: "validname",
		SemaphoreName: "invalid name",
	}))

	// valid request
	assert.NoError(ValidateDeleteSemaphoreRequest(&gracklepb.DeleteSemaphoreRequest{
		NamespaceName: "validname",
		SemaphoreName: "validsemaphore",
	}))
}

func TestValidateListLocksRequest(t *testing.T) {
	assert := assert.New(t)

	// Test empty request - should fail due to missing namespace name
	assert.Error(ValidateListLocksRequest(&gracklepb.ListLocksRequest{}))

	// Test empty namespace name
	assert.Error(ValidateListLocksRequest(&gracklepb.ListLocksRequest{
		NamespaceName: "",
	}))

	// Test namespace name too long
	assert.Error(ValidateListLocksRequest(&gracklepb.ListLocksRequest{
		NamespaceName: string(make([]byte, 129)),
	}))

	// Test invalid namespace name characters
	assert.Error(ValidateListLocksRequest(&gracklepb.ListLocksRequest{
		NamespaceName: "invalid name",
	}))

	// Test valid request with no pagination
	assert.NoError(ValidateListLocksRequest(&gracklepb.ListLocksRequest{
		NamespaceName:   "validname",
		PaginationToken: "",
		Limit:           0,
	}))

	// Test valid request with pagination token
	assert.NoError(ValidateListLocksRequest(&gracklepb.ListLocksRequest{
		NamespaceName:   "validname",
		PaginationToken: "dGVzdA==",
		Limit:           0,
	}))

	// Test valid request with limit
	assert.NoError(ValidateListLocksRequest(&gracklepb.ListLocksRequest{
		NamespaceName:   "validname",
		PaginationToken: "",
		Limit:           50,
	}))

	// Test valid request with both pagination token and limit
	assert.NoError(ValidateListLocksRequest(&gracklepb.ListLocksRequest{
		NamespaceName:   "validname",
		PaginationToken: "dGVzdA==",
		Limit:           100,
	}))

	// Test invalid pagination token (not base64)
	assert.Error(ValidateListLocksRequest(&gracklepb.ListLocksRequest{
		NamespaceName:   "validname",
		PaginationToken: "invalid-base64!@#",
		Limit:           50,
	}))

	// Test pagination token too long
	longToken := string(make([]byte, 1025))
	assert.Error(ValidateListLocksRequest(&gracklepb.ListLocksRequest{
		NamespaceName:   "validname",
		PaginationToken: longToken,
		Limit:           50,
	}))

	// Test limit too high
	assert.Error(ValidateListLocksRequest(&gracklepb.ListLocksRequest{
		NamespaceName:   "validname",
		PaginationToken: "",
		Limit:           101,
	}))

	// Test negative limit
	assert.Error(ValidateListLocksRequest(&gracklepb.ListLocksRequest{
		NamespaceName:   "validname",
		PaginationToken: "",
		Limit:           -1,
	}))

	// Test edge case: limit at maximum
	assert.NoError(ValidateListLocksRequest(&gracklepb.ListLocksRequest{
		NamespaceName:   "validname",
		PaginationToken: "",
		Limit:           100,
	}))

	// Test valid base64 token at maximum length
	validMaxToken := base64.StdEncoding.EncodeToString(make([]byte, 768)) // 768 bytes = 1024 base64 chars
	assert.NoError(ValidateListLocksRequest(&gracklepb.ListLocksRequest{
		NamespaceName:   "validname",
		PaginationToken: validMaxToken,
		Limit:           50,
	}))
}

func TestValidateListSemaphoresRequest(t *testing.T) {
	assert := assert.New(t)

	// Test empty request - should fail due to missing namespace name
	assert.Error(ValidateListSemaphoresRequest(&gracklepb.ListSemaphoresRequest{}))

	// Test empty namespace name
	assert.Error(ValidateListSemaphoresRequest(&gracklepb.ListSemaphoresRequest{
		NamespaceName: "",
	}))

	// Test namespace name too long
	assert.Error(ValidateListSemaphoresRequest(&gracklepb.ListSemaphoresRequest{
		NamespaceName: string(make([]byte, 129)),
	}))

	// Test invalid namespace name characters
	assert.Error(ValidateListSemaphoresRequest(&gracklepb.ListSemaphoresRequest{
		NamespaceName: "invalid name",
	}))

	// Test valid request with no pagination
	assert.NoError(ValidateListSemaphoresRequest(&gracklepb.ListSemaphoresRequest{
		NamespaceName:   "validname",
		PaginationToken: "",
		Limit:           0,
	}))

	// Test valid request with pagination token
	assert.NoError(ValidateListSemaphoresRequest(&gracklepb.ListSemaphoresRequest{
		NamespaceName:   "validname",
		PaginationToken: "dGVzdA==",
		Limit:           0,
	}))

	// Test valid request with limit
	assert.NoError(ValidateListSemaphoresRequest(&gracklepb.ListSemaphoresRequest{
		NamespaceName:   "validname",
		PaginationToken: "",
		Limit:           50,
	}))

	// Test valid request with both pagination token and limit
	assert.NoError(ValidateListSemaphoresRequest(&gracklepb.ListSemaphoresRequest{
		NamespaceName:   "validname",
		PaginationToken: "dGVzdA==",
		Limit:           100,
	}))

	// Test invalid pagination token (not base64)
	assert.Error(ValidateListSemaphoresRequest(&gracklepb.ListSemaphoresRequest{
		NamespaceName:   "validname",
		PaginationToken: "invalid-base64!@#",
		Limit:           50,
	}))

	// Test pagination token too long
	longToken := string(make([]byte, 1025))
	assert.Error(ValidateListSemaphoresRequest(&gracklepb.ListSemaphoresRequest{
		NamespaceName:   "validname",
		PaginationToken: longToken,
		Limit:           50,
	}))

	// Test limit too high
	assert.Error(ValidateListSemaphoresRequest(&gracklepb.ListSemaphoresRequest{
		NamespaceName:   "validname",
		PaginationToken: "",
		Limit:           101,
	}))

	// Test negative limit
	assert.Error(ValidateListSemaphoresRequest(&gracklepb.ListSemaphoresRequest{
		NamespaceName:   "validname",
		PaginationToken: "",
		Limit:           -1,
	}))

	// Test edge case: limit at maximum
	assert.NoError(ValidateListSemaphoresRequest(&gracklepb.ListSemaphoresRequest{
		NamespaceName:   "validname",
		PaginationToken: "",
		Limit:           100,
	}))

	// Test valid base64 token at maximum length
	validMaxToken := base64.StdEncoding.EncodeToString(make([]byte, 768)) // 768 bytes = 1024 base64 chars
	assert.NoError(ValidateListSemaphoresRequest(&gracklepb.ListSemaphoresRequest{
		NamespaceName:   "validname",
		PaginationToken: validMaxToken,
		Limit:           50,
	}))
}

func TestValidateAcquireSemaphoreRequest(t *testing.T) {
	assert := assert.New(t)

	// empty request
	assert.Error(ValidateAcquireSemaphoreRequest(&gracklepb.AcquireSemaphoreRequest{}))

	// empty namespace name
	assert.Error(ValidateAcquireSemaphoreRequest(&gracklepb.AcquireSemaphoreRequest{
		SemaphoreName: "validname",
		ProcessId:     "proc1",
	}))

	// empty semaphore name
	assert.Error(ValidateAcquireSemaphoreRequest(&gracklepb.AcquireSemaphoreRequest{
		NamespaceName: "validname",
		ProcessId:     "proc1",
	}))

	// empty process id
	assert.Error(ValidateAcquireSemaphoreRequest(&gracklepb.AcquireSemaphoreRequest{
		NamespaceName: "validname",
		SemaphoreName: "validname",
	}))

	// namespace name too long
	assert.Error(ValidateAcquireSemaphoreRequest(&gracklepb.AcquireSemaphoreRequest{
		NamespaceName: string(make([]byte, 129)),
		SemaphoreName: "validname",
		ProcessId:     "proc1",
	}))

	// semaphore name too long
	assert.Error(ValidateAcquireSemaphoreRequest(&gracklepb.AcquireSemaphoreRequest{
		NamespaceName: "validname",
		SemaphoreName: string(make([]byte, 129)),
		ProcessId:     "proc1",
	}))

	// process id too long
	assert.Error(ValidateAcquireSemaphoreRequest(&gracklepb.AcquireSemaphoreRequest{
		NamespaceName: "validname",
		SemaphoreName: "validname",
		ProcessId:     string(make([]byte, 129)),
	}))

	// invalid namespace name characters
	assert.Error(ValidateAcquireSemaphoreRequest(&gracklepb.AcquireSemaphoreRequest{
		NamespaceName: "invalid name",
		SemaphoreName: "validname",
		ProcessId:     "proc1",
	}))

	// invalid semaphore name characters
	assert.Error(ValidateAcquireSemaphoreRequest(&gracklepb.AcquireSemaphoreRequest{
		NamespaceName: "validname",
		SemaphoreName: "invalid name",
		ProcessId:     "proc1",
	}))

	// invalid process id characters
	assert.Error(ValidateAcquireSemaphoreRequest(&gracklepb.AcquireSemaphoreRequest{
		NamespaceName: "validname",
		SemaphoreName: "validname",
		ProcessId:     "invalid process id",
	}))

	// valid request
	assert.NoError(ValidateAcquireSemaphoreRequest(&gracklepb.AcquireSemaphoreRequest{
		NamespaceName: "validname",
		SemaphoreName: "validsemaphore",
		ProcessId:     "proc1",
	}))
}
