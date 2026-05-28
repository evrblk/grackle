package preview

import (
	"encoding/base64"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	gracklepb "github.com/evrblk/evrblk-go/grackle/preview"
)

func TestValidateCreateNamespaceRequest(t *testing.T) {
	// empty request
	assert.Error(t, ValidateCreateNamespaceRequest(&gracklepb.CreateNamespaceRequest{}))

	// empty name
	assert.Error(t, ValidateCreateNamespaceRequest(&gracklepb.CreateNamespaceRequest{
		Name: "",
	}))

	// name too long
	assert.Error(t, ValidateCreateNamespaceRequest(&gracklepb.CreateNamespaceRequest{
		Name: string(make([]byte, 129)),
	}))

	// invalid name characters
	assert.Error(t, ValidateCreateNamespaceRequest(&gracklepb.CreateNamespaceRequest{
		Name: "invalid name",
	}))

	// description too long
	assert.Error(t, ValidateCreateNamespaceRequest(&gracklepb.CreateNamespaceRequest{
		Name:        "validname",
		Description: string(make([]byte, 1025)),
	}))

	// valid request
	assert.NoError(t, ValidateCreateNamespaceRequest(&gracklepb.CreateNamespaceRequest{
		Name:        "validname",
		Description: "Valid description",
	}))
}

func TestValidateGetNamespaceRequest(t *testing.T) {
	// empty request
	assert.Error(t, ValidateGetNamespaceRequest(&gracklepb.GetNamespaceRequest{}))

	// empty namespace name
	assert.Error(t, ValidateGetNamespaceRequest(&gracklepb.GetNamespaceRequest{
		NamespaceName: "",
	}))

	// namespace name too long
	assert.Error(t, ValidateGetNamespaceRequest(&gracklepb.GetNamespaceRequest{
		NamespaceName: string(make([]byte, 129)),
	}))

	// invalid namespace name characters
	assert.Error(t, ValidateGetNamespaceRequest(&gracklepb.GetNamespaceRequest{
		NamespaceName: "invalid name",
	}))

	// valid request
	assert.NoError(t, ValidateGetNamespaceRequest(&gracklepb.GetNamespaceRequest{
		NamespaceName: "validname",
	}))
}

func TestValidateUpdateNamespaceRequest(t *testing.T) {
	// empty request
	assert.Error(t, ValidateUpdateNamespaceRequest(&gracklepb.UpdateNamespaceRequest{}))

	// empty namespace name
	assert.Error(t, ValidateUpdateNamespaceRequest(&gracklepb.UpdateNamespaceRequest{
		NamespaceName: "",
	}))

	// namespace name too long
	assert.Error(t, ValidateUpdateNamespaceRequest(&gracklepb.UpdateNamespaceRequest{
		NamespaceName: string(make([]byte, 129)),
	}))

	// invalid namespace name characters
	assert.Error(t, ValidateUpdateNamespaceRequest(&gracklepb.UpdateNamespaceRequest{
		NamespaceName: "invalid name",
	}))

	// description too long
	assert.Error(t, ValidateUpdateNamespaceRequest(&gracklepb.UpdateNamespaceRequest{
		NamespaceName: "validname",
		Description:   string(make([]byte, 1025)),
	}))

	// valid request
	assert.NoError(t, ValidateUpdateNamespaceRequest(&gracklepb.UpdateNamespaceRequest{
		NamespaceName: "validname",
		Description:   "Valid description",
	}))
}

func TestValidateDeleteNamespaceRequest(t *testing.T) {
	// empty request
	assert.Error(t, ValidateDeleteNamespaceRequest(&gracklepb.DeleteNamespaceRequest{}))

	// empty namespace name
	assert.Error(t, ValidateDeleteNamespaceRequest(&gracklepb.DeleteNamespaceRequest{
		NamespaceName: "",
	}))

	// namespace name too long
	assert.Error(t, ValidateDeleteNamespaceRequest(&gracklepb.DeleteNamespaceRequest{
		NamespaceName: string(make([]byte, 129)),
	}))

	// invalid namespace name characters
	assert.Error(t, ValidateDeleteNamespaceRequest(&gracklepb.DeleteNamespaceRequest{
		NamespaceName: "invalid name",
	}))

	// valid request
	assert.NoError(t, ValidateDeleteNamespaceRequest(&gracklepb.DeleteNamespaceRequest{
		NamespaceName: "validname",
	}))
}

func TestValidateListNamespacesRequest(t *testing.T) {
	// Test empty request - should pass as pagination fields are optional
	assert.NoError(t, ValidateListNamespacesRequest(&gracklepb.ListNamespacesRequest{}))

	// Test valid request with no pagination
	assert.NoError(t, ValidateListNamespacesRequest(&gracklepb.ListNamespacesRequest{
		PaginationToken: "",
		Limit:           0,
	}))

	// Test valid request with pagination token
	assert.NoError(t, ValidateListNamespacesRequest(&gracklepb.ListNamespacesRequest{
		PaginationToken: "dGVzdA==",
		Limit:           0,
	}))

	// Test valid request with limit
	assert.NoError(t, ValidateListNamespacesRequest(&gracklepb.ListNamespacesRequest{
		PaginationToken: "",
		Limit:           50,
	}))

	// Test valid request with both pagination token and limit
	assert.NoError(t, ValidateListNamespacesRequest(&gracklepb.ListNamespacesRequest{
		PaginationToken: "dGVzdA==",
		Limit:           100,
	}))

	// Test invalid pagination token (not base64)
	assert.Error(t, ValidateListNamespacesRequest(&gracklepb.ListNamespacesRequest{
		PaginationToken: "invalid-base64!@#",
		Limit:           50,
	}))

	// Test pagination token too long
	longToken := string(make([]byte, 1025))
	assert.Error(t, ValidateListNamespacesRequest(&gracklepb.ListNamespacesRequest{
		PaginationToken: longToken,
		Limit:           50,
	}))

	// Test limit too high
	assert.Error(t, ValidateListNamespacesRequest(&gracklepb.ListNamespacesRequest{
		PaginationToken: "",
		Limit:           101,
	}))

	// Test negative limit
	assert.Error(t, ValidateListNamespacesRequest(&gracklepb.ListNamespacesRequest{
		PaginationToken: "",
		Limit:           -1,
	}))

	// Test edge case: limit at maximum
	assert.NoError(t, ValidateListNamespacesRequest(&gracklepb.ListNamespacesRequest{
		PaginationToken: "",
		Limit:           100,
	}))

	// Test edge case: pagination token at maximum length
	maxToken := string(make([]byte, 1024)) // maxPaginationTokenLength
	assert.Error(t, ValidateListNamespacesRequest(&gracklepb.ListNamespacesRequest{
		PaginationToken: maxToken, // This will fail base64 validation
		Limit:           50,
	}))

	// Test valid base64 token at maximum length
	validMaxToken := base64.StdEncoding.EncodeToString(make([]byte, 768)) // 768 bytes = 1024 base64 chars
	assert.NoError(t, ValidateListNamespacesRequest(&gracklepb.ListNamespacesRequest{
		PaginationToken: validMaxToken,
		Limit:           50,
	}))
}

func TestValidateCreateWaitGroupRequest(t *testing.T) {
	// empty request
	assert.Error(t, ValidateCreateWaitGroupRequest(&gracklepb.CreateWaitGroupRequest{}))

	// empty namespace name
	assert.Error(t, ValidateCreateWaitGroupRequest(&gracklepb.CreateWaitGroupRequest{
		WaitGroupName: "validname",
		Counter:       1,
	}))

	// empty wait group name
	assert.Error(t, ValidateCreateWaitGroupRequest(&gracklepb.CreateWaitGroupRequest{
		NamespaceName: "validname",
		Counter:       1,
	}))

	// namespace name too long
	assert.Error(t, ValidateCreateWaitGroupRequest(&gracklepb.CreateWaitGroupRequest{
		NamespaceName: string(make([]byte, 129)),
		WaitGroupName: "validname",
		Counter:       1,
	}))

	// wait group name too long
	assert.Error(t, ValidateCreateWaitGroupRequest(&gracklepb.CreateWaitGroupRequest{
		NamespaceName: "validname",
		WaitGroupName: string(make([]byte, 129)),
		Counter:       1,
	}))

	// invalid namespace name characters
	assert.Error(t, ValidateCreateWaitGroupRequest(&gracklepb.CreateWaitGroupRequest{
		NamespaceName: "invalid name",
		WaitGroupName: "validname",
		Counter:       1,
	}))

	// invalid wait group name characters
	assert.Error(t, ValidateCreateWaitGroupRequest(&gracklepb.CreateWaitGroupRequest{
		NamespaceName: "validname",
		WaitGroupName: "invalid name",
		Counter:       1,
	}))

	// counter must be greater than 0
	assert.Error(t, ValidateCreateWaitGroupRequest(&gracklepb.CreateWaitGroupRequest{
		NamespaceName: "validname",
		WaitGroupName: "validwaitgroup",
		Counter:       0,
	}))

	// valid request
	assert.NoError(t, ValidateCreateWaitGroupRequest(&gracklepb.CreateWaitGroupRequest{
		NamespaceName: "validname",
		WaitGroupName: "validwaitgroup",
		Counter:       1,
	}))
}

func TestValidateGetWaitGroupRequest(t *testing.T) {
	// empty request
	assert.Error(t, ValidateGetWaitGroupRequest(&gracklepb.GetWaitGroupRequest{}))

	// empty namespace name
	assert.Error(t, ValidateGetWaitGroupRequest(&gracklepb.GetWaitGroupRequest{
		WaitGroupName: "validname",
	}))

	// empty wait group name
	assert.Error(t, ValidateGetWaitGroupRequest(&gracklepb.GetWaitGroupRequest{
		NamespaceName: "validname",
	}))

	// namespace name too long
	assert.Error(t, ValidateGetWaitGroupRequest(&gracklepb.GetWaitGroupRequest{
		NamespaceName: string(make([]byte, 129)),
		WaitGroupName: "validname",
	}))

	// wait group name too long
	assert.Error(t, ValidateGetWaitGroupRequest(&gracklepb.GetWaitGroupRequest{
		NamespaceName: "validname",
		WaitGroupName: string(make([]byte, 129)),
	}))

	// invalid namespace name characters
	assert.Error(t, ValidateGetWaitGroupRequest(&gracklepb.GetWaitGroupRequest{
		NamespaceName: "invalid name",
		WaitGroupName: "validname",
	}))

	// invalid wait group name characters
	assert.Error(t, ValidateGetWaitGroupRequest(&gracklepb.GetWaitGroupRequest{
		NamespaceName: "validname",
		WaitGroupName: "invalid name",
	}))

	// valid request
	assert.NoError(t, ValidateGetWaitGroupRequest(&gracklepb.GetWaitGroupRequest{
		NamespaceName: "validname",
		WaitGroupName: "validwaitgroup",
	}))
}

func TestValidateAddJobsToWaitGroupRequest(t *testing.T) {
	// empty request
	assert.Error(t, ValidateAddJobsToWaitGroupRequest(&gracklepb.AddJobsToWaitGroupRequest{}))

	// empty namespace name
	assert.Error(t, ValidateAddJobsToWaitGroupRequest(&gracklepb.AddJobsToWaitGroupRequest{
		WaitGroupName: "validname",
	}))

	// empty wait group name
	assert.Error(t, ValidateAddJobsToWaitGroupRequest(&gracklepb.AddJobsToWaitGroupRequest{
		NamespaceName: "validname",
	}))

	// namespace name too long
	assert.Error(t, ValidateAddJobsToWaitGroupRequest(&gracklepb.AddJobsToWaitGroupRequest{
		NamespaceName: string(make([]byte, 129)),
		WaitGroupName: "validname",
	}))

	// wait group name too long
	assert.Error(t, ValidateAddJobsToWaitGroupRequest(&gracklepb.AddJobsToWaitGroupRequest{
		NamespaceName: "validname",
		WaitGroupName: string(make([]byte, 129)),
	}))

	// invalid namespace name characters
	assert.Error(t, ValidateAddJobsToWaitGroupRequest(&gracklepb.AddJobsToWaitGroupRequest{
		NamespaceName: "invalid name",
		WaitGroupName: "validname",
	}))

	// invalid wait group name characters
	assert.Error(t, ValidateAddJobsToWaitGroupRequest(&gracklepb.AddJobsToWaitGroupRequest{
		NamespaceName: "validname",
		WaitGroupName: "invalid name",
	}))

	// valid request
	assert.NoError(t, ValidateAddJobsToWaitGroupRequest(&gracklepb.AddJobsToWaitGroupRequest{
		NamespaceName: "validname",
		WaitGroupName: "validwaitgroup",
	}))
}

func TestValidateDeleteWaitGroupRequest(t *testing.T) {
	// empty request
	assert.Error(t, ValidateDeleteWaitGroupRequest(&gracklepb.DeleteWaitGroupRequest{}))

	// empty namespace name
	assert.Error(t, ValidateDeleteWaitGroupRequest(&gracklepb.DeleteWaitGroupRequest{
		WaitGroupName: "validname",
	}))

	// empty wait group name
	assert.Error(t, ValidateDeleteWaitGroupRequest(&gracklepb.DeleteWaitGroupRequest{
		NamespaceName: "validname",
	}))

	// namespace name too long
	assert.Error(t, ValidateDeleteWaitGroupRequest(&gracklepb.DeleteWaitGroupRequest{
		NamespaceName: string(make([]byte, 129)),
		WaitGroupName: "validname",
	}))

	// wait group name too long
	assert.Error(t, ValidateDeleteWaitGroupRequest(&gracklepb.DeleteWaitGroupRequest{
		NamespaceName: "validname",
		WaitGroupName: string(make([]byte, 129)),
	}))

	// invalid namespace name characters
	assert.Error(t, ValidateDeleteWaitGroupRequest(&gracklepb.DeleteWaitGroupRequest{
		NamespaceName: "invalid name",
		WaitGroupName: "validname",
	}))

	// invalid wait group name characters
	assert.Error(t, ValidateDeleteWaitGroupRequest(&gracklepb.DeleteWaitGroupRequest{
		NamespaceName: "validname",
		WaitGroupName: "invalid name",
	}))

	// valid request
	assert.NoError(t, ValidateDeleteWaitGroupRequest(&gracklepb.DeleteWaitGroupRequest{
		NamespaceName: "validname",
		WaitGroupName: "validwaitgroup",
	}))
}

func TestValidateWaitForWaitGroupRequest(t *testing.T) {
	// empty request
	assert.Error(t, ValidateWaitForWaitGroupRequest(&gracklepb.WaitForWaitGroupRequest{}))

	// missing namespace name
	assert.Error(t, ValidateWaitForWaitGroupRequest(&gracklepb.WaitForWaitGroupRequest{
		WaitGroupName:  "validname",
		TimeoutSeconds: 10,
	}))

	// missing wait group name
	assert.Error(t, ValidateWaitForWaitGroupRequest(&gracklepb.WaitForWaitGroupRequest{
		NamespaceName:  "validname",
		TimeoutSeconds: 10,
	}))

	// namespace name too long
	assert.Error(t, ValidateWaitForWaitGroupRequest(&gracklepb.WaitForWaitGroupRequest{
		NamespaceName:  string(make([]byte, 129)),
		WaitGroupName:  "validname",
		TimeoutSeconds: 10,
	}))

	// wait group name too long
	assert.Error(t, ValidateWaitForWaitGroupRequest(&gracklepb.WaitForWaitGroupRequest{
		NamespaceName:  "validname",
		WaitGroupName:  string(make([]byte, 129)),
		TimeoutSeconds: 10,
	}))

	// invalid namespace name characters
	assert.Error(t, ValidateWaitForWaitGroupRequest(&gracklepb.WaitForWaitGroupRequest{
		NamespaceName:  "invalid name",
		WaitGroupName:  "validname",
		TimeoutSeconds: 10,
	}))

	// invalid wait group name characters
	assert.Error(t, ValidateWaitForWaitGroupRequest(&gracklepb.WaitForWaitGroupRequest{
		NamespaceName:  "validname",
		WaitGroupName:  "invalid name",
		TimeoutSeconds: 10,
	}))

	// timeout seconds zero
	assert.Error(t, ValidateWaitForWaitGroupRequest(&gracklepb.WaitForWaitGroupRequest{
		NamespaceName:  "validname",
		WaitGroupName:  "validname",
		TimeoutSeconds: 0,
	}))

	// timeout seconds negative
	assert.Error(t, ValidateWaitForWaitGroupRequest(&gracklepb.WaitForWaitGroupRequest{
		NamespaceName:  "validname",
		WaitGroupName:  "validname",
		TimeoutSeconds: -1,
	}))

	// timeout seconds too high
	assert.Error(t, ValidateWaitForWaitGroupRequest(&gracklepb.WaitForWaitGroupRequest{
		NamespaceName:  "validname",
		WaitGroupName:  "validname",
		TimeoutSeconds: 301,
	}))

	// valid request
	assert.NoError(t, ValidateWaitForWaitGroupRequest(&gracklepb.WaitForWaitGroupRequest{
		NamespaceName:  "validname",
		WaitGroupName:  "validname",
		TimeoutSeconds: 10,
	}))

	// valid request with maximum timeout
	assert.NoError(t, ValidateWaitForWaitGroupRequest(&gracklepb.WaitForWaitGroupRequest{
		NamespaceName:  "validname",
		WaitGroupName:  "validname",
		TimeoutSeconds: 300,
	}))
}

func TestValidateListWaitGroupsRequest(t *testing.T) {
	// Test empty request - should fail due to missing namespace name
	assert.Error(t, ValidateListWaitGroupsRequest(&gracklepb.ListWaitGroupsRequest{}))

	// Test empty namespace name
	assert.Error(t, ValidateListWaitGroupsRequest(&gracklepb.ListWaitGroupsRequest{
		NamespaceName: "",
	}))

	// Test namespace name too long
	assert.Error(t, ValidateListWaitGroupsRequest(&gracklepb.ListWaitGroupsRequest{
		NamespaceName: string(make([]byte, 129)),
	}))

	// Test invalid namespace name characters
	assert.Error(t, ValidateListWaitGroupsRequest(&gracklepb.ListWaitGroupsRequest{
		NamespaceName: "invalid name",
	}))

	// Test valid request with no pagination
	assert.NoError(t, ValidateListWaitGroupsRequest(&gracklepb.ListWaitGroupsRequest{
		NamespaceName:   "validname",
		PaginationToken: "",
		Limit:           0,
	}))

	// Test valid request with pagination token
	assert.NoError(t, ValidateListWaitGroupsRequest(&gracklepb.ListWaitGroupsRequest{
		NamespaceName:   "validname",
		PaginationToken: "dGVzdA==",
		Limit:           0,
	}))

	// Test valid request with limit
	assert.NoError(t, ValidateListWaitGroupsRequest(&gracklepb.ListWaitGroupsRequest{
		NamespaceName:   "validname",
		PaginationToken: "",
		Limit:           50,
	}))

	// Test valid request with both pagination token and limit
	assert.NoError(t, ValidateListWaitGroupsRequest(&gracklepb.ListWaitGroupsRequest{
		NamespaceName:   "validname",
		PaginationToken: "dGVzdA==",
		Limit:           100,
	}))

	// Test invalid pagination token (not base64)
	assert.Error(t, ValidateListWaitGroupsRequest(&gracklepb.ListWaitGroupsRequest{
		NamespaceName:   "validname",
		PaginationToken: "invalid-base64!@#",
		Limit:           50,
	}))

	// Test pagination token too long
	longToken := string(make([]byte, 1025))
	assert.Error(t, ValidateListWaitGroupsRequest(&gracklepb.ListWaitGroupsRequest{
		NamespaceName:   "validname",
		PaginationToken: longToken,
		Limit:           50,
	}))

	// Test limit too high
	assert.Error(t, ValidateListWaitGroupsRequest(&gracklepb.ListWaitGroupsRequest{
		NamespaceName:   "validname",
		PaginationToken: "",
		Limit:           101,
	}))

	// Test negative limit
	assert.Error(t, ValidateListWaitGroupsRequest(&gracklepb.ListWaitGroupsRequest{
		NamespaceName:   "validname",
		PaginationToken: "",
		Limit:           -1,
	}))

	// Test edge case: limit at maximum
	assert.NoError(t, ValidateListWaitGroupsRequest(&gracklepb.ListWaitGroupsRequest{
		NamespaceName:   "validname",
		PaginationToken: "",
		Limit:           100,
	}))

	// Test valid base64 token at maximum length
	validMaxToken := base64.StdEncoding.EncodeToString(make([]byte, 768)) // 768 bytes = 1024 base64 chars
	assert.NoError(t, ValidateListWaitGroupsRequest(&gracklepb.ListWaitGroupsRequest{
		NamespaceName:   "validname",
		PaginationToken: validMaxToken,
		Limit:           50,
	}))
}

func TestValidateCompleteJobsFromWaitGroupRequest(t *testing.T) {
	// empty request
	assert.Error(t, ValidateCompleteJobsFromWaitGroupRequest(&gracklepb.CompleteJobsFromWaitGroupRequest{}))

	// empty namespace name
	assert.Error(t, ValidateCompleteJobsFromWaitGroupRequest(&gracklepb.CompleteJobsFromWaitGroupRequest{
		WaitGroupName: "validname",
		ProcessIds:    []string{"proc1"},
	}))

	// empty wait group name
	assert.Error(t, ValidateCompleteJobsFromWaitGroupRequest(&gracklepb.CompleteJobsFromWaitGroupRequest{
		NamespaceName: "validname",
		ProcessIds:    []string{"proc1"},
	}))

	// namespace name too long
	assert.Error(t, ValidateCompleteJobsFromWaitGroupRequest(&gracklepb.CompleteJobsFromWaitGroupRequest{
		NamespaceName: string(make([]byte, 129)),
		WaitGroupName: "validname",
		ProcessIds:    []string{"proc1"},
	}))

	// wait group name too long
	assert.Error(t, ValidateCompleteJobsFromWaitGroupRequest(&gracklepb.CompleteJobsFromWaitGroupRequest{
		NamespaceName: "validname",
		WaitGroupName: string(make([]byte, 129)),
		ProcessIds:    []string{"proc1"},
	}))

	// invalid namespace name characters
	assert.Error(t, ValidateCompleteJobsFromWaitGroupRequest(&gracklepb.CompleteJobsFromWaitGroupRequest{
		NamespaceName: "invalid name",
		WaitGroupName: "validname",
		ProcessIds:    []string{"proc1"},
	}))

	// invalid wait group name characters
	assert.Error(t, ValidateCompleteJobsFromWaitGroupRequest(&gracklepb.CompleteJobsFromWaitGroupRequest{
		NamespaceName: "validname",
		WaitGroupName: "invalid name",
		ProcessIds:    []string{"proc1"},
	}))

	// too many process ids
	processIds := make([]string, 51)
	for i := 0; i < 51; i++ {
		processIds[i] = "proc1"
	}
	assert.Error(t, ValidateCompleteJobsFromWaitGroupRequest(&gracklepb.CompleteJobsFromWaitGroupRequest{
		NamespaceName: "validname",
		WaitGroupName: "validwaitgroup",
		ProcessIds:    processIds,
	}))

	// invalid process id
	assert.Error(t, ValidateCompleteJobsFromWaitGroupRequest(&gracklepb.CompleteJobsFromWaitGroupRequest{
		NamespaceName: "validname",
		WaitGroupName: "validwaitgroup",
		ProcessIds:    []string{"invalid process id"},
	}))

	// valid request
	assert.NoError(t, ValidateCompleteJobsFromWaitGroupRequest(&gracklepb.CompleteJobsFromWaitGroupRequest{
		NamespaceName: "validname",
		WaitGroupName: "validwaitgroup",
		ProcessIds:    []string{"proc1", "proc2"},
	}))
}

func TestValidateDeleteLockRequest(t *testing.T) {
	// empty request
	assert.Error(t, ValidateDeleteLockRequest(&gracklepb.DeleteLockRequest{}))

	// empty namespace name
	assert.Error(t, ValidateDeleteLockRequest(&gracklepb.DeleteLockRequest{
		LockName: "validname",
	}))

	// empty lock name
	assert.Error(t, ValidateDeleteLockRequest(&gracklepb.DeleteLockRequest{
		NamespaceName: "validname",
	}))

	// namespace name too long
	assert.Error(t, ValidateDeleteLockRequest(&gracklepb.DeleteLockRequest{
		NamespaceName: string(make([]byte, 129)),
		LockName:      "validname",
	}))

	// lock name too long
	assert.Error(t, ValidateDeleteLockRequest(&gracklepb.DeleteLockRequest{
		NamespaceName: "validname",
		LockName:      string(make([]byte, 129)),
	}))

	// invalid namespace name characters
	assert.Error(t, ValidateDeleteLockRequest(&gracklepb.DeleteLockRequest{
		NamespaceName: "invalid name",
		LockName:      "validname",
	}))

	// invalid lock name characters
	assert.Error(t, ValidateDeleteLockRequest(&gracklepb.DeleteLockRequest{
		NamespaceName: "validname",
		LockName:      "invalid name",
	}))

	// valid request
	assert.NoError(t, ValidateDeleteLockRequest(&gracklepb.DeleteLockRequest{
		NamespaceName: "validname",
		LockName:      "validlock",
	}))
}

func TestValidateGetLockRequest(t *testing.T) {
	// empty request
	assert.Error(t, ValidateGetLockRequest(&gracklepb.GetLockRequest{}))

	// empty namespace name
	assert.Error(t, ValidateGetLockRequest(&gracklepb.GetLockRequest{
		LockName: "validname",
	}))

	// empty lock name
	assert.Error(t, ValidateGetLockRequest(&gracklepb.GetLockRequest{
		NamespaceName: "validname",
	}))

	// namespace name too long
	assert.Error(t, ValidateGetLockRequest(&gracklepb.GetLockRequest{
		NamespaceName: string(make([]byte, 129)),
		LockName:      "validname",
	}))

	// lock name too long
	assert.Error(t, ValidateGetLockRequest(&gracklepb.GetLockRequest{
		NamespaceName: "validname",
		LockName:      string(make([]byte, 129)),
	}))

	// invalid namespace name characters
	assert.Error(t, ValidateGetLockRequest(&gracklepb.GetLockRequest{
		NamespaceName: "invalid name",
		LockName:      "validname",
	}))

	// invalid lock name characters
	assert.Error(t, ValidateGetLockRequest(&gracklepb.GetLockRequest{
		NamespaceName: "validname",
		LockName:      "invalid name",
	}))

	// valid request
	assert.NoError(t, ValidateGetLockRequest(&gracklepb.GetLockRequest{
		NamespaceName: "validname",
		LockName:      "validlock",
	}))
}

func TestValidateReleaseLockRequest(t *testing.T) {
	// empty request
	assert.Error(t, ValidateReleaseLockRequest(&gracklepb.ReleaseLockRequest{}))

	// empty namespace name
	assert.Error(t, ValidateReleaseLockRequest(&gracklepb.ReleaseLockRequest{
		LockName: "validname",
		LeaseId:  "lease1",
	}))

	// empty lock name
	assert.Error(t, ValidateReleaseLockRequest(&gracklepb.ReleaseLockRequest{
		NamespaceName: "validname",
		LeaseId:       "lease1",
	}))

	// empty lease id
	assert.Error(t, ValidateReleaseLockRequest(&gracklepb.ReleaseLockRequest{
		NamespaceName: "validname",
		LockName:      "validname",
	}))

	// namespace name too long
	assert.Error(t, ValidateReleaseLockRequest(&gracklepb.ReleaseLockRequest{
		NamespaceName: string(make([]byte, 129)),
		LockName:      "validname",
		LeaseId:       "lease1",
	}))

	// lock name too long
	assert.Error(t, ValidateReleaseLockRequest(&gracklepb.ReleaseLockRequest{
		NamespaceName: "validname",
		LockName:      string(make([]byte, 129)),
		LeaseId:       "lease1",
	}))

	// lease id too long (not validated - strings can be any length)
	// assert.Error(t, ValidateReleaseLockRequest(&gracklepb.ReleaseLockRequest{
	// 	NamespaceName: "validname",
	// 	LockName:      "validname",
	// 	LeaseId:       string(make([]byte, 129)),
	// }))

	// invalid namespace name characters
	assert.Error(t, ValidateReleaseLockRequest(&gracklepb.ReleaseLockRequest{
		NamespaceName: "invalid name",
		LockName:      "validname",
		LeaseId:       "lease1",
	}))

	// invalid lock name characters
	assert.Error(t, ValidateReleaseLockRequest(&gracklepb.ReleaseLockRequest{
		NamespaceName: "validname",
		LockName:      "invalid name",
		LeaseId:       "lease1",
	}))

	// lease id can have any characters - no validation on lease_id format
	// assert.Error(t, ValidateReleaseLockRequest(&gracklepb.ReleaseLockRequest{
	// 	NamespaceName: "validname",
	// 	LockName:      "validname",
	// 	LeaseId:       "invalid lease id",
	// }))

	// valid request
	assert.NoError(t, ValidateReleaseLockRequest(&gracklepb.ReleaseLockRequest{
		NamespaceName: "validname",
		LockName:      "validlock",
		LeaseId:       "lease1",
	}))
}

func TestValidateAcquireLockRequest(t *testing.T) {
	// empty request
	assert.Error(t, ValidateAcquireLockRequest(&gracklepb.AcquireLockRequest{}))

	// empty namespace name
	assert.Error(t, ValidateAcquireLockRequest(&gracklepb.AcquireLockRequest{
		LockName: "validname",
		LeaseId:  "lease1",
	}))

	// empty lock name
	assert.Error(t, ValidateAcquireLockRequest(&gracklepb.AcquireLockRequest{
		NamespaceName: "validname",
		LeaseId:       "lease1",
	}))

	// empty lease id
	assert.Error(t, ValidateAcquireLockRequest(&gracklepb.AcquireLockRequest{
		NamespaceName: "validname",
		LockName:      "validname",
	}))

	// namespace name too long
	assert.Error(t, ValidateAcquireLockRequest(&gracklepb.AcquireLockRequest{
		NamespaceName: string(make([]byte, 129)),
		LockName:      "validname",
		LeaseId:       "lease1",
	}))

	// lock name too long
	assert.Error(t, ValidateAcquireLockRequest(&gracklepb.AcquireLockRequest{
		NamespaceName: "validname",
		LockName:      string(make([]byte, 129)),
		LeaseId:       "lease1",
	}))

	// lease id too long (not validated - strings can be any length)
	// assert.Error(t, ValidateAcquireLockRequest(&gracklepb.AcquireLockRequest{
	// 	NamespaceName: "validname",
	// 	LockName:      "validname",
	// 	LeaseId:       string(make([]byte, 129)),
	// }))

	// invalid namespace name characters
	assert.Error(t, ValidateAcquireLockRequest(&gracklepb.AcquireLockRequest{
		NamespaceName: "invalid name",
		LockName:      "validname",
		LeaseId:       "lease1",
	}))

	// invalid lock name characters
	assert.Error(t, ValidateAcquireLockRequest(&gracklepb.AcquireLockRequest{
		NamespaceName: "validname",
		LockName:      "invalid name",
		LeaseId:       "lease1",
	}))

	// lease id can have any characters - no validation on lease_id format
	// assert.Error(t, ValidateAcquireLockRequest(&gracklepb.AcquireLockRequest{
	// 	NamespaceName: "validname",
	// 	LockName:      "validname",
	// 	LeaseId:       "invalid lease id",
	// }))

	// expires_at no longer exists in the API
	// assert.Error(t, ValidateAcquireLockRequest(&gracklepb.AcquireLockRequest{
	// 	NamespaceName: "validname",
	// 	LockName:      "validlock",
	// 	LeaseId:       "lease1",
	// 	ExpiresAt:     0,
	// }))

	// valid request
	assert.NoError(t, ValidateAcquireLockRequest(&gracklepb.AcquireLockRequest{
		NamespaceName: "validname",
		LockName:      "validlock",
		LeaseId:       "lease1",
	}))
}

func TestValidateCreateSemaphoreRequest(t *testing.T) {
	// empty request
	assert.Error(t, ValidateCreateSemaphoreRequest(&gracklepb.CreateSemaphoreRequest{}))

	// empty namespace name
	assert.Error(t, ValidateCreateSemaphoreRequest(&gracklepb.CreateSemaphoreRequest{
		SemaphoreName: "validname",
		Description:   "validdescription",
		Permits:       1,
	}))

	// empty semaphore name
	assert.Error(t, ValidateCreateSemaphoreRequest(&gracklepb.CreateSemaphoreRequest{
		NamespaceName: "validname",
		Description:   "validdescription",
		Permits:       1,
	}))

	// namespace name too long
	assert.Error(t, ValidateCreateSemaphoreRequest(&gracklepb.CreateSemaphoreRequest{
		NamespaceName: string(make([]byte, 129)),
		SemaphoreName: "validname",
		Description:   "validdescription",
		Permits:       1,
	}))

	// semaphore name too long
	assert.Error(t, ValidateCreateSemaphoreRequest(&gracklepb.CreateSemaphoreRequest{
		NamespaceName: "validname",
		SemaphoreName: string(make([]byte, 129)),
		Description:   "validdescription",
		Permits:       1,
	}))

	// invalid namespace name characters
	assert.Error(t, ValidateCreateSemaphoreRequest(&gracklepb.CreateSemaphoreRequest{
		NamespaceName: "invalid name",
		SemaphoreName: "validname",
		Description:   "validdescription",
		Permits:       1,
	}))

	// invalid semaphore name characters
	assert.Error(t, ValidateCreateSemaphoreRequest(&gracklepb.CreateSemaphoreRequest{
		NamespaceName: "validname",
		SemaphoreName: "invalid name",
		Description:   "validdescription",
		Permits:       1,
	}))

	// permits must be greater than 0
	assert.Error(t, ValidateCreateSemaphoreRequest(&gracklepb.CreateSemaphoreRequest{
		NamespaceName: "validname",
		SemaphoreName: "validsemaphore",
		Description:   "validdescription",
		Permits:       0,
	}))

	// valid request
	assert.NoError(t, ValidateCreateSemaphoreRequest(&gracklepb.CreateSemaphoreRequest{
		NamespaceName: "validname",
		SemaphoreName: "validsemaphore",
		Description:   "validdescription",
		Permits:       1,
	}))
}

func TestValidateGetSemaphoreRequest(t *testing.T) {
	// empty request
	assert.Error(t, ValidateGetSemaphoreRequest(&gracklepb.GetSemaphoreRequest{}))

	// empty namespace name
	assert.Error(t, ValidateGetSemaphoreRequest(&gracklepb.GetSemaphoreRequest{
		SemaphoreName: "validname",
	}))

	// empty semaphore name
	assert.Error(t, ValidateGetSemaphoreRequest(&gracklepb.GetSemaphoreRequest{
		NamespaceName: "validname",
	}))

	// namespace name too long
	assert.Error(t, ValidateGetSemaphoreRequest(&gracklepb.GetSemaphoreRequest{
		NamespaceName: string(make([]byte, 129)),
		SemaphoreName: "validname",
	}))

	// semaphore name too long
	assert.Error(t, ValidateGetSemaphoreRequest(&gracklepb.GetSemaphoreRequest{
		NamespaceName: "validname",
		SemaphoreName: string(make([]byte, 129)),
	}))

	// invalid namespace name characters
	assert.Error(t, ValidateGetSemaphoreRequest(&gracklepb.GetSemaphoreRequest{
		NamespaceName: "invalid name",
		SemaphoreName: "validname",
	}))

	// invalid semaphore name characters
	assert.Error(t, ValidateGetSemaphoreRequest(&gracklepb.GetSemaphoreRequest{
		NamespaceName: "validname",
		SemaphoreName: "invalid name",
	}))

	// valid request
	assert.NoError(t, ValidateGetSemaphoreRequest(&gracklepb.GetSemaphoreRequest{
		NamespaceName: "validname",
		SemaphoreName: "validsemaphore",
	}))
}

func TestValidateReleaseSemaphoreRequest(t *testing.T) {
	// empty request
	assert.Error(t, ValidateReleaseSemaphoreRequest(&gracklepb.ReleaseSemaphoreRequest{}))

	// empty namespace name
	assert.Error(t, ValidateReleaseSemaphoreRequest(&gracklepb.ReleaseSemaphoreRequest{
		SemaphoreName: "validname",
		LeaseId:       "lease1",
	}))

	// empty semaphore name
	assert.Error(t, ValidateReleaseSemaphoreRequest(&gracklepb.ReleaseSemaphoreRequest{
		NamespaceName: "validname",
		LeaseId:       "lease1",
	}))

	// empty process id
	assert.Error(t, ValidateReleaseSemaphoreRequest(&gracklepb.ReleaseSemaphoreRequest{
		NamespaceName: "validname",
		SemaphoreName: "validname",
	}))

	// namespace name too long
	assert.Error(t, ValidateReleaseSemaphoreRequest(&gracklepb.ReleaseSemaphoreRequest{
		NamespaceName: string(make([]byte, 129)),
		SemaphoreName: "validname",
		LeaseId:       "lease1",
	}))

	// semaphore name too long
	assert.Error(t, ValidateReleaseSemaphoreRequest(&gracklepb.ReleaseSemaphoreRequest{
		NamespaceName: "validname",
		SemaphoreName: string(make([]byte, 129)),
		LeaseId:       "lease1",
	}))

	// lease id too long (not validated - strings can be any length)
	// assert.Error(t, ValidateReleaseSemaphoreRequest(&gracklepb.ReleaseSemaphoreRequest{
	// 	NamespaceName: "validname",
	// 	SemaphoreName: "validname",
	// 	LeaseId:       string(make([]byte, 129)),
	// }))

	// invalid namespace name characters
	assert.Error(t, ValidateReleaseSemaphoreRequest(&gracklepb.ReleaseSemaphoreRequest{
		NamespaceName: "invalid name",
		SemaphoreName: "validname",
		LeaseId:       "lease1",
	}))

	// invalid semaphore name characters
	assert.Error(t, ValidateReleaseSemaphoreRequest(&gracklepb.ReleaseSemaphoreRequest{
		NamespaceName: "validname",
		SemaphoreName: "invalid name",
		LeaseId:       "lease1",
	}))

	// lease id can have any characters - no validation on lease_id format
	// assert.Error(t, ValidateReleaseSemaphoreRequest(&gracklepb.ReleaseSemaphoreRequest{
	// 	NamespaceName: "validname",
	// 	SemaphoreName: "validname",
	// 	LeaseId:       "invalid lease id",
	// }))

	// valid request
	assert.NoError(t, ValidateReleaseSemaphoreRequest(&gracklepb.ReleaseSemaphoreRequest{
		NamespaceName: "validname",
		SemaphoreName: "validsemaphore",
		LeaseId:       "lease1",
	}))
}

func TestValidateUpdateSemaphoreRequest(t *testing.T) {
	// empty request
	assert.Error(t, ValidateUpdateSemaphoreRequest(&gracklepb.UpdateSemaphoreRequest{}))

	// empty namespace name
	assert.Error(t, ValidateUpdateSemaphoreRequest(&gracklepb.UpdateSemaphoreRequest{
		SemaphoreName: "validname",
		Description:   "validdescription",
		Permits:       1,
	}))

	// empty semaphore name
	assert.Error(t, ValidateUpdateSemaphoreRequest(&gracklepb.UpdateSemaphoreRequest{
		NamespaceName: "validname",
		Description:   "validdescription",
		Permits:       1,
	}))

	// namespace name too long
	assert.Error(t, ValidateUpdateSemaphoreRequest(&gracklepb.UpdateSemaphoreRequest{
		NamespaceName: string(make([]byte, 129)),
		SemaphoreName: "validname",
		Description:   "validdescription",
		Permits:       1,
	}))

	// semaphore name too long
	assert.Error(t, ValidateUpdateSemaphoreRequest(&gracklepb.UpdateSemaphoreRequest{
		NamespaceName: "validname",
		SemaphoreName: string(make([]byte, 129)),
		Description:   "validdescription",
		Permits:       1,
	}))

	// invalid namespace name characters
	assert.Error(t, ValidateUpdateSemaphoreRequest(&gracklepb.UpdateSemaphoreRequest{
		NamespaceName: "invalid name",
		SemaphoreName: "validname",
		Description:   "validdescription",
		Permits:       1,
	}))

	// invalid semaphore name characters
	assert.Error(t, ValidateUpdateSemaphoreRequest(&gracklepb.UpdateSemaphoreRequest{
		NamespaceName: "validname",
		SemaphoreName: "invalid name",
		Description:   "validdescription",
		Permits:       1,
	}))

	// permits must be greater than 0
	assert.Error(t, ValidateUpdateSemaphoreRequest(&gracklepb.UpdateSemaphoreRequest{
		NamespaceName: "validname",
		SemaphoreName: "validsemaphore",
		Description:   "validdescription",
		Permits:       0,
	}))

	// valid request
	assert.NoError(t, ValidateUpdateSemaphoreRequest(&gracklepb.UpdateSemaphoreRequest{
		NamespaceName: "validname",
		SemaphoreName: "validsemaphore",
		Description:   "validdescription",
		Permits:       1,
	}))
}

func TestValidateDeleteSemaphoreRequest(t *testing.T) {
	// empty request
	assert.Error(t, ValidateDeleteSemaphoreRequest(&gracklepb.DeleteSemaphoreRequest{}))

	// empty namespace name
	assert.Error(t, ValidateDeleteSemaphoreRequest(&gracklepb.DeleteSemaphoreRequest{
		SemaphoreName: "validname",
	}))

	// empty semaphore name
	assert.Error(t, ValidateDeleteSemaphoreRequest(&gracklepb.DeleteSemaphoreRequest{
		NamespaceName: "validname",
	}))

	// namespace name too long
	assert.Error(t, ValidateDeleteSemaphoreRequest(&gracklepb.DeleteSemaphoreRequest{
		NamespaceName: string(make([]byte, 129)),
		SemaphoreName: "validname",
	}))

	// semaphore name too long
	assert.Error(t, ValidateDeleteSemaphoreRequest(&gracklepb.DeleteSemaphoreRequest{
		NamespaceName: "validname",
		SemaphoreName: string(make([]byte, 129)),
	}))

	// invalid namespace name characters
	assert.Error(t, ValidateDeleteSemaphoreRequest(&gracklepb.DeleteSemaphoreRequest{
		NamespaceName: "invalid name",
		SemaphoreName: "validname",
	}))

	// invalid semaphore name characters
	assert.Error(t, ValidateDeleteSemaphoreRequest(&gracklepb.DeleteSemaphoreRequest{
		NamespaceName: "validname",
		SemaphoreName: "invalid name",
	}))

	// valid request
	assert.NoError(t, ValidateDeleteSemaphoreRequest(&gracklepb.DeleteSemaphoreRequest{
		NamespaceName: "validname",
		SemaphoreName: "validsemaphore",
	}))
}

func TestValidateListLocksRequest(t *testing.T) {
	// Test empty request - should fail due to missing namespace name
	assert.Error(t, ValidateListLocksRequest(&gracklepb.ListLocksRequest{}))

	// Test empty namespace name
	assert.Error(t, ValidateListLocksRequest(&gracklepb.ListLocksRequest{
		NamespaceName: "",
	}))

	// Test namespace name too long
	assert.Error(t, ValidateListLocksRequest(&gracklepb.ListLocksRequest{
		NamespaceName: string(make([]byte, 129)),
	}))

	// Test invalid namespace name characters
	assert.Error(t, ValidateListLocksRequest(&gracklepb.ListLocksRequest{
		NamespaceName: "invalid name",
	}))

	// Test valid request with no pagination
	assert.NoError(t, ValidateListLocksRequest(&gracklepb.ListLocksRequest{
		NamespaceName:   "validname",
		PaginationToken: "",
		Limit:           0,
	}))

	// Test valid request with pagination token
	assert.NoError(t, ValidateListLocksRequest(&gracklepb.ListLocksRequest{
		NamespaceName:   "validname",
		PaginationToken: "dGVzdA==",
		Limit:           0,
	}))

	// Test valid request with limit
	assert.NoError(t, ValidateListLocksRequest(&gracklepb.ListLocksRequest{
		NamespaceName:   "validname",
		PaginationToken: "",
		Limit:           50,
	}))

	// Test valid request with both pagination token and limit
	assert.NoError(t, ValidateListLocksRequest(&gracklepb.ListLocksRequest{
		NamespaceName:   "validname",
		PaginationToken: "dGVzdA==",
		Limit:           100,
	}))

	// Test invalid pagination token (not base64)
	assert.Error(t, ValidateListLocksRequest(&gracklepb.ListLocksRequest{
		NamespaceName:   "validname",
		PaginationToken: "invalid-base64!@#",
		Limit:           50,
	}))

	// Test pagination token too long
	longToken := string(make([]byte, 1025))
	assert.Error(t, ValidateListLocksRequest(&gracklepb.ListLocksRequest{
		NamespaceName:   "validname",
		PaginationToken: longToken,
		Limit:           50,
	}))

	// Test limit too high
	assert.Error(t, ValidateListLocksRequest(&gracklepb.ListLocksRequest{
		NamespaceName:   "validname",
		PaginationToken: "",
		Limit:           101,
	}))

	// Test negative limit
	assert.Error(t, ValidateListLocksRequest(&gracklepb.ListLocksRequest{
		NamespaceName:   "validname",
		PaginationToken: "",
		Limit:           -1,
	}))

	// Test edge case: limit at maximum
	assert.NoError(t, ValidateListLocksRequest(&gracklepb.ListLocksRequest{
		NamespaceName:   "validname",
		PaginationToken: "",
		Limit:           100,
	}))

	// Test valid base64 token at maximum length
	validMaxToken := base64.StdEncoding.EncodeToString(make([]byte, 768)) // 768 bytes = 1024 base64 chars
	assert.NoError(t, ValidateListLocksRequest(&gracklepb.ListLocksRequest{
		NamespaceName:   "validname",
		PaginationToken: validMaxToken,
		Limit:           50,
	}))
}

func TestValidateListSemaphoresRequest(t *testing.T) {
	// Test empty request - should fail due to missing namespace name
	assert.Error(t, ValidateListSemaphoresRequest(&gracklepb.ListSemaphoresRequest{}))

	// Test empty namespace name
	assert.Error(t, ValidateListSemaphoresRequest(&gracklepb.ListSemaphoresRequest{
		NamespaceName: "",
	}))

	// Test namespace name too long
	assert.Error(t, ValidateListSemaphoresRequest(&gracklepb.ListSemaphoresRequest{
		NamespaceName: string(make([]byte, 129)),
	}))

	// Test invalid namespace name characters
	assert.Error(t, ValidateListSemaphoresRequest(&gracklepb.ListSemaphoresRequest{
		NamespaceName: "invalid name",
	}))

	// Test valid request with no pagination
	assert.NoError(t, ValidateListSemaphoresRequest(&gracklepb.ListSemaphoresRequest{
		NamespaceName:   "validname",
		PaginationToken: "",
		Limit:           0,
	}))

	// Test valid request with pagination token
	assert.NoError(t, ValidateListSemaphoresRequest(&gracklepb.ListSemaphoresRequest{
		NamespaceName:   "validname",
		PaginationToken: "dGVzdA==",
		Limit:           0,
	}))

	// Test valid request with limit
	assert.NoError(t, ValidateListSemaphoresRequest(&gracklepb.ListSemaphoresRequest{
		NamespaceName:   "validname",
		PaginationToken: "",
		Limit:           50,
	}))

	// Test valid request with both pagination token and limit
	assert.NoError(t, ValidateListSemaphoresRequest(&gracklepb.ListSemaphoresRequest{
		NamespaceName:   "validname",
		PaginationToken: "dGVzdA==",
		Limit:           100,
	}))

	// Test invalid pagination token (not base64)
	assert.Error(t, ValidateListSemaphoresRequest(&gracklepb.ListSemaphoresRequest{
		NamespaceName:   "validname",
		PaginationToken: "invalid-base64!@#",
		Limit:           50,
	}))

	// Test pagination token too long
	longToken := string(make([]byte, 1025))
	assert.Error(t, ValidateListSemaphoresRequest(&gracklepb.ListSemaphoresRequest{
		NamespaceName:   "validname",
		PaginationToken: longToken,
		Limit:           50,
	}))

	// Test limit too high
	assert.Error(t, ValidateListSemaphoresRequest(&gracklepb.ListSemaphoresRequest{
		NamespaceName:   "validname",
		PaginationToken: "",
		Limit:           101,
	}))

	// Test negative limit
	assert.Error(t, ValidateListSemaphoresRequest(&gracklepb.ListSemaphoresRequest{
		NamespaceName:   "validname",
		PaginationToken: "",
		Limit:           -1,
	}))

	// Test edge case: limit at maximum
	assert.NoError(t, ValidateListSemaphoresRequest(&gracklepb.ListSemaphoresRequest{
		NamespaceName:   "validname",
		PaginationToken: "",
		Limit:           100,
	}))

	// Test valid base64 token at maximum length
	validMaxToken := base64.StdEncoding.EncodeToString(make([]byte, 768)) // 768 bytes = 1024 base64 chars
	assert.NoError(t, ValidateListSemaphoresRequest(&gracklepb.ListSemaphoresRequest{
		NamespaceName:   "validname",
		PaginationToken: validMaxToken,
		Limit:           50,
	}))
}

func TestValidateAcquireSemaphoreRequest(t *testing.T) {
	// empty request
	assert.Error(t, ValidateAcquireSemaphoreRequest(&gracklepb.AcquireSemaphoreRequest{}))

	// empty namespace name
	assert.Error(t, ValidateAcquireSemaphoreRequest(&gracklepb.AcquireSemaphoreRequest{
		SemaphoreName: "validname",
		LeaseId:       "lease1",
	}))

	// empty semaphore name
	assert.Error(t, ValidateAcquireSemaphoreRequest(&gracklepb.AcquireSemaphoreRequest{
		NamespaceName: "validname",
		LeaseId:       "lease1",
	}))

	// empty process id
	assert.Error(t, ValidateAcquireSemaphoreRequest(&gracklepb.AcquireSemaphoreRequest{
		NamespaceName: "validname",
		SemaphoreName: "validname",
	}))

	// namespace name too long
	assert.Error(t, ValidateAcquireSemaphoreRequest(&gracklepb.AcquireSemaphoreRequest{
		NamespaceName: string(make([]byte, 129)),
		SemaphoreName: "validname",
		LeaseId:       "lease1",
	}))

	// semaphore name too long
	assert.Error(t, ValidateAcquireSemaphoreRequest(&gracklepb.AcquireSemaphoreRequest{
		NamespaceName: "validname",
		SemaphoreName: string(make([]byte, 129)),
		LeaseId:       "lease1",
	}))

	// lease id too long (not validated - strings can be any length)
	// assert.Error(t, ValidateAcquireSemaphoreRequest(&gracklepb.AcquireSemaphoreRequest{
	// 	NamespaceName: "validname",
	// 	SemaphoreName: "validname",
	// 	LeaseId:       string(make([]byte, 129)),
	// }))

	// invalid namespace name characters
	assert.Error(t, ValidateAcquireSemaphoreRequest(&gracklepb.AcquireSemaphoreRequest{
		NamespaceName: "invalid name",
		SemaphoreName: "validname",
		LeaseId:       "lease1",
	}))

	// invalid semaphore name characters
	assert.Error(t, ValidateAcquireSemaphoreRequest(&gracklepb.AcquireSemaphoreRequest{
		NamespaceName: "validname",
		SemaphoreName: "invalid name",
		LeaseId:       "lease1",
	}))

	// lease id can have any characters - no validation on lease_id format
	// assert.Error(t, ValidateAcquireSemaphoreRequest(&gracklepb.AcquireSemaphoreRequest{
	// 	NamespaceName: "validname",
	// 	SemaphoreName: "validname",
	// 	LeaseId:       "invalid lease id",
	// }))

	// expires_at no longer exists in the API
	// assert.Error(t, ValidateAcquireSemaphoreRequest(&gracklepb.AcquireSemaphoreRequest{
	// 	NamespaceName: "validname",
	// 	SemaphoreName: "validsemaphore",
	// 	LeaseId:       "lease1",
	// 	ExpiresAt:     0,
	// }))

	// valid request
	assert.NoError(t, ValidateAcquireSemaphoreRequest(&gracklepb.AcquireSemaphoreRequest{
		NamespaceName: "validname",
		SemaphoreName: "validsemaphore",
		LeaseId:       "lease1",
	}))
}

func TestValidateListWaitGroupJobsRequest(t *testing.T) {
	// Test empty request - should fail due to missing namespace name
	assert.Error(t, ValidateListWaitGroupJobsRequest(&gracklepb.ListWaitGroupJobsRequest{}))

	// Test missing namespace name
	assert.Error(t, ValidateListWaitGroupJobsRequest(&gracklepb.ListWaitGroupJobsRequest{
		WaitGroupName: "validname",
	}))

	// Test missing wait group name
	assert.Error(t, ValidateListWaitGroupJobsRequest(&gracklepb.ListWaitGroupJobsRequest{
		NamespaceName: "validname",
	}))

	// Test invalid namespace name characters
	assert.Error(t, ValidateListWaitGroupJobsRequest(&gracklepb.ListWaitGroupJobsRequest{
		NamespaceName: "invalid name",
		WaitGroupName: "validname",
	}))

	// Test invalid wait group name characters
	assert.Error(t, ValidateListWaitGroupJobsRequest(&gracklepb.ListWaitGroupJobsRequest{
		NamespaceName: "validname",
		WaitGroupName: "invalid name",
	}))

	// Test namespace name too long
	assert.Error(t, ValidateListWaitGroupJobsRequest(&gracklepb.ListWaitGroupJobsRequest{
		NamespaceName: string(make([]byte, 129)),
		WaitGroupName: "validname",
	}))

	// Test wait group name too long
	assert.Error(t, ValidateListWaitGroupJobsRequest(&gracklepb.ListWaitGroupJobsRequest{
		NamespaceName: "validname",
		WaitGroupName: string(make([]byte, 129)),
	}))

	// Test invalid pagination token (not base64)
	assert.Error(t, ValidateListWaitGroupJobsRequest(&gracklepb.ListWaitGroupJobsRequest{
		NamespaceName:   "validname",
		WaitGroupName:   "validname",
		PaginationToken: "invalid-base64!@#",
	}))

	// Test pagination token too long
	longToken := string(make([]byte, 1025))
	assert.Error(t, ValidateListWaitGroupJobsRequest(&gracklepb.ListWaitGroupJobsRequest{
		NamespaceName:   "validname",
		WaitGroupName:   "validname",
		PaginationToken: longToken,
	}))

	// Test limit too high
	assert.Error(t, ValidateListWaitGroupJobsRequest(&gracklepb.ListWaitGroupJobsRequest{
		NamespaceName: "validname",
		WaitGroupName: "validname",
		Limit:         101,
	}))

	// Test negative limit
	assert.Error(t, ValidateListWaitGroupJobsRequest(&gracklepb.ListWaitGroupJobsRequest{
		NamespaceName: "validname",
		WaitGroupName: "validname",
		Limit:         -1,
	}))

	// Test valid request
	assert.NoError(t, ValidateListWaitGroupJobsRequest(&gracklepb.ListWaitGroupJobsRequest{
		NamespaceName: "validname",
		WaitGroupName: "validname",
	}))

	// Test valid request with pagination
	assert.NoError(t, ValidateListWaitGroupJobsRequest(&gracklepb.ListWaitGroupJobsRequest{
		NamespaceName:   "validname",
		WaitGroupName:   "validname",
		PaginationToken: "dGVzdA==",
		Limit:           50,
	}))
}

func TestValidateListSemaphoreHoldersRequest(t *testing.T) {
	// Test empty request - should fail due to missing namespace name
	assert.Error(t, ValidateListSemaphoreHoldersRequest(&gracklepb.ListSemaphoreHoldersRequest{}))

	// Test missing namespace name
	assert.Error(t, ValidateListSemaphoreHoldersRequest(&gracklepb.ListSemaphoreHoldersRequest{
		SemaphoreName: "validname",
	}))

	// Test missing semaphore name
	assert.Error(t, ValidateListSemaphoreHoldersRequest(&gracklepb.ListSemaphoreHoldersRequest{
		NamespaceName: "validname",
	}))

	// Test invalid namespace name characters
	assert.Error(t, ValidateListSemaphoreHoldersRequest(&gracklepb.ListSemaphoreHoldersRequest{
		NamespaceName: "invalid name",
		SemaphoreName: "validname",
	}))

	// Test invalid semaphore name characters
	assert.Error(t, ValidateListSemaphoreHoldersRequest(&gracklepb.ListSemaphoreHoldersRequest{
		NamespaceName: "validname",
		SemaphoreName: "invalid name",
	}))

	// Test namespace name too long
	assert.Error(t, ValidateListSemaphoreHoldersRequest(&gracklepb.ListSemaphoreHoldersRequest{
		NamespaceName: string(make([]byte, 129)),
		SemaphoreName: "validname",
	}))

	// Test semaphore name too long
	assert.Error(t, ValidateListSemaphoreHoldersRequest(&gracklepb.ListSemaphoreHoldersRequest{
		NamespaceName: "validname",
		SemaphoreName: string(make([]byte, 129)),
	}))

	// Test invalid pagination token (not base64)
	assert.Error(t, ValidateListSemaphoreHoldersRequest(&gracklepb.ListSemaphoreHoldersRequest{
		NamespaceName:   "validname",
		SemaphoreName:   "validname",
		PaginationToken: "invalid-base64!@#",
	}))

	// Test pagination token too long
	longToken := string(make([]byte, 1025))
	assert.Error(t, ValidateListSemaphoreHoldersRequest(&gracklepb.ListSemaphoreHoldersRequest{
		NamespaceName:   "validname",
		SemaphoreName:   "validname",
		PaginationToken: longToken,
	}))

	// Test limit too high
	assert.Error(t, ValidateListSemaphoreHoldersRequest(&gracklepb.ListSemaphoreHoldersRequest{
		NamespaceName: "validname",
		SemaphoreName: "validname",
		Limit:         101,
	}))

	// Test negative limit
	assert.Error(t, ValidateListSemaphoreHoldersRequest(&gracklepb.ListSemaphoreHoldersRequest{
		NamespaceName: "validname",
		SemaphoreName: "validname",
		Limit:         -1,
	}))

	// Test valid request
	assert.NoError(t, ValidateListSemaphoreHoldersRequest(&gracklepb.ListSemaphoreHoldersRequest{
		NamespaceName: "validname",
		SemaphoreName: "validname",
	}))

	// Test valid request with pagination
	assert.NoError(t, ValidateListSemaphoreHoldersRequest(&gracklepb.ListSemaphoreHoldersRequest{
		NamespaceName:   "validname",
		SemaphoreName:   "validname",
		PaginationToken: "dGVzdA==",
		Limit:           50,
	}))
}

func TestValidateCreateBarrierRequest(t *testing.T) {
	// Test empty request
	assert.Error(t, ValidateCreateBarrierRequest(&gracklepb.CreateBarrierRequest{}))

	// Test missing namespace name
	assert.Error(t, ValidateCreateBarrierRequest(&gracklepb.CreateBarrierRequest{
		BarrierName:       "validname",
		ExpectedProcesses: 3,
		ExpiresAt:         time.Now().UnixNano(),
	}))

	// Test missing barrier name
	assert.Error(t, ValidateCreateBarrierRequest(&gracklepb.CreateBarrierRequest{
		NamespaceName:     "validname",
		ExpectedProcesses: 3,
		ExpiresAt:         time.Now().UnixNano(),
	}))

	// Test invalid namespace name characters
	assert.Error(t, ValidateCreateBarrierRequest(&gracklepb.CreateBarrierRequest{
		NamespaceName:     "invalid name",
		BarrierName:       "validname",
		ExpectedProcesses: 3,
		ExpiresAt:         time.Now().UnixNano(),
	}))

	// Test invalid barrier name characters
	assert.Error(t, ValidateCreateBarrierRequest(&gracklepb.CreateBarrierRequest{
		NamespaceName:     "validname",
		BarrierName:       "invalid name",
		ExpectedProcesses: 3,
		ExpiresAt:         time.Now().UnixNano(),
	}))

	// Test namespace name too long
	assert.Error(t, ValidateCreateBarrierRequest(&gracklepb.CreateBarrierRequest{
		NamespaceName:     string(make([]byte, 129)),
		BarrierName:       "validname",
		ExpectedProcesses: 3,
		ExpiresAt:         time.Now().UnixNano(),
	}))

	// Test barrier name too long
	assert.Error(t, ValidateCreateBarrierRequest(&gracklepb.CreateBarrierRequest{
		NamespaceName:     "validname",
		BarrierName:       string(make([]byte, 129)),
		ExpectedProcesses: 3,
		ExpiresAt:         time.Now().UnixNano(),
	}))

	// Test description too long
	assert.Error(t, ValidateCreateBarrierRequest(&gracklepb.CreateBarrierRequest{
		NamespaceName:     "validname",
		BarrierName:       "validname",
		Description:       string(make([]byte, 1025)),
		ExpectedProcesses: 3,
		ExpiresAt:         time.Now().UnixNano(),
	}))

	// Test expected processes zero
	assert.Error(t, ValidateCreateBarrierRequest(&gracklepb.CreateBarrierRequest{
		NamespaceName:     "validname",
		BarrierName:       "validname",
		ExpectedProcesses: 0,
		ExpiresAt:         time.Now().UnixNano(),
	}))

	// Test expires at zero
	assert.Error(t, ValidateCreateBarrierRequest(&gracklepb.CreateBarrierRequest{
		NamespaceName:     "validname",
		BarrierName:       "validname",
		ExpectedProcesses: 3,
		ExpiresAt:         0,
	}))

	// Test expires at negative
	assert.Error(t, ValidateCreateBarrierRequest(&gracklepb.CreateBarrierRequest{
		NamespaceName:     "validname",
		BarrierName:       "validname",
		ExpectedProcesses: 3,
		ExpiresAt:         -1,
	}))

	// Test valid request
	assert.NoError(t, ValidateCreateBarrierRequest(&gracklepb.CreateBarrierRequest{
		NamespaceName:     "validname",
		BarrierName:       "validname",
		ExpectedProcesses: 3,
		ExpiresAt:         time.Now().UnixNano(),
	}))

	// Test valid request with description
	assert.NoError(t, ValidateCreateBarrierRequest(&gracklepb.CreateBarrierRequest{
		NamespaceName:     "validname",
		BarrierName:       "validname",
		Description:       "Valid description",
		ExpectedProcesses: 3,
		ExpiresAt:         time.Now().UnixNano(),
	}))
}

func TestValidateListBarriersRequest(t *testing.T) {
	// Test empty request - should fail due to missing namespace name
	assert.Error(t, ValidateListBarriersRequest(&gracklepb.ListBarriersRequest{}))

	// Test missing namespace name
	assert.Error(t, ValidateListBarriersRequest(&gracklepb.ListBarriersRequest{
		PaginationToken: "",
		Limit:           0,
	}))

	// Test invalid namespace name characters
	assert.Error(t, ValidateListBarriersRequest(&gracklepb.ListBarriersRequest{
		NamespaceName: "invalid name",
	}))

	// Test namespace name too long
	assert.Error(t, ValidateListBarriersRequest(&gracklepb.ListBarriersRequest{
		NamespaceName: string(make([]byte, 129)),
	}))

	// Test invalid pagination token (not base64)
	assert.Error(t, ValidateListBarriersRequest(&gracklepb.ListBarriersRequest{
		NamespaceName:   "validname",
		PaginationToken: "invalid-base64!@#",
	}))

	// Test pagination token too long
	longToken := string(make([]byte, 1025))
	assert.Error(t, ValidateListBarriersRequest(&gracklepb.ListBarriersRequest{
		NamespaceName:   "validname",
		PaginationToken: longToken,
	}))

	// Test limit too high
	assert.Error(t, ValidateListBarriersRequest(&gracklepb.ListBarriersRequest{
		NamespaceName: "validname",
		Limit:         101,
	}))

	// Test negative limit
	assert.Error(t, ValidateListBarriersRequest(&gracklepb.ListBarriersRequest{
		NamespaceName: "validname",
		Limit:         -1,
	}))

	// Test valid request
	assert.NoError(t, ValidateListBarriersRequest(&gracklepb.ListBarriersRequest{
		NamespaceName: "validname",
	}))

	// Test valid request with pagination
	assert.NoError(t, ValidateListBarriersRequest(&gracklepb.ListBarriersRequest{
		NamespaceName:   "validname",
		PaginationToken: "dGVzdA==",
		Limit:           50,
	}))
}

func TestValidateGetBarrierRequest(t *testing.T) {
	// Test empty request
	assert.Error(t, ValidateGetBarrierRequest(&gracklepb.GetBarrierRequest{}))

	// Test missing namespace name
	assert.Error(t, ValidateGetBarrierRequest(&gracklepb.GetBarrierRequest{
		BarrierName: "validname",
	}))

	// Test missing barrier name
	assert.Error(t, ValidateGetBarrierRequest(&gracklepb.GetBarrierRequest{
		NamespaceName: "validname",
	}))

	// Test invalid namespace name characters
	assert.Error(t, ValidateGetBarrierRequest(&gracklepb.GetBarrierRequest{
		NamespaceName: "invalid name",
		BarrierName:   "validname",
	}))

	// Test invalid barrier name characters
	assert.Error(t, ValidateGetBarrierRequest(&gracklepb.GetBarrierRequest{
		NamespaceName: "validname",
		BarrierName:   "invalid name",
	}))

	// Test namespace name too long
	assert.Error(t, ValidateGetBarrierRequest(&gracklepb.GetBarrierRequest{
		NamespaceName: string(make([]byte, 129)),
		BarrierName:   "validname",
	}))

	// Test barrier name too long
	assert.Error(t, ValidateGetBarrierRequest(&gracklepb.GetBarrierRequest{
		NamespaceName: "validname",
		BarrierName:   string(make([]byte, 129)),
	}))

	// Test valid request
	assert.NoError(t, ValidateGetBarrierRequest(&gracklepb.GetBarrierRequest{
		NamespaceName: "validname",
		BarrierName:   "validname",
	}))
}

func TestValidateDeleteBarrierRequest(t *testing.T) {
	// Test empty request
	assert.Error(t, ValidateDeleteBarrierRequest(&gracklepb.DeleteBarrierRequest{}))

	// Test missing namespace name
	assert.Error(t, ValidateDeleteBarrierRequest(&gracklepb.DeleteBarrierRequest{
		BarrierName: "validname",
	}))

	// Test missing barrier name
	assert.Error(t, ValidateDeleteBarrierRequest(&gracklepb.DeleteBarrierRequest{
		NamespaceName: "validname",
	}))

	// Test invalid namespace name characters
	assert.Error(t, ValidateDeleteBarrierRequest(&gracklepb.DeleteBarrierRequest{
		NamespaceName: "invalid name",
		BarrierName:   "validname",
	}))

	// Test invalid barrier name characters
	assert.Error(t, ValidateDeleteBarrierRequest(&gracklepb.DeleteBarrierRequest{
		NamespaceName: "validname",
		BarrierName:   "invalid name",
	}))

	// Test namespace name too long
	assert.Error(t, ValidateDeleteBarrierRequest(&gracklepb.DeleteBarrierRequest{
		NamespaceName: string(make([]byte, 129)),
		BarrierName:   "validname",
	}))

	// Test barrier name too long
	assert.Error(t, ValidateDeleteBarrierRequest(&gracklepb.DeleteBarrierRequest{
		NamespaceName: "validname",
		BarrierName:   string(make([]byte, 129)),
	}))

	// Test valid request
	assert.NoError(t, ValidateDeleteBarrierRequest(&gracklepb.DeleteBarrierRequest{
		NamespaceName: "validname",
		BarrierName:   "validname",
	}))
}

func TestValidateUpdateBarrierRequest(t *testing.T) {
	// Test empty request
	assert.Error(t, ValidateUpdateBarrierRequest(&gracklepb.UpdateBarrierRequest{}))

	// Test missing namespace name
	assert.Error(t, ValidateUpdateBarrierRequest(&gracklepb.UpdateBarrierRequest{
		BarrierName:       "validname",
		Description:       "desc",
		ExpectedProcesses: 3,
	}))

	// Test missing barrier name
	assert.Error(t, ValidateUpdateBarrierRequest(&gracklepb.UpdateBarrierRequest{
		NamespaceName:     "validname",
		Description:       "desc",
		ExpectedProcesses: 3,
	}))

	// Test invalid namespace name characters
	assert.Error(t, ValidateUpdateBarrierRequest(&gracklepb.UpdateBarrierRequest{
		NamespaceName:     "invalid name",
		BarrierName:       "validname",
		Description:       "desc",
		ExpectedProcesses: 3,
	}))

	// Test invalid barrier name characters
	assert.Error(t, ValidateUpdateBarrierRequest(&gracklepb.UpdateBarrierRequest{
		NamespaceName:     "validname",
		BarrierName:       "invalid name",
		Description:       "desc",
		ExpectedProcesses: 3,
	}))

	// Test namespace name too long
	assert.Error(t, ValidateUpdateBarrierRequest(&gracklepb.UpdateBarrierRequest{
		NamespaceName:     string(make([]byte, 129)),
		BarrierName:       "validname",
		Description:       "desc",
		ExpectedProcesses: 3,
	}))

	// Test barrier name too long
	assert.Error(t, ValidateUpdateBarrierRequest(&gracklepb.UpdateBarrierRequest{
		NamespaceName:     "validname",
		BarrierName:       string(make([]byte, 129)),
		Description:       "desc",
		ExpectedProcesses: 3,
	}))

	// Test description too long
	assert.Error(t, ValidateUpdateBarrierRequest(&gracklepb.UpdateBarrierRequest{
		NamespaceName:     "validname",
		BarrierName:       "validname",
		Description:       string(make([]byte, 1025)),
		ExpectedProcesses: 3,
	}))

	// Test expected_processes zero
	assert.Error(t, ValidateUpdateBarrierRequest(&gracklepb.UpdateBarrierRequest{
		NamespaceName:     "validname",
		BarrierName:       "validname",
		Description:       "desc",
		ExpectedProcesses: 0,
	}))

	// Test valid request
	assert.NoError(t, ValidateUpdateBarrierRequest(&gracklepb.UpdateBarrierRequest{
		NamespaceName:     "validname",
		BarrierName:       "validname",
		Description:       "Valid description",
		ExpectedProcesses: 5,
	}))
}

func TestValidateArriveAtBarrierRequest(t *testing.T) {
	// Test empty request
	assert.Error(t, ValidateArriveAtBarrierRequest(&gracklepb.ArriveAtBarrierRequest{}))

	// Test missing namespace name
	assert.Error(t, ValidateArriveAtBarrierRequest(&gracklepb.ArriveAtBarrierRequest{
		BarrierName:        "validname",
		ProcessId:          "proc1",
		ExpectedGeneration: 1,
	}))

	// Test missing barrier name
	assert.Error(t, ValidateArriveAtBarrierRequest(&gracklepb.ArriveAtBarrierRequest{
		NamespaceName:      "validname",
		ProcessId:          "proc1",
		ExpectedGeneration: 1,
	}))

	// Test missing process id
	assert.Error(t, ValidateArriveAtBarrierRequest(&gracklepb.ArriveAtBarrierRequest{
		NamespaceName:      "validname",
		BarrierName:        "validname",
		ExpectedGeneration: 1,
	}))

	// Test invalid namespace name characters
	assert.Error(t, ValidateArriveAtBarrierRequest(&gracklepb.ArriveAtBarrierRequest{
		NamespaceName:      "invalid name",
		BarrierName:        "validname",
		ProcessId:          "proc1",
		ExpectedGeneration: 1,
	}))

	// Test invalid barrier name characters
	assert.Error(t, ValidateArriveAtBarrierRequest(&gracklepb.ArriveAtBarrierRequest{
		NamespaceName:      "validname",
		BarrierName:        "invalid name",
		ProcessId:          "proc1",
		ExpectedGeneration: 1,
	}))

	// Test invalid process id characters
	assert.Error(t, ValidateArriveAtBarrierRequest(&gracklepb.ArriveAtBarrierRequest{
		NamespaceName:      "validname",
		BarrierName:        "validname",
		ProcessId:          "invalid process id",
		ExpectedGeneration: 1,
	}))

	// Test namespace name too long
	assert.Error(t, ValidateArriveAtBarrierRequest(&gracklepb.ArriveAtBarrierRequest{
		NamespaceName:      string(make([]byte, 129)),
		BarrierName:        "validname",
		ProcessId:          "proc1",
		ExpectedGeneration: 1,
	}))

	// Test barrier name too long
	assert.Error(t, ValidateArriveAtBarrierRequest(&gracklepb.ArriveAtBarrierRequest{
		NamespaceName:      "validname",
		BarrierName:        string(make([]byte, 129)),
		ProcessId:          "proc1",
		ExpectedGeneration: 1,
	}))

	// Test process id too long
	assert.Error(t, ValidateArriveAtBarrierRequest(&gracklepb.ArriveAtBarrierRequest{
		NamespaceName:      "validname",
		BarrierName:        "validname",
		ProcessId:          string(make([]byte, 129)),
		ExpectedGeneration: 1,
	}))

	// Test expected generation zero
	assert.Error(t, ValidateArriveAtBarrierRequest(&gracklepb.ArriveAtBarrierRequest{
		NamespaceName:      "validname",
		BarrierName:        "validname",
		ProcessId:          "proc1",
		ExpectedGeneration: 0,
	}))

	// Test valid request
	assert.NoError(t, ValidateArriveAtBarrierRequest(&gracklepb.ArriveAtBarrierRequest{
		NamespaceName:      "validname",
		BarrierName:        "validname",
		ProcessId:          "proc1",
		ExpectedGeneration: 1,
	}))
}

func TestValidateWaitAtBarrierRequest(t *testing.T) {
	// Test empty request
	assert.Error(t, ValidateWaitAtBarrierRequest(&gracklepb.WaitAtBarrierRequest{}))

	// Test missing namespace name
	assert.Error(t, ValidateWaitAtBarrierRequest(&gracklepb.WaitAtBarrierRequest{
		BarrierName:        "validname",
		ExpectedGeneration: 1,
		TimeoutSeconds:     10,
	}))

	// Test missing barrier name
	assert.Error(t, ValidateWaitAtBarrierRequest(&gracklepb.WaitAtBarrierRequest{
		NamespaceName:      "validname",
		ExpectedGeneration: 1,
		TimeoutSeconds:     10,
	}))

	// Test invalid namespace name characters
	assert.Error(t, ValidateWaitAtBarrierRequest(&gracklepb.WaitAtBarrierRequest{
		NamespaceName:      "invalid name",
		BarrierName:        "validname",
		ExpectedGeneration: 1,
		TimeoutSeconds:     10,
	}))

	// Test invalid barrier name characters
	assert.Error(t, ValidateWaitAtBarrierRequest(&gracklepb.WaitAtBarrierRequest{
		NamespaceName:      "validname",
		BarrierName:        "invalid name",
		ExpectedGeneration: 1,
		TimeoutSeconds:     10,
	}))

	// Test namespace name too long
	assert.Error(t, ValidateWaitAtBarrierRequest(&gracklepb.WaitAtBarrierRequest{
		NamespaceName:      string(make([]byte, 129)),
		BarrierName:        "validname",
		ExpectedGeneration: 1,
		TimeoutSeconds:     10,
	}))

	// Test barrier name too long
	assert.Error(t, ValidateWaitAtBarrierRequest(&gracklepb.WaitAtBarrierRequest{
		NamespaceName:      "validname",
		BarrierName:        string(make([]byte, 129)),
		ExpectedGeneration: 1,
		TimeoutSeconds:     10,
	}))

	// Test expected generation zero
	assert.Error(t, ValidateWaitAtBarrierRequest(&gracklepb.WaitAtBarrierRequest{
		NamespaceName:      "validname",
		BarrierName:        "validname",
		ExpectedGeneration: 0,
		TimeoutSeconds:     10,
	}))

	// Test timeout seconds zero
	assert.Error(t, ValidateWaitAtBarrierRequest(&gracklepb.WaitAtBarrierRequest{
		NamespaceName:      "validname",
		BarrierName:        "validname",
		ExpectedGeneration: 1,
		TimeoutSeconds:     0,
	}))

	// Test timeout seconds negative
	assert.Error(t, ValidateWaitAtBarrierRequest(&gracklepb.WaitAtBarrierRequest{
		NamespaceName:      "validname",
		BarrierName:        "validname",
		ExpectedGeneration: 1,
		TimeoutSeconds:     -1,
	}))

	// Test timeout seconds too high
	assert.Error(t, ValidateWaitAtBarrierRequest(&gracklepb.WaitAtBarrierRequest{
		NamespaceName:      "validname",
		BarrierName:        "validname",
		ExpectedGeneration: 1,
		TimeoutSeconds:     301,
	}))

	// Test valid request
	assert.NoError(t, ValidateWaitAtBarrierRequest(&gracklepb.WaitAtBarrierRequest{
		NamespaceName:      "validname",
		BarrierName:        "validname",
		ExpectedGeneration: 1,
		TimeoutSeconds:     10,
	}))

	// Test valid request with maximum timeout
	assert.NoError(t, ValidateWaitAtBarrierRequest(&gracklepb.WaitAtBarrierRequest{
		NamespaceName:      "validname",
		BarrierName:        "validname",
		ExpectedGeneration: 1,
		TimeoutSeconds:     300,
	}))
}

func TestValidateListBarrierParticipantsRequest(t *testing.T) {
	// Test empty request - should fail due to missing namespace name
	assert.Error(t, ValidateListBarrierParticipantsRequest(&gracklepb.ListBarrierParticipantsRequest{}))

	// Test missing namespace name
	assert.Error(t, ValidateListBarrierParticipantsRequest(&gracklepb.ListBarrierParticipantsRequest{
		BarrierName: "validname",
	}))

	// Test missing barrier name
	assert.Error(t, ValidateListBarrierParticipantsRequest(&gracklepb.ListBarrierParticipantsRequest{
		NamespaceName: "validname",
	}))

	// Test invalid namespace name characters
	assert.Error(t, ValidateListBarrierParticipantsRequest(&gracklepb.ListBarrierParticipantsRequest{
		NamespaceName: "invalid name",
		BarrierName:   "validname",
	}))

	// Test invalid barrier name characters
	assert.Error(t, ValidateListBarrierParticipantsRequest(&gracklepb.ListBarrierParticipantsRequest{
		NamespaceName: "validname",
		BarrierName:   "invalid name",
	}))

	// Test namespace name too long
	assert.Error(t, ValidateListBarrierParticipantsRequest(&gracklepb.ListBarrierParticipantsRequest{
		NamespaceName: string(make([]byte, 129)),
		BarrierName:   "validname",
	}))

	// Test barrier name too long
	assert.Error(t, ValidateListBarrierParticipantsRequest(&gracklepb.ListBarrierParticipantsRequest{
		NamespaceName: "validname",
		BarrierName:   string(make([]byte, 129)),
	}))

	// Test invalid pagination token (not base64)
	assert.Error(t, ValidateListBarrierParticipantsRequest(&gracklepb.ListBarrierParticipantsRequest{
		NamespaceName:   "validname",
		BarrierName:     "validname",
		PaginationToken: "invalid-base64!@#",
	}))

	// Test pagination token too long
	longToken := string(make([]byte, 1025))
	assert.Error(t, ValidateListBarrierParticipantsRequest(&gracklepb.ListBarrierParticipantsRequest{
		NamespaceName:   "validname",
		BarrierName:     "validname",
		PaginationToken: longToken,
	}))

	// Test limit too high
	assert.Error(t, ValidateListBarrierParticipantsRequest(&gracklepb.ListBarrierParticipantsRequest{
		NamespaceName: "validname",
		BarrierName:   "validname",
		Limit:         101,
	}))

	// Test negative limit
	assert.Error(t, ValidateListBarrierParticipantsRequest(&gracklepb.ListBarrierParticipantsRequest{
		NamespaceName: "validname",
		BarrierName:   "validname",
		Limit:         -1,
	}))

	// Test valid request
	assert.NoError(t, ValidateListBarrierParticipantsRequest(&gracklepb.ListBarrierParticipantsRequest{
		NamespaceName: "validname",
		BarrierName:   "validname",
	}))

	// Test valid request with pagination
	assert.NoError(t, ValidateListBarrierParticipantsRequest(&gracklepb.ListBarrierParticipantsRequest{
		NamespaceName:   "validname",
		BarrierName:     "validname",
		PaginationToken: "dGVzdA==",
		Limit:           50,
	}))

	// Test valid request with generation
	assert.NoError(t, ValidateListBarrierParticipantsRequest(&gracklepb.ListBarrierParticipantsRequest{
		NamespaceName: "validname",
		BarrierName:   "validname",
		Generation:    1,
	}))
}
