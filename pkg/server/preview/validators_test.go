package preview

import (
	"encoding/base64"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	gracklepb "github.com/evrblk/evrblk-go/grackle/preview"
)

func TestValidateCreateNamespaceRequest(t *testing.T) {
	t.Run("empty request", func(t *testing.T) {
		require.Error(t, ValidateCreateNamespaceRequest(&gracklepb.CreateNamespaceRequest{}))
	})

	t.Run("empty name", func(t *testing.T) {
		require.Error(t, ValidateCreateNamespaceRequest(&gracklepb.CreateNamespaceRequest{
			Name: "",
		}))
	})

	t.Run("name too long", func(t *testing.T) {
		require.Error(t, ValidateCreateNamespaceRequest(&gracklepb.CreateNamespaceRequest{
			Name: string(make([]byte, 129)),
		}))
	})

	t.Run("invalid name characters", func(t *testing.T) {
		require.Error(t, ValidateCreateNamespaceRequest(&gracklepb.CreateNamespaceRequest{
			Name: "invalid name",
		}))
	})

	t.Run("description too long", func(t *testing.T) {
		require.Error(t, ValidateCreateNamespaceRequest(&gracklepb.CreateNamespaceRequest{
			Name:        "validname",
			Description: string(make([]byte, 1025)),
		}))
	})

	t.Run("valid request", func(t *testing.T) {
		require.NoError(t, ValidateCreateNamespaceRequest(&gracklepb.CreateNamespaceRequest{
			Name:        "validname",
			Description: "Valid description",
		}))
	})
}

func TestValidateGetNamespaceRequest(t *testing.T) {
	t.Run("empty request", func(t *testing.T) {
		require.Error(t, ValidateGetNamespaceRequest(&gracklepb.GetNamespaceRequest{}))
	})

	t.Run("empty namespace name", func(t *testing.T) {
		require.Error(t, ValidateGetNamespaceRequest(&gracklepb.GetNamespaceRequest{
			NamespaceName: "",
		}))
	})

	t.Run("namespace name too long", func(t *testing.T) {
		require.Error(t, ValidateGetNamespaceRequest(&gracklepb.GetNamespaceRequest{
			NamespaceName: string(make([]byte, 129)),
		}))
	})

	t.Run("invalid namespace name characters", func(t *testing.T) {
		require.Error(t, ValidateGetNamespaceRequest(&gracklepb.GetNamespaceRequest{
			NamespaceName: "invalid name",
		}))
	})

	t.Run("valid request", func(t *testing.T) {
		require.NoError(t, ValidateGetNamespaceRequest(&gracklepb.GetNamespaceRequest{
			NamespaceName: "validname",
		}))
	})
}

func TestValidateUpdateNamespaceRequest(t *testing.T) {
	t.Run("empty request", func(t *testing.T) {
		require.Error(t, ValidateUpdateNamespaceRequest(&gracklepb.UpdateNamespaceRequest{}))
	})

	t.Run("empty namespace name", func(t *testing.T) {
		require.Error(t, ValidateUpdateNamespaceRequest(&gracklepb.UpdateNamespaceRequest{
			NamespaceName: "",
		}))
	})

	t.Run("namespace name too long", func(t *testing.T) {
		require.Error(t, ValidateUpdateNamespaceRequest(&gracklepb.UpdateNamespaceRequest{
			NamespaceName: string(make([]byte, 129)),
		}))
	})

	t.Run("invalid namespace name characters", func(t *testing.T) {
		require.Error(t, ValidateUpdateNamespaceRequest(&gracklepb.UpdateNamespaceRequest{
			NamespaceName: "invalid name",
		}))
	})

	t.Run("description too long", func(t *testing.T) {
		require.Error(t, ValidateUpdateNamespaceRequest(&gracklepb.UpdateNamespaceRequest{
			NamespaceName: "validname",
			Description:   string(make([]byte, 1025)),
		}))
	})

	t.Run("valid request", func(t *testing.T) {
		require.NoError(t, ValidateUpdateNamespaceRequest(&gracklepb.UpdateNamespaceRequest{
			NamespaceName: "validname",
			Description:   "Valid description",
		}))
	})
}

func TestValidateDeleteNamespaceRequest(t *testing.T) {
	t.Run("empty request", func(t *testing.T) {
		require.Error(t, ValidateDeleteNamespaceRequest(&gracklepb.DeleteNamespaceRequest{}))
	})

	t.Run("empty namespace name", func(t *testing.T) {
		require.Error(t, ValidateDeleteNamespaceRequest(&gracklepb.DeleteNamespaceRequest{
			NamespaceName: "",
		}))
	})

	t.Run("namespace name too long", func(t *testing.T) {
		require.Error(t, ValidateDeleteNamespaceRequest(&gracklepb.DeleteNamespaceRequest{
			NamespaceName: string(make([]byte, 129)),
		}))
	})

	t.Run("invalid namespace name characters", func(t *testing.T) {
		require.Error(t, ValidateDeleteNamespaceRequest(&gracklepb.DeleteNamespaceRequest{
			NamespaceName: "invalid name",
		}))
	})

	t.Run("valid request", func(t *testing.T) {
		require.NoError(t, ValidateDeleteNamespaceRequest(&gracklepb.DeleteNamespaceRequest{
			NamespaceName: "validname",
		}))
	})
}

func TestValidateListNamespacesRequest(t *testing.T) {
	t.Run("empty request - should pass as pagination fields are optional", func(t *testing.T) {
		require.NoError(t, ValidateListNamespacesRequest(&gracklepb.ListNamespacesRequest{}))
	})

	t.Run("valid request with no pagination", func(t *testing.T) {
		require.NoError(t, ValidateListNamespacesRequest(&gracklepb.ListNamespacesRequest{
			PaginationToken: "",
			Limit:           0,
		}))
	})

	t.Run("valid request with pagination token", func(t *testing.T) {
		require.NoError(t, ValidateListNamespacesRequest(&gracklepb.ListNamespacesRequest{
			PaginationToken: "dGVzdA==",
			Limit:           0,
		}))
	})

	t.Run("valid request with limit", func(t *testing.T) {
		require.NoError(t, ValidateListNamespacesRequest(&gracklepb.ListNamespacesRequest{
			PaginationToken: "",
			Limit:           50,
		}))
	})

	t.Run("valid request with both pagination token and limit", func(t *testing.T) {
		require.NoError(t, ValidateListNamespacesRequest(&gracklepb.ListNamespacesRequest{
			PaginationToken: "dGVzdA==",
			Limit:           100,
		}))
	})

	t.Run("invalid pagination token (not base64)", func(t *testing.T) {
		require.Error(t, ValidateListNamespacesRequest(&gracklepb.ListNamespacesRequest{
			PaginationToken: "invalid-base64!@#",
			Limit:           50,
		}))
	})

	t.Run("pagination token too long", func(t *testing.T) {
		longToken := string(make([]byte, 1025))
		require.Error(t, ValidateListNamespacesRequest(&gracklepb.ListNamespacesRequest{
			PaginationToken: longToken,
			Limit:           50,
		}))
	})

	t.Run("limit too high", func(t *testing.T) {
		require.Error(t, ValidateListNamespacesRequest(&gracklepb.ListNamespacesRequest{
			PaginationToken: "",
			Limit:           101,
		}))
	})

	t.Run("negative limit", func(t *testing.T) {
		require.Error(t, ValidateListNamespacesRequest(&gracklepb.ListNamespacesRequest{
			PaginationToken: "",
			Limit:           -1,
		}))
	})

	t.Run("edge case: limit at maximum", func(t *testing.T) {
		require.NoError(t, ValidateListNamespacesRequest(&gracklepb.ListNamespacesRequest{
			PaginationToken: "",
			Limit:           100,
		}))
	})

	t.Run("edge case: pagination token at maximum length", func(t *testing.T) {
		maxToken := string(make([]byte, 1024)) // maxPaginationTokenLength
		require.Error(t, ValidateListNamespacesRequest(&gracklepb.ListNamespacesRequest{
			PaginationToken: maxToken, // This will fail base64 validation
			Limit:           50,
		}))
	})

	t.Run("valid base64 token at maximum length", func(t *testing.T) {
		validMaxToken := base64.StdEncoding.EncodeToString(make([]byte, 768)) // 768 bytes = 1024 base64 chars
		require.NoError(t, ValidateListNamespacesRequest(&gracklepb.ListNamespacesRequest{
			PaginationToken: validMaxToken,
			Limit:           50,
		}))
	})
}

func TestValidateCreateWaitGroupRequest(t *testing.T) {
	t.Run("empty request", func(t *testing.T) {
		require.Error(t, ValidateCreateWaitGroupRequest(&gracklepb.CreateWaitGroupRequest{}))
	})

	t.Run("empty namespace name", func(t *testing.T) {
		require.Error(t, ValidateCreateWaitGroupRequest(&gracklepb.CreateWaitGroupRequest{
			WaitGroupName: "validname",
			Counter:       1,
		}))
	})

	t.Run("empty wait group name", func(t *testing.T) {
		require.Error(t, ValidateCreateWaitGroupRequest(&gracklepb.CreateWaitGroupRequest{
			NamespaceName: "validname",
			Counter:       1,
		}))
	})

	t.Run("namespace name too long", func(t *testing.T) {
		require.Error(t, ValidateCreateWaitGroupRequest(&gracklepb.CreateWaitGroupRequest{
			NamespaceName: string(make([]byte, 129)),
			WaitGroupName: "validname",
			Counter:       1,
		}))
	})

	t.Run("wait group name too long", func(t *testing.T) {
		require.Error(t, ValidateCreateWaitGroupRequest(&gracklepb.CreateWaitGroupRequest{
			NamespaceName: "validname",
			WaitGroupName: string(make([]byte, 129)),
			Counter:       1,
		}))
	})

	t.Run("invalid namespace name characters", func(t *testing.T) {
		require.Error(t, ValidateCreateWaitGroupRequest(&gracklepb.CreateWaitGroupRequest{
			NamespaceName: "invalid name",
			WaitGroupName: "validname",
			Counter:       1,
		}))
	})

	t.Run("invalid wait group name characters", func(t *testing.T) {
		require.Error(t, ValidateCreateWaitGroupRequest(&gracklepb.CreateWaitGroupRequest{
			NamespaceName: "validname",
			WaitGroupName: "invalid name",
			Counter:       1,
		}))
	})

	t.Run("counter must be greater than 0", func(t *testing.T) {
		require.Error(t, ValidateCreateWaitGroupRequest(&gracklepb.CreateWaitGroupRequest{
			NamespaceName: "validname",
			WaitGroupName: "validwaitgroup",
			Counter:       0,
		}))
	})

	t.Run("valid request", func(t *testing.T) {
		require.NoError(t, ValidateCreateWaitGroupRequest(&gracklepb.CreateWaitGroupRequest{
			NamespaceName: "validname",
			WaitGroupName: "validwaitgroup",
			Counter:       1,
		}))
	})
}

func TestValidateGetWaitGroupRequest(t *testing.T) {
	t.Run("empty request", func(t *testing.T) {
		require.Error(t, ValidateGetWaitGroupRequest(&gracklepb.GetWaitGroupRequest{}))
	})

	t.Run("empty namespace name", func(t *testing.T) {
		require.Error(t, ValidateGetWaitGroupRequest(&gracklepb.GetWaitGroupRequest{
			WaitGroupName: "validname",
		}))
	})

	t.Run("empty wait group name", func(t *testing.T) {
		require.Error(t, ValidateGetWaitGroupRequest(&gracklepb.GetWaitGroupRequest{
			NamespaceName: "validname",
		}))
	})

	t.Run("namespace name too long", func(t *testing.T) {
		require.Error(t, ValidateGetWaitGroupRequest(&gracklepb.GetWaitGroupRequest{
			NamespaceName: string(make([]byte, 129)),
			WaitGroupName: "validname",
		}))
	})

	t.Run("wait group name too long", func(t *testing.T) {
		require.Error(t, ValidateGetWaitGroupRequest(&gracklepb.GetWaitGroupRequest{
			NamespaceName: "validname",
			WaitGroupName: string(make([]byte, 129)),
		}))
	})

	t.Run("invalid namespace name characters", func(t *testing.T) {
		require.Error(t, ValidateGetWaitGroupRequest(&gracklepb.GetWaitGroupRequest{
			NamespaceName: "invalid name",
			WaitGroupName: "validname",
		}))
	})

	t.Run("invalid wait group name characters", func(t *testing.T) {
		require.Error(t, ValidateGetWaitGroupRequest(&gracklepb.GetWaitGroupRequest{
			NamespaceName: "validname",
			WaitGroupName: "invalid name",
		}))
	})

	t.Run("valid request", func(t *testing.T) {
		require.NoError(t, ValidateGetWaitGroupRequest(&gracklepb.GetWaitGroupRequest{
			NamespaceName: "validname",
			WaitGroupName: "validwaitgroup",
		}))
	})
}

func TestValidateAddJobsToWaitGroupRequest(t *testing.T) {
	t.Run("empty request", func(t *testing.T) {
		require.Error(t, ValidateAddJobsToWaitGroupRequest(&gracklepb.AddJobsToWaitGroupRequest{}))
	})

	t.Run("empty namespace name", func(t *testing.T) {
		require.Error(t, ValidateAddJobsToWaitGroupRequest(&gracklepb.AddJobsToWaitGroupRequest{
			WaitGroupName: "validname",
			Counter:       1,
		}))
	})

	t.Run("empty wait group name", func(t *testing.T) {
		require.Error(t, ValidateAddJobsToWaitGroupRequest(&gracklepb.AddJobsToWaitGroupRequest{
			NamespaceName: "validname",
			Counter:       1,
		}))
	})

	t.Run("namespace name too long", func(t *testing.T) {
		require.Error(t, ValidateAddJobsToWaitGroupRequest(&gracklepb.AddJobsToWaitGroupRequest{
			NamespaceName: string(make([]byte, 129)),
			WaitGroupName: "validname",
			Counter:       1,
		}))
	})

	t.Run("wait group name too long", func(t *testing.T) {
		require.Error(t, ValidateAddJobsToWaitGroupRequest(&gracklepb.AddJobsToWaitGroupRequest{
			NamespaceName: "validname",
			WaitGroupName: string(make([]byte, 129)),
			Counter:       1,
		}))
	})

	t.Run("invalid namespace name characters", func(t *testing.T) {
		require.Error(t, ValidateAddJobsToWaitGroupRequest(&gracklepb.AddJobsToWaitGroupRequest{
			NamespaceName: "invalid name",
			WaitGroupName: "validname",
			Counter:       1,
		}))
	})

	t.Run("invalid wait group name characters", func(t *testing.T) {
		require.Error(t, ValidateAddJobsToWaitGroupRequest(&gracklepb.AddJobsToWaitGroupRequest{
			NamespaceName: "validname",
			WaitGroupName: "invalid name",
			Counter:       1,
		}))
	})

	t.Run("invalid counter", func(t *testing.T) {
		require.Error(t, ValidateAddJobsToWaitGroupRequest(&gracklepb.AddJobsToWaitGroupRequest{
			NamespaceName: "validname",
			WaitGroupName: "invalid name",
			Counter:       0,
		}))
	})

	t.Run("valid request", func(t *testing.T) {
		require.NoError(t, ValidateAddJobsToWaitGroupRequest(&gracklepb.AddJobsToWaitGroupRequest{
			NamespaceName: "validname",
			WaitGroupName: "validwaitgroup",
			Counter:       1,
		}))
	})
}

func TestValidateDeleteWaitGroupRequest(t *testing.T) {
	t.Run("empty request", func(t *testing.T) {
		require.Error(t, ValidateDeleteWaitGroupRequest(&gracklepb.DeleteWaitGroupRequest{}))
	})

	t.Run("empty namespace name", func(t *testing.T) {
		require.Error(t, ValidateDeleteWaitGroupRequest(&gracklepb.DeleteWaitGroupRequest{
			WaitGroupName: "validname",
		}))
	})

	t.Run("empty wait group name", func(t *testing.T) {
		require.Error(t, ValidateDeleteWaitGroupRequest(&gracklepb.DeleteWaitGroupRequest{
			NamespaceName: "validname",
		}))
	})

	t.Run("namespace name too long", func(t *testing.T) {
		require.Error(t, ValidateDeleteWaitGroupRequest(&gracklepb.DeleteWaitGroupRequest{
			NamespaceName: string(make([]byte, 129)),
			WaitGroupName: "validname",
		}))
	})

	t.Run("wait group name too long", func(t *testing.T) {
		require.Error(t, ValidateDeleteWaitGroupRequest(&gracklepb.DeleteWaitGroupRequest{
			NamespaceName: "validname",
			WaitGroupName: string(make([]byte, 129)),
		}))
	})

	t.Run("invalid namespace name characters", func(t *testing.T) {
		require.Error(t, ValidateDeleteWaitGroupRequest(&gracklepb.DeleteWaitGroupRequest{
			NamespaceName: "invalid name",
			WaitGroupName: "validname",
		}))
	})

	t.Run("invalid wait group name characters", func(t *testing.T) {
		require.Error(t, ValidateDeleteWaitGroupRequest(&gracklepb.DeleteWaitGroupRequest{
			NamespaceName: "validname",
			WaitGroupName: "invalid name",
		}))
	})

	t.Run("valid request", func(t *testing.T) {
		require.NoError(t, ValidateDeleteWaitGroupRequest(&gracklepb.DeleteWaitGroupRequest{
			NamespaceName: "validname",
			WaitGroupName: "validwaitgroup",
		}))
	})
}

func TestValidateWaitForWaitGroupRequest(t *testing.T) {
	t.Run("empty request", func(t *testing.T) {
		require.Error(t, ValidateWaitForWaitGroupRequest(&gracklepb.WaitForWaitGroupRequest{}))
	})

	t.Run("missing namespace name", func(t *testing.T) {
		require.Error(t, ValidateWaitForWaitGroupRequest(&gracklepb.WaitForWaitGroupRequest{
			WaitGroupName:  "validname",
			TimeoutSeconds: 10,
		}))
	})

	t.Run("missing wait group name", func(t *testing.T) {
		require.Error(t, ValidateWaitForWaitGroupRequest(&gracklepb.WaitForWaitGroupRequest{
			NamespaceName:  "validname",
			TimeoutSeconds: 10,
		}))
	})

	t.Run("namespace name too long", func(t *testing.T) {
		require.Error(t, ValidateWaitForWaitGroupRequest(&gracklepb.WaitForWaitGroupRequest{
			NamespaceName:  string(make([]byte, 129)),
			WaitGroupName:  "validname",
			TimeoutSeconds: 10,
		}))
	})

	t.Run("wait group name too long", func(t *testing.T) {
		require.Error(t, ValidateWaitForWaitGroupRequest(&gracklepb.WaitForWaitGroupRequest{
			NamespaceName:  "validname",
			WaitGroupName:  string(make([]byte, 129)),
			TimeoutSeconds: 10,
		}))
	})

	t.Run("invalid namespace name characters", func(t *testing.T) {
		require.Error(t, ValidateWaitForWaitGroupRequest(&gracklepb.WaitForWaitGroupRequest{
			NamespaceName:  "invalid name",
			WaitGroupName:  "validname",
			TimeoutSeconds: 10,
		}))
	})

	t.Run("invalid wait group name characters", func(t *testing.T) {
		require.Error(t, ValidateWaitForWaitGroupRequest(&gracklepb.WaitForWaitGroupRequest{
			NamespaceName:  "validname",
			WaitGroupName:  "invalid name",
			TimeoutSeconds: 10,
		}))
	})

	t.Run("timeout seconds zero", func(t *testing.T) {
		require.Error(t, ValidateWaitForWaitGroupRequest(&gracklepb.WaitForWaitGroupRequest{
			NamespaceName:  "validname",
			WaitGroupName:  "validname",
			TimeoutSeconds: 0,
		}))
	})

	t.Run("timeout seconds negative", func(t *testing.T) {
		require.Error(t, ValidateWaitForWaitGroupRequest(&gracklepb.WaitForWaitGroupRequest{
			NamespaceName:  "validname",
			WaitGroupName:  "validname",
			TimeoutSeconds: -1,
		}))
	})

	t.Run("timeout seconds too high", func(t *testing.T) {
		require.Error(t, ValidateWaitForWaitGroupRequest(&gracklepb.WaitForWaitGroupRequest{
			NamespaceName:  "validname",
			WaitGroupName:  "validname",
			TimeoutSeconds: 301,
		}))
	})

	t.Run("valid request", func(t *testing.T) {
		require.NoError(t, ValidateWaitForWaitGroupRequest(&gracklepb.WaitForWaitGroupRequest{
			NamespaceName:  "validname",
			WaitGroupName:  "validname",
			TimeoutSeconds: 10,
		}))
	})

	t.Run("valid request with maximum timeout", func(t *testing.T) {
		require.NoError(t, ValidateWaitForWaitGroupRequest(&gracklepb.WaitForWaitGroupRequest{
			NamespaceName:  "validname",
			WaitGroupName:  "validname",
			TimeoutSeconds: 300,
		}))
	})
}

func TestValidateListWaitGroupsRequest(t *testing.T) {
	t.Run("empty request - should fail due to missing namespace name", func(t *testing.T) {
		require.Error(t, ValidateListWaitGroupsRequest(&gracklepb.ListWaitGroupsRequest{}))
	})

	t.Run("empty namespace name", func(t *testing.T) {
		require.Error(t, ValidateListWaitGroupsRequest(&gracklepb.ListWaitGroupsRequest{
			NamespaceName: "",
		}))
	})

	t.Run("namespace name too long", func(t *testing.T) {
		require.Error(t, ValidateListWaitGroupsRequest(&gracklepb.ListWaitGroupsRequest{
			NamespaceName: string(make([]byte, 129)),
		}))
	})

	t.Run("invalid namespace name characters", func(t *testing.T) {
		require.Error(t, ValidateListWaitGroupsRequest(&gracklepb.ListWaitGroupsRequest{
			NamespaceName: "invalid name",
		}))
	})

	t.Run("valid request with no pagination", func(t *testing.T) {
		require.NoError(t, ValidateListWaitGroupsRequest(&gracklepb.ListWaitGroupsRequest{
			NamespaceName:   "validname",
			PaginationToken: "",
			Limit:           0,
		}))
	})

	t.Run("valid request with pagination token", func(t *testing.T) {
		require.NoError(t, ValidateListWaitGroupsRequest(&gracklepb.ListWaitGroupsRequest{
			NamespaceName:   "validname",
			PaginationToken: "dGVzdA==",
			Limit:           0,
		}))
	})

	t.Run("valid request with limit", func(t *testing.T) {
		require.NoError(t, ValidateListWaitGroupsRequest(&gracklepb.ListWaitGroupsRequest{
			NamespaceName:   "validname",
			PaginationToken: "",
			Limit:           50,
		}))
	})

	t.Run("valid request with both pagination token and limit", func(t *testing.T) {
		require.NoError(t, ValidateListWaitGroupsRequest(&gracklepb.ListWaitGroupsRequest{
			NamespaceName:   "validname",
			PaginationToken: "dGVzdA==",
			Limit:           100,
		}))
	})

	t.Run("invalid pagination token (not base64)", func(t *testing.T) {
		require.Error(t, ValidateListWaitGroupsRequest(&gracklepb.ListWaitGroupsRequest{
			NamespaceName:   "validname",
			PaginationToken: "invalid-base64!@#",
			Limit:           50,
		}))
	})

	t.Run("pagination token too long", func(t *testing.T) {
		longToken := string(make([]byte, 1025))
		require.Error(t, ValidateListWaitGroupsRequest(&gracklepb.ListWaitGroupsRequest{
			NamespaceName:   "validname",
			PaginationToken: longToken,
			Limit:           50,
		}))
	})

	t.Run("limit too high", func(t *testing.T) {
		require.Error(t, ValidateListWaitGroupsRequest(&gracklepb.ListWaitGroupsRequest{
			NamespaceName:   "validname",
			PaginationToken: "",
			Limit:           101,
		}))
	})

	t.Run("negative limit", func(t *testing.T) {
		require.Error(t, ValidateListWaitGroupsRequest(&gracklepb.ListWaitGroupsRequest{
			NamespaceName:   "validname",
			PaginationToken: "",
			Limit:           -1,
		}))
	})

	t.Run("edge case: limit at maximum", func(t *testing.T) {
		require.NoError(t, ValidateListWaitGroupsRequest(&gracklepb.ListWaitGroupsRequest{
			NamespaceName:   "validname",
			PaginationToken: "",
			Limit:           100,
		}))
	})

	t.Run("valid base64 token at maximum length", func(t *testing.T) {
		validMaxToken := base64.StdEncoding.EncodeToString(make([]byte, 768)) // 768 bytes = 1024 base64 chars
		require.NoError(t, ValidateListWaitGroupsRequest(&gracklepb.ListWaitGroupsRequest{
			NamespaceName:   "validname",
			PaginationToken: validMaxToken,
			Limit:           50,
		}))
	})
}

func TestValidateCompleteJobsFromWaitGroupRequest(t *testing.T) {
	t.Run("empty request", func(t *testing.T) {
		require.Error(t, ValidateCompleteJobsFromWaitGroupRequest(&gracklepb.CompleteJobsFromWaitGroupRequest{}))
	})

	t.Run("empty namespace name", func(t *testing.T) {
		require.Error(t, ValidateCompleteJobsFromWaitGroupRequest(&gracklepb.CompleteJobsFromWaitGroupRequest{
			WaitGroupName: "validname",
			ProcessIds:    []string{"proc1"},
		}))
	})

	t.Run("empty wait group name", func(t *testing.T) {
		require.Error(t, ValidateCompleteJobsFromWaitGroupRequest(&gracklepb.CompleteJobsFromWaitGroupRequest{
			NamespaceName: "validname",
			ProcessIds:    []string{"proc1"},
		}))
	})

	t.Run("namespace name too long", func(t *testing.T) {
		require.Error(t, ValidateCompleteJobsFromWaitGroupRequest(&gracklepb.CompleteJobsFromWaitGroupRequest{
			NamespaceName: string(make([]byte, 129)),
			WaitGroupName: "validname",
			ProcessIds:    []string{"proc1"},
		}))
	})

	t.Run("wait group name too long", func(t *testing.T) {
		require.Error(t, ValidateCompleteJobsFromWaitGroupRequest(&gracklepb.CompleteJobsFromWaitGroupRequest{
			NamespaceName: "validname",
			WaitGroupName: string(make([]byte, 129)),
			ProcessIds:    []string{"proc1"},
		}))
	})

	t.Run("invalid namespace name characters", func(t *testing.T) {
		require.Error(t, ValidateCompleteJobsFromWaitGroupRequest(&gracklepb.CompleteJobsFromWaitGroupRequest{
			NamespaceName: "invalid name",
			WaitGroupName: "validname",
			ProcessIds:    []string{"proc1"},
		}))
	})

	t.Run("invalid wait group name characters", func(t *testing.T) {
		require.Error(t, ValidateCompleteJobsFromWaitGroupRequest(&gracklepb.CompleteJobsFromWaitGroupRequest{
			NamespaceName: "validname",
			WaitGroupName: "invalid name",
			ProcessIds:    []string{"proc1"},
		}))
	})

	t.Run("too many process ids", func(t *testing.T) {
		processIds := make([]string, 51)
		for i := 0; i < 51; i++ {
			processIds[i] = "proc1"
		}
		require.Error(t, ValidateCompleteJobsFromWaitGroupRequest(&gracklepb.CompleteJobsFromWaitGroupRequest{
			NamespaceName: "validname",
			WaitGroupName: "validwaitgroup",
			ProcessIds:    processIds,
		}))
	})

	t.Run("invalid process id", func(t *testing.T) {
		require.Error(t, ValidateCompleteJobsFromWaitGroupRequest(&gracklepb.CompleteJobsFromWaitGroupRequest{
			NamespaceName: "validname",
			WaitGroupName: "validwaitgroup",
			ProcessIds:    []string{"invalid process id"},
		}))
	})

	t.Run("valid request", func(t *testing.T) {
		require.NoError(t, ValidateCompleteJobsFromWaitGroupRequest(&gracklepb.CompleteJobsFromWaitGroupRequest{
			NamespaceName: "validname",
			WaitGroupName: "validwaitgroup",
			ProcessIds:    []string{"proc1", "proc2"},
		}))
	})
}

func TestValidateDeleteLockRequest(t *testing.T) {
	t.Run("empty request", func(t *testing.T) {
		require.Error(t, ValidateDeleteLockRequest(&gracklepb.DeleteLockRequest{}))
	})

	t.Run("empty namespace name", func(t *testing.T) {
		require.Error(t, ValidateDeleteLockRequest(&gracklepb.DeleteLockRequest{
			LockName: "validname",
		}))
	})

	t.Run("empty lock name", func(t *testing.T) {
		require.Error(t, ValidateDeleteLockRequest(&gracklepb.DeleteLockRequest{
			NamespaceName: "validname",
		}))
	})

	t.Run("namespace name too long", func(t *testing.T) {
		require.Error(t, ValidateDeleteLockRequest(&gracklepb.DeleteLockRequest{
			NamespaceName: string(make([]byte, 129)),
			LockName:      "validname",
		}))
	})

	t.Run("lock name too long", func(t *testing.T) {
		require.Error(t, ValidateDeleteLockRequest(&gracklepb.DeleteLockRequest{
			NamespaceName: "validname",
			LockName:      string(make([]byte, 129)),
		}))
	})

	t.Run("invalid namespace name characters", func(t *testing.T) {
		require.Error(t, ValidateDeleteLockRequest(&gracklepb.DeleteLockRequest{
			NamespaceName: "invalid name",
			LockName:      "validname",
		}))
	})

	t.Run("invalid lock name characters", func(t *testing.T) {
		require.Error(t, ValidateDeleteLockRequest(&gracklepb.DeleteLockRequest{
			NamespaceName: "validname",
			LockName:      "invalid name",
		}))
	})

	t.Run("valid request", func(t *testing.T) {
		require.NoError(t, ValidateDeleteLockRequest(&gracklepb.DeleteLockRequest{
			NamespaceName: "validname",
			LockName:      "validlock",
		}))
	})
}

func TestValidateGetLockRequest(t *testing.T) {
	t.Run("empty request", func(t *testing.T) {
		require.Error(t, ValidateGetLockRequest(&gracklepb.GetLockRequest{}))
	})

	t.Run("empty namespace name", func(t *testing.T) {
		require.Error(t, ValidateGetLockRequest(&gracklepb.GetLockRequest{
			LockName: "validname",
		}))
	})

	t.Run("empty lock name", func(t *testing.T) {
		require.Error(t, ValidateGetLockRequest(&gracklepb.GetLockRequest{
			NamespaceName: "validname",
		}))
	})

	t.Run("namespace name too long", func(t *testing.T) {
		require.Error(t, ValidateGetLockRequest(&gracklepb.GetLockRequest{
			NamespaceName: string(make([]byte, 129)),
			LockName:      "validname",
		}))
	})

	t.Run("lock name too long", func(t *testing.T) {
		require.Error(t, ValidateGetLockRequest(&gracklepb.GetLockRequest{
			NamespaceName: "validname",
			LockName:      string(make([]byte, 129)),
		}))
	})

	t.Run("invalid namespace name characters", func(t *testing.T) {
		require.Error(t, ValidateGetLockRequest(&gracklepb.GetLockRequest{
			NamespaceName: "invalid name",
			LockName:      "validname",
		}))
	})

	t.Run("invalid lock name characters", func(t *testing.T) {
		require.Error(t, ValidateGetLockRequest(&gracklepb.GetLockRequest{
			NamespaceName: "validname",
			LockName:      "invalid name",
		}))
	})

	t.Run("valid request", func(t *testing.T) {
		require.NoError(t, ValidateGetLockRequest(&gracklepb.GetLockRequest{
			NamespaceName: "validname",
			LockName:      "validlock",
		}))
	})
}

func TestValidateReleaseLockRequest(t *testing.T) {
	t.Run("empty request", func(t *testing.T) {
		require.Error(t, ValidateReleaseLockRequest(&gracklepb.ReleaseLockRequest{}))
	})

	t.Run("empty namespace name", func(t *testing.T) {
		require.Error(t, ValidateReleaseLockRequest(&gracklepb.ReleaseLockRequest{
			LockName: "validname",
			LeaseId:  "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
		}))
	})

	t.Run("empty lock name", func(t *testing.T) {
		require.Error(t, ValidateReleaseLockRequest(&gracklepb.ReleaseLockRequest{
			NamespaceName: "validname",
			LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
		}))
	})

	t.Run("empty lease id", func(t *testing.T) {
		require.Error(t, ValidateReleaseLockRequest(&gracklepb.ReleaseLockRequest{
			NamespaceName: "validname",
			LockName:      "validname",
		}))
	})

	t.Run("namespace name too long", func(t *testing.T) {
		require.Error(t, ValidateReleaseLockRequest(&gracklepb.ReleaseLockRequest{
			NamespaceName: string(make([]byte, 129)),
			LockName:      "validname",
			LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
		}))
	})

	t.Run("lock name too long", func(t *testing.T) {
		require.Error(t, ValidateReleaseLockRequest(&gracklepb.ReleaseLockRequest{
			NamespaceName: "validname",
			LockName:      string(make([]byte, 129)),
			LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
		}))
	})

	t.Run("lease id too long", func(t *testing.T) {
		require.Error(t, ValidateReleaseLockRequest(&gracklepb.ReleaseLockRequest{
			NamespaceName: "validname",
			LockName:      "validname",
			LeaseId:       string(make([]byte, 65)),
		}))
	})

	t.Run("invalid namespace name characters", func(t *testing.T) {
		require.Error(t, ValidateReleaseLockRequest(&gracklepb.ReleaseLockRequest{
			NamespaceName: "invalid name",
			LockName:      "validname",
			LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
		}))
	})

	t.Run("invalid lock name characters", func(t *testing.T) {
		require.Error(t, ValidateReleaseLockRequest(&gracklepb.ReleaseLockRequest{
			NamespaceName: "validname",
			LockName:      "invalid name",
			LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
		}))
	})

	t.Run("invalid lease id", func(t *testing.T) {
		require.Error(t, ValidateReleaseLockRequest(&gracklepb.ReleaseLockRequest{
			NamespaceName: "validname",
			LockName:      "validname",
			LeaseId:       "invalid lease id",
		}))
	})

	t.Run("valid request", func(t *testing.T) {
		require.NoError(t, ValidateReleaseLockRequest(&gracklepb.ReleaseLockRequest{
			NamespaceName: "validname",
			LockName:      "validlock",
			LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
		}))
	})
}

func TestValidateAcquireLockRequest(t *testing.T) {
	t.Run("empty request", func(t *testing.T) {
		require.Error(t, ValidateAcquireLockRequest(&gracklepb.AcquireLockRequest{}))
	})

	t.Run("empty namespace name", func(t *testing.T) {
		require.Error(t, ValidateAcquireLockRequest(&gracklepb.AcquireLockRequest{
			LockName: "validname",
			LeaseId:  "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
		}))
	})

	t.Run("empty lock name", func(t *testing.T) {
		require.Error(t, ValidateAcquireLockRequest(&gracklepb.AcquireLockRequest{
			NamespaceName: "validname",
			LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
		}))
	})

	t.Run("empty lease id", func(t *testing.T) {
		require.Error(t, ValidateAcquireLockRequest(&gracklepb.AcquireLockRequest{
			NamespaceName: "validname",
			LockName:      "validname",
		}))
	})

	t.Run("namespace name too long", func(t *testing.T) {
		require.Error(t, ValidateAcquireLockRequest(&gracklepb.AcquireLockRequest{
			NamespaceName: string(make([]byte, 129)),
			LockName:      "validname",
			LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
		}))
	})

	t.Run("lock name too long", func(t *testing.T) {
		require.Error(t, ValidateAcquireLockRequest(&gracklepb.AcquireLockRequest{
			NamespaceName: "validname",
			LockName:      string(make([]byte, 129)),
			LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
		}))
	})

	t.Run("lease id too long", func(t *testing.T) {
		require.Error(t, ValidateAcquireLockRequest(&gracklepb.AcquireLockRequest{
			NamespaceName: "validname",
			LockName:      "validname",
			LeaseId:       string(make([]byte, 65)),
		}))
	})

	t.Run("invalid namespace name characters", func(t *testing.T) {
		require.Error(t, ValidateAcquireLockRequest(&gracklepb.AcquireLockRequest{
			NamespaceName: "invalid name",
			LockName:      "validname",
			LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
		}))
	})

	t.Run("invalid lock name characters", func(t *testing.T) {
		require.Error(t, ValidateAcquireLockRequest(&gracklepb.AcquireLockRequest{
			NamespaceName: "validname",
			LockName:      "invalid name",
			LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
		}))
	})

	t.Run("invalid lease id", func(t *testing.T) {
		require.Error(t, ValidateAcquireLockRequest(&gracklepb.AcquireLockRequest{
			NamespaceName: "validname",
			LockName:      "validname",
			LeaseId:       "invalid lease id",
		}))
	})

	t.Run("valid request", func(t *testing.T) {
		require.NoError(t, ValidateAcquireLockRequest(&gracklepb.AcquireLockRequest{
			NamespaceName: "validname",
			LockName:      "validlock",
			LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
		}))
	})
}

func TestValidateCreateSemaphoreRequest(t *testing.T) {
	t.Run("empty request", func(t *testing.T) {
		require.Error(t, ValidateCreateSemaphoreRequest(&gracklepb.CreateSemaphoreRequest{}))
	})

	t.Run("empty namespace name", func(t *testing.T) {
		require.Error(t, ValidateCreateSemaphoreRequest(&gracklepb.CreateSemaphoreRequest{
			SemaphoreName: "validname",
			Description:   "validdescription",
			Permits:       1,
		}))
	})

	t.Run("empty semaphore name", func(t *testing.T) {
		require.Error(t, ValidateCreateSemaphoreRequest(&gracklepb.CreateSemaphoreRequest{
			NamespaceName: "validname",
			Description:   "validdescription",
			Permits:       1,
		}))
	})

	t.Run("namespace name too long", func(t *testing.T) {
		require.Error(t, ValidateCreateSemaphoreRequest(&gracklepb.CreateSemaphoreRequest{
			NamespaceName: string(make([]byte, 129)),
			SemaphoreName: "validname",
			Description:   "validdescription",
			Permits:       1,
		}))
	})

	t.Run("semaphore name too long", func(t *testing.T) {
		require.Error(t, ValidateCreateSemaphoreRequest(&gracklepb.CreateSemaphoreRequest{
			NamespaceName: "validname",
			SemaphoreName: string(make([]byte, 129)),
			Description:   "validdescription",
			Permits:       1,
		}))
	})

	t.Run("invalid namespace name characters", func(t *testing.T) {
		require.Error(t, ValidateCreateSemaphoreRequest(&gracklepb.CreateSemaphoreRequest{
			NamespaceName: "invalid name",
			SemaphoreName: "validname",
			Description:   "validdescription",
			Permits:       1,
		}))
	})

	t.Run("invalid semaphore name characters", func(t *testing.T) {
		require.Error(t, ValidateCreateSemaphoreRequest(&gracklepb.CreateSemaphoreRequest{
			NamespaceName: "validname",
			SemaphoreName: "invalid name",
			Description:   "validdescription",
			Permits:       1,
		}))
	})

	t.Run("permits must be greater than 0", func(t *testing.T) {
		require.Error(t, ValidateCreateSemaphoreRequest(&gracklepb.CreateSemaphoreRequest{
			NamespaceName: "validname",
			SemaphoreName: "validsemaphore",
			Description:   "validdescription",
			Permits:       0,
		}))
	})

	t.Run("valid request", func(t *testing.T) {
		require.NoError(t, ValidateCreateSemaphoreRequest(&gracklepb.CreateSemaphoreRequest{
			NamespaceName: "validname",
			SemaphoreName: "validsemaphore",
			Description:   "validdescription",
			Permits:       1,
		}))
	})
}

func TestValidateGetSemaphoreRequest(t *testing.T) {
	t.Run("empty request", func(t *testing.T) {
		require.Error(t, ValidateGetSemaphoreRequest(&gracklepb.GetSemaphoreRequest{}))
	})

	t.Run("empty namespace name", func(t *testing.T) {
		require.Error(t, ValidateGetSemaphoreRequest(&gracklepb.GetSemaphoreRequest{
			SemaphoreName: "validname",
		}))
	})

	t.Run("empty semaphore name", func(t *testing.T) {
		require.Error(t, ValidateGetSemaphoreRequest(&gracklepb.GetSemaphoreRequest{
			NamespaceName: "validname",
		}))
	})

	t.Run("namespace name too long", func(t *testing.T) {
		require.Error(t, ValidateGetSemaphoreRequest(&gracklepb.GetSemaphoreRequest{
			NamespaceName: string(make([]byte, 129)),
			SemaphoreName: "validname",
		}))
	})

	t.Run("semaphore name too long", func(t *testing.T) {
		require.Error(t, ValidateGetSemaphoreRequest(&gracklepb.GetSemaphoreRequest{
			NamespaceName: "validname",
			SemaphoreName: string(make([]byte, 129)),
		}))
	})

	t.Run("invalid namespace name characters", func(t *testing.T) {
		require.Error(t, ValidateGetSemaphoreRequest(&gracklepb.GetSemaphoreRequest{
			NamespaceName: "invalid name",
			SemaphoreName: "validname",
		}))
	})

	t.Run("invalid semaphore name characters", func(t *testing.T) {
		require.Error(t, ValidateGetSemaphoreRequest(&gracklepb.GetSemaphoreRequest{
			NamespaceName: "validname",
			SemaphoreName: "invalid name",
		}))
	})

	t.Run("valid request", func(t *testing.T) {
		require.NoError(t, ValidateGetSemaphoreRequest(&gracklepb.GetSemaphoreRequest{
			NamespaceName: "validname",
			SemaphoreName: "validsemaphore",
		}))
	})
}

func TestValidateReleaseSemaphoreRequest(t *testing.T) {
	t.Run("empty request", func(t *testing.T) {
		require.Error(t, ValidateReleaseSemaphoreRequest(&gracklepb.ReleaseSemaphoreRequest{}))
	})

	t.Run("empty namespace name", func(t *testing.T) {
		require.Error(t, ValidateReleaseSemaphoreRequest(&gracklepb.ReleaseSemaphoreRequest{
			SemaphoreName: "validname",
			LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
		}))
	})

	t.Run("empty semaphore name", func(t *testing.T) {
		require.Error(t, ValidateReleaseSemaphoreRequest(&gracklepb.ReleaseSemaphoreRequest{
			NamespaceName: "validname",
			LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
		}))
	})

	t.Run("empty process id", func(t *testing.T) {
		require.Error(t, ValidateReleaseSemaphoreRequest(&gracklepb.ReleaseSemaphoreRequest{
			NamespaceName: "validname",
			SemaphoreName: "validname",
		}))
	})

	t.Run("namespace name too long", func(t *testing.T) {
		require.Error(t, ValidateReleaseSemaphoreRequest(&gracklepb.ReleaseSemaphoreRequest{
			NamespaceName: string(make([]byte, 129)),
			SemaphoreName: "validname",
			LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
		}))
	})

	t.Run("semaphore name too long", func(t *testing.T) {
		require.Error(t, ValidateReleaseSemaphoreRequest(&gracklepb.ReleaseSemaphoreRequest{
			NamespaceName: "validname",
			SemaphoreName: string(make([]byte, 129)),
			LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
		}))
	})

	t.Run("lease id too long", func(t *testing.T) {
		require.Error(t, ValidateReleaseSemaphoreRequest(&gracklepb.ReleaseSemaphoreRequest{
			NamespaceName: "validname",
			SemaphoreName: "validname",
			LeaseId:       string(make([]byte, 65)),
		}))
	})

	t.Run("invalid namespace name characters", func(t *testing.T) {
		require.Error(t, ValidateReleaseSemaphoreRequest(&gracklepb.ReleaseSemaphoreRequest{
			NamespaceName: "invalid name",
			SemaphoreName: "validname",
			LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
		}))
	})

	t.Run("invalid semaphore name characters", func(t *testing.T) {
		require.Error(t, ValidateReleaseSemaphoreRequest(&gracklepb.ReleaseSemaphoreRequest{
			NamespaceName: "validname",
			SemaphoreName: "invalid name",
			LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
		}))
	})

	t.Run("invalid lease id", func(t *testing.T) {
		require.Error(t, ValidateReleaseSemaphoreRequest(&gracklepb.ReleaseSemaphoreRequest{
			NamespaceName: "validname",
			SemaphoreName: "validname",
			LeaseId:       "invalid lease id",
		}))
	})

	t.Run("valid request", func(t *testing.T) {
		require.NoError(t, ValidateReleaseSemaphoreRequest(&gracklepb.ReleaseSemaphoreRequest{
			NamespaceName: "validname",
			SemaphoreName: "validsemaphore",
			LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
		}))
	})
}

func TestValidateUpdateSemaphoreRequest(t *testing.T) {
	t.Run("empty request", func(t *testing.T) {
		require.Error(t, ValidateUpdateSemaphoreRequest(&gracklepb.UpdateSemaphoreRequest{}))
	})

	t.Run("empty namespace name", func(t *testing.T) {
		require.Error(t, ValidateUpdateSemaphoreRequest(&gracklepb.UpdateSemaphoreRequest{
			SemaphoreName: "validname",
			Description:   "validdescription",
			Permits:       1,
		}))
	})

	t.Run("empty semaphore name", func(t *testing.T) {
		require.Error(t, ValidateUpdateSemaphoreRequest(&gracklepb.UpdateSemaphoreRequest{
			NamespaceName: "validname",
			Description:   "validdescription",
			Permits:       1,
		}))
	})

	t.Run("namespace name too long", func(t *testing.T) {
		require.Error(t, ValidateUpdateSemaphoreRequest(&gracklepb.UpdateSemaphoreRequest{
			NamespaceName: string(make([]byte, 129)),
			SemaphoreName: "validname",
			Description:   "validdescription",
			Permits:       1,
		}))
	})

	t.Run("semaphore name too long", func(t *testing.T) {
		require.Error(t, ValidateUpdateSemaphoreRequest(&gracklepb.UpdateSemaphoreRequest{
			NamespaceName: "validname",
			SemaphoreName: string(make([]byte, 129)),
			Description:   "validdescription",
			Permits:       1,
		}))
	})

	t.Run("invalid namespace name characters", func(t *testing.T) {
		require.Error(t, ValidateUpdateSemaphoreRequest(&gracklepb.UpdateSemaphoreRequest{
			NamespaceName: "invalid name",
			SemaphoreName: "validname",
			Description:   "validdescription",
			Permits:       1,
		}))
	})

	t.Run("invalid semaphore name characters", func(t *testing.T) {
		require.Error(t, ValidateUpdateSemaphoreRequest(&gracklepb.UpdateSemaphoreRequest{
			NamespaceName: "validname",
			SemaphoreName: "invalid name",
			Description:   "validdescription",
			Permits:       1,
		}))
	})

	t.Run("permits must be greater than 0", func(t *testing.T) {
		require.Error(t, ValidateUpdateSemaphoreRequest(&gracklepb.UpdateSemaphoreRequest{
			NamespaceName: "validname",
			SemaphoreName: "validsemaphore",
			Description:   "validdescription",
			Permits:       0,
		}))
	})

	t.Run("valid request", func(t *testing.T) {
		require.NoError(t, ValidateUpdateSemaphoreRequest(&gracklepb.UpdateSemaphoreRequest{
			NamespaceName: "validname",
			SemaphoreName: "validsemaphore",
			Description:   "validdescription",
			Permits:       1,
		}))
	})
}

func TestValidateDeleteSemaphoreRequest(t *testing.T) {
	t.Run("empty request", func(t *testing.T) {
		require.Error(t, ValidateDeleteSemaphoreRequest(&gracklepb.DeleteSemaphoreRequest{}))
	})

	t.Run("empty namespace name", func(t *testing.T) {
		require.Error(t, ValidateDeleteSemaphoreRequest(&gracklepb.DeleteSemaphoreRequest{
			SemaphoreName: "validname",
		}))
	})

	t.Run("empty semaphore name", func(t *testing.T) {
		require.Error(t, ValidateDeleteSemaphoreRequest(&gracklepb.DeleteSemaphoreRequest{
			NamespaceName: "validname",
		}))
	})

	t.Run("namespace name too long", func(t *testing.T) {
		require.Error(t, ValidateDeleteSemaphoreRequest(&gracklepb.DeleteSemaphoreRequest{
			NamespaceName: string(make([]byte, 129)),
			SemaphoreName: "validname",
		}))
	})

	t.Run("semaphore name too long", func(t *testing.T) {
		require.Error(t, ValidateDeleteSemaphoreRequest(&gracklepb.DeleteSemaphoreRequest{
			NamespaceName: "validname",
			SemaphoreName: string(make([]byte, 129)),
		}))
	})

	t.Run("invalid namespace name characters", func(t *testing.T) {
		require.Error(t, ValidateDeleteSemaphoreRequest(&gracklepb.DeleteSemaphoreRequest{
			NamespaceName: "invalid name",
			SemaphoreName: "validname",
		}))
	})

	t.Run("invalid semaphore name characters", func(t *testing.T) {
		require.Error(t, ValidateDeleteSemaphoreRequest(&gracklepb.DeleteSemaphoreRequest{
			NamespaceName: "validname",
			SemaphoreName: "invalid name",
		}))
	})

	t.Run("valid request", func(t *testing.T) {
		require.NoError(t, ValidateDeleteSemaphoreRequest(&gracklepb.DeleteSemaphoreRequest{
			NamespaceName: "validname",
			SemaphoreName: "validsemaphore",
		}))
	})
}

func TestValidateListLocksRequest(t *testing.T) {
	t.Run("empty request - should fail due to missing namespace name", func(t *testing.T) {
		require.Error(t, ValidateListLocksRequest(&gracklepb.ListLocksRequest{}))
	})

	t.Run("empty namespace name", func(t *testing.T) {
		require.Error(t, ValidateListLocksRequest(&gracklepb.ListLocksRequest{
			NamespaceName: "",
		}))
	})

	t.Run("namespace name too long", func(t *testing.T) {
		require.Error(t, ValidateListLocksRequest(&gracklepb.ListLocksRequest{
			NamespaceName: string(make([]byte, 129)),
		}))
	})

	t.Run("invalid namespace name characters", func(t *testing.T) {
		require.Error(t, ValidateListLocksRequest(&gracklepb.ListLocksRequest{
			NamespaceName: "invalid name",
		}))
	})

	t.Run("valid request with no pagination", func(t *testing.T) {
		require.NoError(t, ValidateListLocksRequest(&gracklepb.ListLocksRequest{
			NamespaceName:   "validname",
			PaginationToken: "",
			Limit:           0,
		}))
	})

	t.Run("valid request with pagination token", func(t *testing.T) {
		require.NoError(t, ValidateListLocksRequest(&gracklepb.ListLocksRequest{
			NamespaceName:   "validname",
			PaginationToken: "dGVzdA==",
			Limit:           0,
		}))
	})

	t.Run("valid request with limit", func(t *testing.T) {
		require.NoError(t, ValidateListLocksRequest(&gracklepb.ListLocksRequest{
			NamespaceName:   "validname",
			PaginationToken: "",
			Limit:           50,
		}))
	})

	t.Run("valid request with both pagination token and limit", func(t *testing.T) {
		require.NoError(t, ValidateListLocksRequest(&gracklepb.ListLocksRequest{
			NamespaceName:   "validname",
			PaginationToken: "dGVzdA==",
			Limit:           100,
		}))
	})

	t.Run("invalid pagination token (not base64)", func(t *testing.T) {
		require.Error(t, ValidateListLocksRequest(&gracklepb.ListLocksRequest{
			NamespaceName:   "validname",
			PaginationToken: "invalid-base64!@#",
			Limit:           50,
		}))
	})

	t.Run("pagination token too long", func(t *testing.T) {
		longToken := string(make([]byte, 1025))
		require.Error(t, ValidateListLocksRequest(&gracklepb.ListLocksRequest{
			NamespaceName:   "validname",
			PaginationToken: longToken,
			Limit:           50,
		}))
	})

	t.Run("limit too high", func(t *testing.T) {
		require.Error(t, ValidateListLocksRequest(&gracklepb.ListLocksRequest{
			NamespaceName:   "validname",
			PaginationToken: "",
			Limit:           101,
		}))
	})

	t.Run("negative limit", func(t *testing.T) {
		require.Error(t, ValidateListLocksRequest(&gracklepb.ListLocksRequest{
			NamespaceName:   "validname",
			PaginationToken: "",
			Limit:           -1,
		}))
	})

	t.Run("edge case: limit at maximum", func(t *testing.T) {
		require.NoError(t, ValidateListLocksRequest(&gracklepb.ListLocksRequest{
			NamespaceName:   "validname",
			PaginationToken: "",
			Limit:           100,
		}))
	})

	t.Run("valid base64 token at maximum length", func(t *testing.T) {
		validMaxToken := base64.StdEncoding.EncodeToString(make([]byte, 768)) // 768 bytes = 1024 base64 chars
		require.NoError(t, ValidateListLocksRequest(&gracklepb.ListLocksRequest{
			NamespaceName:   "validname",
			PaginationToken: validMaxToken,
			Limit:           50,
		}))
	})
}

func TestValidateListSemaphoresRequest(t *testing.T) {
	t.Run("empty request - should fail due to missing namespace name", func(t *testing.T) {
		require.Error(t, ValidateListSemaphoresRequest(&gracklepb.ListSemaphoresRequest{}))
	})

	t.Run("empty namespace name", func(t *testing.T) {
		require.Error(t, ValidateListSemaphoresRequest(&gracklepb.ListSemaphoresRequest{
			NamespaceName: "",
		}))
	})

	t.Run("namespace name too long", func(t *testing.T) {
		require.Error(t, ValidateListSemaphoresRequest(&gracklepb.ListSemaphoresRequest{
			NamespaceName: string(make([]byte, 129)),
		}))
	})

	t.Run("invalid namespace name characters", func(t *testing.T) {
		require.Error(t, ValidateListSemaphoresRequest(&gracklepb.ListSemaphoresRequest{
			NamespaceName: "invalid name",
		}))
	})

	t.Run("valid request with no pagination", func(t *testing.T) {
		require.NoError(t, ValidateListSemaphoresRequest(&gracklepb.ListSemaphoresRequest{
			NamespaceName:   "validname",
			PaginationToken: "",
			Limit:           0,
		}))
	})

	t.Run("valid request with pagination token", func(t *testing.T) {
		require.NoError(t, ValidateListSemaphoresRequest(&gracklepb.ListSemaphoresRequest{
			NamespaceName:   "validname",
			PaginationToken: "dGVzdA==",
			Limit:           0,
		}))
	})

	t.Run("valid request with limit", func(t *testing.T) {
		require.NoError(t, ValidateListSemaphoresRequest(&gracklepb.ListSemaphoresRequest{
			NamespaceName:   "validname",
			PaginationToken: "",
			Limit:           50,
		}))
	})

	t.Run("valid request with both pagination token and limit", func(t *testing.T) {
		require.NoError(t, ValidateListSemaphoresRequest(&gracklepb.ListSemaphoresRequest{
			NamespaceName:   "validname",
			PaginationToken: "dGVzdA==",
			Limit:           100,
		}))
	})

	t.Run("invalid pagination token (not base64)", func(t *testing.T) {
		require.Error(t, ValidateListSemaphoresRequest(&gracklepb.ListSemaphoresRequest{
			NamespaceName:   "validname",
			PaginationToken: "invalid-base64!@#",
			Limit:           50,
		}))
	})

	t.Run("pagination token too long", func(t *testing.T) {
		longToken := string(make([]byte, 1025))
		require.Error(t, ValidateListSemaphoresRequest(&gracklepb.ListSemaphoresRequest{
			NamespaceName:   "validname",
			PaginationToken: longToken,
			Limit:           50,
		}))
	})

	t.Run("limit too high", func(t *testing.T) {
		require.Error(t, ValidateListSemaphoresRequest(&gracklepb.ListSemaphoresRequest{
			NamespaceName:   "validname",
			PaginationToken: "",
			Limit:           101,
		}))
	})

	t.Run("negative limit", func(t *testing.T) {
		require.Error(t, ValidateListSemaphoresRequest(&gracklepb.ListSemaphoresRequest{
			NamespaceName:   "validname",
			PaginationToken: "",
			Limit:           -1,
		}))
	})

	t.Run("edge case: limit at maximum", func(t *testing.T) {
		require.NoError(t, ValidateListSemaphoresRequest(&gracklepb.ListSemaphoresRequest{
			NamespaceName:   "validname",
			PaginationToken: "",
			Limit:           100,
		}))
	})

	t.Run("valid base64 token at maximum length", func(t *testing.T) {
		validMaxToken := base64.StdEncoding.EncodeToString(make([]byte, 768)) // 768 bytes = 1024 base64 chars
		require.NoError(t, ValidateListSemaphoresRequest(&gracklepb.ListSemaphoresRequest{
			NamespaceName:   "validname",
			PaginationToken: validMaxToken,
			Limit:           50,
		}))
	})
}

func TestValidateAcquireSemaphoreRequest(t *testing.T) {
	t.Run("empty request", func(t *testing.T) {
		require.Error(t, ValidateAcquireSemaphoreRequest(&gracklepb.AcquireSemaphoreRequest{}))
	})

	t.Run("empty namespace name", func(t *testing.T) {
		require.Error(t, ValidateAcquireSemaphoreRequest(&gracklepb.AcquireSemaphoreRequest{
			SemaphoreName:  "validname",
			LeaseId:        "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
			TimeoutSeconds: 10,
			Weight:         1,
		}))
	})

	t.Run("empty semaphore name", func(t *testing.T) {
		require.Error(t, ValidateAcquireSemaphoreRequest(&gracklepb.AcquireSemaphoreRequest{
			NamespaceName:  "validname",
			LeaseId:        "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
			TimeoutSeconds: 10,
			Weight:         1,
		}))
	})

	t.Run("empty process id", func(t *testing.T) {
		require.Error(t, ValidateAcquireSemaphoreRequest(&gracklepb.AcquireSemaphoreRequest{
			NamespaceName:  "validname",
			SemaphoreName:  "validname",
			TimeoutSeconds: 10,
			Weight:         1,
		}))
	})

	t.Run("namespace name too long", func(t *testing.T) {
		require.Error(t, ValidateAcquireSemaphoreRequest(&gracklepb.AcquireSemaphoreRequest{
			NamespaceName:  string(make([]byte, 129)),
			SemaphoreName:  "validname",
			LeaseId:        "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
			TimeoutSeconds: 10,
			Weight:         1,
		}))
	})

	t.Run("semaphore name too long", func(t *testing.T) {
		require.Error(t, ValidateAcquireSemaphoreRequest(&gracklepb.AcquireSemaphoreRequest{
			NamespaceName:  "validname",
			SemaphoreName:  string(make([]byte, 129)),
			LeaseId:        "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
			TimeoutSeconds: 10,
			Weight:         1,
		}))
	})

	t.Run("lease id too long", func(t *testing.T) {
		require.Error(t, ValidateAcquireSemaphoreRequest(&gracklepb.AcquireSemaphoreRequest{
			NamespaceName:  "validname",
			SemaphoreName:  "validname",
			LeaseId:        string(make([]byte, 65)),
			TimeoutSeconds: 10,
			Weight:         1,
		}))
	})

	t.Run("invalid namespace name characters", func(t *testing.T) {
		require.Error(t, ValidateAcquireSemaphoreRequest(&gracklepb.AcquireSemaphoreRequest{
			NamespaceName:  "invalid name",
			SemaphoreName:  "validname",
			LeaseId:        "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
			TimeoutSeconds: 10,
			Weight:         1,
		}))
	})

	t.Run("invalid semaphore name characters", func(t *testing.T) {
		require.Error(t, ValidateAcquireSemaphoreRequest(&gracklepb.AcquireSemaphoreRequest{
			NamespaceName:  "validname",
			SemaphoreName:  "invalid name",
			LeaseId:        "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
			TimeoutSeconds: 10,
			Weight:         1,
		}))
	})

	t.Run("invalid lease id", func(t *testing.T) {
		require.Error(t, ValidateAcquireSemaphoreRequest(&gracklepb.AcquireSemaphoreRequest{
			NamespaceName:  "validname",
			SemaphoreName:  "validname",
			LeaseId:        "invalid lease id",
			TimeoutSeconds: 10,
			Weight:         1,
		}))
	})

	t.Run("timeout seconds zero", func(t *testing.T) {
		require.Error(t, ValidateAcquireSemaphoreRequest(&gracklepb.AcquireSemaphoreRequest{
			NamespaceName:  "validname",
			SemaphoreName:  "validname",
			LeaseId:        "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
			TimeoutSeconds: 0,
			Weight:         1,
		}))
	})

	t.Run("timeout seconds negative", func(t *testing.T) {
		require.Error(t, ValidateAcquireSemaphoreRequest(&gracklepb.AcquireSemaphoreRequest{
			NamespaceName:  "validname",
			SemaphoreName:  "validname",
			LeaseId:        "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
			TimeoutSeconds: -1,
			Weight:         1,
		}))
	})

	t.Run("timeout seconds too high", func(t *testing.T) {
		require.Error(t, ValidateAcquireSemaphoreRequest(&gracklepb.AcquireSemaphoreRequest{
			NamespaceName:  "validname",
			SemaphoreName:  "validname",
			LeaseId:        "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
			TimeoutSeconds: 301,
			Weight:         1,
		}))
	})

	t.Run("zero weight", func(t *testing.T) {
		require.Error(t, ValidateAcquireSemaphoreRequest(&gracklepb.AcquireSemaphoreRequest{
			NamespaceName:  "validname",
			SemaphoreName:  "validname",
			LeaseId:        "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
			TimeoutSeconds: 10,
			Weight:         0,
		}))
	})

	t.Run("valid request", func(t *testing.T) {
		require.NoError(t, ValidateAcquireSemaphoreRequest(&gracklepb.AcquireSemaphoreRequest{
			NamespaceName:  "validname",
			SemaphoreName:  "validname",
			LeaseId:        "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
			TimeoutSeconds: 10,
			Weight:         1,
		}))
	})

	t.Run("valid request with maximum timeout", func(t *testing.T) {
		require.NoError(t, ValidateAcquireSemaphoreRequest(&gracklepb.AcquireSemaphoreRequest{
			NamespaceName:  "validname",
			SemaphoreName:  "validname",
			LeaseId:        "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
			TimeoutSeconds: 300,
			Weight:         1,
		}))
	})
}

func TestValidateListWaitGroupJobsRequest(t *testing.T) {
	t.Run("empty request - should fail due to missing namespace name", func(t *testing.T) {
		require.Error(t, ValidateListWaitGroupJobsRequest(&gracklepb.ListWaitGroupJobsRequest{}))
	})

	t.Run("missing namespace name", func(t *testing.T) {
		require.Error(t, ValidateListWaitGroupJobsRequest(&gracklepb.ListWaitGroupJobsRequest{
			WaitGroupName: "validname",
		}))
	})

	t.Run("missing wait group name", func(t *testing.T) {
		require.Error(t, ValidateListWaitGroupJobsRequest(&gracklepb.ListWaitGroupJobsRequest{
			NamespaceName: "validname",
		}))
	})

	t.Run("invalid namespace name characters", func(t *testing.T) {
		require.Error(t, ValidateListWaitGroupJobsRequest(&gracklepb.ListWaitGroupJobsRequest{
			NamespaceName: "invalid name",
			WaitGroupName: "validname",
		}))
	})

	t.Run("invalid wait group name characters", func(t *testing.T) {
		require.Error(t, ValidateListWaitGroupJobsRequest(&gracklepb.ListWaitGroupJobsRequest{
			NamespaceName: "validname",
			WaitGroupName: "invalid name",
		}))
	})

	t.Run("namespace name too long", func(t *testing.T) {
		require.Error(t, ValidateListWaitGroupJobsRequest(&gracklepb.ListWaitGroupJobsRequest{
			NamespaceName: string(make([]byte, 129)),
			WaitGroupName: "validname",
		}))
	})

	t.Run("wait group name too long", func(t *testing.T) {
		require.Error(t, ValidateListWaitGroupJobsRequest(&gracklepb.ListWaitGroupJobsRequest{
			NamespaceName: "validname",
			WaitGroupName: string(make([]byte, 129)),
		}))
	})

	t.Run("invalid pagination token (not base64)", func(t *testing.T) {
		require.Error(t, ValidateListWaitGroupJobsRequest(&gracklepb.ListWaitGroupJobsRequest{
			NamespaceName:   "validname",
			WaitGroupName:   "validname",
			PaginationToken: "invalid-base64!@#",
		}))
	})

	t.Run("pagination token too long", func(t *testing.T) {
		longToken := string(make([]byte, 1025))
		require.Error(t, ValidateListWaitGroupJobsRequest(&gracklepb.ListWaitGroupJobsRequest{
			NamespaceName:   "validname",
			WaitGroupName:   "validname",
			PaginationToken: longToken,
		}))
	})

	t.Run("limit too high", func(t *testing.T) {
		require.Error(t, ValidateListWaitGroupJobsRequest(&gracklepb.ListWaitGroupJobsRequest{
			NamespaceName: "validname",
			WaitGroupName: "validname",
			Limit:         101,
		}))
	})

	t.Run("negative limit", func(t *testing.T) {
		require.Error(t, ValidateListWaitGroupJobsRequest(&gracklepb.ListWaitGroupJobsRequest{
			NamespaceName: "validname",
			WaitGroupName: "validname",
			Limit:         -1,
		}))
	})

	t.Run("valid request", func(t *testing.T) {
		require.NoError(t, ValidateListWaitGroupJobsRequest(&gracklepb.ListWaitGroupJobsRequest{
			NamespaceName: "validname",
			WaitGroupName: "validname",
		}))
	})

	t.Run("valid request with pagination", func(t *testing.T) {
		require.NoError(t, ValidateListWaitGroupJobsRequest(&gracklepb.ListWaitGroupJobsRequest{
			NamespaceName:   "validname",
			WaitGroupName:   "validname",
			PaginationToken: "dGVzdA==",
			Limit:           50,
		}))
	})
}

func TestValidateListSemaphoreHoldersRequest(t *testing.T) {
	t.Run("empty request - should fail due to missing namespace name", func(t *testing.T) {
		require.Error(t, ValidateListSemaphoreHoldersRequest(&gracklepb.ListSemaphoreHoldersRequest{}))
	})

	t.Run("missing namespace name", func(t *testing.T) {
		require.Error(t, ValidateListSemaphoreHoldersRequest(&gracklepb.ListSemaphoreHoldersRequest{
			SemaphoreName: "validname",
		}))
	})

	t.Run("missing semaphore name", func(t *testing.T) {
		require.Error(t, ValidateListSemaphoreHoldersRequest(&gracklepb.ListSemaphoreHoldersRequest{
			NamespaceName: "validname",
		}))
	})

	t.Run("invalid namespace name characters", func(t *testing.T) {
		require.Error(t, ValidateListSemaphoreHoldersRequest(&gracklepb.ListSemaphoreHoldersRequest{
			NamespaceName: "invalid name",
			SemaphoreName: "validname",
		}))
	})

	t.Run("invalid semaphore name characters", func(t *testing.T) {
		require.Error(t, ValidateListSemaphoreHoldersRequest(&gracklepb.ListSemaphoreHoldersRequest{
			NamespaceName: "validname",
			SemaphoreName: "invalid name",
		}))
	})

	t.Run("namespace name too long", func(t *testing.T) {
		require.Error(t, ValidateListSemaphoreHoldersRequest(&gracklepb.ListSemaphoreHoldersRequest{
			NamespaceName: string(make([]byte, 129)),
			SemaphoreName: "validname",
		}))
	})

	t.Run("semaphore name too long", func(t *testing.T) {
		require.Error(t, ValidateListSemaphoreHoldersRequest(&gracklepb.ListSemaphoreHoldersRequest{
			NamespaceName: "validname",
			SemaphoreName: string(make([]byte, 129)),
		}))
	})

	t.Run("invalid pagination token (not base64)", func(t *testing.T) {
		require.Error(t, ValidateListSemaphoreHoldersRequest(&gracklepb.ListSemaphoreHoldersRequest{
			NamespaceName:   "validname",
			SemaphoreName:   "validname",
			PaginationToken: "invalid-base64!@#",
		}))
	})

	t.Run("pagination token too long", func(t *testing.T) {
		longToken := string(make([]byte, 1025))
		require.Error(t, ValidateListSemaphoreHoldersRequest(&gracklepb.ListSemaphoreHoldersRequest{
			NamespaceName:   "validname",
			SemaphoreName:   "validname",
			PaginationToken: longToken,
		}))
	})

	t.Run("limit too high", func(t *testing.T) {
		require.Error(t, ValidateListSemaphoreHoldersRequest(&gracklepb.ListSemaphoreHoldersRequest{
			NamespaceName: "validname",
			SemaphoreName: "validname",
			Limit:         101,
		}))
	})

	t.Run("negative limit", func(t *testing.T) {
		require.Error(t, ValidateListSemaphoreHoldersRequest(&gracklepb.ListSemaphoreHoldersRequest{
			NamespaceName: "validname",
			SemaphoreName: "validname",
			Limit:         -1,
		}))
	})

	t.Run("valid request", func(t *testing.T) {
		require.NoError(t, ValidateListSemaphoreHoldersRequest(&gracklepb.ListSemaphoreHoldersRequest{
			NamespaceName: "validname",
			SemaphoreName: "validname",
		}))
	})

	t.Run("valid request with pagination", func(t *testing.T) {
		require.NoError(t, ValidateListSemaphoreHoldersRequest(&gracklepb.ListSemaphoreHoldersRequest{
			NamespaceName:   "validname",
			SemaphoreName:   "validname",
			PaginationToken: "dGVzdA==",
			Limit:           50,
		}))
	})
}

func TestValidateCreateBarrierRequest(t *testing.T) {
	t.Run("empty request", func(t *testing.T) {
		require.Error(t, ValidateCreateBarrierRequest(&gracklepb.CreateBarrierRequest{}))
	})

	t.Run("missing namespace name", func(t *testing.T) {
		require.Error(t, ValidateCreateBarrierRequest(&gracklepb.CreateBarrierRequest{
			BarrierName:       "validname",
			ExpectedProcesses: 3,
			ExpiresAt:         time.Now().UnixNano(),
		}))
	})

	t.Run("missing barrier name", func(t *testing.T) {
		require.Error(t, ValidateCreateBarrierRequest(&gracklepb.CreateBarrierRequest{
			NamespaceName:     "validname",
			ExpectedProcesses: 3,
			ExpiresAt:         time.Now().UnixNano(),
		}))
	})

	t.Run("invalid namespace name characters", func(t *testing.T) {
		require.Error(t, ValidateCreateBarrierRequest(&gracklepb.CreateBarrierRequest{
			NamespaceName:     "invalid name",
			BarrierName:       "validname",
			ExpectedProcesses: 3,
			ExpiresAt:         time.Now().UnixNano(),
		}))
	})

	t.Run("invalid barrier name characters", func(t *testing.T) {
		require.Error(t, ValidateCreateBarrierRequest(&gracklepb.CreateBarrierRequest{
			NamespaceName:     "validname",
			BarrierName:       "invalid name",
			ExpectedProcesses: 3,
			ExpiresAt:         time.Now().UnixNano(),
		}))
	})

	t.Run("namespace name too long", func(t *testing.T) {
		require.Error(t, ValidateCreateBarrierRequest(&gracklepb.CreateBarrierRequest{
			NamespaceName:     string(make([]byte, 129)),
			BarrierName:       "validname",
			ExpectedProcesses: 3,
			ExpiresAt:         time.Now().UnixNano(),
		}))
	})

	t.Run("barrier name too long", func(t *testing.T) {
		require.Error(t, ValidateCreateBarrierRequest(&gracklepb.CreateBarrierRequest{
			NamespaceName:     "validname",
			BarrierName:       string(make([]byte, 129)),
			ExpectedProcesses: 3,
			ExpiresAt:         time.Now().UnixNano(),
		}))
	})

	t.Run("description too long", func(t *testing.T) {
		require.Error(t, ValidateCreateBarrierRequest(&gracklepb.CreateBarrierRequest{
			NamespaceName:     "validname",
			BarrierName:       "validname",
			Description:       string(make([]byte, 1025)),
			ExpectedProcesses: 3,
			ExpiresAt:         time.Now().UnixNano(),
		}))
	})

	t.Run("expected processes zero", func(t *testing.T) {
		require.Error(t, ValidateCreateBarrierRequest(&gracklepb.CreateBarrierRequest{
			NamespaceName:     "validname",
			BarrierName:       "validname",
			ExpectedProcesses: 0,
			ExpiresAt:         time.Now().UnixNano(),
		}))
	})

	t.Run("expires at zero", func(t *testing.T) {
		require.Error(t, ValidateCreateBarrierRequest(&gracklepb.CreateBarrierRequest{
			NamespaceName:     "validname",
			BarrierName:       "validname",
			ExpectedProcesses: 3,
			ExpiresAt:         0,
		}))
	})

	t.Run("expires at negative", func(t *testing.T) {
		require.Error(t, ValidateCreateBarrierRequest(&gracklepb.CreateBarrierRequest{
			NamespaceName:     "validname",
			BarrierName:       "validname",
			ExpectedProcesses: 3,
			ExpiresAt:         -1,
		}))
	})

	t.Run("valid request", func(t *testing.T) {
		require.NoError(t, ValidateCreateBarrierRequest(&gracklepb.CreateBarrierRequest{
			NamespaceName:     "validname",
			BarrierName:       "validname",
			ExpectedProcesses: 3,
			ExpiresAt:         time.Now().UnixNano(),
		}))
	})

	t.Run("valid request with description", func(t *testing.T) {
		require.NoError(t, ValidateCreateBarrierRequest(&gracklepb.CreateBarrierRequest{
			NamespaceName:     "validname",
			BarrierName:       "validname",
			Description:       "Valid description",
			ExpectedProcesses: 3,
			ExpiresAt:         time.Now().UnixNano(),
		}))
	})
}

func TestValidateListBarriersRequest(t *testing.T) {
	t.Run("empty request - should fail due to missing namespace name", func(t *testing.T) {
		require.Error(t, ValidateListBarriersRequest(&gracklepb.ListBarriersRequest{}))
	})

	t.Run("missing namespace name", func(t *testing.T) {
		require.Error(t, ValidateListBarriersRequest(&gracklepb.ListBarriersRequest{
			PaginationToken: "",
			Limit:           0,
		}))
	})

	t.Run("invalid namespace name characters", func(t *testing.T) {
		require.Error(t, ValidateListBarriersRequest(&gracklepb.ListBarriersRequest{
			NamespaceName: "invalid name",
		}))
	})

	t.Run("namespace name too long", func(t *testing.T) {
		require.Error(t, ValidateListBarriersRequest(&gracklepb.ListBarriersRequest{
			NamespaceName: string(make([]byte, 129)),
		}))
	})

	t.Run("invalid pagination token (not base64)", func(t *testing.T) {
		require.Error(t, ValidateListBarriersRequest(&gracklepb.ListBarriersRequest{
			NamespaceName:   "validname",
			PaginationToken: "invalid-base64!@#",
		}))
	})

	t.Run("pagination token too long", func(t *testing.T) {
		longToken := string(make([]byte, 1025))
		require.Error(t, ValidateListBarriersRequest(&gracklepb.ListBarriersRequest{
			NamespaceName:   "validname",
			PaginationToken: longToken,
		}))
	})

	t.Run("limit too high", func(t *testing.T) {
		require.Error(t, ValidateListBarriersRequest(&gracklepb.ListBarriersRequest{
			NamespaceName: "validname",
			Limit:         101,
		}))
	})

	t.Run("negative limit", func(t *testing.T) {
		require.Error(t, ValidateListBarriersRequest(&gracklepb.ListBarriersRequest{
			NamespaceName: "validname",
			Limit:         -1,
		}))
	})

	t.Run("valid request", func(t *testing.T) {
		require.NoError(t, ValidateListBarriersRequest(&gracklepb.ListBarriersRequest{
			NamespaceName: "validname",
		}))
	})

	t.Run("valid request with pagination", func(t *testing.T) {
		require.NoError(t, ValidateListBarriersRequest(&gracklepb.ListBarriersRequest{
			NamespaceName:   "validname",
			PaginationToken: "dGVzdA==",
			Limit:           50,
		}))
	})
}

func TestValidateGetBarrierRequest(t *testing.T) {
	t.Run("empty request", func(t *testing.T) {
		require.Error(t, ValidateGetBarrierRequest(&gracklepb.GetBarrierRequest{}))
	})

	t.Run("missing namespace name", func(t *testing.T) {
		require.Error(t, ValidateGetBarrierRequest(&gracklepb.GetBarrierRequest{
			BarrierName: "validname",
		}))
	})

	t.Run("missing barrier name", func(t *testing.T) {
		require.Error(t, ValidateGetBarrierRequest(&gracklepb.GetBarrierRequest{
			NamespaceName: "validname",
		}))
	})

	t.Run("invalid namespace name characters", func(t *testing.T) {
		require.Error(t, ValidateGetBarrierRequest(&gracklepb.GetBarrierRequest{
			NamespaceName: "invalid name",
			BarrierName:   "validname",
		}))
	})

	t.Run("invalid barrier name characters", func(t *testing.T) {
		require.Error(t, ValidateGetBarrierRequest(&gracklepb.GetBarrierRequest{
			NamespaceName: "validname",
			BarrierName:   "invalid name",
		}))
	})

	t.Run("namespace name too long", func(t *testing.T) {
		require.Error(t, ValidateGetBarrierRequest(&gracklepb.GetBarrierRequest{
			NamespaceName: string(make([]byte, 129)),
			BarrierName:   "validname",
		}))
	})

	t.Run("barrier name too long", func(t *testing.T) {
		require.Error(t, ValidateGetBarrierRequest(&gracklepb.GetBarrierRequest{
			NamespaceName: "validname",
			BarrierName:   string(make([]byte, 129)),
		}))
	})

	t.Run("valid request", func(t *testing.T) {
		require.NoError(t, ValidateGetBarrierRequest(&gracklepb.GetBarrierRequest{
			NamespaceName: "validname",
			BarrierName:   "validname",
		}))
	})
}

func TestValidateDeleteBarrierRequest(t *testing.T) {
	t.Run("empty request", func(t *testing.T) {
		require.Error(t, ValidateDeleteBarrierRequest(&gracklepb.DeleteBarrierRequest{}))
	})

	t.Run("missing namespace name", func(t *testing.T) {
		require.Error(t, ValidateDeleteBarrierRequest(&gracklepb.DeleteBarrierRequest{
			BarrierName: "validname",
		}))
	})

	t.Run("missing barrier name", func(t *testing.T) {
		require.Error(t, ValidateDeleteBarrierRequest(&gracklepb.DeleteBarrierRequest{
			NamespaceName: "validname",
		}))
	})

	t.Run("invalid namespace name characters", func(t *testing.T) {
		require.Error(t, ValidateDeleteBarrierRequest(&gracklepb.DeleteBarrierRequest{
			NamespaceName: "invalid name",
			BarrierName:   "validname",
		}))
	})

	t.Run("invalid barrier name characters", func(t *testing.T) {
		require.Error(t, ValidateDeleteBarrierRequest(&gracklepb.DeleteBarrierRequest{
			NamespaceName: "validname",
			BarrierName:   "invalid name",
		}))
	})

	t.Run("namespace name too long", func(t *testing.T) {
		require.Error(t, ValidateDeleteBarrierRequest(&gracklepb.DeleteBarrierRequest{
			NamespaceName: string(make([]byte, 129)),
			BarrierName:   "validname",
		}))
	})

	t.Run("barrier name too long", func(t *testing.T) {
		require.Error(t, ValidateDeleteBarrierRequest(&gracklepb.DeleteBarrierRequest{
			NamespaceName: "validname",
			BarrierName:   string(make([]byte, 129)),
		}))
	})

	t.Run("valid request", func(t *testing.T) {
		require.NoError(t, ValidateDeleteBarrierRequest(&gracklepb.DeleteBarrierRequest{
			NamespaceName: "validname",
			BarrierName:   "validname",
		}))
	})
}

func TestValidateUpdateBarrierRequest(t *testing.T) {
	t.Run("empty request", func(t *testing.T) {
		require.Error(t, ValidateUpdateBarrierRequest(&gracklepb.UpdateBarrierRequest{}))
	})

	t.Run("missing namespace name", func(t *testing.T) {
		require.Error(t, ValidateUpdateBarrierRequest(&gracklepb.UpdateBarrierRequest{
			BarrierName:       "validname",
			Description:       "desc",
			ExpectedProcesses: 3,
		}))
	})

	t.Run("missing barrier name", func(t *testing.T) {
		require.Error(t, ValidateUpdateBarrierRequest(&gracklepb.UpdateBarrierRequest{
			NamespaceName:     "validname",
			Description:       "desc",
			ExpectedProcesses: 3,
		}))
	})

	t.Run("invalid namespace name characters", func(t *testing.T) {
		require.Error(t, ValidateUpdateBarrierRequest(&gracklepb.UpdateBarrierRequest{
			NamespaceName:     "invalid name",
			BarrierName:       "validname",
			Description:       "desc",
			ExpectedProcesses: 3,
		}))
	})

	t.Run("invalid barrier name characters", func(t *testing.T) {
		require.Error(t, ValidateUpdateBarrierRequest(&gracklepb.UpdateBarrierRequest{
			NamespaceName:     "validname",
			BarrierName:       "invalid name",
			Description:       "desc",
			ExpectedProcesses: 3,
		}))
	})

	t.Run("namespace name too long", func(t *testing.T) {
		require.Error(t, ValidateUpdateBarrierRequest(&gracklepb.UpdateBarrierRequest{
			NamespaceName:     string(make([]byte, 129)),
			BarrierName:       "validname",
			Description:       "desc",
			ExpectedProcesses: 3,
		}))
	})

	t.Run("barrier name too long", func(t *testing.T) {
		require.Error(t, ValidateUpdateBarrierRequest(&gracklepb.UpdateBarrierRequest{
			NamespaceName:     "validname",
			BarrierName:       string(make([]byte, 129)),
			Description:       "desc",
			ExpectedProcesses: 3,
		}))
	})

	t.Run("description too long", func(t *testing.T) {
		require.Error(t, ValidateUpdateBarrierRequest(&gracklepb.UpdateBarrierRequest{
			NamespaceName:     "validname",
			BarrierName:       "validname",
			Description:       string(make([]byte, 1025)),
			ExpectedProcesses: 3,
		}))
	})

	t.Run("expected_processes zero", func(t *testing.T) {
		require.Error(t, ValidateUpdateBarrierRequest(&gracklepb.UpdateBarrierRequest{
			NamespaceName:     "validname",
			BarrierName:       "validname",
			Description:       "desc",
			ExpectedProcesses: 0,
		}))
	})

	t.Run("valid request", func(t *testing.T) {
		require.NoError(t, ValidateUpdateBarrierRequest(&gracklepb.UpdateBarrierRequest{
			NamespaceName:     "validname",
			BarrierName:       "validname",
			Description:       "Valid description",
			ExpectedProcesses: 5,
		}))
	})
}

func TestValidateArriveAtBarrierRequest(t *testing.T) {
	t.Run("empty request", func(t *testing.T) {
		require.Error(t, ValidateArriveAtBarrierRequest(&gracklepb.ArriveAtBarrierRequest{}))
	})

	t.Run("missing namespace name", func(t *testing.T) {
		require.Error(t, ValidateArriveAtBarrierRequest(&gracklepb.ArriveAtBarrierRequest{
			BarrierName:        "validname",
			ProcessId:          "proc1",
			ExpectedGeneration: 1,
		}))
	})

	t.Run("missing barrier name", func(t *testing.T) {
		require.Error(t, ValidateArriveAtBarrierRequest(&gracklepb.ArriveAtBarrierRequest{
			NamespaceName:      "validname",
			ProcessId:          "proc1",
			ExpectedGeneration: 1,
		}))
	})

	t.Run("missing process id", func(t *testing.T) {
		require.Error(t, ValidateArriveAtBarrierRequest(&gracklepb.ArriveAtBarrierRequest{
			NamespaceName:      "validname",
			BarrierName:        "validname",
			ExpectedGeneration: 1,
		}))
	})

	t.Run("invalid namespace name characters", func(t *testing.T) {
		require.Error(t, ValidateArriveAtBarrierRequest(&gracklepb.ArriveAtBarrierRequest{
			NamespaceName:      "invalid name",
			BarrierName:        "validname",
			ProcessId:          "proc1",
			ExpectedGeneration: 1,
		}))
	})

	t.Run("invalid barrier name characters", func(t *testing.T) {
		require.Error(t, ValidateArriveAtBarrierRequest(&gracklepb.ArriveAtBarrierRequest{
			NamespaceName:      "validname",
			BarrierName:        "invalid name",
			ProcessId:          "proc1",
			ExpectedGeneration: 1,
		}))
	})

	t.Run("invalid process id characters", func(t *testing.T) {
		require.Error(t, ValidateArriveAtBarrierRequest(&gracklepb.ArriveAtBarrierRequest{
			NamespaceName:      "validname",
			BarrierName:        "validname",
			ProcessId:          "invalid process id",
			ExpectedGeneration: 1,
		}))
	})

	t.Run("namespace name too long", func(t *testing.T) {
		require.Error(t, ValidateArriveAtBarrierRequest(&gracklepb.ArriveAtBarrierRequest{
			NamespaceName:      string(make([]byte, 129)),
			BarrierName:        "validname",
			ProcessId:          "proc1",
			ExpectedGeneration: 1,
		}))
	})

	t.Run("barrier name too long", func(t *testing.T) {
		require.Error(t, ValidateArriveAtBarrierRequest(&gracklepb.ArriveAtBarrierRequest{
			NamespaceName:      "validname",
			BarrierName:        string(make([]byte, 129)),
			ProcessId:          "proc1",
			ExpectedGeneration: 1,
		}))
	})

	t.Run("process id too long", func(t *testing.T) {
		require.Error(t, ValidateArriveAtBarrierRequest(&gracklepb.ArriveAtBarrierRequest{
			NamespaceName:      "validname",
			BarrierName:        "validname",
			ProcessId:          string(make([]byte, 129)),
			ExpectedGeneration: 1,
		}))
	})

	t.Run("expected generation zero", func(t *testing.T) {
		require.Error(t, ValidateArriveAtBarrierRequest(&gracklepb.ArriveAtBarrierRequest{
			NamespaceName:      "validname",
			BarrierName:        "validname",
			ProcessId:          "proc1",
			ExpectedGeneration: 0,
		}))
	})

	t.Run("valid request", func(t *testing.T) {
		require.NoError(t, ValidateArriveAtBarrierRequest(&gracklepb.ArriveAtBarrierRequest{
			NamespaceName:      "validname",
			BarrierName:        "validname",
			ProcessId:          "proc1",
			ExpectedGeneration: 1,
		}))
	})
}

func TestValidateWaitAtBarrierRequest(t *testing.T) {
	t.Run("empty request", func(t *testing.T) {
		require.Error(t, ValidateWaitAtBarrierRequest(&gracklepb.WaitAtBarrierRequest{}))
	})

	t.Run("missing namespace name", func(t *testing.T) {
		require.Error(t, ValidateWaitAtBarrierRequest(&gracklepb.WaitAtBarrierRequest{
			BarrierName:        "validname",
			ExpectedGeneration: 1,
			TimeoutSeconds:     10,
		}))
	})

	t.Run("missing barrier name", func(t *testing.T) {
		require.Error(t, ValidateWaitAtBarrierRequest(&gracklepb.WaitAtBarrierRequest{
			NamespaceName:      "validname",
			ExpectedGeneration: 1,
			TimeoutSeconds:     10,
		}))
	})

	t.Run("invalid namespace name characters", func(t *testing.T) {
		require.Error(t, ValidateWaitAtBarrierRequest(&gracklepb.WaitAtBarrierRequest{
			NamespaceName:      "invalid name",
			BarrierName:        "validname",
			ExpectedGeneration: 1,
			TimeoutSeconds:     10,
		}))
	})

	t.Run("invalid barrier name characters", func(t *testing.T) {
		require.Error(t, ValidateWaitAtBarrierRequest(&gracklepb.WaitAtBarrierRequest{
			NamespaceName:      "validname",
			BarrierName:        "invalid name",
			ExpectedGeneration: 1,
			TimeoutSeconds:     10,
		}))
	})

	t.Run("namespace name too long", func(t *testing.T) {
		require.Error(t, ValidateWaitAtBarrierRequest(&gracklepb.WaitAtBarrierRequest{
			NamespaceName:      string(make([]byte, 129)),
			BarrierName:        "validname",
			ExpectedGeneration: 1,
			TimeoutSeconds:     10,
		}))
	})

	t.Run("barrier name too long", func(t *testing.T) {
		require.Error(t, ValidateWaitAtBarrierRequest(&gracklepb.WaitAtBarrierRequest{
			NamespaceName:      "validname",
			BarrierName:        string(make([]byte, 129)),
			ExpectedGeneration: 1,
			TimeoutSeconds:     10,
		}))
	})

	t.Run("expected generation zero", func(t *testing.T) {
		require.Error(t, ValidateWaitAtBarrierRequest(&gracklepb.WaitAtBarrierRequest{
			NamespaceName:      "validname",
			BarrierName:        "validname",
			ExpectedGeneration: 0,
			TimeoutSeconds:     10,
		}))
	})

	t.Run("timeout seconds zero", func(t *testing.T) {
		require.Error(t, ValidateWaitAtBarrierRequest(&gracklepb.WaitAtBarrierRequest{
			NamespaceName:      "validname",
			BarrierName:        "validname",
			ExpectedGeneration: 1,
			TimeoutSeconds:     0,
		}))
	})

	t.Run("timeout seconds negative", func(t *testing.T) {
		require.Error(t, ValidateWaitAtBarrierRequest(&gracklepb.WaitAtBarrierRequest{
			NamespaceName:      "validname",
			BarrierName:        "validname",
			ExpectedGeneration: 1,
			TimeoutSeconds:     -1,
		}))
	})

	t.Run("timeout seconds too high", func(t *testing.T) {
		require.Error(t, ValidateWaitAtBarrierRequest(&gracklepb.WaitAtBarrierRequest{
			NamespaceName:      "validname",
			BarrierName:        "validname",
			ExpectedGeneration: 1,
			TimeoutSeconds:     301,
		}))
	})

	t.Run("valid request", func(t *testing.T) {
		require.NoError(t, ValidateWaitAtBarrierRequest(&gracklepb.WaitAtBarrierRequest{
			NamespaceName:      "validname",
			BarrierName:        "validname",
			ExpectedGeneration: 1,
			TimeoutSeconds:     10,
		}))
	})

	t.Run("valid request with maximum timeout", func(t *testing.T) {
		require.NoError(t, ValidateWaitAtBarrierRequest(&gracklepb.WaitAtBarrierRequest{
			NamespaceName:      "validname",
			BarrierName:        "validname",
			ExpectedGeneration: 1,
			TimeoutSeconds:     300,
		}))
	})
}

func TestValidateListBarrierParticipantsRequest(t *testing.T) {
	t.Run("empty request - should fail due to missing namespace name", func(t *testing.T) {
		require.Error(t, ValidateListBarrierParticipantsRequest(&gracklepb.ListBarrierParticipantsRequest{}))
	})

	t.Run("missing namespace name", func(t *testing.T) {
		require.Error(t, ValidateListBarrierParticipantsRequest(&gracklepb.ListBarrierParticipantsRequest{
			BarrierName: "validname",
		}))
	})

	t.Run("missing barrier name", func(t *testing.T) {
		require.Error(t, ValidateListBarrierParticipantsRequest(&gracklepb.ListBarrierParticipantsRequest{
			NamespaceName: "validname",
		}))
	})

	t.Run("invalid namespace name characters", func(t *testing.T) {
		require.Error(t, ValidateListBarrierParticipantsRequest(&gracklepb.ListBarrierParticipantsRequest{
			NamespaceName: "invalid name",
			BarrierName:   "validname",
		}))
	})

	t.Run("invalid barrier name characters", func(t *testing.T) {
		require.Error(t, ValidateListBarrierParticipantsRequest(&gracklepb.ListBarrierParticipantsRequest{
			NamespaceName: "validname",
			BarrierName:   "invalid name",
		}))
	})

	t.Run("namespace name too long", func(t *testing.T) {
		require.Error(t, ValidateListBarrierParticipantsRequest(&gracklepb.ListBarrierParticipantsRequest{
			NamespaceName: string(make([]byte, 129)),
			BarrierName:   "validname",
		}))
	})

	t.Run("barrier name too long", func(t *testing.T) {
		require.Error(t, ValidateListBarrierParticipantsRequest(&gracklepb.ListBarrierParticipantsRequest{
			NamespaceName: "validname",
			BarrierName:   string(make([]byte, 129)),
		}))
	})

	t.Run("invalid pagination token (not base64)", func(t *testing.T) {
		require.Error(t, ValidateListBarrierParticipantsRequest(&gracklepb.ListBarrierParticipantsRequest{
			NamespaceName:   "validname",
			BarrierName:     "validname",
			PaginationToken: "invalid-base64!@#",
		}))
	})

	t.Run("pagination token too long", func(t *testing.T) {
		longToken := string(make([]byte, 1025))
		require.Error(t, ValidateListBarrierParticipantsRequest(&gracklepb.ListBarrierParticipantsRequest{
			NamespaceName:   "validname",
			BarrierName:     "validname",
			PaginationToken: longToken,
		}))
	})

	t.Run("limit too high", func(t *testing.T) {
		require.Error(t, ValidateListBarrierParticipantsRequest(&gracklepb.ListBarrierParticipantsRequest{
			NamespaceName: "validname",
			BarrierName:   "validname",
			Limit:         101,
		}))
	})

	t.Run("negative limit", func(t *testing.T) {
		require.Error(t, ValidateListBarrierParticipantsRequest(&gracklepb.ListBarrierParticipantsRequest{
			NamespaceName: "validname",
			BarrierName:   "validname",
			Limit:         -1,
		}))
	})

	t.Run("valid request", func(t *testing.T) {
		require.NoError(t, ValidateListBarrierParticipantsRequest(&gracklepb.ListBarrierParticipantsRequest{
			NamespaceName: "validname",
			BarrierName:   "validname",
		}))
	})

	t.Run("valid request with pagination", func(t *testing.T) {
		require.NoError(t, ValidateListBarrierParticipantsRequest(&gracklepb.ListBarrierParticipantsRequest{
			NamespaceName:   "validname",
			BarrierName:     "validname",
			PaginationToken: "dGVzdA==",
			Limit:           50,
		}))
	})

	t.Run("valid request with generation", func(t *testing.T) {
		require.NoError(t, ValidateListBarrierParticipantsRequest(&gracklepb.ListBarrierParticipantsRequest{
			NamespaceName: "validname",
			BarrierName:   "validname",
			Generation:    1,
		}))
	})
}

func TestValidateCreateSemaphoreLeaseRequest(t *testing.T) {
	t.Run("empty namespace name", func(t *testing.T) {
		require.Error(t, ValidateCreateSemaphoreLeaseRequest(&gracklepb.CreateSemaphoreLeaseRequest{
			NamespaceName: "",
			ProcessId:     "process1",
			TtlSeconds:    60,
		}))
	})

	t.Run("invalid namespace name characters", func(t *testing.T) {
		require.Error(t, ValidateCreateSemaphoreLeaseRequest(&gracklepb.CreateSemaphoreLeaseRequest{
			NamespaceName: "invalid name",
			ProcessId:     "process1",
			TtlSeconds:    60,
		}))
	})

	t.Run("namespace name too long", func(t *testing.T) {
		require.Error(t, ValidateCreateSemaphoreLeaseRequest(&gracklepb.CreateSemaphoreLeaseRequest{
			NamespaceName: string(make([]byte, 129)),
			ProcessId:     "process1",
			TtlSeconds:    60,
		}))
	})

	t.Run("empty process id", func(t *testing.T) {
		require.Error(t, ValidateCreateSemaphoreLeaseRequest(&gracklepb.CreateSemaphoreLeaseRequest{
			NamespaceName: "validname",
			ProcessId:     "",
			TtlSeconds:    60,
		}))
	})

	t.Run("invalid process id characters", func(t *testing.T) {
		require.Error(t, ValidateCreateSemaphoreLeaseRequest(&gracklepb.CreateSemaphoreLeaseRequest{
			NamespaceName: "validname",
			ProcessId:     "invalid process id",
			TtlSeconds:    60,
		}))
	})

	t.Run("process id too long", func(t *testing.T) {
		require.Error(t, ValidateCreateSemaphoreLeaseRequest(&gracklepb.CreateSemaphoreLeaseRequest{
			NamespaceName: "validname",
			ProcessId:     string(make([]byte, 129)),
			TtlSeconds:    60,
		}))
	})

	t.Run("ttl seconds zero", func(t *testing.T) {
		require.Error(t, ValidateCreateSemaphoreLeaseRequest(&gracklepb.CreateSemaphoreLeaseRequest{
			NamespaceName: "validname",
			ProcessId:     "process1",
			TtlSeconds:    0,
		}))
	})

	t.Run("ttl seconds too large", func(t *testing.T) {
		require.Error(t, ValidateCreateSemaphoreLeaseRequest(&gracklepb.CreateSemaphoreLeaseRequest{
			NamespaceName: "validname",
			ProcessId:     "process1",
			TtlSeconds:    301,
		}))
	})

	t.Run("valid request", func(t *testing.T) {
		require.NoError(t, ValidateCreateSemaphoreLeaseRequest(&gracklepb.CreateSemaphoreLeaseRequest{
			NamespaceName: "validname",
			ProcessId:     "process1",
			TtlSeconds:    60,
		}))
	})

	t.Run("valid request with max ttl", func(t *testing.T) {
		require.NoError(t, ValidateCreateSemaphoreLeaseRequest(&gracklepb.CreateSemaphoreLeaseRequest{
			NamespaceName: "validname",
			ProcessId:     "process1",
			TtlSeconds:    300,
		}))
	})
}

func TestValidateRevokeSemaphoreLeaseRequest(t *testing.T) {
	t.Run("empty namespace name", func(t *testing.T) {
		require.Error(t, ValidateRevokeSemaphoreLeaseRequest(&gracklepb.RevokeSemaphoreLeaseRequest{
			NamespaceName: "",
			LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
		}))
	})

	t.Run("invalid namespace name characters", func(t *testing.T) {
		require.Error(t, ValidateRevokeSemaphoreLeaseRequest(&gracklepb.RevokeSemaphoreLeaseRequest{
			NamespaceName: "invalid name",
			LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
		}))
	})

	t.Run("namespace name too long", func(t *testing.T) {
		require.Error(t, ValidateRevokeSemaphoreLeaseRequest(&gracklepb.RevokeSemaphoreLeaseRequest{
			NamespaceName: string(make([]byte, 129)),
			LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
		}))
	})

	t.Run("empty lease id", func(t *testing.T) {
		require.Error(t, ValidateRevokeSemaphoreLeaseRequest(&gracklepb.RevokeSemaphoreLeaseRequest{
			NamespaceName: "validname",
			LeaseId:       "",
		}))
	})

	t.Run("invalid lease id format (wrong prefix)", func(t *testing.T) {
		require.Error(t, ValidateRevokeSemaphoreLeaseRequest(&gracklepb.RevokeSemaphoreLeaseRequest{
			NamespaceName: "validname",
			LeaseId:       "err_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
		}))
	})

	t.Run("invalid lease id format (too long)", func(t *testing.T) {
		require.Error(t, ValidateRevokeSemaphoreLeaseRequest(&gracklepb.RevokeSemaphoreLeaseRequest{
			NamespaceName: "validname",
			LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRBsd",
		}))
	})

	t.Run("invalid lease id format (invalid base62)", func(t *testing.T) {
		require.Error(t, ValidateRevokeSemaphoreLeaseRequest(&gracklepb.RevokeSemaphoreLeaseRequest{
			NamespaceName: "validname",
			LeaseId:       "ls_NfKKeiPbP18NFeU3lLGr+WWgDJRB",
		}))
	})

	t.Run("lease id too long", func(t *testing.T) {
		require.Error(t, ValidateRevokeSemaphoreLeaseRequest(&gracklepb.RevokeSemaphoreLeaseRequest{
			NamespaceName: "validname",
			LeaseId:       string(make([]byte, 65)),
		}))
	})

	t.Run("valid request", func(t *testing.T) {
		require.NoError(t, ValidateRevokeSemaphoreLeaseRequest(&gracklepb.RevokeSemaphoreLeaseRequest{
			NamespaceName: "validname",
			LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
		}))
	})
}

func TestValidateRefreshSemaphoreLeaseRequest(t *testing.T) {
	t.Run("empty namespace name", func(t *testing.T) {
		require.Error(t, ValidateRefreshSemaphoreLeaseRequest(&gracklepb.RefreshSemaphoreLeaseRequest{
			NamespaceName: "",
			LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
			TtlSeconds:    60,
		}))
	})

	t.Run("invalid namespace name characters", func(t *testing.T) {
		require.Error(t, ValidateRefreshSemaphoreLeaseRequest(&gracklepb.RefreshSemaphoreLeaseRequest{
			NamespaceName: "invalid name",
			LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
			TtlSeconds:    60,
		}))
	})

	t.Run("namespace name too long", func(t *testing.T) {
		require.Error(t, ValidateRefreshSemaphoreLeaseRequest(&gracklepb.RefreshSemaphoreLeaseRequest{
			NamespaceName: string(make([]byte, 129)),
			LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
			TtlSeconds:    60,
		}))
	})

	t.Run("empty lease id", func(t *testing.T) {
		require.Error(t, ValidateRefreshSemaphoreLeaseRequest(&gracklepb.RefreshSemaphoreLeaseRequest{
			NamespaceName: "validname",
			LeaseId:       "",
			TtlSeconds:    60,
		}))
	})

	t.Run("invalid lease id format", func(t *testing.T) {
		require.Error(t, ValidateRefreshSemaphoreLeaseRequest(&gracklepb.RefreshSemaphoreLeaseRequest{
			NamespaceName: "validname",
			LeaseId:       "err_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
			TtlSeconds:    60,
		}))
	})

	t.Run("lease id too long", func(t *testing.T) {
		require.Error(t, ValidateRefreshSemaphoreLeaseRequest(&gracklepb.RefreshSemaphoreLeaseRequest{
			NamespaceName: "validname",
			LeaseId:       string(make([]byte, 65)),
			TtlSeconds:    60,
		}))
	})

	t.Run("ttl seconds zero", func(t *testing.T) {
		require.Error(t, ValidateRefreshSemaphoreLeaseRequest(&gracklepb.RefreshSemaphoreLeaseRequest{
			NamespaceName: "validname",
			LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
			TtlSeconds:    0,
		}))
	})

	t.Run("ttl seconds too large", func(t *testing.T) {
		require.Error(t, ValidateRefreshSemaphoreLeaseRequest(&gracklepb.RefreshSemaphoreLeaseRequest{
			NamespaceName: "validname",
			LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
			TtlSeconds:    301,
		}))
	})

	t.Run("valid request", func(t *testing.T) {
		require.NoError(t, ValidateRefreshSemaphoreLeaseRequest(&gracklepb.RefreshSemaphoreLeaseRequest{
			NamespaceName: "validname",
			LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
			TtlSeconds:    60,
		}))
	})

	t.Run("valid request with max ttl", func(t *testing.T) {
		require.NoError(t, ValidateRefreshSemaphoreLeaseRequest(&gracklepb.RefreshSemaphoreLeaseRequest{
			NamespaceName: "validname",
			LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
			TtlSeconds:    300,
		}))
	})
}

func TestValidateListSemaphoreLeasesRequest(t *testing.T) {
	t.Run("empty namespace name", func(t *testing.T) {
		require.Error(t, ValidateListSemaphoreLeasesRequest(&gracklepb.ListSemaphoreLeasesRequest{
			NamespaceName: "",
		}))
	})

	t.Run("invalid namespace name characters", func(t *testing.T) {
		require.Error(t, ValidateListSemaphoreLeasesRequest(&gracklepb.ListSemaphoreLeasesRequest{
			NamespaceName: "invalid name",
		}))
	})

	t.Run("namespace name too long", func(t *testing.T) {
		require.Error(t, ValidateListSemaphoreLeasesRequest(&gracklepb.ListSemaphoreLeasesRequest{
			NamespaceName: string(make([]byte, 129)),
		}))
	})

	t.Run("invalid pagination token (not base64)", func(t *testing.T) {
		require.Error(t, ValidateListSemaphoreLeasesRequest(&gracklepb.ListSemaphoreLeasesRequest{
			NamespaceName:   "validname",
			PaginationToken: "invalid-base64!@#",
		}))
	})

	t.Run("pagination token too long", func(t *testing.T) {
		require.Error(t, ValidateListSemaphoreLeasesRequest(&gracklepb.ListSemaphoreLeasesRequest{
			NamespaceName:   "validname",
			PaginationToken: string(make([]byte, 1025)),
		}))
	})

	t.Run("limit too high", func(t *testing.T) {
		require.Error(t, ValidateListSemaphoreLeasesRequest(&gracklepb.ListSemaphoreLeasesRequest{
			NamespaceName: "validname",
			Limit:         101,
		}))
	})

	t.Run("negative limit", func(t *testing.T) {
		require.Error(t, ValidateListSemaphoreLeasesRequest(&gracklepb.ListSemaphoreLeasesRequest{
			NamespaceName: "validname",
			Limit:         -1,
		}))
	})

	t.Run("valid request", func(t *testing.T) {
		require.NoError(t, ValidateListSemaphoreLeasesRequest(&gracklepb.ListSemaphoreLeasesRequest{
			NamespaceName: "validname",
		}))
	})

	t.Run("valid request with pagination", func(t *testing.T) {
		require.NoError(t, ValidateListSemaphoreLeasesRequest(&gracklepb.ListSemaphoreLeasesRequest{
			NamespaceName:   "validname",
			PaginationToken: "dGVzdA==",
			Limit:           50,
		}))
	})
}

func TestValidateGetSemaphoreLeaseRequest(t *testing.T) {
	t.Run("empty namespace name", func(t *testing.T) {
		require.Error(t, ValidateGetSemaphoreLeaseRequest(&gracklepb.GetSemaphoreLeaseRequest{
			NamespaceName: "",
			LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
		}))
	})

	t.Run("invalid namespace name characters", func(t *testing.T) {
		require.Error(t, ValidateGetSemaphoreLeaseRequest(&gracklepb.GetSemaphoreLeaseRequest{
			NamespaceName: "invalid name",
			LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
		}))
	})

	t.Run("namespace name too long", func(t *testing.T) {
		require.Error(t, ValidateGetSemaphoreLeaseRequest(&gracklepb.GetSemaphoreLeaseRequest{
			NamespaceName: string(make([]byte, 129)),
			LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
		}))
	})

	t.Run("empty lease id", func(t *testing.T) {
		require.Error(t, ValidateGetSemaphoreLeaseRequest(&gracklepb.GetSemaphoreLeaseRequest{
			NamespaceName: "validname",
			LeaseId:       "",
		}))
	})

	t.Run("invalid lease id format", func(t *testing.T) {
		require.Error(t, ValidateGetSemaphoreLeaseRequest(&gracklepb.GetSemaphoreLeaseRequest{
			NamespaceName: "validname",
			LeaseId:       "err_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
		}))
	})

	t.Run("lease id too long", func(t *testing.T) {
		require.Error(t, ValidateGetSemaphoreLeaseRequest(&gracklepb.GetSemaphoreLeaseRequest{
			NamespaceName: "validname",
			LeaseId:       string(make([]byte, 65)),
		}))
	})

	t.Run("valid request", func(t *testing.T) {
		require.NoError(t, ValidateGetSemaphoreLeaseRequest(&gracklepb.GetSemaphoreLeaseRequest{
			NamespaceName: "validname",
			LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
		}))
	})
}

func TestValidateCreateLockLeaseRequest(t *testing.T) {
	t.Run("empty namespace name", func(t *testing.T) {
		require.Error(t, ValidateCreateLockLeaseRequest(&gracklepb.CreateLockLeaseRequest{
			NamespaceName: "",
			ProcessId:     "process1",
			TtlSeconds:    60,
		}))
	})

	t.Run("invalid namespace name characters", func(t *testing.T) {
		require.Error(t, ValidateCreateLockLeaseRequest(&gracklepb.CreateLockLeaseRequest{
			NamespaceName: "invalid name",
			ProcessId:     "process1",
			TtlSeconds:    60,
		}))
	})

	t.Run("namespace name too long", func(t *testing.T) {
		require.Error(t, ValidateCreateLockLeaseRequest(&gracklepb.CreateLockLeaseRequest{
			NamespaceName: string(make([]byte, 129)),
			ProcessId:     "process1",
			TtlSeconds:    60,
		}))
	})

	t.Run("empty process id", func(t *testing.T) {
		require.Error(t, ValidateCreateLockLeaseRequest(&gracklepb.CreateLockLeaseRequest{
			NamespaceName: "validname",
			ProcessId:     "",
			TtlSeconds:    60,
		}))
	})

	t.Run("invalid process id characters", func(t *testing.T) {
		require.Error(t, ValidateCreateLockLeaseRequest(&gracklepb.CreateLockLeaseRequest{
			NamespaceName: "validname",
			ProcessId:     "invalid process id",
			TtlSeconds:    60,
		}))
	})

	t.Run("process id too long", func(t *testing.T) {
		require.Error(t, ValidateCreateLockLeaseRequest(&gracklepb.CreateLockLeaseRequest{
			NamespaceName: "validname",
			ProcessId:     string(make([]byte, 129)),
			TtlSeconds:    60,
		}))
	})

	t.Run("ttl seconds zero", func(t *testing.T) {
		require.Error(t, ValidateCreateLockLeaseRequest(&gracklepb.CreateLockLeaseRequest{
			NamespaceName: "validname",
			ProcessId:     "process1",
			TtlSeconds:    0,
		}))
	})

	t.Run("ttl seconds too large", func(t *testing.T) {
		require.Error(t, ValidateCreateLockLeaseRequest(&gracklepb.CreateLockLeaseRequest{
			NamespaceName: "validname",
			ProcessId:     "process1",
			TtlSeconds:    301,
		}))
	})

	t.Run("valid request", func(t *testing.T) {
		require.NoError(t, ValidateCreateLockLeaseRequest(&gracklepb.CreateLockLeaseRequest{
			NamespaceName: "validname",
			ProcessId:     "process1",
			TtlSeconds:    60,
		}))
	})

	t.Run("valid request with max ttl", func(t *testing.T) {
		require.NoError(t, ValidateCreateLockLeaseRequest(&gracklepb.CreateLockLeaseRequest{
			NamespaceName: "validname",
			ProcessId:     "process1",
			TtlSeconds:    300,
		}))
	})
}

func TestValidateRevokeLockLeaseRequest(t *testing.T) {
	t.Run("empty namespace name", func(t *testing.T) {
		require.Error(t, ValidateRevokeLockLeaseRequest(&gracklepb.RevokeLockLeaseRequest{
			NamespaceName: "",
			LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
		}))
	})

	t.Run("invalid namespace name characters", func(t *testing.T) {
		require.Error(t, ValidateRevokeLockLeaseRequest(&gracklepb.RevokeLockLeaseRequest{
			NamespaceName: "invalid name",
			LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
		}))
	})

	t.Run("namespace name too long", func(t *testing.T) {
		require.Error(t, ValidateRevokeLockLeaseRequest(&gracklepb.RevokeLockLeaseRequest{
			NamespaceName: string(make([]byte, 129)),
			LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
		}))
	})

	t.Run("empty lease id", func(t *testing.T) {
		require.Error(t, ValidateRevokeLockLeaseRequest(&gracklepb.RevokeLockLeaseRequest{
			NamespaceName: "validname",
			LeaseId:       "",
		}))
	})

	t.Run("invalid lease id format (wrong prefix)", func(t *testing.T) {
		require.Error(t, ValidateRevokeLockLeaseRequest(&gracklepb.RevokeLockLeaseRequest{
			NamespaceName: "validname",
			LeaseId:       "err_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
		}))
	})

	t.Run("invalid lease id format (too long)", func(t *testing.T) {
		require.Error(t, ValidateRevokeLockLeaseRequest(&gracklepb.RevokeLockLeaseRequest{
			NamespaceName: "validname",
			LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRBsd",
		}))
	})

	t.Run("invalid lease id format (invalid base62)", func(t *testing.T) {
		require.Error(t, ValidateRevokeLockLeaseRequest(&gracklepb.RevokeLockLeaseRequest{
			NamespaceName: "validname",
			LeaseId:       "ls_NfKKeiPbP18NFeU3lLGr+WWgDJRB",
		}))
	})

	t.Run("lease id too long", func(t *testing.T) {
		require.Error(t, ValidateRevokeLockLeaseRequest(&gracklepb.RevokeLockLeaseRequest{
			NamespaceName: "validname",
			LeaseId:       string(make([]byte, 65)),
		}))
	})

	t.Run("valid request", func(t *testing.T) {
		require.NoError(t, ValidateRevokeLockLeaseRequest(&gracklepb.RevokeLockLeaseRequest{
			NamespaceName: "validname",
			LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
		}))
	})
}

func TestValidateRefreshLockLeaseRequest(t *testing.T) {
	t.Run("empty namespace name", func(t *testing.T) {
		require.Error(t, ValidateRefreshLockLeaseRequest(&gracklepb.RefreshLockLeaseRequest{
			NamespaceName: "",
			LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
			TtlSeconds:    60,
		}))
	})

	t.Run("invalid namespace name characters", func(t *testing.T) {
		require.Error(t, ValidateRefreshLockLeaseRequest(&gracklepb.RefreshLockLeaseRequest{
			NamespaceName: "invalid name",
			LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
			TtlSeconds:    60,
		}))
	})

	t.Run("namespace name too long", func(t *testing.T) {
		require.Error(t, ValidateRefreshLockLeaseRequest(&gracklepb.RefreshLockLeaseRequest{
			NamespaceName: string(make([]byte, 129)),
			LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
			TtlSeconds:    60,
		}))
	})

	t.Run("empty lease id", func(t *testing.T) {
		require.Error(t, ValidateRefreshLockLeaseRequest(&gracklepb.RefreshLockLeaseRequest{
			NamespaceName: "validname",
			LeaseId:       "",
			TtlSeconds:    60,
		}))
	})

	t.Run("invalid lease id format", func(t *testing.T) {
		require.Error(t, ValidateRefreshLockLeaseRequest(&gracklepb.RefreshLockLeaseRequest{
			NamespaceName: "validname",
			LeaseId:       "err_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
			TtlSeconds:    60,
		}))
	})

	t.Run("lease id too long", func(t *testing.T) {
		require.Error(t, ValidateRefreshLockLeaseRequest(&gracklepb.RefreshLockLeaseRequest{
			NamespaceName: "validname",
			LeaseId:       string(make([]byte, 65)),
			TtlSeconds:    60,
		}))
	})

	t.Run("ttl seconds zero", func(t *testing.T) {
		require.Error(t, ValidateRefreshLockLeaseRequest(&gracklepb.RefreshLockLeaseRequest{
			NamespaceName: "validname",
			LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
			TtlSeconds:    0,
		}))
	})

	t.Run("ttl seconds too large", func(t *testing.T) {
		require.Error(t, ValidateRefreshLockLeaseRequest(&gracklepb.RefreshLockLeaseRequest{
			NamespaceName: "validname",
			LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
			TtlSeconds:    301,
		}))
	})

	t.Run("valid request", func(t *testing.T) {
		require.NoError(t, ValidateRefreshLockLeaseRequest(&gracklepb.RefreshLockLeaseRequest{
			NamespaceName: "validname",
			LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
			TtlSeconds:    60,
		}))
	})

	t.Run("valid request with max ttl", func(t *testing.T) {
		require.NoError(t, ValidateRefreshLockLeaseRequest(&gracklepb.RefreshLockLeaseRequest{
			NamespaceName: "validname",
			LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
			TtlSeconds:    300,
		}))
	})
}

func TestValidateListLockLeasesRequest(t *testing.T) {
	t.Run("empty namespace name", func(t *testing.T) {
		require.Error(t, ValidateListLockLeasesRequest(&gracklepb.ListLockLeasesRequest{
			NamespaceName: "",
		}))
	})

	t.Run("invalid namespace name characters", func(t *testing.T) {
		require.Error(t, ValidateListLockLeasesRequest(&gracklepb.ListLockLeasesRequest{
			NamespaceName: "invalid name",
		}))
	})

	t.Run("namespace name too long", func(t *testing.T) {
		require.Error(t, ValidateListLockLeasesRequest(&gracklepb.ListLockLeasesRequest{
			NamespaceName: string(make([]byte, 129)),
		}))
	})

	t.Run("invalid pagination token (not base64)", func(t *testing.T) {
		require.Error(t, ValidateListLockLeasesRequest(&gracklepb.ListLockLeasesRequest{
			NamespaceName:   "validname",
			PaginationToken: "invalid-base64!@#",
		}))
	})

	t.Run("pagination token too long", func(t *testing.T) {
		require.Error(t, ValidateListLockLeasesRequest(&gracklepb.ListLockLeasesRequest{
			NamespaceName:   "validname",
			PaginationToken: string(make([]byte, 1025)),
		}))
	})

	t.Run("limit too high", func(t *testing.T) {
		require.Error(t, ValidateListLockLeasesRequest(&gracklepb.ListLockLeasesRequest{
			NamespaceName: "validname",
			Limit:         101,
		}))
	})

	t.Run("negative limit", func(t *testing.T) {
		require.Error(t, ValidateListLockLeasesRequest(&gracklepb.ListLockLeasesRequest{
			NamespaceName: "validname",
			Limit:         -1,
		}))
	})

	t.Run("valid request", func(t *testing.T) {
		require.NoError(t, ValidateListLockLeasesRequest(&gracklepb.ListLockLeasesRequest{
			NamespaceName: "validname",
		}))
	})

	t.Run("valid request with pagination", func(t *testing.T) {
		require.NoError(t, ValidateListLockLeasesRequest(&gracklepb.ListLockLeasesRequest{
			NamespaceName:   "validname",
			PaginationToken: "dGVzdA==",
			Limit:           50,
		}))
	})
}

func TestValidateGetLockLeaseRequest(t *testing.T) {
	t.Run("empty namespace name", func(t *testing.T) {
		require.Error(t, ValidateGetLockLeaseRequest(&gracklepb.GetLockLeaseRequest{
			NamespaceName: "",
			LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
		}))
	})

	t.Run("invalid namespace name characters", func(t *testing.T) {
		require.Error(t, ValidateGetLockLeaseRequest(&gracklepb.GetLockLeaseRequest{
			NamespaceName: "invalid name",
			LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
		}))
	})

	t.Run("namespace name too long", func(t *testing.T) {
		require.Error(t, ValidateGetLockLeaseRequest(&gracklepb.GetLockLeaseRequest{
			NamespaceName: string(make([]byte, 129)),
			LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
		}))
	})

	t.Run("empty lease id", func(t *testing.T) {
		require.Error(t, ValidateGetLockLeaseRequest(&gracklepb.GetLockLeaseRequest{
			NamespaceName: "validname",
			LeaseId:       "",
		}))
	})

	t.Run("invalid lease id format", func(t *testing.T) {
		require.Error(t, ValidateGetLockLeaseRequest(&gracklepb.GetLockLeaseRequest{
			NamespaceName: "validname",
			LeaseId:       "err_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
		}))
	})

	t.Run("lease id too long", func(t *testing.T) {
		require.Error(t, ValidateGetLockLeaseRequest(&gracklepb.GetLockLeaseRequest{
			NamespaceName: "validname",
			LeaseId:       string(make([]byte, 65)),
		}))
	})

	t.Run("valid request", func(t *testing.T) {
		require.NoError(t, ValidateGetLockLeaseRequest(&gracklepb.GetLockLeaseRequest{
			NamespaceName: "validname",
			LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
		}))
	})
}
