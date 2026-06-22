package v1beta

import (
	"encoding/base64"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	gracklepb "github.com/evrblk/evrblk-go/grackle/v1beta"
)

func TestValidateMetadata(t *testing.T) {
	longKey := string(make([]byte, maxMetadataKeyLength+1))
	longValue := string(make([]byte, maxMetadataValueLength+1))

	tests := []struct {
		name        string
		metadata    map[string]string
		shouldError bool
	}{
		{name: "nil metadata", metadata: nil, shouldError: false},
		{name: "empty metadata", metadata: map[string]string{}, shouldError: false},
		{name: "valid metadata", metadata: map[string]string{"team": "search", "env": "prod"}, shouldError: false},
		{name: "valid empty value", metadata: map[string]string{"key": ""}, shouldError: false},
		{name: "too many entries", metadata: func() map[string]string {
			m := make(map[string]string, maxMetadataEntries+1)
			for i := 0; i <= maxMetadataEntries; i++ {
				m[fmt.Sprintf("key-%d", i)] = "v"
			}
			return m
		}(), shouldError: true},
		{name: "empty key", metadata: map[string]string{"": "value"}, shouldError: true},
		{name: "key too long", metadata: map[string]string{longKey: "value"}, shouldError: true},
		{name: "value too long", metadata: map[string]string{"key": longValue}, shouldError: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateMetadata(test.metadata, "Metadata")
			if test.shouldError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestValidateMetadataThroughRequests verifies that metadata limits are
// enforced by the request-level validators that accept metadata, including the
// per-job metadata on CompleteJobsFromWaitGroupRequest.
func TestValidateMetadataThroughRequests(t *testing.T) {
	tooMany := make(map[string]string, maxMetadataEntries+1)
	for i := 0; i <= maxMetadataEntries; i++ {
		tooMany[fmt.Sprintf("key-%d", i)] = "v"
	}

	t.Run("create namespace rejects oversized metadata", func(t *testing.T) {
		require.Error(t, ValidateCreateNamespaceRequest(&gracklepb.CreateNamespaceRequest{
			Name:     "validname",
			Metadata: tooMany,
		}))
		require.NoError(t, ValidateCreateNamespaceRequest(&gracklepb.CreateNamespaceRequest{
			Name:     "validname",
			Metadata: map[string]string{"team": "search"},
		}))
	})

	t.Run("complete jobs rejects oversized job metadata", func(t *testing.T) {
		require.Error(t, ValidateCompleteJobsFromWaitGroupRequest(&gracklepb.CompleteJobsFromWaitGroupRequest{
			NamespaceName: "validname",
			WaitGroupName: "validwaitgroup",
			Jobs: []*gracklepb.CompleteJobRequest{
				{JobId: "job1", Metadata: tooMany},
			},
		}))
		require.NoError(t, ValidateCompleteJobsFromWaitGroupRequest(&gracklepb.CompleteJobsFromWaitGroupRequest{
			NamespaceName: "validname",
			WaitGroupName: "validwaitgroup",
			Jobs: []*gracklepb.CompleteJobRequest{
				{JobId: "job1", Metadata: map[string]string{"worker": "w1"}},
			},
		}))
	})
}

func TestValidateCreateNamespaceRequest(t *testing.T) {
	tests := []struct {
		name        string
		request     *gracklepb.CreateNamespaceRequest
		shouldError bool
	}{
		{
			name:        "empty request",
			request:     &gracklepb.CreateNamespaceRequest{},
			shouldError: true,
		},
		{
			name: "empty name",
			request: &gracklepb.CreateNamespaceRequest{
				Name: "",
			},
			shouldError: true,
		},
		{
			name: "name too long",
			request: &gracklepb.CreateNamespaceRequest{
				Name: string(make([]byte, 129)),
			},
			shouldError: true,
		},
		{
			name: "invalid name characters",
			request: &gracklepb.CreateNamespaceRequest{
				Name: "invalid name",
			},
			shouldError: true,
		},
		{
			name: "description too long",
			request: &gracklepb.CreateNamespaceRequest{
				Name:        "validname",
				Description: string(make([]byte, 1025)),
			},
			shouldError: true,
		},
		{
			name: "valid request",
			request: &gracklepb.CreateNamespaceRequest{
				Name:        "validname",
				Description: "Valid description",
			},
			shouldError: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.shouldError {
				require.Error(t, ValidateCreateNamespaceRequest(test.request))
			} else {
				require.NoError(t, ValidateCreateNamespaceRequest(test.request))
			}
		})
	}
}

func TestValidateGetNamespaceRequest(t *testing.T) {
	tests := []struct {
		name        string
		request     *gracklepb.GetNamespaceRequest
		shouldError bool
	}{
		{
			name:        "empty request",
			request:     &gracklepb.GetNamespaceRequest{},
			shouldError: true,
		},
		{
			name: "empty namespace name",
			request: &gracklepb.GetNamespaceRequest{
				NamespaceName: "",
			},
			shouldError: true,
		},
		{
			name: "namespace name too long",
			request: &gracklepb.GetNamespaceRequest{
				NamespaceName: string(make([]byte, 129)),
			},
			shouldError: true,
		},
		{
			name: "invalid namespace name characters",
			request: &gracklepb.GetNamespaceRequest{
				NamespaceName: "invalid name",
			},
			shouldError: true,
		},
		{
			name: "valid request",
			request: &gracklepb.GetNamespaceRequest{
				NamespaceName: "validname",
			},
			shouldError: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.shouldError {
				require.Error(t, ValidateGetNamespaceRequest(test.request))
			} else {
				require.NoError(t, ValidateGetNamespaceRequest(test.request))
			}
		})
	}
}

func TestValidateUpdateNamespaceRequest(t *testing.T) {
	tests := []struct {
		name        string
		request     *gracklepb.UpdateNamespaceRequest
		shouldError bool
	}{
		{
			name:        "empty request",
			request:     &gracklepb.UpdateNamespaceRequest{},
			shouldError: true,
		},
		{
			name: "empty namespace name",
			request: &gracklepb.UpdateNamespaceRequest{
				NamespaceName: "",
			},
			shouldError: true,
		},
		{
			name: "namespace name too long",
			request: &gracklepb.UpdateNamespaceRequest{
				NamespaceName: string(make([]byte, 129)),
			},
			shouldError: true,
		},
		{
			name: "invalid namespace name characters",
			request: &gracklepb.UpdateNamespaceRequest{
				NamespaceName: "invalid name",
			},
			shouldError: true,
		},
		{
			name: "description too long",
			request: &gracklepb.UpdateNamespaceRequest{
				NamespaceName: "validname",
				Description:   string(make([]byte, 1025)),
			},
			shouldError: true,
		},
		{
			name: "valid request",
			request: &gracklepb.UpdateNamespaceRequest{
				NamespaceName: "validname",
				Description:   "Valid description",
			},
			shouldError: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.shouldError {
				require.Error(t, ValidateUpdateNamespaceRequest(test.request))
			} else {
				require.NoError(t, ValidateUpdateNamespaceRequest(test.request))
			}
		})
	}
}

func TestValidateDeleteNamespaceRequest(t *testing.T) {
	tests := []struct {
		name        string
		request     *gracklepb.DeleteNamespaceRequest
		shouldError bool
	}{
		{
			name:        "empty request",
			request:     &gracklepb.DeleteNamespaceRequest{},
			shouldError: true,
		},
		{
			name: "namespace name too long",
			request: &gracklepb.DeleteNamespaceRequest{
				NamespaceName: string(make([]byte, 129)),
			},
			shouldError: true,
		},
		{
			name: "invalid namespace name characters",
			request: &gracklepb.DeleteNamespaceRequest{
				NamespaceName: "invalid name",
			},
			shouldError: true,
		},
		{
			name: "valid request",
			request: &gracklepb.DeleteNamespaceRequest{
				NamespaceName: "validname",
			},
			shouldError: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.shouldError {
				require.Error(t, ValidateDeleteNamespaceRequest(test.request))
			} else {
				require.NoError(t, ValidateDeleteNamespaceRequest(test.request))
			}
		})
	}
}

func TestValidateListNamespacesRequest(t *testing.T) {
	tests := []struct {
		name        string
		request     *gracklepb.ListNamespacesRequest
		shouldError bool
	}{
		{
			name:        "empty request",
			request:     &gracklepb.ListNamespacesRequest{},
			shouldError: false,
		},
		{
			name: "valid request with pagination token",
			request: &gracklepb.ListNamespacesRequest{
				PaginationToken: "dGVzdA==",
				Limit:           0,
			},
			shouldError: false,
		},
		{
			name: "valid request with both pagination token and limit",
			request: &gracklepb.ListNamespacesRequest{
				PaginationToken: "dGVzdA==",
				Limit:           100,
			},
			shouldError: false,
		},
		{
			name: "pagination token too long",
			request: &gracklepb.ListNamespacesRequest{
				PaginationToken: string(make([]byte, 1025)),
				Limit:           50,
			},
			shouldError: true,
		},
		{
			name: "limit too high",
			request: &gracklepb.ListNamespacesRequest{
				PaginationToken: "",
				Limit:           251,
			},
			shouldError: true,
		},
		{
			name: "negative limit",
			request: &gracklepb.ListNamespacesRequest{
				PaginationToken: "",
				Limit:           -1,
			},
			shouldError: true,
		},
		{
			name: "edge case: limit at maximum",
			request: &gracklepb.ListNamespacesRequest{
				PaginationToken: "",
				Limit:           250,
			},
			shouldError: false,
		},
		{
			name: "edge case: pagination token at maximum length",
			request: &gracklepb.ListNamespacesRequest{
				PaginationToken: string(make([]byte, 1024)),
				Limit:           50,
			},
			shouldError: true,
		},
		{
			name: "valid request",
			request: &gracklepb.ListNamespacesRequest{
				PaginationToken: "",
				Limit:           50,
			},
			shouldError: false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.shouldError {
				require.Error(t, ValidateListNamespacesRequest(test.request))
			} else {
				require.NoError(t, ValidateListNamespacesRequest(test.request))
			}
		})
	}
}

func TestValidateCreateWaitGroupRequest(t *testing.T) {
	tests := []struct {
		name        string
		request     *gracklepb.CreateWaitGroupRequest
		shouldError bool
	}{
		{
			name:        "empty request",
			request:     &gracklepb.CreateWaitGroupRequest{},
			shouldError: true,
		},
		{
			name: "empty namespace name",
			request: &gracklepb.CreateWaitGroupRequest{
				WaitGroupName: "validname",
				Counter:       1,
			},
			shouldError: true,
		},
		{
			name: "empty wait group name",
			request: &gracklepb.CreateWaitGroupRequest{
				NamespaceName: "validname",
				Counter:       1,
			},
			shouldError: true,
		},
		{
			name: "namespace name too long",
			request: &gracklepb.CreateWaitGroupRequest{
				NamespaceName: string(make([]byte, 129)),
				WaitGroupName: "validname",
				Counter:       1,
			},
			shouldError: true,
		},
		{
			name: "wait group name too long",
			request: &gracklepb.CreateWaitGroupRequest{
				NamespaceName: "validname",
				WaitGroupName: string(make([]byte, 129)),
				Counter:       1,
			},
			shouldError: true,
		},
		{
			name: "invalid namespace name characters",
			request: &gracklepb.CreateWaitGroupRequest{
				NamespaceName: "invalid name",
				WaitGroupName: "validname",
				Counter:       1,
			},
			shouldError: true,
		},
		{
			name: "invalid wait group name characters",
			request: &gracklepb.CreateWaitGroupRequest{
				NamespaceName: "validname",
				WaitGroupName: "invalid name",
				Counter:       1,
			},
			shouldError: true,
		},
		{
			name: "counter must be greater than 0",
			request: &gracklepb.CreateWaitGroupRequest{
				NamespaceName: "validname",
				WaitGroupName: "validwaitgroup",
				Counter:       0,
			},
			shouldError: true,
		},
		{
			name: "valid request",
			request: &gracklepb.CreateWaitGroupRequest{
				NamespaceName: "validname",
				WaitGroupName: "validwaitgroup",
				Counter:       1,
			},
			shouldError: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.shouldError {
				require.Error(t, ValidateCreateWaitGroupRequest(test.request))
			} else {
				require.NoError(t, ValidateCreateWaitGroupRequest(test.request))
			}
		})
	}
}

func TestValidateGetWaitGroupRequest(t *testing.T) {
	tests := []struct {
		name        string
		request     *gracklepb.GetWaitGroupRequest
		shouldError bool
	}{
		{
			name:        "empty request",
			request:     &gracklepb.GetWaitGroupRequest{},
			shouldError: true,
		},
		{
			name: "empty namespace name",
			request: &gracklepb.GetWaitGroupRequest{
				WaitGroupName: "validname",
			},
			shouldError: true,
		},
		{
			name: "empty wait group name",
			request: &gracklepb.GetWaitGroupRequest{
				NamespaceName: "validname",
			},
			shouldError: true,
		},
		{
			name: "namespace name too long",
			request: &gracklepb.GetWaitGroupRequest{
				NamespaceName: string(make([]byte, 129)),
				WaitGroupName: "validname",
			},
			shouldError: true,
		},
		{
			name: "wait group name too long",
			request: &gracklepb.GetWaitGroupRequest{
				NamespaceName: "validname",
				WaitGroupName: string(make([]byte, 129)),
			},
			shouldError: true,
		},
		{
			name: "invalid namespace name characters",
			request: &gracklepb.GetWaitGroupRequest{
				NamespaceName: "invalid name",
				WaitGroupName: "validname",
			},
			shouldError: true,
		},
		{
			name: "invalid wait group name characters",
			request: &gracklepb.GetWaitGroupRequest{
				NamespaceName: "validname",
				WaitGroupName: "invalid name",
			},
			shouldError: true,
		},
		{
			name: "valid request",
			request: &gracklepb.GetWaitGroupRequest{
				NamespaceName: "validname",
				WaitGroupName: "validwaitgroup",
			},
			shouldError: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.shouldError {
				require.Error(t, ValidateGetWaitGroupRequest(test.request))
			} else {
				require.NoError(t, ValidateGetWaitGroupRequest(test.request))
			}
		})
	}
}

func TestValidateDeleteWaitGroupRequest(t *testing.T) {
	tests := []struct {
		name        string
		request     *gracklepb.DeleteWaitGroupRequest
		shouldError bool
	}{
		{
			name:        "empty request",
			request:     &gracklepb.DeleteWaitGroupRequest{},
			shouldError: true,
		},
		{
			name: "empty namespace name",
			request: &gracklepb.DeleteWaitGroupRequest{
				WaitGroupName: "validname",
			},
			shouldError: true,
		},
		{
			name: "empty wait group name",
			request: &gracklepb.DeleteWaitGroupRequest{
				NamespaceName: "validname",
			},
			shouldError: true,
		},
		{
			name: "namespace name too long",
			request: &gracklepb.DeleteWaitGroupRequest{
				NamespaceName: string(make([]byte, 129)),
				WaitGroupName: "validname",
			},
			shouldError: true,
		},
		{
			name: "wait group name too long",
			request: &gracklepb.DeleteWaitGroupRequest{
				NamespaceName: "validname",
				WaitGroupName: string(make([]byte, 129)),
			},
			shouldError: true,
		},
		{
			name: "invalid namespace name characters",
			request: &gracklepb.DeleteWaitGroupRequest{
				NamespaceName: "invalid name",
				WaitGroupName: "validname",
			},
			shouldError: true,
		},
		{
			name: "invalid wait group name characters",
			request: &gracklepb.DeleteWaitGroupRequest{
				NamespaceName: "validname",
				WaitGroupName: "invalid name",
			},
			shouldError: true,
		},
		{
			name: "valid request",
			request: &gracklepb.DeleteWaitGroupRequest{
				NamespaceName: "validname",
				WaitGroupName: "validwaitgroup",
			},
			shouldError: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.shouldError {
				require.Error(t, ValidateDeleteWaitGroupRequest(test.request))
			} else {
				require.NoError(t, ValidateDeleteWaitGroupRequest(test.request))
			}
		})
	}
}

func TestValidateWaitForWaitGroupRequest(t *testing.T) {
	tests := []struct {
		name        string
		request     *gracklepb.WaitForWaitGroupRequest
		shouldError bool
	}{
		{
			name:        "empty request",
			request:     &gracklepb.WaitForWaitGroupRequest{},
			shouldError: true,
		},
		{
			name: "missing namespace name",
			request: &gracklepb.WaitForWaitGroupRequest{
				WaitGroupName:  "validname",
				TimeoutSeconds: 10,
			},
			shouldError: true,
		},
		{
			name: "missing wait group name",
			request: &gracklepb.WaitForWaitGroupRequest{
				NamespaceName:  "validname",
				TimeoutSeconds: 10,
			},
			shouldError: true,
		},
		{
			name: "namespace name too long",
			request: &gracklepb.WaitForWaitGroupRequest{
				NamespaceName:  string(make([]byte, 129)),
				WaitGroupName:  "validname",
				TimeoutSeconds: 10,
			},
			shouldError: true,
		},
		{
			name: "wait group name too long",
			request: &gracklepb.WaitForWaitGroupRequest{
				NamespaceName:  "validname",
				WaitGroupName:  string(make([]byte, 129)),
				TimeoutSeconds: 10,
			},
			shouldError: true,
		},
		{
			name: "invalid namespace name characters",
			request: &gracklepb.WaitForWaitGroupRequest{
				NamespaceName:  "invalid name",
				WaitGroupName:  "validname",
				TimeoutSeconds: 10,
			},
			shouldError: true,
		},
		{
			name: "invalid wait group name characters",
			request: &gracklepb.WaitForWaitGroupRequest{
				NamespaceName:  "validname",
				WaitGroupName:  "invalid name",
				TimeoutSeconds: 10,
			},
			shouldError: true,
		},
		{
			name: "timeout seconds zero",
			request: &gracklepb.WaitForWaitGroupRequest{
				NamespaceName:  "validname",
				WaitGroupName:  "validname",
				TimeoutSeconds: 0,
			},
			shouldError: true,
		},
		{
			name: "timeout seconds negative",
			request: &gracklepb.WaitForWaitGroupRequest{
				NamespaceName:  "validname",
				WaitGroupName:  "validname",
				TimeoutSeconds: -1,
			},
			shouldError: true,
		},
		{
			name: "timeout seconds too high",
			request: &gracklepb.WaitForWaitGroupRequest{
				NamespaceName:  "validname",
				WaitGroupName:  "validname",
				TimeoutSeconds: 301,
			},
			shouldError: true,
		},
		{
			name: "valid request",
			request: &gracklepb.WaitForWaitGroupRequest{
				NamespaceName:  "validname",
				WaitGroupName:  "validname",
				TimeoutSeconds: 10,
			},
			shouldError: false,
		},
		{
			name: "valid request with maximum timeout",
			request: &gracklepb.WaitForWaitGroupRequest{
				NamespaceName:  "validname",
				WaitGroupName:  "validname",
				TimeoutSeconds: 300,
			},
			shouldError: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.shouldError {
				require.Error(t, ValidateWaitForWaitGroupRequest(test.request))
			} else {
				require.NoError(t, ValidateWaitForWaitGroupRequest(test.request))
			}
		})
	}
}

func TestValidateListWaitGroupsRequest(t *testing.T) {
	tests := []struct {
		name        string
		request     *gracklepb.ListWaitGroupsRequest
		shouldError bool
	}{
		{
			name:        "empty request - should fail due to missing namespace name",
			request:     &gracklepb.ListWaitGroupsRequest{},
			shouldError: true,
		},
		{
			name: "empty namespace name",
			request: &gracklepb.ListWaitGroupsRequest{
				NamespaceName: "",
			},
			shouldError: true,
		},
		{
			name: "namespace name too long",
			request: &gracklepb.ListWaitGroupsRequest{
				NamespaceName: string(make([]byte, 129)),
			},
			shouldError: true,
		},
		{
			name: "invalid namespace name characters",
			request: &gracklepb.ListWaitGroupsRequest{
				NamespaceName: "invalid name",
			},
			shouldError: true,
		},
		{
			name: "valid request with no pagination",
			request: &gracklepb.ListWaitGroupsRequest{
				NamespaceName:   "validname",
				PaginationToken: "",
				Limit:           0,
			},
			shouldError: false,
		},
		{
			name: "valid request with pagination token",
			request: &gracklepb.ListWaitGroupsRequest{
				NamespaceName:   "validname",
				PaginationToken: "dGVzdA==",
				Limit:           0,
			},
			shouldError: false,
		},
		{
			name: "valid request with limit",
			request: &gracklepb.ListWaitGroupsRequest{
				NamespaceName:   "validname",
				PaginationToken: "",
				Limit:           50,
			},
			shouldError: false,
		},
		{
			name: "valid request with both pagination token and limit",
			request: &gracklepb.ListWaitGroupsRequest{
				NamespaceName:   "validname",
				PaginationToken: "dGVzdA==",
				Limit:           100,
			},
			shouldError: false,
		},
		{
			name: "invalid pagination token (not base64)",
			request: &gracklepb.ListWaitGroupsRequest{
				NamespaceName:   "validname",
				PaginationToken: "invalid-base64!@#",
				Limit:           50,
			},
			shouldError: true,
		},
		{
			name: "pagination token too long",
			request: &gracklepb.ListWaitGroupsRequest{
				NamespaceName:   "validname",
				PaginationToken: string(make([]byte, 1025)),
				Limit:           50,
			},
			shouldError: true,
		},
		{
			name: "limit too high",
			request: &gracklepb.ListWaitGroupsRequest{
				NamespaceName:   "validname",
				PaginationToken: "",
				Limit:           251,
			},
			shouldError: true,
		},
		{
			name: "negative limit",
			request: &gracklepb.ListWaitGroupsRequest{
				NamespaceName:   "validname",
				PaginationToken: "",
				Limit:           -1,
			},
			shouldError: true,
		},
		{
			name: "edge case: limit at maximum",
			request: &gracklepb.ListWaitGroupsRequest{
				NamespaceName:   "validname",
				PaginationToken: "",
				Limit:           250,
			},
			shouldError: false,
		},
		{
			name: "valid base64 token at maximum length",
			request: &gracklepb.ListWaitGroupsRequest{
				NamespaceName:   "validname",
				PaginationToken: base64.StdEncoding.EncodeToString(make([]byte, 768)),
				Limit:           50,
			},
			shouldError: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.shouldError {
				require.Error(t, ValidateListWaitGroupsRequest(test.request))
			} else {
				require.NoError(t, ValidateListWaitGroupsRequest(test.request))
			}
		})
	}
}

func TestValidateCompleteJobsFromWaitGroupRequest(t *testing.T) {
	tests := []struct {
		name        string
		request     *gracklepb.CompleteJobsFromWaitGroupRequest
		shouldError bool
	}{
		{
			name:        "empty request",
			request:     &gracklepb.CompleteJobsFromWaitGroupRequest{},
			shouldError: true,
		},
		{
			name: "empty namespace name",
			request: &gracklepb.CompleteJobsFromWaitGroupRequest{
				WaitGroupName: "validname",
				Jobs:          []*gracklepb.CompleteJobRequest{{JobId: "job1"}},
			},
			shouldError: true,
		},
		{
			name: "empty wait group name",
			request: &gracklepb.CompleteJobsFromWaitGroupRequest{
				NamespaceName: "validname",
				Jobs:          []*gracklepb.CompleteJobRequest{{JobId: "job1"}},
			},
			shouldError: true,
		},
		{
			name: "namespace name too long",
			request: &gracklepb.CompleteJobsFromWaitGroupRequest{
				NamespaceName: string(make([]byte, 129)),
				WaitGroupName: "validname",
				Jobs:          []*gracklepb.CompleteJobRequest{{JobId: "job1"}},
			},
			shouldError: true,
		},
		{
			name: "wait group name too long",
			request: &gracklepb.CompleteJobsFromWaitGroupRequest{
				NamespaceName: "validname",
				WaitGroupName: string(make([]byte, 129)),
				Jobs:          []*gracklepb.CompleteJobRequest{{JobId: "job1"}},
			},
			shouldError: true,
		},
		{
			name: "invalid namespace name characters",
			request: &gracklepb.CompleteJobsFromWaitGroupRequest{
				NamespaceName: "invalid name",
				WaitGroupName: "validname",
				Jobs:          []*gracklepb.CompleteJobRequest{{JobId: "job1"}},
			},
			shouldError: true,
		},
		{
			name: "invalid wait group name characters",
			request: &gracklepb.CompleteJobsFromWaitGroupRequest{
				NamespaceName: "validname",
				WaitGroupName: "invalid name",
				Jobs:          []*gracklepb.CompleteJobRequest{{JobId: "job1"}},
			},
			shouldError: true,
		},
		{
			name: "too many process ids",
			request: &gracklepb.CompleteJobsFromWaitGroupRequest{
				NamespaceName: "validname",
				WaitGroupName: "validwaitgroup",
				Jobs:          make([]*gracklepb.CompleteJobRequest, 51),
			},
			shouldError: true,
		},
		{
			name: "invalid process id",
			request: &gracklepb.CompleteJobsFromWaitGroupRequest{
				NamespaceName: "validname",
				WaitGroupName: "validwaitgroup",
				Jobs:          []*gracklepb.CompleteJobRequest{{JobId: "invalid job id"}},
			},
			shouldError: true,
		},
		{
			name: "valid request",
			request: &gracklepb.CompleteJobsFromWaitGroupRequest{
				NamespaceName: "validname",
				WaitGroupName: "validwaitgroup",
				Jobs:          []*gracklepb.CompleteJobRequest{{JobId: "job1"}, {JobId: "job2"}},
			},
			shouldError: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.shouldError {
				require.Error(t, ValidateCompleteJobsFromWaitGroupRequest(test.request))
			} else {
				require.NoError(t, ValidateCompleteJobsFromWaitGroupRequest(test.request))
			}
		})
	}
}

func TestValidateDeleteLockRequest(t *testing.T) {
	tests := []struct {
		name        string
		request     *gracklepb.DeleteLockRequest
		shouldError bool
	}{
		{
			name:        "empty request",
			request:     &gracklepb.DeleteLockRequest{},
			shouldError: true,
		},
		{
			name: "empty namespace name",
			request: &gracklepb.DeleteLockRequest{
				LockName: "validname",
			},
			shouldError: true,
		},
		{
			name: "empty lock name",
			request: &gracklepb.DeleteLockRequest{
				NamespaceName: "validname",
			},
			shouldError: true,
		},
		{
			name: "namespace name too long",
			request: &gracklepb.DeleteLockRequest{
				NamespaceName: string(make([]byte, 129)),
				LockName:      "validname",
			},
			shouldError: true,
		},
		{
			name: "lock name too long",
			request: &gracklepb.DeleteLockRequest{
				NamespaceName: "validname",
				LockName:      string(make([]byte, 129)),
			},
			shouldError: true,
		},
		{
			name: "invalid namespace name characters",
			request: &gracklepb.DeleteLockRequest{
				NamespaceName: "invalid name",
				LockName:      "validname",
			},
			shouldError: true,
		},
		{
			name: "invalid lock name characters",
			request: &gracklepb.DeleteLockRequest{
				NamespaceName: "validname",
				LockName:      "invalid name",
			},
			shouldError: true,
		},
		{
			name: "valid request",
			request: &gracklepb.DeleteLockRequest{
				NamespaceName: "validname",
				LockName:      "validlock",
			},
			shouldError: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.shouldError {
				require.Error(t, ValidateDeleteLockRequest(test.request))
			} else {
				require.NoError(t, ValidateDeleteLockRequest(test.request))
			}
		})
	}
}

func TestValidateGetLockRequest(t *testing.T) {
	tests := []struct {
		name        string
		request     *gracklepb.GetLockRequest
		shouldError bool
	}{
		{
			name:        "empty request",
			request:     &gracklepb.GetLockRequest{},
			shouldError: true,
		},
		{
			name: "empty namespace name",
			request: &gracklepb.GetLockRequest{
				LockName: "validname",
			},
			shouldError: true,
		},
		{
			name: "empty lock name",
			request: &gracklepb.GetLockRequest{
				NamespaceName: "validname",
			},
			shouldError: true,
		},
		{
			name: "namespace name too long",
			request: &gracklepb.GetLockRequest{
				NamespaceName: string(make([]byte, 129)),
				LockName:      "validname",
			},
			shouldError: true,
		},
		{
			name: "lock name too long",
			request: &gracklepb.GetLockRequest{
				NamespaceName: "validname",
				LockName:      string(make([]byte, 129)),
			},
			shouldError: true,
		},
		{
			name: "invalid namespace name characters",
			request: &gracklepb.GetLockRequest{
				NamespaceName: "invalid name",
				LockName:      "validname",
			},
			shouldError: true,
		},
		{
			name: "invalid lock name characters",
			request: &gracklepb.GetLockRequest{
				NamespaceName: "validname",
				LockName:      "invalid name",
			},
			shouldError: true,
		},
		{
			name: "valid request",
			request: &gracklepb.GetLockRequest{
				NamespaceName: "validname",
				LockName:      "validlock",
			},
			shouldError: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.shouldError {
				require.Error(t, ValidateGetLockRequest(test.request))
			} else {
				require.NoError(t, ValidateGetLockRequest(test.request))
			}
		})
	}
}

func TestValidateReleaseLockRequest(t *testing.T) {
	tests := []struct {
		name        string
		request     *gracklepb.ReleaseLockRequest
		shouldError bool
	}{
		{
			name:        "empty request",
			request:     &gracklepb.ReleaseLockRequest{},
			shouldError: true,
		},
		{
			name: "empty namespace name",
			request: &gracklepb.ReleaseLockRequest{
				LockName: "validname",
				LeaseId:  "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
			},
			shouldError: true,
		},
		{
			name: "empty lock name",
			request: &gracklepb.ReleaseLockRequest{
				NamespaceName: "validname",
				LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
			},
			shouldError: true,
		},
		{
			name: "empty lease id",
			request: &gracklepb.ReleaseLockRequest{
				NamespaceName: "validname",
				LockName:      "validname",
			},
			shouldError: true,
		},
		{
			name: "namespace name too long",
			request: &gracklepb.ReleaseLockRequest{
				NamespaceName: string(make([]byte, 129)),
				LockName:      "validname",
				LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
			},
			shouldError: true,
		},
		{
			name: "lock name too long",
			request: &gracklepb.ReleaseLockRequest{
				NamespaceName: "validname",
				LockName:      string(make([]byte, 129)),
				LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
			},
			shouldError: true,
		},
		{
			name: "lease id too long",
			request: &gracklepb.ReleaseLockRequest{
				NamespaceName: "validname",
				LockName:      "validname",
				LeaseId:       string(make([]byte, 65)),
			},
			shouldError: true,
		},
		{
			name: "invalid namespace name characters",
			request: &gracklepb.ReleaseLockRequest{
				NamespaceName: "invalid name",
				LockName:      "validname",
				LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
			},
			shouldError: true,
		},
		{
			name: "invalid lock name characters",
			request: &gracklepb.ReleaseLockRequest{
				NamespaceName: "validname",
				LockName:      "invalid name",
				LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
			},
			shouldError: true,
		},
		{
			name: "invalid lease id",
			request: &gracklepb.ReleaseLockRequest{
				NamespaceName: "validname",
				LockName:      "validname",
				LeaseId:       "invalid lease id",
			},
			shouldError: true,
		},
		{
			name: "valid request",
			request: &gracklepb.ReleaseLockRequest{
				NamespaceName: "validname",
				LockName:      "validlock",
				LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
			},
			shouldError: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.shouldError {
				require.Error(t, ValidateReleaseLockRequest(test.request))
			} else {
				require.NoError(t, ValidateReleaseLockRequest(test.request))
			}
		})
	}
}

func TestValidateAcquireLockRequest(t *testing.T) {
	tests := []struct {
		name        string
		request     *gracklepb.AcquireLockRequest
		shouldError bool
	}{
		{
			name:        "empty request",
			request:     &gracklepb.AcquireLockRequest{},
			shouldError: true,
		},
		{
			name: "empty namespace name",
			request: &gracklepb.AcquireLockRequest{
				LockName: "validname",
				LeaseId:  "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
			},
			shouldError: true,
		},
		{
			name: "empty lock name",
			request: &gracklepb.AcquireLockRequest{
				NamespaceName: "validname",
				LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
			},
			shouldError: true,
		},
		{
			name: "empty lease id",
			request: &gracklepb.AcquireLockRequest{
				NamespaceName: "validname",
				LockName:      "validname",
			},
			shouldError: true,
		},
		{
			name: "namespace name too long",
			request: &gracklepb.AcquireLockRequest{
				NamespaceName: string(make([]byte, 129)),
				LockName:      "validname",
				LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
			},
			shouldError: true,
		},
		{
			name: "lock name too long",
			request: &gracklepb.AcquireLockRequest{
				NamespaceName: "validname",
				LockName:      string(make([]byte, 129)),
				LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
			},
			shouldError: true,
		},
		{
			name: "lease id too long",
			request: &gracklepb.AcquireLockRequest{
				NamespaceName: "validname",
				LockName:      "validname",
				LeaseId:       string(make([]byte, 65)),
			},
			shouldError: true,
		},
		{
			name: "invalid namespace name characters",
			request: &gracklepb.AcquireLockRequest{
				NamespaceName: "invalid name",
				LockName:      "validname",
				LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
			},
			shouldError: true,
		},
		{
			name: "invalid lock name characters",
			request: &gracklepb.AcquireLockRequest{
				NamespaceName: "validname",
				LockName:      "invalid name",
				LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
			},
			shouldError: true,
		},
		{
			name: "invalid lease id",
			request: &gracklepb.AcquireLockRequest{
				NamespaceName: "validname",
				LockName:      "validname",
				LeaseId:       "invalid lease id",
			},
			shouldError: true,
		},
		{
			name: "valid request",
			request: &gracklepb.AcquireLockRequest{
				NamespaceName: "validname",
				LockName:      "validlock",
				LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
			},
			shouldError: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.shouldError {
				require.Error(t, ValidateAcquireLockRequest(test.request))
			} else {
				require.NoError(t, ValidateAcquireLockRequest(test.request))
			}
		})
	}
}

func TestValidateCreateSemaphoreRequest(t *testing.T) {
	tests := []struct {
		name        string
		request     *gracklepb.CreateSemaphoreRequest
		shouldError bool
	}{
		{
			name:        "empty request",
			request:     &gracklepb.CreateSemaphoreRequest{},
			shouldError: true,
		},
		{
			name: "empty namespace name",
			request: &gracklepb.CreateSemaphoreRequest{
				SemaphoreName: "validname",
				Description:   "validdescription",
				Permits:       1,
			},
			shouldError: true,
		},
		{
			name: "empty semaphore name",
			request: &gracklepb.CreateSemaphoreRequest{
				NamespaceName: "validname",
				Description:   "validdescription",
				Permits:       1,
			},
			shouldError: true,
		},
		{
			name: "namespace name too long",
			request: &gracklepb.CreateSemaphoreRequest{
				NamespaceName: string(make([]byte, 129)),
				SemaphoreName: "validname",
				Description:   "validdescription",
				Permits:       1,
			},
			shouldError: true,
		},
		{
			name: "semaphore name too long",
			request: &gracklepb.CreateSemaphoreRequest{
				NamespaceName: "validname",
				SemaphoreName: string(make([]byte, 129)),
				Description:   "validdescription",
				Permits:       1,
			},
			shouldError: true,
		},
		{
			name: "invalid namespace name characters",
			request: &gracklepb.CreateSemaphoreRequest{
				NamespaceName: "invalid name",
				SemaphoreName: "validname",
				Description:   "validdescription",
				Permits:       1,
			},
			shouldError: true,
		},
		{
			name: "invalid semaphore name characters",
			request: &gracklepb.CreateSemaphoreRequest{
				NamespaceName: "validname",
				SemaphoreName: "invalid name",
				Description:   "validdescription",
				Permits:       1,
			},
			shouldError: true,
		},
		{
			name: "permits must be greater than 0",
			request: &gracklepb.CreateSemaphoreRequest{
				NamespaceName: "validname",
				SemaphoreName: "validsemaphore",
				Description:   "validdescription",
				Permits:       0,
			},
			shouldError: true,
		},
		{
			name: "valid request",
			request: &gracklepb.CreateSemaphoreRequest{
				NamespaceName: "validname",
				SemaphoreName: "validsemaphore",
				Description:   "validdescription",
				Permits:       1,
			},
			shouldError: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.shouldError {
				require.Error(t, ValidateCreateSemaphoreRequest(test.request))
			} else {
				require.NoError(t, ValidateCreateSemaphoreRequest(test.request))
			}
		})
	}
}

func TestValidateGetSemaphoreRequest(t *testing.T) {
	tests := []struct {
		name        string
		request     *gracklepb.GetSemaphoreRequest
		shouldError bool
	}{
		{
			name:        "empty request",
			request:     &gracklepb.GetSemaphoreRequest{},
			shouldError: true,
		},
		{
			name: "empty namespace name",
			request: &gracklepb.GetSemaphoreRequest{
				SemaphoreName: "validname",
			},
			shouldError: true,
		},
		{
			name: "empty semaphore name",
			request: &gracklepb.GetSemaphoreRequest{
				NamespaceName: "validname",
			},
			shouldError: true,
		},
		{
			name: "namespace name too long",
			request: &gracklepb.GetSemaphoreRequest{
				NamespaceName: string(make([]byte, 129)),
				SemaphoreName: "validname",
			},
			shouldError: true,
		},
		{
			name: "semaphore name too long",
			request: &gracklepb.GetSemaphoreRequest{
				NamespaceName: "validname",
				SemaphoreName: string(make([]byte, 129)),
			},
			shouldError: true,
		},
		{
			name: "invalid namespace name characters",
			request: &gracklepb.GetSemaphoreRequest{
				NamespaceName: "invalid name",
				SemaphoreName: "validname",
			},
			shouldError: true,
		},
		{
			name: "invalid semaphore name characters",
			request: &gracklepb.GetSemaphoreRequest{
				NamespaceName: "validname",
				SemaphoreName: "invalid name",
			},
			shouldError: true,
		},
		{
			name: "valid request",
			request: &gracklepb.GetSemaphoreRequest{
				NamespaceName: "validname",
				SemaphoreName: "validsemaphore",
			},
			shouldError: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.shouldError {
				require.Error(t, ValidateGetSemaphoreRequest(test.request))
			} else {
				require.NoError(t, ValidateGetSemaphoreRequest(test.request))
			}
		})
	}
}

func TestValidateReleaseSemaphoreRequest(t *testing.T) {
	tests := []struct {
		name        string
		request     *gracklepb.ReleaseSemaphoreRequest
		shouldError bool
	}{
		{
			name:        "empty request",
			request:     &gracklepb.ReleaseSemaphoreRequest{},
			shouldError: true,
		},
		{
			name: "empty namespace name",
			request: &gracklepb.ReleaseSemaphoreRequest{
				SemaphoreName: "validname",
				LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
			},
			shouldError: true,
		},
		{
			name: "empty semaphore name",
			request: &gracklepb.ReleaseSemaphoreRequest{
				NamespaceName: "validname",
				LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
			},
			shouldError: true,
		},
		{
			name: "empty process id",
			request: &gracklepb.ReleaseSemaphoreRequest{
				NamespaceName: "validname",
				SemaphoreName: "validname",
			},
			shouldError: true,
		},
		{
			name: "namespace name too long",
			request: &gracklepb.ReleaseSemaphoreRequest{
				NamespaceName: string(make([]byte, 129)),
				SemaphoreName: "validname",
				LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
			},
			shouldError: true,
		},
		{
			name: "semaphore name too long",
			request: &gracklepb.ReleaseSemaphoreRequest{
				NamespaceName: "validname",
				SemaphoreName: string(make([]byte, 129)),
				LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
			},
			shouldError: true,
		},
		{
			name: "lease id too long",
			request: &gracklepb.ReleaseSemaphoreRequest{
				NamespaceName: "validname",
				SemaphoreName: "validname",
				LeaseId:       string(make([]byte, 65)),
			},
			shouldError: true,
		},
		{
			name: "invalid namespace name characters",
			request: &gracklepb.ReleaseSemaphoreRequest{
				NamespaceName: "invalid name",
				SemaphoreName: "validname",
				LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
			},
			shouldError: true,
		},
		{
			name: "invalid semaphore name characters",
			request: &gracklepb.ReleaseSemaphoreRequest{
				NamespaceName: "validname",
				SemaphoreName: "invalid name",
				LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
			},
			shouldError: true,
		},
		{
			name: "invalid lease id",
			request: &gracklepb.ReleaseSemaphoreRequest{
				NamespaceName: "validname",
				SemaphoreName: "validname",
				LeaseId:       "invalid lease id",
			},
			shouldError: true,
		},
		{
			name: "valid request",
			request: &gracklepb.ReleaseSemaphoreRequest{
				NamespaceName: "validname",
				SemaphoreName: "validsemaphore",
				LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
			},
			shouldError: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.shouldError {
				require.Error(t, ValidateReleaseSemaphoreRequest(test.request))
			} else {
				require.NoError(t, ValidateReleaseSemaphoreRequest(test.request))
			}
		})
	}
}

func TestValidateUpdateSemaphoreRequest(t *testing.T) {
	tests := []struct {
		name        string
		request     *gracklepb.UpdateSemaphoreRequest
		shouldError bool
	}{
		{
			name:        "empty request",
			request:     &gracklepb.UpdateSemaphoreRequest{},
			shouldError: true,
		},
		{
			name: "empty namespace name",
			request: &gracklepb.UpdateSemaphoreRequest{
				SemaphoreName: "validname",
				Description:   "validdescription",
				Permits:       1,
			},
			shouldError: true,
		},
		{
			name: "empty semaphore name",
			request: &gracklepb.UpdateSemaphoreRequest{
				NamespaceName: "validname",
				Description:   "validdescription",
				Permits:       1,
			},
			shouldError: true,
		},
		{
			name: "namespace name too long",
			request: &gracklepb.UpdateSemaphoreRequest{
				NamespaceName: string(make([]byte, 129)),
				SemaphoreName: "validname",
				Description:   "validdescription",
				Permits:       1,
			},
			shouldError: true,
		},
		{
			name: "semaphore name too long",
			request: &gracklepb.UpdateSemaphoreRequest{
				NamespaceName: "validname",
				SemaphoreName: string(make([]byte, 129)),
				Description:   "validdescription",
				Permits:       1,
			},
			shouldError: true,
		},
		{
			name: "invalid namespace name characters",
			request: &gracklepb.UpdateSemaphoreRequest{
				NamespaceName: "invalid name",
				SemaphoreName: "validname",
				Description:   "validdescription",
				Permits:       1,
			},
			shouldError: true,
		},
		{
			name: "invalid semaphore name characters",
			request: &gracklepb.UpdateSemaphoreRequest{
				NamespaceName: "validname",
				SemaphoreName: "invalid name",
				Description:   "validdescription",
				Permits:       1,
			},
			shouldError: true,
		},
		{
			name: "permits must be greater than 0",
			request: &gracklepb.UpdateSemaphoreRequest{
				NamespaceName: "validname",
				SemaphoreName: "validsemaphore",
				Description:   "validdescription",
				Permits:       0,
			},
			shouldError: true,
		},
		{
			name: "valid request",
			request: &gracklepb.UpdateSemaphoreRequest{
				NamespaceName: "validname",
				SemaphoreName: "validsemaphore",
				Description:   "validdescription",
				Permits:       1,
			},
			shouldError: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.shouldError {
				require.Error(t, ValidateUpdateSemaphoreRequest(test.request))
			} else {
				require.NoError(t, ValidateUpdateSemaphoreRequest(test.request))
			}
		})
	}
}

func TestValidateDeleteSemaphoreRequest(t *testing.T) {
	tests := []struct {
		name        string
		request     *gracklepb.DeleteSemaphoreRequest
		shouldError bool
	}{
		{
			name:        "empty request",
			request:     &gracklepb.DeleteSemaphoreRequest{},
			shouldError: true,
		},
		{
			name: "empty namespace name",
			request: &gracklepb.DeleteSemaphoreRequest{
				SemaphoreName: "validname",
			},
			shouldError: true,
		},
		{
			name: "empty semaphore name",
			request: &gracklepb.DeleteSemaphoreRequest{
				NamespaceName: "validname",
			},
			shouldError: true,
		},
		{
			name: "namespace name too long",
			request: &gracklepb.DeleteSemaphoreRequest{
				NamespaceName: string(make([]byte, 129)),
				SemaphoreName: "validname",
			},
			shouldError: true,
		},
		{
			name: "semaphore name too long",
			request: &gracklepb.DeleteSemaphoreRequest{
				NamespaceName: "validname",
				SemaphoreName: string(make([]byte, 129)),
			},
			shouldError: true,
		},
		{
			name: "invalid namespace name characters",
			request: &gracklepb.DeleteSemaphoreRequest{
				NamespaceName: "invalid name",
				SemaphoreName: "validname",
			},
			shouldError: true,
		},
		{
			name: "invalid semaphore name characters",
			request: &gracklepb.DeleteSemaphoreRequest{
				NamespaceName: "validname",
				SemaphoreName: "invalid name",
			},
			shouldError: true,
		},
		{
			name: "valid request",
			request: &gracklepb.DeleteSemaphoreRequest{
				NamespaceName: "validname",
				SemaphoreName: "validsemaphore",
			},
			shouldError: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.shouldError {
				require.Error(t, ValidateDeleteSemaphoreRequest(test.request))
			} else {
				require.NoError(t, ValidateDeleteSemaphoreRequest(test.request))
			}
		})
	}
}

func TestValidateListLocksRequest(t *testing.T) {
	tests := []struct {
		name        string
		request     *gracklepb.ListLocksRequest
		shouldError bool
	}{
		{
			name:        "empty request - should fail due to missing namespace name",
			request:     &gracklepb.ListLocksRequest{},
			shouldError: true,
		},
		{
			name: "empty namespace name",
			request: &gracklepb.ListLocksRequest{
				NamespaceName: "",
			},
			shouldError: true,
		},
		{
			name: "namespace name too long",
			request: &gracklepb.ListLocksRequest{
				NamespaceName: string(make([]byte, 129)),
			},
			shouldError: true,
		},
		{
			name: "invalid namespace name characters",
			request: &gracklepb.ListLocksRequest{
				NamespaceName: "invalid name",
			},
			shouldError: true,
		},
		{
			name: "valid request with no pagination",
			request: &gracklepb.ListLocksRequest{
				NamespaceName:   "validname",
				PaginationToken: "",
				Limit:           0,
			},
			shouldError: false,
		},
		{
			name: "valid request with pagination token",
			request: &gracklepb.ListLocksRequest{
				NamespaceName:   "validname",
				PaginationToken: "dGVzdA==",
				Limit:           0,
			},
			shouldError: false,
		},
		{
			name: "valid request with limit",
			request: &gracklepb.ListLocksRequest{
				NamespaceName:   "validname",
				PaginationToken: "",
				Limit:           50,
			},
			shouldError: false,
		},
		{
			name: "valid request with both pagination token and limit",
			request: &gracklepb.ListLocksRequest{
				NamespaceName:   "validname",
				PaginationToken: "dGVzdA==",
				Limit:           100,
			},
			shouldError: false,
		},
		{
			name: "invalid pagination token (not base64)",
			request: &gracklepb.ListLocksRequest{
				NamespaceName:   "validname",
				PaginationToken: "invalid-base64!@#",
				Limit:           50,
			},
			shouldError: true,
		},
		{
			name: "pagination token too long",
			request: &gracklepb.ListLocksRequest{
				NamespaceName:   "validname",
				PaginationToken: string(make([]byte, 1025)),
				Limit:           50,
			},
			shouldError: true,
		},
		{
			name: "limit too high",
			request: &gracklepb.ListLocksRequest{
				NamespaceName:   "validname",
				PaginationToken: "",
				Limit:           251,
			},
			shouldError: true,
		},
		{
			name: "negative limit",
			request: &gracklepb.ListLocksRequest{
				NamespaceName:   "validname",
				PaginationToken: "",
				Limit:           -1,
			},
			shouldError: true,
		},
		{
			name: "edge case: limit at maximum",
			request: &gracklepb.ListLocksRequest{
				NamespaceName:   "validname",
				PaginationToken: "",
				Limit:           250,
			},
			shouldError: false,
		},
		{
			name: "valid base64 token at maximum length",
			request: &gracklepb.ListLocksRequest{
				NamespaceName:   "validname",
				PaginationToken: base64.StdEncoding.EncodeToString(make([]byte, 768)),
				Limit:           50,
			},
			shouldError: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.shouldError {
				require.Error(t, ValidateListLocksRequest(test.request))
			} else {
				require.NoError(t, ValidateListLocksRequest(test.request))
			}
		})
	}
}

func TestValidateListSemaphoresRequest(t *testing.T) {
	tests := []struct {
		name        string
		request     *gracklepb.ListSemaphoresRequest
		shouldError bool
	}{
		{
			name:        "empty request - should fail due to missing namespace name",
			request:     &gracklepb.ListSemaphoresRequest{},
			shouldError: true,
		},
		{
			name: "empty namespace name",
			request: &gracklepb.ListSemaphoresRequest{
				NamespaceName: "",
			},
			shouldError: true,
		},
		{
			name: "namespace name too long",
			request: &gracklepb.ListSemaphoresRequest{
				NamespaceName: string(make([]byte, 129)),
			},
			shouldError: true,
		},
		{
			name: "invalid namespace name characters",
			request: &gracklepb.ListSemaphoresRequest{
				NamespaceName: "invalid name",
			},
			shouldError: true,
		},
		{
			name: "valid request with no pagination",
			request: &gracklepb.ListSemaphoresRequest{
				NamespaceName:   "validname",
				PaginationToken: "",
				Limit:           0,
			},
			shouldError: false,
		},
		{
			name: "valid request with pagination token",
			request: &gracklepb.ListSemaphoresRequest{
				NamespaceName:   "validname",
				PaginationToken: "dGVzdA==",
				Limit:           0,
			},
			shouldError: false,
		},
		{
			name: "valid request with limit",
			request: &gracklepb.ListSemaphoresRequest{
				NamespaceName:   "validname",
				PaginationToken: "",
				Limit:           50,
			},
			shouldError: false,
		},
		{
			name: "valid request with both pagination token and limit",
			request: &gracklepb.ListSemaphoresRequest{
				NamespaceName:   "validname",
				PaginationToken: "dGVzdA==",
				Limit:           100,
			},
			shouldError: false,
		},
		{
			name: "invalid pagination token (not base64)",
			request: &gracklepb.ListSemaphoresRequest{
				NamespaceName:   "validname",
				PaginationToken: "invalid-base64!@#",
				Limit:           50,
			},
			shouldError: true,
		},
		{
			name: "pagination token too long",
			request: &gracklepb.ListSemaphoresRequest{
				NamespaceName:   "validname",
				PaginationToken: string(make([]byte, 1025)),
				Limit:           50,
			},
			shouldError: true,
		},
		{
			name: "limit too high",
			request: &gracklepb.ListSemaphoresRequest{
				NamespaceName:   "validname",
				PaginationToken: "",
				Limit:           251,
			},
			shouldError: true,
		},
		{
			name: "negative limit",
			request: &gracklepb.ListSemaphoresRequest{
				NamespaceName:   "validname",
				PaginationToken: "",
				Limit:           -1,
			},
			shouldError: true,
		},
		{
			name: "edge case: limit at maximum",
			request: &gracklepb.ListSemaphoresRequest{
				NamespaceName:   "validname",
				PaginationToken: "",
				Limit:           250,
			},
			shouldError: false,
		},
		{
			name: "valid base64 token at maximum length",
			request: &gracklepb.ListSemaphoresRequest{
				NamespaceName:   "validname",
				PaginationToken: base64.StdEncoding.EncodeToString(make([]byte, 768)),
				Limit:           50,
			},
			shouldError: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.shouldError {
				require.Error(t, ValidateListSemaphoresRequest(test.request))
			} else {
				require.NoError(t, ValidateListSemaphoresRequest(test.request))
			}
		})
	}
}

func TestValidateAcquireSemaphoreRequest(t *testing.T) {
	tests := []struct {
		name        string
		request     *gracklepb.AcquireSemaphoreRequest
		shouldError bool
	}{
		{
			name:        "empty request",
			request:     &gracklepb.AcquireSemaphoreRequest{},
			shouldError: true,
		},
		{
			name: "empty namespace name",
			request: &gracklepb.AcquireSemaphoreRequest{
				SemaphoreName:  "validname",
				LeaseId:        "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
				TimeoutSeconds: 10,
				Weight:         1,
			},
			shouldError: true,
		},
		{
			name: "empty semaphore name",
			request: &gracklepb.AcquireSemaphoreRequest{
				NamespaceName:  "validname",
				LeaseId:        "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
				TimeoutSeconds: 10,
				Weight:         1,
			},
			shouldError: true,
		},
		{
			name: "empty process id",
			request: &gracklepb.AcquireSemaphoreRequest{
				NamespaceName:  "validname",
				SemaphoreName:  "validname",
				TimeoutSeconds: 10,
				Weight:         1,
			},
			shouldError: true,
		},
		{
			name: "namespace name too long",
			request: &gracklepb.AcquireSemaphoreRequest{
				NamespaceName:  string(make([]byte, 129)),
				SemaphoreName:  "validname",
				LeaseId:        "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
				TimeoutSeconds: 10,
				Weight:         1,
			},
			shouldError: true,
		},
		{
			name: "semaphore name too long",
			request: &gracklepb.AcquireSemaphoreRequest{
				NamespaceName:  "validname",
				SemaphoreName:  string(make([]byte, 129)),
				LeaseId:        "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
				TimeoutSeconds: 10,
				Weight:         1,
			},
			shouldError: true,
		},
		{
			name: "lease id too long",
			request: &gracklepb.AcquireSemaphoreRequest{
				NamespaceName:  "validname",
				SemaphoreName:  "validname",
				LeaseId:        string(make([]byte, 65)),
				TimeoutSeconds: 10,
				Weight:         1,
			},
			shouldError: true,
		},
		{
			name: "invalid namespace name characters",
			request: &gracklepb.AcquireSemaphoreRequest{
				NamespaceName:  "invalid name",
				SemaphoreName:  "validname",
				LeaseId:        "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
				TimeoutSeconds: 10,
				Weight:         1,
			},
			shouldError: true,
		},
		{
			name: "invalid semaphore name characters",
			request: &gracklepb.AcquireSemaphoreRequest{
				NamespaceName:  "validname",
				SemaphoreName:  "invalid name",
				LeaseId:        "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
				TimeoutSeconds: 10,
				Weight:         1,
			},
			shouldError: true,
		},
		{
			name: "invalid lease id",
			request: &gracklepb.AcquireSemaphoreRequest{
				NamespaceName:  "validname",
				SemaphoreName:  "validname",
				LeaseId:        "invalid lease id",
				TimeoutSeconds: 10,
				Weight:         1,
			},
			shouldError: true,
		},
		{
			name: "timeout seconds zero",
			request: &gracklepb.AcquireSemaphoreRequest{
				NamespaceName:  "validname",
				SemaphoreName:  "validname",
				LeaseId:        "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
				TimeoutSeconds: 0,
				Weight:         1,
			},
			shouldError: true,
		},
		{
			name: "timeout seconds negative",
			request: &gracklepb.AcquireSemaphoreRequest{
				NamespaceName:  "validname",
				SemaphoreName:  "validname",
				LeaseId:        "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
				TimeoutSeconds: -1,
				Weight:         1,
			},
			shouldError: true,
		},
		{
			name: "timeout seconds too high",
			request: &gracklepb.AcquireSemaphoreRequest{
				NamespaceName:  "validname",
				SemaphoreName:  "validname",
				LeaseId:        "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
				TimeoutSeconds: 301,
				Weight:         1,
			},
			shouldError: true,
		},
		{
			name: "zero weight",
			request: &gracklepb.AcquireSemaphoreRequest{
				NamespaceName:  "validname",
				SemaphoreName:  "validname",
				LeaseId:        "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
				TimeoutSeconds: 10,
				Weight:         0,
			},
			shouldError: true,
		},
		{
			name: "valid request",
			request: &gracklepb.AcquireSemaphoreRequest{
				NamespaceName:  "validname",
				SemaphoreName:  "validname",
				LeaseId:        "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
				TimeoutSeconds: 10,
				Weight:         1,
			},
			shouldError: false,
		},
		{
			name: "valid request with maximum timeout",
			request: &gracklepb.AcquireSemaphoreRequest{
				NamespaceName:  "validname",
				SemaphoreName:  "validname",
				LeaseId:        "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
				TimeoutSeconds: 300,
				Weight:         1,
			},
			shouldError: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.shouldError {
				require.Error(t, ValidateAcquireSemaphoreRequest(test.request))
			} else {
				require.NoError(t, ValidateAcquireSemaphoreRequest(test.request))
			}
		})
	}
}

func TestValidateListWaitGroupCompletedJobsRequest(t *testing.T) {
	tests := []struct {
		name        string
		request     *gracklepb.ListWaitGroupCompletedJobsRequest
		shouldError bool
	}{
		{
			name:        "empty request - should fail due to missing namespace name",
			request:     &gracklepb.ListWaitGroupCompletedJobsRequest{},
			shouldError: true,
		},
		{
			name: "missing namespace name",
			request: &gracklepb.ListWaitGroupCompletedJobsRequest{
				WaitGroupName: "validname",
			},
			shouldError: true,
		},
		{
			name: "missing wait group name",
			request: &gracklepb.ListWaitGroupCompletedJobsRequest{
				NamespaceName: "validname",
			},
			shouldError: true,
		},
		{
			name: "invalid namespace name characters",
			request: &gracklepb.ListWaitGroupCompletedJobsRequest{
				NamespaceName: "invalid name",
				WaitGroupName: "validname",
			},
			shouldError: true,
		},
		{
			name: "invalid wait group name characters",
			request: &gracklepb.ListWaitGroupCompletedJobsRequest{
				NamespaceName: "validname",
				WaitGroupName: "invalid name",
			},
			shouldError: true,
		},
		{
			name: "namespace name too long",
			request: &gracklepb.ListWaitGroupCompletedJobsRequest{
				NamespaceName: string(make([]byte, 129)),
				WaitGroupName: "validname",
			},
			shouldError: true,
		},
		{
			name: "wait group name too long",
			request: &gracklepb.ListWaitGroupCompletedJobsRequest{
				NamespaceName: "validname",
				WaitGroupName: string(make([]byte, 129)),
			},
			shouldError: true,
		},
		{
			name: "invalid pagination token (not base64)",
			request: &gracklepb.ListWaitGroupCompletedJobsRequest{
				NamespaceName:   "validname",
				WaitGroupName:   "validname",
				PaginationToken: "invalid-base64!@#",
			},
			shouldError: true,
		},
		{
			name: "pagination token too long",
			request: &gracklepb.ListWaitGroupCompletedJobsRequest{
				NamespaceName:   "validname",
				WaitGroupName:   "validname",
				PaginationToken: string(make([]byte, 1025)),
			},
			shouldError: true,
		},
		{
			name: "limit too high",
			request: &gracklepb.ListWaitGroupCompletedJobsRequest{
				NamespaceName: "validname",
				WaitGroupName: "validname",
				Limit:         251,
			},
			shouldError: true,
		},
		{
			name: "negative limit",
			request: &gracklepb.ListWaitGroupCompletedJobsRequest{
				NamespaceName: "validname",
				WaitGroupName: "validname",
				Limit:         -1,
			},
			shouldError: true,
		},
		{
			name: "valid request",
			request: &gracklepb.ListWaitGroupCompletedJobsRequest{
				NamespaceName: "validname",
				WaitGroupName: "validname",
			},
			shouldError: false,
		},
		{
			name: "valid request with pagination",
			request: &gracklepb.ListWaitGroupCompletedJobsRequest{
				NamespaceName:   "validname",
				WaitGroupName:   "validname",
				PaginationToken: "dGVzdA==",
				Limit:           50,
			},
			shouldError: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.shouldError {
				require.Error(t, ValidateListWaitGroupCompletedJobsRequest(test.request))
			} else {
				require.NoError(t, ValidateListWaitGroupCompletedJobsRequest(test.request))
			}
		})
	}
}

func TestValidateListSemaphoreHoldersRequest(t *testing.T) {
	tests := []struct {
		name        string
		request     *gracklepb.ListSemaphoreHoldersRequest
		shouldError bool
	}{
		{
			name:        "empty request - should fail due to missing namespace name",
			request:     &gracklepb.ListSemaphoreHoldersRequest{},
			shouldError: true,
		},
		{
			name: "missing namespace name",
			request: &gracklepb.ListSemaphoreHoldersRequest{
				SemaphoreName: "validname",
			},
			shouldError: true,
		},
		{
			name: "missing semaphore name",
			request: &gracklepb.ListSemaphoreHoldersRequest{
				NamespaceName: "validname",
			},
			shouldError: true,
		},
		{
			name: "invalid namespace name characters",
			request: &gracklepb.ListSemaphoreHoldersRequest{
				NamespaceName: "invalid name",
				SemaphoreName: "validname",
			},
			shouldError: true,
		},
		{
			name: "invalid semaphore name characters",
			request: &gracklepb.ListSemaphoreHoldersRequest{
				NamespaceName: "validname",
				SemaphoreName: "invalid name",
			},
			shouldError: true,
		},
		{
			name: "namespace name too long",
			request: &gracklepb.ListSemaphoreHoldersRequest{
				NamespaceName: string(make([]byte, 129)),
				SemaphoreName: "validname",
			},
			shouldError: true,
		},
		{
			name: "semaphore name too long",
			request: &gracklepb.ListSemaphoreHoldersRequest{
				NamespaceName: "validname",
				SemaphoreName: string(make([]byte, 129)),
			},
			shouldError: true,
		},
		{
			name: "invalid pagination token (not base64)",
			request: &gracklepb.ListSemaphoreHoldersRequest{
				NamespaceName:   "validname",
				SemaphoreName:   "validname",
				PaginationToken: "invalid-base64!@#",
			},
			shouldError: true,
		},
		{
			name: "pagination token too long",
			request: &gracklepb.ListSemaphoreHoldersRequest{
				NamespaceName:   "validname",
				SemaphoreName:   "validname",
				PaginationToken: string(make([]byte, 1025)),
			},
			shouldError: true,
		},
		{
			name: "limit too high",
			request: &gracklepb.ListSemaphoreHoldersRequest{
				NamespaceName: "validname",
				SemaphoreName: "validname",
				Limit:         251,
			},
			shouldError: true,
		},
		{
			name: "negative limit",
			request: &gracklepb.ListSemaphoreHoldersRequest{
				NamespaceName: "validname",
				SemaphoreName: "validname",
				Limit:         -1,
			},
			shouldError: true,
		},
		{
			name: "valid request",
			request: &gracklepb.ListSemaphoreHoldersRequest{
				NamespaceName: "validname",
				SemaphoreName: "validname",
			},
			shouldError: false,
		},
		{
			name: "valid request with pagination",
			request: &gracklepb.ListSemaphoreHoldersRequest{
				NamespaceName:   "validname",
				SemaphoreName:   "validname",
				PaginationToken: "dGVzdA==",
				Limit:           50,
			},
			shouldError: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.shouldError {
				require.Error(t, ValidateListSemaphoreHoldersRequest(test.request))
			} else {
				require.NoError(t, ValidateListSemaphoreHoldersRequest(test.request))
			}
		})
	}
}

func TestValidateCreateBarrierRequest(t *testing.T) {
	tests := []struct {
		name        string
		request     *gracklepb.CreateBarrierRequest
		shouldError bool
	}{
		{
			name:        "empty request",
			request:     &gracklepb.CreateBarrierRequest{},
			shouldError: true,
		},
		{
			name: "missing namespace name",
			request: &gracklepb.CreateBarrierRequest{
				BarrierName:       "validname",
				ExpectedProcesses: 3,
				ExpiresAt:         time.Now().UnixNano(),
			},
			shouldError: true,
		},
		{
			name: "missing barrier name",
			request: &gracklepb.CreateBarrierRequest{
				NamespaceName:     "validname",
				ExpectedProcesses: 3,
				ExpiresAt:         time.Now().UnixNano(),
			},
			shouldError: true,
		},
		{
			name: "invalid namespace name characters",
			request: &gracklepb.CreateBarrierRequest{
				NamespaceName:     "invalid name",
				BarrierName:       "validname",
				ExpectedProcesses: 3,
				ExpiresAt:         time.Now().UnixNano(),
			},
			shouldError: true,
		},
		{
			name: "invalid barrier name characters",
			request: &gracklepb.CreateBarrierRequest{
				NamespaceName:     "validname",
				BarrierName:       "invalid name",
				ExpectedProcesses: 3,
				ExpiresAt:         time.Now().UnixNano(),
			},
			shouldError: true,
		},
		{
			name: "namespace name too long",
			request: &gracklepb.CreateBarrierRequest{
				NamespaceName:     string(make([]byte, 129)),
				BarrierName:       "validname",
				ExpectedProcesses: 3,
				ExpiresAt:         time.Now().UnixNano(),
			},
			shouldError: true,
		},
		{
			name: "barrier name too long",
			request: &gracklepb.CreateBarrierRequest{
				NamespaceName:     "validname",
				BarrierName:       string(make([]byte, 129)),
				ExpectedProcesses: 3,
				ExpiresAt:         time.Now().UnixNano(),
			},
			shouldError: true,
		},
		{
			name: "description too long",
			request: &gracklepb.CreateBarrierRequest{
				NamespaceName:     "validname",
				BarrierName:       "validname",
				Description:       string(make([]byte, 1025)),
				ExpectedProcesses: 3,
				ExpiresAt:         time.Now().UnixNano(),
			},
			shouldError: true,
		},
		{
			name: "expected processes zero",
			request: &gracklepb.CreateBarrierRequest{
				NamespaceName:     "validname",
				BarrierName:       "validname",
				ExpectedProcesses: 0,
				ExpiresAt:         time.Now().UnixNano(),
			},
			shouldError: true,
		},
		{
			name: "expires at zero",
			request: &gracklepb.CreateBarrierRequest{
				NamespaceName:     "validname",
				BarrierName:       "validname",
				ExpectedProcesses: 3,
				ExpiresAt:         0,
			},
			shouldError: true,
		},
		{
			name: "expires at negative",
			request: &gracklepb.CreateBarrierRequest{
				NamespaceName:     "validname",
				BarrierName:       "validname",
				ExpectedProcesses: 3,
				ExpiresAt:         -1,
			},
			shouldError: true,
		},
		{
			name: "valid request",
			request: &gracklepb.CreateBarrierRequest{
				NamespaceName:     "validname",
				BarrierName:       "validname",
				ExpectedProcesses: 3,
				ExpiresAt:         time.Now().UnixNano(),
			},
			shouldError: false,
		},
		{
			name: "valid request with description",
			request: &gracklepb.CreateBarrierRequest{
				NamespaceName:     "validname",
				BarrierName:       "validname",
				Description:       "Valid description",
				ExpectedProcesses: 3,
				ExpiresAt:         time.Now().UnixNano(),
			},
			shouldError: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.shouldError {
				require.Error(t, ValidateCreateBarrierRequest(test.request))
			} else {
				require.NoError(t, ValidateCreateBarrierRequest(test.request))
			}
		})
	}
}

func TestValidateListBarriersRequest(t *testing.T) {
	tests := []struct {
		name        string
		request     *gracklepb.ListBarriersRequest
		shouldError bool
	}{
		{
			name:        "empty request - should fail due to missing namespace name",
			request:     &gracklepb.ListBarriersRequest{},
			shouldError: true,
		},
		{
			name: "missing namespace name",
			request: &gracklepb.ListBarriersRequest{
				PaginationToken: "",
				Limit:           0,
			},
			shouldError: true,
		},
		{
			name: "invalid namespace name characters",
			request: &gracklepb.ListBarriersRequest{
				NamespaceName: "invalid name",
			},
			shouldError: true,
		},
		{
			name: "namespace name too long",
			request: &gracklepb.ListBarriersRequest{
				NamespaceName: string(make([]byte, 129)),
			},
			shouldError: true,
		},
		{
			name: "invalid pagination token (not base64)",
			request: &gracklepb.ListBarriersRequest{
				NamespaceName:   "validname",
				PaginationToken: "invalid-base64!@#",
			},
			shouldError: true,
		},
		{
			name: "pagination token too long",
			request: &gracklepb.ListBarriersRequest{
				NamespaceName:   "validname",
				PaginationToken: string(make([]byte, 1025)),
			},
			shouldError: true,
		},
		{
			name: "limit too high",
			request: &gracklepb.ListBarriersRequest{
				NamespaceName: "validname",
				Limit:         251,
			},
			shouldError: true,
		},
		{
			name: "negative limit",
			request: &gracklepb.ListBarriersRequest{
				NamespaceName: "validname",
				Limit:         -1,
			},
			shouldError: true,
		},
		{
			name: "valid request",
			request: &gracklepb.ListBarriersRequest{
				NamespaceName: "validname",
			},
			shouldError: false,
		},
		{
			name: "valid request with pagination",
			request: &gracklepb.ListBarriersRequest{
				NamespaceName:   "validname",
				PaginationToken: "dGVzdA==",
				Limit:           50,
			},
			shouldError: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.shouldError {
				require.Error(t, ValidateListBarriersRequest(test.request))
			} else {
				require.NoError(t, ValidateListBarriersRequest(test.request))
			}
		})
	}
}

func TestValidateGetBarrierRequest(t *testing.T) {
	tests := []struct {
		name        string
		request     *gracklepb.GetBarrierRequest
		shouldError bool
	}{
		{
			name:        "empty request",
			request:     &gracklepb.GetBarrierRequest{},
			shouldError: true,
		},
		{
			name: "missing namespace name",
			request: &gracklepb.GetBarrierRequest{
				BarrierName: "validname",
			},
			shouldError: true,
		},
		{
			name: "missing barrier name",
			request: &gracklepb.GetBarrierRequest{
				NamespaceName: "validname",
			},
			shouldError: true,
		},
		{
			name: "invalid namespace name characters",
			request: &gracklepb.GetBarrierRequest{
				NamespaceName: "invalid name",
				BarrierName:   "validname",
			},
			shouldError: true,
		},
		{
			name: "invalid barrier name characters",
			request: &gracklepb.GetBarrierRequest{
				NamespaceName: "validname",
				BarrierName:   "invalid name",
			},
			shouldError: true,
		},
		{
			name: "namespace name too long",
			request: &gracklepb.GetBarrierRequest{
				NamespaceName: string(make([]byte, 129)),
				BarrierName:   "validname",
			},
			shouldError: true,
		},
		{
			name: "barrier name too long",
			request: &gracklepb.GetBarrierRequest{
				NamespaceName: "validname",
				BarrierName:   string(make([]byte, 129)),
			},
			shouldError: true,
		},
		{
			name: "valid request",
			request: &gracklepb.GetBarrierRequest{
				NamespaceName: "validname",
				BarrierName:   "validname",
			},
			shouldError: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.shouldError {
				require.Error(t, ValidateGetBarrierRequest(test.request))
			} else {
				require.NoError(t, ValidateGetBarrierRequest(test.request))
			}
		})
	}
}

func TestValidateDeleteBarrierRequest(t *testing.T) {
	tests := []struct {
		name        string
		request     *gracklepb.DeleteBarrierRequest
		shouldError bool
	}{
		{
			name:        "empty request",
			request:     &gracklepb.DeleteBarrierRequest{},
			shouldError: true,
		},
		{
			name: "missing namespace name",
			request: &gracklepb.DeleteBarrierRequest{
				BarrierName: "validname",
			},
			shouldError: true,
		},
		{
			name: "missing barrier name",
			request: &gracklepb.DeleteBarrierRequest{
				NamespaceName: "validname",
			},
			shouldError: true,
		},
		{
			name: "invalid namespace name characters",
			request: &gracklepb.DeleteBarrierRequest{
				NamespaceName: "invalid name",
				BarrierName:   "validname",
			},
			shouldError: true,
		},
		{
			name: "invalid barrier name characters",
			request: &gracklepb.DeleteBarrierRequest{
				NamespaceName: "validname",
				BarrierName:   "invalid name",
			},
			shouldError: true,
		},
		{
			name: "namespace name too long",
			request: &gracklepb.DeleteBarrierRequest{
				NamespaceName: string(make([]byte, 129)),
				BarrierName:   "validname",
			},
			shouldError: true,
		},
		{
			name: "barrier name too long",
			request: &gracklepb.DeleteBarrierRequest{
				NamespaceName: "validname",
				BarrierName:   string(make([]byte, 129)),
			},
			shouldError: true,
		},
		{
			name: "valid request",
			request: &gracklepb.DeleteBarrierRequest{
				NamespaceName: "validname",
				BarrierName:   "validname",
			},
			shouldError: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.shouldError {
				require.Error(t, ValidateDeleteBarrierRequest(test.request))
			} else {
				require.NoError(t, ValidateDeleteBarrierRequest(test.request))
			}
		})
	}
}

func TestValidateUpdateBarrierRequest(t *testing.T) {
	tests := []struct {
		name        string
		request     *gracklepb.UpdateBarrierRequest
		shouldError bool
	}{
		{
			name:        "empty request",
			request:     &gracklepb.UpdateBarrierRequest{},
			shouldError: true,
		},
		{
			name: "missing namespace name",
			request: &gracklepb.UpdateBarrierRequest{
				BarrierName:       "validname",
				Description:       "desc",
				ExpectedProcesses: 3,
			},
			shouldError: true,
		},
		{
			name: "missing barrier name",
			request: &gracklepb.UpdateBarrierRequest{
				NamespaceName:     "validname",
				Description:       "desc",
				ExpectedProcesses: 3,
			},
			shouldError: true,
		},
		{
			name: "invalid namespace name characters",
			request: &gracklepb.UpdateBarrierRequest{
				NamespaceName:     "invalid name",
				BarrierName:       "validname",
				Description:       "desc",
				ExpectedProcesses: 3,
			},
			shouldError: true,
		},
		{
			name: "invalid barrier name characters",
			request: &gracklepb.UpdateBarrierRequest{
				NamespaceName:     "validname",
				BarrierName:       "invalid name",
				Description:       "desc",
				ExpectedProcesses: 3,
			},
			shouldError: true,
		},
		{
			name: "namespace name too long",
			request: &gracklepb.UpdateBarrierRequest{
				NamespaceName:     string(make([]byte, 129)),
				BarrierName:       "validname",
				Description:       "desc",
				ExpectedProcesses: 3,
			},
			shouldError: true,
		},
		{
			name: "barrier name too long",
			request: &gracklepb.UpdateBarrierRequest{
				NamespaceName:     "validname",
				BarrierName:       string(make([]byte, 129)),
				Description:       "desc",
				ExpectedProcesses: 3,
			},
			shouldError: true,
		},
		{
			name: "description too long",
			request: &gracklepb.UpdateBarrierRequest{
				NamespaceName:     "validname",
				BarrierName:       "validname",
				Description:       string(make([]byte, 1025)),
				ExpectedProcesses: 3,
			},
			shouldError: true,
		},
		{
			name: "expected_processes zero",
			request: &gracklepb.UpdateBarrierRequest{
				NamespaceName:     "validname",
				BarrierName:       "validname",
				Description:       "desc",
				ExpectedProcesses: 0,
			},
			shouldError: true,
		},
		{
			name: "valid request",
			request: &gracklepb.UpdateBarrierRequest{
				NamespaceName:     "validname",
				BarrierName:       "validname",
				Description:       "Valid description",
				ExpectedProcesses: 5,
			},
			shouldError: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.shouldError {
				require.Error(t, ValidateUpdateBarrierRequest(test.request))
			} else {
				require.NoError(t, ValidateUpdateBarrierRequest(test.request))
			}
		})
	}
}

func TestValidateArriveAtBarrierRequest(t *testing.T) {
	tests := []struct {
		name        string
		request     *gracklepb.ArriveAtBarrierRequest
		shouldError bool
	}{
		{
			name:        "empty request",
			request:     &gracklepb.ArriveAtBarrierRequest{},
			shouldError: true,
		},
		{
			name: "missing namespace name",
			request: &gracklepb.ArriveAtBarrierRequest{
				BarrierName:        "validname",
				ProcessId:          "proc1",
				ExpectedGeneration: 1,
			},
			shouldError: true,
		},
		{
			name: "missing barrier name",
			request: &gracklepb.ArriveAtBarrierRequest{
				NamespaceName:      "validname",
				ProcessId:          "proc1",
				ExpectedGeneration: 1,
			},
			shouldError: true,
		},
		{
			name: "missing process id",
			request: &gracklepb.ArriveAtBarrierRequest{
				NamespaceName:      "validname",
				BarrierName:        "validname",
				ExpectedGeneration: 1,
			},
			shouldError: true,
		},
		{
			name: "invalid namespace name characters",
			request: &gracklepb.ArriveAtBarrierRequest{
				NamespaceName:      "invalid name",
				BarrierName:        "validname",
				ProcessId:          "proc1",
				ExpectedGeneration: 1,
			},
			shouldError: true,
		},
		{
			name: "invalid barrier name characters",
			request: &gracklepb.ArriveAtBarrierRequest{
				NamespaceName:      "validname",
				BarrierName:        "invalid name",
				ProcessId:          "proc1",
				ExpectedGeneration: 1,
			},
			shouldError: true,
		},
		{
			name: "invalid process id characters",
			request: &gracklepb.ArriveAtBarrierRequest{
				NamespaceName:      "validname",
				BarrierName:        "validname",
				ProcessId:          "invalid process id",
				ExpectedGeneration: 1,
			},
			shouldError: true,
		},
		{
			name: "namespace name too long",
			request: &gracklepb.ArriveAtBarrierRequest{
				NamespaceName:      string(make([]byte, 129)),
				BarrierName:        "validname",
				ProcessId:          "proc1",
				ExpectedGeneration: 1,
			},
			shouldError: true,
		},
		{
			name: "barrier name too long",
			request: &gracklepb.ArriveAtBarrierRequest{
				NamespaceName:      "validname",
				BarrierName:        string(make([]byte, 129)),
				ProcessId:          "proc1",
				ExpectedGeneration: 1,
			},
			shouldError: true,
		},
		{
			name: "process id too long",
			request: &gracklepb.ArriveAtBarrierRequest{
				NamespaceName:      "validname",
				BarrierName:        "validname",
				ProcessId:          string(make([]byte, 129)),
				ExpectedGeneration: 1,
			},
			shouldError: true,
		},
		{
			name: "expected generation zero",
			request: &gracklepb.ArriveAtBarrierRequest{
				NamespaceName:      "validname",
				BarrierName:        "validname",
				ProcessId:          "proc1",
				ExpectedGeneration: 0,
			},
			shouldError: true,
		},
		{
			name: "valid request",
			request: &gracklepb.ArriveAtBarrierRequest{
				NamespaceName:      "validname",
				BarrierName:        "validname",
				ProcessId:          "proc1",
				ExpectedGeneration: 1,
			},
			shouldError: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.shouldError {
				require.Error(t, ValidateArriveAtBarrierRequest(test.request))
			} else {
				require.NoError(t, ValidateArriveAtBarrierRequest(test.request))
			}
		})
	}
}

func TestValidateWaitAtBarrierRequest(t *testing.T) {
	tests := []struct {
		name        string
		request     *gracklepb.WaitAtBarrierRequest
		shouldError bool
	}{
		{
			name:        "empty request",
			request:     &gracklepb.WaitAtBarrierRequest{},
			shouldError: true,
		},
		{
			name: "missing namespace name",
			request: &gracklepb.WaitAtBarrierRequest{
				BarrierName:        "validname",
				ExpectedGeneration: 1,
				TimeoutSeconds:     10,
			},
			shouldError: true,
		},
		{
			name: "missing barrier name",
			request: &gracklepb.WaitAtBarrierRequest{
				NamespaceName:      "validname",
				ExpectedGeneration: 1,
				TimeoutSeconds:     10,
			},
			shouldError: true,
		},
		{
			name: "invalid namespace name characters",
			request: &gracklepb.WaitAtBarrierRequest{
				NamespaceName:      "invalid name",
				BarrierName:        "validname",
				ExpectedGeneration: 1,
				TimeoutSeconds:     10,
			},
			shouldError: true,
		},
		{
			name: "invalid barrier name characters",
			request: &gracklepb.WaitAtBarrierRequest{
				NamespaceName:      "validname",
				BarrierName:        "invalid name",
				ExpectedGeneration: 1,
				TimeoutSeconds:     10,
			},
			shouldError: true,
		},
		{
			name: "namespace name too long",
			request: &gracklepb.WaitAtBarrierRequest{
				NamespaceName:      string(make([]byte, 129)),
				BarrierName:        "validname",
				ExpectedGeneration: 1,
				TimeoutSeconds:     10,
			},
			shouldError: true,
		},
		{
			name: "barrier name too long",
			request: &gracklepb.WaitAtBarrierRequest{
				NamespaceName:      "validname",
				BarrierName:        string(make([]byte, 129)),
				ExpectedGeneration: 1,
				TimeoutSeconds:     10,
			},
			shouldError: true,
		},
		{
			name: "expected generation zero",
			request: &gracklepb.WaitAtBarrierRequest{
				NamespaceName:      "validname",
				BarrierName:        "validname",
				ExpectedGeneration: 0,
				TimeoutSeconds:     10,
			},
			shouldError: true,
		},
		{
			name: "timeout seconds zero",
			request: &gracklepb.WaitAtBarrierRequest{
				NamespaceName:      "validname",
				BarrierName:        "validname",
				ExpectedGeneration: 1,
				TimeoutSeconds:     0,
			},
			shouldError: true,
		},
		{
			name: "timeout seconds negative",
			request: &gracklepb.WaitAtBarrierRequest{
				NamespaceName:      "validname",
				BarrierName:        "validname",
				ExpectedGeneration: 1,
				TimeoutSeconds:     -1,
			},
			shouldError: true,
		},
		{
			name: "timeout seconds too high",
			request: &gracklepb.WaitAtBarrierRequest{
				NamespaceName:      "validname",
				BarrierName:        "validname",
				ExpectedGeneration: 1,
				TimeoutSeconds:     301,
			},
			shouldError: true,
		},
		{
			name: "valid request",
			request: &gracklepb.WaitAtBarrierRequest{
				NamespaceName:      "validname",
				BarrierName:        "validname",
				ExpectedGeneration: 1,
				TimeoutSeconds:     10,
			},
			shouldError: false,
		},
		{
			name: "valid request with maximum timeout",
			request: &gracklepb.WaitAtBarrierRequest{
				NamespaceName:      "validname",
				BarrierName:        "validname",
				ExpectedGeneration: 1,
				TimeoutSeconds:     300,
			},
			shouldError: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.shouldError {
				require.Error(t, ValidateWaitAtBarrierRequest(test.request))
			} else {
				require.NoError(t, ValidateWaitAtBarrierRequest(test.request))
			}
		})
	}
}

func TestValidateListBarrierParticipantsRequest(t *testing.T) {
	tests := []struct {
		name        string
		request     *gracklepb.ListBarrierParticipantsRequest
		shouldError bool
	}{
		{
			name:        "empty request - should fail due to missing namespace name",
			request:     &gracklepb.ListBarrierParticipantsRequest{},
			shouldError: true,
		},
		{
			name: "missing namespace name",
			request: &gracklepb.ListBarrierParticipantsRequest{
				BarrierName: "validname",
			},
			shouldError: true,
		},
		{
			name: "missing barrier name",
			request: &gracklepb.ListBarrierParticipantsRequest{
				NamespaceName: "validname",
			},
			shouldError: true,
		},
		{
			name: "invalid namespace name characters",
			request: &gracklepb.ListBarrierParticipantsRequest{
				NamespaceName: "invalid name",
				BarrierName:   "validname",
			},
			shouldError: true,
		},
		{
			name: "invalid barrier name characters",
			request: &gracklepb.ListBarrierParticipantsRequest{
				NamespaceName: "validname",
				BarrierName:   "invalid name",
			},
			shouldError: true,
		},
		{
			name: "namespace name too long",
			request: &gracklepb.ListBarrierParticipantsRequest{
				NamespaceName: string(make([]byte, 129)),
				BarrierName:   "validname",
			},
			shouldError: true,
		},
		{
			name: "barrier name too long",
			request: &gracklepb.ListBarrierParticipantsRequest{
				NamespaceName: "validname",
				BarrierName:   string(make([]byte, 129)),
			},
			shouldError: true,
		},
		{
			name: "invalid pagination token (not base64)",
			request: &gracklepb.ListBarrierParticipantsRequest{
				NamespaceName:   "validname",
				BarrierName:     "validname",
				PaginationToken: "invalid-base64!@#",
			},
			shouldError: true,
		},
		{
			name: "pagination token too long",
			request: &gracklepb.ListBarrierParticipantsRequest{
				NamespaceName:   "validname",
				BarrierName:     "validname",
				PaginationToken: string(make([]byte, 1025)),
			},
			shouldError: true,
		},
		{
			name: "limit too high",
			request: &gracklepb.ListBarrierParticipantsRequest{
				NamespaceName: "validname",
				BarrierName:   "validname",
				Limit:         251,
			},
			shouldError: true,
		},
		{
			name: "negative limit",
			request: &gracklepb.ListBarrierParticipantsRequest{
				NamespaceName: "validname",
				BarrierName:   "validname",
				Limit:         -1,
			},
			shouldError: true,
		},
		{
			name: "valid request",
			request: &gracklepb.ListBarrierParticipantsRequest{
				NamespaceName: "validname",
				BarrierName:   "validname",
			},
			shouldError: false,
		},
		{
			name: "valid request with pagination",
			request: &gracklepb.ListBarrierParticipantsRequest{
				NamespaceName:   "validname",
				BarrierName:     "validname",
				PaginationToken: "dGVzdA==",
				Limit:           50,
			},
			shouldError: false,
		},
		{
			name: "valid request with generation",
			request: &gracklepb.ListBarrierParticipantsRequest{
				NamespaceName: "validname",
				BarrierName:   "validname",
				Generation:    1,
			},
			shouldError: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.shouldError {
				require.Error(t, ValidateListBarrierParticipantsRequest(test.request))
			} else {
				require.NoError(t, ValidateListBarrierParticipantsRequest(test.request))
			}
		})
	}
}

func TestValidateCreateSemaphoreLeaseRequest(t *testing.T) {
	tests := []struct {
		name        string
		request     *gracklepb.CreateSemaphoreLeaseRequest
		shouldError bool
	}{
		{
			name: "empty namespace name",
			request: &gracklepb.CreateSemaphoreLeaseRequest{
				NamespaceName: "",
				ProcessId:     "process1",
				TtlSeconds:    60,
			},
			shouldError: true,
		},
		{
			name: "invalid namespace name characters",
			request: &gracklepb.CreateSemaphoreLeaseRequest{
				NamespaceName: "invalid name",
				ProcessId:     "process1",
				TtlSeconds:    60,
			},
			shouldError: true,
		},
		{
			name: "namespace name too long",
			request: &gracklepb.CreateSemaphoreLeaseRequest{
				NamespaceName: string(make([]byte, 129)),
				ProcessId:     "process1",
				TtlSeconds:    60,
			},
			shouldError: true,
		},
		{
			name: "empty process id",
			request: &gracklepb.CreateSemaphoreLeaseRequest{
				NamespaceName: "validname",
				ProcessId:     "",
				TtlSeconds:    60,
			},
			shouldError: true,
		},
		{
			name: "invalid process id characters",
			request: &gracklepb.CreateSemaphoreLeaseRequest{
				NamespaceName: "validname",
				ProcessId:     "invalid process id",
				TtlSeconds:    60,
			},
			shouldError: true,
		},
		{
			name: "process id too long",
			request: &gracklepb.CreateSemaphoreLeaseRequest{
				NamespaceName: "validname",
				ProcessId:     string(make([]byte, 129)),
				TtlSeconds:    60,
			},
			shouldError: true,
		},
		{
			name: "ttl seconds zero",
			request: &gracklepb.CreateSemaphoreLeaseRequest{
				NamespaceName: "validname",
				ProcessId:     "process1",
				TtlSeconds:    0,
			},
			shouldError: true,
		},
		{
			name: "ttl seconds too large",
			request: &gracklepb.CreateSemaphoreLeaseRequest{
				NamespaceName: "validname",
				ProcessId:     "process1",
				TtlSeconds:    301,
			},
			shouldError: true,
		},
		{
			name: "valid request",
			request: &gracklepb.CreateSemaphoreLeaseRequest{
				NamespaceName: "validname",
				ProcessId:     "process1",
				TtlSeconds:    60,
			},
			shouldError: false,
		},
		{
			name: "valid request with max ttl",
			request: &gracklepb.CreateSemaphoreLeaseRequest{
				NamespaceName: "validname",
				ProcessId:     "process1",
				TtlSeconds:    300,
			},
			shouldError: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.shouldError {
				require.Error(t, ValidateCreateSemaphoreLeaseRequest(test.request))
			} else {
				require.NoError(t, ValidateCreateSemaphoreLeaseRequest(test.request))
			}
		})
	}
}

func TestValidateRevokeSemaphoreLeaseRequest(t *testing.T) {
	tests := []struct {
		name        string
		request     *gracklepb.RevokeSemaphoreLeaseRequest
		shouldError bool
	}{
		{
			name: "empty namespace name",
			request: &gracklepb.RevokeSemaphoreLeaseRequest{
				NamespaceName: "",
				LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
			},
			shouldError: true,
		},
		{
			name: "invalid namespace name characters",
			request: &gracklepb.RevokeSemaphoreLeaseRequest{
				NamespaceName: "invalid name",
				LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
			},
			shouldError: true,
		},
		{
			name: "namespace name too long",
			request: &gracklepb.RevokeSemaphoreLeaseRequest{
				NamespaceName: string(make([]byte, 129)),
				LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
			},
			shouldError: true,
		},
		{
			name: "empty lease id",
			request: &gracklepb.RevokeSemaphoreLeaseRequest{
				NamespaceName: "validname",
				LeaseId:       "",
			},
			shouldError: true,
		},
		{
			name: "invalid lease id format (wrong prefix)",
			request: &gracklepb.RevokeSemaphoreLeaseRequest{
				NamespaceName: "validname",
				LeaseId:       "err_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
			},
			shouldError: true,
		},
		{
			name: "invalid lease id format (too long)",
			request: &gracklepb.RevokeSemaphoreLeaseRequest{
				NamespaceName: "validname",
				LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRBsd",
			},
			shouldError: true,
		},
		{
			name: "invalid lease id format (invalid base62)",
			request: &gracklepb.RevokeSemaphoreLeaseRequest{
				NamespaceName: "validname",
				LeaseId:       "ls_NfKKeiPbP18NFeU3lLGr+WWgDJRB",
			},
			shouldError: true,
		},
		{
			name: "lease id too long",
			request: &gracklepb.RevokeSemaphoreLeaseRequest{
				NamespaceName: "validname",
				LeaseId:       string(make([]byte, 65)),
			},
			shouldError: true,
		},
		{
			name: "valid request",
			request: &gracklepb.RevokeSemaphoreLeaseRequest{
				NamespaceName: "validname",
				LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
			},
			shouldError: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.shouldError {
				require.Error(t, ValidateRevokeSemaphoreLeaseRequest(test.request))
			} else {
				require.NoError(t, ValidateRevokeSemaphoreLeaseRequest(test.request))
			}
		})
	}
}

func TestValidateRefreshSemaphoreLeaseRequest(t *testing.T) {
	tests := []struct {
		name        string
		request     *gracklepb.RefreshSemaphoreLeaseRequest
		shouldError bool
	}{
		{
			name: "empty namespace name",
			request: &gracklepb.RefreshSemaphoreLeaseRequest{
				NamespaceName: "",
				LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
				TtlSeconds:    60,
			},
			shouldError: true,
		},
		{
			name: "invalid namespace name characters",
			request: &gracklepb.RefreshSemaphoreLeaseRequest{
				NamespaceName: "invalid name",
				LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
				TtlSeconds:    60,
			},
			shouldError: true,
		},
		{
			name: "namespace name too long",
			request: &gracklepb.RefreshSemaphoreLeaseRequest{
				NamespaceName: string(make([]byte, 129)),
				LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
				TtlSeconds:    60,
			},
			shouldError: true,
		},
		{
			name: "empty lease id",
			request: &gracklepb.RefreshSemaphoreLeaseRequest{
				NamespaceName: "validname",
				LeaseId:       "",
				TtlSeconds:    60,
			},
			shouldError: true,
		},
		{
			name: "invalid lease id format",
			request: &gracklepb.RefreshSemaphoreLeaseRequest{
				NamespaceName: "validname",
				LeaseId:       "err_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
				TtlSeconds:    60,
			},
			shouldError: true,
		},
		{
			name: "lease id too long",
			request: &gracklepb.RefreshSemaphoreLeaseRequest{
				NamespaceName: "validname",
				LeaseId:       string(make([]byte, 65)),
				TtlSeconds:    60,
			},
			shouldError: true,
		},
		{
			name: "ttl seconds zero",
			request: &gracklepb.RefreshSemaphoreLeaseRequest{
				NamespaceName: "validname",
				LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
				TtlSeconds:    0,
			},
			shouldError: true,
		},
		{
			name: "ttl seconds too large",
			request: &gracklepb.RefreshSemaphoreLeaseRequest{
				NamespaceName: "validname",
				LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
				TtlSeconds:    301,
			},
			shouldError: true,
		},
		{
			name: "valid request",
			request: &gracklepb.RefreshSemaphoreLeaseRequest{
				NamespaceName: "validname",
				LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
				TtlSeconds:    60,
			},
			shouldError: false,
		},
		{
			name: "valid request with max ttl",
			request: &gracklepb.RefreshSemaphoreLeaseRequest{
				NamespaceName: "validname",
				LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
				TtlSeconds:    300,
			},
			shouldError: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.shouldError {
				require.Error(t, ValidateRefreshSemaphoreLeaseRequest(test.request))
			} else {
				require.NoError(t, ValidateRefreshSemaphoreLeaseRequest(test.request))
			}
		})
	}
}

func TestValidateListSemaphoreLeasesRequest(t *testing.T) {
	tests := []struct {
		name        string
		request     *gracklepb.ListSemaphoreLeasesRequest
		shouldError bool
	}{
		{
			name: "empty namespace name",
			request: &gracklepb.ListSemaphoreLeasesRequest{
				NamespaceName: "",
			},
			shouldError: true,
		},
		{
			name: "invalid namespace name characters",
			request: &gracklepb.ListSemaphoreLeasesRequest{
				NamespaceName: "invalid name",
			},
			shouldError: true,
		},
		{
			name: "namespace name too long",
			request: &gracklepb.ListSemaphoreLeasesRequest{
				NamespaceName: string(make([]byte, 129)),
			},
			shouldError: true,
		},
		{
			name: "invalid pagination token (not base64)",
			request: &gracklepb.ListSemaphoreLeasesRequest{
				NamespaceName:   "validname",
				PaginationToken: "invalid-base64!@#",
			},
			shouldError: true,
		},
		{
			name: "pagination token too long",
			request: &gracklepb.ListSemaphoreLeasesRequest{
				NamespaceName:   "validname",
				PaginationToken: string(make([]byte, 1025)),
			},
			shouldError: true,
		},
		{
			name: "limit too high",
			request: &gracklepb.ListSemaphoreLeasesRequest{
				NamespaceName: "validname",
				Limit:         251,
			},
			shouldError: true,
		},
		{
			name: "negative limit",
			request: &gracklepb.ListSemaphoreLeasesRequest{
				NamespaceName: "validname",
				Limit:         -1,
			},
			shouldError: true,
		},
		{
			name: "valid request",
			request: &gracklepb.ListSemaphoreLeasesRequest{
				NamespaceName: "validname",
			},
			shouldError: false,
		},
		{
			name: "valid request with pagination",
			request: &gracklepb.ListSemaphoreLeasesRequest{
				NamespaceName:   "validname",
				PaginationToken: "dGVzdA==",
				Limit:           50,
			},
			shouldError: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.shouldError {
				require.Error(t, ValidateListSemaphoreLeasesRequest(test.request))
			} else {
				require.NoError(t, ValidateListSemaphoreLeasesRequest(test.request))
			}
		})
	}
}

func TestValidateGetSemaphoreLeaseRequest(t *testing.T) {
	tests := []struct {
		name        string
		request     *gracklepb.GetSemaphoreLeaseRequest
		shouldError bool
	}{
		{
			name: "empty namespace name",
			request: &gracklepb.GetSemaphoreLeaseRequest{
				NamespaceName: "",
				LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
			},
			shouldError: true,
		},
		{
			name: "invalid namespace name characters",
			request: &gracklepb.GetSemaphoreLeaseRequest{
				NamespaceName: "invalid name",
				LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
			},
			shouldError: true,
		},
		{
			name: "namespace name too long",
			request: &gracklepb.GetSemaphoreLeaseRequest{
				NamespaceName: string(make([]byte, 129)),
				LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
			},
			shouldError: true,
		},
		{
			name: "empty lease id",
			request: &gracklepb.GetSemaphoreLeaseRequest{
				NamespaceName: "validname",
				LeaseId:       "",
			},
			shouldError: true,
		},
		{
			name: "invalid lease id format",
			request: &gracklepb.GetSemaphoreLeaseRequest{
				NamespaceName: "validname",
				LeaseId:       "err_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
			},
			shouldError: true,
		},
		{
			name: "lease id too long",
			request: &gracklepb.GetSemaphoreLeaseRequest{
				NamespaceName: "validname",
				LeaseId:       string(make([]byte, 65)),
			},
			shouldError: true,
		},
		{
			name: "valid request",
			request: &gracklepb.GetSemaphoreLeaseRequest{
				NamespaceName: "validname",
				LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
			},
			shouldError: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.shouldError {
				require.Error(t, ValidateGetSemaphoreLeaseRequest(test.request))
			} else {
				require.NoError(t, ValidateGetSemaphoreLeaseRequest(test.request))
			}
		})
	}
}

func TestValidateCreateLockLeaseRequest(t *testing.T) {
	tests := []struct {
		name        string
		request     *gracklepb.CreateLockLeaseRequest
		shouldError bool
	}{
		{
			name: "empty namespace name",
			request: &gracklepb.CreateLockLeaseRequest{
				NamespaceName: "",
				ProcessId:     "process1",
				TtlSeconds:    60,
			},
			shouldError: true,
		},
		{
			name: "invalid namespace name characters",
			request: &gracklepb.CreateLockLeaseRequest{
				NamespaceName: "invalid name",
				ProcessId:     "process1",
				TtlSeconds:    60,
			},
			shouldError: true,
		},
		{
			name: "namespace name too long",
			request: &gracklepb.CreateLockLeaseRequest{
				NamespaceName: string(make([]byte, 129)),
				ProcessId:     "process1",
				TtlSeconds:    60,
			},
			shouldError: true,
		},
		{
			name: "empty process id",
			request: &gracklepb.CreateLockLeaseRequest{
				NamespaceName: "validname",
				ProcessId:     "",
				TtlSeconds:    60,
			},
			shouldError: true,
		},
		{
			name: "invalid process id characters",
			request: &gracklepb.CreateLockLeaseRequest{
				NamespaceName: "validname",
				ProcessId:     "invalid process id",
				TtlSeconds:    60,
			},
			shouldError: true,
		},
		{
			name: "process id too long",
			request: &gracklepb.CreateLockLeaseRequest{
				NamespaceName: "validname",
				ProcessId:     string(make([]byte, 129)),
				TtlSeconds:    60,
			},
			shouldError: true,
		},
		{
			name: "ttl seconds zero",
			request: &gracklepb.CreateLockLeaseRequest{
				NamespaceName: "validname",
				ProcessId:     "process1",
				TtlSeconds:    0,
			},
			shouldError: true,
		},
		{
			name: "ttl seconds too large",
			request: &gracklepb.CreateLockLeaseRequest{
				NamespaceName: "validname",
				ProcessId:     "process1",
				TtlSeconds:    301,
			},
			shouldError: true,
		},
		{
			name: "valid request",
			request: &gracklepb.CreateLockLeaseRequest{
				NamespaceName: "validname",
				ProcessId:     "process1",
				TtlSeconds:    60,
			},
			shouldError: false,
		},
		{
			name: "valid request with max ttl",
			request: &gracklepb.CreateLockLeaseRequest{
				NamespaceName: "validname",
				ProcessId:     "process1",
				TtlSeconds:    300,
			},
			shouldError: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.shouldError {
				require.Error(t, ValidateCreateLockLeaseRequest(test.request))
			} else {
				require.NoError(t, ValidateCreateLockLeaseRequest(test.request))
			}
		})
	}
}

func TestValidateRevokeLockLeaseRequest(t *testing.T) {
	tests := []struct {
		name        string
		request     *gracklepb.RevokeLockLeaseRequest
		shouldError bool
	}{
		{
			name: "empty namespace name",
			request: &gracklepb.RevokeLockLeaseRequest{
				NamespaceName: "",
				LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
			},
			shouldError: true,
		},
		{
			name: "invalid namespace name characters",
			request: &gracklepb.RevokeLockLeaseRequest{
				NamespaceName: "invalid name",
				LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
			},
			shouldError: true,
		},
		{
			name: "namespace name too long",
			request: &gracklepb.RevokeLockLeaseRequest{
				NamespaceName: string(make([]byte, 129)),
				LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
			},
			shouldError: true,
		},
		{
			name: "empty lease id",
			request: &gracklepb.RevokeLockLeaseRequest{
				NamespaceName: "validname",
				LeaseId:       "",
			},
			shouldError: true,
		},
		{
			name: "invalid lease id format (wrong prefix)",
			request: &gracklepb.RevokeLockLeaseRequest{
				NamespaceName: "validname",
				LeaseId:       "err_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
			},
			shouldError: true,
		},
		{
			name: "invalid lease id format (too long)",
			request: &gracklepb.RevokeLockLeaseRequest{
				NamespaceName: "validname",
				LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRBsd",
			},
			shouldError: true,
		},
		{
			name: "invalid lease id format (invalid base62)",
			request: &gracklepb.RevokeLockLeaseRequest{
				NamespaceName: "validname",
				LeaseId:       "ls_NfKKeiPbP18NFeU3lLGr+WWgDJRB",
			},
			shouldError: true,
		},
		{
			name: "lease id too long",
			request: &gracklepb.RevokeLockLeaseRequest{
				NamespaceName: "validname",
				LeaseId:       string(make([]byte, 65)),
			},
			shouldError: true,
		},
		{
			name: "valid request",
			request: &gracklepb.RevokeLockLeaseRequest{
				NamespaceName: "validname",
				LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
			},
			shouldError: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.shouldError {
				require.Error(t, ValidateRevokeLockLeaseRequest(test.request))
			} else {
				require.NoError(t, ValidateRevokeLockLeaseRequest(test.request))
			}
		})
	}
}

func TestValidateRefreshLockLeaseRequest(t *testing.T) {
	tests := []struct {
		name        string
		request     *gracklepb.RefreshLockLeaseRequest
		shouldError bool
	}{
		{
			name: "empty namespace name",
			request: &gracklepb.RefreshLockLeaseRequest{
				NamespaceName: "",
				LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
				TtlSeconds:    60,
			},
			shouldError: true,
		},
		{
			name: "invalid namespace name characters",
			request: &gracklepb.RefreshLockLeaseRequest{
				NamespaceName: "invalid name",
				LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
				TtlSeconds:    60,
			},
			shouldError: true,
		},
		{
			name: "namespace name too long",
			request: &gracklepb.RefreshLockLeaseRequest{
				NamespaceName: string(make([]byte, 129)),
				LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
				TtlSeconds:    60,
			},
			shouldError: true,
		},
		{
			name: "empty lease id",
			request: &gracklepb.RefreshLockLeaseRequest{
				NamespaceName: "validname",
				LeaseId:       "",
				TtlSeconds:    60,
			},
			shouldError: true,
		},
		{
			name: "invalid lease id format",
			request: &gracklepb.RefreshLockLeaseRequest{
				NamespaceName: "validname",
				LeaseId:       "err_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
				TtlSeconds:    60,
			},
			shouldError: true,
		},
		{
			name: "lease id too long",
			request: &gracklepb.RefreshLockLeaseRequest{
				NamespaceName: "validname",
				LeaseId:       string(make([]byte, 65)),
				TtlSeconds:    60,
			},
			shouldError: true,
		},
		{
			name: "ttl seconds zero",
			request: &gracklepb.RefreshLockLeaseRequest{
				NamespaceName: "validname",
				LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
				TtlSeconds:    0,
			},
			shouldError: true,
		},
		{
			name: "ttl seconds too large",
			request: &gracklepb.RefreshLockLeaseRequest{
				NamespaceName: "validname",
				LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
				TtlSeconds:    301,
			},
			shouldError: true,
		},
		{
			name: "valid request",
			request: &gracklepb.RefreshLockLeaseRequest{
				NamespaceName: "validname",
				LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
				TtlSeconds:    60,
			},
			shouldError: false,
		},
		{
			name: "valid request with max ttl",
			request: &gracklepb.RefreshLockLeaseRequest{
				NamespaceName: "validname",
				LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
				TtlSeconds:    300,
			},
			shouldError: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.shouldError {
				require.Error(t, ValidateRefreshLockLeaseRequest(test.request))
			} else {
				require.NoError(t, ValidateRefreshLockLeaseRequest(test.request))
			}
		})
	}
}

func TestValidateListLockLeasesRequest(t *testing.T) {
	tests := []struct {
		name        string
		request     *gracklepb.ListLockLeasesRequest
		shouldError bool
	}{
		{
			name: "empty namespace name",
			request: &gracklepb.ListLockLeasesRequest{
				NamespaceName: "",
			},
			shouldError: true,
		},
		{
			name: "invalid namespace name characters",
			request: &gracklepb.ListLockLeasesRequest{
				NamespaceName: "invalid name",
			},
			shouldError: true,
		},
		{
			name: "namespace name too long",
			request: &gracklepb.ListLockLeasesRequest{
				NamespaceName: string(make([]byte, 129)),
			},
			shouldError: true,
		},
		{
			name: "invalid pagination token (not base64)",
			request: &gracklepb.ListLockLeasesRequest{
				NamespaceName:   "validname",
				PaginationToken: "invalid-base64!@#",
			},
			shouldError: true,
		},
		{
			name: "pagination token too long",
			request: &gracklepb.ListLockLeasesRequest{
				NamespaceName:   "validname",
				PaginationToken: string(make([]byte, 1025)),
			},
			shouldError: true,
		},
		{
			name: "limit too high",
			request: &gracklepb.ListLockLeasesRequest{
				NamespaceName: "validname",
				Limit:         251,
			},
			shouldError: true,
		},
		{
			name: "negative limit",
			request: &gracklepb.ListLockLeasesRequest{
				NamespaceName: "validname",
				Limit:         -1,
			},
			shouldError: true,
		},
		{
			name: "valid request",
			request: &gracklepb.ListLockLeasesRequest{
				NamespaceName: "validname",
			},
			shouldError: false,
		},
		{
			name: "valid request with pagination",
			request: &gracklepb.ListLockLeasesRequest{
				NamespaceName:   "validname",
				PaginationToken: "dGVzdA==",
				Limit:           50,
			},
			shouldError: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.shouldError {
				require.Error(t, ValidateListLockLeasesRequest(test.request))
			} else {
				require.NoError(t, ValidateListLockLeasesRequest(test.request))
			}
		})
	}
}

func TestValidateGetLockLeaseRequest(t *testing.T) {
	tests := []struct {
		name        string
		request     *gracklepb.GetLockLeaseRequest
		shouldError bool
	}{
		{
			name: "empty namespace name",
			request: &gracklepb.GetLockLeaseRequest{
				NamespaceName: "",
				LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
			},
			shouldError: true,
		},
		{
			name: "invalid namespace name characters",
			request: &gracklepb.GetLockLeaseRequest{
				NamespaceName: "invalid name",
				LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
			},
			shouldError: true,
		},
		{
			name: "namespace name too long",
			request: &gracklepb.GetLockLeaseRequest{
				NamespaceName: string(make([]byte, 129)),
				LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
			},
			shouldError: true,
		},
		{
			name: "empty lease id",
			request: &gracklepb.GetLockLeaseRequest{
				NamespaceName: "validname",
				LeaseId:       "",
			},
			shouldError: true,
		},
		{
			name: "invalid lease id format",
			request: &gracklepb.GetLockLeaseRequest{
				NamespaceName: "validname",
				LeaseId:       "err_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
			},
			shouldError: true,
		},
		{
			name: "lease id too long",
			request: &gracklepb.GetLockLeaseRequest{
				NamespaceName: "validname",
				LeaseId:       string(make([]byte, 65)),
			},
			shouldError: true,
		},
		{
			name: "valid request",
			request: &gracklepb.GetLockLeaseRequest{
				NamespaceName: "validname",
				LeaseId:       "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
			},
			shouldError: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.shouldError {
				require.Error(t, ValidateGetLockLeaseRequest(test.request))
			} else {
				require.NoError(t, ValidateGetLockLeaseRequest(test.request))
			}
		})
	}
}
