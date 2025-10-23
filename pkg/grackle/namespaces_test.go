package grackle

import (
	"math/rand/v2"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/evrblk/grackle/pkg/corepb"
	"github.com/evrblk/monstera"
)

func TestCreateAndGetNamespace(t *testing.T) {
	namespacesCore := newNamespacesCore()

	now := time.Now()

	// Create namespace
	response1, err := namespacesCore.CreateNamespace(&corepb.CreateNamespaceRequest{
		AccountId:             rand.Uint64(),
		Name:                  "test_namespace",
		Description:           "test description",
		Now:                   now.UnixNano(),
		MaxNumberOfNamespaces: 20,
	})

	require.NoError(t, err)
	require.NotNil(t, response1.Namespace)

	// Get this newly created namespace
	response2, err := namespacesCore.GetNamespace(&corepb.GetNamespaceRequest{
		NamespaceId: response1.Namespace.Id,
	})

	require.NoError(t, err)
	require.NotNil(t, response2.Namespace)

	require.Equal(t, "test_namespace", response2.Namespace.Id.NamespaceName)
	require.Equal(t, "test description", response2.Namespace.Description)
	require.Equal(t, now.UnixNano(), response2.Namespace.CreatedAt)
	require.Equal(t, now.UnixNano(), response2.Namespace.UpdatedAt)

	// Get non-existent namespace
	_, err = namespacesCore.GetNamespace(&corepb.GetNamespaceRequest{
		NamespaceId: &corepb.NamespaceId{
			AccountId:     rand.Uint64(),
			NamespaceName: "random_name",
		},
	})

	require.Error(t, err)
}

func TestListNamespaces(t *testing.T) {
	namespacesCore := newNamespacesCore()

	now := time.Now()

	accountId := rand.Uint64()

	// Create namespace 1
	response1, err := namespacesCore.CreateNamespace(&corepb.CreateNamespaceRequest{
		AccountId:             accountId,
		Name:                  "test_namespace_1",
		Now:                   now.UnixNano(),
		MaxNumberOfNamespaces: 20,
	})

	require.NoError(t, err)
	require.NotNil(t, response1.Namespace)

	// Create namespace 2
	response2, err := namespacesCore.CreateNamespace(&corepb.CreateNamespaceRequest{
		AccountId:             accountId,
		Name:                  "test_namespace_2",
		Now:                   now.UnixNano(),
		MaxNumberOfNamespaces: 20,
	})

	require.NoError(t, err)
	require.NotNil(t, response2.Namespace)

	// List namespaces
	response3, err := namespacesCore.ListNamespaces(&corepb.ListNamespacesRequest{
		AccountId: accountId,
	})

	require.NoError(t, err)
	require.Len(t, response3.Namespaces, 2)
}

func TestMaxNumberOfNamespaces(t *testing.T) {
	namespacesCore := newNamespacesCore()

	now := time.Now()

	accountId := rand.Uint64()

	// Create namespace 1
	response1, err := namespacesCore.CreateNamespace(&corepb.CreateNamespaceRequest{
		AccountId:             accountId,
		Name:                  "test_namespace_1",
		Now:                   now.UnixNano(),
		MaxNumberOfNamespaces: 1,
	})

	require.NoError(t, err)
	require.NotNil(t, response1.Namespace)

	// Create namespace 2
	_, err = namespacesCore.CreateNamespace(&corepb.CreateNamespaceRequest{
		AccountId:             accountId,
		Name:                  "test_namespace_2",
		Now:                   now.UnixNano(),
		MaxNumberOfNamespaces: 1,
	})

	require.Error(t, err)
}

func newNamespacesCore() *NamespacesCore {
	return NewNamespacesCore(monstera.NewBadgerInMemoryStore(), []byte{0x1d, 0x36, 0x00, 0x00}, []byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})
}

func TestUpdateNamespace(t *testing.T) {
	namespacesCore := newNamespacesCore()
	now := time.Now()
	accountId := rand.Uint64()

	// Create a namespace first
	createResponse, err := namespacesCore.CreateNamespace(&corepb.CreateNamespaceRequest{
		AccountId:             accountId,
		Name:                  "test_namespace",
		Description:           "original description",
		Now:                   now.UnixNano(),
		MaxNumberOfNamespaces: 20,
	})

	require.NoError(t, err)
	require.NotNil(t, createResponse.Namespace)
	require.Equal(t, "original description", createResponse.Namespace.Description)

	// Update the namespace
	updateTime := time.Now().Add(time.Hour)
	updateResponse, err := namespacesCore.UpdateNamespace(&corepb.UpdateNamespaceRequest{
		NamespaceId: createResponse.Namespace.Id,
		Description: "updated description",
		Now:         updateTime.UnixNano(),
	})

	require.NoError(t, err)
	require.NotNil(t, updateResponse.Namespace)
	require.Equal(t, "updated description", updateResponse.Namespace.Description)
	require.Equal(t, updateTime.UnixNano(), updateResponse.Namespace.UpdatedAt)
	require.Equal(t, now.UnixNano(), updateResponse.Namespace.CreatedAt)

	// Verify the update by getting the namespace
	getResponse, err := namespacesCore.GetNamespace(&corepb.GetNamespaceRequest{
		NamespaceId: createResponse.Namespace.Id,
	})

	require.NoError(t, err)
	require.NotNil(t, getResponse.Namespace)
	require.Equal(t, "updated description", getResponse.Namespace.Description)
	require.Equal(t, updateTime.UnixNano(), getResponse.Namespace.UpdatedAt)
}

func TestUpdateNamespaceNotFound(t *testing.T) {
	namespacesCore := newNamespacesCore()
	now := time.Now()

	// Try to update a non-existent namespace
	_, err := namespacesCore.UpdateNamespace(&corepb.UpdateNamespaceRequest{
		NamespaceId: &corepb.NamespaceId{
			AccountId:     rand.Uint64(),
			NamespaceName: "non_existent_namespace",
		},
		Description: "updated description",
		Now:         now.UnixNano(),
	})

	require.Error(t, err)
}

func TestUpdateNamespaceEmptyDescription(t *testing.T) {
	namespacesCore := newNamespacesCore()
	now := time.Now()
	accountId := rand.Uint64()

	// Create a namespace first
	createResponse, err := namespacesCore.CreateNamespace(&corepb.CreateNamespaceRequest{
		AccountId:             accountId,
		Name:                  "test_namespace",
		Description:           "original description",
		Now:                   now.UnixNano(),
		MaxNumberOfNamespaces: 20,
	})

	require.NoError(t, err)
	require.NotNil(t, createResponse.Namespace)

	// Update the namespace with empty description
	updateTime := time.Now().Add(time.Hour)
	updateResponse, err := namespacesCore.UpdateNamespace(&corepb.UpdateNamespaceRequest{
		NamespaceId: createResponse.Namespace.Id,
		Description: "",
		Now:         updateTime.UnixNano(),
	})

	require.NoError(t, err)
	require.NotNil(t, updateResponse.Namespace)
	require.Equal(t, "", updateResponse.Namespace.Description)
	require.Equal(t, updateTime.UnixNano(), updateResponse.Namespace.UpdatedAt)
}

func TestDeleteNamespace(t *testing.T) {
	namespacesCore := newNamespacesCore()
	now := time.Now()
	accountId := rand.Uint64()

	// Create a namespace first
	createResponse, err := namespacesCore.CreateNamespace(&corepb.CreateNamespaceRequest{
		AccountId:             accountId,
		Name:                  "test_namespace",
		Description:           "test description",
		Now:                   now.UnixNano(),
		MaxNumberOfNamespaces: 20,
	})

	require.NoError(t, err)
	require.NotNil(t, createResponse.Namespace)

	// Verify the namespace exists
	getResponse, err := namespacesCore.GetNamespace(&corepb.GetNamespaceRequest{
		NamespaceId: createResponse.Namespace.Id,
	})

	require.NoError(t, err)
	require.NotNil(t, getResponse.Namespace)

	// Delete the namespace
	deleteResponse, err := namespacesCore.DeleteNamespace(&corepb.DeleteNamespaceRequest{
		NamespaceId: createResponse.Namespace.Id,
	})

	require.NoError(t, err)
	require.NotNil(t, deleteResponse)

	// Verify the namespace no longer exists
	_, err = namespacesCore.GetNamespace(&corepb.GetNamespaceRequest{
		NamespaceId: createResponse.Namespace.Id,
	})

	require.Error(t, err)
	// The error should be a NotFound error
}

func TestDeleteNamespaceNotFound(t *testing.T) {
	namespacesCore := newNamespacesCore()

	// Try to delete a non-existent namespace
	_, err := namespacesCore.DeleteNamespace(&corepb.DeleteNamespaceRequest{
		NamespaceId: &corepb.NamespaceId{
			AccountId:     rand.Uint64(),
			NamespaceName: "non_existent_namespace",
		},
	})

	require.NoError(t, err)
}

func TestDeleteNamespaceMultipleNamespaces(t *testing.T) {
	namespacesCore := newNamespacesCore()
	now := time.Now()
	accountId := rand.Uint64()

	// Create multiple namespaces
	createResponse1, err := namespacesCore.CreateNamespace(&corepb.CreateNamespaceRequest{
		AccountId:             accountId,
		Name:                  "test_namespace_1",
		Description:           "test description 1",
		Now:                   now.UnixNano(),
		MaxNumberOfNamespaces: 20,
	})

	require.NoError(t, err)
	require.NotNil(t, createResponse1.Namespace)

	createResponse2, err := namespacesCore.CreateNamespace(&corepb.CreateNamespaceRequest{
		AccountId:             accountId,
		Name:                  "test_namespace_2",
		Description:           "test description 2",
		Now:                   now.UnixNano(),
		MaxNumberOfNamespaces: 20,
	})

	require.NoError(t, err)
	require.NotNil(t, createResponse2.Namespace)

	// Verify both namespaces exist
	listResponse, err := namespacesCore.ListNamespaces(&corepb.ListNamespacesRequest{
		AccountId: accountId,
	})

	require.NoError(t, err)
	require.Len(t, listResponse.Namespaces, 2)

	// Delete the first namespace
	deleteResponse, err := namespacesCore.DeleteNamespace(&corepb.DeleteNamespaceRequest{
		NamespaceId: createResponse1.Namespace.Id,
	})

	require.NoError(t, err)
	require.NotNil(t, deleteResponse)

	// Verify only the second namespace remains
	listResponse2, err := namespacesCore.ListNamespaces(&corepb.ListNamespacesRequest{
		AccountId: accountId,
	})

	require.NoError(t, err)
	require.Len(t, listResponse2.Namespaces, 1)
	require.Equal(t, "test_namespace_2", listResponse2.Namespaces[0].Id.NamespaceName)

	// Verify the first namespace no longer exists
	_, err = namespacesCore.GetNamespace(&corepb.GetNamespaceRequest{
		NamespaceId: createResponse1.Namespace.Id,
	})

	require.Error(t, err)

	// Verify the second namespace still exists
	getResponse, err := namespacesCore.GetNamespace(&corepb.GetNamespaceRequest{
		NamespaceId: createResponse2.Namespace.Id,
	})

	require.NoError(t, err)
	require.NotNil(t, getResponse.Namespace)
	require.Equal(t, "test_namespace_2", getResponse.Namespace.Id.NamespaceName)
}

func TestUpdateAndDeleteNamespaceWorkflow(t *testing.T) {
	namespacesCore := newNamespacesCore()
	now := time.Now()
	accountId := rand.Uint64()

	// Create a namespace
	createResponse, err := namespacesCore.CreateNamespace(&corepb.CreateNamespaceRequest{
		AccountId:             accountId,
		Name:                  "test_namespace",
		Description:           "original description",
		Now:                   now.UnixNano(),
		MaxNumberOfNamespaces: 20,
	})

	require.NoError(t, err)
	require.NotNil(t, createResponse.Namespace)

	// Update the namespace multiple times
	updateTime1 := time.Now().Add(time.Hour)
	updateResponse1, err := namespacesCore.UpdateNamespace(&corepb.UpdateNamespaceRequest{
		NamespaceId: createResponse.Namespace.Id,
		Description: "first update",
		Now:         updateTime1.UnixNano(),
	})

	require.NoError(t, err)
	require.Equal(t, "first update", updateResponse1.Namespace.Description)
	require.Equal(t, updateTime1.UnixNano(), updateResponse1.Namespace.UpdatedAt)

	updateTime2 := time.Now().Add(2 * time.Hour)
	updateResponse2, err := namespacesCore.UpdateNamespace(&corepb.UpdateNamespaceRequest{
		NamespaceId: createResponse.Namespace.Id,
		Description: "second update",
		Now:         updateTime2.UnixNano(),
	})

	require.NoError(t, err)
	require.Equal(t, "second update", updateResponse2.Namespace.Description)
	require.Equal(t, updateTime2.UnixNano(), updateResponse2.Namespace.UpdatedAt)

	// Verify the final state
	getResponse, err := namespacesCore.GetNamespace(&corepb.GetNamespaceRequest{
		NamespaceId: createResponse.Namespace.Id,
	})

	require.NoError(t, err)
	require.Equal(t, "second update", getResponse.Namespace.Description)
	require.Equal(t, updateTime2.UnixNano(), getResponse.Namespace.UpdatedAt)
	require.Equal(t, now.UnixNano(), getResponse.Namespace.CreatedAt)

	// Delete the namespace
	deleteResponse, err := namespacesCore.DeleteNamespace(&corepb.DeleteNamespaceRequest{
		NamespaceId: createResponse.Namespace.Id,
	})

	require.NoError(t, err)
	require.NotNil(t, deleteResponse)

	// Verify it's gone
	_, err = namespacesCore.GetNamespace(&corepb.GetNamespaceRequest{
		NamespaceId: createResponse.Namespace.Id,
	})

	require.Error(t, err)
}
