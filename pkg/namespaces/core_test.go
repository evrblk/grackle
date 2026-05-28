package namespaces

import (
	"bytes"
	"io"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/evrblk/grackle/pkg/corepb"
	"github.com/evrblk/grackle/pkg/tables"
	"github.com/evrblk/monstera/store"
	monsterax "github.com/evrblk/monstera/x"
)

func init() {
	registry := monsterax.NewBaseTableRegistry(1)
	tables.RegisterGracklePrefixes(registry)
}

func TestCore_CreateNamespace(t *testing.T) {
	t.Run("create a namespace", func(t *testing.T) {
		namespacesCore := newNamespacesCore(t)

		now := time.Now()

		// Create namespace
		response1, err := namespacesCore.CreateNamespace(&corepb.CreateNamespaceRequest{
			NamespaceId: &corepb.NamespaceId{
				AccountId:   rand.Uint64(),
				NamespaceId: rand.Uint32(),
			},
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

		require.Equal(t, "test_namespace", response2.Namespace.Name)
		require.Equal(t, "test description", response2.Namespace.Description)
		require.Equal(t, now.UnixNano(), response2.Namespace.CreatedAt)
		require.Equal(t, now.UnixNano(), response2.Namespace.UpdatedAt)

		// Get nonexistent namespace
		_, err = namespacesCore.GetNamespace(&corepb.GetNamespaceRequest{
			NamespaceId: &corepb.NamespaceId{
				AccountId:   rand.Uint64(),
				NamespaceId: rand.Uint32(),
			},
		})

		require.Error(t, err)
	})

	t.Run("maximum number of namespaces", func(t *testing.T) {
		namespacesCore := newNamespacesCore(t)

		now := time.Now()

		accountId := rand.Uint64()

		// Create namespace 1
		response1, err := namespacesCore.CreateNamespace(&corepb.CreateNamespaceRequest{
			NamespaceId: &corepb.NamespaceId{
				AccountId:   accountId,
				NamespaceId: rand.Uint32(),
			},
			Name:                  "test_namespace_1",
			Now:                   now.UnixNano(),
			MaxNumberOfNamespaces: 1,
		})

		require.NoError(t, err)
		require.NotNil(t, response1.Namespace)

		// Create namespace 2
		_, err = namespacesCore.CreateNamespace(&corepb.CreateNamespaceRequest{
			NamespaceId: &corepb.NamespaceId{
				AccountId:   accountId,
				NamespaceId: rand.Uint32(),
			},
			Name:                  "test_namespace_2",
			Now:                   now.UnixNano(),
			MaxNumberOfNamespaces: 1,
		})

		require.Error(t, err)
	})

	t.Run("create namespace with duplicate name", func(t *testing.T) {
		namespacesCore := newNamespacesCore(t)

		now := time.Now()

		accountId := rand.Uint64()

		// Create first namespace
		response1, err := namespacesCore.CreateNamespace(&corepb.CreateNamespaceRequest{
			NamespaceId: &corepb.NamespaceId{
				AccountId:   accountId,
				NamespaceId: rand.Uint32(),
			},
			Name:                  "test_namespace",
			Description:           "first description",
			Now:                   now.UnixNano(),
			MaxNumberOfNamespaces: 20,
		})

		require.NoError(t, err)
		require.NotNil(t, response1.Namespace)

		// Try to create second namespace with the same name in the same account
		_, err = namespacesCore.CreateNamespace(&corepb.CreateNamespaceRequest{
			NamespaceId: &corepb.NamespaceId{
				AccountId:   accountId,
				NamespaceId: rand.Uint32(),
			},
			Name:                  "test_namespace",
			Description:           "duplicate description",
			Now:                   now.UnixNano(),
			MaxNumberOfNamespaces: 20,
		})

		require.Error(t, err)
	})

	t.Run("create namespace with same name in different accounts", func(t *testing.T) {
		namespacesCore := newNamespacesCore(t)

		now := time.Now()

		accountId1 := rand.Uint64()
		accountId2 := rand.Uint64()

		// Create namespace in first account
		response1, err := namespacesCore.CreateNamespace(&corepb.CreateNamespaceRequest{
			NamespaceId: &corepb.NamespaceId{
				AccountId:   accountId1,
				NamespaceId: rand.Uint32(),
			},
			Name:                  "test_namespace",
			Description:           "account 1 description",
			Now:                   now.UnixNano(),
			MaxNumberOfNamespaces: 20,
		})

		require.NoError(t, err)
		require.NotNil(t, response1.Namespace)

		// Create namespace with the same name in a different account (should succeed)
		response2, err := namespacesCore.CreateNamespace(&corepb.CreateNamespaceRequest{
			NamespaceId: &corepb.NamespaceId{
				AccountId:   accountId2,
				NamespaceId: rand.Uint32(),
			},
			Name:                  "test_namespace",
			Description:           "account 2 description",
			Now:                   now.UnixNano(),
			MaxNumberOfNamespaces: 20,
		})

		require.NoError(t, err)
		require.NotNil(t, response2.Namespace)
		require.Equal(t, "test_namespace", response2.Namespace.Name)
		require.Equal(t, "account 2 description", response2.Namespace.Description)
	})
}

func TestCore_GetNamespaceByName(t *testing.T) {
	t.Run("get existing namespace by name", func(t *testing.T) {
		namespacesCore := newNamespacesCore(t)

		now := time.Now()
		accountId := rand.Uint64()

		// Create namespace
		createResponse, err := namespacesCore.CreateNamespace(&corepb.CreateNamespaceRequest{
			NamespaceId: &corepb.NamespaceId{
				AccountId:   accountId,
				NamespaceId: rand.Uint32(),
			},
			Name:                  "test_namespace",
			Description:           "test description",
			Now:                   now.UnixNano(),
			MaxNumberOfNamespaces: 20,
		})

		require.NoError(t, err)
		require.NotNil(t, createResponse.Namespace)

		// Get namespace by name
		getResponse, err := namespacesCore.GetNamespaceByName(&corepb.GetNamespaceByNameRequest{
			AccountId:     accountId,
			NamespaceName: "test_namespace",
		})

		require.NoError(t, err)
		require.NotNil(t, getResponse.Namespace)
		require.Equal(t, "test_namespace", getResponse.Namespace.Name)
		require.Equal(t, "test description", getResponse.Namespace.Description)
		require.Equal(t, createResponse.Namespace.Id.NamespaceId, getResponse.Namespace.Id.NamespaceId)
		require.Equal(t, accountId, getResponse.Namespace.Id.AccountId)
	})

	t.Run("get nonexistent namespace by name", func(t *testing.T) {
		namespacesCore := newNamespacesCore(t)

		accountId := rand.Uint64()

		// Try to get nonexistent namespace
		_, err := namespacesCore.GetNamespaceByName(&corepb.GetNamespaceByNameRequest{
			AccountId:     accountId,
			NamespaceName: "nonexistent_namespace",
		})

		require.Error(t, err)
	})

	t.Run("get namespace by name from different account", func(t *testing.T) {
		namespacesCore := newNamespacesCore(t)

		now := time.Now()
		accountId1 := rand.Uint64()
		accountId2 := rand.Uint64()

		// Create namespace in account 1
		createResponse, err := namespacesCore.CreateNamespace(&corepb.CreateNamespaceRequest{
			NamespaceId: &corepb.NamespaceId{
				AccountId:   accountId1,
				NamespaceId: rand.Uint32(),
			},
			Name:                  "test_namespace",
			Description:           "account 1 description",
			Now:                   now.UnixNano(),
			MaxNumberOfNamespaces: 20,
		})

		require.NoError(t, err)
		require.NotNil(t, createResponse.Namespace)

		// Try to get namespace by name from account 2 (should fail)
		_, err = namespacesCore.GetNamespaceByName(&corepb.GetNamespaceByNameRequest{
			AccountId:     accountId2,
			NamespaceName: "test_namespace",
		})

		require.Error(t, err)

		// Verify can get from correct account
		getResponse, err := namespacesCore.GetNamespaceByName(&corepb.GetNamespaceByNameRequest{
			AccountId:     accountId1,
			NamespaceName: "test_namespace",
		})

		require.NoError(t, err)
		require.NotNil(t, getResponse.Namespace)
		require.Equal(t, "test_namespace", getResponse.Namespace.Name)
		require.Equal(t, "account 1 description", getResponse.Namespace.Description)
	})

	t.Run("get namespace by name after update", func(t *testing.T) {
		namespacesCore := newNamespacesCore(t)

		now := time.Now()
		accountId := rand.Uint64()

		// Create namespace
		createResponse, err := namespacesCore.CreateNamespace(&corepb.CreateNamespaceRequest{
			NamespaceId: &corepb.NamespaceId{
				AccountId:   accountId,
				NamespaceId: rand.Uint32(),
			},
			Name:                  "test_namespace",
			Description:           "original description",
			Now:                   now.UnixNano(),
			MaxNumberOfNamespaces: 20,
		})

		require.NoError(t, err)
		require.NotNil(t, createResponse.Namespace)

		// Update namespace
		updateTime := now.Add(time.Hour)
		_, err = namespacesCore.UpdateNamespace(&corepb.UpdateNamespaceRequest{
			NamespaceId: createResponse.Namespace.Id,
			Description: "updated description",
			Now:         updateTime.UnixNano(),
		})

		require.NoError(t, err)

		// Get namespace by name and verify update
		getResponse, err := namespacesCore.GetNamespaceByName(&corepb.GetNamespaceByNameRequest{
			AccountId:     accountId,
			NamespaceName: "test_namespace",
		})

		require.NoError(t, err)
		require.NotNil(t, getResponse.Namespace)
		require.Equal(t, "test_namespace", getResponse.Namespace.Name)
		require.Equal(t, "updated description", getResponse.Namespace.Description)
		require.Equal(t, updateTime.UnixNano(), getResponse.Namespace.UpdatedAt)
		require.Equal(t, now.UnixNano(), getResponse.Namespace.CreatedAt)
	})

	t.Run("get multiple namespaces by name", func(t *testing.T) {
		namespacesCore := newNamespacesCore(t)

		now := time.Now()
		accountId := rand.Uint64()

		// Create multiple namespaces
		_, err := namespacesCore.CreateNamespace(&corepb.CreateNamespaceRequest{
			NamespaceId: &corepb.NamespaceId{
				AccountId:   accountId,
				NamespaceId: rand.Uint32(),
			},
			Name:                  "namespace_1",
			Description:           "description 1",
			Now:                   now.UnixNano(),
			MaxNumberOfNamespaces: 20,
		})
		require.NoError(t, err)

		_, err = namespacesCore.CreateNamespace(&corepb.CreateNamespaceRequest{
			NamespaceId: &corepb.NamespaceId{
				AccountId:   accountId,
				NamespaceId: rand.Uint32(),
			},
			Name:                  "namespace_2",
			Description:           "description 2",
			Now:                   now.UnixNano(),
			MaxNumberOfNamespaces: 20,
		})
		require.NoError(t, err)

		// Get first namespace by name
		getResponse1, err := namespacesCore.GetNamespaceByName(&corepb.GetNamespaceByNameRequest{
			AccountId:     accountId,
			NamespaceName: "namespace_1",
		})

		require.NoError(t, err)
		require.NotNil(t, getResponse1.Namespace)
		require.Equal(t, "namespace_1", getResponse1.Namespace.Name)
		require.Equal(t, "description 1", getResponse1.Namespace.Description)

		// Get second namespace by name
		getResponse2, err := namespacesCore.GetNamespaceByName(&corepb.GetNamespaceByNameRequest{
			AccountId:     accountId,
			NamespaceName: "namespace_2",
		})

		require.NoError(t, err)
		require.NotNil(t, getResponse2.Namespace)
		require.Equal(t, "namespace_2", getResponse2.Namespace.Name)
		require.Equal(t, "description 2", getResponse2.Namespace.Description)

		// Verify they are different namespaces
		require.NotEqual(t, getResponse1.Namespace.Id.NamespaceId, getResponse2.Namespace.Id.NamespaceId)
	})
}

func TestCore_ListNamespaces(t *testing.T) {
	namespacesCore := newNamespacesCore(t)

	now := time.Now()

	accountId := rand.Uint64()

	// Create namespace 1
	response1, err := namespacesCore.CreateNamespace(&corepb.CreateNamespaceRequest{
		NamespaceId: &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: rand.Uint32(),
		},
		Name:                  "test_namespace_1",
		Now:                   now.UnixNano(),
		MaxNumberOfNamespaces: 20,
	})

	require.NoError(t, err)
	require.NotNil(t, response1.Namespace)

	// Create namespace 2
	response2, err := namespacesCore.CreateNamespace(&corepb.CreateNamespaceRequest{
		NamespaceId: &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: rand.Uint32(),
		},
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

func TestCore_UpdateNamespace(t *testing.T) {
	t.Run("update existing namespace", func(t *testing.T) {
		namespacesCore := newNamespacesCore(t)
		now := time.Now()
		accountId := rand.Uint64()

		// Create a namespace first
		createResponse, err := namespacesCore.CreateNamespace(&corepb.CreateNamespaceRequest{
			NamespaceId: &corepb.NamespaceId{
				AccountId:   accountId,
				NamespaceId: rand.Uint32(),
			},
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
	})

	t.Run("update nonexistent namespace", func(t *testing.T) {
		namespacesCore := newNamespacesCore(t)
		now := time.Now()

		_, err := namespacesCore.UpdateNamespace(&corepb.UpdateNamespaceRequest{
			NamespaceId: &corepb.NamespaceId{
				AccountId:   rand.Uint64(),
				NamespaceId: rand.Uint32(),
			},
			Description: "updated description",
			Now:         now.UnixNano(),
		})

		require.Error(t, err)
	})
}

func TestCore_DeleteNamespace(t *testing.T) {
	t.Run("delete existing namespace", func(t *testing.T) {
		namespacesCore := newNamespacesCore(t)
		now := time.Now()
		accountId := rand.Uint64()

		// Create a namespace first
		createResponse, err := namespacesCore.CreateNamespace(&corepb.CreateNamespaceRequest{
			NamespaceId: &corepb.NamespaceId{
				AccountId:   accountId,
				NamespaceId: rand.Uint32(),
			},
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
	})

	t.Run("delete nonexistent namespace", func(t *testing.T) {
		namespacesCore := newNamespacesCore(t)

		_, err := namespacesCore.DeleteNamespace(&corepb.DeleteNamespaceRequest{
			NamespaceId: &corepb.NamespaceId{
				AccountId:   rand.Uint64(),
				NamespaceId: rand.Uint32(),
			},
		})

		require.NoError(t, err)
	})

	t.Run("delete with multiple namespaces", func(t *testing.T) {
		namespacesCore := newNamespacesCore(t)
		now := time.Now()
		accountId := rand.Uint64()

		// Create multiple namespaces
		createResponse1, err := namespacesCore.CreateNamespace(&corepb.CreateNamespaceRequest{
			NamespaceId: &corepb.NamespaceId{
				AccountId:   accountId,
				NamespaceId: rand.Uint32(),
			},
			Name:                  "test_namespace_1",
			Description:           "test description 1",
			Now:                   now.UnixNano(),
			MaxNumberOfNamespaces: 20,
		})

		require.NoError(t, err)
		require.NotNil(t, createResponse1.Namespace)

		createResponse2, err := namespacesCore.CreateNamespace(&corepb.CreateNamespaceRequest{
			NamespaceId: &corepb.NamespaceId{
				AccountId:   accountId,
				NamespaceId: rand.Uint32(),
			},
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
		require.Equal(t, "test_namespace_2", listResponse2.Namespaces[0].Name)

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
		require.Equal(t, "test_namespace_2", getResponse.Namespace.Name)
	})
}

func TestCore_SnapshotAndRestore(t *testing.T) {
	t.Run("snapshot and restore namespaces", func(t *testing.T) {
		now := time.Now()
		accountId := rand.Uint64()

		// Create two cores for testing snapshot and restore
		namespacesCore1 := newNamespacesCore(t)
		namespacesCore2 := newNamespacesCore(t)

		// Create multiple namespaces in core1
		createResponse1, err := namespacesCore1.CreateNamespace(&corepb.CreateNamespaceRequest{
			NamespaceId: &corepb.NamespaceId{
				AccountId:   accountId,
				NamespaceId: rand.Uint32(),
			},
			Name:                  "test_namespace_1",
			Description:           "description 1",
			Now:                   now.UnixNano(),
			MaxNumberOfNamespaces: 20,
		})
		require.NoError(t, err)
		require.NotNil(t, createResponse1.Namespace)

		createResponse2, err := namespacesCore1.CreateNamespace(&corepb.CreateNamespaceRequest{
			NamespaceId: &corepb.NamespaceId{
				AccountId:   accountId,
				NamespaceId: rand.Uint32(),
			},
			Name:                  "test_namespace_2",
			Description:           "description 2",
			Now:                   now.UnixNano(),
			MaxNumberOfNamespaces: 20,
		})
		require.NoError(t, err)
		require.NotNil(t, createResponse2.Namespace)

		// Take snapshot
		snapshot := namespacesCore1.Snapshot()

		// Update a namespace after snapshot
		updateTime := now.Add(time.Hour)
		_, err = namespacesCore1.UpdateNamespace(&corepb.UpdateNamespaceRequest{
			NamespaceId: createResponse1.Namespace.Id,
			Description: "updated description",
			Now:         updateTime.UnixNano(),
		})
		require.NoError(t, err)

		// Create another namespace after snapshot
		_, err = namespacesCore1.CreateNamespace(&corepb.CreateNamespaceRequest{
			NamespaceId: &corepb.NamespaceId{
				AccountId:   accountId,
				NamespaceId: rand.Uint32(),
			},
			Name:                  "test_namespace_3",
			Description:           "description 3",
			Now:                   now.Add(2 * time.Hour).UnixNano(),
			MaxNumberOfNamespaces: 20,
		})
		require.NoError(t, err)

		// Write snapshot to buffer
		buf := &bytes.Buffer{}
		err = snapshot.Write(buf)
		require.NoError(t, err)

		// Restore snapshot to second core
		err = namespacesCore2.Restore(io.NopCloser(buf))
		require.NoError(t, err)

		// Verify restored state matches snapshot (before updates)
		// Should have 2 namespaces (the third was created after snapshot)
		listResponse, err := namespacesCore2.ListNamespaces(&corepb.ListNamespacesRequest{
			AccountId: accountId,
		})
		require.NoError(t, err)
		require.Len(t, listResponse.Namespaces, 2)

		// Verify first namespace has original description (not updated)
		getResponse1, err := namespacesCore2.GetNamespace(&corepb.GetNamespaceRequest{
			NamespaceId: createResponse1.Namespace.Id,
		})
		require.NoError(t, err)
		require.Equal(t, "description 1", getResponse1.Namespace.Description)
		require.Equal(t, now.UnixNano(), getResponse1.Namespace.UpdatedAt)

		// Verify second namespace exists
		getResponse2, err := namespacesCore2.GetNamespace(&corepb.GetNamespaceRequest{
			NamespaceId: createResponse2.Namespace.Id,
		})
		require.NoError(t, err)
		require.Equal(t, "description 2", getResponse2.Namespace.Description)

		// Verify third namespace doesn't exist in restored state
		_, err = namespacesCore2.GetNamespaceByName(&corepb.GetNamespaceByNameRequest{
			AccountId:     accountId,
			NamespaceName: "test_namespace_3",
		})
		require.Error(t, err)

		// Verify name index works correctly in restored state
		getByNameResponse, err := namespacesCore2.GetNamespaceByName(&corepb.GetNamespaceByNameRequest{
			AccountId:     accountId,
			NamespaceName: "test_namespace_1",
		})
		require.NoError(t, err)
		require.Equal(t, "test_namespace_1", getByNameResponse.Namespace.Name)
		require.Equal(t, "description 1", getByNameResponse.Namespace.Description)
	})

	t.Run("snapshot and restore with multiple accounts", func(t *testing.T) {
		now := time.Now()

		namespacesCore1 := newNamespacesCore(t)
		namespacesCore2 := newNamespacesCore(t)

		accountId1 := rand.Uint64()
		accountId2 := rand.Uint64()

		// Create namespaces for account 1
		createResponse1, err := namespacesCore1.CreateNamespace(&corepb.CreateNamespaceRequest{
			NamespaceId: &corepb.NamespaceId{
				AccountId:   accountId1,
				NamespaceId: rand.Uint32(),
			},
			Name:                  "account1_namespace",
			Description:           "account 1 description",
			Now:                   now.UnixNano(),
			MaxNumberOfNamespaces: 20,
		})
		require.NoError(t, err)

		// Create namespaces for account 2
		createResponse2, err := namespacesCore1.CreateNamespace(&corepb.CreateNamespaceRequest{
			NamespaceId: &corepb.NamespaceId{
				AccountId:   accountId2,
				NamespaceId: rand.Uint32(),
			},
			Name:                  "account2_namespace",
			Description:           "account 2 description",
			Now:                   now.UnixNano(),
			MaxNumberOfNamespaces: 20,
		})
		require.NoError(t, err)

		// Take snapshot
		snapshot := namespacesCore1.Snapshot()

		// Write snapshot to buffer
		buf := &bytes.Buffer{}
		err = snapshot.Write(buf)
		require.NoError(t, err)

		// Restore snapshot to second core
		err = namespacesCore2.Restore(io.NopCloser(buf))
		require.NoError(t, err)

		// Verify both accounts' namespaces are restored
		getResponse1, err := namespacesCore2.GetNamespace(&corepb.GetNamespaceRequest{
			NamespaceId: createResponse1.Namespace.Id,
		})
		require.NoError(t, err)
		require.Equal(t, "account1_namespace", getResponse1.Namespace.Name)

		getResponse2, err := namespacesCore2.GetNamespace(&corepb.GetNamespaceRequest{
			NamespaceId: createResponse2.Namespace.Id,
		})
		require.NoError(t, err)
		require.Equal(t, "account2_namespace", getResponse2.Namespace.Name)

		// Verify account isolation is maintained
		listResponse1, err := namespacesCore2.ListNamespaces(&corepb.ListNamespacesRequest{
			AccountId: accountId1,
		})
		require.NoError(t, err)
		require.Len(t, listResponse1.Namespaces, 1)

		listResponse2, err := namespacesCore2.ListNamespaces(&corepb.ListNamespacesRequest{
			AccountId: accountId2,
		})
		require.NoError(t, err)
		require.Len(t, listResponse2.Namespaces, 1)
	})

	t.Run("snapshot empty core", func(t *testing.T) {
		namespacesCore1 := newNamespacesCore(t)
		namespacesCore2 := newNamespacesCore(t)

		// Take snapshot of empty core
		snapshot := namespacesCore1.Snapshot()

		// Write snapshot to buffer
		buf := &bytes.Buffer{}
		err := snapshot.Write(buf)
		require.NoError(t, err)

		// Restore snapshot to second core
		err = namespacesCore2.Restore(io.NopCloser(buf))
		require.NoError(t, err)

		// Verify restored core is also empty
		listResponse, err := namespacesCore2.ListNamespaces(&corepb.ListNamespacesRequest{
			AccountId: rand.Uint64(),
		})
		require.NoError(t, err)
		require.Len(t, listResponse.Namespaces, 0)
	})

	t.Run("restore and continue operations", func(t *testing.T) {
		now := time.Now()
		accountId := rand.Uint64()

		namespacesCore1 := newNamespacesCore(t)
		namespacesCore2 := newNamespacesCore(t)

		// Create namespace in core1
		createResponse1, err := namespacesCore1.CreateNamespace(&corepb.CreateNamespaceRequest{
			NamespaceId: &corepb.NamespaceId{
				AccountId:   accountId,
				NamespaceId: rand.Uint32(),
			},
			Name:                  "test_namespace",
			Description:           "original description",
			Now:                   now.UnixNano(),
			MaxNumberOfNamespaces: 20,
		})
		require.NoError(t, err)

		// Take snapshot
		snapshot := namespacesCore1.Snapshot()
		buf := &bytes.Buffer{}
		err = snapshot.Write(buf)
		require.NoError(t, err)

		// Restore to core2
		err = namespacesCore2.Restore(io.NopCloser(buf))
		require.NoError(t, err)

		// Update namespace in restored core
		updateTime := now.Add(time.Hour)
		updateResponse, err := namespacesCore2.UpdateNamespace(&corepb.UpdateNamespaceRequest{
			NamespaceId: createResponse1.Namespace.Id,
			Description: "updated after restore",
			Now:         updateTime.UnixNano(),
		})
		require.NoError(t, err)
		require.Equal(t, "updated after restore", updateResponse.Namespace.Description)

		// Create new namespace in restored core
		createResponse2, err := namespacesCore2.CreateNamespace(&corepb.CreateNamespaceRequest{
			NamespaceId: &corepb.NamespaceId{
				AccountId:   accountId,
				NamespaceId: rand.Uint32(),
			},
			Name:                  "new_namespace",
			Description:           "created after restore",
			Now:                   now.Add(2 * time.Hour).UnixNano(),
			MaxNumberOfNamespaces: 20,
		})
		require.NoError(t, err)
		require.NotNil(t, createResponse2.Namespace)

		// Verify both namespaces exist in restored core
		listResponse, err := namespacesCore2.ListNamespaces(&corepb.ListNamespacesRequest{
			AccountId: accountId,
		})
		require.NoError(t, err)
		require.Len(t, listResponse.Namespaces, 2)
	})
}

func newNamespacesCore(t *testing.T) *Core {
	store, err := store.NewBadgerInMemoryStore()
	require.NoError(t, err)
	return NewCore(store, []byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})
}
