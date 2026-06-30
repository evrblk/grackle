package namespaces

import (
	"bytes"
	"io"
	"math/rand/v2"
	"testing"
	"time"

	mrpc "github.com/evrblk/monstera/rpc"
	"github.com/evrblk/monstera/store"
	"github.com/evrblk/yellowstone-common/honey"
	"github.com/stretchr/testify/require"

	"github.com/evrblk/grackle/pkg/coreapis"
	"github.com/evrblk/grackle/pkg/corepb"
	"github.com/evrblk/grackle/pkg/tables"
)

func init() {
	registry := honey.NewBaseTableRegistry(1)
	tables.RegisterGracklePrefixes(registry)
}

func TestCore_CreateNamespace(t *testing.T) {
	t.Run("create a namespace", func(t *testing.T) {
		core := newNamespacesCore(t)
		now := time.Now()
		namespaceId := &corepb.NamespaceId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint64(),
		}

		// Create namespace
		namespace := createNamespace(t, core, namespaceId, "test_namespace", 20, now)
		require.Equal(t, "test_namespace", namespace.Name)
		require.Equal(t, "test description", namespace.Description)
		require.Equal(t, now.UnixNano(), namespace.CreatedAt)
		require.Equal(t, now.UnixNano(), namespace.UpdatedAt)

		// Get this newly created namespace
		namespace = getNamespace(t, core, namespaceId)
		require.Equal(t, "test_namespace", namespace.Name)
		require.Equal(t, "test description", namespace.Description)
		require.Equal(t, now.UnixNano(), namespace.CreatedAt)
		require.Equal(t, now.UnixNano(), namespace.UpdatedAt)

		// Get nonexistent namespace
		appErr := getNamespaceWithError(t, core, &corepb.NamespaceId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint64(),
		})
		require.Equal(t, mrpc.NotFound, appErr.Code)
	})

	t.Run("maximum number of namespaces", func(t *testing.T) {
		core := newNamespacesCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespace1Id := &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: rand.Uint64(),
		}
		namespace2Id := &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: rand.Uint64(),
		}

		// Create namespace 1
		_ = createNamespace(t, core, namespace1Id, "test_namespace_1", 1, now)

		// Create namespace 2
		appErr := createNamespaceWithError(t, core, namespace2Id, "test_namespace_2", 1, now)
		require.Equal(t, mrpc.ResourceExhausted, appErr.Code)
	})

	t.Run("create namespace with duplicate name", func(t *testing.T) {
		core := newNamespacesCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespace1Id := &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: rand.Uint64(),
		}
		namespace2Id := &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: rand.Uint64(),
		}

		// Create first namespace
		_ = createNamespace(t, core, namespace1Id, "test_namespace", 20, now)

		// Try to create second namespace with the same name in the same account
		appErr := createNamespaceWithError(t, core, namespace2Id, "test_namespace", 20, now)
		require.Equal(t, mrpc.AlreadyExists, appErr.Code)
	})

	t.Run("create namespace with duplicate id", func(t *testing.T) {
		core := newNamespacesCore(t)
		now := time.Now()
		namespaceId := &corepb.NamespaceId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint64(),
		}

		// Create first namespace
		_ = createNamespace(t, core, namespaceId, "test_namespace_1", 20, now)

		// Try to create a second namespace reusing the same ID (a different name,
		// so this is an ID collision and not a name conflict)
		appErr := createNamespaceWithError(t, core, namespaceId, "test_namespace_2", 20, now)
		require.Equal(t, mrpc.IDCollision, appErr.Code)
	})

	t.Run("create namespace with same name in different accounts", func(t *testing.T) {
		core := newNamespacesCore(t)
		now := time.Now()
		accountId1 := rand.Uint64()
		accountId2 := rand.Uint64()
		namespace1Id := &corepb.NamespaceId{
			AccountId:   accountId1,
			NamespaceId: rand.Uint64(),
		}
		namespace2Id := &corepb.NamespaceId{
			AccountId:   accountId2,
			NamespaceId: rand.Uint64(),
		}

		// Create namespace in first account
		namespace1 := createNamespace(t, core, namespace1Id, "test_namespace", 20, now)
		require.Equal(t, "test_namespace", namespace1.Name)

		// Create namespace with the same name in a different account (should succeed)
		namespace2 := createNamespace(t, core, namespace2Id, "test_namespace", 20, now)
		require.Equal(t, "test_namespace", namespace2.Name)
	})
}

func TestCore_GetNamespaceByName(t *testing.T) {
	t.Run("get existing namespace by name", func(t *testing.T) {
		core := newNamespacesCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespaceId := &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: rand.Uint64(),
		}

		// Create namespace
		_ = createNamespace(t, core, namespaceId, "test_namespace", 20, now)

		// Get namespace by name
		namespace := getNamespaceByName(t, core, accountId, "test_namespace")
		require.Equal(t, "test_namespace", namespace.Name)
		require.Equal(t, "test description", namespace.Description)
		require.Equal(t, namespace.Id.NamespaceId, namespace.Id.NamespaceId)
		require.Equal(t, accountId, namespace.Id.AccountId)
	})

	t.Run("get nonexistent namespace by name", func(t *testing.T) {
		core := newNamespacesCore(t)
		accountId := rand.Uint64()

		// Try to get nonexistent namespace
		appErr := getNamespaceByNameWithError(t, core, accountId, "nonexistent_namespace")
		require.Equal(t, mrpc.NotFound, appErr.Code)
	})

	t.Run("get namespace by name from different account", func(t *testing.T) {
		core := newNamespacesCore(t)
		now := time.Now()
		accountId1 := rand.Uint64()
		accountId2 := rand.Uint64()
		namespace1Id := &corepb.NamespaceId{
			AccountId:   accountId1,
			NamespaceId: rand.Uint64(),
		}

		// Create namespace in account 1
		_ = createNamespace(t, core, namespace1Id, "test_namespace", 20, now)

		// Try to get namespace by name from account 2 (should fail)
		appErr := getNamespaceByNameWithError(t, core, accountId2, "test_namespace")
		require.Equal(t, mrpc.NotFound, appErr.Code)

		// Verify can get from correct account
		namespace := getNamespaceByName(t, core, accountId1, "test_namespace")
		require.Equal(t, "test_namespace", namespace.Name)
		require.Equal(t, "test description", namespace.Description)
	})

	t.Run("get namespace by name after update", func(t *testing.T) {
		core := newNamespacesCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespaceId := &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: rand.Uint64(),
		}

		// Create namespace
		_ = createNamespace(t, core, namespaceId, "test_namespace", 20, now)

		// Update namespace
		updateTime := now.Add(time.Hour)
		_ = updateNamespace(t, core, accountId, "test_namespace", "updated description", 1, updateTime)

		// Get namespace by name and verify update
		namespace := getNamespaceByName(t, core, accountId, "test_namespace")
		require.Equal(t, "test_namespace", namespace.Name)
		require.Equal(t, "updated description", namespace.Description)
		require.Equal(t, updateTime.UnixNano(), namespace.UpdatedAt)
		require.Equal(t, now.UnixNano(), namespace.CreatedAt)
	})

	t.Run("get multiple namespaces by name", func(t *testing.T) {
		core := newNamespacesCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespace1Id := &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: rand.Uint64(),
		}
		namespace2Id := &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: rand.Uint64(),
		}

		// Create multiple namespaces
		_ = createNamespace(t, core, namespace1Id, "namespace_1", 20, now)
		_ = createNamespace(t, core, namespace2Id, "namespace_2", 20, now)

		// Get first namespace by name
		namespace1 := getNamespaceByName(t, core, accountId, "namespace_1")
		require.Equal(t, "namespace_1", namespace1.Name)

		// Get second namespace by name
		namespace2 := getNamespaceByName(t, core, accountId, "namespace_2")
		require.Equal(t, "namespace_2", namespace2.Name)

		// Verify they are different namespaces
		require.NotEqual(t, namespace1.Id.NamespaceId, namespace2.Id.NamespaceId)
	})
}

func TestCore_ListNamespaces(t *testing.T) {
	core := newNamespacesCore(t)
	now := time.Now()
	accountId := rand.Uint64()
	namespace1Id := &corepb.NamespaceId{
		AccountId:   accountId,
		NamespaceId: rand.Uint64(),
	}
	namespace2Id := &corepb.NamespaceId{
		AccountId:   accountId,
		NamespaceId: rand.Uint64(),
	}

	// Create two namespaces
	_ = createNamespace(t, core, namespace1Id, "namespace_1", 20, now)
	_ = createNamespace(t, core, namespace2Id, "namespace_2", 20, now)

	// List namespaces
	list := listNamespaces(t, core, accountId)
	require.Len(t, list.Namespaces, 2)
}

func TestCore_UpdateNamespace(t *testing.T) {
	t.Run("update existing namespace", func(t *testing.T) {
		core := newNamespacesCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespaceId := &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: rand.Uint64(),
		}

		// Create a namespace first
		_ = createNamespace(t, core, namespaceId, "namespace_1", 20, now)

		// Update the namespace
		updateTime := time.Now().Add(time.Hour)
		namespace := updateNamespace(t, core, accountId, "namespace_1", "updated description", 1, updateTime)
		require.Equal(t, "updated description", namespace.Description)
		require.Equal(t, updateTime.UnixNano(), namespace.UpdatedAt)
		require.Equal(t, now.UnixNano(), namespace.CreatedAt)

		// Verify the update by getting the namespace
		namespace = getNamespace(t, core, namespaceId)
		require.Equal(t, "updated description", namespace.Description)
		require.Equal(t, updateTime.UnixNano(), namespace.UpdatedAt)
	})

	t.Run("update nonexistent namespace", func(t *testing.T) {
		core := newNamespacesCore(t)
		now := time.Now()

		appErr := updateNamespaceWithError(t, core, rand.Uint64(), "nonexistent_namespace", "updated description", 2, now)
		require.Equal(t, mrpc.NotFound, appErr.Code)
	})

	t.Run("version increments on each successful update", func(t *testing.T) {
		core := newNamespacesCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespaceId := &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: rand.Uint64(),
		}

		// A freshly created namespace starts at version 1
		created := createNamespace(t, core, namespaceId, "namespace_1", 20, now)
		require.EqualValues(t, 1, created.Version)

		// Updating with the matching current version succeeds and bumps the version
		updated := updateNamespace(t, core, accountId, "namespace_1", "desc v2", 1, now.Add(time.Minute))
		require.EqualValues(t, 2, updated.Version)

		// The next update must use the new version
		updated = updateNamespace(t, core, accountId, "namespace_1", "desc v3", 2, now.Add(2*time.Minute))
		require.EqualValues(t, 3, updated.Version)
	})

	t.Run("update with stale version", func(t *testing.T) {
		core := newNamespacesCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespaceId := &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: rand.Uint64(),
		}

		_ = createNamespace(t, core, namespaceId, "namespace_1", 20, now)

		// First update with version 1 succeeds (namespace is now at version 2)
		_ = updateNamespace(t, core, accountId, "namespace_1", "desc v2", 1, now.Add(time.Minute))

		// Reusing the stale version 1 is rejected with a version mismatch
		appErr := updateNamespaceWithError(t, core, accountId, "namespace_1", "desc v3", 1, now.Add(2*time.Minute))
		require.Equal(t, mrpc.InvalidRequest, appErr.Code)
		require.Contains(t, appErr.Message, "version mismatch")

		// The rejected update did not change anything
		namespace := getNamespace(t, core, namespaceId)
		require.Equal(t, "desc v2", namespace.Description)
		require.EqualValues(t, 2, namespace.Version)
	})

	t.Run("update with future version", func(t *testing.T) {
		core := newNamespacesCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespaceId := &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: rand.Uint64(),
		}

		_ = createNamespace(t, core, namespaceId, "namespace_1", 20, now)

		// Passing a version the namespace has never reached is rejected
		appErr := updateNamespaceWithError(t, core, accountId, "namespace_1", "desc", 99, now.Add(time.Minute))
		require.Equal(t, mrpc.InvalidRequest, appErr.Code)
		require.Contains(t, appErr.Message, "version mismatch")
	})
}

func TestCore_DeleteNamespace(t *testing.T) {
	t.Run("delete existing namespace", func(t *testing.T) {
		core := newNamespacesCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespaceId := &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: rand.Uint64(),
		}

		// Create a namespace first
		_ = createNamespace(t, core, namespaceId, "test_namespace", 20, now)

		// Verify the namespace exists
		_ = getNamespace(t, core, namespaceId)

		// Delete the namespace
		_ = deleteNamespace(t, core, accountId, "test_namespace", now)

		// Verify the namespace no longer exists
		appErr := getNamespaceWithError(t, core, namespaceId)
		require.Equal(t, mrpc.NotFound, appErr.Code)
	})

	t.Run("delete nonexistent namespace", func(t *testing.T) {
		core := newNamespacesCore(t)
		now := time.Now()

		_ = deleteNamespace(t, core, rand.Uint64(), "nonexistent_namespace", now)
	})

	t.Run("delete with multiple namespaces", func(t *testing.T) {
		core := newNamespacesCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespace1Id := &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: rand.Uint64(),
		}
		namespace2Id := &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: rand.Uint64(),
		}

		// Create multiple namespaces
		_ = createNamespace(t, core, namespace1Id, "test_namespace_1", 20, now)
		_ = createNamespace(t, core, namespace2Id, "test_namespace_2", 20, now)

		// Verify both namespaces exist
		list := listNamespaces(t, core, accountId)
		require.Len(t, list.Namespaces, 2)

		// Delete the first namespace
		_ = deleteNamespace(t, core, accountId, "test_namespace_1", now)

		// Verify only the second namespace remains
		list = listNamespaces(t, core, accountId)
		require.Len(t, list.Namespaces, 1)
		require.Equal(t, "test_namespace_2", list.Namespaces[0].Name)

		// Verify the first namespace no longer exists
		appErr := getNamespaceWithError(t, core, namespace1Id)
		require.Equal(t, mrpc.NotFound, appErr.Code)

		// Verify the second namespace still exists
		namespace2 := getNamespace(t, core, namespace2Id)
		require.Equal(t, "test_namespace_2", namespace2.Name)
	})
}

func TestCore_SnapshotAndRestore(t *testing.T) {
	t.Run("snapshot and restore namespaces", func(t *testing.T) {
		now := time.Now()
		accountId := rand.Uint64()
		namespace1Id := &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: rand.Uint64(),
		}
		namespace2Id := &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: rand.Uint64(),
		}
		namespace3Id := &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: rand.Uint64(),
		}

		// Create two cores for testing snapshot and restore
		core1 := newNamespacesCore(t)
		core2 := newNamespacesCore(t)

		// Create multiple namespaces in core1
		_ = createNamespace(t, core1, namespace1Id, "test_namespace_1", 20, now)
		_ = createNamespace(t, core1, namespace2Id, "test_namespace_2", 20, now)

		// Take snapshot
		snapshot := core1.Snapshot()

		// Update a namespace after snapshot
		updateTime := now.Add(time.Hour)
		_ = updateNamespace(t, core1, accountId, "test_namespace_1", "updated description", 1, updateTime)

		// Create another namespace after snapshot
		_ = createNamespace(t, core1, namespace3Id, "test_namespace_3", 20, now.Add(2*time.Hour))

		// Write snapshot to buffer
		buf := &bytes.Buffer{}
		err := snapshot.Write(buf)
		require.NoError(t, err)

		// Restore snapshot to second core
		err = core2.Restore(io.NopCloser(buf))
		require.NoError(t, err)

		// Verify restored state matches snapshot (before updates)
		// Should have 2 namespaces (the third was created after snapshot)
		list := listNamespaces(t, core2, accountId)
		require.Len(t, list.Namespaces, 2)

		// Verify first namespace has original description (not updated)
		namespace1 := getNamespace(t, core2, namespace1Id)
		require.Equal(t, now.UnixNano(), namespace1.UpdatedAt)

		// Verify second namespace exists
		_ = getNamespace(t, core2, namespace2Id)

		// Verify third namespace doesn't exist in restored state
		appErr := getNamespaceByNameWithError(t, core2, accountId, "test_namespace_3")
		require.Equal(t, mrpc.NotFound, appErr.Code)

		// Verify name index works correctly in restored state
		namespace1 = getNamespaceByName(t, core2, accountId, "test_namespace_1")
		require.Equal(t, "test_namespace_1", namespace1.Name)
	})

	t.Run("snapshot and restore with multiple accounts", func(t *testing.T) {
		now := time.Now()
		accountId1 := rand.Uint64()
		accountId2 := rand.Uint64()
		namespace1Id := &corepb.NamespaceId{AccountId: accountId1, NamespaceId: rand.Uint64()}
		namespace2Id := &corepb.NamespaceId{AccountId: accountId2, NamespaceId: rand.Uint64()}

		// Create two cores for testing snapshot and restore
		core1 := newNamespacesCore(t)
		core2 := newNamespacesCore(t)

		// Create namespaces for account 1
		_ = createNamespace(t, core1, namespace1Id, "account1_namespace", 20, now)

		// Create namespaces for account 2
		_ = createNamespace(t, core1, namespace2Id, "account2_namespace", 20, now)

		// Take snapshot
		snapshot := core1.Snapshot()

		// Write snapshot to buffer
		buf := &bytes.Buffer{}
		err := snapshot.Write(buf)
		require.NoError(t, err)

		// Restore snapshot to second core
		err = core2.Restore(io.NopCloser(buf))
		require.NoError(t, err)

		// Verify both accounts' namespaces are restored
		namespace1 := getNamespace(t, core2, namespace1Id)
		require.Equal(t, "account1_namespace", namespace1.Name)

		namespace2 := getNamespace(t, core2, namespace2Id)
		require.Equal(t, "account2_namespace", namespace2.Name)

		// Verify account isolation is maintained
		list := listNamespaces(t, core2, accountId1)
		require.Len(t, list.Namespaces, 1)

		list = listNamespaces(t, core2, accountId2)
		require.Len(t, list.Namespaces, 1)
	})

	t.Run("snapshot empty core", func(t *testing.T) {
		// Create two cores for testing snapshot and restore
		core1 := newNamespacesCore(t)
		core2 := newNamespacesCore(t)

		// Take snapshot of empty core
		snapshot := core1.Snapshot()

		// Write snapshot to buffer
		buf := &bytes.Buffer{}
		err := snapshot.Write(buf)
		require.NoError(t, err)

		// Restore snapshot to second core
		err = core2.Restore(io.NopCloser(buf))
		require.NoError(t, err)

		// Verify restored core is also empty
		list := listNamespaces(t, core2, rand.Uint64())
		require.Len(t, list.Namespaces, 0)
	})

	t.Run("restore and continue operations", func(t *testing.T) {
		now := time.Now()
		accountId := rand.Uint64()
		namespace1Id := &corepb.NamespaceId{AccountId: accountId, NamespaceId: rand.Uint64()}
		namespace2Id := &corepb.NamespaceId{AccountId: accountId, NamespaceId: rand.Uint64()}

		// Create two cores for testing snapshot and restore
		core1 := newNamespacesCore(t)
		core2 := newNamespacesCore(t)

		// Create namespace in core1
		_ = createNamespace(t, core1, namespace1Id, "test_namespace", 20, now)

		// Take snapshot
		snapshot := core1.Snapshot()
		buf := &bytes.Buffer{}
		err := snapshot.Write(buf)
		require.NoError(t, err)

		// Restore to core2
		err = core2.Restore(io.NopCloser(buf))
		require.NoError(t, err)

		// Update namespace in restored core
		updateTime := now.Add(time.Hour)
		namespace := updateNamespace(t, core2, accountId, "test_namespace", "updated after restore", 1, updateTime)
		require.Equal(t, "updated after restore", namespace.Description)

		// Create new namespace in restored core
		_ = createNamespace(t, core2, namespace2Id, "new_namespace", 20, now)

		// Verify both namespaces exist in restored core
		resp := listNamespaces(t, core2, accountId)
		require.Len(t, resp.Namespaces, 2)
	})
}

func TestCore_NamespaceMetadata(t *testing.T) {
	core := newNamespacesCore(t)
	now := time.Now()
	accountId := rand.Uint64()
	namespaceId := &corepb.NamespaceId{
		AccountId:   accountId,
		NamespaceId: rand.Uint64(),
	}

	// Create namespace with metadata
	resp1, err := core.CreateNamespace(&coreapis.CreateNamespaceRequest{
		Payload: &corepb.CreateNamespaceRequest{
			NamespaceId:           namespaceId,
			Name:                  "test_namespace",
			Description:           "test description",
			Metadata:              map[string]string{"team": "search", "cost-center": "1234"},
			MaxNumberOfNamespaces: 100,
		},
		Now: now.UnixNano(),
	})
	require.NoError(t, err)
	require.Nil(t, resp1.ApplicationError)
	require.Equal(t, map[string]string{"team": "search", "cost-center": "1234"}, resp1.Payload.Namespace.Metadata)

	// Get namespace and confirm metadata persisted
	ns := getNamespaceByName(t, core, namespaceId.AccountId, "test_namespace")
	require.Equal(t, map[string]string{"team": "search", "cost-center": "1234"}, ns.Metadata)

	// Update namespace metadata
	resp2, err := core.UpdateNamespace(&coreapis.UpdateNamespaceRequest{
		Payload: &corepb.UpdateNamespaceRequest{
			AccountId:       accountId,
			NamespaceName:   "test_namespace",
			Description:     "updated description",
			Metadata:        map[string]string{"team": "search", "cost-center": "5678"},
			ExpectedVersion: 1,
		},
		Now: now.Add(time.Minute).UnixNano(),
	})
	require.NoError(t, err)
	require.Nil(t, resp2.ApplicationError)
	require.Equal(t, map[string]string{"team": "search", "cost-center": "5678"}, resp2.Payload.Namespace.Metadata)

	// Confirm the updated metadata is persisted
	ns = getNamespaceByName(t, core, namespaceId.AccountId, "test_namespace")
	require.Equal(t, map[string]string{"team": "search", "cost-center": "5678"}, ns.Metadata)
}

func newNamespacesCore(t *testing.T) *Core {
	store, err := store.NewBadgerInMemoryStore()
	require.NoError(t, err)
	return NewCore(store, []byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})
}

func createNamespace(t *testing.T, core *Core, namespaceId *corepb.NamespaceId, name string, maxNumberOfNamespaces int64, now time.Time) *corepb.Namespace {
	t.Helper()

	resp, err := core.CreateNamespace(&coreapis.CreateNamespaceRequest{
		Payload: &corepb.CreateNamespaceRequest{
			NamespaceId:           namespaceId,
			Name:                  name,
			Description:           "test description",
			MaxNumberOfNamespaces: maxNumberOfNamespaces,
		},
		Now: now.UnixNano(),
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Nil(t, resp.ApplicationError)
	require.NotNil(t, resp.Payload)
	require.NotNil(t, resp.Payload.Namespace)

	return resp.Payload.Namespace
}

func createNamespaceWithError(t *testing.T, core *Core, namespaceId *corepb.NamespaceId, name string, maxNumberOfNamespaces int64, now time.Time) *mrpc.Error {
	t.Helper()

	resp, err := core.CreateNamespace(&coreapis.CreateNamespaceRequest{
		Payload: &corepb.CreateNamespaceRequest{
			NamespaceId:           namespaceId,
			Name:                  name,
			Description:           "test description",
			MaxNumberOfNamespaces: maxNumberOfNamespaces,
		},
		Now: now.UnixNano(),
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Nil(t, resp.Payload)
	require.NotNil(t, resp.ApplicationError)

	return resp.ApplicationError
}

func getNamespaceByName(t *testing.T, core *Core, accountId uint64, name string) *corepb.Namespace {
	t.Helper()

	resp, err := core.GetNamespaceByName(&coreapis.GetNamespaceByNameRequest{
		Payload: &corepb.GetNamespaceByNameRequest{
			AccountId:     accountId,
			NamespaceName: name,
		},
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Nil(t, resp.ApplicationError)
	require.NotNil(t, resp.Payload)
	require.NotNil(t, resp.Payload.Namespace)

	return resp.Payload.Namespace
}

func getNamespace(t *testing.T, core *Core, namespaceId *corepb.NamespaceId) *corepb.Namespace {
	t.Helper()

	resp, err := core.GetNamespace(&coreapis.GetNamespaceRequest{
		Payload: &corepb.GetNamespaceRequest{
			NamespaceId: namespaceId,
		},
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Nil(t, resp.ApplicationError)
	require.NotNil(t, resp.Payload)
	require.NotNil(t, resp.Payload.Namespace)

	return resp.Payload.Namespace
}

func getNamespaceWithError(t *testing.T, core *Core, namespaceId *corepb.NamespaceId) *mrpc.Error {
	t.Helper()

	resp, err := core.GetNamespace(&coreapis.GetNamespaceRequest{
		Payload: &corepb.GetNamespaceRequest{
			NamespaceId: namespaceId,
		},
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.NotNil(t, resp.ApplicationError)
	require.Nil(t, resp.Payload)

	return resp.ApplicationError
}

func getNamespaceByNameWithError(t *testing.T, core *Core, accountId uint64, name string) *mrpc.Error {
	t.Helper()

	resp, err := core.GetNamespaceByName(&coreapis.GetNamespaceByNameRequest{
		Payload: &corepb.GetNamespaceByNameRequest{
			AccountId:     accountId,
			NamespaceName: "test_namespace",
		},
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Nil(t, resp.Payload)
	require.NotNil(t, resp.ApplicationError)

	return resp.ApplicationError
}

func listNamespaces(t *testing.T, core *Core, accountId uint64) *corepb.ListNamespacesResponse {
	t.Helper()

	resp, err := core.ListNamespaces(&coreapis.ListNamespacesRequest{
		Payload: &corepb.ListNamespacesRequest{
			AccountId: accountId,
		},
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Nil(t, resp.ApplicationError)
	require.NotNil(t, resp.Payload)

	return resp.Payload
}

func updateNamespace(t *testing.T, core *Core, accountId uint64, namespaceName string, description string, version int64, now time.Time) *corepb.Namespace {
	t.Helper()

	resp, err := core.UpdateNamespace(&coreapis.UpdateNamespaceRequest{
		Payload: &corepb.UpdateNamespaceRequest{
			AccountId:       accountId,
			NamespaceName:   namespaceName,
			Description:     description,
			ExpectedVersion: version,
		},
		Now: now.UnixNano(),
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Nil(t, resp.ApplicationError)
	require.NotNil(t, resp.Payload)
	require.NotNil(t, resp.Payload.Namespace)

	return resp.Payload.Namespace
}

func updateNamespaceWithError(t *testing.T, core *Core, accountId uint64, namespaceName string, description string, version int64, now time.Time) *mrpc.Error {
	t.Helper()

	resp, err := core.UpdateNamespace(&coreapis.UpdateNamespaceRequest{
		Payload: &corepb.UpdateNamespaceRequest{
			AccountId:       accountId,
			NamespaceName:   namespaceName,
			Description:     description,
			ExpectedVersion: version,
		},
		Now: now.UnixNano(),
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Nil(t, resp.Payload)
	require.NotNil(t, resp.ApplicationError)

	return resp.ApplicationError
}

func deleteNamespace(t *testing.T, core *Core, accountId uint64, namespaceName string, now time.Time) *corepb.DeleteNamespaceResponse {
	t.Helper()

	resp, err := core.DeleteNamespace(&coreapis.DeleteNamespaceRequest{
		Payload: &corepb.DeleteNamespaceRequest{
			AccountId:     accountId,
			NamespaceName: namespaceName,
		},
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Nil(t, resp.ApplicationError)
	require.NotNil(t, resp.Payload)

	return resp.Payload
}
