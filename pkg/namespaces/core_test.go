package namespaces

import (
	"bytes"
	"io"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/evrblk/monstera/store"
	monsterax "github.com/evrblk/monstera/x"
	"github.com/stretchr/testify/require"

	"github.com/evrblk/grackle/pkg/coreapis"
	"github.com/evrblk/grackle/pkg/corepb"
	"github.com/evrblk/grackle/pkg/tables"
)

func init() {
	registry := monsterax.NewBaseTableRegistry(1)
	tables.RegisterGracklePrefixes(registry)
}

func TestCore_CreateNamespace(t *testing.T) {
	t.Run("create a namespace", func(t *testing.T) {
		core := newNamespacesCore(t)
		now := time.Now()
		namespaceId := &corepb.NamespaceId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
		}

		// Create namespace
		namespace := createNamespace(t, core, namespaceId, "test_namespace", now)

		// Get this newly created namespace
		resp2, err := core.GetNamespace(&coreapis.GetNamespaceRequest{
			Payload: &corepb.GetNamespaceRequest{
				NamespaceId: namespace.Id,
			},
		})

		require.NoError(t, err)
		require.NotNil(t, resp2)
		require.Nil(t, resp2.ApplicationError)
		require.NotNil(t, resp2.Payload)
		require.NotNil(t, resp2.Payload.Namespace)
		require.Equal(t, "test_namespace", resp2.Payload.Namespace.Name)
		require.Equal(t, "test description", resp2.Payload.Namespace.Description)
		require.Equal(t, now.UnixNano(), resp2.Payload.Namespace.CreatedAt)
		require.Equal(t, now.UnixNano(), resp2.Payload.Namespace.UpdatedAt)

		// Get nonexistent namespace
		resp3, err := core.GetNamespace(&coreapis.GetNamespaceRequest{
			Payload: &corepb.GetNamespaceRequest{
				NamespaceId: &corepb.NamespaceId{
					AccountId:   rand.Uint64(),
					NamespaceId: rand.Uint32(),
				},
			},
		})

		require.NoError(t, err)
		require.NotNil(t, resp3)
		require.Nil(t, resp3.Payload)
		require.NotNil(t, resp3.ApplicationError)
		require.Equal(t, monsterax.NotFound, resp3.ApplicationError.Code)
	})

	t.Run("maximum number of namespaces", func(t *testing.T) {
		core := newNamespacesCore(t)
		now := time.Now()
		accountId := rand.Uint64()

		// Create namespace 1
		resp1, err := core.CreateNamespace(&coreapis.CreateNamespaceRequest{
			Payload: &corepb.CreateNamespaceRequest{
				NamespaceId: &corepb.NamespaceId{
					AccountId:   accountId,
					NamespaceId: rand.Uint32(),
				},
				Name:                  "test_namespace_1",
				Now:                   now.UnixNano(),
				MaxNumberOfNamespaces: 1,
			},
		})

		require.NoError(t, err)
		require.NotNil(t, resp1)
		require.Nil(t, resp1.ApplicationError)
		require.NotNil(t, resp1.Payload)
		require.NotNil(t, resp1.Payload.Namespace)

		// Create namespace 2
		resp2, err := core.CreateNamespace(&coreapis.CreateNamespaceRequest{
			Payload: &corepb.CreateNamespaceRequest{
				NamespaceId: &corepb.NamespaceId{
					AccountId:   accountId,
					NamespaceId: rand.Uint32(),
				},
				Name:                  "test_namespace_2",
				Now:                   now.UnixNano(),
				MaxNumberOfNamespaces: 1,
			},
		})

		require.NoError(t, err)
		require.NotNil(t, resp2)
		require.Nil(t, resp2.Payload)
		require.NotNil(t, resp2.ApplicationError)
		require.Equal(t, monsterax.ResourceExhausted, resp2.ApplicationError.Code)
	})

	t.Run("create namespace with duplicate name", func(t *testing.T) {
		core := newNamespacesCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespace1Id := &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: rand.Uint32(),
		}
		namespace2Id := &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: rand.Uint32(),
		}

		// Create first namespace
		_ = createNamespace(t, core, namespace1Id, "test_namespace", now)

		// Try to create second namespace with the same name in the same account
		resp2, err := core.CreateNamespace(&coreapis.CreateNamespaceRequest{
			Payload: &corepb.CreateNamespaceRequest{
				NamespaceId:           namespace2Id,
				Name:                  "test_namespace",
				Description:           "duplicate description",
				Now:                   now.UnixNano(),
				MaxNumberOfNamespaces: 20,
			},
		})

		require.NoError(t, err)
		require.NotNil(t, resp2)
		require.Nil(t, resp2.Payload)
		require.NotNil(t, resp2.ApplicationError)
		require.Equal(t, monsterax.AlreadyExists, resp2.ApplicationError.Code)
	})

	t.Run("create namespace with same name in different accounts", func(t *testing.T) {
		core := newNamespacesCore(t)
		now := time.Now()
		accountId1 := rand.Uint64()
		accountId2 := rand.Uint64()
		namespace1Id := &corepb.NamespaceId{
			AccountId:   accountId1,
			NamespaceId: rand.Uint32(),
		}
		namespace2Id := &corepb.NamespaceId{
			AccountId:   accountId2,
			NamespaceId: rand.Uint32(),
		}

		// Create namespace in first account
		namespace1 := createNamespace(t, core, namespace1Id, "test_namespace", now)

		require.Equal(t, "test_namespace", namespace1.Name)

		// Create namespace with the same name in a different account (should succeed)
		namespace2 := createNamespace(t, core, namespace2Id, "test_namespace", now)

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
			NamespaceId: rand.Uint32(),
		}

		// Create namespace
		_ = createNamespace(t, core, namespaceId, "test_namespace", now)

		// Get namespace by name
		resp2, err := core.GetNamespaceByName(&coreapis.GetNamespaceByNameRequest{
			Payload: &corepb.GetNamespaceByNameRequest{
				AccountId:     accountId,
				NamespaceName: "test_namespace",
			},
		})

		require.NoError(t, err)
		require.NotNil(t, resp2)
		require.Nil(t, resp2.ApplicationError)
		require.NotNil(t, resp2.Payload)
		require.NotNil(t, resp2.Payload.Namespace)
		require.Equal(t, "test_namespace", resp2.Payload.Namespace.Name)
		require.Equal(t, "test description", resp2.Payload.Namespace.Description)
		require.Equal(t, resp2.Payload.Namespace.Id.NamespaceId, resp2.Payload.Namespace.Id.NamespaceId)
		require.Equal(t, accountId, resp2.Payload.Namespace.Id.AccountId)
	})

	t.Run("get nonexistent namespace by name", func(t *testing.T) {
		core := newNamespacesCore(t)
		accountId := rand.Uint64()

		// Try to get nonexistent namespace
		resp1, err := core.GetNamespaceByName(&coreapis.GetNamespaceByNameRequest{
			Payload: &corepb.GetNamespaceByNameRequest{
				AccountId:     accountId,
				NamespaceName: "nonexistent_namespace",
			},
		})

		require.NoError(t, err)
		require.NotNil(t, resp1)
		require.Nil(t, resp1.Payload)
		require.NotNil(t, resp1.ApplicationError)
		require.Equal(t, monsterax.NotFound, resp1.ApplicationError.Code)
	})

	t.Run("get namespace by name from different account", func(t *testing.T) {
		core := newNamespacesCore(t)
		now := time.Now()
		accountId1 := rand.Uint64()
		accountId2 := rand.Uint64()
		namespace1Id := &corepb.NamespaceId{
			AccountId:   accountId1,
			NamespaceId: rand.Uint32(),
		}

		// Create namespace in account 1
		_ = createNamespace(t, core, namespace1Id, "test_namespace", now)

		// Try to get namespace by name from account 2 (should fail)
		resp2, err := core.GetNamespaceByName(&coreapis.GetNamespaceByNameRequest{
			Payload: &corepb.GetNamespaceByNameRequest{
				AccountId:     accountId2,
				NamespaceName: "test_namespace",
			},
		})

		require.NoError(t, err)
		require.NotNil(t, resp2)
		require.Nil(t, resp2.Payload)
		require.NotNil(t, resp2.ApplicationError)
		require.Equal(t, monsterax.NotFound, resp2.ApplicationError.Code)

		// Verify can get from correct account
		resp3, err := core.GetNamespaceByName(&coreapis.GetNamespaceByNameRequest{
			Payload: &corepb.GetNamespaceByNameRequest{
				AccountId:     accountId1,
				NamespaceName: "test_namespace",
			},
		})

		require.NoError(t, err)
		require.NotNil(t, resp3)
		require.Nil(t, resp3.ApplicationError)
		require.NotNil(t, resp3.Payload)
		require.NotNil(t, resp3.Payload.Namespace)
		require.Equal(t, "test_namespace", resp3.Payload.Namespace.Name)
		require.Equal(t, "test description", resp3.Payload.Namespace.Description)
	})

	t.Run("get namespace by name after update", func(t *testing.T) {
		core := newNamespacesCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespaceId := &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: rand.Uint32(),
		}

		// Create namespace
		_ = createNamespace(t, core, namespaceId, "test_namespace", now)

		// Update namespace
		updateTime := now.Add(time.Hour)
		resp2, err := core.UpdateNamespace(&coreapis.UpdateNamespaceRequest{
			Payload: &corepb.UpdateNamespaceRequest{
				NamespaceId: namespaceId,
				Description: "updated description",
				Now:         updateTime.UnixNano(),
			},
		})

		require.NoError(t, err)
		require.NotNil(t, resp2)
		require.Nil(t, resp2.ApplicationError)
		require.NotNil(t, resp2.Payload)
		require.NotNil(t, resp2.Payload.Namespace)

		// Get namespace by name and verify update
		resp3, err := core.GetNamespaceByName(&coreapis.GetNamespaceByNameRequest{
			Payload: &corepb.GetNamespaceByNameRequest{
				AccountId:     accountId,
				NamespaceName: "test_namespace",
			},
		})

		require.NoError(t, err)
		require.NotNil(t, resp3)
		require.Nil(t, resp3.ApplicationError)
		require.NotNil(t, resp3.Payload.Namespace)
		require.Equal(t, "test_namespace", resp3.Payload.Namespace.Name)
		require.Equal(t, "updated description", resp3.Payload.Namespace.Description)
		require.Equal(t, updateTime.UnixNano(), resp3.Payload.Namespace.UpdatedAt)
		require.Equal(t, now.UnixNano(), resp3.Payload.Namespace.CreatedAt)
	})

	t.Run("get multiple namespaces by name", func(t *testing.T) {
		core := newNamespacesCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespace1Id := &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: rand.Uint32(),
		}
		namespace2Id := &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: rand.Uint32(),
		}

		// Create multiple namespaces
		_ = createNamespace(t, core, namespace1Id, "namespace_1", now)
		_ = createNamespace(t, core, namespace2Id, "namespace_2", now)

		// Get first namespace by name
		resp3, err := core.GetNamespaceByName(&coreapis.GetNamespaceByNameRequest{
			Payload: &corepb.GetNamespaceByNameRequest{
				AccountId:     accountId,
				NamespaceName: "namespace_1",
			},
		})

		require.NoError(t, err)
		require.NotNil(t, resp3)
		require.Nil(t, resp3.ApplicationError)
		require.NotNil(t, resp3.Payload)
		require.NotNil(t, resp3.Payload.Namespace)
		require.Equal(t, "namespace_1", resp3.Payload.Namespace.Name)

		// Get second namespace by name
		resp4, err := core.GetNamespaceByName(&coreapis.GetNamespaceByNameRequest{
			Payload: &corepb.GetNamespaceByNameRequest{
				AccountId:     accountId,
				NamespaceName: "namespace_2",
			},
		})

		require.NoError(t, err)
		require.NotNil(t, resp4)
		require.Nil(t, resp4.ApplicationError)
		require.NotNil(t, resp4.Payload)
		require.NotNil(t, resp4.Payload.Namespace)
		require.Equal(t, "namespace_2", resp4.Payload.Namespace.Name)

		// Verify they are different namespaces
		require.NotEqual(t, resp3.Payload.Namespace.Id.NamespaceId, resp4.Payload.Namespace.Id.NamespaceId)
	})
}

func TestCore_ListNamespaces(t *testing.T) {
	core := newNamespacesCore(t)
	now := time.Now()
	accountId := rand.Uint64()
	namespace1Id := &corepb.NamespaceId{
		AccountId:   accountId,
		NamespaceId: rand.Uint32(),
	}
	namespace2Id := &corepb.NamespaceId{
		AccountId:   accountId,
		NamespaceId: rand.Uint32(),
	}

	// Create namespace 1
	_ = createNamespace(t, core, namespace1Id, "namespace_1", now)

	// Create namespace 2
	_ = createNamespace(t, core, namespace2Id, "namespace_2", now)

	// List namespaces
	resp3, err := core.ListNamespaces(&coreapis.ListNamespacesRequest{
		Payload: &corepb.ListNamespacesRequest{
			AccountId: accountId,
		},
	})

	require.NoError(t, err)
	require.NotNil(t, resp3)
	require.Nil(t, resp3.ApplicationError)
	require.NotNil(t, resp3.Payload)
	require.Len(t, resp3.Payload.Namespaces, 2)
}

func TestCore_UpdateNamespace(t *testing.T) {
	t.Run("update existing namespace", func(t *testing.T) {
		core := newNamespacesCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespaceId := &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: rand.Uint32(),
		}

		// Create a namespace first
		_ = createNamespace(t, core, namespaceId, "namespace_1", now)

		// Update the namespace
		updateTime := time.Now().Add(time.Hour)
		resp2, err := core.UpdateNamespace(&coreapis.UpdateNamespaceRequest{
			Payload: &corepb.UpdateNamespaceRequest{
				NamespaceId: namespaceId,
				Description: "updated description",
				Now:         updateTime.UnixNano(),
			},
		})

		require.NoError(t, err)
		require.NotNil(t, resp2)
		require.Nil(t, resp2.ApplicationError)
		require.NotNil(t, resp2.Payload)
		require.NotNil(t, resp2.Payload.Namespace)
		require.Equal(t, "updated description", resp2.Payload.Namespace.Description)
		require.Equal(t, updateTime.UnixNano(), resp2.Payload.Namespace.UpdatedAt)
		require.Equal(t, now.UnixNano(), resp2.Payload.Namespace.CreatedAt)

		// Verify the update by getting the namespace
		resp3, err := core.GetNamespace(&coreapis.GetNamespaceRequest{
			Payload: &corepb.GetNamespaceRequest{
				NamespaceId: resp2.Payload.Namespace.Id,
			},
		})

		require.NoError(t, err)
		require.NotNil(t, resp3)
		require.Nil(t, resp3.ApplicationError)
		require.NotNil(t, resp3.Payload)
		require.NotNil(t, resp3.Payload.Namespace)
		require.Equal(t, "updated description", resp3.Payload.Namespace.Description)
		require.Equal(t, updateTime.UnixNano(), resp3.Payload.Namespace.UpdatedAt)
	})

	t.Run("update nonexistent namespace", func(t *testing.T) {
		core := newNamespacesCore(t)
		now := time.Now()

		resp1, err := core.UpdateNamespace(&coreapis.UpdateNamespaceRequest{
			Payload: &corepb.UpdateNamespaceRequest{
				NamespaceId: &corepb.NamespaceId{
					AccountId:   rand.Uint64(),
					NamespaceId: rand.Uint32(),
				},
				Description: "updated description",
				Now:         now.UnixNano(),
			},
		})

		require.NoError(t, err)
		require.NotNil(t, resp1)
		require.Nil(t, resp1.Payload)
		require.NotNil(t, resp1.ApplicationError)
		require.Equal(t, monsterax.NotFound, resp1.ApplicationError.Code)
	})
}

func TestCore_DeleteNamespace(t *testing.T) {
	t.Run("delete existing namespace", func(t *testing.T) {
		core := newNamespacesCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespaceId := &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: rand.Uint32(),
		}

		// Create a namespace first
		_ = createNamespace(t, core, namespaceId, "test_namespace", now)

		// Verify the namespace exists
		resp2, err := core.GetNamespace(&coreapis.GetNamespaceRequest{
			Payload: &corepb.GetNamespaceRequest{
				NamespaceId: namespaceId,
			},
		})

		require.NoError(t, err)
		require.NotNil(t, resp2)
		require.Nil(t, resp2.ApplicationError)
		require.NotNil(t, resp2.Payload)
		require.NotNil(t, resp2.Payload.Namespace)

		// Delete the namespace
		resp3, err := core.DeleteNamespace(&coreapis.DeleteNamespaceRequest{
			Payload: &corepb.DeleteNamespaceRequest{
				NamespaceId: namespaceId,
			},
		})

		require.NoError(t, err)
		require.NotNil(t, resp3)
		require.Nil(t, resp3.ApplicationError)
		require.NotNil(t, resp3.Payload)

		// Verify the namespace no longer exists
		resp4, err := core.GetNamespace(&coreapis.GetNamespaceRequest{
			Payload: &corepb.GetNamespaceRequest{
				NamespaceId: namespaceId,
			},
		})

		require.NoError(t, err)
		require.NotNil(t, resp4)
		require.Nil(t, resp4.Payload)
		require.NotNil(t, resp4.ApplicationError)
		require.Equal(t, monsterax.NotFound, resp4.ApplicationError.Code)
	})

	t.Run("delete nonexistent namespace", func(t *testing.T) {
		core := newNamespacesCore(t)

		resp1, err := core.DeleteNamespace(&coreapis.DeleteNamespaceRequest{
			Payload: &corepb.DeleteNamespaceRequest{
				NamespaceId: &corepb.NamespaceId{
					AccountId:   rand.Uint64(),
					NamespaceId: rand.Uint32(),
				},
			},
		})

		require.NoError(t, err)
		require.NotNil(t, resp1)
		require.Nil(t, resp1.ApplicationError)
		require.NotNil(t, resp1.Payload)
	})

	t.Run("delete with multiple namespaces", func(t *testing.T) {
		core := newNamespacesCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespace1Id := &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: rand.Uint32(),
		}
		namespace2Id := &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: rand.Uint32(),
		}

		// Create multiple namespaces
		_ = createNamespace(t, core, namespace1Id, "test_namespace_1", now)
		_ = createNamespace(t, core, namespace2Id, "test_namespace_2", now)

		// Verify both namespaces exist
		resp3, err := core.ListNamespaces(&coreapis.ListNamespacesRequest{
			Payload: &corepb.ListNamespacesRequest{
				AccountId: accountId,
			},
		})

		require.NoError(t, err)
		require.NotNil(t, resp3)
		require.Nil(t, resp3.ApplicationError)
		require.NotNil(t, resp3.Payload)
		require.Len(t, resp3.Payload.Namespaces, 2)

		// Delete the first namespace
		resp4, err := core.DeleteNamespace(&coreapis.DeleteNamespaceRequest{
			Payload: &corepb.DeleteNamespaceRequest{
				NamespaceId: namespace1Id,
			},
		})

		require.NoError(t, err)
		require.NotNil(t, resp4)
		require.Nil(t, resp4.ApplicationError)
		require.NotNil(t, resp4.Payload)

		// Verify only the second namespace remains
		resp5, err := core.ListNamespaces(&coreapis.ListNamespacesRequest{
			Payload: &corepb.ListNamespacesRequest{
				AccountId: accountId,
			},
		})

		require.NoError(t, err)
		require.NotNil(t, resp5)
		require.Nil(t, resp5.ApplicationError)
		require.NotNil(t, resp5.Payload)
		require.Len(t, resp5.Payload.Namespaces, 1)
		require.Equal(t, "test_namespace_2", resp5.Payload.Namespaces[0].Name)

		// Verify the first namespace no longer exists
		resp6, err := core.GetNamespace(&coreapis.GetNamespaceRequest{
			Payload: &corepb.GetNamespaceRequest{
				NamespaceId: namespace1Id,
			},
		})

		require.NoError(t, err)
		require.NotNil(t, resp6)
		require.Nil(t, resp6.Payload)
		require.NotNil(t, resp6.ApplicationError)
		require.Equal(t, monsterax.NotFound, resp6.ApplicationError.Code)

		// Verify the second namespace still exists
		resp7, err := core.GetNamespace(&coreapis.GetNamespaceRequest{
			Payload: &corepb.GetNamespaceRequest{
				NamespaceId: namespace2Id,
			},
		})

		require.NoError(t, err)
		require.NotNil(t, resp7)
		require.Nil(t, resp7.ApplicationError)
		require.NotNil(t, resp7.Payload)
		require.NotNil(t, resp7.Payload.Namespace)
		require.Equal(t, "test_namespace_2", resp7.Payload.Namespace.Name)
	})
}

func TestCore_SnapshotAndRestore(t *testing.T) {
	t.Run("snapshot and restore namespaces", func(t *testing.T) {
		now := time.Now()
		accountId := rand.Uint64()
		namespace1Id := &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: rand.Uint32(),
		}
		namespace2Id := &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: rand.Uint32(),
		}
		namespace3Id := &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: rand.Uint32(),
		}

		// Create two cores for testing snapshot and restore
		core1 := newNamespacesCore(t)
		core2 := newNamespacesCore(t)

		// Create multiple namespaces in core1
		_ = createNamespace(t, core1, namespace1Id, "test_namespace_1", now)
		_ = createNamespace(t, core1, namespace2Id, "test_namespace_2", now)

		// Take snapshot
		snapshot := core1.Snapshot()

		// Update a namespace after snapshot
		updateTime := now.Add(time.Hour)
		resp3, err := core1.UpdateNamespace(&coreapis.UpdateNamespaceRequest{
			Payload: &corepb.UpdateNamespaceRequest{
				NamespaceId: namespace1Id,
				Description: "updated description",
				Now:         updateTime.UnixNano(),
			},
		})
		require.NoError(t, err)
		require.NotNil(t, resp3)
		require.Nil(t, resp3.ApplicationError)
		require.NotNil(t, resp3.Payload)
		require.NotNil(t, resp3.Payload.Namespace)

		// Create another namespace after snapshot
		_ = createNamespace(t, core1, namespace3Id, "test_namespace_3", now.Add(2*time.Hour))

		// Write snapshot to buffer
		buf := &bytes.Buffer{}
		err = snapshot.Write(buf)
		require.NoError(t, err)

		// Restore snapshot to second core
		err = core2.Restore(io.NopCloser(buf))
		require.NoError(t, err)

		// Verify restored state matches snapshot (before updates)
		// Should have 2 namespaces (the third was created after snapshot)
		resp5, err := core2.ListNamespaces(&coreapis.ListNamespacesRequest{
			Payload: &corepb.ListNamespacesRequest{
				AccountId: accountId,
			},
		})
		require.NoError(t, err)
		require.NotNil(t, resp5)
		require.Nil(t, resp5.ApplicationError)
		require.NotNil(t, resp5.Payload)
		require.Len(t, resp5.Payload.Namespaces, 2)

		// Verify first namespace has original description (not updated)
		resp6, err := core2.GetNamespace(&coreapis.GetNamespaceRequest{
			Payload: &corepb.GetNamespaceRequest{
				NamespaceId: namespace1Id,
			},
		})
		require.NoError(t, err)
		require.NotNil(t, resp6)
		require.Nil(t, resp6.ApplicationError)
		require.NotNil(t, resp6.Payload)
		require.NotNil(t, resp6.Payload.Namespace)
		require.Equal(t, now.UnixNano(), resp6.Payload.Namespace.UpdatedAt)

		// Verify second namespace exists
		resp7, err := core2.GetNamespace(&coreapis.GetNamespaceRequest{
			Payload: &corepb.GetNamespaceRequest{
				NamespaceId: namespace2Id,
			},
		})
		require.NoError(t, err)
		require.NotNil(t, resp7)
		require.Nil(t, resp7.ApplicationError)
		require.NotNil(t, resp7.Payload)
		require.NotNil(t, resp7.Payload.Namespace)

		// Verify third namespace doesn't exist in restored state
		resp8, err := core2.GetNamespaceByName(&coreapis.GetNamespaceByNameRequest{
			Payload: &corepb.GetNamespaceByNameRequest{
				AccountId:     accountId,
				NamespaceName: "test_namespace_3",
			},
		})
		require.NoError(t, err)
		require.NotNil(t, resp8)
		require.Nil(t, resp8.Payload)
		require.NotNil(t, resp8.ApplicationError)
		require.Equal(t, monsterax.NotFound, resp8.ApplicationError.Code)

		// Verify name index works correctly in restored state
		resp9, err := core2.GetNamespaceByName(&coreapis.GetNamespaceByNameRequest{
			Payload: &corepb.GetNamespaceByNameRequest{
				AccountId:     accountId,
				NamespaceName: "test_namespace_1",
			},
		})
		require.NoError(t, err)
		require.NotNil(t, resp9)
		require.Nil(t, resp9.ApplicationError)
		require.NotNil(t, resp9.Payload)
		require.NotNil(t, resp9.Payload.Namespace)
		require.Equal(t, "test_namespace_1", resp9.Payload.Namespace.Name)
	})

	t.Run("snapshot and restore with multiple accounts", func(t *testing.T) {
		now := time.Now()
		accountId1 := rand.Uint64()
		accountId2 := rand.Uint64()
		namespace1Id := &corepb.NamespaceId{AccountId: accountId1, NamespaceId: rand.Uint32()}
		namespace2Id := &corepb.NamespaceId{AccountId: accountId2, NamespaceId: rand.Uint32()}

		// Create two cores for testing snapshot and restore
		core1 := newNamespacesCore(t)
		core2 := newNamespacesCore(t)

		// Create namespaces for account 1
		_ = createNamespace(t, core1, namespace1Id, "account1_namespace", now)

		// Create namespaces for account 2
		_ = createNamespace(t, core1, namespace2Id, "account2_namespace", now)

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
		resp3, err := core2.GetNamespace(&coreapis.GetNamespaceRequest{
			Payload: &corepb.GetNamespaceRequest{
				NamespaceId: namespace1Id,
			},
		})
		require.NoError(t, err)
		require.NotNil(t, resp3)
		require.Nil(t, resp3.ApplicationError)
		require.NotNil(t, resp3.Payload)
		require.NotNil(t, resp3.Payload.Namespace)
		require.Equal(t, "account1_namespace", resp3.Payload.Namespace.Name)

		resp4, err := core2.GetNamespace(&coreapis.GetNamespaceRequest{
			Payload: &corepb.GetNamespaceRequest{
				NamespaceId: namespace2Id,
			},
		})
		require.NoError(t, err)
		require.NotNil(t, resp4)
		require.Nil(t, resp4.ApplicationError)
		require.NotNil(t, resp4.Payload)
		require.NotNil(t, resp4.Payload.Namespace)
		require.Equal(t, "account2_namespace", resp4.Payload.Namespace.Name)

		// Verify account isolation is maintained
		resp5, err := core2.ListNamespaces(&coreapis.ListNamespacesRequest{
			Payload: &corepb.ListNamespacesRequest{
				AccountId: accountId1,
			},
		})
		require.NoError(t, err)
		require.NotNil(t, resp5)
		require.Nil(t, resp5.ApplicationError)
		require.NotNil(t, resp5.Payload)
		require.Len(t, resp5.Payload.Namespaces, 1)

		resp6, err := core2.ListNamespaces(&coreapis.ListNamespacesRequest{
			Payload: &corepb.ListNamespacesRequest{
				AccountId: accountId2,
			},
		})
		require.NoError(t, err)
		require.NotNil(t, resp6)
		require.Nil(t, resp6.ApplicationError)
		require.NotNil(t, resp6.Payload)
		require.Len(t, resp6.Payload.Namespaces, 1)
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
		resp1, err := core2.ListNamespaces(&coreapis.ListNamespacesRequest{
			Payload: &corepb.ListNamespacesRequest{
				AccountId: rand.Uint64(),
			},
		})
		require.NoError(t, err)
		require.NotNil(t, resp1)
		require.Nil(t, resp1.ApplicationError)
		require.NotNil(t, resp1.Payload)
		require.Len(t, resp1.Payload.Namespaces, 0)
	})

	t.Run("restore and continue operations", func(t *testing.T) {
		now := time.Now()
		accountId := rand.Uint64()
		namespace1Id := &corepb.NamespaceId{AccountId: accountId, NamespaceId: rand.Uint32()}
		namespace2Id := &corepb.NamespaceId{AccountId: accountId, NamespaceId: rand.Uint32()}

		// Create two cores for testing snapshot and restore
		core1 := newNamespacesCore(t)
		core2 := newNamespacesCore(t)

		// Create namespace in core1
		_ = createNamespace(t, core1, namespace1Id, "test_namespace", now)

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
		resp2, err := core2.UpdateNamespace(&coreapis.UpdateNamespaceRequest{
			Payload: &corepb.UpdateNamespaceRequest{
				NamespaceId: namespace1Id,
				Description: "updated after restore",
				Now:         updateTime.UnixNano(),
			},
		})
		require.NoError(t, err)
		require.NotNil(t, resp2)
		require.Nil(t, resp2.ApplicationError)
		require.NotNil(t, resp2.Payload)
		require.NotNil(t, resp2.Payload.Namespace)
		require.Equal(t, "updated after restore", resp2.Payload.Namespace.Description)

		// Create new namespace in restored core
		_ = createNamespace(t, core2, namespace2Id, "new_namespace", now)

		// Verify both namespaces exist in restored core
		resp4, err := core2.ListNamespaces(&coreapis.ListNamespacesRequest{
			Payload: &corepb.ListNamespacesRequest{
				AccountId: accountId,
			},
		})
		require.NoError(t, err)
		require.NotNil(t, resp4)
		require.Nil(t, resp4.ApplicationError)
		require.NotNil(t, resp4.Payload)
		require.Len(t, resp4.Payload.Namespaces, 2)
	})
}

func newNamespacesCore(t *testing.T) *Core {
	store, err := store.NewBadgerInMemoryStore()
	require.NoError(t, err)
	return NewCore(store, []byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})
}

func createNamespace(t *testing.T, core *Core, namespaceId *corepb.NamespaceId, name string, now time.Time) *corepb.Namespace {
	t.Helper()

	resp, err := core.CreateNamespace(&coreapis.CreateNamespaceRequest{
		Payload: &corepb.CreateNamespaceRequest{
			NamespaceId:           namespaceId,
			Name:                  name,
			Description:           "test description",
			Now:                   now.UnixNano(),
			MaxNumberOfNamespaces: 20,
		},
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Nil(t, resp.ApplicationError)
	require.NotNil(t, resp.Payload)
	require.NotNil(t, resp.Payload.Namespace)

	return resp.Payload.Namespace
}
