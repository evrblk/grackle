package integration_test

import (
	"context"
	"fmt"
	"testing"

	gracklepb "github.com/evrblk/evrblk-go/grackle/v1beta"
	"github.com/stretchr/testify/require"
)

func TestCreateNamespace(t *testing.T) {
	t.Run("validation", func(t *testing.T) {
		server := setupGrackleApiServer(t)
		ctx := context.Background()

		// Valid request
		resp, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
			Name:        "namespace1",
			Description: "Test namespace",
		})
		require.NoError(t, err)
		require.NotNil(t, resp.Namespace)

		// Invalid request - invalid namespace name
		_, err = server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
			Name:        "invalid@namespace",
			Description: "Test namespace",
		})
		require.Error(t, err)
	})
}

func TestGetNamespace(t *testing.T) {
	t.Run("validation", func(t *testing.T) {
		server := setupGrackleApiServer(t)
		ctx := context.Background()

		// Create namespace
		_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
			Name: "namespace1",
		})
		require.NoError(t, err)

		// Valid request
		resp, err := server.GetNamespace(ctx, &gracklepb.GetNamespaceRequest{
			NamespaceName: "namespace1",
		})
		require.NoError(t, err)
		require.NotNil(t, resp.Namespace)

		// Invalid request - invalid namespace name
		_, err = server.GetNamespace(ctx, &gracklepb.GetNamespaceRequest{
			NamespaceName: "invalid@namespace",
		})
		require.Error(t, err)
	})
}

func TestUpdateNamespace(t *testing.T) {
	t.Run("validation", func(t *testing.T) {
		server := setupGrackleApiServer(t)
		ctx := context.Background()

		// Create namespace
		_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
			Name: "namespace1",
		})
		require.NoError(t, err)

		// Valid request
		resp, err := server.UpdateNamespace(ctx, &gracklepb.UpdateNamespaceRequest{
			NamespaceName: "namespace1",
			Description:   "Updated description",
		})
		require.NoError(t, err)
		require.NotNil(t, resp.Namespace)

		// Invalid request - invalid namespace name
		_, err = server.UpdateNamespace(ctx, &gracklepb.UpdateNamespaceRequest{
			NamespaceName: "invalid@namespace",
			Description:   "Updated description",
		})
		require.Error(t, err)
	})
}

func TestDeleteNamespace(t *testing.T) {
	t.Run("validation", func(t *testing.T) {
		server := setupGrackleApiServer(t)
		ctx := context.Background()

		// Create namespace
		_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
			Name: "namespace1",
		})
		require.NoError(t, err)

		// Valid request
		_, err = server.DeleteNamespace(ctx, &gracklepb.DeleteNamespaceRequest{
			NamespaceName: "namespace1",
		})
		require.NoError(t, err)

		// Invalid request - invalid namespace name
		_, err = server.DeleteNamespace(ctx, &gracklepb.DeleteNamespaceRequest{
			NamespaceName: "invalid@namespace",
		})
		require.Error(t, err)
	})
}

func TestListNamespaces(t *testing.T) {
	t.Run("validation", func(t *testing.T) {
		server := setupGrackleApiServer(t)
		ctx := context.Background()

		// Valid request
		resp, err := server.ListNamespaces(ctx, &gracklepb.ListNamespacesRequest{})
		require.NoError(t, err)
		require.NotNil(t, resp.Namespaces)
	})

	t.Run("pagination", func(t *testing.T) {
		server := setupGrackleApiServer(t)
		ctx := context.Background()

		// Create 25 namespaces to test pagination (3 pages with limit 10)
		namespaceNames := make([]string, 25)
		for i := range 25 {
			namespaceNames[i] = fmt.Sprintf("namespace_%03d", i+1)
			_, err := server.CreateNamespace(ctx, &gracklepb.CreateNamespaceRequest{
				Name:        namespaceNames[i],
				Description: fmt.Sprintf("Test namespace %d", i+1),
			})
			require.NoError(t, err)
		}

		// Test forward pagination through 3 pages
		var allNamespaces []*gracklepb.Namespace

		// Page 1: Get first 10 namespaces
		resp1, err := server.ListNamespaces(ctx, &gracklepb.ListNamespacesRequest{
			Limit: 10,
		})
		require.NoError(t, err)
		require.Len(t, resp1.Namespaces, 10)
		require.NotEmpty(t, resp1.NextPaginationToken)
		require.Empty(t, resp1.PreviousPaginationToken) // First page has no previous token

		allNamespaces = append(allNamespaces, resp1.Namespaces...)

		// Page 2: Get next 10 namespaces
		resp2, err := server.ListNamespaces(ctx, &gracklepb.ListNamespacesRequest{
			PaginationToken: resp1.NextPaginationToken,
			Limit:           10,
		})
		require.NoError(t, err)
		require.Len(t, resp2.Namespaces, 10)
		require.NotEmpty(t, resp2.NextPaginationToken)
		require.NotEmpty(t, resp2.PreviousPaginationToken) // Middle page has both tokens

		allNamespaces = append(allNamespaces, resp2.Namespaces...)

		// Page 3: Get remaining 5 namespaces
		resp3, err := server.ListNamespaces(ctx, &gracklepb.ListNamespacesRequest{
			PaginationToken: resp2.NextPaginationToken,
			Limit:           10,
		})
		require.NoError(t, err)
		require.Len(t, resp3.Namespaces, 5)         // Should be 5 remaining namespaces
		require.Empty(t, resp3.NextPaginationToken) // Last page has no next token
		require.NotEmpty(t, resp3.PreviousPaginationToken)

		allNamespaces = append(allNamespaces, resp3.Namespaces...)

		// Verify we got all 25 namespaces
		require.Len(t, allNamespaces, 25)

		// Test backward pagination from the last page
		var backwardNamespaces = resp3.Namespaces

		// Go back to page 2
		resp4, err := server.ListNamespaces(ctx, &gracklepb.ListNamespacesRequest{
			PaginationToken: resp3.PreviousPaginationToken,
			Limit:           10,
		})
		require.NoError(t, err)
		require.Len(t, resp4.Namespaces, 10)
		require.NotEmpty(t, resp4.NextPaginationToken)
		require.NotEmpty(t, resp4.PreviousPaginationToken)

		backwardNamespaces = append(backwardNamespaces, resp4.Namespaces...)

		// Go back to page 1
		resp5, err := server.ListNamespaces(ctx, &gracklepb.ListNamespacesRequest{
			PaginationToken: resp4.PreviousPaginationToken,
			Limit:           10,
		})
		require.NoError(t, err)
		require.Len(t, resp5.Namespaces, 10)
		require.NotEmpty(t, resp5.NextPaginationToken)
		require.Empty(t, resp5.PreviousPaginationToken) // First page has no previous token

		backwardNamespaces = append(backwardNamespaces, resp5.Namespaces...)

		// Verify we got all 25 namespaces
		require.Len(t, backwardNamespaces, 25)

		// Test pagination with different limits
		resp6, err := server.ListNamespaces(ctx, &gracklepb.ListNamespacesRequest{
			Limit: 5,
		})
		require.NoError(t, err)
		require.Len(t, resp6.Namespaces, 5)
		require.NotEmpty(t, resp6.NextPaginationToken)

		// Test pagination with invalid token
		_, err = server.ListNamespaces(ctx, &gracklepb.ListNamespacesRequest{
			PaginationToken: "invalid_token",
			Limit:           10,
		})
		require.Error(t, err)

		// Test pagination with zero limit (should use default)
		resp7, err := server.ListNamespaces(ctx, &gracklepb.ListNamespacesRequest{
			Limit: 0,
		})
		require.NoError(t, err)
		require.NotEmpty(t, resp7.Namespaces)
		require.Len(t, resp7.Namespaces, 25)
		require.Empty(t, resp7.NextPaginationToken)

		// Test pagination with negative limit (should use default)
		_, err = server.ListNamespaces(ctx, &gracklepb.ListNamespacesRequest{
			Limit: -1,
		})
		require.Error(t, err)
	})
}
