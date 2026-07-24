package namespaces

import (
	"errors"
	"fmt"
	"io"

	"github.com/evrblk/monstera"
	mrpc "github.com/evrblk/monstera/rpc"
	"github.com/evrblk/monstera/store"

	"github.com/evrblk/grackle/pkg/coreapis"
	"github.com/evrblk/grackle/pkg/corepb"
	"github.com/evrblk/grackle/pkg/ids"
	"github.com/evrblk/grackle/pkg/pagination"
	"github.com/evrblk/grackle/pkg/tables"
)

// Core implements the per-shard namespaces state machine on top of a Badger store.
// It is the Monstera application core for the namespaces service and owns the
// namespaces, their name index, and the per-account namespace counters.
type Core struct {
	badgerStore *store.BadgerStore

	shardPrefix     []byte
	shardLowerBound []byte
	shardUpperBound []byte

	namespaces *namespacesTable
	counters   *countersTable
}

var _ coreapis.GrackleNamespacesCoreApi = &Core{}

// NewCore constructs a Core bound to a single shard of the namespaces keyspace.
// shardPrefix is a shard-unique prefix (derived from the shard id) nested
// under every table id; the lower/upper bounds delimit the shard's key range
// and drive the bounds-filtered portable Restore.
func NewCore(badgerStore *store.BadgerStore, shardPrefix []byte, shardLowerBound []byte, shardUpperBound []byte) *Core {
	return &Core{
		badgerStore: badgerStore,

		shardPrefix:     shardPrefix,
		shardLowerBound: shardLowerBound,
		shardUpperBound: shardUpperBound,

		namespaces: newNamespacesTable(shardPrefix),
		counters:   newCountersTable(shardPrefix),
	}
}

// Close releases any Core-owned resources. The underlying Badger store is
// shared across cores and is not closed here.
func (c *Core) Close() {

}

func (c *Core) snapshotSections() []tables.Section {
	return []tables.Section{
		{Name: "Grackle.NamespacesCore.Namespaces", Table: c.namespaces},
		{Name: "Grackle.NamespacesCore.Counters", Table: c.counters},
	}
}

// Snapshot returns a consistent, portable snapshot of this core's primary
// entities (a pinned view; Write streams from it concurrently with subsequent
// updates).
func (c *Core) Snapshot() monstera.ApplicationCoreSnapshot {
	return tables.NewSnapshot(c.badgerStore, "GrackleNamespaces", c.snapshotSections())
}

// Restore replaces this core's state with the union of the entities from the
// given streams that belong to this core's shard bounds (one stream for a
// Raft restore or split seed, two for a merge seed), inserting them through
// the tables' own methods (which rebuild all secondary indexes under this
// core's prefix).
func (c *Core) Restore(readers ...io.ReadCloser) error {
	return tables.RestoreSnapshot(c.badgerStore, c.snapshotSections(),
		tables.ShardRange{Lower: c.shardLowerBound, Upper: c.shardUpperBound}, readers...)
}

// CreateNamespace creates a new namespace and bumps the per-account namespace
// counter. Returns InvalidRequest if the name is empty, AlreadyExists if a
// namespace with the same name already exists in the account, ResourceExhausted
// if creating it would exceed MaxNumberOfNamespaces, or IDCollision if the
// randomly generated id is already taken (the caller regenerates the id and
// retries; IDCollision is never surfaced to clients).
func (c *Core) CreateNamespace(req *coreapis.CreateNamespaceRequest) (*coreapis.CreateNamespaceResponse, error) {
	if req.Payload.Name == "" {
		return &coreapis.CreateNamespaceResponse{
			ApplicationError: mrpc.NewErrorWithContext(
				mrpc.InvalidRequest,
				"Name should not be empty",
				map[string]string{}),
		}, nil
	}

	txn := c.badgerStore.Update()
	defer txn.Discard()

	// Get counters for that account
	counters, err := c.counters.Get(txn, req.Payload.NamespaceId.AccountId)
	if err != nil {
		return nil, err
	}

	// Checking name uniqueness
	_, err = c.namespaces.GetByName(txn, req.Payload.NamespaceId.AccountId, req.Payload.Name)
	if err != nil {
		if !errors.Is(err, store.ErrNotFound) {
			return nil, err
		}
	} else {
		return &coreapis.CreateNamespaceResponse{
			ApplicationError: mrpc.NewErrorWithContext(
				mrpc.AlreadyExists,
				"namespace with this name already exists",
				map[string]string{"namespace_name": req.Payload.Name}),
		}, nil
	}

	// Checking max number of namespaces
	if counters.NumberOfNamespaces >= req.Payload.MaxNumberOfNamespaces {
		return &coreapis.CreateNamespaceResponse{
			ApplicationError: mrpc.NewErrorWithContext(
				mrpc.ResourceExhausted,
				"max number of namespaces reached",
				map[string]string{"limit": fmt.Sprintf("%d", req.Payload.MaxNumberOfNamespaces)},
			),
		}, nil
	}

	// Checking ID uniqueness. The ID is randomly generated and passed to the core,
	// so a collision is expected to be rare; when it does happen we return IDCollision so
	// the caller can regenerate the ID and retry. This is not a user-facing error.
	// Without this check c.namespaces.Create would silently overwrite the colliding namespace.
	_, err = c.namespaces.Get(txn, req.Payload.NamespaceId)
	if err != nil {
		if !errors.Is(err, store.ErrNotFound) {
			return nil, err
		}
	} else {
		return &coreapis.CreateNamespaceResponse{
			ApplicationError: mrpc.NewErrorWithContext(
				mrpc.IDCollision,
				"namespace with this id already exists",
				map[string]string{"namespace_id": fmt.Sprintf("%d", req.Payload.NamespaceId.NamespaceId)}),
		}, nil
	}

	namespace := &corepb.Namespace{
		Id:          req.Payload.NamespaceId,
		Name:        req.Payload.Name,
		Description: req.Payload.Description,
		CreatedAt:   req.Now,
		UpdatedAt:   req.Now,
		Metadata:    req.Payload.Metadata,
		Version:     1,
	}

	err = c.namespaces.Create(txn, namespace)
	if err != nil {
		return nil, err
	}

	// Update counters
	counters.NumberOfNamespaces = counters.NumberOfNamespaces + 1
	err = c.counters.Set(txn, req.Payload.NamespaceId.AccountId, counters)
	if err != nil {
		return nil, err
	}

	err = txn.Commit()
	if err != nil {
		return nil, err
	}

	return &coreapis.CreateNamespaceResponse{
		Payload: &corepb.CreateNamespaceResponse{
			Namespace: namespace,
		},
	}, nil
}

// UpdateNamespace updates the description and metadata of an existing namespace
// and bumps its version. It uses optimistic concurrency: returns InvalidRequest
// on a version mismatch (ExpectedVersion != the stored version), or NotFound if
// the namespace does not exist. The namespace name is immutable.
func (c *Core) UpdateNamespace(req *coreapis.UpdateNamespaceRequest) (*coreapis.UpdateNamespaceResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	namespace, err := c.namespaces.GetByName(txn, req.Payload.AccountId, req.Payload.NamespaceName)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return &coreapis.UpdateNamespaceResponse{
				ApplicationError: mrpc.NewErrorWithContext(
					mrpc.NotFound,
					"namespace not found",
					map[string]string{"namespace_name": req.Payload.NamespaceName},
				),
			}, nil
		}

		return nil, err
	}

	if namespace.Version != req.Payload.ExpectedVersion {
		return &coreapis.UpdateNamespaceResponse{
			ApplicationError: mrpc.NewErrorWithContext(
				mrpc.InvalidRequest,
				"version mismatch",
				map[string]string{
					"namespace_name":   req.Payload.NamespaceName,
					"actual_version":   fmt.Sprintf("%d", namespace.Version),
					"expected_version": fmt.Sprintf("%d", req.Payload.ExpectedVersion),
				},
			),
		}, nil
	}

	namespace.Description = req.Payload.Description
	namespace.UpdatedAt = req.Now
	namespace.Metadata = req.Payload.Metadata
	namespace.Version += 1

	err = c.namespaces.Update(txn, namespace)
	if err != nil {
		return nil, err
	}

	err = txn.Commit()
	if err != nil {
		return nil, err
	}

	return &coreapis.UpdateNamespaceResponse{
		Payload: &corepb.UpdateNamespaceResponse{
			Namespace: namespace,
		},
	}, nil
}

// DeleteNamespace removes the named namespace and decrements the per-account
// namespace counter. Deleting a namespace that does not exist is a no-op and
// returns success. This deletes only the namespace row and its counter; the
// primitives (locks, semaphores, wait groups, barriers) living in the namespace
// are torn down separately by the front handler's cross-primitive fan-out.
func (c *Core) DeleteNamespace(req *coreapis.DeleteNamespaceRequest) (*coreapis.DeleteNamespaceResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	namespace, err := c.namespaces.GetByName(txn, req.Payload.AccountId, req.Payload.NamespaceName)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return &coreapis.DeleteNamespaceResponse{
				Payload: &corepb.DeleteNamespaceResponse{},
			}, nil
		}

		return nil, err
	}

	// Get counters for that account
	counters, err := c.counters.Get(txn, namespace.Id.AccountId)
	if err != nil {
		return nil, err
	}

	err = c.namespaces.Delete(txn, namespace)
	if err != nil {
		return nil, err
	}

	// Update counters
	counters.NumberOfNamespaces = counters.NumberOfNamespaces - 1
	err = c.counters.Set(txn, namespace.Id.AccountId, counters)
	if err != nil {
		return nil, err
	}

	err = txn.Commit()
	if err != nil {
		return nil, err
	}

	return &coreapis.DeleteNamespaceResponse{
		Payload: &corepb.DeleteNamespaceResponse{},
	}, nil
}

// GetNamespace looks up a namespace by its full NamespaceId. Returns a NotFound
// application error if no namespace with that id exists.
func (c *Core) GetNamespace(req *coreapis.GetNamespaceRequest) (*coreapis.GetNamespaceResponse, error) {
	txn := c.badgerStore.View()
	defer txn.Discard()

	namespace, err := c.namespaces.Get(txn, req.Payload.NamespaceId)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return &coreapis.GetNamespaceResponse{
				ApplicationError: mrpc.NewErrorWithContext(
					mrpc.NotFound,
					"namespace not found",
					map[string]string{"namespace_id": ids.EncodeNamespaceId(req.Payload.NamespaceId)},
				),
			}, nil
		}

		return nil, err
	}

	return &coreapis.GetNamespaceResponse{
		Payload: &corepb.GetNamespaceResponse{
			Namespace: namespace,
		},
	}, nil
}

// GetNamespaceByName looks up a namespace by its (account, name) pair. Returns a
// NotFound application error if no namespace with that name exists in the account.
func (c *Core) GetNamespaceByName(req *coreapis.GetNamespaceByNameRequest) (*coreapis.GetNamespaceByNameResponse, error) {
	txn := c.badgerStore.View()
	defer txn.Discard()

	namespace, err := c.namespaces.GetByName(txn, req.Payload.AccountId, req.Payload.NamespaceName)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return &coreapis.GetNamespaceByNameResponse{
				ApplicationError: mrpc.NewErrorWithContext(
					mrpc.NotFound,
					"namespace not found",
					map[string]string{"namespace_name": req.Payload.NamespaceName},
				),
			}, nil
		}

		return nil, err
	}

	return &coreapis.GetNamespaceByNameResponse{
		Payload: &corepb.GetNamespaceByNameResponse{
			Namespace: namespace,
		},
	}, nil
}

// ListNamespaces returns a page of namespaces in the given account, ordered by
// name. Use the returned NextPaginationToken / PreviousPaginationToken to walk
// forward or backward through pages.
func (c *Core) ListNamespaces(req *coreapis.ListNamespacesRequest) (*coreapis.ListNamespacesResponse, error) {
	txn := c.badgerStore.View()
	defer txn.Discard()

	result, err := c.namespaces.List(txn, req.Payload.AccountId, req.Payload.PaginationToken, pagination.GetLimitWithDefaults(int(req.Payload.Limit)))
	if err != nil {
		return nil, err
	}

	return &coreapis.ListNamespacesResponse{
		Payload: &corepb.ListNamespacesResponse{
			Namespaces:              result.Namespaces,
			NextPaginationToken:     result.NextPaginationToken,
			PreviousPaginationToken: result.PreviousPaginationToken,
		},
	}, nil
}
