package namespaces

import (
	"errors"
	"fmt"
	"io"

	"github.com/evrblk/monstera"
	"github.com/evrblk/monstera/store"
	monsterax "github.com/evrblk/monstera/x"

	"github.com/evrblk/grackle/pkg/corepb"
	"github.com/evrblk/grackle/pkg/ids"
	"github.com/evrblk/grackle/pkg/monsteragen"
	"github.com/evrblk/grackle/pkg/pagination"
)

type Core struct {
	badgerStore *store.BadgerStore

	namespaces *namespacesTable
	counters   *countersTable
}

var _ monsteragen.GrackleNamespacesCoreApi = &Core{}

func NewCore(badgerStore *store.BadgerStore, shardLowerBound []byte, shardUpperBound []byte) *Core {
	return &Core{
		badgerStore: badgerStore,

		namespaces: newNamespacesTable(shardLowerBound, shardUpperBound),
		counters:   newCountersTable(shardLowerBound, shardUpperBound),
	}
}

func (c *Core) ranges() []monsterax.KeyRange {
	ranges := []monsterax.KeyRange{
		c.counters.GetTableKeyRange(),
	}
	ranges = append(ranges, c.namespaces.GetTableKeyRanges()...)
	return ranges
}

func (c *Core) Snapshot() monstera.ApplicationCoreSnapshot {
	return monsterax.Snapshot(c.badgerStore, c.ranges())
}

func (c *Core) Restore(reader io.ReadCloser) error {
	return monsterax.Restore(c.badgerStore, c.ranges(), reader)
}

func (c *Core) Close() {

}

func (c *Core) CreateNamespace(request *corepb.CreateNamespaceRequest) (*corepb.CreateNamespaceResponse, error) {
	// Validations
	if request.Name == "" {
		return nil, monsterax.NewErrorWithContext(
			monsterax.InvalidArgument,
			"Name should not be empty",
			map[string]string{})
	}

	txn := c.badgerStore.Update()
	defer txn.Discard()

	// Get counters for that account
	counters, err := c.counters.Get(txn, request.NamespaceId.AccountId)
	panicIfNotNil(err)

	// Checking name uniqueness
	_, err = c.namespaces.GetByName(txn, request.NamespaceId.AccountId, request.Name)
	if err != nil {
		if !errors.Is(err, store.ErrNotFound) {
			return nil, err
		}
	} else {
		return nil, monsterax.NewErrorWithContext(
			monsterax.AlreadyExists,
			"namespace with this name already exists",
			map[string]string{"namespace_name": request.Name})
	}

	// Checking max number of namespaces
	if counters.NumberOfNamespaces >= request.MaxNumberOfNamespaces {
		return nil, monsterax.NewErrorWithContext(
			monsterax.ResourceExhausted,
			"max number of namespaces reached",
			map[string]string{"limit": fmt.Sprintf("%d", request.MaxNumberOfNamespaces)})
	}

	namespace := &corepb.Namespace{
		Id:          request.NamespaceId,
		Name:        request.Name,
		Description: request.Description,
		CreatedAt:   request.Now,
		UpdatedAt:   request.Now,
	}

	err = c.namespaces.Create(txn, namespace)
	panicIfNotNil(err)

	// Update counters
	counters.NumberOfNamespaces = counters.NumberOfNamespaces + 1
	err = c.counters.Set(txn, request.NamespaceId.AccountId, counters)
	panicIfNotNil(err)

	err = txn.Commit()
	panicIfNotNil(err)

	return &corepb.CreateNamespaceResponse{
		Namespace: namespace,
	}, nil
}

func (c *Core) UpdateNamespace(request *corepb.UpdateNamespaceRequest) (*corepb.UpdateNamespaceResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	namespace, err := c.namespaces.Get(txn, request.NamespaceId)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, monsterax.NewErrorWithContext(
				monsterax.NotFound,
				"namespace not found",
				map[string]string{"namespace_id": ids.EncodeNamespaceId(request.NamespaceId)})
		}

		panic(err)
	}

	namespace.Description = request.Description
	namespace.UpdatedAt = request.Now

	err = c.namespaces.Update(txn, namespace)
	panicIfNotNil(err)

	err = txn.Commit()
	panicIfNotNil(err)

	return &corepb.UpdateNamespaceResponse{
		Namespace: namespace,
	}, nil
}

func (c *Core) DeleteNamespace(request *corepb.DeleteNamespaceRequest) (*corepb.DeleteNamespaceResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	namespace, err := c.namespaces.Get(txn, request.NamespaceId)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return &corepb.DeleteNamespaceResponse{}, nil
		}

		panic(err)
	}

	// Get counters for that account
	counters, err := c.counters.Get(txn, request.NamespaceId.AccountId)
	panicIfNotNil(err)

	err = c.namespaces.Delete(txn, namespace)
	panicIfNotNil(err)

	// Update counters
	counters.NumberOfNamespaces = counters.NumberOfNamespaces - 1
	err = c.counters.Set(txn, request.NamespaceId.AccountId, counters)
	panicIfNotNil(err)

	err = txn.Commit()
	panicIfNotNil(err)

	return &corepb.DeleteNamespaceResponse{}, nil
}

func (c *Core) GetNamespace(request *corepb.GetNamespaceRequest) (*corepb.GetNamespaceResponse, error) {
	txn := c.badgerStore.View()
	defer txn.Discard()

	namespace, err := c.namespaces.Get(txn, request.NamespaceId)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, monsterax.NewErrorWithContext(
				monsterax.NotFound,
				"namespace not found",
				map[string]string{"namespace_id": ids.EncodeNamespaceId(request.NamespaceId)})
		}

		panic(err)
	}

	return &corepb.GetNamespaceResponse{
		Namespace: namespace,
	}, nil
}

func (c *Core) GetNamespaceByName(request *corepb.GetNamespaceByNameRequest) (*corepb.GetNamespaceByNameResponse, error) {
	txn := c.badgerStore.View()
	defer txn.Discard()

	namespace, err := c.namespaces.GetByName(txn, request.AccountId, request.NamespaceName)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, monsterax.NewErrorWithContext(
				monsterax.NotFound,
				"namespace not found",
				map[string]string{"namespace_name": request.NamespaceName})
		}

		panic(err)
	}

	return &corepb.GetNamespaceByNameResponse{
		Namespace: namespace,
	}, nil
}

func (c *Core) ListNamespaces(request *corepb.ListNamespacesRequest) (*corepb.ListNamespacesResponse, error) {
	txn := c.badgerStore.View()
	defer txn.Discard()

	result, err := c.namespaces.List(txn, request.AccountId, request.PaginationToken, pagination.GetLimitWithDefaults(int(request.Limit)))
	panicIfNotNil(err)

	return &corepb.ListNamespacesResponse{
		Namespaces:              result.Namespaces,
		NextPaginationToken:     result.NextPaginationToken,
		PreviousPaginationToken: result.PreviousPaginationToken,
	}, nil
}

func panicIfNotNil(err error) {
	if err != nil {
		panic(err)
	}
}
