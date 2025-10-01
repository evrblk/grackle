package grackle

import (
	"fmt"
	"io"

	"github.com/go-errors/errors"

	"github.com/evrblk/grackle/pkg/corepb"
	"github.com/evrblk/monstera"
	monsterax "github.com/evrblk/monstera/x"
)

type NamespacesCore struct {
	badgerStore *monstera.BadgerStore

	namespacesTable         *monsterax.CompositeKeyTable[*corepb.Namespace, corepb.Namespace]
	namespacesCountersTable *monsterax.SimpleKeyTable[*corepb.NamespacesCounter, corepb.NamespacesCounter]
}

var _ GrackleNamespacesCoreApi = &NamespacesCore{}

func NewNamespacesCore(badgerStore *monstera.BadgerStore, shardGlobalIndexPrefix []byte, shardLowerBound []byte, shardUpperBound []byte) *NamespacesCore {
	return &NamespacesCore{
		badgerStore: badgerStore,

		namespacesTable:         monsterax.NewCompositeKeyTable[*corepb.Namespace, corepb.Namespace](GrackleNamespacesTableId, shardLowerBound, shardUpperBound),
		namespacesCountersTable: monsterax.NewSimpleKeyTable[*corepb.NamespacesCounter, corepb.NamespacesCounter](GrackleNamespacesCountersTableId, shardLowerBound, shardUpperBound),
	}
}

func (c *NamespacesCore) ranges() []monstera.KeyRange {
	return []monstera.KeyRange{
		c.namespacesTable.GetTableKeyRange(),
		c.namespacesCountersTable.GetTableKeyRange(),
	}
}

func (c *NamespacesCore) Snapshot() monstera.ApplicationCoreSnapshot {
	return monsterax.Snapshot(c.badgerStore, c.ranges())
}

func (c *NamespacesCore) Restore(reader io.ReadCloser) error {
	return monsterax.Restore(c.badgerStore, c.ranges(), reader)
}

func (c *NamespacesCore) Close() {

}

func (c *NamespacesCore) CreateNamespace(request *corepb.CreateNamespaceRequest) (*corepb.CreateNamespaceResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	// Validations
	if request.Name == "" {
		return nil, monsterax.NewErrorWithContext(
			monsterax.InvalidArgument,
			"Name should not be empty",
			map[string]string{})
	}

	// Get counters for that account
	counters, err := c.getCounters(txn, request.AccountId)
	panicIfNotNil(err)

	namespaceId := &corepb.NamespaceId{
		AccountId:     request.AccountId,
		NamespaceName: request.Name,
	}

	// Checking name uniqueness
	_, err = c.getNamespace(txn, namespaceId)
	if err != nil {
		if !errors.Is(err, monstera.ErrNotFound) {
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
		Id:          namespaceId,
		Description: request.Description,
		CreatedAt:   request.Now,
		UpdatedAt:   request.Now,
	}

	err = c.createNamespace(txn, namespace)
	panicIfNotNil(err)

	// Update counters
	counters.NumberOfNamespaces = counters.NumberOfNamespaces + 1
	err = c.setCounters(txn, request.AccountId, counters)
	panicIfNotNil(err)

	err = txn.Commit()
	panicIfNotNil(err)

	return &corepb.CreateNamespaceResponse{
		Namespace: namespace,
	}, nil
}

func (c *NamespacesCore) UpdateNamespace(request *corepb.UpdateNamespaceRequest) (*corepb.UpdateNamespaceResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	namespace, err := c.getNamespace(txn, request.NamespaceId)
	if err != nil {
		if errors.Is(err, monstera.ErrNotFound) {
			return nil, monsterax.NewErrorWithContext(
				monsterax.NotFound,
				"namespace not found",
				map[string]string{"namespace_name": request.NamespaceId.NamespaceName})
		} else {
			panic(err)
		}
	}

	namespace.Description = request.Description
	namespace.UpdatedAt = request.Now

	err = c.updateNamespace(txn, namespace)
	panicIfNotNil(err)

	err = txn.Commit()
	panicIfNotNil(err)

	return &corepb.UpdateNamespaceResponse{
		Namespace: namespace,
	}, nil
}

// TODO should delete non-existent namespace gracefully (return NotFound error)?
func (c *NamespacesCore) DeleteNamespace(request *corepb.DeleteNamespaceRequest) (*corepb.DeleteNamespaceResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	namespace, err := c.getNamespace(txn, request.NamespaceId)
	if err != nil {
		if errors.Is(err, monstera.ErrNotFound) {
			return &corepb.DeleteNamespaceResponse{}, nil
		} else {
			panic(err)
		}
	}

	// Get counters for that account
	counters, err := c.getCounters(txn, request.NamespaceId.AccountId)
	panicIfNotNil(err)

	err = c.deleteNamespace(txn, namespace)
	panicIfNotNil(err)

	// Update counters
	counters.NumberOfNamespaces = counters.NumberOfNamespaces - 1
	err = c.setCounters(txn, request.NamespaceId.AccountId, counters)
	panicIfNotNil(err)

	// TODO delete locks, semaphores, wgs

	err = txn.Commit()
	panicIfNotNil(err)

	return &corepb.DeleteNamespaceResponse{}, nil
}

func (c *NamespacesCore) GetNamespace(request *corepb.GetNamespaceRequest) (*corepb.GetNamespaceResponse, error) {
	txn := c.badgerStore.View()
	defer txn.Discard()

	namespace, err := c.getNamespace(txn, request.NamespaceId)
	if err != nil {
		if errors.Is(err, monstera.ErrNotFound) {
			return nil, monsterax.NewErrorWithContext(
				monsterax.NotFound,
				"namespace not found",
				map[string]string{"namespace_name": request.NamespaceId.NamespaceName})
		} else {
			panic(err)
		}
	}

	return &corepb.GetNamespaceResponse{
		Namespace: namespace,
	}, nil
}

func (c *NamespacesCore) ListNamespaces(request *corepb.ListNamespacesRequest) (*corepb.ListNamespacesResponse, error) {
	txn := c.badgerStore.View()
	defer txn.Discard()

	result, err := c.listNamespaces(txn, request.AccountId, request.PaginationToken, getLimit(int(request.Limit)))
	panicIfNotNil(err)

	return &corepb.ListNamespacesResponse{
		Namespaces:              result.namespaces,
		NextPaginationToken:     result.nextPaginationToken,
		PreviousPaginationToken: result.previousPaginationToken,
	}, nil
}

func (c *NamespacesCore) getNamespace(txn *monstera.Txn, namespaceId *corepb.NamespaceId) (*corepb.Namespace, error) {
	return c.namespacesTable.Get(txn, namespacesTablePK(namespaceId.AccountId), namespacesTableSK(namespaceId))
}

type listNamespacesResult struct {
	namespaces              []*corepb.Namespace
	nextPaginationToken     *corepb.PaginationToken
	previousPaginationToken *corepb.PaginationToken
}

func (c *NamespacesCore) listNamespaces(txn *monstera.Txn, accountId uint64, paginationToken *corepb.PaginationToken, limit int) (*listNamespacesResult, error) {
	result, err := c.namespacesTable.ListPaginated(txn, namespacesTablePK(accountId), paginationTokenToMonstera(paginationToken), limit)
	if err != nil {
		return nil, err
	}

	return &listNamespacesResult{
		namespaces:              result.Items,
		nextPaginationToken:     monsteraPaginationTokenToCore(result.NextPaginationToken),
		previousPaginationToken: monsteraPaginationTokenToCore(result.PreviousPaginationToken),
	}, nil
}

func (c *NamespacesCore) createNamespace(txn *monstera.Txn, namespace *corepb.Namespace) error {
	return c.namespacesTable.Set(txn, namespacesTablePK(namespace.Id.AccountId), namespacesTableSK(namespace.Id), namespace)
}

func (c *NamespacesCore) deleteNamespace(txn *monstera.Txn, namespace *corepb.Namespace) error {
	// Remove namespace from main namespacesTable
	return c.namespacesTable.Delete(txn, namespacesTablePK(namespace.Id.AccountId), namespacesTableSK(namespace.Id))
}

func (c *NamespacesCore) updateNamespace(txn *monstera.Txn, namespace *corepb.Namespace) error {
	return c.namespacesTable.Set(txn, namespacesTablePK(namespace.Id.AccountId), namespacesTableSK(namespace.Id), namespace)
}

func (c *NamespacesCore) getCounters(txn *monstera.Txn, accountId uint64) (*corepb.NamespacesCounter, error) {
	countres, err := c.namespacesCountersTable.Get(txn, namespacesCountersTablePK(accountId))
	if err != nil {
		if errors.Is(err, monstera.ErrNotFound) {
			return &corepb.NamespacesCounter{}, nil
		}
		return nil, err
	}
	return countres, nil
}

func (c *NamespacesCore) setCounters(txn *monstera.Txn, accountId uint64, counters *corepb.NamespacesCounter) error {
	return c.namespacesCountersTable.Set(txn, namespacesCountersTablePK(accountId), counters)
}

type namespaceIdIntf interface {
	GetAccountId() uint64
	GetNamespaceName() string
}

// 1. shard key (by account id)
// 2. account id
func namespacesTablePK(accountId uint64) []byte {
	return monstera.ConcatBytes(shardByAccount(accountId), accountId)
}

// 1. namespace name
func namespacesTableSK(n namespaceIdIntf) []byte {
	return monstera.ConcatBytes(n.GetNamespaceName())
}

// 1. shard key (by account id)
// 2. account id
func namespacesCountersTablePK(accountId uint64) []byte {
	return monstera.ConcatBytes(shardByAccount(accountId), accountId)
}

func panicIfNotNil(err error) {
	if err != nil {
		panic(err)
	}
}

func getLimit(requestedLimit int) int {
	maxLimit := 250
	defaultLimit := 100

	if requestedLimit > 0 && requestedLimit < maxLimit {
		return requestedLimit
	} else if requestedLimit <= 0 {
		return defaultLimit
	} else {
		return maxLimit
	}
}

func paginationTokenToMonstera(paginationToken *corepb.PaginationToken) *monsterax.PaginationToken {
	if paginationToken == nil {
		return nil
	}

	return &monsterax.PaginationToken{
		Key:     paginationToken.Value,
		Reverse: paginationToken.Type == corepb.PaginationToken_PREVIOUS,
	}
}

func monsteraPaginationTokenToCore(monsteraPaginationToken *monsterax.PaginationToken) *corepb.PaginationToken {
	if monsteraPaginationToken == nil {
		return nil
	}

	result := &corepb.PaginationToken{
		Value: monsteraPaginationToken.Key,
	}

	if monsteraPaginationToken.Reverse {
		result.Type = corepb.PaginationToken_PREVIOUS
	} else {
		result.Type = corepb.PaginationToken_NEXT
	}

	return result
}
