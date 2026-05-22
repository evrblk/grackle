package barriers

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

	barriers          *barriersTable
	participants      *participantsTable
	counters          *countersTable
	gcRecords         *gcRecordsTable
	expirationRecords *expirationRecordsTable
}

var _ monsteragen.GrackleBarriersCoreApi = &Core{}

func NewCore(badgerStore *store.BadgerStore, globalIndexPrefix []byte, shardLowerBound []byte, shardUpperBound []byte) *Core {
	return &Core{
		badgerStore: badgerStore,

		barriers:          newBarriersTable(shardLowerBound, shardUpperBound),
		participants:      newParticipantsTable(shardLowerBound, shardUpperBound),
		counters:          newCountersTable(shardLowerBound, shardUpperBound),
		gcRecords:         newGCRecordsTable(globalIndexPrefix),
		expirationRecords: newExpirationRecordsTable(globalIndexPrefix),
	}
}

func (c *Core) ranges() []monsterax.KeyRange {
	ranges := []monsterax.KeyRange{
		c.counters.GetTableKeyRange(),
		c.gcRecords.GetTableKeyRange(),
		c.expirationRecords.GetTableKeyRange(),
		c.participants.GetTableKeyRange(),
	}

	ranges = append(ranges, c.barriers.GetTableKeyRanges()...)

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

func (c *Core) GetBarrier(request *corepb.GetBarrierRequest) (*corepb.GetBarrierResponse, error) {
	txn := c.badgerStore.View()
	defer txn.Discard()

	barrier, err := c.barriers.Get(txn, request.BarrierId)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, monsterax.NewErrorWithContext(
				monsterax.NotFound,
				"barrier not found",
				map[string]string{
					"barrier_id": ids.EncodeBarrierId(request.BarrierId),
				})
		} else {
			panic(err)
		}
	}

	return &corepb.GetBarrierResponse{
		Barrier: barrier,
	}, nil
}

func (c *Core) GetBarrierByName(request *corepb.GetBarrierByNameRequest) (*corepb.GetBarrierByNameResponse, error) {
	txn := c.badgerStore.View()
	defer txn.Discard()

	barrier, err := c.barriers.GetByName(txn, request.NamespaceId.AccountId, request.NamespaceId.NamespaceId, request.BarrierName)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, monsterax.NewErrorWithContext(
				monsterax.NotFound,
				"barrier not found",
				map[string]string{
					"barrier_name": request.BarrierName,
				})
		} else {
			panic(err)
		}
	}

	return &corepb.GetBarrierByNameResponse{
		Barrier: barrier,
	}, nil
}

func (c *Core) ListBarriers(request *corepb.ListBarriersRequest) (*corepb.ListBarriersResponse, error) {
	txn := c.badgerStore.View()
	defer txn.Discard()

	result, err := c.barriers.List(txn, request.NamespaceId.AccountId, request.NamespaceId.NamespaceId, request.PaginationToken, pagination.GetLimitWithDefaults(int(request.Limit)))
	panicIfNotNil(err)

	return &corepb.ListBarriersResponse{
		Barriers:                result.barriers,
		NextPaginationToken:     result.nextPaginationToken,
		PreviousPaginationToken: result.previousPaginationToken,
	}, nil
}

func (c *Core) ListBarrierParticipants(request *corepb.ListBarrierParticipantsRequest) (*corepb.ListBarrierParticipantsResponse, error) {
	txn := c.badgerStore.View()
	defer txn.Discard()

	barrier, err := c.barriers.GetByName(txn, request.NamespaceId.AccountId, request.NamespaceId.NamespaceId, request.BarrierName)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, monsterax.NewErrorWithContext(
				monsterax.NotFound,
				"barrier not found",
				map[string]string{
					"barrier_name": request.BarrierName,
				})
		} else {
			panic(err)
		}
	}

	result, err := c.participants.List(txn, request.NamespaceId.AccountId, request.NamespaceId.NamespaceId, barrier.Id.BarrierId, request.PaginationToken, pagination.GetLimitWithDefaults(int(request.Limit)))
	panicIfNotNil(err)

	return &corepb.ListBarrierParticipantsResponse{
		Participants:            result.participants,
		NextPaginationToken:     result.nextPaginationToken,
		PreviousPaginationToken: result.previousPaginationToken,
	}, nil
}

func (c *Core) CreateBarrier(request *corepb.CreateBarrierRequest) (*corepb.CreateBarrierResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	// Get counters for that namespace
	counters, err := c.counters.Get(txn, request.BarrierId.AccountId, request.BarrierId.NamespaceId)
	panicIfNotNil(err)

	// Checking max number of barriers
	if counters.NumberOfBarriers >= request.MaxNumberOfBarriersPerNamespace {
		return nil, monsterax.NewErrorWithContext(
			monsterax.ResourceExhausted,
			"max number of barriers per namespace reached",
			map[string]string{"limit": fmt.Sprintf("%d", request.MaxNumberOfBarriersPerNamespace)})
	}

	barrier := &corepb.Barrier{
		Id:                request.BarrierId,
		Name:              request.Name,
		Description:       request.Description,
		ExpectedProcesses: request.ExpectedProcesses,
		ArrivedProcesses:  0,
		Generation:        1,
		CreatedAt:         request.Now,
		UpdatedAt:         request.Now,
	}

	err = c.barriers.Create(txn, barrier)
	if err != nil {
		merr := &monsterax.Error{}
		if errors.As(err, &merr) {
			return nil, merr
		}

		panic(err)
	}

	// Update counters
	counters.NumberOfBarriers += 1
	err = c.counters.Set(txn, request.BarrierId.AccountId, request.BarrierId.NamespaceId, counters)
	panicIfNotNil(err)

	err = txn.Commit()
	panicIfNotNil(err)

	return &corepb.CreateBarrierResponse{
		Barrier: barrier,
	}, nil
}

func (c *Core) DeleteBarrier(request *corepb.DeleteBarrierRequest) (*corepb.DeleteBarrierResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	barrier, err := c.barriers.GetByName(txn, request.NamespaceId.AccountId, request.NamespaceId.NamespaceId, request.BarrierName)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			// No barrier exists, do nothing
			return &corepb.DeleteBarrierResponse{}, nil
		} else {
			panic(err)
		}
	}

	// Get counters for this namespace
	counters, err := c.counters.Get(txn, request.NamespaceId.AccountId, request.NamespaceId.NamespaceId)
	panicIfNotNil(err)

	err = c.barriers.Delete(txn, barrier.Id)
	panicIfNotNil(err)

	// TODO put gc record for barrier

	// Update counters
	counters.NumberOfBarriers -= 1
	err = c.counters.Set(txn, request.NamespaceId.AccountId, request.NamespaceId.NamespaceId, counters)
	panicIfNotNil(err)

	err = txn.Commit()
	panicIfNotNil(err)

	return &corepb.DeleteBarrierResponse{}, nil
}

func (c *Core) UpdateBarrier(request *corepb.UpdateBarrierRequest) (*corepb.UpdateBarrierResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	barrier, err := c.barriers.Get(txn, request.BarrierId)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, monsterax.NewErrorWithContext(
				monsterax.NotFound,
				"barrier not found",
				map[string]string{
					"barrier_id": ids.EncodeBarrierId(request.BarrierId),
				})
		} else {
			panic(err)
		}
	}

	// If there are currently more arrived processes than the new expected processes
	if barrier.ArrivedProcesses > request.ExpectedProcesses {
		return nil, monsterax.NewErrorWithContext(
			monsterax.InvalidArgument,
			"there are currently more arrived processes than the new expected processes",
			map[string]string{})
	}

	barrier.Description = request.Description
	barrier.ExpectedProcesses = request.ExpectedProcesses
	barrier.UpdatedAt = request.Now

	err = c.barriers.Update(txn, barrier)
	panicIfNotNil(err)

	err = txn.Commit()
	panicIfNotNil(err)

	return &corepb.UpdateBarrierResponse{
		Barrier: barrier,
	}, nil
}

func (c *Core) ArriveAtBarrier(request *corepb.ArriveAtBarrierRequest) (*corepb.ArriveAtBarrierResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	barrier, err := c.barriers.GetByName(txn, request.NamespaceId.AccountId, request.NamespaceId.NamespaceId, request.BarrierName)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, monsterax.NewErrorWithContext(
				monsterax.NotFound,
				"barrier not found",
				map[string]string{
					"barrier_name": request.BarrierName,
				})
		} else {
			panic(err)
		}
	}

	_, err = c.participants.Get(txn, barrier.Id.AccountId, barrier.Id.NamespaceId, barrier.Id.BarrierId, request.Generation, request.ProcessId)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			// Not arrived yet
		} else {
			panic(err)
		}
	} else {
		// This process has already arrived, nothing to do
		// TODO
		return &corepb.ArriveAtBarrierResponse{}, nil
	}

	participant := &corepb.BarrierParticipant{
		ProcessId:  request.ProcessId,
		Generation: request.Generation,
		ArrivedAt:  request.Now,
	}

	err = c.participants.Create(txn, barrier.Id.AccountId, barrier.Id.NamespaceId, barrier.Id.BarrierId, participant)
	panicIfNotNil(err)

	// Increment the counter of arrived processes
	barrier.ArrivedProcesses += 1

	// TODO check if barrier is reached, so we should not accept new participants

	err = c.barriers.Update(txn, barrier)
	panicIfNotNil(err)

	err = txn.Commit()
	panicIfNotNil(err)

	return &corepb.ArriveAtBarrierResponse{}, nil
}

func (c *Core) RunBarriersGarbageCollection(request *corepb.RunBarriersGarbageCollectionRequest) (*corepb.RunBarriersGarbageCollectionResponse, error) {
	return &corepb.RunBarriersGarbageCollectionResponse{}, nil
}

func (c *Core) BarriersDeleteNamespace(request *corepb.BarriersDeleteNamespaceRequest) (*corepb.BarriersDeleteNamespaceResponse, error) {
	return &corepb.BarriersDeleteNamespaceResponse{}, nil
}

func panicIfNotNil(err error) {
	if err != nil {
		panic(err)
	}
}
