package barriers

import (
	"errors"
	"fmt"
	"io"

	"github.com/evrblk/monstera"
	"github.com/evrblk/monstera/store"
	monsterax "github.com/evrblk/monstera/x"

	"github.com/evrblk/grackle/pkg/coreapis"
	"github.com/evrblk/grackle/pkg/corepb"
	"github.com/evrblk/grackle/pkg/ids"
	"github.com/evrblk/grackle/pkg/pagination"
	"github.com/evrblk/grackle/pkg/tables"
)

type Core struct {
	badgerStore *store.BadgerStore

	barriers          *barriersTable
	participants      *participantsTable
	counters          *tables.CountersTable[*corepb.BarriersCounter, corepb.BarriersCounter]
	gcRecords         *tables.GCRecordsTable[*corepb.BarriersGarbageCollectionRecord, corepb.BarriersGarbageCollectionRecord]
	expirationRecords *expirationRecordsTable
}

var _ coreapis.GrackleBarriersCoreApi = &Core{}

func NewCore(badgerStore *store.BadgerStore, globalIndexPrefix []byte, shardLowerBound []byte, shardUpperBound []byte) *Core {
	return &Core{
		badgerStore: badgerStore,

		barriers:     newBarriersTable(shardLowerBound, shardUpperBound),
		participants: newParticipantsTable(shardLowerBound, shardUpperBound),
		counters: tables.NewCountersTable[*corepb.BarriersCounter, corepb.BarriersCounter](
			tables.Grackle["Grackle.BarriersCore.Counters.Table"].Bytes(),
			shardLowerBound,
			shardUpperBound,
		),
		gcRecords: tables.NewGCRecordsTable[*corepb.BarriersGarbageCollectionRecord, corepb.BarriersGarbageCollectionRecord](
			tables.Grackle["Grackle.BarriersCore.GarbageCollectionRecords.Table"].Bytes(),
			globalIndexPrefix,
		),
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

func (c *Core) GetBarrier(req *coreapis.GetBarrierRequest) (*coreapis.GetBarrierResponse, error) {
	txn := c.badgerStore.View()
	defer txn.Discard()

	barrier, err := c.barriers.Get(txn, req.Payload.BarrierId)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return &coreapis.GetBarrierResponse{
				ApplicationError: monsterax.NewErrorWithContext(
					monsterax.NotFound,
					"barrier not found",
					map[string]string{
						"barrier_id": ids.EncodeBarrierId(req.Payload.BarrierId),
					}),
			}, nil
		}

		return nil, err
	}

	return &coreapis.GetBarrierResponse{
		Payload: &corepb.GetBarrierResponse{
			Barrier: barrier,
		},
	}, nil
}

func (c *Core) GetBarrierByName(req *coreapis.GetBarrierByNameRequest) (*coreapis.GetBarrierByNameResponse, error) {
	txn := c.badgerStore.View()
	defer txn.Discard()

	barrier, err := c.barriers.GetByName(txn, req.Payload.NamespaceId.AccountId, req.Payload.NamespaceId.NamespaceId, req.Payload.BarrierName)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return &coreapis.GetBarrierByNameResponse{
				ApplicationError: monsterax.NewErrorWithContext(
					monsterax.NotFound,
					"barrier not found",
					map[string]string{
						"barrier_name": req.Payload.BarrierName,
					}),
			}, nil
		}

		return nil, err
	}

	return &coreapis.GetBarrierByNameResponse{
		Payload: &corepb.GetBarrierByNameResponse{
			Barrier: barrier,
		},
	}, nil
}

func (c *Core) ListBarriers(req *coreapis.ListBarriersRequest) (*coreapis.ListBarriersResponse, error) {
	txn := c.badgerStore.View()
	defer txn.Discard()

	result, err := c.barriers.List(txn, req.Payload.NamespaceId.AccountId, req.Payload.NamespaceId.NamespaceId, req.Payload.PaginationToken, pagination.GetLimitWithDefaults(int(req.Payload.Limit)))
	if err != nil {
		return nil, err
	}

	return &coreapis.ListBarriersResponse{
		Payload: &corepb.ListBarriersResponse{
			Barriers:                result.barriers,
			NextPaginationToken:     result.nextPaginationToken,
			PreviousPaginationToken: result.previousPaginationToken,
		},
	}, nil
}

func (c *Core) ListBarrierParticipants(req *coreapis.ListBarrierParticipantsRequest) (*coreapis.ListBarrierParticipantsResponse, error) {
	txn := c.badgerStore.View()
	defer txn.Discard()

	barrier, err := c.barriers.GetByName(txn, req.Payload.NamespaceId.AccountId, req.Payload.NamespaceId.NamespaceId, req.Payload.BarrierName)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return &coreapis.ListBarrierParticipantsResponse{
				ApplicationError: monsterax.NewErrorWithContext(
					monsterax.NotFound,
					"barrier not found",
					map[string]string{
						"barrier_name": req.Payload.BarrierName,
					}),
			}, nil
		}

		return nil, err
	}

	result, err := c.participants.List(txn, req.Payload.NamespaceId.AccountId, req.Payload.NamespaceId.NamespaceId, barrier.Id.BarrierId, req.Payload.PaginationToken, pagination.GetLimitWithDefaults(int(req.Payload.Limit)))
	if err != nil {
		return nil, err
	}

	return &coreapis.ListBarrierParticipantsResponse{
		Payload: &corepb.ListBarrierParticipantsResponse{
			Participants:            result.participants,
			NextPaginationToken:     result.nextPaginationToken,
			PreviousPaginationToken: result.previousPaginationToken,
		},
	}, nil
}

func (c *Core) CreateBarrier(req *coreapis.CreateBarrierRequest) (*coreapis.CreateBarrierResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	// Get counters for that namespace
	counters, err := c.counters.Get(txn, req.Payload.BarrierId.AccountId, req.Payload.BarrierId.NamespaceId)
	if err != nil {
		return nil, err
	}

	// Checking max number of barriers
	if counters.NumberOfBarriers >= req.Payload.MaxNumberOfBarriersPerNamespace {
		return &coreapis.CreateBarrierResponse{
			ApplicationError: monsterax.NewErrorWithContext(
				monsterax.ResourceExhausted,
				"max number of barriers per namespace reached",
				map[string]string{
					"limit": fmt.Sprintf("%d", req.Payload.MaxNumberOfBarriersPerNamespace),
				}),
		}, nil
	}

	barrier := &corepb.Barrier{
		Id:                req.Payload.BarrierId,
		Name:              req.Payload.Name,
		Description:       req.Payload.Description,
		ExpectedProcesses: req.Payload.ExpectedProcesses,
		ArrivedProcesses:  0,
		Generation:        1,
		CreatedAt:         req.Payload.Now,
		UpdatedAt:         req.Payload.Now,
	}

	appErr, err := c.barriers.Create(txn, barrier)
	if err != nil {
		return nil, err
	}
	if appErr != nil {
		return &coreapis.CreateBarrierResponse{
			ApplicationError: appErr,
		}, nil
	}

	// Update counters
	counters.NumberOfBarriers += 1
	err = c.counters.Set(txn, req.Payload.BarrierId.AccountId, req.Payload.BarrierId.NamespaceId, counters)
	if err != nil {
		return nil, err
	}

	err = txn.Commit()
	if err != nil {
		return nil, err
	}

	return &coreapis.CreateBarrierResponse{
		Payload: &corepb.CreateBarrierResponse{
			Barrier: barrier,
		},
	}, nil
}

func (c *Core) DeleteBarrier(req *coreapis.DeleteBarrierRequest) (*coreapis.DeleteBarrierResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	barrier, err := c.barriers.GetByName(txn, req.Payload.NamespaceId.AccountId, req.Payload.NamespaceId.NamespaceId, req.Payload.BarrierName)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			// No barrier exists, do nothing
			return &coreapis.DeleteBarrierResponse{
				Payload: &corepb.DeleteBarrierResponse{},
			}, nil
		}

		return nil, err
	}

	// Get counters for this namespace
	counters, err := c.counters.Get(txn, req.Payload.NamespaceId.AccountId, req.Payload.NamespaceId.NamespaceId)
	if err != nil {
		return nil, err
	}

	err = c.barriers.Delete(txn, barrier.Id)
	if err != nil {
		return nil, err
	}

	// TODO put gc record for barrier

	// Update counters
	counters.NumberOfBarriers -= 1
	err = c.counters.Set(txn, req.Payload.NamespaceId.AccountId, req.Payload.NamespaceId.NamespaceId, counters)
	if err != nil {
		return nil, err
	}

	err = txn.Commit()
	if err != nil {
		return nil, err
	}

	return &coreapis.DeleteBarrierResponse{
		Payload: &corepb.DeleteBarrierResponse{},
	}, nil
}

func (c *Core) UpdateBarrier(req *coreapis.UpdateBarrierRequest) (*coreapis.UpdateBarrierResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	barrier, err := c.barriers.Get(txn, req.Payload.BarrierId)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return &coreapis.UpdateBarrierResponse{
				ApplicationError: monsterax.NewErrorWithContext(
					monsterax.NotFound,
					"barrier not found",
					map[string]string{
						"barrier_id": ids.EncodeBarrierId(req.Payload.BarrierId),
					}),
			}, nil
		}

		return nil, err
	}

	// If there are currently more arrived processes than the new expected processes
	if barrier.ArrivedProcesses > req.Payload.ExpectedProcesses {
		return &coreapis.UpdateBarrierResponse{
			ApplicationError: monsterax.NewErrorWithContext(
				monsterax.InvalidArgument,
				"there are currently more arrived processes than the new expected processes",
				map[string]string{}),
		}, nil
	}

	barrier.Description = req.Payload.Description
	barrier.ExpectedProcesses = req.Payload.ExpectedProcesses
	barrier.UpdatedAt = req.Payload.Now

	err = c.barriers.Update(txn, barrier)
	if err != nil {
		return nil, err
	}

	err = txn.Commit()
	if err != nil {
		return nil, err
	}

	return &coreapis.UpdateBarrierResponse{
		Payload: &corepb.UpdateBarrierResponse{
			Barrier: barrier,
		},
	}, nil
}

func (c *Core) ArriveAtBarrier(req *coreapis.ArriveAtBarrierRequest) (*coreapis.ArriveAtBarrierResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	barrier, err := c.barriers.GetByName(txn, req.Payload.NamespaceId.AccountId, req.Payload.NamespaceId.NamespaceId, req.Payload.BarrierName)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return &coreapis.ArriveAtBarrierResponse{
				ApplicationError: monsterax.NewErrorWithContext(
					monsterax.NotFound,
					"barrier not found",
					map[string]string{
						"barrier_name": req.Payload.BarrierName,
					}),
			}, nil
		}

		return nil, err
	}

	_, err = c.participants.Get(txn, barrier.Id.AccountId, barrier.Id.NamespaceId, barrier.Id.BarrierId, req.Payload.Generation, req.Payload.ProcessId)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			// Not arrived yet
		} else {
			return nil, err
		}
	} else {
		// This process has already arrived, nothing to do
		// TODO
		return &coreapis.ArriveAtBarrierResponse{
			Payload: &corepb.ArriveAtBarrierResponse{
				Barrier: barrier,
			},
		}, nil
	}

	participant := &corepb.BarrierParticipant{
		ProcessId:  req.Payload.ProcessId,
		Generation: req.Payload.Generation,
		ArrivedAt:  req.Payload.Now,
	}

	err = c.participants.Create(txn, barrier.Id.AccountId, barrier.Id.NamespaceId, barrier.Id.BarrierId, participant)
	if err != nil {
		return nil, err
	}

	// Increment the counter of arrived processes
	barrier.ArrivedProcesses += 1

	// TODO check if barrier is reached, so we should not accept new participants

	err = c.barriers.Update(txn, barrier)
	if err != nil {
		return nil, err
	}

	err = txn.Commit()
	if err != nil {
		return nil, err
	}

	return &coreapis.ArriveAtBarrierResponse{
		Payload: &corepb.ArriveAtBarrierResponse{
			Barrier: barrier,
		},
	}, nil
}

func (c *Core) RunBarriersGarbageCollection(req *coreapis.RunBarriersGarbageCollectionRequest) (*coreapis.RunBarriersGarbageCollectionResponse, error) {
	// TODO: implement
	return &coreapis.RunBarriersGarbageCollectionResponse{
		Payload: &corepb.RunBarriersGarbageCollectionResponse{},
	}, nil
}

func (c *Core) BarriersDeleteNamespace(req *coreapis.BarriersDeleteNamespaceRequest) (*coreapis.BarriersDeleteNamespaceResponse, error) {
	// TODO: implement
	return &coreapis.BarriersDeleteNamespaceResponse{
		Payload: &corepb.BarriersDeleteNamespaceResponse{},
	}, nil
}
