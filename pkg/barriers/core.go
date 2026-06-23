package barriers

import (
	"errors"
	"fmt"
	"io"
	"time"

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

	barriers        *barriersTable
	participants    *participantsTable
	counters        *tables.CountersTable[*corepb.BarriersCounter, corepb.BarriersCounter]
	gcRecords       *tables.GCRecordsTable[*corepb.BarriersGarbageCollectionRecord, corepb.BarriersGarbageCollectionRecord]
	deletionRecords *deletionRecordsTable
}

var _ coreapis.GrackleBarriersCoreApi = &Core{}

// NewCore constructs a Core bound to a single shard of the barriers keyspace.
// The given lower/upper bounds delimit the shard's local key range (used for
// Snapshot/Restore), while globalIndexPrefix scopes cross-shard global
// indexes such as GC records and expiration records.
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
		deletionRecords: newDeletionRecordsTable(globalIndexPrefix),
	}
}

func (c *Core) ranges() []monsterax.KeyRange {
	ranges := []monsterax.KeyRange{
		c.counters.GetTableKeyRange(),
		c.gcRecords.GetTableKeyRange(),
		c.deletionRecords.GetTableKeyRange(),
		c.participants.GetTableKeyRange(),
	}

	ranges = append(ranges, c.barriers.GetTableKeyRanges()...)

	return ranges
}

// Snapshot returns a consistent snapshot of every key range owned by this
// shard's barriers Core, suitable for Raft snapshot transfer.
func (c *Core) Snapshot() monstera.ApplicationCoreSnapshot {
	return monsterax.Snapshot(c.badgerStore, c.ranges())
}

// Restore replaces the contents of this shard's key ranges with the data read
// from reader. Any existing keys in those ranges are removed first.
func (c *Core) Restore(reader io.ReadCloser) error {
	return monsterax.Restore(c.badgerStore, c.ranges(), reader)
}

// Close releases any Core-owned resources. The underlying Badger store is
// shared across cores and is not closed here.
func (c *Core) Close() {

}

// GetBarrier looks up a barrier by its full BarrierId. Returns a NotFound
// application error if no barrier with that id exists.
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

// GetBarrierByName looks up a barrier by its (account, namespace, name)
// triple. Returns a NotFound application error if no barrier with that name
// exists in the given namespace.
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

// ListBarriers returns a page of barriers in the given namespace, ordered by
// name. Use the returned NextPaginationToken / PreviousPaginationToken to
// walk forward or backward through pages.
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

// ListBarrierParticipants returns a page of participants currently recorded
// for the named barrier, across all generations. Returns a NotFound
// application error if the barrier does not exist.
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

// CreateBarrier creates a new barrier at generation 1 with the given
// ExpectedProcesses and bumps the per-namespace barrier counter. Returns
// AlreadyExists if a barrier with the same name already exists in the
// namespace, or ResourceExhausted if creating it would exceed
// MaxNumberOfBarriersPerNamespace.
func (c *Core) CreateBarrier(req *coreapis.CreateBarrierRequest) (*coreapis.CreateBarrierResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	// A barrier that expects zero processes can never trip; reject it outright.
	if req.Payload.ExpectedProcesses == 0 {
		return &coreapis.CreateBarrierResponse{
			ApplicationError: monsterax.NewErrorWithContext(
				monsterax.InvalidArgument,
				"expected processes must be greater than 0",
				map[string]string{
					"barrier_name": req.Payload.Name,
				}),
		}, nil
	}

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
		Id:                         req.Payload.BarrierId,
		Name:                       req.Payload.Name,
		Description:                req.Payload.Description,
		ExpectedProcesses:          req.Payload.ExpectedProcesses,
		ArrivedProcesses:           0,
		Generation:                 1,
		CreatedAt:                  req.Payload.Now,
		UpdatedAt:                  req.Payload.Now,
		Metadata:                   req.Payload.Metadata,
		Version:                    1,
		LastActivityAt:             req.Payload.Now,
		DeleteInactiveAfterSeconds: req.Payload.DeleteInactiveAfterSeconds,
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

	// Schedule auto-deletion after the inactivity window.
	err = c.deletionRecords.Add(txn, deletionTime(barrier.LastActivityAt, barrier.DeleteInactiveAfterSeconds), barrier.Id)
	if err != nil {
		return nil, err
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

// DeleteBarrier removes the named barrier and decrements the per-namespace
// barrier counter. Deleting a barrier that does not exist is a no-op and
// returns success. Leftover participant rows are not deleted synchronously;
// instead a GC record is created so that RunBarriersGarbageCollection can
// drain them in bounded batches.
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

	// Remove the pending auto-deletion record. No-op if it does not exist.
	err = c.deletionRecords.Delete(txn, deletionTime(barrier.LastActivityAt, barrier.DeleteInactiveAfterSeconds), barrier.Id)
	if err != nil {
		return nil, err
	}

	// Schedule asynchronous cleanup of leftover participants. The barrier record itself is already
	// gone; GC just needs the barrier_id to drain the remaining participant rows.
	err = c.gcRecords.Create(txn, &corepb.BarriersGarbageCollectionRecord{
		Id: req.Payload.RecordId,
		Record: &corepb.BarriersGarbageCollectionRecord_BarrierId{
			BarrierId: barrier.Id,
		},
	})
	if err != nil {
		return nil, err
	}

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

// UpdateBarrier updates the barrier's description and ExpectedProcesses.
// Returns NotFound if the barrier does not exist, or InvalidArgument if
// ExpectedProcesses is 0 or is smaller than the number of participants that
// have already arrived (which would leave the barrier in an inconsistent
// state). Lowering ExpectedProcesses to exactly ArrivedProcesses trips the
// barrier (resetting arrived and advancing the generation) rather than leaving
// it wedged — see the trip logic below.
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

	if barrier.Version != req.Payload.ExpectedVersion {
		return &coreapis.UpdateBarrierResponse{
			ApplicationError: monsterax.NewErrorWithContext(
				monsterax.InvalidArgument,
				"version mismatch",
				map[string]string{
					"barrier_id":       ids.EncodeBarrierId(req.Payload.BarrierId),
					"actual_version":   fmt.Sprintf("%d", barrier.Version),
					"expected_version": fmt.Sprintf("%d", req.Payload.ExpectedVersion),
				},
			),
		}, nil
	}

	// A barrier that expects zero processes can never trip; reject it outright.
	if req.Payload.ExpectedProcesses == 0 {
		return &coreapis.UpdateBarrierResponse{
			ApplicationError: monsterax.NewErrorWithContext(
				monsterax.InvalidArgument,
				"expected processes must be greater than 0",
				map[string]string{
					"barrier_id": ids.EncodeBarrierId(req.Payload.BarrierId),
				}),
		}, nil
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

	// Reconcile the auto-deletion schedule if the inactivity window changed. An
	// update is not itself activity, so last_activity_at (and thus the base of
	// the deletion time) does not change here.
	oldDeleteAt := deletionTime(barrier.LastActivityAt, barrier.DeleteInactiveAfterSeconds)
	newDeleteAt := deletionTime(barrier.LastActivityAt, req.Payload.DeleteInactiveAfterSeconds)
	if oldDeleteAt != newDeleteAt {
		err = c.deletionRecords.Delete(txn, oldDeleteAt, barrier.Id)
		if err != nil {
			return nil, err
		}
		err = c.deletionRecords.Add(txn, newDeleteAt, barrier.Id)
		if err != nil {
			return nil, err
		}
	}

	barrier.Description = req.Payload.Description
	barrier.ExpectedProcesses = req.Payload.ExpectedProcesses
	barrier.UpdatedAt = req.Payload.Now
	barrier.Metadata = req.Payload.Metadata
	barrier.Version += 1
	barrier.DeleteInactiveAfterSeconds = req.Payload.DeleteInactiveAfterSeconds

	allArrived := false

	// Lowering ExpectedProcesses down to the number of already-arrived processes satisfies the
	// release condition, but the trip logic only runs on arrival (in ArriveAtBarrier). Without
	// tripping here the barrier would wedge: it can no longer trip on its own, and the next
	// ArriveAtBarrier is rejected by the ArrivedProcesses >= ExpectedProcesses guard. Trip it now —
	// reset arrived and advance the generation — exactly as the final arrival would.
	if barrier.ArrivedProcesses >= barrier.ExpectedProcesses {
		barrier.ArrivedProcesses = 0
		barrier.Generation += 1
		allArrived = true
	}

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
			Barrier:    barrier,
			AllArrived: allArrived,
		},
	}, nil
}

// ArriveAtBarrier records the given process as having reached the named
// barrier for req.Generation, and increments ArrivedProcesses. When the
// increment makes ArrivedProcesses equal to ExpectedProcesses the barrier
// auto-trips: ArrivedProcesses is reset to 0 and Generation is incremented,
// releasing anyone polling WaitAtBarrier at the old generation. A process
// arriving twice for the same generation is a no-op. Returns NotFound if the
// barrier does not exist, or InvalidArgument if req.Generation is older than
// the barrier's current generation; in the InvalidArgument case the
// transaction is discarded and no participant rows are persisted.
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

	// Reject arrivals for a generation older than the barrier's current one:
	// the caller is referring to an already-tripped (and reused) barrier generation.
	if req.Payload.Generation < barrier.Generation {
		return &coreapis.ArriveAtBarrierResponse{
			ApplicationError: monsterax.NewErrorWithContext(
				monsterax.InvalidArgument,
				"generation is older than the current barrier generation",
				map[string]string{
					"barrier_name":       req.Payload.BarrierName,
					"current_generation": fmt.Sprintf("%d", barrier.Generation),
					"request_generation": fmt.Sprintf("%d", req.Payload.Generation),
				}),
		}, nil
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
		return &coreapis.ArriveAtBarrierResponse{
			Payload: &corepb.ArriveAtBarrierResponse{
				Barrier: barrier,
			},
		}, nil
	}

	// Defense in depth: auto-trip below should make this unreachable, since ArrivedProcesses
	// is reset to 0 the moment it would reach ExpectedProcesses. If we ever observe the
	// invariant broken, reject loudly rather than silently overflowing the counter.
	if barrier.ArrivedProcesses >= barrier.ExpectedProcesses {
		return &coreapis.ArriveAtBarrierResponse{
			ApplicationError: monsterax.NewErrorWithContext(
				monsterax.InvalidArgument,
				"too many participants arrived at the barrier",
				map[string]string{
					"barrier_name":       req.Payload.BarrierName,
					"expected_processes": fmt.Sprintf("%d", barrier.ExpectedProcesses),
				}),
		}, nil
	}

	participant := &corepb.BarrierParticipant{
		ProcessId:  req.Payload.ProcessId,
		Generation: req.Payload.Generation,
		ArrivedAt:  req.Payload.Now,
		Metadata:   req.Payload.Metadata,
	}

	err = c.participants.Create(txn, barrier.Id.AccountId, barrier.Id.NamespaceId, barrier.Id.BarrierId, participant)
	if err != nil {
		return nil, err
	}

	allArrived := false

	// Increment the counter of arrived processes
	barrier.ArrivedProcesses += 1

	// Auto-trip on the last expected arrival: reset the counter and advance the generation
	// so any waiter polling WaitAtBarrier at the old generation observes the trip.
	if barrier.ArrivedProcesses == barrier.ExpectedProcesses {
		barrier.ArrivedProcesses = 0
		barrier.Generation += 1
		allArrived = true
	}

	// Arriving is activity: advance last_activity_at and push the auto-deletion
	// time out accordingly.
	oldDeleteAt := deletionTime(barrier.LastActivityAt, barrier.DeleteInactiveAfterSeconds)
	barrier.LastActivityAt = req.Payload.Now
	newDeleteAt := deletionTime(barrier.LastActivityAt, barrier.DeleteInactiveAfterSeconds)
	if oldDeleteAt != newDeleteAt {
		err = c.deletionRecords.Delete(txn, oldDeleteAt, barrier.Id)
		if err != nil {
			return nil, err
		}
		err = c.deletionRecords.Add(txn, newDeleteAt, barrier.Id)
		if err != nil {
			return nil, err
		}
	}

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
			Barrier:    barrier,
			AllArrived: allArrived,
		},
	}, nil
}

// RunBarriersGarbageCollection performs a single bounded GC pass. It
// processes namespace deletion records (deleting participant rows for every
// barrier in the namespace, then the barrier itself) and barrier deletion
// records (draining the leftover participants of a previously deleted
// barrier). The pass stops once MaxVisited total records (participants +
// barriers + counter rows) have been touched so that one invocation cannot
// produce an unbounded transaction. Intended to be invoked periodically by
// the scheduler.
func (c *Core) RunBarriersGarbageCollection(req *coreapis.RunBarriersGarbageCollectionRequest) (*coreapis.RunBarriersGarbageCollectionResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	visited := int64(0)
	participantsPageSize := int(req.Payload.GcRecordParticipantsPageSize)

	// List one page of GC records
	gcRecords, err := c.gcRecords.List(txn, int(req.Payload.GcRecordsPageSize))
	if err != nil {
		return nil, err
	}

	for _, gcRecord := range gcRecords {
		switch r := gcRecord.Record.(type) {
		case *corepb.BarriersGarbageCollectionRecord_NamespaceId:
			// Delete counters for that namespace. Will not fail if counters do not exist.
			err := c.counters.Delete(txn, r.NamespaceId.AccountId, r.NamespaceId.NamespaceId)
			if err != nil {
				return nil, err
			}

			// List one page of barriers for that namespace
			result, err := c.barriers.List(txn, r.NamespaceId.AccountId, r.NamespaceId.NamespaceId, nil, int(req.Payload.GcRecordBarriersPageSize))
			if err != nil {
				return nil, err
			}

			allBarriersDeleted := true
			for _, barrier := range result.barriers {
				// Drain one page of participants first; if there are more participants than fit on the
				// page, leave the barrier record in place and let a later GC pass continue.
				participantsDrained, err := c.gcDeleteBarrierParticipants(txn, barrier.Id, participantsPageSize, &visited, req.Payload.MaxVisited)
				if err != nil {
					return nil, err
				}
				if visited >= req.Payload.MaxVisited {
					goto commit
				}
				if !participantsDrained {
					allBarriersDeleted = false
					break
				}

				// All participants for this barrier are gone — delete the barrier record itself.
				err = c.barriers.Delete(txn, barrier.Id)
				if err != nil {
					return nil, err
				}

				// Remove its pending auto-deletion record. No-op if absent.
				err = c.deletionRecords.Delete(txn, deletionTime(barrier.LastActivityAt, barrier.DeleteInactiveAfterSeconds), barrier.Id)
				if err != nil {
					return nil, err
				}
				visited++
				if visited >= req.Payload.MaxVisited {
					goto commit
				}
			}

			// Delete the GC record only when every barrier in this namespace has been fully drained
			// (no remaining participants, no more barrier pages).
			if allBarriersDeleted && result.nextPaginationToken == nil {
				err := c.gcRecords.Delete(txn, gcRecord)
				if err != nil {
					return nil, err
				}
			}
		case *corepb.BarriersGarbageCollectionRecord_BarrierId:
			// The barrier record itself is already deleted by DeleteBarrier; we just need to drain
			// whatever participants are still attached to its id.
			participantsDrained, err := c.gcDeleteBarrierParticipants(txn, r.BarrierId, participantsPageSize, &visited, req.Payload.MaxVisited)
			if err != nil {
				return nil, err
			}
			if visited >= req.Payload.MaxVisited {
				goto commit
			}
			if participantsDrained {
				err = c.gcRecords.Delete(txn, gcRecord)
				if err != nil {
					return nil, err
				}
			}
		}
	}

commit:

	// Auto-delete barriers that have been inactive past their retention window.
	err = c.deleteInactiveBarriers(txn, req.Payload.Now, int(req.Payload.GcRecordBarriersPageSize), participantsPageSize, &visited, req.Payload.MaxVisited)
	if err != nil {
		return nil, err
	}

	err = txn.Commit()
	if err != nil {
		return nil, err
	}

	return &coreapis.RunBarriersGarbageCollectionResponse{
		Payload: &corepb.RunBarriersGarbageCollectionResponse{},
	}, nil
}

// BarriersDeleteNamespace records a GC marker that will, on subsequent
// RunBarriersGarbageCollection ticks, delete every barrier and participant
// row belonging to the given namespace. The deletion itself is asynchronous;
// this call only enqueues the request.
func (c *Core) BarriersDeleteNamespace(req *coreapis.BarriersDeleteNamespaceRequest) (*coreapis.BarriersDeleteNamespaceResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	// Mark the namespace as deleted
	err := c.gcRecords.Create(txn, &corepb.BarriersGarbageCollectionRecord{
		Id: req.Payload.RecordId,
		Record: &corepb.BarriersGarbageCollectionRecord_NamespaceId{
			NamespaceId: req.Payload.NamespaceId,
		},
	})
	if err != nil {
		return nil, err
	}

	err = txn.Commit()
	if err != nil {
		return nil, err
	}

	return &coreapis.BarriersDeleteNamespaceResponse{
		Payload: &corepb.BarriersDeleteNamespaceResponse{},
	}, nil
}

// deletionTime returns the timestamp (ns) at which an inactive barrier should
// be auto-deleted, given its last activity time and inactivity window in
// seconds.
func deletionTime(lastActivityAt int64, deleteInactiveAfterSeconds int64) int64 {
	return lastActivityAt + deleteInactiveAfterSeconds*int64(time.Second)
}

// deleteInactiveBarriers deletes barriers whose auto-deletion time (delete_at)
// has passed, draining their participants first. It shares the GC pass's visit
// budget; a barrier whose participants do not fully drain within the budget is
// left in place (along with its deletion record) and resumed on the next tick.
func (c *Core) deleteInactiveBarriers(txn *store.Txn, now int64, barriersPageSize int, participantsPageSize int, visited *int64, maxVisited int64) error {
	if *visited >= maxVisited {
		return nil
	}

	barriersPageSize = pagination.GetLimitWithDefaults(barriersPageSize)

	records := make([]*corepb.BarriersDeletionRecord, 0, barriersPageSize)
	err := c.deletionRecords.ListByDeletion(txn, 0, now, func(record *corepb.BarriersDeletionRecord) (bool, error) {
		records = append(records, record)
		return len(records) < barriersPageSize, nil
	})
	if err != nil {
		return err
	}

	for _, record := range records {
		if *visited >= maxVisited {
			return nil
		}

		barrier, err := c.barriers.Get(txn, record.BarrierId)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				// Barrier already gone; drop the stale deletion record.
				if err := c.deletionRecords.Delete(txn, record.DeleteAt, record.BarrierId); err != nil {
					return err
				}
				*visited++
				continue
			}
			return err
		}

		// Drain one page of participants first.
		participantsDrained, err := c.gcDeleteBarrierParticipants(txn, barrier.Id, participantsPageSize, visited, maxVisited)
		if err != nil {
			return err
		}
		if !participantsDrained {
			// More participants remain (or the visit budget was exhausted). Leave the
			// barrier and its deletion record in place; a later GC pass resumes.
			return nil
		}

		// All participants drained — delete the barrier and its bookkeeping.
		counters, err := c.counters.Get(txn, barrier.Id.AccountId, barrier.Id.NamespaceId)
		if err != nil {
			return err
		}
		counters.NumberOfBarriers -= 1
		if err := c.counters.Set(txn, barrier.Id.AccountId, barrier.Id.NamespaceId, counters); err != nil {
			return err
		}

		if err := c.barriers.Delete(txn, barrier.Id); err != nil {
			return err
		}
		*visited++

		if err := c.deletionRecords.Delete(txn, record.DeleteAt, record.BarrierId); err != nil {
			return err
		}
	}

	return nil
}

// gcDeleteBarrierParticipants deletes up to one page of participants for the given barrier,
// decrementing the visit budget for each one. Returns true if the barrier has no remaining
// participants (every page drained); false if the page-size limit or the visit budget cut the
// run short. The caller owns the txn lifecycle.
func (c *Core) gcDeleteBarrierParticipants(txn *store.Txn, barrierId *corepb.BarrierId, pageSize int, visited *int64, maxVisited int64) (bool, error) {
	result, err := c.participants.List(txn, barrierId.AccountId, barrierId.NamespaceId, barrierId.BarrierId, nil, pageSize)
	if err != nil {
		return false, err
	}

	for _, participant := range result.participants {
		err := c.participants.Delete(txn, barrierId.AccountId, barrierId.NamespaceId, barrierId.BarrierId, participant.Generation, participant.ProcessId)
		if err != nil {
			return false, err
		}
		*visited++
		if *visited >= maxVisited {
			return false, nil
		}
	}

	// Drained iff this was the last page.
	return result.nextPaginationToken == nil, nil
}
