package barriers

import (
	"fmt"

	"github.com/evrblk/monstera/store"
	"github.com/evrblk/monstera/utils"
	"github.com/evrblk/yellowstone-common/honey"

	"github.com/evrblk/grackle/pkg/corepb"
	"github.com/evrblk/grackle/pkg/pagination"
	"github.com/evrblk/grackle/pkg/sharding"
)

// participantsTable is a table of barrier participants indexed by participant ID
//
// Table Primary Key:
// 1. account id
// 2. namespace id
// 3. barrier id
//
// Table Sort Key:
// 1. generation
// 2. process id
type participantsTable struct {
	table *honey.BinaryTable[*corepb.BarrierParticipant, corepb.BarrierParticipant]
}

// newParticipantsTable scopes the table under the shard-unique prefix; see
// newBarriersTable.
func newParticipantsTable(replicaPrefix []byte) *participantsTable {
	return &participantsTable{
		table: honey.NewBinaryTable[*corepb.BarrierParticipant, corepb.BarrierParticipant](
			utils.ConcatBytes(replicaPrefix, tablePrefixParticipants),
		),
	}
}

// Clear deletes every participant row.
func (t *participantsTable) Clear(badgerStore *store.BadgerStore) error {
	return badgerStore.DeletePrefix(t.table.TableId())
}

// EachEntity streams every participant as (canonical key, stored value).
func (t *participantsTable) EachEntity(txn *store.Txn, fn func(key []byte, value []byte) (bool, error)) error {
	return t.table.EachEntry(txn, fn)
}

// RestoreEntity decodes one streamed participant and, if owned, inserts it.
// A participant value carries no identity of its own — the identity lives in
// the canonical key, whose layout this table defines (see tablePK/tableSK):
// <8-byte account id><8-byte namespace id><8-byte barrier id><sort key>. The
// canonical key needs no rewriting: it IS the table-relative key.
func (t *participantsTable) RestoreEntity(txn *store.Txn, key []byte, value []byte, bounds honey.ShardRange) (bool, error) {
	if len(key) < 8+8+8 {
		return false, fmt.Errorf("participant key has %d bytes, want at least 24", len(key))
	}
	accountId := utils.BytesToUint64(key[0:8])
	namespaceId := utils.BytesToUint64(key[8:16])
	if !bounds.Owns(sharding.ByAccountAndNamespace(accountId, namespaceId)) {
		return false, nil
	}

	participant := &corepb.BarrierParticipant{}
	if err := participant.UnmarshalBinary(value); err != nil {
		return false, err
	}
	return true, t.table.Set(txn, key, participant)
}

func (t *participantsTable) Get(txn *store.Txn, accountId uint64, namespaceId uint64, barrierId uint64, generation int64, processId string) (*corepb.BarrierParticipant, error) {
	return t.table.Get(txn,
		utils.ConcatBytes(
			t.tablePK(accountId, namespaceId, barrierId),
			t.tableSK(generation, processId)))
}

func (t *participantsTable) Create(txn *store.Txn, accountId uint64, namespaceId uint64, barrierId uint64, participant *corepb.BarrierParticipant) error {
	return t.table.Set(txn,
		utils.ConcatBytes(
			t.tablePK(accountId, namespaceId, barrierId),
			t.tableSK(participant.Generation, participant.ProcessId)),
		participant)
}

func (t *participantsTable) Delete(txn *store.Txn, accountId uint64, namespaceId uint64, barrierId uint64, generation int64, processId string) error {
	return t.table.Delete(txn,
		utils.ConcatBytes(
			t.tablePK(accountId, namespaceId, barrierId),
			t.tableSK(generation, processId)))
}

type listParticipantResult struct {
	participants            []*corepb.BarrierParticipant
	nextPaginationToken     *corepb.PaginationToken
	previousPaginationToken *corepb.PaginationToken
}

// List scans every participant of a barrier, across every generation it
// has ever had. This is what GC's unconditional drain needs (it must clean
// up leftover rows from every past round, not just the current one); it is
// deliberately not what ListBarrierParticipants uses — see ListByGeneration.
func (t *participantsTable) List(txn *store.Txn, accountId uint64, namespaceId uint64, barrierId uint64,
	paginationToken *corepb.PaginationToken, limit int) (*listParticipantResult, error) {
	result, err := t.table.ListPaginated(txn,
		t.tablePK(accountId, namespaceId, barrierId),
		pagination.CoreToMonstera(paginationToken),
		limit)
	if err != nil {
		return nil, err
	}

	return &listParticipantResult{
		participants:            result.Items,
		nextPaginationToken:     pagination.MonsteraToCore(result.NextPaginationToken),
		previousPaginationToken: pagination.MonsteraToCore(result.PreviousPaginationToken),
	}, nil
}

// ListByGeneration scans the participants of one specific generation of a
// barrier — generation is the leading component of this table's sort key
// (see tableSK), so it can be folded straight into the scan prefix. Unlike
// List, this reaches only one round, even if the barrier has since tripped
// past it: a completed round's participants stay queryable by their own
// generation number until GC reaps them.
func (t *participantsTable) ListByGeneration(txn *store.Txn, accountId uint64, namespaceId uint64, barrierId uint64, generation int64,
	paginationToken *corepb.PaginationToken, limit int) (*listParticipantResult, error) {
	result, err := t.table.ListPaginated(txn,
		utils.ConcatBytes(t.tablePK(accountId, namespaceId, barrierId), generation),
		pagination.CoreToMonstera(paginationToken),
		limit)
	if err != nil {
		return nil, err
	}

	return &listParticipantResult{
		participants:            result.Items,
		nextPaginationToken:     pagination.MonsteraToCore(result.NextPaginationToken),
		previousPaginationToken: pagination.MonsteraToCore(result.PreviousPaginationToken),
	}, nil
}

func (t *participantsTable) tablePK(accountId uint64, namespaceId uint64, barrierId uint64) []byte {
	return utils.ConcatBytes(
		accountId,
		namespaceId,
		barrierId,
	)
}

// generation is encoded as plain big-endian, so byte-lexicographic ordering
// only matches numeric ordering for generation >= 0 (two's complement makes
// negative values sort after positive ones). It's the leading sort field of
// this table, so List's paginated iteration order depends on generation
// never being negative; in practice it never is, since Barrier.Generation
// starts at 1 (see core.go) and ArriveAtBarrier rejects any request whose
// generation doesn't exactly match the barrier's current one before a
// participant row is ever written.
func (t *participantsTable) tableSK(generation int64, processId string) []byte {
	return utils.ConcatBytes(
		generation,
		processId,
	)
}
