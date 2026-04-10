package barriers

import (
	"github.com/evrblk/monstera/store"
	"github.com/evrblk/monstera/utils"
	monsterax "github.com/evrblk/monstera/x"

	"github.com/evrblk/grackle/pkg/corepb"
	"github.com/evrblk/grackle/pkg/pagination"
	"github.com/evrblk/grackle/pkg/sharding"
	"github.com/evrblk/grackle/pkg/tables"
)

// participantsTable is a table of barrier participants indexed by participant ID
//
// Table Primary Key:
// 1. shard key (by account id and namespace id)
// 2. account id
// 3. namespace id
// 4. barrier id
//
// Table Sort Key:
// 1. generation
// 2. process id
type participantsTable struct {
	table *monsterax.BinaryTable[*corepb.BarrierParticipant, corepb.BarrierParticipant]
}

func newParticipantsTable(shardLowerBound []byte, shardUpperBound []byte) *participantsTable {
	return &participantsTable{
		table: monsterax.NewBinaryTable[*corepb.BarrierParticipant, corepb.BarrierParticipant](tables.GrackleBarrierParticipantsTableId, shardLowerBound, shardUpperBound),
	}
}

func (t *participantsTable) GetTableKeyRange() monsterax.KeyRange {
	return t.table.GetTableKeyRange()
}

func (t *participantsTable) Get(txn *store.Txn, accountId uint64, namespaceId uint32, barrierId uint64, generation uint64, processId string) (*corepb.BarrierParticipant, error) {
	return t.table.Get(txn,
		utils.ConcatBytes(
			t.tablePK(accountId, namespaceId, barrierId),
			t.tableSK(generation, processId)))
}

func (t *participantsTable) Create(txn *store.Txn, accountId uint64, namespaceId uint32, barrierId uint64, participant *corepb.BarrierParticipant) error {
	return t.table.Set(txn,
		utils.ConcatBytes(
			t.tablePK(accountId, namespaceId, barrierId),
			t.tableSK(participant.Generation, participant.ProcessId)),
		participant)
}

func (t *participantsTable) Delete(txn *store.Txn, accountId uint64, namespaceId uint32, barrierId uint64, generation uint64, processId string) error {
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

func (t *participantsTable) List(txn *store.Txn, accountId uint64, namespaceId uint32, semaphoreId uint64,
	paginationToken *corepb.PaginationToken, limit int) (*listParticipantResult, error) {
	result, err := t.table.ListPaginated(txn,
		t.tablePK(accountId, namespaceId, semaphoreId),
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

func (t *participantsTable) tablePK(accountId uint64, namespaceId uint32, barrierId uint64) []byte {
	return utils.ConcatBytes(
		sharding.ByAccountAndNamespace(accountId, namespaceId),
		accountId,
		namespaceId,
		barrierId,
	)
}

func (t *participantsTable) tableSK(generation uint64, processId string) []byte {
	return utils.ConcatBytes(
		generation,
		processId,
	)
}
