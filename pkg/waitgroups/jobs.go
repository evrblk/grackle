package waitgroups

import (
	"github.com/evrblk/monstera/store"
	"github.com/evrblk/monstera/utils"
	monsterax "github.com/evrblk/monstera/x"

	"github.com/evrblk/grackle/pkg/corepb"
	"github.com/evrblk/grackle/pkg/pagination"
	"github.com/evrblk/grackle/pkg/sharding"
	"github.com/evrblk/grackle/pkg/tables"
)

// jobsTable stores completed jobs for wait groups indexed by process ID.
//
// Table Primary Key:
// 1. shard key (by account id and namespace id)
// 2. account id
// 3. namespace id
// 4. wait group id
//
// Table Sort Key:
// 1. process id
type jobsTable struct {
	table *monsterax.BinaryTable[*corepb.WaitGroupJob, corepb.WaitGroupJob]
}

func newJobsTable(shardLowerBound []byte, shardUpperBound []byte) *jobsTable {
	return &jobsTable{
		table: monsterax.NewBinaryTable[*corepb.WaitGroupJob, corepb.WaitGroupJob](
			tables.Grackle["Grackle.WaitGroupsCore.Jobs.Table"].Bytes(),
			shardLowerBound,
			shardUpperBound,
		),
	}
}

func (t *jobsTable) GetTableKeyRange() monsterax.KeyRange {
	return t.table.GetTableKeyRange()
}

type listWaitGroupJobsResult struct {
	jobs                    []*corepb.WaitGroupJob
	nextPaginationToken     *corepb.PaginationToken
	previousPaginationToken *corepb.PaginationToken
}

func (t *jobsTable) List(txn *store.Txn, accountId uint64, namespaceId uint32, waitGroupId uint64, paginationToken *corepb.PaginationToken, limit int) (*listWaitGroupJobsResult, error) {
	result, err := t.table.ListPaginated(txn, tablePK(accountId, namespaceId, waitGroupId), pagination.CoreToMonstera(paginationToken), limit)
	if err != nil {
		return nil, err
	}

	return &listWaitGroupJobsResult{
		jobs:                    result.Items,
		nextPaginationToken:     pagination.MonsteraToCore(result.NextPaginationToken),
		previousPaginationToken: pagination.MonsteraToCore(result.PreviousPaginationToken),
	}, nil
}

func (t *jobsTable) Get(txn *store.Txn, waitGroupJobId *corepb.WaitGroupJobId) (*corepb.WaitGroupJob, error) {
	return t.table.Get(txn,
		utils.ConcatBytes(
			tablePK(waitGroupJobId.AccountId, waitGroupJobId.NamespaceId, waitGroupJobId.WaitGroupId),
			tableSK(waitGroupJobId.ProcessId)))
}

func (t *jobsTable) Create(txn *store.Txn, waitGroupJob *corepb.WaitGroupJob) error {
	return t.table.Set(txn,
		utils.ConcatBytes(
			tablePK(waitGroupJob.Id.AccountId, waitGroupJob.Id.NamespaceId, waitGroupJob.Id.WaitGroupId),
			tableSK(waitGroupJob.Id.ProcessId)),
		waitGroupJob)
}

func (t *jobsTable) Delete(txn *store.Txn, waitGroupJobId *corepb.WaitGroupJobId) error {
	return t.table.Delete(txn,
		utils.ConcatBytes(
			tablePK(waitGroupJobId.AccountId, waitGroupJobId.NamespaceId, waitGroupJobId.WaitGroupId),
			tableSK(waitGroupJobId.ProcessId)))
}

func tablePK(accountId uint64, namespaceId uint32, waitGroupId uint64) []byte {
	return utils.ConcatBytes(
		sharding.ByAccountAndNamespace(accountId, namespaceId),
		accountId,
		namespaceId,
		waitGroupId,
	)
}

func tableSK(processId string) []byte {
	return utils.ConcatBytes(
		processId,
	)
}
