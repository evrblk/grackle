package waitgroups

import (
	"github.com/evrblk/monstera/store"
	"github.com/evrblk/monstera/utils"
	"github.com/evrblk/yellowstone-common/honey"

	"github.com/evrblk/grackle/pkg/corepb"
	"github.com/evrblk/grackle/pkg/pagination"
	"github.com/evrblk/grackle/pkg/sharding"
	"github.com/evrblk/grackle/pkg/tables"
)

// jobsTable stores completed jobs for wait groups indexed by job ID.
//
// Table Primary Key:
// 1. account id
// 2. namespace id
// 3. wait group id
//
// Table Sort Key:
// 1. job id
type jobsTable struct {
	table *honey.BinaryTable[*corepb.WaitGroupJob, corepb.WaitGroupJob]
}

// newJobsTable scopes the table under the shard-unique prefix; see
// newWaitGroupsTable.
func newJobsTable(shardPrefix []byte) *jobsTable {
	return &jobsTable{
		table: honey.NewBinaryTable[*corepb.WaitGroupJob, corepb.WaitGroupJob](
			utils.ConcatBytes(tables.Grackle["Grackle.WaitGroupsCore.Jobs.Table"].Bytes(), shardPrefix),
			nil,
			nil,
		),
	}
}

// Clear deletes every job row.
func (t *jobsTable) Clear(badgerStore *store.BadgerStore) error {
	return badgerStore.DropPrefix(t.table.TableId())
}

// EachEntity streams every job as (canonical key, stored value).
func (t *jobsTable) EachEntity(txn *store.Txn, fn func(key []byte, value []byte) (bool, error)) error {
	return t.table.EachEntry(txn, fn)
}

// RestoreEntity decodes one streamed job and, if owned, inserts it through
// Create — re-deriving its key from the job's own identity fields.
func (t *jobsTable) RestoreEntity(txn *store.Txn, key []byte, value []byte, bounds tables.ShardRange) (bool, error) {
	job := &corepb.WaitGroupJob{}
	if err := job.UnmarshalBinary(value); err != nil {
		return false, err
	}
	if !bounds.Owns(sharding.ByAccountAndNamespace(job.Id.AccountId, job.Id.NamespaceId)) {
		return false, nil
	}
	return true, t.Create(txn, job)
}

type listWaitGroupJobsResult struct {
	jobs                    []*corepb.WaitGroupJob
	nextPaginationToken     *corepb.PaginationToken
	previousPaginationToken *corepb.PaginationToken
}

func (t *jobsTable) List(txn *store.Txn, accountId uint64, namespaceId uint64, waitGroupId uint64, paginationToken *corepb.PaginationToken, limit int) (*listWaitGroupJobsResult, error) {
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
			tableSK(waitGroupJobId.JobId)))
}

func (t *jobsTable) Create(txn *store.Txn, waitGroupJob *corepb.WaitGroupJob) error {
	return t.table.Set(txn,
		utils.ConcatBytes(
			tablePK(waitGroupJob.Id.AccountId, waitGroupJob.Id.NamespaceId, waitGroupJob.Id.WaitGroupId),
			tableSK(waitGroupJob.Id.JobId)),
		waitGroupJob)
}

func (t *jobsTable) Delete(txn *store.Txn, waitGroupJobId *corepb.WaitGroupJobId) error {
	return t.table.Delete(txn,
		utils.ConcatBytes(
			tablePK(waitGroupJobId.AccountId, waitGroupJobId.NamespaceId, waitGroupJobId.WaitGroupId),
			tableSK(waitGroupJobId.JobId)))
}

func tablePK(accountId uint64, namespaceId uint64, waitGroupId uint64) []byte {
	return utils.ConcatBytes(
		accountId,
		namespaceId,
		waitGroupId,
	)
}

func tableSK(jobId string) []byte {
	return utils.ConcatBytes(
		jobId,
	)
}
