package semaphores

import (
	"github.com/evrblk/monstera/store"
	"github.com/evrblk/monstera/utils"
	"github.com/evrblk/yellowstone-common/honey"

	"github.com/evrblk/grackle/pkg/corepb"
	"github.com/evrblk/grackle/pkg/tables"
)

// expirationRecordsTable stores expiration queue items indexed by semaphore id
//
// Index Primary Key:
// 1. shard id
// 2. timestamp
// 3. account id
// 4. namespace id
// 5. semaphore id
//
// Index Prefix:
// 1. shard id
// 2. timestamp
type expirationRecordsTable struct {
	shardGlobalIndexPrefix []byte

	table *honey.BinaryTable[*corepb.SemaphoresExpirationRecord, corepb.SemaphoresExpirationRecord]
}

func newExpirationRecordsTable(shardGlobalIndexPrefix []byte) *expirationRecordsTable {
	return &expirationRecordsTable{
		shardGlobalIndexPrefix: shardGlobalIndexPrefix,

		table: honey.NewBinaryTable[*corepb.SemaphoresExpirationRecord, corepb.SemaphoresExpirationRecord](
			tables.Grackle["Grackle.SemaphoresCore.ExpirationRecords.Table"].Bytes(),
			shardGlobalIndexPrefix,
			shardGlobalIndexPrefix,
		),
	}
}

func (t *expirationRecordsTable) GetTableKeyRange() honey.KeyRange {
	return t.table.GetTableKeyRange()
}

func (t *expirationRecordsTable) List(txn *store.Txn, from int64, to int64, fn func(record *corepb.SemaphoresExpirationRecord) (bool, error)) error {
	return t.table.ListInRange(txn, t.tablePrefix(from), t.tablePrefix(to), false, func(record *corepb.SemaphoresExpirationRecord) (bool, error) {
		return fn(record)
	})
}

func (t *expirationRecordsTable) Delete(txn *store.Txn, expiresAt int64, semaphoreId *corepb.SemaphoreId) error {
	return t.table.Delete(txn,
		t.tablePK(expiresAt, semaphoreId.AccountId, semaphoreId.NamespaceId, semaphoreId.SemaphoreId))
}

func (t *expirationRecordsTable) Add(txn *store.Txn, expiresAt int64, semaphoreId *corepb.SemaphoreId) error {
	return t.table.Set(txn,
		t.tablePK(expiresAt, semaphoreId.AccountId, semaphoreId.NamespaceId, semaphoreId.SemaphoreId),
		&corepb.SemaphoresExpirationRecord{
			ExpiresAt:   expiresAt,
			SemaphoreId: semaphoreId,
		},
	)
}

func (t *expirationRecordsTable) tablePK(time int64, accountId uint64, namespaceId uint64, semaphoreId uint64) []byte {
	return utils.ConcatBytes(
		t.shardGlobalIndexPrefix,
		time,
		accountId,
		namespaceId,
		semaphoreId,
	)
}

func (t *expirationRecordsTable) tablePrefix(time int64) []byte {
	return utils.ConcatBytes(
		t.shardGlobalIndexPrefix,
		time,
	)
}
