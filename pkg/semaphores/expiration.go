package semaphores

import (
	"github.com/evrblk/monstera/store"
	"github.com/evrblk/monstera/utils"
	"github.com/evrblk/yellowstone-common/honey"

	"github.com/evrblk/grackle/pkg/corepb"
	"github.com/evrblk/grackle/pkg/sharding"
	"github.com/evrblk/grackle/pkg/tables"
)

// expirationRecordsTable stores expiration queue items indexed by semaphore id.
// Keys are ordered by time (not by identity); the shard prefix lives in the
// table id, not in the record key.
//
// Index Primary Key:
// 1. timestamp
// 2. account id
// 3. namespace id
// 4. semaphore id
//
// Index Prefix:
// 1. timestamp
type expirationRecordsTable struct {
	table *honey.BinaryTable[*corepb.SemaphoresExpirationRecord, corepb.SemaphoresExpirationRecord]
}

func newExpirationRecordsTable(replicaPrefix []byte) *expirationRecordsTable {
	return &expirationRecordsTable{
		table: honey.NewBinaryTable[*corepb.SemaphoresExpirationRecord, corepb.SemaphoresExpirationRecord](
			utils.ConcatBytes(replicaPrefix, tablePrefixExpirationRecords),
		),
	}
}

// Clear deletes every expiration record row of this shard (all rows under the
// table id, which is scoped by this shard's prefix).
func (t *expirationRecordsTable) Clear(badgerStore *store.BadgerStore) error {
	return badgerStore.DeletePrefix(t.table.TableId())
}

// EachEntity streams every expiration record as (canonical key, stored value).
func (t *expirationRecordsTable) EachEntity(txn *store.Txn, fn func(key []byte, value []byte) (bool, error)) error {
	return t.table.EachEntry(txn, fn)
}

// RestoreEntity decodes one streamed expiration record and, if owned, inserts
// it through Add — which re-derives its key under this table's own prefix
// (the canonical key is not needed; identity comes from the record itself).
func (t *expirationRecordsTable) RestoreEntity(txn *store.Txn, key []byte, value []byte, bounds tables.ShardRange) (bool, error) {
	record := &corepb.SemaphoresExpirationRecord{}
	if err := record.UnmarshalBinary(value); err != nil {
		return false, err
	}
	if !bounds.Owns(sharding.ByAccountAndNamespace(record.SemaphoreId.AccountId, record.SemaphoreId.NamespaceId)) {
		return false, nil
	}
	return true, t.Add(txn, record.ExpiresAt, record.SemaphoreId)
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
		time,
		accountId,
		namespaceId,
		semaphoreId,
	)
}

func (t *expirationRecordsTable) tablePrefix(time int64) []byte {
	return utils.ConcatBytes(
		time,
	)
}
