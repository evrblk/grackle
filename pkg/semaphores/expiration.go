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
// Keys are ordered by time (not by identity), so the shard prefix travels
// in-key instead of in the table id.
//
// Index Primary Key:
// 1. shard prefix
// 2. timestamp
// 3. account id
// 4. namespace id
// 5. semaphore id
//
// Index Prefix:
// 1. shard prefix
// 2. timestamp
type expirationRecordsTable struct {
	shardPrefix []byte

	table *honey.BinaryTable[*corepb.SemaphoresExpirationRecord, corepb.SemaphoresExpirationRecord]
}

func newExpirationRecordsTable(shardPrefix []byte) *expirationRecordsTable {
	return &expirationRecordsTable{
		shardPrefix: shardPrefix,

		table: honey.NewBinaryTable[*corepb.SemaphoresExpirationRecord, corepb.SemaphoresExpirationRecord](
			tables.Grackle["Grackle.SemaphoresCore.ExpirationRecords.Table"].Bytes(),
			shardPrefix,
			shardPrefix,
		),
	}
}

// Clear deletes every expiration record row of this shard (keys carry the
// shard prefix in-key under the registry table id).
func (t *expirationRecordsTable) Clear(badgerStore *store.BadgerStore) error {
	return badgerStore.DropPrefix(utils.ConcatBytes(t.table.TableId(), t.shardPrefix))
}

// EachEntity streams every expiration record as (canonical key, stored
// value). The stored key embeds this shard's prefix — identity of the
// producing shard, which must not travel in a portable stream — so the
// canonical key starts at the timestamp.
func (t *expirationRecordsTable) EachEntity(txn *store.Txn, fn func(key []byte, value []byte) (bool, error)) error {
	return t.table.EachEntry(txn, func(key []byte, value []byte) (bool, error) {
		return fn(key[len(t.shardPrefix):], value)
	})
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
		t.shardPrefix,
		time,
		accountId,
		namespaceId,
		semaphoreId,
	)
}

func (t *expirationRecordsTable) tablePrefix(time int64) []byte {
	return utils.ConcatBytes(
		t.shardPrefix,
		time,
	)
}
