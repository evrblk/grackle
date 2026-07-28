package waitgroups

import (
	"github.com/evrblk/monstera/store"
	"github.com/evrblk/monstera/utils"
	"github.com/evrblk/yellowstone-common/honey"

	"github.com/evrblk/grackle/pkg/corepb"
	"github.com/evrblk/grackle/pkg/sharding"
	"github.com/evrblk/grackle/pkg/tables"
)

// expirationRecordsTable stores wait group expiration records indexed by wait group ID and expiration time.
//
// Keys are ordered by time (not by identity), so the shard prefix travels
// in-key instead of in the table id.
//
// Table Primary Key:
// 1. shard prefix
// 2. timestamp
// 3. account id
// 4. namespace id
// 5. wait group id
//
// Table Prefix:
// 1. shard prefix
// 2. timestamp
type expirationRecordsTable struct {
	shardPrefix []byte

	table *honey.BinaryTable[*corepb.WaitGroupsExpirationRecord, corepb.WaitGroupsExpirationRecord]
}

func newExpirationRecordsTable(shardPrefix []byte) *expirationRecordsTable {
	return &expirationRecordsTable{
		shardPrefix: shardPrefix,

		table: honey.NewBinaryTable[*corepb.WaitGroupsExpirationRecord, corepb.WaitGroupsExpirationRecord](
			tables.Grackle["Grackle.WaitGroupsCore.ExpirationRecords.Table"].Bytes(),
			shardPrefix,
			shardPrefix,
		),
	}
}

// Clear deletes every expiration record row of this shard (keys carry the
// shard prefix in-key under the registry table id).
func (t *expirationRecordsTable) Clear(badgerStore *store.BadgerStore) error {
	return badgerStore.DeletePrefix(utils.ConcatBytes(t.table.TableId(), t.shardPrefix))
}

// EachEntity streams every expiration record as (canonical key, stored value).
// The stored key embeds this shard's prefix — identity of the producing
// shard, which must not travel in a portable stream — so the canonical key
// starts at the timestamp.
func (t *expirationRecordsTable) EachEntity(txn *store.Txn, fn func(key []byte, value []byte) (bool, error)) error {
	return t.table.EachEntry(txn, func(key []byte, value []byte) (bool, error) {
		return fn(key[len(t.shardPrefix):], value)
	})
}

// RestoreEntity decodes one streamed expiration record and, if owned, inserts it
// through Add — which re-derives its key under this table's own prefix.
func (t *expirationRecordsTable) RestoreEntity(txn *store.Txn, key []byte, value []byte, bounds tables.ShardRange) (bool, error) {
	record := &corepb.WaitGroupsExpirationRecord{}
	if err := record.UnmarshalBinary(value); err != nil {
		return false, err
	}
	if !bounds.Owns(sharding.ByAccountAndNamespace(record.WaitGroupId.AccountId, record.WaitGroupId.NamespaceId)) {
		return false, nil
	}
	return true, t.Add(txn, record.ExpiresAt, record.WaitGroupId)
}

func (t *expirationRecordsTable) Delete(txn *store.Txn, expiresAt int64, waitGroupId *corepb.WaitGroupId) error {
	return t.table.Delete(txn,
		t.tablePK(expiresAt, waitGroupId.AccountId, waitGroupId.NamespaceId, waitGroupId.WaitGroupId),
	)
}

func (t *expirationRecordsTable) Add(txn *store.Txn, expiresAt int64, waitGroupId *corepb.WaitGroupId) error {
	return t.table.Set(txn,
		t.tablePK(expiresAt, waitGroupId.AccountId, waitGroupId.NamespaceId, waitGroupId.WaitGroupId),
		&corepb.WaitGroupsExpirationRecord{
			ExpiresAt:   expiresAt,
			WaitGroupId: waitGroupId,
		})
}

func (t *expirationRecordsTable) ListByExpiration(txn *store.Txn, from int64, to int64, fn func(record *corepb.WaitGroupsExpirationRecord) (bool, error)) error {
	return t.table.ListInRange(txn, t.tablePrefix(from), t.tablePrefix(to), false, func(record *corepb.WaitGroupsExpirationRecord) (bool, error) {
		return fn(record)
	})
}

func (t *expirationRecordsTable) tablePK(time int64, accountId uint64, namespaceId uint64, waitGroupId uint64) []byte {
	return utils.ConcatBytes(
		t.shardPrefix,
		time,
		accountId,
		namespaceId,
		waitGroupId,
	)
}

func (t *expirationRecordsTable) tablePrefix(time int64) []byte {
	return utils.ConcatBytes(
		t.shardPrefix,
		time,
	)
}
