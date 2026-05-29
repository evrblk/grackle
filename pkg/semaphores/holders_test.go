package semaphores

import (
	"math/rand/v2"
	"testing"
	"time"

	"github.com/evrblk/monstera/store"
	"github.com/evrblk/monstera/utils"
	"github.com/stretchr/testify/require"

	"github.com/evrblk/grackle/pkg/corepb"
)

func TestHoldersTable_Get(t *testing.T) {
	store, err := store.NewBadgerInMemoryStore()
	require.NoError(t, err)

	table := newHoldersTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

	accountId := rand.Uint64()
	namespaceId := rand.Uint32()
	semaphoreId := rand.Uint64()
	leaseId := rand.Uint64()
	now := time.Now()

	holder := &corepb.SemaphoreHolder{
		Id: &corepb.SemaphoreHolderId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			SemaphoreId: semaphoreId,
			LeaseId:     leaseId,
		},
		LockedAt:  now.UnixNano(),
		ExpiresAt: now.Add(time.Hour).UnixNano(),
		Weight:    1,
	}

	// Create holder
	txn := store.Update()
	err = table.Create(txn, holder)
	require.NoError(t, err)
	require.NoError(t, txn.Commit())

	// Get holder
	txn = store.View()
	actual, err := table.Get(txn, holder.Id)
	txn.Discard()

	require.NoError(t, err)
	require.NotNil(t, actual)
	require.Equal(t, holder.Id.AccountId, actual.Id.AccountId)
	require.Equal(t, holder.Id.NamespaceId, actual.Id.NamespaceId)
	require.Equal(t, holder.Id.SemaphoreId, actual.Id.SemaphoreId)
	require.Equal(t, holder.Id.LeaseId, actual.Id.LeaseId)
	require.Equal(t, holder.Weight, actual.Weight)
	require.Equal(t, holder.LockedAt, actual.LockedAt)
	require.Equal(t, holder.ExpiresAt, actual.ExpiresAt)
}

func TestHoldersTable_GetNonExistent(t *testing.T) {
	store, err := store.NewBadgerInMemoryStore()
	require.NoError(t, err)

	table := newHoldersTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

	accountId := rand.Uint64()
	namespaceId := rand.Uint32()
	semaphoreId := rand.Uint64()
	leaseId := rand.Uint64()

	holderId := &corepb.SemaphoreHolderId{
		AccountId:   accountId,
		NamespaceId: namespaceId,
		SemaphoreId: semaphoreId,
		LeaseId:     leaseId,
	}

	txn := store.View()
	_, err = table.Get(txn, holderId)
	txn.Discard()

	require.Error(t, err)
	require.Contains(t, err.Error(), "not found")
}

func TestHoldersTable_Create(t *testing.T) {
	store, err := store.NewBadgerInMemoryStore()
	require.NoError(t, err)

	table := newHoldersTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

	accountId := rand.Uint64()
	namespaceId := rand.Uint32()
	semaphoreId := rand.Uint64()
	leaseId := rand.Uint64()
	now := time.Now()

	holder := &corepb.SemaphoreHolder{
		Id: &corepb.SemaphoreHolderId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			SemaphoreId: semaphoreId,
			LeaseId:     leaseId,
		},
		LockedAt:  now.UnixNano(),
		ExpiresAt: now.Add(time.Hour).UnixNano(),
		Weight:    2,
	}

	// Create holder
	txn := store.Update()
	err = table.Create(txn, holder)
	require.NoError(t, err)
	require.NoError(t, txn.Commit())

	// Verify holder was created in main table
	txn = store.View()
	actual, err := table.Get(txn, holder.Id)
	require.NoError(t, err)
	require.NotNil(t, actual)
	require.Equal(t, holder.Id.LeaseId, actual.Id.LeaseId)
	require.Equal(t, holder.Weight, actual.Weight)

	// Verify holder was created in expiration index
	indexPK := table.expirationIndexPK(accountId, namespaceId, semaphoreId)
	indexSK := table.expirationIndexSK(holder.ExpiresAt, leaseId)
	indexKey := utils.ConcatBytes(indexPK, indexSK)

	exists, err := table.expirationIndex.NotEmpty(txn, indexKey)
	txn.Discard()

	require.NoError(t, err)
	require.True(t, exists)
}

func TestHoldersTable_Update(t *testing.T) {
	store, err := store.NewBadgerInMemoryStore()
	require.NoError(t, err)

	table := newHoldersTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

	accountId := rand.Uint64()
	namespaceId := rand.Uint32()
	semaphoreId := rand.Uint64()
	leaseId := rand.Uint64()
	now := time.Now()

	holder := &corepb.SemaphoreHolder{
		Id: &corepb.SemaphoreHolderId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			SemaphoreId: semaphoreId,
			LeaseId:     leaseId,
		},
		LockedAt:  now.UnixNano(),
		ExpiresAt: now.Add(time.Hour).UnixNano(),
		Weight:    1,
	}

	// Create holder
	txn := store.Update()
	err = table.Create(txn, holder)
	require.NoError(t, err)
	require.NoError(t, txn.Commit())

	// Update holder with new expiration time
	newExpiresAt := now.Add(2 * time.Hour).UnixNano()
	updatedHolder := &corepb.SemaphoreHolder{
		Id:        holder.Id,
		LockedAt:  holder.LockedAt,
		ExpiresAt: newExpiresAt,
		Weight:    3,
	}

	txn = store.Update()
	err = table.Update(txn, updatedHolder)
	require.NoError(t, err)
	require.NoError(t, txn.Commit())

	// Verify holder was updated
	txn = store.View()
	actual, err := table.Get(txn, holder.Id)
	require.NoError(t, err)
	require.Equal(t, newExpiresAt, actual.ExpiresAt)
	require.Equal(t, uint64(3), actual.Weight)

	// Verify old expiration index entry was deleted
	oldIndexKey := utils.ConcatBytes(
		table.expirationIndexPK(accountId, namespaceId, semaphoreId),
		table.expirationIndexSK(holder.ExpiresAt, leaseId),
	)
	exists, err := table.expirationIndex.NotEmpty(txn, oldIndexKey)
	require.NoError(t, err)
	require.False(t, exists)

	// Verify new expiration index entry was created
	newIndexKey := utils.ConcatBytes(
		table.expirationIndexPK(accountId, namespaceId, semaphoreId),
		table.expirationIndexSK(newExpiresAt, leaseId),
	)
	exists, err = table.expirationIndex.NotEmpty(txn, newIndexKey)
	txn.Discard()

	require.NoError(t, err)
	require.True(t, exists)
}

func TestHoldersTable_UpdateSameExpiration(t *testing.T) {
	store, err := store.NewBadgerInMemoryStore()
	require.NoError(t, err)

	table := newHoldersTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

	accountId := rand.Uint64()
	namespaceId := rand.Uint32()
	semaphoreId := rand.Uint64()
	leaseId := rand.Uint64()
	now := time.Now()
	expiresAt := now.Add(time.Hour).UnixNano()

	holder := &corepb.SemaphoreHolder{
		Id: &corepb.SemaphoreHolderId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			SemaphoreId: semaphoreId,
			LeaseId:     leaseId,
		},
		LockedAt:  now.UnixNano(),
		ExpiresAt: expiresAt,
		Weight:    1,
	}

	// Create holder
	txn := store.Update()
	err = table.Create(txn, holder)
	require.NoError(t, err)
	require.NoError(t, txn.Commit())

	// Update holder with same expiration time but different weight
	updatedHolder := &corepb.SemaphoreHolder{
		Id:        holder.Id,
		LockedAt:  holder.LockedAt,
		ExpiresAt: expiresAt,
		Weight:    2,
	}

	txn = store.Update()
	err = table.Update(txn, updatedHolder)
	require.NoError(t, err)
	require.NoError(t, txn.Commit())

	// Verify holder was updated
	txn = store.View()
	actual, err := table.Get(txn, holder.Id)
	require.NoError(t, err)
	require.Equal(t, expiresAt, actual.ExpiresAt)
	require.Equal(t, uint64(2), actual.Weight)

	// Verify expiration index entry still exists
	indexKey := utils.ConcatBytes(
		table.expirationIndexPK(accountId, namespaceId, semaphoreId),
		table.expirationIndexSK(expiresAt, leaseId),
	)
	exists, err := table.expirationIndex.NotEmpty(txn, indexKey)
	txn.Discard()

	require.NoError(t, err)
	require.True(t, exists)
}

func TestHoldersTable_UpdateNonExistent(t *testing.T) {
	store, err := store.NewBadgerInMemoryStore()
	require.NoError(t, err)

	table := newHoldersTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

	accountId := rand.Uint64()
	namespaceId := rand.Uint32()
	semaphoreId := rand.Uint64()
	leaseId := rand.Uint64()
	now := time.Now()

	holder := &corepb.SemaphoreHolder{
		Id: &corepb.SemaphoreHolderId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			SemaphoreId: semaphoreId,
			LeaseId:     leaseId,
		},
		LockedAt:  now.UnixNano(),
		ExpiresAt: now.Add(time.Hour).UnixNano(),
		Weight:    1,
	}

	txn := store.Update()
	err = table.Update(txn, holder)
	txn.Discard()

	require.Error(t, err)
	require.Contains(t, err.Error(), "not found")
}

func TestHoldersTable_Delete(t *testing.T) {
	store, err := store.NewBadgerInMemoryStore()
	require.NoError(t, err)

	table := newHoldersTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

	accountId := rand.Uint64()
	namespaceId := rand.Uint32()
	semaphoreId := rand.Uint64()
	leaseId := rand.Uint64()
	now := time.Now()

	holder := &corepb.SemaphoreHolder{
		Id: &corepb.SemaphoreHolderId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			SemaphoreId: semaphoreId,
			LeaseId:     leaseId,
		},
		LockedAt:  now.UnixNano(),
		ExpiresAt: now.Add(time.Hour).UnixNano(),
		Weight:    1,
	}

	// Create holder
	txn := store.Update()
	err = table.Create(txn, holder)
	require.NoError(t, err)
	require.NoError(t, txn.Commit())

	// Delete holder
	txn = store.Update()
	err = table.Delete(txn, holder.Id)
	require.NoError(t, err)
	require.NoError(t, txn.Commit())

	// Verify holder was deleted from main table
	txn = store.View()
	_, err = table.Get(txn, holder.Id)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not found")

	// Verify holder was deleted from expiration index
	indexKey := utils.ConcatBytes(
		table.expirationIndexPK(accountId, namespaceId, semaphoreId),
		table.expirationIndexSK(holder.ExpiresAt, leaseId),
	)
	exists, err := table.expirationIndex.NotEmpty(txn, indexKey)
	txn.Discard()

	require.NoError(t, err)
	require.False(t, exists)
}

func TestHoldersTable_DeleteNonExistent(t *testing.T) {
	store, err := store.NewBadgerInMemoryStore()
	require.NoError(t, err)

	table := newHoldersTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

	accountId := rand.Uint64()
	namespaceId := rand.Uint32()
	semaphoreId := rand.Uint64()
	leaseId := rand.Uint64()

	holderId := &corepb.SemaphoreHolderId{
		AccountId:   accountId,
		NamespaceId: namespaceId,
		SemaphoreId: semaphoreId,
		LeaseId:     leaseId,
	}

	txn := store.Update()
	err = table.Delete(txn, holderId)
	txn.Discard()

	require.Error(t, err)
	require.Contains(t, err.Error(), "not found")
}

func TestHoldersTable_List(t *testing.T) {
	store, err := store.NewBadgerInMemoryStore()
	require.NoError(t, err)

	table := newHoldersTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

	accountId := rand.Uint64()
	namespaceId := rand.Uint32()
	semaphoreId := rand.Uint64()
	now := time.Now()

	// Create multiple holders
	holders := make([]*corepb.SemaphoreHolder, 5)
	for i := range holders {
		holders[i] = &corepb.SemaphoreHolder{
			Id: &corepb.SemaphoreHolderId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
				SemaphoreId: semaphoreId,
				LeaseId:     uint64(i),
			},
			LockedAt:  now.Add(time.Duration(i) * time.Minute).UnixNano(),
			ExpiresAt: now.Add(time.Duration(i+1) * time.Hour).UnixNano(),
			Weight:    uint64(i + 1),
		}
	}

	// Create all holders
	txn := store.Update()
	for _, holder := range holders {
		err := table.Create(txn, holder)
		require.NoError(t, err)
	}
	require.NoError(t, txn.Commit())

	// List all holders
	txn = store.View()
	result, err := table.List(txn, accountId, namespaceId, semaphoreId, nil, 10)
	txn.Discard()

	require.NoError(t, err)
	require.Len(t, result.holders, 5)
	require.Nil(t, result.nextPaginationToken)

	// Verify holders are sorted by process id (sort key)
	for i, holder := range result.holders {
		require.EqualValues(t, i, holder.Id.LeaseId)
	}
}

func TestHoldersTable_ListWithPagination(t *testing.T) {
	store, err := store.NewBadgerInMemoryStore()
	require.NoError(t, err)

	table := newHoldersTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

	accountId := rand.Uint64()
	namespaceId := rand.Uint32()
	semaphoreId := rand.Uint64()
	now := time.Now()

	// Create multiple holders
	numHolders := 10
	for i := range numHolders {
		holder := &corepb.SemaphoreHolder{
			Id: &corepb.SemaphoreHolderId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
				SemaphoreId: semaphoreId,
				LeaseId:     uint64(i),
			},
			LockedAt:  now.Add(time.Duration(i) * time.Minute).UnixNano(),
			ExpiresAt: now.Add(time.Duration(i+1) * time.Hour).UnixNano(),
			Weight:    uint64(i + 1),
		}

		txn := store.Update()
		err := table.Create(txn, holder)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())
	}

	// List first page
	txn := store.View()
	result1, err := table.List(txn, accountId, namespaceId, semaphoreId, nil, 3)
	txn.Discard()

	require.NoError(t, err)
	require.Len(t, result1.holders, 3)
	require.NotNil(t, result1.nextPaginationToken)
	require.Nil(t, result1.previousPaginationToken)
	require.EqualValues(t, 0, result1.holders[0].Id.LeaseId)
	require.EqualValues(t, 1, result1.holders[1].Id.LeaseId)
	require.EqualValues(t, 2, result1.holders[2].Id.LeaseId)

	// List second page
	txn = store.View()
	result2, err := table.List(txn, accountId, namespaceId, semaphoreId, result1.nextPaginationToken, 3)
	txn.Discard()

	require.NoError(t, err)
	require.Len(t, result2.holders, 3)
	require.NotNil(t, result2.nextPaginationToken)
	require.NotNil(t, result2.previousPaginationToken)
	require.EqualValues(t, 3, result2.holders[0].Id.LeaseId)
	require.EqualValues(t, 4, result2.holders[1].Id.LeaseId)
	require.EqualValues(t, 5, result2.holders[2].Id.LeaseId)

	// List third page
	txn = store.View()
	result3, err := table.List(txn, accountId, namespaceId, semaphoreId, result2.nextPaginationToken, 3)
	txn.Discard()

	require.NoError(t, err)
	require.Len(t, result3.holders, 3)
	require.NotNil(t, result3.nextPaginationToken)
	require.NotNil(t, result3.previousPaginationToken)

	// List fourth page (last page)
	txn = store.View()
	result4, err := table.List(txn, accountId, namespaceId, semaphoreId, result3.nextPaginationToken, 3)
	txn.Discard()

	require.NoError(t, err)
	require.Len(t, result4.holders, 1)
	require.Nil(t, result4.nextPaginationToken)
	require.NotNil(t, result4.previousPaginationToken)
	require.EqualValues(t, 9, result4.holders[0].Id.LeaseId)
}

func TestHoldersTable_ListEmpty(t *testing.T) {
	store, err := store.NewBadgerInMemoryStore()
	require.NoError(t, err)

	table := newHoldersTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

	accountId := rand.Uint64()
	namespaceId := rand.Uint32()
	semaphoreId := rand.Uint64()

	txn := store.View()
	result, err := table.List(txn, accountId, namespaceId, semaphoreId, nil, 10)
	txn.Discard()

	require.NoError(t, err)
	require.Empty(t, result.holders)
	require.Nil(t, result.nextPaginationToken)
}

func TestHoldersTable_ListByExpiration(t *testing.T) {
	store, err := store.NewBadgerInMemoryStore()
	require.NoError(t, err)

	table := newHoldersTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

	accountId := rand.Uint64()
	namespaceId := rand.Uint32()
	semaphoreId := rand.Uint64()
	now := time.Now()

	// Create holders with different expiration times
	holders := []*corepb.SemaphoreHolder{
		{
			Id: &corepb.SemaphoreHolderId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
				SemaphoreId: semaphoreId,
				LeaseId:     1,
			},
			LockedAt:  now.UnixNano(),
			ExpiresAt: now.Add(10 * time.Minute).UnixNano(),
			Weight:    1,
		},
		{
			Id: &corepb.SemaphoreHolderId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
				SemaphoreId: semaphoreId,
				LeaseId:     2,
			},
			LockedAt:  now.UnixNano(),
			ExpiresAt: now.Add(30 * time.Minute).UnixNano(),
			Weight:    1,
		},
		{
			Id: &corepb.SemaphoreHolderId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
				SemaphoreId: semaphoreId,
				LeaseId:     3,
			},
			LockedAt:  now.UnixNano(),
			ExpiresAt: now.Add(50 * time.Minute).UnixNano(),
			Weight:    1,
		},
		{
			Id: &corepb.SemaphoreHolderId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
				SemaphoreId: semaphoreId,
				LeaseId:     4,
			},
			LockedAt:  now.UnixNano(),
			ExpiresAt: now.Add(70 * time.Minute).UnixNano(),
			Weight:    1,
		},
	}

	// Create all holders
	txn := store.Update()
	for _, holder := range holders {
		err := table.Create(txn, holder)
		require.NoError(t, err)
	}
	require.NoError(t, txn.Commit())

	// List holders expiring between 20 and 60 minutes
	from := now.Add(20 * time.Minute).UnixNano()
	to := now.Add(60 * time.Minute).UnixNano()

	var actuals []*corepb.SemaphoreHolder
	txn = store.View()
	err = table.ListByExpiration(txn, &corepb.SemaphoreId{
		AccountId:   accountId,
		NamespaceId: namespaceId,
		SemaphoreId: semaphoreId,
	}, from, to, func(holder *corepb.SemaphoreHolder) (bool, error) {
		actuals = append(actuals, holder)
		return true, nil
	})
	txn.Discard()

	require.NoError(t, err)
	require.Len(t, actuals, 2)
	require.EqualValues(t, 2, actuals[0].Id.LeaseId)
	require.EqualValues(t, 3, actuals[1].Id.LeaseId)
}

func TestHoldersTable_ListByExpirationStopEarly(t *testing.T) {
	store, err := store.NewBadgerInMemoryStore()
	require.NoError(t, err)

	table := newHoldersTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

	accountId := rand.Uint64()
	namespaceId := rand.Uint32()
	semaphoreId := rand.Uint64()
	now := time.Now()

	// Create holders with different expiration times
	for i := range 5 {
		holder := &corepb.SemaphoreHolder{
			Id: &corepb.SemaphoreHolderId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
				SemaphoreId: semaphoreId,
				LeaseId:     uint64(i + 1),
			},
			LockedAt:  now.UnixNano(),
			ExpiresAt: now.Add(time.Duration(i+1) * 10 * time.Minute).UnixNano(),
			Weight:    1,
		}

		txn := store.Update()
		err := table.Create(txn, holder)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())
	}

	// List holders but stop after processing 2 holders
	from := now.UnixNano()
	to := now.Add(time.Hour).UnixNano()

	var actuals []*corepb.SemaphoreHolder
	txn := store.View()
	err = table.ListByExpiration(txn, &corepb.SemaphoreId{
		AccountId:   accountId,
		NamespaceId: namespaceId,
		SemaphoreId: semaphoreId,
	}, from, to, func(holder *corepb.SemaphoreHolder) (bool, error) {
		actuals = append(actuals, holder)
		return len(actuals) < 2, nil
	})
	txn.Discard()

	require.NoError(t, err)
	require.Len(t, actuals, 2)
}

func TestHoldersTable_ListByExpirationEmpty(t *testing.T) {
	store, err := store.NewBadgerInMemoryStore()
	require.NoError(t, err)

	table := newHoldersTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

	accountId := rand.Uint64()
	namespaceId := rand.Uint32()
	semaphoreId := rand.Uint64()
	leaseId := rand.Uint64()
	now := time.Now()

	// Create holders with expiration times outside the query range
	holder := &corepb.SemaphoreHolder{
		Id: &corepb.SemaphoreHolderId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			SemaphoreId: semaphoreId,
			LeaseId:     leaseId,
		},
		LockedAt:  now.UnixNano(),
		ExpiresAt: now.Add(2 * time.Hour).UnixNano(),
		Weight:    1,
	}

	txn := store.Update()
	err = table.Create(txn, holder)
	require.NoError(t, err)
	require.NoError(t, txn.Commit())

	// List holders expiring in a range that doesn't include any holders
	from := now.UnixNano()
	to := now.Add(time.Hour).UnixNano()

	var actuals []*corepb.SemaphoreHolder
	txn = store.View()
	err = table.ListByExpiration(txn, &corepb.SemaphoreId{
		AccountId:   accountId,
		NamespaceId: namespaceId,
		SemaphoreId: semaphoreId,
	}, from, to, func(holder *corepb.SemaphoreHolder) (bool, error) {
		actuals = append(actuals, holder)
		return true, nil
	})
	txn.Discard()

	require.NoError(t, err)
	require.Empty(t, actuals)
}

func TestHoldersTable_MultipleSemaphores(t *testing.T) {
	store, err := store.NewBadgerInMemoryStore()
	require.NoError(t, err)

	table := newHoldersTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

	accountId := rand.Uint64()
	namespaceId := rand.Uint32()
	semaphoreId1 := rand.Uint64()
	semaphoreId2 := rand.Uint64()
	now := time.Now()

	// Create holders for first semaphore
	holder1 := &corepb.SemaphoreHolder{
		Id: &corepb.SemaphoreHolderId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			SemaphoreId: semaphoreId1,
			LeaseId:     1,
		},
		LockedAt:  now.UnixNano(),
		ExpiresAt: now.Add(time.Hour).UnixNano(),
		Weight:    1,
	}

	// Create holders for second semaphore
	holder2 := &corepb.SemaphoreHolder{
		Id: &corepb.SemaphoreHolderId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			SemaphoreId: semaphoreId2,
			LeaseId:     2,
		},
		LockedAt:  now.UnixNano(),
		ExpiresAt: now.Add(time.Hour).UnixNano(),
		Weight:    1,
	}

	txn := store.Update()
	err = table.Create(txn, holder1)
	require.NoError(t, err)
	err = table.Create(txn, holder2)
	require.NoError(t, err)
	require.NoError(t, txn.Commit())

	// List holders for first semaphore
	txn = store.View()
	result1, err := table.List(txn, accountId, namespaceId, semaphoreId1, nil, 10)
	require.NoError(t, err)
	require.Len(t, result1.holders, 1)
	require.EqualValues(t, 1, result1.holders[0].Id.LeaseId)

	// List holders for second semaphore
	result2, err := table.List(txn, accountId, namespaceId, semaphoreId2, nil, 10)
	txn.Discard()

	require.NoError(t, err)
	require.Len(t, result2.holders, 1)
	require.EqualValues(t, 2, result2.holders[0].Id.LeaseId)
}
