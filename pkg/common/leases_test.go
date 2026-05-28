package common

import (
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/evrblk/monstera/store"
	"github.com/stretchr/testify/require"

	"github.com/evrblk/grackle/pkg/corepb"
)

func TestLeasesTable_Get(t *testing.T) {
	store, err := store.NewBadgerInMemoryStore()
	require.NoError(t, err)

	table := NewLeasesTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff}, []byte{0x1d, 0x36, 0x00, 0x00}, []byte{0x01}, []byte{0x02}, []byte{0x03})

	accountId := rand.Uint64()
	namespaceId := rand.Uint32()
	leaseId := rand.Uint64()
	processId := "test_process_1"
	now := time.Now()

	lease := &corepb.Lease{
		Id: &corepb.LeaseId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LeaseId:     leaseId,
		},
		ProcessId: processId,
		ExpiresAt: now.Add(time.Hour).UnixNano(),
		CreatedAt: now.UnixNano(),
	}

	// Create lease
	txn := store.Update()
	err = table.Create(txn, lease)
	require.NoError(t, err)
	require.NoError(t, txn.Commit())

	// Get lease
	txn = store.View()
	actual, err := table.Get(txn, lease.Id)
	txn.Discard()

	require.NoError(t, err)
	require.NotNil(t, actual)
	require.Equal(t, lease.Id.AccountId, actual.Id.AccountId)
	require.Equal(t, lease.Id.NamespaceId, actual.Id.NamespaceId)
	require.Equal(t, lease.Id.LeaseId, actual.Id.LeaseId)
	require.Equal(t, lease.ProcessId, actual.ProcessId)
	require.Equal(t, lease.ExpiresAt, actual.ExpiresAt)
	require.Equal(t, lease.CreatedAt, actual.CreatedAt)
}

func TestLeasesTable_GetNonExistent(t *testing.T) {
	store, err := store.NewBadgerInMemoryStore()
	require.NoError(t, err)

	table := NewLeasesTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff}, []byte{0x1d, 0x36, 0x00, 0x00}, []byte{0x01}, []byte{0x02}, []byte{0x03})

	accountId := rand.Uint64()
	namespaceId := rand.Uint32()
	leaseId := rand.Uint64()

	leaseIdObj := &corepb.LeaseId{
		AccountId:   accountId,
		NamespaceId: namespaceId,
		LeaseId:     leaseId,
	}

	txn := store.View()
	_, err = table.Get(txn, leaseIdObj)
	txn.Discard()

	require.Error(t, err)
	require.Contains(t, err.Error(), "not found")
}

func TestLeasesTable_CreateMultipleLeases(t *testing.T) {
	store, err := store.NewBadgerInMemoryStore()
	require.NoError(t, err)

	table := NewLeasesTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff}, []byte{0x1d, 0x36, 0x00, 0x00}, []byte{0x01}, []byte{0x02}, []byte{0x03})

	accountId := rand.Uint64()
	namespaceId := rand.Uint32()
	processId := "test_process_1"
	now := time.Now()

	// Create multiple leases
	leases := []*corepb.Lease{
		{
			Id: &corepb.LeaseId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
				LeaseId:     rand.Uint64(),
			},
			ProcessId: processId,
			ExpiresAt: now.Add(time.Hour).UnixNano(),
			CreatedAt: now.UnixNano(),
		},
		{
			Id: &corepb.LeaseId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
				LeaseId:     rand.Uint64(),
			},
			ProcessId: processId,
			ExpiresAt: now.Add(2 * time.Hour).UnixNano(),
			CreatedAt: now.UnixNano(),
		},
		{
			Id: &corepb.LeaseId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
				LeaseId:     rand.Uint64(),
			},
			ProcessId: processId,
			ExpiresAt: now.Add(3 * time.Hour).UnixNano(),
			CreatedAt: now.UnixNano(),
		},
	}

	txn := store.Update()
	for _, lease := range leases {
		err = table.Create(txn, lease)
		require.NoError(t, err)
	}
	require.NoError(t, txn.Commit())

	// Verify all leases were created
	txn = store.View()
	for _, lease := range leases {
		actual, err := table.Get(txn, lease.Id)
		require.NoError(t, err)
		require.NotNil(t, actual)
		require.Equal(t, lease.Id.LeaseId, actual.Id.LeaseId)
	}
	txn.Discard()
}

func TestLeasesTable_Update(t *testing.T) {
	store, err := store.NewBadgerInMemoryStore()
	require.NoError(t, err)

	table := NewLeasesTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff}, []byte{0x1d, 0x36, 0x00, 0x00}, []byte{0x01}, []byte{0x02}, []byte{0x03})

	accountId := rand.Uint64()
	namespaceId := rand.Uint32()
	leaseId := rand.Uint64()
	processId := "test_process_1"
	now := time.Now()

	lease := &corepb.Lease{
		Id: &corepb.LeaseId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LeaseId:     leaseId,
		},
		ProcessId: processId,
		ExpiresAt: now.Add(time.Hour).UnixNano(),
		CreatedAt: now.UnixNano(),
	}

	// Create lease
	txn := store.Update()
	err = table.Create(txn, lease)
	require.NoError(t, err)
	require.NoError(t, txn.Commit())

	// Update lease with new expiration time
	newExpiresAt := now.Add(2 * time.Hour).UnixNano()
	lease.ExpiresAt = newExpiresAt

	txn = store.Update()
	err = table.Update(txn, lease)
	require.NoError(t, err)
	require.NoError(t, txn.Commit())

	// Verify lease was updated
	txn = store.View()
	actual, err := table.Get(txn, lease.Id)
	txn.Discard()

	require.NoError(t, err)
	require.NotNil(t, actual)
	require.Equal(t, newExpiresAt, actual.ExpiresAt)
}

func TestLeasesTable_UpdateNonExistent(t *testing.T) {
	store, err := store.NewBadgerInMemoryStore()
	require.NoError(t, err)

	table := NewLeasesTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff}, []byte{0x1d, 0x36, 0x00, 0x00}, []byte{0x01}, []byte{0x02}, []byte{0x03})

	accountId := rand.Uint64()
	namespaceId := rand.Uint32()
	leaseId := rand.Uint64()
	processId := "test_process_1"
	now := time.Now()

	lease := &corepb.Lease{
		Id: &corepb.LeaseId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LeaseId:     leaseId,
		},
		ProcessId: processId,
		ExpiresAt: now.Add(time.Hour).UnixNano(),
		CreatedAt: now.UnixNano(),
	}

	txn := store.Update()
	err = table.Update(txn, lease)
	txn.Discard()

	require.Error(t, err)
	require.Contains(t, err.Error(), "not found")
}

func TestLeasesTable_UpdateExpirationIndexMaintenance(t *testing.T) {
	store, err := store.NewBadgerInMemoryStore()
	require.NoError(t, err)

	table := NewLeasesTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff}, []byte{0x1d, 0x36, 0x00, 0x00}, []byte{0x01}, []byte{0x02}, []byte{0x03})

	accountId := rand.Uint64()
	namespaceId := rand.Uint32()
	leaseId := rand.Uint64()
	processId := "test_process_1"
	now := time.Now()

	lease := &corepb.Lease{
		Id: &corepb.LeaseId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LeaseId:     leaseId,
		},
		ProcessId: processId,
		ExpiresAt: now.Add(time.Hour).UnixNano(),
		CreatedAt: now.UnixNano(),
	}

	// Create lease
	txn := store.Update()
	err = table.Create(txn, lease)
	require.NoError(t, err)
	require.NoError(t, txn.Commit())

	// Verify lease appears in expiration index at old expiration time
	oldExpiresAt := lease.ExpiresAt
	txn = store.View()
	foundOld := false
	err = table.ListByExpiration(txn, oldExpiresAt-1, oldExpiresAt+1, func(l *corepb.Lease) (bool, error) {
		if l.Id.LeaseId == leaseId {
			foundOld = true
			return false, nil // stop iteration
		}
		return true, nil // continue
	})
	txn.Discard()
	require.NoError(t, err)
	require.True(t, foundOld, "Lease should be found at old expiration time")

	// Update lease with new expiration time
	newExpiresAt := now.Add(3 * time.Hour).UnixNano()
	lease.ExpiresAt = newExpiresAt

	txn = store.Update()
	err = table.Update(txn, lease)
	require.NoError(t, err)
	require.NoError(t, txn.Commit())

	// Verify lease no longer appears at old expiration time
	txn = store.View()
	foundAtOld := false
	err = table.ListByExpiration(txn, oldExpiresAt-1, oldExpiresAt+1, func(l *corepb.Lease) (bool, error) {
		if l.Id.LeaseId == leaseId {
			foundAtOld = true
			return false, nil
		}
		return true, nil
	})
	txn.Discard()
	require.NoError(t, err)
	require.False(t, foundAtOld, "Lease should not be found at old expiration time after update")

	// Verify lease appears at new expiration time
	txn = store.View()
	foundAtNew := false
	err = table.ListByExpiration(txn, newExpiresAt-1, newExpiresAt+1, func(l *corepb.Lease) (bool, error) {
		if l.Id.LeaseId == leaseId {
			foundAtNew = true
			return false, nil
		}
		return true, nil
	})
	txn.Discard()
	require.NoError(t, err)
	require.True(t, foundAtNew, "Lease should be found at new expiration time after update")
}

func TestLeasesTable_Delete(t *testing.T) {
	store, err := store.NewBadgerInMemoryStore()
	require.NoError(t, err)

	table := NewLeasesTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff}, []byte{0x1d, 0x36, 0x00, 0x00}, []byte{0x01}, []byte{0x02}, []byte{0x03})

	accountId := rand.Uint64()
	namespaceId := rand.Uint32()
	leaseId := rand.Uint64()
	processId := "test_process_1"
	now := time.Now()

	lease := &corepb.Lease{
		Id: &corepb.LeaseId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LeaseId:     leaseId,
		},
		ProcessId: processId,
		ExpiresAt: now.Add(time.Hour).UnixNano(),
		CreatedAt: now.UnixNano(),
	}

	// Create lease
	txn := store.Update()
	err = table.Create(txn, lease)
	require.NoError(t, err)
	require.NoError(t, txn.Commit())

	// Delete lease
	txn = store.Update()
	err = table.Delete(txn, lease)
	require.NoError(t, err)
	require.NoError(t, txn.Commit())

	// Verify lease was deleted
	txn = store.View()
	_, err = table.Get(txn, lease.Id)
	txn.Discard()

	require.Error(t, err)
	require.Contains(t, err.Error(), "not found")
}

func TestLeasesTable_DeleteIndexMaintenance(t *testing.T) {
	store, err := store.NewBadgerInMemoryStore()
	require.NoError(t, err)

	table := NewLeasesTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff}, []byte{0x1d, 0x36, 0x00, 0x00}, []byte{0x01}, []byte{0x02}, []byte{0x03})

	accountId := rand.Uint64()
	namespaceId := rand.Uint32()
	leaseId := rand.Uint64()
	processId := "test_process_1"
	now := time.Now()

	lease := &corepb.Lease{
		Id: &corepb.LeaseId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LeaseId:     leaseId,
		},
		ProcessId: processId,
		ExpiresAt: now.Add(time.Hour).UnixNano(),
		CreatedAt: now.UnixNano(),
	}

	// Create lease
	txn := store.Update()
	err = table.Create(txn, lease)
	require.NoError(t, err)
	require.NoError(t, txn.Commit())

	// Delete lease
	txn = store.Update()
	err = table.Delete(txn, lease)
	require.NoError(t, err)
	require.NoError(t, txn.Commit())

	// Verify lease is not in expiration index
	txn = store.View()
	foundInExpiration := false
	err = table.ListByExpiration(txn, lease.ExpiresAt-1, lease.ExpiresAt+1, func(l *corepb.Lease) (bool, error) {
		if l.Id.LeaseId == leaseId {
			foundInExpiration = true
			return false, nil
		}
		return true, nil
	})
	txn.Discard()
	require.NoError(t, err)
	require.False(t, foundInExpiration, "Lease should not be in expiration index after delete")

	// Verify lease is not in process ID index
	txn = store.View()
	result, err := table.ListByProcessId(txn, &corepb.NamespaceId{
		AccountId:   accountId,
		NamespaceId: namespaceId,
	}, processId, nil, 100)
	txn.Discard()
	require.NoError(t, err)
	require.Empty(t, result.Leases, "Lease should not be in process ID index after delete")
}

func TestLeasesTable_List(t *testing.T) {
	store, err := store.NewBadgerInMemoryStore()
	require.NoError(t, err)

	table := NewLeasesTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff}, []byte{0x1d, 0x36, 0x00, 0x00}, []byte{0x01}, []byte{0x02}, []byte{0x03})

	accountId := rand.Uint64()
	namespaceId := rand.Uint32()
	processId := "test_process_1"
	now := time.Now()

	// Create multiple leases
	numLeases := 5
	leases := make([]*corepb.Lease, numLeases)
	for i := 0; i < numLeases; i++ {
		leases[i] = &corepb.Lease{
			Id: &corepb.LeaseId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
				LeaseId:     rand.Uint64(),
			},
			ProcessId: processId,
			ExpiresAt: now.Add(time.Duration(i) * time.Hour).UnixNano(),
			CreatedAt: now.UnixNano(),
		}
	}

	txn := store.Update()
	for _, lease := range leases {
		err = table.Create(txn, lease)
		require.NoError(t, err)
	}
	require.NoError(t, txn.Commit())

	// List all leases
	txn = store.View()
	result, err := table.List(txn, &corepb.NamespaceId{
		AccountId:   accountId,
		NamespaceId: namespaceId,
	}, nil, 100)
	txn.Discard()

	require.NoError(t, err)
	require.Len(t, result.Leases, numLeases)
}

func TestLeasesTable_ListEmpty(t *testing.T) {
	store, err := store.NewBadgerInMemoryStore()
	require.NoError(t, err)

	table := NewLeasesTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff}, []byte{0x1d, 0x36, 0x00, 0x00}, []byte{0x01}, []byte{0x02}, []byte{0x03})

	accountId := rand.Uint64()
	namespaceId := rand.Uint32()

	txn := store.View()
	result, err := table.List(txn, &corepb.NamespaceId{
		AccountId:   accountId,
		NamespaceId: namespaceId,
	}, nil, 100)
	txn.Discard()

	require.NoError(t, err)
	require.Empty(t, result.Leases)
}

func TestLeasesTable_ListWithPagination(t *testing.T) {
	store, err := store.NewBadgerInMemoryStore()
	require.NoError(t, err)

	table := NewLeasesTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff}, []byte{0x1d, 0x36, 0x00, 0x00}, []byte{0x01}, []byte{0x02}, []byte{0x03})

	accountId := rand.Uint64()
	namespaceId := rand.Uint32()
	processId := "test_process_1"
	now := time.Now()

	// Create 10 leases
	numLeases := 10
	leases := make([]*corepb.Lease, numLeases)
	for i := 0; i < numLeases; i++ {
		leases[i] = &corepb.Lease{
			Id: &corepb.LeaseId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
				LeaseId:     uint64(i + 1), // Use sequential IDs for predictable ordering
			},
			ProcessId: processId,
			ExpiresAt: now.Add(time.Duration(i) * time.Hour).UnixNano(),
			CreatedAt: now.UnixNano(),
		}
	}

	txn := store.Update()
	for _, lease := range leases {
		err = table.Create(txn, lease)
		require.NoError(t, err)
	}
	require.NoError(t, txn.Commit())

	// List with pagination (3 per page)
	pageSize := 3
	allFetchedLeases := make([]*corepb.Lease, 0)
	var paginationToken *corepb.PaginationToken

	for {
		txn = store.View()
		result, err := table.List(txn, &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
		}, paginationToken, pageSize)
		txn.Discard()

		require.NoError(t, err)
		allFetchedLeases = append(allFetchedLeases, result.Leases...)

		if result.NextPaginationToken == nil {
			break
		}
		paginationToken = result.NextPaginationToken
	}

	// Verify we got all leases
	require.Len(t, allFetchedLeases, numLeases)
}

func TestLeasesTable_ListByExpiration(t *testing.T) {
	store, err := store.NewBadgerInMemoryStore()
	require.NoError(t, err)

	table := NewLeasesTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff}, []byte{0x1d, 0x36, 0x00, 0x00}, []byte{0x01}, []byte{0x02}, []byte{0x03})

	accountId := rand.Uint64()
	namespaceId := rand.Uint32()
	processId := "test_process_1"
	now := time.Now()

	// Create leases with different expiration times
	leases := []*corepb.Lease{
		{
			Id: &corepb.LeaseId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
				LeaseId:     rand.Uint64(),
			},
			ProcessId: processId,
			ExpiresAt: now.Add(1 * time.Hour).UnixNano(),
			CreatedAt: now.UnixNano(),
		},
		{
			Id: &corepb.LeaseId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
				LeaseId:     rand.Uint64(),
			},
			ProcessId: processId,
			ExpiresAt: now.Add(2 * time.Hour).UnixNano(),
			CreatedAt: now.UnixNano(),
		},
		{
			Id: &corepb.LeaseId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
				LeaseId:     rand.Uint64(),
			},
			ProcessId: processId,
			ExpiresAt: now.Add(3 * time.Hour).UnixNano(),
			CreatedAt: now.UnixNano(),
		},
	}

	txn := store.Update()
	for _, lease := range leases {
		err = table.Create(txn, lease)
		require.NoError(t, err)
	}
	require.NoError(t, txn.Commit())

	// List leases expiring in the next 2.5 hours
	txn = store.View()
	foundLeases := make([]*corepb.Lease, 0)
	err = table.ListByExpiration(txn, now.UnixNano(), now.Add(150*time.Minute).UnixNano(), func(lease *corepb.Lease) (bool, error) {
		foundLeases = append(foundLeases, lease)
		return true, nil // continue iteration
	})
	txn.Discard()

	require.NoError(t, err)
	require.Len(t, foundLeases, 2, "Should find 2 leases expiring in the next 2.5 hours")
}

func TestLeasesTable_ListByExpirationEmpty(t *testing.T) {
	store, err := store.NewBadgerInMemoryStore()
	require.NoError(t, err)

	table := NewLeasesTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff}, []byte{0x1d, 0x36, 0x00, 0x00}, []byte{0x01}, []byte{0x02}, []byte{0x03})

	now := time.Now()

	// List leases in empty table
	txn := store.View()
	foundLeases := make([]*corepb.Lease, 0)
	err = table.ListByExpiration(txn, now.UnixNano(), now.Add(time.Hour).UnixNano(), func(lease *corepb.Lease) (bool, error) {
		foundLeases = append(foundLeases, lease)
		return true, nil
	})
	txn.Discard()

	require.NoError(t, err)
	require.Empty(t, foundLeases, "Should find no leases in empty table")
}

func TestLeasesTable_ListByExpirationEarlyStop(t *testing.T) {
	store, err := store.NewBadgerInMemoryStore()
	require.NoError(t, err)

	table := NewLeasesTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff}, []byte{0x1d, 0x36, 0x00, 0x00}, []byte{0x01}, []byte{0x02}, []byte{0x03})

	accountId := rand.Uint64()
	namespaceId := rand.Uint32()
	processId := "test_process_1"
	now := time.Now()

	// Create multiple leases
	numLeases := 10
	for i := 0; i < numLeases; i++ {
		lease := &corepb.Lease{
			Id: &corepb.LeaseId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
				LeaseId:     rand.Uint64(),
			},
			ProcessId: processId,
			ExpiresAt: now.Add(time.Duration(i) * time.Minute).UnixNano(),
			CreatedAt: now.UnixNano(),
		}

		txn := store.Update()
		err = table.Create(txn, lease)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())
	}

	// List with early stop after 3 leases
	txn := store.View()
	foundLeases := make([]*corepb.Lease, 0)
	err = table.ListByExpiration(txn, now.UnixNano(), now.Add(time.Hour).UnixNano(), func(lease *corepb.Lease) (bool, error) {
		foundLeases = append(foundLeases, lease)
		if len(foundLeases) >= 3 {
			return false, nil // stop iteration
		}
		return true, nil // continue
	})
	txn.Discard()

	require.NoError(t, err)
	require.Len(t, foundLeases, 3, "Should stop after finding 3 leases")
}

func TestLeasesTable_ListByProcessId(t *testing.T) {
	store, err := store.NewBadgerInMemoryStore()
	require.NoError(t, err)

	table := NewLeasesTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff}, []byte{0x1d, 0x36, 0x00, 0x00}, []byte{0x01}, []byte{0x02}, []byte{0x03})

	accountId := rand.Uint64()
	namespaceId := rand.Uint32()
	processId1 := "test_process_1"
	processId2 := "test_process_2"
	now := time.Now()

	// Create leases for two different processes
	leasesForProcess1 := []*corepb.Lease{
		{
			Id: &corepb.LeaseId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
				LeaseId:     rand.Uint64(),
			},
			ProcessId: processId1,
			ExpiresAt: now.Add(time.Hour).UnixNano(),
			CreatedAt: now.UnixNano(),
		},
		{
			Id: &corepb.LeaseId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
				LeaseId:     rand.Uint64(),
			},
			ProcessId: processId1,
			ExpiresAt: now.Add(2 * time.Hour).UnixNano(),
			CreatedAt: now.UnixNano(),
		},
	}

	leasesForProcess2 := []*corepb.Lease{
		{
			Id: &corepb.LeaseId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
				LeaseId:     rand.Uint64(),
			},
			ProcessId: processId2,
			ExpiresAt: now.Add(time.Hour).UnixNano(),
			CreatedAt: now.UnixNano(),
		},
	}

	txn := store.Update()
	for _, lease := range append(leasesForProcess1, leasesForProcess2...) {
		err = table.Create(txn, lease)
		require.NoError(t, err)
	}
	require.NoError(t, txn.Commit())

	// List leases for process 1
	txn = store.View()
	result, err := table.ListByProcessId(txn, &corepb.NamespaceId{
		AccountId:   accountId,
		NamespaceId: namespaceId,
	}, processId1, nil, 100)
	txn.Discard()

	require.NoError(t, err)
	require.Len(t, result.Leases, 2, "Should find 2 leases for process 1")

	// Verify all returned leases belong to process 1
	for _, lease := range result.Leases {
		require.Equal(t, processId1, lease.ProcessId)
	}

	// List leases for process 2
	txn = store.View()
	result, err = table.ListByProcessId(txn, &corepb.NamespaceId{
		AccountId:   accountId,
		NamespaceId: namespaceId,
	}, processId2, nil, 100)
	txn.Discard()

	require.NoError(t, err)
	require.Len(t, result.Leases, 1, "Should find 1 lease for process 2")
	require.Equal(t, processId2, result.Leases[0].ProcessId)
}

func TestLeasesTable_ListByProcessIdEmpty(t *testing.T) {
	store, err := store.NewBadgerInMemoryStore()
	require.NoError(t, err)

	table := NewLeasesTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff}, []byte{0x1d, 0x36, 0x00, 0x00}, []byte{0x01}, []byte{0x02}, []byte{0x03})

	accountId := rand.Uint64()
	namespaceId := rand.Uint32()
	processId := "nonexistent_process"

	txn := store.View()
	result, err := table.ListByProcessId(txn, &corepb.NamespaceId{
		AccountId:   accountId,
		NamespaceId: namespaceId,
	}, processId, nil, 100)
	txn.Discard()

	require.NoError(t, err)
	require.Empty(t, result.Leases, "Should find no leases for nonexistent process")
}

func TestLeasesTable_ListByProcessIdWithPagination(t *testing.T) {
	store, err := store.NewBadgerInMemoryStore()
	require.NoError(t, err)

	table := NewLeasesTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff}, []byte{0x1d, 0x36, 0x00, 0x00}, []byte{0x01}, []byte{0x02}, []byte{0x03})

	accountId := rand.Uint64()
	namespaceId := rand.Uint32()
	processId := "test_process_1"
	now := time.Now()

	// Create 10 leases for the same process
	numLeases := 10
	for i := 0; i < numLeases; i++ {
		lease := &corepb.Lease{
			Id: &corepb.LeaseId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
				LeaseId:     uint64(i + 1), // Sequential IDs
			},
			ProcessId: processId,
			ExpiresAt: now.Add(time.Duration(i) * time.Hour).UnixNano(),
			CreatedAt: now.UnixNano(),
		}

		txn := store.Update()
		err = table.Create(txn, lease)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())
	}

	// List with pagination (3 per page)
	pageSize := 3
	allFetchedLeases := make([]*corepb.Lease, 0)
	var paginationToken *corepb.PaginationToken

	for {
		txn := store.View()
		result, err := table.ListByProcessId(txn, &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
		}, processId, paginationToken, pageSize)
		txn.Discard()

		require.NoError(t, err)
		allFetchedLeases = append(allFetchedLeases, result.Leases...)

		if result.NextPaginationToken == nil {
			break
		}
		paginationToken = result.NextPaginationToken
	}

	// Verify we got all leases
	require.Len(t, allFetchedLeases, numLeases)

	// Verify all leases belong to the same process
	for _, lease := range allFetchedLeases {
		require.Equal(t, processId, lease.ProcessId)
	}
}

func TestLeasesTable_IndexConsistency(t *testing.T) {
	store, err := store.NewBadgerInMemoryStore()
	require.NoError(t, err)

	table := NewLeasesTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff}, []byte{0x1d, 0x36, 0x00, 0x00}, []byte{0x01}, []byte{0x02}, []byte{0x03})

	accountId := rand.Uint64()
	namespaceId := rand.Uint32()
	leaseId := rand.Uint64()
	processId := "test_process_1"
	now := time.Now()

	lease := &corepb.Lease{
		Id: &corepb.LeaseId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LeaseId:     leaseId,
		},
		ProcessId: processId,
		ExpiresAt: now.Add(time.Hour).UnixNano(),
		CreatedAt: now.UnixNano(),
	}

	// Create lease
	txn := store.Update()
	err = table.Create(txn, lease)
	require.NoError(t, err)
	require.NoError(t, txn.Commit())

	// Verify lease is in main table
	txn = store.View()
	_, err = table.Get(txn, lease.Id)
	txn.Discard()
	require.NoError(t, err, "Lease should be in main table")

	// Verify lease is in process ID index
	txn = store.View()
	processResult, err := table.ListByProcessId(txn, &corepb.NamespaceId{
		AccountId:   accountId,
		NamespaceId: namespaceId,
	}, processId, nil, 100)
	txn.Discard()
	require.NoError(t, err)
	require.Len(t, processResult.Leases, 1, "Lease should be in process ID index")
	require.Equal(t, leaseId, processResult.Leases[0].Id.LeaseId)

	// Verify lease is in expiration index
	txn = store.View()
	foundInExpiration := false
	err = table.ListByExpiration(txn, lease.ExpiresAt-1, lease.ExpiresAt+1, func(l *corepb.Lease) (bool, error) {
		if l.Id.LeaseId == leaseId {
			foundInExpiration = true
			return false, nil
		}
		return true, nil
	})
	txn.Discard()
	require.NoError(t, err)
	require.True(t, foundInExpiration, "Lease should be in expiration index")
}

func TestLeasesTable_MultipleNamespaces(t *testing.T) {
	store, err := store.NewBadgerInMemoryStore()
	require.NoError(t, err)

	table := NewLeasesTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff}, []byte{0x1d, 0x36, 0x00, 0x00}, []byte{0x01}, []byte{0x02}, []byte{0x03})

	accountId := rand.Uint64()
	namespaceId1 := rand.Uint32()
	namespaceId2 := rand.Uint32()
	processId := "test_process_1"
	now := time.Now()

	// Create leases in two different namespaces
	lease1 := &corepb.Lease{
		Id: &corepb.LeaseId{
			AccountId:   accountId,
			NamespaceId: namespaceId1,
			LeaseId:     rand.Uint64(),
		},
		ProcessId: processId,
		ExpiresAt: now.Add(time.Hour).UnixNano(),
		CreatedAt: now.UnixNano(),
	}

	lease2 := &corepb.Lease{
		Id: &corepb.LeaseId{
			AccountId:   accountId,
			NamespaceId: namespaceId2,
			LeaseId:     rand.Uint64(),
		},
		ProcessId: processId,
		ExpiresAt: now.Add(time.Hour).UnixNano(),
		CreatedAt: now.UnixNano(),
	}

	txn := store.Update()
	err = table.Create(txn, lease1)
	require.NoError(t, err)
	err = table.Create(txn, lease2)
	require.NoError(t, err)
	require.NoError(t, txn.Commit())

	// List leases for namespace 1
	txn = store.View()
	result1, err := table.List(txn, &corepb.NamespaceId{
		AccountId:   accountId,
		NamespaceId: namespaceId1,
	}, nil, 100)
	txn.Discard()
	require.NoError(t, err)
	require.Len(t, result1.Leases, 1)
	require.Equal(t, namespaceId1, result1.Leases[0].Id.NamespaceId)

	// List leases for namespace 2
	txn = store.View()
	result2, err := table.List(txn, &corepb.NamespaceId{
		AccountId:   accountId,
		NamespaceId: namespaceId2,
	}, nil, 100)
	txn.Discard()
	require.NoError(t, err)
	require.Len(t, result2.Leases, 1)
	require.Equal(t, namespaceId2, result2.Leases[0].Id.NamespaceId)
}

func TestLeasesTable_ListByExpirationWithError(t *testing.T) {
	store, err := store.NewBadgerInMemoryStore()
	require.NoError(t, err)

	table := NewLeasesTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff}, []byte{0x1d, 0x36, 0x00, 0x00}, []byte{0x01}, []byte{0x02}, []byte{0x03})

	accountId := rand.Uint64()
	namespaceId := rand.Uint32()
	processId := "test_process_1"
	now := time.Now()

	// Create a lease
	lease := &corepb.Lease{
		Id: &corepb.LeaseId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LeaseId:     rand.Uint64(),
		},
		ProcessId: processId,
		ExpiresAt: now.Add(time.Hour).UnixNano(),
		CreatedAt: now.UnixNano(),
	}

	txn := store.Update()
	err = table.Create(txn, lease)
	require.NoError(t, err)
	require.NoError(t, txn.Commit())

	// List with callback that returns an error
	txn = store.View()
	testError := fmt.Errorf("test error")
	err = table.ListByExpiration(txn, now.UnixNano(), now.Add(2*time.Hour).UnixNano(), func(l *corepb.Lease) (bool, error) {
		return false, testError
	})
	txn.Discard()

	require.Error(t, err)
	require.Equal(t, testError, err)
}
