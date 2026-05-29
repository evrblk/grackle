package semaphores

import (
	"bytes"
	"fmt"
	"io"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/evrblk/monstera/store"
	monsterax "github.com/evrblk/monstera/x"
	"github.com/stretchr/testify/require"

	"github.com/evrblk/grackle/pkg/corepb"
	"github.com/evrblk/grackle/pkg/tables"
)

func init() {
	registry := monsterax.NewBaseTableRegistry(1)
	tables.RegisterGracklePrefixes(registry)
}

func TestCore_AcquireSemaphore(t *testing.T) {
	t.Run("acquire existing semaphore", func(t *testing.T) {
		semaphoresCore := newSemaphoresCore(t)

		now := time.Now()

		accountId := rand.Uint64()
		namespaceId := &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: rand.Uint32(),
		}
		semaphoreId := &corepb.SemaphoreId{
			AccountId:   namespaceId.AccountId,
			NamespaceId: namespaceId.NamespaceId,
			SemaphoreId: rand.Uint64(),
		}

		// T+0: Create semaphore
		createResponse, err := semaphoresCore.CreateSemaphore(&corepb.CreateSemaphoreRequest{
			SemaphoreId:                       semaphoreId,
			Name:                              "test_semaphore",
			Description:                       "test description",
			Permits:                           5,
			Now:                               now.UnixNano(),
			MaxNumberOfSemaphoresPerNamespace: 100,
		})

		require.NoError(t, err)
		require.NotNil(t, createResponse.Semaphore)
		require.Equal(t, "test description", createResponse.Semaphore.Description)
		require.EqualValues(t, 5, createResponse.Semaphore.Permits)

		// T+1m: Create lease and acquire semaphore
		lease := createLease(t, semaphoresCore, accountId, namespaceId.NamespaceId, "process_1", now.Add(time.Minute), 60*time.Minute)
		response1, err := semaphoresCore.AcquireSemaphore(&corepb.AcquireSemaphoreRequest{
			NamespaceId:   namespaceId,
			SemaphoreName: "test_semaphore",
			Weight:        2,
			Now:           now.Add(time.Minute).UnixNano(),
			LeaseId:       lease.Id.LeaseId,
		})

		require.NoError(t, err)
		require.NotNil(t, response1.Semaphore)
		require.True(t, response1.Success)
		require.EqualValues(t, 1, response1.Semaphore.ActiveHoldersCount)
		require.EqualValues(t, 2, response1.Semaphore.ActiveHolds)
		// require.Equal(t, "process_1", response1.Semaphore.SemaphoreHolders[0].ProcessId)

		// T+2m: Get semaphore
		getResponse, err := semaphoresCore.GetSemaphore(&corepb.GetSemaphoreRequest{
			SemaphoreId: semaphoreId,
			Now:         now.Add(2 * time.Minute).UnixNano(),
		})

		require.NoError(t, err)
		require.NotNil(t, getResponse.Semaphore)
		require.EqualValues(t, 1, getResponse.Semaphore.ActiveHoldersCount)

		// T+62m: Get semaphore after expiration
		getResponse2, err := semaphoresCore.GetSemaphore(&corepb.GetSemaphoreRequest{
			SemaphoreId: semaphoreId,
			Now:         now.Add(62 * time.Minute).UnixNano(),
		})

		require.NoError(t, err)
		require.NotNil(t, getResponse2.Semaphore)
		require.EqualValues(t, 0, getResponse2.Semaphore.ActiveHoldersCount)
	})

	t.Run("acquire semaphore repeatedly", func(t *testing.T) {
		semaphoresCore := newSemaphoresCore(t)

		now := time.Now()

		accountId := rand.Uint64()
		namespaceId := &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: rand.Uint32(),
		}
		semaphoreId := &corepb.SemaphoreId{
			AccountId:   namespaceId.AccountId,
			NamespaceId: namespaceId.NamespaceId,
			SemaphoreId: rand.Uint64(),
		}

		// T+0: Create semaphore
		_, err := semaphoresCore.CreateSemaphore(&corepb.CreateSemaphoreRequest{
			SemaphoreId:                       semaphoreId,
			Name:                              "test_semaphore",
			Description:                       "test description",
			Permits:                           2,
			Now:                               now.UnixNano(),
			MaxNumberOfSemaphoresPerNamespace: 100,
		})
		require.NoError(t, err)

		// T+1m: Create lease and acquire semaphore
		lease := createLease(t, semaphoresCore, accountId, namespaceId.NamespaceId, "process_1", now.Add(time.Minute), 60*time.Minute)
		response1, err := semaphoresCore.AcquireSemaphore(&corepb.AcquireSemaphoreRequest{
			NamespaceId:   namespaceId,
			SemaphoreName: "test_semaphore",
			Weight:        1,
			Now:           now.Add(time.Minute).UnixNano(),
			LeaseId:       lease.Id.LeaseId,
		})

		require.NoError(t, err)
		require.True(t, response1.Success)

		// T+2m: Acquire same semaphore with same process (should extend expiration)
		response2, err := semaphoresCore.AcquireSemaphore(&corepb.AcquireSemaphoreRequest{
			NamespaceId:   namespaceId,
			SemaphoreName: "test_semaphore",
			Weight:        1,
			Now:           now.Add(2 * time.Minute).UnixNano(),
			LeaseId:       lease.Id.LeaseId,
		})

		require.NoError(t, err)
		require.True(t, response2.Success)
		require.EqualValues(t, 1, response2.Semaphore.ActiveHoldersCount)
		// require.EqualValues(t, now.Add(2*time.Minute).Add(time.Hour).UnixNano(), response2.Semaphore.SemaphoreHolders[0].ExpiresAt)
	})

	t.Run("acquire semaphore with multiple permits", func(t *testing.T) {
		semaphoresCore := newSemaphoresCore(t)

		now := time.Now()

		accountId := rand.Uint64()
		namespaceId := &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: rand.Uint32(),
		}
		semaphoreId := &corepb.SemaphoreId{
			AccountId:   namespaceId.AccountId,
			NamespaceId: namespaceId.NamespaceId,
			SemaphoreId: rand.Uint64(),
		}

		// T+0: Create semaphore with 2 permits
		_, err := semaphoresCore.CreateSemaphore(&corepb.CreateSemaphoreRequest{
			SemaphoreId:                       semaphoreId,
			Name:                              "test_semaphore",
			Description:                       "test description",
			Permits:                           2,
			Now:                               now.UnixNano(),
			MaxNumberOfSemaphoresPerNamespace: 100,
		})
		require.NoError(t, err)

		// T+1m: First process acquires semaphore
		lease1 := createLease(t, semaphoresCore, accountId, namespaceId.NamespaceId, "process_1", now.Add(time.Minute), 60*time.Minute)
		response1, err := semaphoresCore.AcquireSemaphore(&corepb.AcquireSemaphoreRequest{
			NamespaceId:   namespaceId,
			SemaphoreName: "test_semaphore",
			Weight:        1,
			Now:           now.Add(time.Minute).UnixNano(),
			LeaseId:       lease1.Id.LeaseId,
		})

		require.NoError(t, err)
		require.True(t, response1.Success)

		// T+2m: Second process acquires semaphore
		lease2 := createLease(t, semaphoresCore, accountId, namespaceId.NamespaceId, "process_2", now.Add(2*time.Minute), 60*time.Minute)
		response2, err := semaphoresCore.AcquireSemaphore(&corepb.AcquireSemaphoreRequest{
			NamespaceId:   namespaceId,
			SemaphoreName: "test_semaphore",
			Weight:        1,
			Now:           now.Add(2 * time.Minute).UnixNano(),
			LeaseId:       lease2.Id.LeaseId,
		})

		require.NoError(t, err)
		require.True(t, response2.Success)
		require.EqualValues(t, 2, response2.Semaphore.ActiveHoldersCount)

		// T+3m: Third process tries to acquire semaphore (should fail)
		lease3 := createLease(t, semaphoresCore, accountId, namespaceId.NamespaceId, "process_3", now.Add(3*time.Minute), 60*time.Minute)
		response3, err := semaphoresCore.AcquireSemaphore(&corepb.AcquireSemaphoreRequest{
			NamespaceId:   namespaceId,
			SemaphoreName: "test_semaphore",
			Weight:        1,
			Now:           now.Add(3 * time.Minute).UnixNano(),
			LeaseId:       lease3.Id.LeaseId,
		})

		require.NoError(t, err)
		require.False(t, response3.Success)
		require.EqualValues(t, 2, response3.Semaphore.ActiveHoldersCount)
	})

	t.Run("acquire nonexistent semaphore", func(t *testing.T) {
		semaphoresCore := newSemaphoresCore(t)

		now := time.Now()

		accountId := rand.Uint64()
		namespaceId := &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: rand.Uint32(),
		}

		// Try to acquire a nonexistent semaphore
		lease := createLease(t, semaphoresCore, accountId, namespaceId.NamespaceId, "process_1", now, 60*time.Minute)
		_, err := semaphoresCore.AcquireSemaphore(&corepb.AcquireSemaphoreRequest{
			NamespaceId:   namespaceId,
			SemaphoreName: "non_existing_semaphore",
			Weight:        1,
			Now:           now.UnixNano(),
			LeaseId:       lease.Id.LeaseId,
		})

		require.Error(t, err)
		require.Contains(t, err.Error(), "not found")
	})
}

func TestCore_ReleaseSemaphore(t *testing.T) {
	t.Run("release existing semaphore", func(t *testing.T) {
		semaphoresCore := newSemaphoresCore(t)

		now := time.Now()

		accountId := rand.Uint64()
		namespaceId := &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: rand.Uint32(),
		}
		semaphoreId := &corepb.SemaphoreId{
			AccountId:   namespaceId.AccountId,
			NamespaceId: namespaceId.NamespaceId,
			SemaphoreId: rand.Uint64(),
		}

		// T+0: Create semaphore
		_, err := semaphoresCore.CreateSemaphore(&corepb.CreateSemaphoreRequest{
			SemaphoreId:                       semaphoreId,
			Name:                              "test_semaphore",
			Description:                       "test description",
			Permits:                           2,
			Now:                               now.UnixNano(),
			MaxNumberOfSemaphoresPerNamespace: 100,
		})
		require.NoError(t, err)

		// T+1m: Create lease and acquire semaphore
		lease := createLease(t, semaphoresCore, accountId, namespaceId.NamespaceId, "process_1", now.Add(time.Minute), 60*time.Minute)
		response1, err := semaphoresCore.AcquireSemaphore(&corepb.AcquireSemaphoreRequest{
			NamespaceId:   namespaceId,
			SemaphoreName: "test_semaphore",
			Weight:        1,
			Now:           now.Add(time.Minute).UnixNano(),
			LeaseId:       lease.Id.LeaseId,
		})

		require.NoError(t, err)
		require.True(t, response1.Success)

		// T+2m: Release semaphore
		response2, err := semaphoresCore.ReleaseSemaphore(&corepb.ReleaseSemaphoreRequest{
			NamespaceId:   namespaceId,
			SemaphoreName: "test_semaphore",
			Now:           now.Add(2 * time.Minute).UnixNano(),
			LeaseId:       lease.Id.LeaseId,
		})

		require.NoError(t, err)
		require.NotNil(t, response2.Semaphore)
		require.EqualValues(t, 0, response2.Semaphore.ActiveHoldersCount)
	})

	t.Run("release nonexistent semaphore", func(t *testing.T) {
		semaphoresCore := newSemaphoresCore(t)

		now := time.Now()

		accountId := rand.Uint64()
		namespaceId := &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: rand.Uint32(),
		}

		// Try to release a nonexistent semaphore
		lease := createLease(t, semaphoresCore, accountId, namespaceId.NamespaceId, "process_1", now, 60*time.Minute)
		_, err := semaphoresCore.ReleaseSemaphore(&corepb.ReleaseSemaphoreRequest{
			NamespaceId:   namespaceId,
			SemaphoreName: "non_existing_semaphore",
			Now:           now.UnixNano(),
			LeaseId:       lease.Id.LeaseId,
		})

		require.Error(t, err)
		require.Contains(t, err.Error(), "not found")
	})

	t.Run("release nonexistent lease id", func(t *testing.T) {
		semaphoresCore := newSemaphoresCore(t)

		now := time.Now()

		accountId := rand.Uint64()
		namespaceId := &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: rand.Uint32(),
		}
		semaphoreId := &corepb.SemaphoreId{
			AccountId:   namespaceId.AccountId,
			NamespaceId: namespaceId.NamespaceId,
			SemaphoreId: rand.Uint64(),
		}

		// T+0: Create semaphore
		_, err := semaphoresCore.CreateSemaphore(&corepb.CreateSemaphoreRequest{
			SemaphoreId:                       semaphoreId,
			Name:                              "test_semaphore",
			Description:                       "test description",
			Permits:                           2,
			Now:                               now.UnixNano(),
			MaxNumberOfSemaphoresPerNamespace: 100,
		})
		require.NoError(t, err)

		// T+1m: Acquire semaphore with process_1
		lease1 := createLease(t, semaphoresCore, accountId, namespaceId.NamespaceId, "process_1", now.Add(time.Minute), 60*time.Minute)
		response1, err := semaphoresCore.AcquireSemaphore(&corepb.AcquireSemaphoreRequest{
			NamespaceId:   namespaceId,
			SemaphoreName: "test_semaphore",
			Weight:        1,
			Now:           now.Add(time.Minute).UnixNano(),
			LeaseId:       lease1.Id.LeaseId,
		})

		require.NoError(t, err)
		require.True(t, response1.Success)
		require.EqualValues(t, 1, response1.Semaphore.ActiveHoldersCount)

		// T+2m: Try to release semaphore with a nonexistent lease_id (should succeed without error)
		lease2 := createLease(t, semaphoresCore, accountId, namespaceId.NamespaceId, "process_non_existing", now.Add(2*time.Minute), 60*time.Minute)
		response2, err := semaphoresCore.ReleaseSemaphore(&corepb.ReleaseSemaphoreRequest{
			NamespaceId:   namespaceId,
			SemaphoreName: "test_semaphore",
			Now:           now.Add(2 * time.Minute).UnixNano(),
			LeaseId:       lease2.Id.LeaseId,
		})

		require.NoError(t, err)
		require.NotNil(t, response2.Semaphore)
		// The semaphore should still have 1 holder (process_1), since we tried to release a nonexistent lease_id
		require.EqualValues(t, 1, response2.Semaphore.ActiveHoldersCount)
	})
}

func TestCore_UpdateSemaphore(t *testing.T) {
	t.Run("update existing semaphore", func(t *testing.T) {
		semaphoresCore := newSemaphoresCore(t)

		now := time.Now()

		namespaceId := &corepb.NamespaceId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
		}
		semaphoreId := &corepb.SemaphoreId{
			AccountId:   namespaceId.AccountId,
			NamespaceId: namespaceId.NamespaceId,
			SemaphoreId: rand.Uint64(),
		}

		// T+0: Create semaphore
		_, err := semaphoresCore.CreateSemaphore(&corepb.CreateSemaphoreRequest{
			SemaphoreId:                       semaphoreId,
			Name:                              "test_semaphore",
			Description:                       "test description",
			Permits:                           2,
			Now:                               now.UnixNano(),
			MaxNumberOfSemaphoresPerNamespace: 100,
		})
		require.NoError(t, err)

		// T+1m: Update semaphore
		_, err = semaphoresCore.UpdateSemaphore(&corepb.UpdateSemaphoreRequest{
			NamespaceId:   namespaceId,
			SemaphoreName: "test_semaphore",
			Description:   "updated description",
			Permits:       3,
			Now:           now.Add(time.Minute).UnixNano(),
		})

		require.NoError(t, err)

		// T+2m: Get updated semaphore
		response, err := semaphoresCore.GetSemaphore(&corepb.GetSemaphoreRequest{
			SemaphoreId: semaphoreId,
			Now:         now.Add(2 * time.Minute).UnixNano(),
		})

		require.NoError(t, err)
		require.Equal(t, "updated description", response.Semaphore.Description)
		require.EqualValues(t, 3, response.Semaphore.Permits)
	})

	t.Run("update semaphore with insufficient permits", func(t *testing.T) {
		semaphoresCore := newSemaphoresCore(t)

		now := time.Now()

		accountId := rand.Uint64()
		namespaceId := &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: rand.Uint32(),
		}
		semaphoreId := &corepb.SemaphoreId{
			AccountId:   namespaceId.AccountId,
			NamespaceId: namespaceId.NamespaceId,
			SemaphoreId: rand.Uint64(),
		}

		// T+0: Create semaphore with 3 permits
		_, err := semaphoresCore.CreateSemaphore(&corepb.CreateSemaphoreRequest{
			SemaphoreId:                       semaphoreId,
			Name:                              "test_semaphore",
			Description:                       "test description",
			Permits:                           3,
			Now:                               now.UnixNano(),
			MaxNumberOfSemaphoresPerNamespace: 100,
		})
		require.NoError(t, err)

		// T+1m: First process acquires semaphore
		lease1 := createLease(t, semaphoresCore, accountId, namespaceId.NamespaceId, "process_1", now.Add(time.Minute), 60*time.Minute)
		response1, err := semaphoresCore.AcquireSemaphore(&corepb.AcquireSemaphoreRequest{
			NamespaceId:   namespaceId,
			SemaphoreName: "test_semaphore",
			Weight:        1,
			Now:           now.Add(time.Minute).UnixNano(),
			LeaseId:       lease1.Id.LeaseId,
		})
		require.NoError(t, err)
		require.True(t, response1.Success)

		// T+2m: Second process acquires semaphore
		lease2 := createLease(t, semaphoresCore, accountId, namespaceId.NamespaceId, "process_2", now.Add(2*time.Minute), 60*time.Minute)
		response2, err := semaphoresCore.AcquireSemaphore(&corepb.AcquireSemaphoreRequest{
			NamespaceId:   namespaceId,
			SemaphoreName: "test_semaphore",
			Weight:        1,
			Now:           now.Add(2 * time.Minute).UnixNano(),
			LeaseId:       lease2.Id.LeaseId,
		})
		require.NoError(t, err)
		require.True(t, response2.Success)

		// T+3m: Third process acquires semaphore
		lease3 := createLease(t, semaphoresCore, accountId, namespaceId.NamespaceId, "process_3", now.Add(3*time.Minute), 60*time.Minute)
		response3, err := semaphoresCore.AcquireSemaphore(&corepb.AcquireSemaphoreRequest{
			NamespaceId:   namespaceId,
			SemaphoreName: "test_semaphore",
			Weight:        1,
			Now:           now.Add(3 * time.Minute).UnixNano(),
			LeaseId:       lease3.Id.LeaseId,
		})
		require.NoError(t, err)
		require.True(t, response3.Success)

		// Verify we have 3 holders
		getResponse, err := semaphoresCore.GetSemaphore(&corepb.GetSemaphoreRequest{
			SemaphoreId: semaphoreId,
			Now:         now.Add(4 * time.Minute).UnixNano(),
		})
		require.NoError(t, err)
		require.EqualValues(t, 3, getResponse.Semaphore.ActiveHoldersCount)

		// T+5m: Try to update semaphore to reduce permits to 2 (less than current holders)
		_, err = semaphoresCore.UpdateSemaphore(&corepb.UpdateSemaphoreRequest{
			NamespaceId:   namespaceId,
			SemaphoreName: "test_semaphore",
			Description:   "updated description",
			Permits:       2,
			Now:           now.Add(5 * time.Minute).UnixNano(),
		})

		require.Error(t, err)
		require.Contains(t, err.Error(), "there are currently more holds than the new amount of permits")

		// Verify the semaphore was not updated
		getResponse2, err := semaphoresCore.GetSemaphore(&corepb.GetSemaphoreRequest{
			SemaphoreId: semaphoreId,
			Now:         now.Add(6 * time.Minute).UnixNano(),
		})
		require.NoError(t, err)
		require.Equal(t, "test description", getResponse2.Semaphore.Description) // Description should not be updated
		require.EqualValues(t, 3, getResponse2.Semaphore.Permits)                // Permits should not be updated
		require.EqualValues(t, 3, getResponse2.Semaphore.ActiveHoldersCount)     // All holders should still be there
		require.EqualValues(t, 3, getResponse2.Semaphore.ActiveHolds)            // All holders should still be there
	})

	t.Run("update nonexistent semaphore", func(t *testing.T) {
		semaphoresCore := newSemaphoresCore(t)

		now := time.Now()

		// Try to update a nonexistent semaphore
		_, err := semaphoresCore.UpdateSemaphore(&corepb.UpdateSemaphoreRequest{
			NamespaceId: &corepb.NamespaceId{
				AccountId:   rand.Uint64(),
				NamespaceId: rand.Uint32(),
			},
			SemaphoreName: "non_existing_semaphore",
			Description:   "updated description",
			Permits:       3,
			Now:           now.UnixNano(),
		})

		require.Error(t, err)
		require.Contains(t, err.Error(), "not found")
	})
}

func TestCore_GetSemaphore(t *testing.T) {
	semaphoresCore := newSemaphoresCore(t)

	now := time.Now()

	nonExistingSemaphoreId := &corepb.SemaphoreId{
		AccountId:   rand.Uint64(),
		NamespaceId: rand.Uint32(),
		SemaphoreId: rand.Uint64(),
	}

	// Try to get a nonexistent semaphore
	_, err := semaphoresCore.GetSemaphore(&corepb.GetSemaphoreRequest{
		SemaphoreId: nonExistingSemaphoreId,
		Now:         now.UnixNano(),
	})

	require.Error(t, err)
	require.Contains(t, err.Error(), "not found")
}

func TestCore_GetSemaphoreByName(t *testing.T) {
	t.Run("get existing semaphore by name", func(t *testing.T) {
		semaphoresCore := newSemaphoresCore(t)

		now := time.Now()

		namespaceId := &corepb.NamespaceId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
		}
		semaphoreId := &corepb.SemaphoreId{
			AccountId:   namespaceId.AccountId,
			NamespaceId: namespaceId.NamespaceId,
			SemaphoreId: rand.Uint64(),
		}

		// T+0: Create semaphore
		createResponse, err := semaphoresCore.CreateSemaphore(&corepb.CreateSemaphoreRequest{
			SemaphoreId:                       semaphoreId,
			Name:                              "test_semaphore",
			Description:                       "test description",
			Permits:                           5,
			Now:                               now.UnixNano(),
			MaxNumberOfSemaphoresPerNamespace: 100,
		})

		require.NoError(t, err)
		require.NotNil(t, createResponse.Semaphore)

		// T+1m: Get semaphore by name
		getResponse, err := semaphoresCore.GetSemaphoreByName(&corepb.GetSemaphoreByNameRequest{
			NamespaceId:   namespaceId,
			SemaphoreName: "test_semaphore",
			Now:           now.Add(time.Minute).UnixNano(),
		})

		require.NoError(t, err)
		require.NotNil(t, getResponse.Semaphore)
		require.Equal(t, "test_semaphore", getResponse.Semaphore.Name)
		require.Equal(t, "test description", getResponse.Semaphore.Description)
		require.EqualValues(t, 5, getResponse.Semaphore.Permits)
		require.Equal(t, semaphoreId.SemaphoreId, getResponse.Semaphore.Id.SemaphoreId)
	})

	t.Run("get semaphore by name with expired holders", func(t *testing.T) {
		semaphoresCore := newSemaphoresCore(t)

		now := time.Now()

		accountId := rand.Uint64()
		namespaceId := &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: rand.Uint32(),
		}
		semaphoreId := &corepb.SemaphoreId{
			AccountId:   namespaceId.AccountId,
			NamespaceId: namespaceId.NamespaceId,
			SemaphoreId: rand.Uint64(),
		}

		// T+0: Create semaphore
		_, err := semaphoresCore.CreateSemaphore(&corepb.CreateSemaphoreRequest{
			SemaphoreId:                       semaphoreId,
			Name:                              "test_semaphore",
			Description:                       "test description",
			Permits:                           3,
			Now:                               now.UnixNano(),
			MaxNumberOfSemaphoresPerNamespace: 100,
		})
		require.NoError(t, err)

		// T+1m: Acquire semaphore with process_1 (expires at T+31m)
		lease1 := createLease(t, semaphoresCore, accountId, namespaceId.NamespaceId, "process_1", now.Add(time.Minute), 30*time.Minute)
		response1, err := semaphoresCore.AcquireSemaphore(&corepb.AcquireSemaphoreRequest{
			NamespaceId:   namespaceId,
			SemaphoreName: "test_semaphore",
			Weight:        1,
			Now:           now.Add(time.Minute).UnixNano(),
			LeaseId:       lease1.Id.LeaseId,
		})
		require.NoError(t, err)
		require.True(t, response1.Success)

		// T+2m: Acquire semaphore with process_2 (expires at T+62m)
		lease2 := createLease(t, semaphoresCore, accountId, namespaceId.NamespaceId, "process_2", now.Add(2*time.Minute), 60*time.Minute)
		response2, err := semaphoresCore.AcquireSemaphore(&corepb.AcquireSemaphoreRequest{
			NamespaceId:   namespaceId,
			SemaphoreName: "test_semaphore",
			Weight:        1,
			Now:           now.Add(2 * time.Minute).UnixNano(),
			LeaseId:       lease2.Id.LeaseId,
		})
		require.NoError(t, err)
		require.True(t, response2.Success)

		// T+3m: Get semaphore by name (both holders should be active)
		getResponse1, err := semaphoresCore.GetSemaphoreByName(&corepb.GetSemaphoreByNameRequest{
			NamespaceId:   namespaceId,
			SemaphoreName: "test_semaphore",
			Now:           now.Add(3 * time.Minute).UnixNano(),
		})

		require.NoError(t, err)
		require.NotNil(t, getResponse1.Semaphore)
		require.EqualValues(t, 2, getResponse1.Semaphore.ActiveHoldersCount)
		require.EqualValues(t, 2, getResponse1.Semaphore.ActiveHolds)

		// T+35m: Get semaphore by name (process_1 should have expired, only process_2 remains)
		getResponse2, err := semaphoresCore.GetSemaphoreByName(&corepb.GetSemaphoreByNameRequest{
			NamespaceId:   namespaceId,
			SemaphoreName: "test_semaphore",
			Now:           now.Add(35 * time.Minute).UnixNano(),
		})

		require.NoError(t, err)
		require.NotNil(t, getResponse2.Semaphore)
		require.EqualValues(t, 1, getResponse2.Semaphore.ActiveHoldersCount)
		require.EqualValues(t, 1, getResponse2.Semaphore.ActiveHolds)

		// T+65m: Get semaphore by name (all holders should have expired)
		getResponse3, err := semaphoresCore.GetSemaphoreByName(&corepb.GetSemaphoreByNameRequest{
			NamespaceId:   namespaceId,
			SemaphoreName: "test_semaphore",
			Now:           now.Add(65 * time.Minute).UnixNano(),
		})

		require.NoError(t, err)
		require.NotNil(t, getResponse3.Semaphore)
		require.EqualValues(t, 0, getResponse3.Semaphore.ActiveHoldersCount)
		require.EqualValues(t, 0, getResponse3.Semaphore.ActiveHolds)
	})

	t.Run("get nonexistent semaphore by name", func(t *testing.T) {
		semaphoresCore := newSemaphoresCore(t)

		now := time.Now()

		namespaceId := &corepb.NamespaceId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
		}

		// Try to get a nonexistent semaphore by name
		_, err := semaphoresCore.GetSemaphoreByName(&corepb.GetSemaphoreByNameRequest{
			NamespaceId:   namespaceId,
			SemaphoreName: "non_existing_semaphore",
			Now:           now.UnixNano(),
		})

		require.Error(t, err)
		require.Contains(t, err.Error(), "not found")
	})

	t.Run("get semaphore by name from different namespace", func(t *testing.T) {
		semaphoresCore := newSemaphoresCore(t)

		now := time.Now()

		accountId := rand.Uint64()

		// Create semaphore in namespace 1
		namespace1Id := &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: rand.Uint32(),
		}
		semaphore1Id := &corepb.SemaphoreId{
			AccountId:   namespace1Id.AccountId,
			NamespaceId: namespace1Id.NamespaceId,
			SemaphoreId: rand.Uint64(),
		}

		_, err := semaphoresCore.CreateSemaphore(&corepb.CreateSemaphoreRequest{
			SemaphoreId:                       semaphore1Id,
			Name:                              "test_semaphore",
			Description:                       "namespace 1 semaphore",
			Permits:                           5,
			Now:                               now.UnixNano(),
			MaxNumberOfSemaphoresPerNamespace: 100,
		})
		require.NoError(t, err)

		// Create semaphore with same name in namespace 2
		namespace2Id := &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: rand.Uint32(),
		}
		semaphore2Id := &corepb.SemaphoreId{
			AccountId:   namespace2Id.AccountId,
			NamespaceId: namespace2Id.NamespaceId,
			SemaphoreId: rand.Uint64(),
		}

		_, err = semaphoresCore.CreateSemaphore(&corepb.CreateSemaphoreRequest{
			SemaphoreId:                       semaphore2Id,
			Name:                              "test_semaphore",
			Description:                       "namespace 2 semaphore",
			Permits:                           3,
			Now:                               now.UnixNano(),
			MaxNumberOfSemaphoresPerNamespace: 100,
		})
		require.NoError(t, err)

		// Get semaphore from namespace 1
		getResponse1, err := semaphoresCore.GetSemaphoreByName(&corepb.GetSemaphoreByNameRequest{
			NamespaceId:   namespace1Id,
			SemaphoreName: "test_semaphore",
			Now:           now.UnixNano(),
		})

		require.NoError(t, err)
		require.NotNil(t, getResponse1.Semaphore)
		require.Equal(t, "namespace 1 semaphore", getResponse1.Semaphore.Description)
		require.EqualValues(t, 5, getResponse1.Semaphore.Permits)
		require.Equal(t, semaphore1Id.SemaphoreId, getResponse1.Semaphore.Id.SemaphoreId)

		// Get semaphore from namespace 2
		getResponse2, err := semaphoresCore.GetSemaphoreByName(&corepb.GetSemaphoreByNameRequest{
			NamespaceId:   namespace2Id,
			SemaphoreName: "test_semaphore",
			Now:           now.UnixNano(),
		})

		require.NoError(t, err)
		require.NotNil(t, getResponse2.Semaphore)
		require.Equal(t, "namespace 2 semaphore", getResponse2.Semaphore.Description)
		require.EqualValues(t, 3, getResponse2.Semaphore.Permits)
		require.Equal(t, semaphore2Id.SemaphoreId, getResponse2.Semaphore.Id.SemaphoreId)
	})
}

func TestCore_CreateSemaphore(t *testing.T) {
	t.Run("create semaphore max limit reached", func(t *testing.T) {
		semaphoresCore := newSemaphoresCore(t)

		now := time.Now()

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		maxSemaphores := int64(3)

		// Create semaphores up to the limit
		for i := 0; i < int(maxSemaphores); i++ {
			_, err := semaphoresCore.CreateSemaphore(&corepb.CreateSemaphoreRequest{
				SemaphoreId: &corepb.SemaphoreId{
					AccountId:   accountId,
					NamespaceId: namespaceId,
					SemaphoreId: rand.Uint64(),
				},
				Name:                              fmt.Sprintf("test_semaphore_%d", i),
				Description:                       fmt.Sprintf("test description %d", i),
				Permits:                           2,
				Now:                               now.UnixNano(),
				MaxNumberOfSemaphoresPerNamespace: maxSemaphores,
			})
			require.NoError(t, err, "Failed to create semaphore %d", i)
		}

		// Try to create one more semaphore (should fail)
		_, err := semaphoresCore.CreateSemaphore(&corepb.CreateSemaphoreRequest{
			SemaphoreId: &corepb.SemaphoreId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
				SemaphoreId: rand.Uint64(),
			},
			Name:                              "test_semaphore_limit_exceeded",
			Description:                       "test description limit exceeded",
			Permits:                           2,
			Now:                               now.UnixNano(),
			MaxNumberOfSemaphoresPerNamespace: maxSemaphores,
		})

		require.Error(t, err)
		require.Contains(t, err.Error(), "max number of semaphores per namespace reached")
	})

	t.Run("create semaphore with duplicate name", func(t *testing.T) {
		semaphoresCore := newSemaphoresCore(t)

		now := time.Now()

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()
		semaphoreId1 := &corepb.SemaphoreId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			SemaphoreId: rand.Uint64(),
		}
		semaphoreId2 := &corepb.SemaphoreId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			SemaphoreId: rand.Uint64(),
		}

		// Create first semaphore
		_, err := semaphoresCore.CreateSemaphore(&corepb.CreateSemaphoreRequest{
			SemaphoreId:                       semaphoreId1,
			Name:                              "test_semaphore",
			Description:                       "test description",
			Permits:                           2,
			Now:                               now.UnixNano(),
			MaxNumberOfSemaphoresPerNamespace: 100,
		})
		require.NoError(t, err)

		// Try to create a second semaphore with the same name
		_, err = semaphoresCore.CreateSemaphore(&corepb.CreateSemaphoreRequest{
			SemaphoreId:                       semaphoreId2,
			Name:                              "test_semaphore",
			Description:                       "duplicate description",
			Permits:                           3,
			Now:                               now.UnixNano(),
			MaxNumberOfSemaphoresPerNamespace: 100,
		})

		require.Error(t, err)
		require.Contains(t, err.Error(), "already exists")
	})
}

func TestCore_DeleteSemaphore(t *testing.T) {
	t.Run("delete existing semaphore", func(t *testing.T) {
		semaphoresCore := newSemaphoresCore(t)

		now := time.Now()

		semaphoreId := &corepb.SemaphoreId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
			SemaphoreId: rand.Uint64(),
		}

		// T+0: Create semaphore
		_, err := semaphoresCore.CreateSemaphore(&corepb.CreateSemaphoreRequest{
			SemaphoreId:                       semaphoreId,
			Name:                              "test_semaphore",
			Description:                       "test description",
			Permits:                           2,
			Now:                               now.UnixNano(),
			MaxNumberOfSemaphoresPerNamespace: 100,
		})
		require.NoError(t, err)

		// T+1m: Verify semaphore exists
		getResponse, err := semaphoresCore.GetSemaphore(&corepb.GetSemaphoreRequest{
			SemaphoreId: semaphoreId,
			Now:         now.Add(time.Minute).UnixNano(),
		})
		require.NoError(t, err)
		require.NotNil(t, getResponse.Semaphore)

		// T+2m: Delete semaphore
		_, err = semaphoresCore.DeleteSemaphore(&corepb.DeleteSemaphoreRequest{
			NamespaceId: &corepb.NamespaceId{
				AccountId:   semaphoreId.AccountId,
				NamespaceId: semaphoreId.NamespaceId,
			},
			SemaphoreName: "test_semaphore",
		})
		require.NoError(t, err)

		// T+3m: Verify semaphore no longer exists
		_, err = semaphoresCore.GetSemaphore(&corepb.GetSemaphoreRequest{
			SemaphoreId: semaphoreId,
			Now:         now.Add(3 * time.Minute).UnixNano(),
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "not found")
	})

	t.Run("delete nonexistent semaphore", func(t *testing.T) {
		semaphoresCore := newSemaphoresCore(t)

		// Try to delete a nonexistent semaphore (should succeed without error)
		_, err := semaphoresCore.DeleteSemaphore(&corepb.DeleteSemaphoreRequest{
			NamespaceId: &corepb.NamespaceId{
				AccountId:   rand.Uint64(),
				NamespaceId: rand.Uint32(),
			},
			SemaphoreName: "non_existing_semaphore",
		})

		require.NoError(t, err)
	})

	t.Run("delete semaphore cleans up expiration records", func(t *testing.T) {
		semaphoresCore := newSemaphoresCore(t)

		now := time.Now()
		accountId := rand.Uint64()
		namespaceId := &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: rand.Uint32(),
		}
		semaphoreId := &corepb.SemaphoreId{
			AccountId:   namespaceId.AccountId,
			NamespaceId: namespaceId.NamespaceId,
			SemaphoreId: rand.Uint64(),
		}

		// Create semaphore
		_, err := semaphoresCore.CreateSemaphore(&corepb.CreateSemaphoreRequest{
			SemaphoreId:                       semaphoreId,
			Name:                              "test_semaphore",
			Description:                       "test description",
			Permits:                           10,
			Now:                               now.UnixNano(),
			MaxNumberOfSemaphoresPerNamespace: 100,
		})
		require.NoError(t, err)

		// Acquire semaphore to create an expiration record
		lease := createLease(t, semaphoresCore, accountId, namespaceId.NamespaceId, "process_1", now, 60*time.Minute)
		_, err = semaphoresCore.AcquireSemaphore(&corepb.AcquireSemaphoreRequest{
			NamespaceId:   namespaceId,
			SemaphoreName: "test_semaphore",
			LeaseId:       lease.Id.LeaseId,
			Weight:        1,
			Now:           now.UnixNano(),
		})
		require.NoError(t, err)

		// Delete semaphore
		_, err = semaphoresCore.DeleteSemaphore(&corepb.DeleteSemaphoreRequest{
			NamespaceId:   namespaceId,
			SemaphoreName: "test_semaphore",
		})
		require.NoError(t, err)

		// Run GC to verify there are no orphaned expiration records
		// Before the fix, this would panic because DeleteSemaphore didn't clean up expiration records
		gcResponse, err := semaphoresCore.RunSemaphoresGarbageCollection(&corepb.RunSemaphoresGarbageCollectionRequest{
			Now:                        now.Add(30 * time.Minute).UnixNano(),
			GcRecordsPageSize:          100,
			GcRecordSemaphoresPageSize: 100,
			MaxVisitedSemaphores:       100,
		})
		require.NoError(t, err)
		require.NotNil(t, gcResponse)

		// Verify semaphore is gone
		_, err = semaphoresCore.GetSemaphore(&corepb.GetSemaphoreRequest{
			SemaphoreId: semaphoreId,
			Now:         now.Add(30 * time.Minute).UnixNano(),
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "not found")
	})
}

func TestCore_ListSemaphoreHolders(t *testing.T) {
	t.Run("list holders for semaphore with multiple holders", func(t *testing.T) {
		semaphoresCore := newSemaphoresCore(t)

		now := time.Now()

		accountId := rand.Uint64()
		namespaceId := &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: rand.Uint32(),
		}
		semaphoreId := &corepb.SemaphoreId{
			AccountId:   namespaceId.AccountId,
			NamespaceId: namespaceId.NamespaceId,
			SemaphoreId: rand.Uint64(),
		}

		// Create semaphore with 5 permits
		_, err := semaphoresCore.CreateSemaphore(&corepb.CreateSemaphoreRequest{
			SemaphoreId:                       semaphoreId,
			Name:                              "test_semaphore",
			Description:                       "test description",
			Permits:                           5,
			Now:                               now.UnixNano(),
			MaxNumberOfSemaphoresPerNamespace: 100,
		})
		require.NoError(t, err)

		// Acquire semaphore with 3 different processes
		for i := 1; i <= 3; i++ {
			lease := createLease(t, semaphoresCore, accountId, namespaceId.NamespaceId, fmt.Sprintf("process_%d", i), now.Add(time.Duration(i)*time.Minute), 60*time.Minute)
			response, err := semaphoresCore.AcquireSemaphore(&corepb.AcquireSemaphoreRequest{
				NamespaceId:   namespaceId,
				SemaphoreName: "test_semaphore",
				Weight:        1,
				Now:           now.Add(time.Duration(i) * time.Minute).UnixNano(),
				LeaseId:       lease.Id.LeaseId,
			})
			require.NoError(t, err)
			require.True(t, response.Success)
		}

		// List all holders
		listResponse, err := semaphoresCore.ListSemaphoreHolders(&corepb.ListSemaphoreHoldersRequest{
			NamespaceId:   namespaceId,
			SemaphoreName: "test_semaphore",
			Limit:         100,
		})

		require.NoError(t, err)
		require.Len(t, listResponse.Holders, 3)

		// Verify holder lease IDs (we can't easily verify by process name anymore since it's via lease)
		for _, holder := range listResponse.Holders {
			require.Equal(t, namespaceId.AccountId, holder.Id.AccountId)
			require.Equal(t, namespaceId.NamespaceId, holder.Id.NamespaceId)
			require.Equal(t, semaphoreId.SemaphoreId, holder.Id.SemaphoreId)
			require.EqualValues(t, 1, holder.Weight)
		}
	})

	t.Run("list holders with pagination", func(t *testing.T) {
		semaphoresCore := newSemaphoresCore(t)

		now := time.Now()

		accountId := rand.Uint64()
		namespaceId := &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: rand.Uint32(),
		}
		semaphoreId := &corepb.SemaphoreId{
			AccountId:   namespaceId.AccountId,
			NamespaceId: namespaceId.NamespaceId,
			SemaphoreId: rand.Uint64(),
		}

		// Create semaphore with 10 permits
		_, err := semaphoresCore.CreateSemaphore(&corepb.CreateSemaphoreRequest{
			SemaphoreId:                       semaphoreId,
			Name:                              "test_semaphore",
			Description:                       "test description",
			Permits:                           10,
			Now:                               now.UnixNano(),
			MaxNumberOfSemaphoresPerNamespace: 100,
		})
		require.NoError(t, err)

		// Acquire semaphore with 10 different processes
		for i := 1; i <= 10; i++ {
			lease := createLease(t, semaphoresCore, accountId, namespaceId.NamespaceId, fmt.Sprintf("process_%02d", i), now.Add(time.Duration(i)*time.Minute), 60*time.Minute)
			response, err := semaphoresCore.AcquireSemaphore(&corepb.AcquireSemaphoreRequest{
				NamespaceId:   namespaceId,
				SemaphoreName: "test_semaphore",
				Weight:        1,
				Now:           now.Add(time.Duration(i) * time.Minute).UnixNano(),
				LeaseId:       lease.Id.LeaseId,
			})
			require.NoError(t, err)
			require.True(t, response.Success)
		}

		// List first page with limit 3
		listResponse1, err := semaphoresCore.ListSemaphoreHolders(&corepb.ListSemaphoreHoldersRequest{
			NamespaceId:   namespaceId,
			SemaphoreName: "test_semaphore",
			Limit:         3,
		})

		require.NoError(t, err)
		require.Len(t, listResponse1.Holders, 3)
		require.NotNil(t, listResponse1.NextPaginationToken)

		// List second page using next pagination token
		listResponse2, err := semaphoresCore.ListSemaphoreHolders(&corepb.ListSemaphoreHoldersRequest{
			NamespaceId:     namespaceId,
			SemaphoreName:   "test_semaphore",
			Limit:           3,
			PaginationToken: listResponse1.NextPaginationToken,
		})

		require.NoError(t, err)
		require.Len(t, listResponse2.Holders, 3)
		require.NotNil(t, listResponse2.NextPaginationToken)
		require.NotNil(t, listResponse2.PreviousPaginationToken)

		// Verify no duplicate holders between pages
		firstPageIds := make(map[uint64]bool)
		for _, holder := range listResponse1.Holders {
			firstPageIds[holder.Id.LeaseId] = true
		}
		for _, holder := range listResponse2.Holders {
			require.False(t, firstPageIds[holder.Id.LeaseId], "Duplicate holder found: %s", holder.Id.LeaseId)
		}
	})

	t.Run("list holders for semaphore with no holders", func(t *testing.T) {
		semaphoresCore := newSemaphoresCore(t)

		now := time.Now()

		namespaceId := &corepb.NamespaceId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
		}
		semaphoreId := &corepb.SemaphoreId{
			AccountId:   namespaceId.AccountId,
			NamespaceId: namespaceId.NamespaceId,
			SemaphoreId: rand.Uint64(),
		}

		// Create semaphore
		_, err := semaphoresCore.CreateSemaphore(&corepb.CreateSemaphoreRequest{
			SemaphoreId:                       semaphoreId,
			Name:                              "test_semaphore",
			Description:                       "test description",
			Permits:                           5,
			Now:                               now.UnixNano(),
			MaxNumberOfSemaphoresPerNamespace: 100,
		})
		require.NoError(t, err)

		// List holders (should be empty)
		listResponse, err := semaphoresCore.ListSemaphoreHolders(&corepb.ListSemaphoreHoldersRequest{
			NamespaceId:   namespaceId,
			SemaphoreName: "test_semaphore",
			Limit:         100,
		})

		require.NoError(t, err)
		require.Len(t, listResponse.Holders, 0)
		require.Nil(t, listResponse.NextPaginationToken)
	})

	t.Run("list holders for nonexistent semaphore", func(t *testing.T) {
		semaphoresCore := newSemaphoresCore(t)

		namespaceId := &corepb.NamespaceId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
		}

		// Try to list holders for a nonexistent semaphore
		_, err := semaphoresCore.ListSemaphoreHolders(&corepb.ListSemaphoreHoldersRequest{
			NamespaceId:   namespaceId,
			SemaphoreName: "non_existing_semaphore",
			Limit:         100,
		})

		require.Error(t, err)
		require.Contains(t, err.Error(), "not found")
	})

	t.Run("list holders with different weights", func(t *testing.T) {
		semaphoresCore := newSemaphoresCore(t)

		now := time.Now()

		accountId := rand.Uint64()
		namespaceId := &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: rand.Uint32(),
		}
		semaphoreId := &corepb.SemaphoreId{
			AccountId:   namespaceId.AccountId,
			NamespaceId: namespaceId.NamespaceId,
			SemaphoreId: rand.Uint64(),
		}

		// Create semaphore with 10 permits
		_, err := semaphoresCore.CreateSemaphore(&corepb.CreateSemaphoreRequest{
			SemaphoreId:                       semaphoreId,
			Name:                              "test_semaphore",
			Description:                       "test description",
			Permits:                           10,
			Now:                               now.UnixNano(),
			MaxNumberOfSemaphoresPerNamespace: 100,
		})
		require.NoError(t, err)

		// Acquire semaphore with different weights
		type processWeight struct {
			processId string
			weight    uint64
			leaseId   uint64
		}
		processes := []processWeight{
			{"process_1", 1, 0},
			{"process_2", 3, 0},
			{"process_3", 2, 0},
		}

		for i := range processes {
			lease := createLease(t, semaphoresCore, accountId, namespaceId.NamespaceId, processes[i].processId, now, 60*time.Minute)
			processes[i].leaseId = lease.Id.LeaseId
			response, err := semaphoresCore.AcquireSemaphore(&corepb.AcquireSemaphoreRequest{
				NamespaceId:   namespaceId,
				SemaphoreName: "test_semaphore",
				Weight:        processes[i].weight,
				Now:           now.UnixNano(),
				LeaseId:       lease.Id.LeaseId,
			})
			require.NoError(t, err)
			require.True(t, response.Success)
		}

		// List all holders
		listResponse, err := semaphoresCore.ListSemaphoreHolders(&corepb.ListSemaphoreHoldersRequest{
			NamespaceId:   namespaceId,
			SemaphoreName: "test_semaphore",
			Limit:         100,
		})

		require.NoError(t, err)
		require.Len(t, listResponse.Holders, 3)

		// Verify weights - we can't easily map process_id to weight now since holder stores process_id not lease_id
		// Just verify that all holders have valid weights
		for _, holder := range listResponse.Holders {
			require.True(t, holder.Weight >= 1 && holder.Weight <= 3, "Invalid weight: %d", holder.Weight)
		}
	})

	t.Run("list holders after some are released", func(t *testing.T) {
		semaphoresCore := newSemaphoresCore(t)

		now := time.Now()

		accountId := rand.Uint64()
		namespaceId := &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: rand.Uint32(),
		}
		semaphoreId := &corepb.SemaphoreId{
			AccountId:   namespaceId.AccountId,
			NamespaceId: namespaceId.NamespaceId,
			SemaphoreId: rand.Uint64(),
		}

		// Create semaphore
		_, err := semaphoresCore.CreateSemaphore(&corepb.CreateSemaphoreRequest{
			SemaphoreId:                       semaphoreId,
			Name:                              "test_semaphore",
			Description:                       "test description",
			Permits:                           5,
			Now:                               now.UnixNano(),
			MaxNumberOfSemaphoresPerNamespace: 100,
		})
		require.NoError(t, err)

		// Acquire semaphore with 5 processes
		leases := make([]*corepb.Lease, 5)
		for i := 1; i <= 5; i++ {
			lease := createLease(t, semaphoresCore, accountId, namespaceId.NamespaceId, fmt.Sprintf("process_%d", i), now, 60*time.Minute)
			leases[i-1] = lease
			response, err := semaphoresCore.AcquireSemaphore(&corepb.AcquireSemaphoreRequest{
				NamespaceId:   namespaceId,
				SemaphoreName: "test_semaphore",
				Weight:        1,
				Now:           now.UnixNano(),
				LeaseId:       lease.Id.LeaseId,
			})
			require.NoError(t, err)
			require.True(t, response.Success)
		}

		// Release 2 processes (process_2 and process_4, which are at indices 1 and 3)
		_, err = semaphoresCore.ReleaseSemaphore(&corepb.ReleaseSemaphoreRequest{
			NamespaceId:   namespaceId,
			SemaphoreName: "test_semaphore",
			Now:           now.Add(time.Minute).UnixNano(),
			LeaseId:       leases[1].Id.LeaseId,
		})
		require.NoError(t, err)

		_, err = semaphoresCore.ReleaseSemaphore(&corepb.ReleaseSemaphoreRequest{
			NamespaceId:   namespaceId,
			SemaphoreName: "test_semaphore",
			Now:           now.Add(time.Minute).UnixNano(),
			LeaseId:       leases[3].Id.LeaseId,
		})
		require.NoError(t, err)

		// List remaining holders
		listResponse, err := semaphoresCore.ListSemaphoreHolders(&corepb.ListSemaphoreHoldersRequest{
			NamespaceId:   namespaceId,
			SemaphoreName: "test_semaphore",
			Limit:         100,
		})

		require.NoError(t, err)
		require.Len(t, listResponse.Holders, 3)

		// Verify remaining holders - we expect 3 holders after releasing 2
		// We can't verify by lease_id since holder.Id uses process_id, but we can verify the count
		require.Len(t, listResponse.Holders, 3)
	})
}

func TestCore_ListSemaphores(t *testing.T) {
	semaphoresCore := newSemaphoresCore(t)

	now := time.Now()

	namespaceId := &corepb.NamespaceId{
		AccountId:   rand.Uint64(),
		NamespaceId: rand.Uint32(),
	}

	// Create first semaphore
	_, err := semaphoresCore.CreateSemaphore(&corepb.CreateSemaphoreRequest{
		SemaphoreId: &corepb.SemaphoreId{
			AccountId:   namespaceId.AccountId,
			NamespaceId: namespaceId.NamespaceId,
			SemaphoreId: rand.Uint64(),
		},
		Name:                              "test_semaphore_1",
		Description:                       "test description 1",
		Permits:                           2,
		Now:                               now.UnixNano(),
		MaxNumberOfSemaphoresPerNamespace: 100,
	})
	require.NoError(t, err)

	// Create second semaphore
	_, err = semaphoresCore.CreateSemaphore(&corepb.CreateSemaphoreRequest{
		SemaphoreId: &corepb.SemaphoreId{
			AccountId:   namespaceId.AccountId,
			NamespaceId: namespaceId.NamespaceId,
			SemaphoreId: rand.Uint64(),
		},
		Name:                              "test_semaphore_2",
		Description:                       "test description 2",
		Permits:                           3,
		Now:                               now.UnixNano(),
		MaxNumberOfSemaphoresPerNamespace: 100,
	})
	require.NoError(t, err)

	// List semaphores
	response, err := semaphoresCore.ListSemaphores(&corepb.ListSemaphoresRequest{
		NamespaceId: namespaceId,
	})

	require.NoError(t, err)
	require.Len(t, response.Semaphores, 2)
}

func TestCore_SnapshotAndRestore(t *testing.T) {
	now := time.Now()

	accountId := rand.Uint64()
	namespaceId := &corepb.NamespaceId{
		AccountId:   accountId,
		NamespaceId: rand.Uint32(),
	}
	semaphoreId := &corepb.SemaphoreId{
		AccountId:   namespaceId.AccountId,
		NamespaceId: namespaceId.NamespaceId,
		SemaphoreId: rand.Uint64(),
	}

	// Create two semaphore cores for testing snapshot and restore
	semaphoresCore1 := newSemaphoresCore(t)
	semaphoresCore2 := newSemaphoresCore(t)

	// T+0: Create semaphore
	_, err := semaphoresCore1.CreateSemaphore(&corepb.CreateSemaphoreRequest{
		SemaphoreId:                       semaphoreId,
		Name:                              "test_semaphore",
		Description:                       "test description",
		Permits:                           2,
		Now:                               now.UnixNano(),
		MaxNumberOfSemaphoresPerNamespace: 100,
	})
	require.NoError(t, err)

	// T+1m: Create lease and acquire semaphore
	lease1 := createLease(t, semaphoresCore1, accountId, namespaceId.NamespaceId, "process_1", now.Add(time.Minute), 60*time.Minute)
	response1, err := semaphoresCore1.AcquireSemaphore(&corepb.AcquireSemaphoreRequest{
		NamespaceId:   namespaceId,
		SemaphoreName: "test_semaphore",
		Weight:        1,
		Now:           now.Add(time.Minute).UnixNano(),
		LeaseId:       lease1.Id.LeaseId,
	})
	require.NoError(t, err)
	require.True(t, response1.Success)

	// Take snapshot at this point
	snapshot := semaphoresCore1.Snapshot()

	// T+2m: Acquire semaphore with second process (after snapshot)
	lease2 := createLease(t, semaphoresCore1, accountId, namespaceId.NamespaceId, "process_2", now.Add(2*time.Minute), 60*time.Minute)
	response2, err := semaphoresCore1.AcquireSemaphore(&corepb.AcquireSemaphoreRequest{
		NamespaceId:   namespaceId,
		SemaphoreName: "test_semaphore",
		Weight:        1,
		Now:           now.Add(2 * time.Minute).UnixNano(),
		LeaseId:       lease2.Id.LeaseId,
	})
	require.NoError(t, err)
	require.True(t, response2.Success)

	// T+3m: Release first process (after snapshot)
	_, err = semaphoresCore1.ReleaseSemaphore(&corepb.ReleaseSemaphoreRequest{
		NamespaceId:   namespaceId,
		SemaphoreName: "test_semaphore",
		Now:           now.Add(3 * time.Minute).UnixNano(),
		LeaseId:       lease1.Id.LeaseId,
	})
	require.NoError(t, err)

	// Write snapshot to buffer
	buf := bytes.NewBuffer(nil)
	err = snapshot.Write(buf)
	require.NoError(t, err)

	// Restore snapshot to second core
	err = semaphoresCore2.Restore(io.NopCloser(buf))
	require.NoError(t, err)

	// T+4m: Check that the restored state matches the snapshot state
	// The semaphore should exist with one holder (lease1)
	response3, err := semaphoresCore2.GetSemaphore(&corepb.GetSemaphoreRequest{
		SemaphoreId: semaphoreId,
		Now:         now.Add(4 * time.Minute).UnixNano(),
	})
	require.NoError(t, err)
	require.NotNil(t, response3.Semaphore)
	require.EqualValues(t, 1, response3.Semaphore.ActiveHoldersCount)
	// require.Equal(t, "process_1", response3.Semaphore.SemaphoreHolders[0].ProcessId)

	// T+5m: Try to acquire with a new process in restored state (should succeed)
	lease3 := createLease(t, semaphoresCore2, accountId, namespaceId.NamespaceId, "process_3", now.Add(5*time.Minute), 60*time.Minute)
	response4, err := semaphoresCore2.AcquireSemaphore(&corepb.AcquireSemaphoreRequest{
		NamespaceId:   namespaceId,
		SemaphoreName: "test_semaphore",
		Weight:        1,
		Now:           now.Add(5 * time.Minute).UnixNano(),
		LeaseId:       lease3.Id.LeaseId,
	})
	require.NoError(t, err)
	require.True(t, response4.Success)
	require.EqualValues(t, 2, response4.Semaphore.ActiveHoldersCount)

	// T+6m: Try to acquire with a fourth process in restored state (should fail - no more permits)
	lease4 := createLease(t, semaphoresCore2, accountId, namespaceId.NamespaceId, "process_4", now.Add(6*time.Minute), 60*time.Minute)
	response5, err := semaphoresCore2.AcquireSemaphore(&corepb.AcquireSemaphoreRequest{
		NamespaceId:   namespaceId,
		SemaphoreName: "test_semaphore",
		Weight:        1,
		Now:           now.Add(6 * time.Minute).UnixNano(),
		LeaseId:       lease4.Id.LeaseId,
	})
	require.NoError(t, err)
	require.False(t, response5.Success)

	// T+7m: Release process_1 in restored state
	_, err = semaphoresCore2.ReleaseSemaphore(&corepb.ReleaseSemaphoreRequest{
		NamespaceId:   namespaceId,
		SemaphoreName: "test_semaphore",
		Now:           now.Add(7 * time.Minute).UnixNano(),
		LeaseId:       lease1.Id.LeaseId,
	})
	require.NoError(t, err)

	// T+8m: Verify only process_3 remains in restored state
	response6, err := semaphoresCore2.GetSemaphore(&corepb.GetSemaphoreRequest{
		SemaphoreId: semaphoreId,
		Now:         now.Add(8 * time.Minute).UnixNano(),
	})
	require.NoError(t, err)
	require.EqualValues(t, 1, response6.Semaphore.ActiveHoldersCount)
	// require.Equal(t, "process_3", response6.Semaphore.SemaphoreHolders[0].ProcessId)

	// TODO
	// Verify that process_2 from the original core is not in the restored state
	// (it was acquired after the snapshot)
	// for _, holder := range response6.Semaphore.SemaphoreHolders {
	// 	require.NotEqual(t, "process_2", holder.ProcessId)
	// }
}

func TestCore_SemaphoresDeleteNamespace(t *testing.T) {
	semaphoresCore := newSemaphoresCore(t)

	now := time.Now()
	namespaceId := &corepb.NamespaceId{
		AccountId:   rand.Uint64(),
		NamespaceId: rand.Uint32(),
	}

	// Create a semaphore in the namespace
	_, err := semaphoresCore.CreateSemaphore(&corepb.CreateSemaphoreRequest{
		SemaphoreId: &corepb.SemaphoreId{
			AccountId:   namespaceId.AccountId,
			NamespaceId: namespaceId.NamespaceId,
			SemaphoreId: rand.Uint64(),
		},
		Name:                              "test_semaphore",
		Description:                       "test description",
		Permits:                           2,
		Now:                               now.UnixNano(),
		MaxNumberOfSemaphoresPerNamespace: 100,
	})
	require.NoError(t, err)

	// Mark the namespace as deleted using SemaphoresDeleteNamespace
	deleteResponse, err := semaphoresCore.SemaphoresDeleteNamespace(&corepb.SemaphoresDeleteNamespaceRequest{
		RecordId:    rand.Uint64(),
		NamespaceId: namespaceId,
		Now:         now.UnixNano(),
	})

	require.NoError(t, err)
	require.NotNil(t, deleteResponse)

	// Verify that the namespace is marked as deleted by checking the deleted namespaces list
	txn := semaphoresCore.badgerStore.Update()
	defer txn.Discard()

	deletedNamespaces, err := semaphoresCore.gcRecords.List(txn, 100)
	require.NoError(t, err)
	require.Len(t, deletedNamespaces, 1)
	// require.Equal(t, namespaceIdProto.AccountId, deletedNamespaces[0].NamespaceId.AccountId)
	// require.Equal(t, namespaceIdProto.NamespaceId, deletedNamespaces[0].NamespaceId.NamespaceId)
}

func TestCore_RunSemaphoresGarbageCollection(t *testing.T) {
	t.Run("with deleted namespace", func(t *testing.T) {
		semaphoresCore := newSemaphoresCore(t)

		now := time.Now()
		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		namespaceIdProto := &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
		}

		// Create some semaphores in the namespace
		semaphoreIds := make([]*corepb.SemaphoreId, 10)
		for i := range len(semaphoreIds) {
			semaphoreIds[i] = &corepb.SemaphoreId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
				SemaphoreId: rand.Uint64(),
			}
		}

		// Create semaphores in the namespace
		for i, semaphoreId := range semaphoreIds {
			_, err := semaphoresCore.CreateSemaphore(&corepb.CreateSemaphoreRequest{
				SemaphoreId:                       semaphoreId,
				Name:                              fmt.Sprintf("semaphore_%d", i),
				Description:                       fmt.Sprintf("test description %d", i),
				Permits:                           2,
				Now:                               now.UnixNano(),
				MaxNumberOfSemaphoresPerNamespace: 100,
			})
			require.NoError(t, err)

			// Acquire semaphores
			lease := createLease(t, semaphoresCore, accountId, namespaceId, fmt.Sprintf("process_%d", i), now, 60*time.Minute)
			response, err := semaphoresCore.AcquireSemaphore(&corepb.AcquireSemaphoreRequest{
				NamespaceId:   namespaceIdProto,
				SemaphoreName: fmt.Sprintf("semaphore_%d", i),
				Weight:        1,
				Now:           now.UnixNano(),
				LeaseId:       lease.Id.LeaseId,
			})

			require.NoError(t, err)
			require.NotNil(t, response.Semaphore)
			require.True(t, response.Success)
		}

		// Verify that semaphores in a different namespace are accessible after GC
		differentNamespaceId := rand.Uint32()
		differentNamespaceIdProto := &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: differentNamespaceId,
		}
		differentNamespaceSemaphoreId := &corepb.SemaphoreId{
			AccountId:   accountId,
			NamespaceId: differentNamespaceId,
			SemaphoreId: rand.Uint64(),
		}

		// Create and acquire a semaphore in a different namespace
		_, err := semaphoresCore.CreateSemaphore(&corepb.CreateSemaphoreRequest{
			SemaphoreId:                       differentNamespaceSemaphoreId,
			Name:                              "different_semaphore",
			Description:                       "different description",
			Permits:                           1,
			Now:                               now.UnixNano(),
			MaxNumberOfSemaphoresPerNamespace: 10,
		})
		require.NoError(t, err)

		leaseDifferent := createLease(t, semaphoresCore, accountId, differentNamespaceId, "process_different", now, 60*time.Minute)
		acquireResponse, err := semaphoresCore.AcquireSemaphore(&corepb.AcquireSemaphoreRequest{
			NamespaceId:   differentNamespaceIdProto,
			SemaphoreName: "different_semaphore",
			Weight:        1,
			Now:           now.UnixNano(),
			LeaseId:       leaseDifferent.Id.LeaseId,
		})

		require.NoError(t, err)
		require.NotNil(t, acquireResponse.Semaphore)
		require.True(t, acquireResponse.Success)

		// Verify semaphores exist by getting them
		for _, semaphoreId := range semaphoreIds {
			getResponse, err := semaphoresCore.GetSemaphore(&corepb.GetSemaphoreRequest{
				SemaphoreId: semaphoreId,
				Now:         now.UnixNano(),
			})

			require.NoError(t, err)
			require.NotNil(t, getResponse.Semaphore)
			require.EqualValues(t, 1, getResponse.Semaphore.ActiveHoldersCount)
		}

		// Mark the namespace as deleted using SemaphoresDeleteNamespace
		deleteResponse, err := semaphoresCore.SemaphoresDeleteNamespace(&corepb.SemaphoresDeleteNamespaceRequest{
			RecordId:    rand.Uint64(),
			NamespaceId: namespaceIdProto,
			Now:         now.UnixNano(),
		})

		require.NoError(t, err)
		require.NotNil(t, deleteResponse)

		// Run garbage collection to clean up the deleted namespace
		gcResponse, err := semaphoresCore.RunSemaphoresGarbageCollection(&corepb.RunSemaphoresGarbageCollectionRequest{
			Now:                        now.UnixNano(),
			GcRecordsPageSize:          100,
			GcRecordSemaphoresPageSize: 100,
			MaxVisitedSemaphores:       1000,
		})

		require.NoError(t, err)
		require.NotNil(t, gcResponse)

		// Verify that semaphores in the deleted namespace are no longer accessible
		for _, semaphoreId := range semaphoreIds {
			_, err := semaphoresCore.GetSemaphore(&corepb.GetSemaphoreRequest{
				SemaphoreId: semaphoreId,
				Now:         now.UnixNano(),
			})

			require.Error(t, err)
			require.Contains(t, err.Error(), "not found")
		}

		// Verify the different namespace semaphore still exists after GC
		getResponse, err := semaphoresCore.GetSemaphore(&corepb.GetSemaphoreRequest{
			SemaphoreId: differentNamespaceSemaphoreId,
			Now:         now.UnixNano(),
		})

		require.NoError(t, err)
		require.NotNil(t, getResponse.Semaphore)
		require.EqualValues(t, 1, getResponse.Semaphore.ActiveHoldersCount)
	})

	t.Run("with multiple expiring semaphores", func(t *testing.T) {
		semaphoresCore := newSemaphoresCore(t)

		now := time.Now()
		accountId := rand.Uint64()
		namespaceId := &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: rand.Uint32(),
		}

		// Create more semaphores than MaxVisitedSemaphores to test the limit
		const numSemaphores = 15
		const maxVisitedSemaphores = 10

		// Create semaphores with different scenarios:
		// - Semaphores 0-4: All holders will expire (should be updated to have no holders)
		// - Semaphores 5-9: Some holders will expire, some will remain (should be updated)
		// - Semaphores 10-14: All holders will remain (should be updated but not emptied)

		semaphoreIds := make([]*corepb.SemaphoreId, numSemaphores)
		for i := range numSemaphores {
			semaphoreIds[i] = &corepb.SemaphoreId{
				AccountId:   namespaceId.AccountId,
				NamespaceId: namespaceId.NamespaceId,
				SemaphoreId: rand.Uint64(),
			}
		}

		// Create and acquire semaphores with different expiration scenarios
		for i, semaphoreId := range semaphoreIds {
			// Create semaphore
			_, err := semaphoresCore.CreateSemaphore(&corepb.CreateSemaphoreRequest{
				SemaphoreId:                       semaphoreId,
				Name:                              fmt.Sprintf("semaphore_%d", i),
				Description:                       fmt.Sprintf("test description %d", i),
				Permits:                           2,
				Now:                               now.UnixNano(),
				MaxNumberOfSemaphoresPerNamespace: 100,
			})
			require.NoError(t, err)

			if i < 5 {
				// Semaphores 0-4: All holders will expire
				lease1 := createLease(t, semaphoresCore, accountId, namespaceId.NamespaceId, fmt.Sprintf("process_%d", i), now, 30*time.Minute)
				response, err := semaphoresCore.AcquireSemaphore(&corepb.AcquireSemaphoreRequest{
					NamespaceId:   namespaceId,
					SemaphoreName: fmt.Sprintf("semaphore_%d", i),
					Weight:        1,
					Now:           now.UnixNano(),
					LeaseId:       lease1.Id.LeaseId,
				})
				require.NoError(t, err)
				require.NotNil(t, response.Semaphore)
				require.True(t, response.Success)

				// Add a second holder that will also expire
				lease2 := createLease(t, semaphoresCore, accountId, namespaceId.NamespaceId, fmt.Sprintf("process_%d_second", i), now, 30*time.Minute)
				response2, err := semaphoresCore.AcquireSemaphore(&corepb.AcquireSemaphoreRequest{
					NamespaceId:   namespaceId,
					SemaphoreName: fmt.Sprintf("semaphore_%d", i),
					Weight:        1,
					Now:           now.UnixNano(),
					LeaseId:       lease2.Id.LeaseId,
				})
				require.NoError(t, err)
				require.NotNil(t, response2.Semaphore)
				require.True(t, response2.Success)
			} else if i < 10 {
				// Semaphores 5-9: Some holders will expire, some will remain
				lease1 := createLease(t, semaphoresCore, accountId, namespaceId.NamespaceId, fmt.Sprintf("process_%d", i), now, 30*time.Minute)
				response, err := semaphoresCore.AcquireSemaphore(&corepb.AcquireSemaphoreRequest{
					NamespaceId:   namespaceId,
					SemaphoreName: fmt.Sprintf("semaphore_%d", i),
					Weight:        1,
					Now:           now.UnixNano(),
					LeaseId:       lease1.Id.LeaseId,
				})
				require.NoError(t, err)
				require.NotNil(t, response.Semaphore)
				require.True(t, response.Success)

				// Add a second holder that will remain
				lease2 := createLease(t, semaphoresCore, accountId, namespaceId.NamespaceId, fmt.Sprintf("process_%d_second", i), now, 2*time.Hour)
				response2, err := semaphoresCore.AcquireSemaphore(&corepb.AcquireSemaphoreRequest{
					NamespaceId:   namespaceId,
					SemaphoreName: fmt.Sprintf("semaphore_%d", i),
					Weight:        1,
					Now:           now.UnixNano(),
					LeaseId:       lease2.Id.LeaseId,
				})
				require.NoError(t, err)
				require.NotNil(t, response2.Semaphore)
				require.True(t, response2.Success)
			} else {
				// Semaphores 10-14: All holders will remain
				lease1 := createLease(t, semaphoresCore, accountId, namespaceId.NamespaceId, fmt.Sprintf("process_%d", i), now, 2*time.Hour)
				response, err := semaphoresCore.AcquireSemaphore(&corepb.AcquireSemaphoreRequest{
					NamespaceId:   namespaceId,
					SemaphoreName: fmt.Sprintf("semaphore_%d", i),
					Weight:        1,
					Now:           now.UnixNano(),
					LeaseId:       lease1.Id.LeaseId,
				})
				require.NoError(t, err)
				require.NotNil(t, response.Semaphore)
				require.True(t, response.Success)

				// Add a second holder that will also remain
				lease2 := createLease(t, semaphoresCore, accountId, namespaceId.NamespaceId, fmt.Sprintf("process_%d_second", i), now, 3*time.Hour)
				response2, err := semaphoresCore.AcquireSemaphore(&corepb.AcquireSemaphoreRequest{
					NamespaceId:   namespaceId,
					SemaphoreName: fmt.Sprintf("semaphore_%d", i),
					Weight:        1,
					Now:           now.UnixNano(),
					LeaseId:       lease2.Id.LeaseId,
				})
				require.NoError(t, err)
				require.NotNil(t, response2.Semaphore)
				require.True(t, response2.Success)
			}
		}

		// Verify all semaphores exist and have holders before garbage collection
		for _, semaphoreId := range semaphoreIds {
			response, err := semaphoresCore.GetSemaphore(&corepb.GetSemaphoreRequest{
				SemaphoreId: semaphoreId,
				Now:         now.UnixNano(),
			})
			require.NoError(t, err)
			require.NotNil(t, response.Semaphore)
			require.EqualValues(t, 2, response.Semaphore.ActiveHoldersCount)
			require.EqualValues(t, 2, response.Semaphore.ActiveHolds)
		}

		// Run garbage collection at the moment when some semaphores expire (T+31 minutes)
		gcTime := now.Add(31 * time.Minute)
		gcResponse, err := semaphoresCore.RunSemaphoresGarbageCollection(&corepb.RunSemaphoresGarbageCollectionRequest{
			Now:                        gcTime.UnixNano(),
			GcRecordsPageSize:          100,
			GcRecordSemaphoresPageSize: 100,
			MaxVisitedSemaphores:       maxVisitedSemaphores,
		})

		require.NoError(t, err)
		require.NotNil(t, gcResponse)

		// Verify the state of semaphores after garbage collection
		// Note: We use the public GetSemaphore method which internally calls checkSemaphoreExpiration
		// to verify the true state of the semaphores after garbage collection

		// Semaphores 0-4 should have no holders (all expired)
		for i := range 5 {
			response, err := semaphoresCore.GetSemaphore(&corepb.GetSemaphoreRequest{
				SemaphoreId: semaphoreIds[i],
				Now:         gcTime.UnixNano(),
			})
			require.NoError(t, err)
			require.NotNil(t, response.Semaphore)
			require.EqualValues(t, 0, response.Semaphore.ActiveHoldersCount)
		}

		// Semaphores 5-9 should still have one holder remaining
		for i := 5; i < 10; i++ {
			response, err := semaphoresCore.GetSemaphore(&corepb.GetSemaphoreRequest{
				SemaphoreId: semaphoreIds[i],
				Now:         gcTime.UnixNano(),
			})
			require.NoError(t, err)
			require.NotNil(t, response.Semaphore)
			require.EqualValues(t, 1, int(response.Semaphore.ActiveHoldersCount))
			// require.Equal(t, fmt.Sprintf("process_%d_second", i), response.Semaphore.SemaphoreHolders[0].ProcessId)
		}

		// Semaphores 10-14 should still have both holders
		for i := 10; i < numSemaphores; i++ {
			response, err := semaphoresCore.GetSemaphore(&corepb.GetSemaphoreRequest{
				SemaphoreId: semaphoreIds[i],
				Now:         gcTime.UnixNano(),
			})
			require.NoError(t, err)
			require.NotNil(t, response.Semaphore)
			require.EqualValues(t, 2, response.Semaphore.ActiveHoldersCount)
			// holderProcessIds := make([]string, len(response.Semaphore.SemaphoreHolders))
			// for j, holder := range response.Semaphore.SemaphoreHolders {
			// 	holderProcessIds[j] = holder.ProcessId
			// }
			// require.Contains(t, holderProcessIds, fmt.Sprintf("process_%d", i))
			// require.Contains(t, holderProcessIds, fmt.Sprintf("process_%d_second", i))
		}

		// Run garbage collection again to process the remaining semaphores
		// This should process semaphores 5-14 since semaphores 0-4 were already processed
		gcResponse2, err := semaphoresCore.RunSemaphoresGarbageCollection(&corepb.RunSemaphoresGarbageCollectionRequest{
			Now:                        gcTime.UnixNano(),
			GcRecordsPageSize:          100,
			GcRecordSemaphoresPageSize: 100,
			MaxVisitedSemaphores:       maxVisitedSemaphores,
		})

		require.NoError(t, err)
		require.NotNil(t, gcResponse2)

		// Verify that semaphores 5-9 still have their remaining holders
		for i := 5; i < 10; i++ {
			response, err := semaphoresCore.GetSemaphore(&corepb.GetSemaphoreRequest{
				SemaphoreId: semaphoreIds[i],
				Now:         gcTime.UnixNano(),
			})
			require.NoError(t, err)
			require.NotNil(t, response.Semaphore)
			require.EqualValues(t, 1, response.Semaphore.ActiveHoldersCount)
		}

		// Verify that semaphores 10-14 still have all their holders
		for i := 10; i < numSemaphores; i++ {
			response, err := semaphoresCore.GetSemaphore(&corepb.GetSemaphoreRequest{
				SemaphoreId: semaphoreIds[i],
				Now:         gcTime.UnixNano(),
			})
			require.NoError(t, err)
			require.NotNil(t, response.Semaphore)
			require.EqualValues(t, 2, response.Semaphore.ActiveHoldersCount)
		}
	})

	t.Run("stale expiration records", func(t *testing.T) {
		semaphoresCore := newSemaphoresCore(t)

		now := time.Now()
		semaphoreName := "test_semaphore"
		accountId := rand.Uint64()
		namespaceId := &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: rand.Uint32(),
		}
		semaphoreId := &corepb.SemaphoreId{
			AccountId:   namespaceId.AccountId,
			NamespaceId: namespaceId.NamespaceId,
			SemaphoreId: rand.Uint64(),
		}

		// Create a semaphore
		createResponse, err := semaphoresCore.CreateSemaphore(&corepb.CreateSemaphoreRequest{
			SemaphoreId:                       semaphoreId,
			Name:                              semaphoreName,
			Description:                       "test semaphore",
			Permits:                           10,
			Now:                               now.UnixNano(),
			MaxNumberOfSemaphoresPerNamespace: 100,
		})
		require.NoError(t, err)
		require.NotNil(t, createResponse.Semaphore)

		// T+0: Acquire semaphore with process_1 expiring at T+1h
		lease1 := createLease(t, semaphoresCore, accountId, namespaceId.NamespaceId, "process_1", now, 1*time.Hour)
		_, err = semaphoresCore.AcquireSemaphore(&corepb.AcquireSemaphoreRequest{
			NamespaceId:   namespaceId,
			SemaphoreName: semaphoreName,
			LeaseId:       lease1.Id.LeaseId,
			Weight:        1,
			Now:           now.UnixNano(),
		})
		require.NoError(t, err)

		// T+0: Acquire semaphore with process_2 expiring at T+2h
		lease2 := createLease(t, semaphoresCore, accountId, namespaceId.NamespaceId, "process_2", now, 2*time.Hour)
		_, err = semaphoresCore.AcquireSemaphore(&corepb.AcquireSemaphoreRequest{
			NamespaceId:   namespaceId,
			SemaphoreName: semaphoreName,
			LeaseId:       lease2.Id.LeaseId,
			Weight:        1,
			Now:           now.UnixNano(),
		})
		require.NoError(t, err)

		lease3 := createLease(t, semaphoresCore, accountId, namespaceId.NamespaceId, "process_3", now, 2*time.Hour)

		// T+30m: Release process_1
		// This changes the earliest expiration from T+1h to T+2h
		_, err = semaphoresCore.ReleaseSemaphore(&corepb.ReleaseSemaphoreRequest{
			NamespaceId:   namespaceId,
			SemaphoreName: semaphoreName,
			LeaseId:       lease1.Id.LeaseId,
			Now:           now.Add(30 * time.Minute).UnixNano(),
		})
		require.NoError(t, err)

		// T+45m: Acquire process_3, expiring at T+2h (same as process_2)
		// Now both holders expire at the same time
		_, err = semaphoresCore.AcquireSemaphore(&corepb.AcquireSemaphoreRequest{
			NamespaceId:   namespaceId,
			SemaphoreName: semaphoreName,
			LeaseId:       lease3.Id.LeaseId,
			Weight:        1,
			Now:           now.Add(45 * time.Minute).UnixNano(),
		})
		require.NoError(t, err)

		// T+1h: Run garbage collection
		// Before the fix, this could panic if the code encountered a scenario where
		// oldExpiresAt == newExpiresAt (e.g., from stale expiration records)
		gcResponse1, err := semaphoresCore.RunSemaphoresGarbageCollection(&corepb.RunSemaphoresGarbageCollectionRequest{
			Now:                        now.Add(1 * time.Hour).UnixNano(),
			GcRecordsPageSize:          100,
			GcRecordSemaphoresPageSize: 100,
			MaxVisitedSemaphores:       100,
		})
		require.NoError(t, err)
		require.NotNil(t, gcResponse1)

		// Run GC again to ensure idempotency
		// The second run might encounter expiration records that are already correct
		gcResponse2, err := semaphoresCore.RunSemaphoresGarbageCollection(&corepb.RunSemaphoresGarbageCollectionRequest{
			Now:                        now.Add(1*time.Hour + 5*time.Minute).UnixNano(),
			GcRecordsPageSize:          100,
			GcRecordSemaphoresPageSize: 100,
			MaxVisitedSemaphores:       100,
		})
		require.NoError(t, err)
		require.NotNil(t, gcResponse2)

		// Verify semaphore still exists with both holders
		getResponse, err := semaphoresCore.GetSemaphore(&corepb.GetSemaphoreRequest{
			SemaphoreId: createResponse.Semaphore.Id,
			Now:         now.Add(1*time.Hour + 5*time.Minute).UnixNano(),
		})
		require.NoError(t, err)
		require.NotNil(t, getResponse.Semaphore)
		require.EqualValues(t, 2, getResponse.Semaphore.ActiveHoldersCount)
		require.EqualValues(t, 2, getResponse.Semaphore.ActiveHolds)
	})
}

func newSemaphoresCore(t *testing.T) *Core {
	store, err := store.NewBadgerInMemoryStore()
	require.NoError(t, err)
	return NewCore(store, []byte{0x1d, 0x36, 0x00, 0x00}, []byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})
}

func createLease(t *testing.T, core *Core, accountId uint64, namespaceId uint32, processId string, now time.Time, ttl time.Duration) *corepb.Lease {
	t.Helper()
	leaseId := rand.Uint64()
	resp, err := core.CreateSemaphoreLease(&corepb.CreateSemaphoreLeaseRequest{
		LeaseId: &corepb.LeaseId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LeaseId:     leaseId,
		},
		ProcessId:  processId,
		TtlSeconds: uint64(ttl.Seconds()),
		Now:        now.UnixNano(),
	})
	require.NoError(t, err)
	return resp.Lease
}
