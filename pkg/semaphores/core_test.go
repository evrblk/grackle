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

	"github.com/evrblk/grackle/pkg/coreapis"
	"github.com/evrblk/grackle/pkg/corepb"
	"github.com/evrblk/grackle/pkg/tables"
)

func init() {
	registry := monsterax.NewBaseTableRegistry(1)
	tables.RegisterGracklePrefixes(registry)
}

func TestCore_AcquireSemaphore(t *testing.T) {
	t.Run("acquire existing semaphore", func(t *testing.T) {
		core := newSemaphoresCore(t)
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
		semaphore := createSemaphore(t, core, semaphoreId, "test_semaphore", 5, now)

		require.Equal(t, "test_semaphore", semaphore.Name)
		require.EqualValues(t, 5, semaphore.Permits)

		// T+1m: Create lease and acquire semaphore
		lease := createLease(t, core, accountId, namespaceId.NamespaceId, "process_1", now.Add(time.Minute), 60*time.Minute)

		success, semaphore := acquireSemaphore(t, core, namespaceId, lease.Id, "test_semaphore", 2, now.Add(time.Minute))
		require.True(t, success)
		require.EqualValues(t, 1, semaphore.ActiveHoldersCount)
		require.EqualValues(t, 2, semaphore.ActiveHolds)
		// require.Equal(t, "process_1", resp2.Payload.Semaphore.SemaphoreHolders[0].ProcessId)

		// T+2m: Get semaphore
		semaphore = getSemaphore(t, core, semaphoreId, now.Add(2*time.Minute))
		require.EqualValues(t, 1, semaphore.ActiveHoldersCount)

		// T+62m: Get semaphore after expiration
		semaphore = getSemaphore(t, core, semaphoreId, now.Add(62*time.Minute))
		require.EqualValues(t, 0, semaphore.ActiveHoldersCount)
	})

	t.Run("acquire semaphore repeatedly", func(t *testing.T) {
		core := newSemaphoresCore(t)
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
		_ = createSemaphore(t, core, semaphoreId, "test_semaphore", 2, now)

		// T+1m: Create lease and acquire semaphore
		lease := createLease(t, core, accountId, namespaceId.NamespaceId, "process_1", now.Add(time.Minute), 60*time.Minute)
		success, _ := acquireSemaphore(t, core, namespaceId, lease.Id, "test_semaphore", 1, now.Add(time.Minute))
		require.True(t, success)

		// T+2m: Acquire same semaphore with same process
		success, semaphore := acquireSemaphore(t, core, namespaceId, lease.Id, "test_semaphore", 2, now.Add(2*time.Minute))
		require.True(t, success)
		require.EqualValues(t, 1, semaphore.ActiveHoldersCount)
	})

	t.Run("acquire semaphore with multiple permits", func(t *testing.T) {
		core := newSemaphoresCore(t)
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
		_ = createSemaphore(t, core, semaphoreId, "test_semaphore", 2, now)

		// T+1m: First process acquires semaphore
		lease1 := createLease(t, core, accountId, namespaceId.NamespaceId, "process_1", now.Add(time.Minute), 60*time.Minute)
		success, semaphore := acquireSemaphore(t, core, namespaceId, lease1.Id, "test_semaphore", 1, now.Add(time.Minute))
		require.True(t, success)
		require.EqualValues(t, 1, semaphore.ActiveHoldersCount)

		// T+2m: Second process acquires semaphore
		lease2 := createLease(t, core, accountId, namespaceId.NamespaceId, "process_2", now.Add(2*time.Minute), 60*time.Minute)
		success, semaphore2 := acquireSemaphore(t, core, namespaceId, lease2.Id, "test_semaphore", 1, now.Add(2*time.Minute))
		require.True(t, success)
		require.EqualValues(t, 2, semaphore2.ActiveHoldersCount)

		// T+3m: Third process tries to acquire semaphore (should fail)
		lease3 := createLease(t, core, accountId, namespaceId.NamespaceId, "process_3", now.Add(3*time.Minute), 60*time.Minute)
		success, semaphore3 := acquireSemaphore(t, core, namespaceId, lease3.Id, "test_semaphore", 1, now.Add(3*time.Minute))
		require.False(t, success)
		require.EqualValues(t, 2, semaphore3.ActiveHoldersCount)
	})

	t.Run("acquire nonexistent semaphore", func(t *testing.T) {
		core := newSemaphoresCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespaceId := &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: rand.Uint32(),
		}

		// Try to acquire a nonexistent semaphore
		lease := createLease(t, core, accountId, namespaceId.NamespaceId, "process_1", now, 60*time.Minute)
		appErr := acquireSemaphoreWithError(t, core, namespaceId, lease.Id, "non_existing_semaphore", 1, now)
		require.Equal(t, monsterax.NotFound, appErr.Code)
	})
}

func TestCore_ReleaseSemaphore(t *testing.T) {
	t.Run("release existing semaphore", func(t *testing.T) {
		core := newSemaphoresCore(t)
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
		_ = createSemaphore(t, core, semaphoreId, "test_semaphore", 2, now)

		// T+1m: Create lease and acquire semaphore
		lease := createLease(t, core, accountId, namespaceId.NamespaceId, "process_1", now.Add(time.Minute), 60*time.Minute)
		success, _ := acquireSemaphore(t, core, namespaceId, lease.Id, "test_semaphore", 1, now.Add(time.Minute))
		require.True(t, success)

		// T+2m: Release semaphore
		semaphore := releaseSemaphore(t, core, namespaceId, "test_semaphore", lease.Id, now.Add(2*time.Minute))
		require.EqualValues(t, 0, semaphore.ActiveHoldersCount)
	})

	t.Run("release nonexistent semaphore", func(t *testing.T) {
		core := newSemaphoresCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespaceId := &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: rand.Uint32(),
		}

		// Try to release a nonexistent semaphore
		lease := createLease(t, core, accountId, namespaceId.NamespaceId, "process_1", now, 60*time.Minute)
		appErr := releaseSemaphoreWithError(t, core, namespaceId, "nonexistent_semaphore", lease.Id, now)
		require.Equal(t, monsterax.NotFound, appErr.Code)
	})

	t.Run("release nonexistent lease id", func(t *testing.T) {
		core := newSemaphoresCore(t)
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
		_ = createSemaphore(t, core, semaphoreId, "test_semaphore", 2, now)

		// T+1m: Acquire semaphore with process_1
		lease1 := createLease(t, core, accountId, namespaceId.NamespaceId, "process_1", now.Add(time.Minute), 60*time.Minute)
		success, semaphore := acquireSemaphore(t, core, namespaceId, lease1.Id, "test_semaphore", 1, now.Add(time.Minute))
		require.True(t, success)
		require.EqualValues(t, 1, semaphore.ActiveHoldersCount)

		// T+2m: Try to release semaphore with a nonexistent lease_id (should succeed without error)
		lease2 := createLease(t, core, accountId, namespaceId.NamespaceId, "process_non_existing", now.Add(2*time.Minute), 60*time.Minute)
		semaphore = releaseSemaphore(t, core, namespaceId, "test_semaphore", lease2.Id, now.Add(2*time.Minute))
		// The semaphore should still have 1 holder (process_1), since we tried to release a nonexistent lease_id
		require.EqualValues(t, 1, semaphore.ActiveHoldersCount)
	})
}

func TestCore_UpdateSemaphore(t *testing.T) {
	t.Run("update existing semaphore", func(t *testing.T) {
		core := newSemaphoresCore(t)
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
		_ = createSemaphore(t, core, semaphoreId, "test_semaphore", 2, now)

		// T+1m: Update semaphore
		_ = updateSemaphore(t, core, namespaceId, "test_semaphore", "updated description", 3, now.Add(time.Minute))

		// T+2m: Get updated semaphore
		semaphore := getSemaphore(t, core, semaphoreId, now.Add(2*time.Minute))
		require.Equal(t, "updated description", semaphore.Description)
		require.EqualValues(t, 3, semaphore.Permits)
	})

	t.Run("update semaphore with insufficient permits", func(t *testing.T) {
		core := newSemaphoresCore(t)
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
		_ = createSemaphore(t, core, semaphoreId, "test_semaphore", 3, now)

		// T+1m: First process acquires semaphore
		lease1 := createLease(t, core, accountId, namespaceId.NamespaceId, "process_1", now.Add(time.Minute), 60*time.Minute)
		success, _ := acquireSemaphore(t, core, namespaceId, lease1.Id, "test_semaphore", 1, now.Add(time.Minute))
		require.True(t, success)

		// T+2m: Second process acquires semaphore
		lease2 := createLease(t, core, accountId, namespaceId.NamespaceId, "process_2", now.Add(2*time.Minute), 60*time.Minute)
		success, _ = acquireSemaphore(t, core, namespaceId, lease2.Id, "test_semaphore", 1, now.Add(2*time.Minute))
		require.True(t, success)

		// T+3m: Third process acquires semaphore
		lease3 := createLease(t, core, accountId, namespaceId.NamespaceId, "process_3", now.Add(3*time.Minute), 60*time.Minute)
		success, _ = acquireSemaphore(t, core, namespaceId, lease3.Id, "test_semaphore", 1, now.Add(3*time.Minute))
		require.True(t, success)

		// Verify we have 3 holders
		semaphore := getSemaphore(t, core, semaphoreId, now.Add(4*time.Minute))
		require.EqualValues(t, 3, semaphore.ActiveHoldersCount)

		// T+5m: Try to update semaphore to reduce permits to 2 (less than current holders)
		appErr := updateSemaphoreWithError(t, core, namespaceId, "test_semaphore", "updated description", 2, now.Add(5*time.Minute))
		require.Equal(t, monsterax.InvalidArgument, appErr.Code)

		// Verify the semaphore was not updated
		semaphore = getSemaphore(t, core, semaphoreId, now.Add(6*time.Minute))
		require.Equal(t, "test description", semaphore.Description) // Description should not be updated
		require.EqualValues(t, 3, semaphore.Permits)                // Permits should not be updated
		require.EqualValues(t, 3, semaphore.ActiveHoldersCount)     // All holders should still be there
		require.EqualValues(t, 3, semaphore.ActiveHolds)            // All holders should still be there
	})

	t.Run("update nonexistent semaphore", func(t *testing.T) {
		core := newSemaphoresCore(t)
		now := time.Now()
		namespaceId := &corepb.NamespaceId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
		}

		// Try to update a nonexistent semaphore
		appErr := updateSemaphoreWithError(t, core, namespaceId, "nonexistent_semaphore", "updated description", 3, now)
		require.Equal(t, monsterax.NotFound, appErr.Code)
	})
}

func TestCore_GetSemaphore(t *testing.T) {
	t.Run("get nonexistent semaphore", func(t *testing.T) {
		core := newSemaphoresCore(t)
		now := time.Now()
		nonExistingSemaphoreId := &corepb.SemaphoreId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
			SemaphoreId: rand.Uint64(),
		}

		appErr := getSemaphoreWithError(t, core, nonExistingSemaphoreId, now)
		require.Equal(t, monsterax.NotFound, appErr.Code)
	})
}

func TestCore_GetSemaphoreByName(t *testing.T) {
	t.Run("get existing semaphore by name", func(t *testing.T) {
		core := newSemaphoresCore(t)
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
		_ = createSemaphore(t, core, semaphoreId, "test_semaphore", 5, now)

		// T+1m: Get semaphore by name
		semaphore := getSemaphoreByName(t, core, namespaceId, "test_semaphore", now.Add(time.Minute))
		require.Equal(t, "test_semaphore", semaphore.Name)
		require.Equal(t, "test description", semaphore.Description)
		require.EqualValues(t, 5, semaphore.Permits)
		require.Equal(t, semaphoreId.SemaphoreId, semaphore.Id.SemaphoreId)
	})

	t.Run("get semaphore by name with expired holders", func(t *testing.T) {
		core := newSemaphoresCore(t)
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
		_ = createSemaphore(t, core, semaphoreId, "test_semaphore", 3, now)

		// T+1m: Acquire semaphore with process_1 (expires at T+31m)
		lease1 := createLease(t, core, accountId, namespaceId.NamespaceId, "process_1", now.Add(time.Minute), 30*time.Minute)
		success, _ := acquireSemaphore(t, core, namespaceId, lease1.Id, "test_semaphore", 1, now.Add(time.Minute))
		require.True(t, success)

		// T+2m: Acquire semaphore with process_2 (expires at T+62m)
		lease2 := createLease(t, core, accountId, namespaceId.NamespaceId, "process_2", now.Add(2*time.Minute), 60*time.Minute)
		success, _ = acquireSemaphore(t, core, namespaceId, lease2.Id, "test_semaphore", 1, now.Add(2*time.Minute))
		require.True(t, success)

		// T+3m: Get semaphore by name (both holders should be active)
		semaphore := getSemaphoreByName(t, core, namespaceId, "test_semaphore", now.Add(3*time.Minute))
		require.EqualValues(t, 2, semaphore.ActiveHoldersCount)
		require.EqualValues(t, 2, semaphore.ActiveHolds)

		// T+35m: Get semaphore by name (process_1 should have expired, only process_2 remains)
		semaphore = getSemaphoreByName(t, core, namespaceId, "test_semaphore", now.Add(35*time.Minute))
		require.EqualValues(t, 1, semaphore.ActiveHoldersCount)
		require.EqualValues(t, 1, semaphore.ActiveHolds)

		// T+65m: Get semaphore by name (all holders should have expired)
		semaphore = getSemaphoreByName(t, core, namespaceId, "test_semaphore", now.Add(65*time.Minute))
		require.EqualValues(t, 0, semaphore.ActiveHoldersCount)
		require.EqualValues(t, 0, semaphore.ActiveHolds)
	})

	t.Run("get nonexistent semaphore by name", func(t *testing.T) {
		core := newSemaphoresCore(t)
		now := time.Now()
		namespaceId := &corepb.NamespaceId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
		}

		// Try to get a nonexistent semaphore by name
		appErr := getSemaphoreByNameWithError(t, core, namespaceId, "nonexistent_semaphore", now)
		require.Equal(t, monsterax.NotFound, appErr.Code)
	})

	t.Run("get semaphore by name from different namespace", func(t *testing.T) {
		core := newSemaphoresCore(t)
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

		_ = createSemaphore(t, core, semaphore1Id, "test_semaphore", 5, now)

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

		_ = createSemaphore(t, core, semaphore2Id, "test_semaphore", 3, now)

		// Get semaphore from namespace 1
		semaphore := getSemaphoreByName(t, core, namespace1Id, "test_semaphore", now)
		require.EqualValues(t, 5, semaphore.Permits)
		require.Equal(t, semaphore1Id, semaphore.Id)

		// Get semaphore from namespace 2
		semaphore = getSemaphoreByName(t, core, namespace2Id, "test_semaphore", now)
		require.EqualValues(t, 3, semaphore.Permits)
		require.Equal(t, semaphore2Id, semaphore.Id)
	})
}

func TestCore_CreateSemaphore(t *testing.T) {
	t.Run("create semaphore max limit reached", func(t *testing.T) {
		core := newSemaphoresCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespaceId := rand.Uint32()
		maxSemaphores := int64(3)

		// Create semaphores up to the limit
		for i := 0; i < int(maxSemaphores); i++ {
			semaphoreId := &corepb.SemaphoreId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
				SemaphoreId: rand.Uint64(),
			}
			_ = createSemaphore(t, core, semaphoreId, fmt.Sprintf("test_semaphore_%d", i), 2, now)
		}

		// Try to create one more semaphore (should fail)
		semaphoreId := &corepb.SemaphoreId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			SemaphoreId: rand.Uint64(),
		}
		appErr := createSemaphoreWithError(t, core, semaphoreId, "test_semaphore_limit_exceeded", 2, maxSemaphores, now)
		require.Equal(t, monsterax.ResourceExhausted, appErr.Code)
	})

	t.Run("create semaphore with duplicate name", func(t *testing.T) {
		core := newSemaphoresCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespaceId := rand.Uint32()
		semaphore1Id := &corepb.SemaphoreId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			SemaphoreId: rand.Uint64(),
		}
		semaphore2Id := &corepb.SemaphoreId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			SemaphoreId: rand.Uint64(),
		}

		// Create first semaphore
		_ = createSemaphore(t, core, semaphore1Id, "test_semaphore", 2, now)

		// Try to create a second semaphore with the same name
		appErr := createSemaphoreWithError(t, core, semaphore2Id, "test_semaphore", 3, 100, now)
		require.Equal(t, monsterax.AlreadyExists, appErr.Code)
	})
}

func TestCore_DeleteSemaphore(t *testing.T) {
	t.Run("delete existing semaphore", func(t *testing.T) {
		core := newSemaphoresCore(t)
		now := time.Now()
		semaphoreId := &corepb.SemaphoreId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
			SemaphoreId: rand.Uint64(),
		}

		// T+0: Create semaphore
		_ = createSemaphore(t, core, semaphoreId, "test_semaphore", 2, now)

		// T+1m: Verify semaphore exists
		_ = getSemaphore(t, core, semaphoreId, now.Add(time.Minute))

		// T+2m: Delete semaphore
		resp3, err := core.DeleteSemaphore(&coreapis.DeleteSemaphoreRequest{
			Payload: &corepb.DeleteSemaphoreRequest{
				NamespaceId: &corepb.NamespaceId{
					AccountId:   semaphoreId.AccountId,
					NamespaceId: semaphoreId.NamespaceId,
				},
				SemaphoreName: "test_semaphore",
			},
		})
		require.NoError(t, err)
		require.NotNil(t, resp3)
		require.Nil(t, resp3.ApplicationError)
		require.NotNil(t, resp3.Payload)

		// T+3m: Verify semaphore no longer exists
		appErr := getSemaphoreWithError(t, core, semaphoreId, now.Add(3*time.Minute))
		require.Equal(t, monsterax.NotFound, appErr.Code)
	})

	t.Run("delete nonexistent semaphore", func(t *testing.T) {
		core := newSemaphoresCore(t)

		// Try to delete a nonexistent semaphore (should succeed without error)
		resp1, err := core.DeleteSemaphore(&coreapis.DeleteSemaphoreRequest{
			Payload: &corepb.DeleteSemaphoreRequest{
				NamespaceId: &corepb.NamespaceId{
					AccountId:   rand.Uint64(),
					NamespaceId: rand.Uint32(),
				},
				SemaphoreName: "nonexistent_semaphore",
			},
		})

		require.NoError(t, err)
		require.NotNil(t, resp1)
		require.Nil(t, resp1.ApplicationError)
		require.NotNil(t, resp1.Payload)
	})

	t.Run("delete semaphore cleans up expiration records", func(t *testing.T) {
		core := newSemaphoresCore(t)
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
		_ = createSemaphore(t, core, semaphoreId, "test_semaphore", 2, now)

		// Acquire semaphore to create an expiration record
		lease := createLease(t, core, accountId, namespaceId.NamespaceId, "process_1", now, 60*time.Minute)
		success, _ := acquireSemaphore(t, core, namespaceId, lease.Id, "test_semaphore", 1, now.Add(time.Minute))
		require.True(t, success)

		// Delete semaphore
		resp3, err := core.DeleteSemaphore(&coreapis.DeleteSemaphoreRequest{
			Payload: &corepb.DeleteSemaphoreRequest{
				NamespaceId:   namespaceId,
				SemaphoreName: "test_semaphore",
			},
		})
		require.NoError(t, err)
		require.NotNil(t, resp3)
		require.Nil(t, resp3.ApplicationError)
		require.NotNil(t, resp3.Payload)

		// Run GC to verify there are no orphaned expiration records
		resp4, err := core.RunSemaphoresGarbageCollection(&coreapis.RunSemaphoresGarbageCollectionRequest{
			Payload: &corepb.RunSemaphoresGarbageCollectionRequest{
				Now:                        now.Add(30 * time.Minute).UnixNano(),
				GcRecordsPageSize:          100,
				GcRecordSemaphoresPageSize: 100,
				MaxVisitedSemaphores:       100,
			},
		})
		require.NoError(t, err)
		require.NotNil(t, resp4)
		require.Nil(t, resp4.ApplicationError)
		require.NotNil(t, resp4.Payload)

		// Verify semaphore is gone
		appErr := getSemaphoreWithError(t, core, semaphoreId, now.Add(30*time.Minute))
		require.Equal(t, monsterax.NotFound, appErr.Code)
	})
}

func TestCore_ListSemaphoreHolders(t *testing.T) {
	t.Run("list holders for semaphore with multiple holders", func(t *testing.T) {
		core := newSemaphoresCore(t)
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
		_ = createSemaphore(t, core, semaphoreId, "test_semaphore", 5, now)

		// Acquire semaphore with 3 different processes
		for i := 1; i <= 3; i++ {
			lease := createLease(t, core, accountId, namespaceId.NamespaceId, fmt.Sprintf("process_%d", i), now.Add(time.Duration(i)*time.Minute), 60*time.Minute)
			success, _ := acquireSemaphore(t, core, namespaceId, lease.Id, "test_semaphore", 1, now.Add(time.Duration(i)*time.Minute))
			require.True(t, success)
		}

		// List all holders
		list := listSemaphoreHolders(t, core, namespaceId, "test_semaphore")
		require.Len(t, list.Holders, 3)

		// Verify holder lease IDs (we can't easily verify by process name anymore since it's via lease)
		for _, holder := range list.Holders {
			require.Equal(t, namespaceId.AccountId, holder.Id.AccountId)
			require.Equal(t, namespaceId.NamespaceId, holder.Id.NamespaceId)
			require.Equal(t, semaphoreId.SemaphoreId, holder.Id.SemaphoreId)
			require.EqualValues(t, 1, holder.Weight)
		}
	})

	t.Run("list holders with pagination", func(t *testing.T) {
		core := newSemaphoresCore(t)
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
		_ = createSemaphore(t, core, semaphoreId, "test_semaphore", 10, now)

		// Acquire semaphore with 10 different processes
		for i := 1; i <= 10; i++ {
			lease := createLease(t, core, accountId, namespaceId.NamespaceId, fmt.Sprintf("process_%02d", i), now.Add(time.Duration(i)*time.Minute), 60*time.Minute)
			success, _ := acquireSemaphore(t, core, namespaceId, lease.Id, "test_semaphore", 1, now.Add(time.Duration(i)*time.Minute))
			require.True(t, success)
		}

		// List first page with limit 3
		resp3, err := core.ListSemaphoreHolders(&coreapis.ListSemaphoreHoldersRequest{
			Payload: &corepb.ListSemaphoreHoldersRequest{
				NamespaceId:   namespaceId,
				SemaphoreName: "test_semaphore",
				Limit:         3,
			},
		})

		require.NoError(t, err)
		require.NotNil(t, resp3)
		require.Nil(t, resp3.ApplicationError)
		require.NotNil(t, resp3.Payload)
		require.Len(t, resp3.Payload.Holders, 3)
		require.NotNil(t, resp3.Payload.NextPaginationToken)

		// List second page using next pagination token
		resp4, err := core.ListSemaphoreHolders(&coreapis.ListSemaphoreHoldersRequest{
			Payload: &corepb.ListSemaphoreHoldersRequest{
				NamespaceId:     namespaceId,
				SemaphoreName:   "test_semaphore",
				Limit:           3,
				PaginationToken: resp3.Payload.NextPaginationToken,
			},
		})

		require.NoError(t, err)
		require.NotNil(t, resp4)
		require.Nil(t, resp4.ApplicationError)
		require.NotNil(t, resp4.Payload)
		require.Len(t, resp4.Payload.Holders, 3)
		require.NotNil(t, resp4.Payload.NextPaginationToken)
		require.NotNil(t, resp4.Payload.PreviousPaginationToken)

		// Verify no duplicate holders between pages
		firstPageIds := make(map[uint64]bool)
		for _, holder := range resp3.Payload.Holders {
			firstPageIds[holder.Id.LeaseId] = true
		}
		for _, holder := range resp4.Payload.Holders {
			require.False(t, firstPageIds[holder.Id.LeaseId], "Duplicate holder found: %s", holder.Id.LeaseId)
		}
	})

	t.Run("list holders for semaphore with no holders", func(t *testing.T) {
		core := newSemaphoresCore(t)
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
		_ = createSemaphore(t, core, semaphoreId, "test_semaphore", 5, now)

		// List holders (should be empty)
		list := listSemaphoreHolders(t, core, namespaceId, "test_semaphore")
		require.Len(t, list.Holders, 0)
		require.Nil(t, list.NextPaginationToken)
	})

	t.Run("list holders for nonexistent semaphore", func(t *testing.T) {
		core := newSemaphoresCore(t)
		namespaceId := &corepb.NamespaceId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
		}

		// Try to list holders for a nonexistent semaphore
		resp1, err := core.ListSemaphoreHolders(&coreapis.ListSemaphoreHoldersRequest{
			Payload: &corepb.ListSemaphoreHoldersRequest{
				NamespaceId:   namespaceId,
				SemaphoreName: "non_existing_semaphore",
				Limit:         100,
			},
		})

		require.NoError(t, err)
		require.NotNil(t, resp1)
		require.Nil(t, resp1.Payload)
		require.NotNil(t, resp1.ApplicationError)
		require.Equal(t, monsterax.NotFound, resp1.ApplicationError.Code)
	})

	t.Run("list holders with different weights", func(t *testing.T) {
		core := newSemaphoresCore(t)
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
		_ = createSemaphore(t, core, semaphoreId, "test_semaphore", 10, now)

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
			lease := createLease(t, core, accountId, namespaceId.NamespaceId, processes[i].processId, now, 60*time.Minute)
			processes[i].leaseId = lease.Id.LeaseId
			success, _ := acquireSemaphore(t, core, namespaceId, lease.Id, "test_semaphore", processes[i].weight, now)
			require.True(t, success)
		}

		// List all holders
		list := listSemaphoreHolders(t, core, namespaceId, "test_semaphore")
		require.Len(t, list.Holders, 3)

		// Verify weights - we can't easily map process_id to weight now since holder stores process_id not lease_id
		// Just verify that all holders have valid weights
		for _, holder := range list.Holders {
			require.True(t, holder.Weight >= 1 && holder.Weight <= 3, "Invalid weight: %d", holder.Weight)
		}
	})

	t.Run("list holders after some are released", func(t *testing.T) {
		core := newSemaphoresCore(t)
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
		_ = createSemaphore(t, core, semaphoreId, "test_semaphore", 5, now)

		// Acquire semaphore with 5 processes
		leases := make([]*corepb.Lease, 5)
		for i := 1; i <= 5; i++ {
			lease := createLease(t, core, accountId, namespaceId.NamespaceId, fmt.Sprintf("process_%d", i), now, 60*time.Minute)
			leases[i-1] = lease
			success, _ := acquireSemaphore(t, core, namespaceId, lease.Id, "test_semaphore", 1, now)
			require.True(t, success)
		}

		// Release 2 processes (process_2 and process_4, which are at indices 1 and 3)
		_ = releaseSemaphore(t, core, namespaceId, "test_semaphore", leases[1].Id, now.Add(time.Minute))
		_ = releaseSemaphore(t, core, namespaceId, "test_semaphore", leases[3].Id, now.Add(time.Minute))

		// List remaining holders
		list := listSemaphoreHolders(t, core, namespaceId, "test_semaphore")

		// Verify remaining holders - we expect 3 holders after releasing 2
		require.Len(t, list.Holders, 3)
	})
}

func TestCore_ListSemaphores(t *testing.T) {
	core := newSemaphoresCore(t)
	now := time.Now()
	namespaceId := &corepb.NamespaceId{
		AccountId:   rand.Uint64(),
		NamespaceId: rand.Uint32(),
	}

	// Create first semaphore
	semaphore1Id := &corepb.SemaphoreId{
		AccountId:   namespaceId.AccountId,
		NamespaceId: namespaceId.NamespaceId,
		SemaphoreId: rand.Uint64(),
	}
	_ = createSemaphore(t, core, semaphore1Id, "test_semaphore_1", 2, now)

	// Create second semaphore
	semaphore2Id := &corepb.SemaphoreId{
		AccountId:   namespaceId.AccountId,
		NamespaceId: namespaceId.NamespaceId,
		SemaphoreId: rand.Uint64(),
	}
	_ = createSemaphore(t, core, semaphore2Id, "test_semaphore_2", 3, now)

	// List semaphores
	resp3, err := core.ListSemaphores(&coreapis.ListSemaphoresRequest{
		Payload: &corepb.ListSemaphoresRequest{
			NamespaceId: namespaceId,
		},
	})

	require.NoError(t, err)
	require.NotNil(t, resp3)
	require.Nil(t, resp3.ApplicationError)
	require.NotNil(t, resp3.Payload)
	require.Len(t, resp3.Payload.Semaphores, 2)
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
	core1 := newSemaphoresCore(t)
	core2 := newSemaphoresCore(t)

	// T+0: Create semaphore
	_ = createSemaphore(t, core1, semaphoreId, "test_semaphore", 2, now)

	// T+1m: Create lease and acquire semaphore
	lease1 := createLease(t, core1, accountId, namespaceId.NamespaceId, "process_1", now.Add(time.Minute), 60*time.Minute)
	success, _ := acquireSemaphore(t, core1, namespaceId, lease1.Id, "test_semaphore", 1, now.Add(time.Minute))
	require.True(t, success)

	// Take snapshot at this point
	snapshot := core1.Snapshot()

	// T+2m: Acquire semaphore with second process (after snapshot)
	lease2 := createLease(t, core1, accountId, namespaceId.NamespaceId, "process_2", now.Add(2*time.Minute), 60*time.Minute)
	success, _ = acquireSemaphore(t, core1, namespaceId, lease2.Id, "test_semaphore", 1, now.Add(2*time.Minute))
	require.True(t, success)

	// T+3m: Release first process (after snapshot)
	_ = releaseSemaphore(t, core1, namespaceId, "test_semaphore", lease1.Id, now.Add(3*time.Minute))

	// Write snapshot to buffer
	buf := bytes.NewBuffer(nil)
	err := snapshot.Write(buf)
	require.NoError(t, err)

	// Restore snapshot to second core
	err = core2.Restore(io.NopCloser(buf))
	require.NoError(t, err)

	// T+4m: Check that the restored state matches the snapshot state
	// The semaphore should exist with one holder (lease1)
	semaphore := getSemaphore(t, core2, semaphoreId, now.Add(4*time.Minute))
	require.EqualValues(t, 1, semaphore.ActiveHoldersCount)
	// require.Equal(t, "process_1", semaphore.SemaphoreHolders[0].ProcessId)

	// T+5m: Try to acquire with a new process in restored state (should succeed)
	lease3 := createLease(t, core2, accountId, namespaceId.NamespaceId, "process_3", now.Add(5*time.Minute), 60*time.Minute)
	success, semaphore = acquireSemaphore(t, core2, namespaceId, lease3.Id, "test_semaphore", 1, now.Add(5*time.Minute))
	require.True(t, success)
	require.EqualValues(t, 2, semaphore.ActiveHoldersCount)

	// T+6m: Try to acquire with a fourth process in restored state (should fail - no more permits)
	lease4 := createLease(t, core2, accountId, namespaceId.NamespaceId, "process_4", now.Add(6*time.Minute), 60*time.Minute)
	success, _ = acquireSemaphore(t, core2, namespaceId, lease4.Id, "test_semaphore", 1, now.Add(6*time.Minute))
	require.False(t, success)

	// T+7m: Release process_1 in restored state
	_ = releaseSemaphore(t, core2, namespaceId, "test_semaphore", lease1.Id, now.Add(7*time.Minute))

	// T+8m: Verify only process_3 remains in restored state
	semaphore = getSemaphore(t, core2, semaphoreId, now.Add(8*time.Minute))
	require.EqualValues(t, 1, semaphore.ActiveHoldersCount)
	// require.Equal(t, "process_3", semaphore.SemaphoreHolders[0].ProcessId)

	// TODO
	// Verify that process_2 from the original core is not in the restored state
	// (it was acquired after the snapshot)
	// for _, holder := range resp9.Payload.Semaphore.SemaphoreHolders {
	// 	require.NotEqual(t, "process_2", holder.ProcessId)
	// }
}

func TestCore_SemaphoresDeleteNamespace(t *testing.T) {
	core := newSemaphoresCore(t)
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

	// Create a semaphore in the namespace
	_ = createSemaphore(t, core, semaphoreId, "test_semaphore", 2, now)

	// Mark the namespace as deleted using SemaphoresDeleteNamespace
	resp2, err := core.SemaphoresDeleteNamespace(&coreapis.SemaphoresDeleteNamespaceRequest{
		Payload: &corepb.SemaphoresDeleteNamespaceRequest{
			RecordId:    rand.Uint64(),
			NamespaceId: namespaceId,
			Now:         now.UnixNano(),
		},
	})

	require.NoError(t, err)
	require.NotNil(t, resp2)
	require.Nil(t, resp2.ApplicationError)
	require.NotNil(t, resp2.Payload)

	// Verify that the namespace is marked as deleted by checking the deleted namespaces list
	txn := core.badgerStore.Update()
	defer txn.Discard()

	deletedNamespaces, err := core.gcRecords.List(txn, 100)
	require.NoError(t, err)
	require.Len(t, deletedNamespaces, 1)
	// require.Equal(t, namespaceIdProto.AccountId, deletedNamespaces[0].NamespaceId.AccountId)
	// require.Equal(t, namespaceIdProto.NamespaceId, deletedNamespaces[0].NamespaceId.NamespaceId)
}

func TestCore_RunSemaphoresGarbageCollection(t *testing.T) {
	t.Run("with deleted namespace", func(t *testing.T) {
		core := newSemaphoresCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespaceId := &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: rand.Uint32(),
		}

		// Create some semaphores in the namespace
		semaphoreIds := make([]*corepb.SemaphoreId, 10)
		for i := range len(semaphoreIds) {
			semaphoreIds[i] = &corepb.SemaphoreId{
				AccountId:   accountId,
				NamespaceId: namespaceId.NamespaceId,
				SemaphoreId: rand.Uint64(),
			}
		}

		// Create semaphores in the namespace
		for i, semaphoreId := range semaphoreIds {
			_ = createSemaphore(t, core, semaphoreId, fmt.Sprintf("semaphore_%d", i), 2, now)

			// Acquire semaphores
			lease := createLease(t, core, accountId, namespaceId.NamespaceId, fmt.Sprintf("process_%d", i), now, 60*time.Minute)
			success, _ := acquireSemaphore(t, core, namespaceId, lease.Id, fmt.Sprintf("semaphore_%d", i), 1, now)
			require.True(t, success)
		}

		// Verify that semaphores in a different namespace are accessible after GC
		differentNamespaceId := &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: rand.Uint32(),
		}
		differentNamespaceSemaphoreId := &corepb.SemaphoreId{
			AccountId:   accountId,
			NamespaceId: differentNamespaceId.NamespaceId,
			SemaphoreId: rand.Uint64(),
		}

		// Create and acquire a semaphore in a different namespace
		_ = createSemaphore(t, core, differentNamespaceSemaphoreId, "different_semaphore", 1, now)

		leaseDifferent := createLease(t, core, accountId, differentNamespaceSemaphoreId.NamespaceId, "process_different", now, 60*time.Minute)
		success, _ := acquireSemaphore(t, core, differentNamespaceId, leaseDifferent.Id, "different_semaphore", 1, now)
		require.True(t, success)

		// Verify semaphores exist by getting them
		for _, semaphoreId := range semaphoreIds {
			semaphore := getSemaphore(t, core, semaphoreId, now)
			require.EqualValues(t, 1, semaphore.ActiveHoldersCount)
		}

		// Mark the namespace as deleted using SemaphoresDeleteNamespace
		resp6, err := core.SemaphoresDeleteNamespace(&coreapis.SemaphoresDeleteNamespaceRequest{
			Payload: &corepb.SemaphoresDeleteNamespaceRequest{
				RecordId:    rand.Uint64(),
				NamespaceId: namespaceId,
				Now:         now.UnixNano(),
			},
		})

		require.NoError(t, err)
		require.NotNil(t, resp6)
		require.Nil(t, resp6.ApplicationError)
		require.NotNil(t, resp6.Payload)

		// Run garbage collection to clean up the deleted namespace
		resp7, err := core.RunSemaphoresGarbageCollection(&coreapis.RunSemaphoresGarbageCollectionRequest{
			Payload: &corepb.RunSemaphoresGarbageCollectionRequest{
				Now:                        now.UnixNano(),
				GcRecordsPageSize:          100,
				GcRecordSemaphoresPageSize: 100,
				MaxVisitedSemaphores:       1000,
			},
		})

		require.NoError(t, err)
		require.NotNil(t, resp7)
		require.Nil(t, resp7.ApplicationError)
		require.NotNil(t, resp7.Payload)

		// Verify that semaphores in the deleted namespace are no longer accessible
		for _, semaphoreId := range semaphoreIds {
			appErr := getSemaphoreWithError(t, core, semaphoreId, now)
			require.Equal(t, monsterax.NotFound, appErr.Code)
		}

		// Verify the different namespace semaphore still exists after GC
		semaphore := getSemaphore(t, core, differentNamespaceSemaphoreId, now)
		require.EqualValues(t, 1, semaphore.ActiveHoldersCount)
	})

	t.Run("with multiple expiring semaphores", func(t *testing.T) {
		core := newSemaphoresCore(t)
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
			_ = createSemaphore(t, core, semaphoreId, fmt.Sprintf("semaphore_%d", i), 2, now)

			if i < 5 {
				// Semaphores 0-4: All holders will expire
				lease1 := createLease(t, core, accountId, namespaceId.NamespaceId, fmt.Sprintf("process_%d", i), now, 30*time.Minute)
				success, _ := acquireSemaphore(t, core, namespaceId, lease1.Id, fmt.Sprintf("semaphore_%d", i), 1, now)
				require.True(t, success)

				// Add a second holder that will also expire
				lease2 := createLease(t, core, accountId, namespaceId.NamespaceId, fmt.Sprintf("process_%d_second", i), now, 30*time.Minute)
				success, _ = acquireSemaphore(t, core, namespaceId, lease2.Id, fmt.Sprintf("semaphore_%d", i), 1, now)
				require.True(t, success)
			} else if i < 10 {
				// Semaphores 5-9: Some holders will expire, some will remain
				lease1 := createLease(t, core, accountId, namespaceId.NamespaceId, fmt.Sprintf("process_%d", i), now, 30*time.Minute)
				success, _ := acquireSemaphore(t, core, namespaceId, lease1.Id, fmt.Sprintf("semaphore_%d", i), 1, now)
				require.True(t, success)

				// Add a second holder that will remain
				lease2 := createLease(t, core, accountId, namespaceId.NamespaceId, fmt.Sprintf("process_%d_second", i), now, 2*time.Hour)
				success, _ = acquireSemaphore(t, core, namespaceId, lease2.Id, fmt.Sprintf("semaphore_%d", i), 1, now)
				require.True(t, success)
			} else {
				// Semaphores 10-14: All holders will remain
				lease1 := createLease(t, core, accountId, namespaceId.NamespaceId, fmt.Sprintf("process_%d", i), now, 2*time.Hour)
				success, _ := acquireSemaphore(t, core, namespaceId, lease1.Id, fmt.Sprintf("semaphore_%d", i), 1, now)
				require.True(t, success)

				// Add a second holder that will also remain
				lease2 := createLease(t, core, accountId, namespaceId.NamespaceId, fmt.Sprintf("process_%d_second", i), now, 3*time.Hour)
				success, _ = acquireSemaphore(t, core, namespaceId, lease2.Id, fmt.Sprintf("semaphore_%d", i), 1, now)
				require.True(t, success)
			}
		}

		// Verify all semaphores exist and have holders before garbage collection
		for _, semaphoreId := range semaphoreIds {
			semaphore := getSemaphore(t, core, semaphoreId, now)
			require.EqualValues(t, 2, semaphore.ActiveHoldersCount)
			require.EqualValues(t, 2, semaphore.ActiveHolds)
		}

		// Run garbage collection at the moment when some semaphores expire (T+31 minutes)
		gcTime := now.Add(31 * time.Minute)
		resp9, err := core.RunSemaphoresGarbageCollection(&coreapis.RunSemaphoresGarbageCollectionRequest{
			Payload: &corepb.RunSemaphoresGarbageCollectionRequest{
				Now:                        gcTime.UnixNano(),
				GcRecordsPageSize:          100,
				GcRecordSemaphoresPageSize: 100,
				MaxVisitedSemaphores:       maxVisitedSemaphores,
			},
		})

		require.NoError(t, err)
		require.NotNil(t, resp9)
		require.Nil(t, resp9.ApplicationError)
		require.NotNil(t, resp9.Payload)

		// Verify the state of semaphores after garbage collection
		// Note: We use the public GetSemaphore method which internally calls checkSemaphoreExpiration
		// to verify the true state of the semaphores after garbage collection

		// Semaphores 0-4 should have no holders (all expired)
		for i := range 5 {
			semaphore := getSemaphore(t, core, semaphoreIds[i], gcTime)
			require.EqualValues(t, 0, semaphore.ActiveHoldersCount)
		}

		// Semaphores 5-9 should still have one holder remaining
		for i := 5; i < 10; i++ {
			semaphore := getSemaphore(t, core, semaphoreIds[i], gcTime)
			require.EqualValues(t, 1, int(semaphore.ActiveHoldersCount))
			// require.Equal(t, fmt.Sprintf("process_%d_second", i), semaphore.SemaphoreHolders[0].ProcessId)
		}

		// Semaphores 10-14 should still have both holders
		for i := 10; i < numSemaphores; i++ {
			semaphore := getSemaphore(t, core, semaphoreIds[i], gcTime)
			require.EqualValues(t, 2, int(semaphore.ActiveHoldersCount))
			// holderProcessIds := make([]string, len(semaphore.SemaphoreHolders))
			// for j, holder := range semaphore.SemaphoreHolders {
			// 	holderProcessIds[j] = holder.ProcessId
			// }
			// require.Contains(t, holderProcessIds, fmt.Sprintf("process_%d", i))
			// require.Contains(t, holderProcessIds, fmt.Sprintf("process_%d_second", i))
		}

		// Run garbage collection again to process the remaining semaphores
		// This should process semaphores 5-14 since semaphores 0-4 were already processed
		resp13, err := core.RunSemaphoresGarbageCollection(&coreapis.RunSemaphoresGarbageCollectionRequest{
			Payload: &corepb.RunSemaphoresGarbageCollectionRequest{
				Now:                        gcTime.UnixNano(),
				GcRecordsPageSize:          100,
				GcRecordSemaphoresPageSize: 100,
				MaxVisitedSemaphores:       maxVisitedSemaphores,
			},
		})

		require.NoError(t, err)
		require.NotNil(t, resp13)
		require.Nil(t, resp13.ApplicationError)
		require.NotNil(t, resp13.Payload)

		// Verify that semaphores 5-9 still have their remaining holders
		for i := 5; i < 10; i++ {
			semaphore := getSemaphore(t, core, semaphoreIds[i], gcTime)
			require.EqualValues(t, 1, int(semaphore.ActiveHoldersCount))
		}

		// Verify that semaphores 10-14 still have all their holders
		for i := 10; i < numSemaphores; i++ {
			semaphore := getSemaphore(t, core, semaphoreIds[i], gcTime)
			require.EqualValues(t, 2, int(semaphore.ActiveHoldersCount))
		}
	})

	t.Run("stale expiration records", func(t *testing.T) {
		core := newSemaphoresCore(t)
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
		_ = createSemaphore(t, core, semaphoreId, semaphoreName, 10, now)

		// T+0: Acquire semaphore with process_1 expiring at T+1h
		lease1 := createLease(t, core, accountId, namespaceId.NamespaceId, "process_1", now, 1*time.Hour)
		success, _ := acquireSemaphore(t, core, namespaceId, lease1.Id, semaphoreName, 1, now)
		require.True(t, success)

		// T+0: Acquire semaphore with process_2 expiring at T+2h
		lease2 := createLease(t, core, accountId, namespaceId.NamespaceId, "process_2", now, 2*time.Hour)
		success, _ = acquireSemaphore(t, core, namespaceId, lease2.Id, semaphoreName, 1, now)
		require.True(t, success)

		// T+0: Acquire semaphore with process_3 expiring at T+2h
		lease3 := createLease(t, core, accountId, namespaceId.NamespaceId, "process_3", now, 2*time.Hour)

		// T+30m: Release process_1
		// This changes the earliest expiration from T+1h to T+2h
		_ = releaseSemaphore(t, core, namespaceId, semaphoreName, lease1.Id, now.Add(30*time.Minute))

		// T+45m: Acquire process_3, expiring at T+2h (same as process_2)
		// Now both holders expire at the same time
		success, _ = acquireSemaphore(t, core, namespaceId, lease3.Id, semaphoreName, 1, now.Add(45*time.Minute))
		require.True(t, success)

		// T+1h: Run garbage collection
		resp6, err := core.RunSemaphoresGarbageCollection(&coreapis.RunSemaphoresGarbageCollectionRequest{
			Payload: &corepb.RunSemaphoresGarbageCollectionRequest{
				Now:                        now.Add(1 * time.Hour).UnixNano(),
				GcRecordsPageSize:          100,
				GcRecordSemaphoresPageSize: 100,
				MaxVisitedSemaphores:       100,
			},
		})
		require.NoError(t, err)
		require.NotNil(t, resp6)
		require.Nil(t, resp6.ApplicationError)
		require.NotNil(t, resp6.Payload)

		// Run GC again to ensure idempotency
		// The second run might encounter expiration records that are already correct
		resp7, err := core.RunSemaphoresGarbageCollection(&coreapis.RunSemaphoresGarbageCollectionRequest{
			Payload: &corepb.RunSemaphoresGarbageCollectionRequest{
				Now:                        now.Add(1*time.Hour + 5*time.Minute).UnixNano(),
				GcRecordsPageSize:          100,
				GcRecordSemaphoresPageSize: 100,
				MaxVisitedSemaphores:       100,
			},
		})
		require.NoError(t, err)
		require.NotNil(t, resp7)
		require.Nil(t, resp7.ApplicationError)
		require.NotNil(t, resp7.Payload)

		// Verify semaphore still exists with both holders
		semaphore := getSemaphore(t, core, semaphoreId, now.Add(1*time.Hour+5*time.Minute))
		require.EqualValues(t, 2, semaphore.ActiveHoldersCount)
		require.EqualValues(t, 2, semaphore.ActiveHolds)
	})
}

func TestCore_RevokeSemaphoreLease(t *testing.T) {
	t.Run("revokes all semaphores with pagination", func(t *testing.T) {
		core := newSemaphoresCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespaceId := &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: rand.Uint32(),
		}

		// Create a lease
		lease := createLease(t, core, accountId, namespaceId.NamespaceId, "process-1", now, 60*time.Minute)

		// Acquire 1500 semaphores to test pagination
		numSemaphores := 1500
		semaphoreIds := make([]*corepb.SemaphoreId, numSemaphores)
		for i := range numSemaphores {
			semaphoreIds[i] = &corepb.SemaphoreId{
				AccountId:   accountId,
				NamespaceId: namespaceId.NamespaceId,
				SemaphoreId: rand.Uint64(),
			}

			// Create the semaphore
			_ = createSemaphore(t, core, semaphoreIds[i], fmt.Sprintf("test_semaphore_%d", i), 10, now)

			// Acquire the semaphore
			success, semaphore := acquireSemaphore(t, core, namespaceId, lease.Id, fmt.Sprintf("test_semaphore_%d", i), 1, now)
			require.True(t, success)
			require.EqualValues(t, 1, semaphore.ActiveHoldersCount)
		}

		// Verify that counters show the correct number of semaphores and leases
		counters, err := core.counters.Get(core.badgerStore.View(), accountId, namespaceId.NamespaceId)
		require.NoError(t, err)
		require.EqualValues(t, numSemaphores, counters.NumberOfSemaphores)
		require.EqualValues(t, 1, counters.NumberOfLeases)

		// Revoke the lease
		resp3, err := core.RevokeSemaphoreLease(&coreapis.RevokeSemaphoreLeaseRequest{
			Payload: &corepb.RevokeSemaphoreLeaseRequest{
				LeaseId: lease.Id,
				Now:     now.UnixNano(),
			},
		})
		require.NoError(t, err)
		require.NotNil(t, resp3)
		require.Nil(t, resp3.ApplicationError)
		require.NotNil(t, resp3.Payload)

		// Verify that all semaphores are released
		for i := range numSemaphores {
			semaphore := getSemaphoreByName(t, core, namespaceId, fmt.Sprintf("test_semaphore_%d", i), now)
			require.EqualValues(t, 0, semaphore.ActiveHoldersCount)
			require.EqualValues(t, 0, semaphore.ActiveHolds)
		}

		// Verify that counters are updated correctly
		counters, err = core.counters.Get(core.badgerStore.View(), accountId, namespaceId.NamespaceId)
		require.NoError(t, err)
		require.EqualValues(t, numSemaphores, counters.NumberOfSemaphores) // Semaphores still exist
		require.EqualValues(t, 0, counters.NumberOfLeases)

		// Verify the lease is deleted
		resp5, err := core.GetSemaphoreLease(&coreapis.GetSemaphoreLeaseRequest{
			Payload: &corepb.GetSemaphoreLeaseRequest{
				LeaseId: lease.Id,
			},
		})
		require.NoError(t, err)
		require.NotNil(t, resp5)
		require.Nil(t, resp5.Payload)
		require.NotNil(t, resp5.ApplicationError)
		require.Equal(t, monsterax.NotFound, resp5.ApplicationError.Code)
	})

	t.Run("releases multiple holders", func(t *testing.T) {
		core := newSemaphoresCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespaceId := &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: rand.Uint32(),
		}

		// Create two leases
		lease1 := createLease(t, core, accountId, namespaceId.NamespaceId, "process-1", now, 60*time.Minute)
		lease2 := createLease(t, core, accountId, namespaceId.NamespaceId, "process-2", now, 60*time.Minute)

		semaphoreId := &corepb.SemaphoreId{
			AccountId:   accountId,
			NamespaceId: namespaceId.NamespaceId,
			SemaphoreId: rand.Uint64(),
		}

		// Create the semaphore with 10 permits
		_ = createSemaphore(t, core, semaphoreId, "shared_semaphore", 10, now)

		// Acquire semaphore with lease1 (5 permits)
		success, _ := acquireSemaphore(t, core, namespaceId, lease1.Id, "shared_semaphore", 5, now)
		require.True(t, success)

		// Acquire semaphore with lease2 (3 permits)
		success, semaphore := acquireSemaphore(t, core, namespaceId, lease2.Id, "shared_semaphore", 3, now)
		require.True(t, success)
		require.EqualValues(t, 2, semaphore.ActiveHoldersCount)
		require.EqualValues(t, 8, semaphore.ActiveHolds)

		// Revoke lease1
		resp4, err := core.RevokeSemaphoreLease(&coreapis.RevokeSemaphoreLeaseRequest{
			Payload: &corepb.RevokeSemaphoreLeaseRequest{
				LeaseId: lease1.Id,
				Now:     now.UnixNano(),
			},
		})
		require.NoError(t, err)
		require.NotNil(t, resp4)
		require.Nil(t, resp4.ApplicationError)
		require.NotNil(t, resp4.Payload)

		// Verify that the semaphore is still held by lease2
		semaphore = getSemaphoreByName(t, core, namespaceId, "shared_semaphore", now)
		require.EqualValues(t, 1, semaphore.ActiveHoldersCount)
		require.EqualValues(t, 3, semaphore.ActiveHolds)

		// Revoke lease2
		resp6, err := core.RevokeSemaphoreLease(&coreapis.RevokeSemaphoreLeaseRequest{
			Payload: &corepb.RevokeSemaphoreLeaseRequest{
				LeaseId: lease2.Id,
				Now:     now.UnixNano(),
			},
		})
		require.NoError(t, err)
		require.NotNil(t, resp6)
		require.Nil(t, resp6.ApplicationError)
		require.NotNil(t, resp6.Payload)

		// Verify that the semaphore is now released
		semaphore = getSemaphoreByName(t, core, namespaceId, "shared_semaphore", now)
		require.EqualValues(t, 0, semaphore.ActiveHoldersCount)
		require.EqualValues(t, 0, semaphore.ActiveHolds)
	})
}

func TestCore_RefreshSemaphoreLease(t *testing.T) {
	t.Run("expired lease", func(t *testing.T) {
		core := newSemaphoresCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespaceId := &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: rand.Uint32(),
		}

		// Create a lease with 1 minute TTL
		lease := createLease(t, core, accountId, namespaceId.NamespaceId, "process-1", now, 1*time.Minute)

		// Create and acquire multiple semaphores
		numSemaphores := 10
		for i := range numSemaphores {
			semaphoreId := &corepb.SemaphoreId{
				AccountId:   accountId,
				NamespaceId: namespaceId.NamespaceId,
				SemaphoreId: rand.Uint64(),
			}
			semaphoreName := fmt.Sprintf("test_semaphore_%d", i)

			// Create the semaphore
			_ = createSemaphore(t, core, semaphoreId, semaphoreName, 10, now)

			// Acquire the semaphore
			success, _ := acquireSemaphore(t, core, namespaceId, lease.Id, semaphoreName, 2, now)
			require.True(t, success)
		}

		// Verify counters before refresh
		counters, err := core.counters.Get(core.badgerStore.View(), accountId, namespaceId.NamespaceId)
		require.NoError(t, err)
		require.EqualValues(t, numSemaphores, counters.NumberOfSemaphores)
		require.EqualValues(t, 1, counters.NumberOfLeases)

		// Try to refresh the lease after it has expired (2 minutes later)
		futureTime := now.Add(2 * time.Minute)
		resp3, err := core.RefreshSemaphoreLease(&coreapis.RefreshSemaphoreLeaseRequest{
			Payload: &corepb.RefreshSemaphoreLeaseRequest{
				LeaseId:    lease.Id,
				TtlSeconds: 60,
				Now:        futureTime.UnixNano(),
			},
		})
		require.NoError(t, err)
		require.NotNil(t, resp3)
		require.Nil(t, resp3.Payload)
		require.NotNil(t, resp3.ApplicationError)
		require.Equal(t, monsterax.NotFound, resp3.ApplicationError.Code)

		// Verify that all semaphores are released
		for i := range numSemaphores {
			semaphore := getSemaphoreByName(t, core, namespaceId, fmt.Sprintf("test_semaphore_%d", i), futureTime)
			require.EqualValues(t, 0, semaphore.ActiveHoldersCount)
			require.EqualValues(t, 0, semaphore.ActiveHolds)
		}

		// Verify that counters are updated correctly (semaphores still exist, but lease is gone)
		counters, err = core.counters.Get(core.badgerStore.View(), accountId, namespaceId.NamespaceId)
		require.NoError(t, err)
		require.EqualValues(t, numSemaphores, counters.NumberOfSemaphores)
		require.EqualValues(t, 0, counters.NumberOfLeases)

		// Verify the lease is deleted
		resp5, err := core.GetSemaphoreLease(&coreapis.GetSemaphoreLeaseRequest{
			Payload: &corepb.GetSemaphoreLeaseRequest{
				LeaseId: lease.Id,
			},
		})
		require.NoError(t, err)
		require.NotNil(t, resp5)
		require.Nil(t, resp5.Payload)
		require.NotNil(t, resp5.ApplicationError)
		require.Equal(t, monsterax.NotFound, resp5.ApplicationError.Code)
	})

	t.Run("valid lease", func(t *testing.T) {
		core := newSemaphoresCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		// Create a lease with 1 minute TTL
		lease := createLease(t, core, accountId, namespaceId, "process-1", now, 1*time.Minute)
		originalExpiresAt := lease.ExpiresAt

		// Refresh the lease before it expires (30 seconds later)
		futureTime := now.Add(30 * time.Second)
		resp1, err := core.RefreshSemaphoreLease(&coreapis.RefreshSemaphoreLeaseRequest{
			Payload: &corepb.RefreshSemaphoreLeaseRequest{
				LeaseId:    lease.Id,
				TtlSeconds: 120, // 2 minutes
				Now:        futureTime.UnixNano(),
			},
		})
		require.NoError(t, err)
		require.NotNil(t, resp1)
		require.Nil(t, resp1.ApplicationError)
		require.NotNil(t, resp1.Payload)
		require.NotNil(t, resp1.Payload.Lease)

		// Verify the lease expiration was updated
		require.Greater(t, resp1.Payload.Lease.ExpiresAt, originalExpiresAt)
		expectedExpiresAt := futureTime.UnixNano() + int64(120*time.Second)
		require.Equal(t, expectedExpiresAt, resp1.Payload.Lease.ExpiresAt)

		// Verify the lease still exists and can be retrieved
		resp2, err := core.GetSemaphoreLease(&coreapis.GetSemaphoreLeaseRequest{
			Payload: &corepb.GetSemaphoreLeaseRequest{
				LeaseId: lease.Id,
			},
		})
		require.NoError(t, err)
		require.NotNil(t, resp2)
		require.Nil(t, resp2.ApplicationError)
		require.NotNil(t, resp2.Payload)
		require.Equal(t, resp1.Payload.Lease.ExpiresAt, resp2.Payload.Lease.ExpiresAt)
	})
}

func TestCore_ListSemaphoreLeases(t *testing.T) {
	t.Run("lists multiple leases", func(t *testing.T) {
		core := newSemaphoresCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		// Create multiple leases
		lease1 := createLease(t, core, accountId, namespaceId, "process-1", now, 60*time.Minute)
		lease2 := createLease(t, core, accountId, namespaceId, "process-2", now, 60*time.Minute)
		lease3 := createLease(t, core, accountId, namespaceId, "process-3", now, 60*time.Minute)

		// List leases
		resp1, err := core.ListSemaphoreLeases(&coreapis.ListSemaphoreLeasesRequest{
			Payload: &corepb.ListSemaphoreLeasesRequest{
				NamespaceId: &corepb.NamespaceId{
					AccountId:   accountId,
					NamespaceId: namespaceId,
				},
				Now: now.UnixNano(),
			},
		})
		require.NoError(t, err)
		require.NotNil(t, resp1)
		require.Nil(t, resp1.ApplicationError)
		require.NotNil(t, resp1.Payload)
		require.Len(t, resp1.Payload.Leases, 3)

		// Verify lease IDs
		leaseIds := make(map[uint64]bool)
		for _, lease := range resp1.Payload.Leases {
			leaseIds[lease.Id.LeaseId] = true
		}
		require.True(t, leaseIds[lease1.Id.LeaseId])
		require.True(t, leaseIds[lease2.Id.LeaseId])
		require.True(t, leaseIds[lease3.Id.LeaseId])
	})

	t.Run("filters out expired leases", func(t *testing.T) {
		core := newSemaphoresCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		// Create leases with different TTLs
		lease1 := createLease(t, core, accountId, namespaceId, "process-1", now, 1*time.Minute)
		lease2 := createLease(t, core, accountId, namespaceId, "process-2", now, 5*time.Minute)
		lease3 := createLease(t, core, accountId, namespaceId, "process-3", now, 10*time.Minute)

		// List leases 2 minutes later (lease1 expired, lease2 and lease3 still valid)
		futureTime := now.Add(2 * time.Minute)
		resp1, err := core.ListSemaphoreLeases(&coreapis.ListSemaphoreLeasesRequest{
			Payload: &corepb.ListSemaphoreLeasesRequest{
				NamespaceId: &corepb.NamespaceId{
					AccountId:   accountId,
					NamespaceId: namespaceId,
				},
				Now: futureTime.UnixNano(),
			},
		})
		require.NoError(t, err)
		require.NotNil(t, resp1)
		require.Nil(t, resp1.ApplicationError)
		require.NotNil(t, resp1.Payload)
		require.Len(t, resp1.Payload.Leases, 2)

		// Verify only non-expired leases are returned
		leaseIds := make(map[uint64]bool)
		for _, lease := range resp1.Payload.Leases {
			leaseIds[lease.Id.LeaseId] = true
		}
		require.False(t, leaseIds[lease1.Id.LeaseId])
		require.True(t, leaseIds[lease2.Id.LeaseId])
		require.True(t, leaseIds[lease3.Id.LeaseId])
	})

	t.Run("returns empty list for namespace with no leases", func(t *testing.T) {
		core := newSemaphoresCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		resp1, err := core.ListSemaphoreLeases(&coreapis.ListSemaphoreLeasesRequest{
			Payload: &corepb.ListSemaphoreLeasesRequest{
				NamespaceId: &corepb.NamespaceId{
					AccountId:   accountId,
					NamespaceId: namespaceId,
				},
				Now: now.UnixNano(),
			},
		})
		require.NoError(t, err)
		require.NotNil(t, resp1)
		require.Nil(t, resp1.ApplicationError)
		require.NotNil(t, resp1.Payload)
		require.Len(t, resp1.Payload.Leases, 0)
	})
}

func TestCore_ListSemaphoreLeasesByProcessId(t *testing.T) {
	t.Run("lists leases for specific process", func(t *testing.T) {
		core := newSemaphoresCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		// Create leases for different processes
		lease1 := createLease(t, core, accountId, namespaceId, "process-1", now, 60*time.Minute)
		lease2 := createLease(t, core, accountId, namespaceId, "process-1", now, 60*time.Minute)
		lease3 := createLease(t, core, accountId, namespaceId, "process-2", now, 60*time.Minute)

		// List leases for process-1
		resp1, err := core.ListSemaphoreLeasesByProcessId(&coreapis.ListSemaphoreLeasesByProcessIdRequest{
			Payload: &corepb.ListSemaphoreLeasesByProcessIdRequest{
				NamespaceId: &corepb.NamespaceId{
					AccountId:   accountId,
					NamespaceId: namespaceId,
				},
				ProcessId: "process-1",
				Now:       now.UnixNano(),
			},
		})
		require.NoError(t, err)
		require.NotNil(t, resp1)
		require.Nil(t, resp1.ApplicationError)
		require.NotNil(t, resp1.Payload)
		require.Len(t, resp1.Payload.Leases, 2)

		// Verify lease IDs
		leaseIds := make(map[uint64]bool)
		for _, lease := range resp1.Payload.Leases {
			leaseIds[lease.Id.LeaseId] = true
			require.Equal(t, "process-1", lease.ProcessId)
		}
		require.True(t, leaseIds[lease1.Id.LeaseId])
		require.True(t, leaseIds[lease2.Id.LeaseId])
		require.False(t, leaseIds[lease3.Id.LeaseId])
	})

	t.Run("filters out expired leases", func(t *testing.T) {
		core := newSemaphoresCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		// Create leases for the same process with different TTLs
		_ = createLease(t, core, accountId, namespaceId, "process-1", now, 1*time.Minute) // Will expire
		lease2 := createLease(t, core, accountId, namespaceId, "process-1", now, 5*time.Minute)

		// List leases 2 minutes later (lease1 expired, lease2 still valid)
		futureTime := now.Add(2 * time.Minute)
		resp1, err := core.ListSemaphoreLeasesByProcessId(&coreapis.ListSemaphoreLeasesByProcessIdRequest{
			Payload: &corepb.ListSemaphoreLeasesByProcessIdRequest{
				NamespaceId: &corepb.NamespaceId{
					AccountId:   accountId,
					NamespaceId: namespaceId,
				},
				ProcessId: "process-1",
				Now:       futureTime.UnixNano(),
			},
		})
		require.NoError(t, err)
		require.NotNil(t, resp1)
		require.Nil(t, resp1.ApplicationError)
		require.NotNil(t, resp1.Payload)
		require.Len(t, resp1.Payload.Leases, 1)
		require.Equal(t, lease2.Id.LeaseId, resp1.Payload.Leases[0].Id.LeaseId)
	})

	t.Run("returns empty list for process with no leases", func(t *testing.T) {
		core := newSemaphoresCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		// Create lease for a different process
		createLease(t, core, accountId, namespaceId, "process-1", now, 60*time.Minute)

		resp1, err := core.ListSemaphoreLeasesByProcessId(&coreapis.ListSemaphoreLeasesByProcessIdRequest{
			Payload: &corepb.ListSemaphoreLeasesByProcessIdRequest{
				NamespaceId: &corepb.NamespaceId{
					AccountId:   accountId,
					NamespaceId: namespaceId,
				},
				ProcessId: "process-2",
				Now:       now.UnixNano(),
			},
		})
		require.NoError(t, err)
		require.NotNil(t, resp1)
		require.Nil(t, resp1.ApplicationError)
		require.NotNil(t, resp1.Payload)
		require.Len(t, resp1.Payload.Leases, 0)
	})
}

func TestCore_ListSemaphoresByLeaseId(t *testing.T) {
	t.Run("lists semaphores held by lease", func(t *testing.T) {
		core := newSemaphoresCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespaceId := &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: rand.Uint32(),
		}

		// Create leases
		lease1 := createLease(t, core, accountId, namespaceId.NamespaceId, "process-1", now, 60*time.Minute)
		lease2 := createLease(t, core, accountId, namespaceId.NamespaceId, "process-2", now, 60*time.Minute)

		// Create semaphores
		var semaphoreIds []*corepb.SemaphoreId
		for i := 0; i < 5; i++ {
			semaphoreId := &corepb.SemaphoreId{
				AccountId:   accountId,
				NamespaceId: namespaceId.NamespaceId,
				SemaphoreId: rand.Uint64(),
			}
			semaphoreIds = append(semaphoreIds, semaphoreId)

			_ = createSemaphore(t, core, semaphoreId, fmt.Sprintf("test_semaphore_%d", i), 10, now)
		}

		// Acquire first 3 semaphores with lease1
		for i := 0; i < 3; i++ {
			success, _ := acquireSemaphore(t, core, namespaceId, lease1.Id, fmt.Sprintf("test_semaphore_%d", i), 1, now)
			require.True(t, success)
		}

		// Acquire last 2 semaphores with lease2
		for i := 3; i < 5; i++ {
			success, _ := acquireSemaphore(t, core, namespaceId, lease2.Id, fmt.Sprintf("test_semaphore_%d", i), 1, now)
			require.True(t, success)
		}

		// List semaphores for lease1
		resp4, err := core.ListSemaphoresByLeaseId(&coreapis.ListSemaphoresByLeaseIdRequest{
			Payload: &corepb.ListSemaphoresByLeaseIdRequest{
				LeaseId: lease1.Id,
			},
		})
		require.NoError(t, err)
		require.NotNil(t, resp4)
		require.Nil(t, resp4.ApplicationError)
		require.NotNil(t, resp4.Payload)
		require.Len(t, resp4.Payload.Semaphores, 3)

		// Verify semaphore IDs
		semIds := make(map[uint64]bool)
		for _, sem := range resp4.Payload.Semaphores {
			semIds[sem.Id.SemaphoreId] = true
		}
		require.True(t, semIds[semaphoreIds[0].SemaphoreId])
		require.True(t, semIds[semaphoreIds[1].SemaphoreId])
		require.True(t, semIds[semaphoreIds[2].SemaphoreId])
		require.False(t, semIds[semaphoreIds[3].SemaphoreId])
		require.False(t, semIds[semaphoreIds[4].SemaphoreId])
	})

	t.Run("returns empty list for lease with no semaphores", func(t *testing.T) {
		core := newSemaphoresCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		// Create lease without acquiring any semaphores
		lease := createLease(t, core, accountId, namespaceId, "process-1", now, 60*time.Minute)

		resp1, err := core.ListSemaphoresByLeaseId(&coreapis.ListSemaphoresByLeaseIdRequest{
			Payload: &corepb.ListSemaphoresByLeaseIdRequest{
				LeaseId: lease.Id,
			},
		})
		require.NoError(t, err)
		require.NotNil(t, resp1)
		require.Nil(t, resp1.ApplicationError)
		require.NotNil(t, resp1.Payload)
		require.Len(t, resp1.Payload.Semaphores, 0)
	})

	t.Run("returns semaphores after other lease releases", func(t *testing.T) {
		core := newSemaphoresCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespaceId := &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: rand.Uint32(),
		}

		// Create leases
		lease1 := createLease(t, core, accountId, namespaceId.NamespaceId, "process-1", now, 60*time.Minute)
		lease2 := createLease(t, core, accountId, namespaceId.NamespaceId, "process-2", now, 60*time.Minute)

		// Create semaphore
		semaphoreId := &corepb.SemaphoreId{
			AccountId:   accountId,
			NamespaceId: namespaceId.NamespaceId,
			SemaphoreId: rand.Uint64(),
		}

		_ = createSemaphore(t, core, semaphoreId, "shared_semaphore", 10, now)

		// Acquire with both leases
		success, _ := acquireSemaphore(t, core, namespaceId, lease1.Id, "shared_semaphore", 3, now)
		require.True(t, success)

		success, _ = acquireSemaphore(t, core, namespaceId, lease2.Id, "shared_semaphore", 2, now)
		require.True(t, success)

		// List semaphores for lease1 (should still show the semaphore)
		resp4, err := core.ListSemaphoresByLeaseId(&coreapis.ListSemaphoresByLeaseIdRequest{
			Payload: &corepb.ListSemaphoresByLeaseIdRequest{
				LeaseId: lease1.Id,
			},
		})
		require.NoError(t, err)
		require.NotNil(t, resp4)
		require.Nil(t, resp4.ApplicationError)
		require.NotNil(t, resp4.Payload)
		require.Len(t, resp4.Payload.Semaphores, 1)

		// Release lease2
		_ = releaseSemaphore(t, core, namespaceId, "shared_semaphore", lease2.Id, now)

		// List semaphores for lease1 (should still show the semaphore)
		resp6, err := core.ListSemaphoresByLeaseId(&coreapis.ListSemaphoresByLeaseIdRequest{
			Payload: &corepb.ListSemaphoresByLeaseIdRequest{
				LeaseId: lease1.Id,
			},
		})
		require.NoError(t, err)
		require.NotNil(t, resp6)
		require.Nil(t, resp6.ApplicationError)
		require.NotNil(t, resp6.Payload)
		require.Len(t, resp6.Payload.Semaphores, 1)
		require.Equal(t, semaphoreId.SemaphoreId, resp6.Payload.Semaphores[0].Id.SemaphoreId)
	})
}

func newSemaphoresCore(t *testing.T) *Core {
	t.Helper()

	store, err := store.NewBadgerInMemoryStore()
	require.NoError(t, err)
	return NewCore(store, []byte{0x1d, 0x36, 0x00, 0x00}, []byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})
}

func createLease(t *testing.T, core *Core, accountId uint64, namespaceId uint32, processId string, now time.Time, ttl time.Duration) *corepb.Lease {
	t.Helper()

	leaseId := rand.Uint64()
	resp, err := core.CreateSemaphoreLease(&coreapis.CreateSemaphoreLeaseRequest{
		Payload: &corepb.CreateSemaphoreLeaseRequest{
			LeaseId: &corepb.LeaseId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
				LeaseId:     leaseId,
			},
			ProcessId:                  processId,
			TtlSeconds:                 uint64(ttl.Seconds()),
			Now:                        now.UnixNano(),
			MaxNumberOfSemaphoreLeases: 100,
		},
	})
	require.NoError(t, err)
	require.Nil(t, resp.ApplicationError)
	require.NotNil(t, resp.Payload)
	require.NotNil(t, resp.Payload.Lease)
	require.EqualValues(t, now.UnixNano(), resp.Payload.Lease.CreatedAt)
	require.EqualValues(t, now.Add(ttl).UnixNano(), resp.Payload.Lease.ExpiresAt)

	return resp.Payload.Lease
}

func acquireSemaphore(t *testing.T, core *Core, namespaceId *corepb.NamespaceId, leaseId *corepb.LeaseId, semaphoreName string, weight uint64, now time.Time) (bool, *corepb.Semaphore) {
	t.Helper()

	resp, err := core.AcquireSemaphore(&coreapis.AcquireSemaphoreRequest{
		Payload: &corepb.AcquireSemaphoreRequest{
			NamespaceId:   namespaceId,
			SemaphoreName: semaphoreName,
			Weight:        weight,
			Now:           now.UnixNano(),
			LeaseId:       leaseId.LeaseId,
		},
	})

	require.NoError(t, err)
	require.Nil(t, resp.ApplicationError)
	require.NotNil(t, resp.Payload)
	require.NotNil(t, resp.Payload.Semaphore)

	return resp.Payload.Success, resp.Payload.Semaphore
}

func releaseSemaphore(t *testing.T, core *Core, namespaceId *corepb.NamespaceId, semaphoreName string, leaseId *corepb.LeaseId, now time.Time) *corepb.Semaphore {
	t.Helper()

	resp, err := core.ReleaseSemaphore(&coreapis.ReleaseSemaphoreRequest{
		Payload: &corepb.ReleaseSemaphoreRequest{
			NamespaceId:   namespaceId,
			SemaphoreName: semaphoreName,
			Now:           now.UnixNano(),
			LeaseId:       leaseId.LeaseId,
		},
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Nil(t, resp.ApplicationError)
	require.NotNil(t, resp.Payload)
	require.NotNil(t, resp.Payload.Semaphore)

	return resp.Payload.Semaphore
}

func releaseSemaphoreWithError(t *testing.T, core *Core, namespaceId *corepb.NamespaceId, semaphoreName string, leaseId *corepb.LeaseId, now time.Time) *monsterax.Error {
	t.Helper()

	resp, err := core.ReleaseSemaphore(&coreapis.ReleaseSemaphoreRequest{
		Payload: &corepb.ReleaseSemaphoreRequest{
			NamespaceId:   namespaceId,
			SemaphoreName: semaphoreName,
			Now:           now.UnixNano(),
			LeaseId:       leaseId.LeaseId,
		},
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.NotNil(t, resp.ApplicationError)
	require.Nil(t, resp.Payload)

	return resp.ApplicationError
}

func createSemaphore(t *testing.T, core *Core, semaphoreId *corepb.SemaphoreId, semaphoreName string, permits uint64, now time.Time) *corepb.Semaphore {
	t.Helper()

	resp, err := core.CreateSemaphore(&coreapis.CreateSemaphoreRequest{
		Payload: &corepb.CreateSemaphoreRequest{
			SemaphoreId:                       semaphoreId,
			Name:                              semaphoreName,
			Description:                       "test description",
			Permits:                           permits,
			Now:                               now.UnixNano(),
			MaxNumberOfSemaphoresPerNamespace: 10000,
		},
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Nil(t, resp.ApplicationError)
	require.NotNil(t, resp.Payload)
	require.NotNil(t, resp.Payload.Semaphore)
	require.Equal(t, "test description", resp.Payload.Semaphore.Description)
	require.EqualValues(t, permits, resp.Payload.Semaphore.Permits)
	require.Equal(t, semaphoreName, resp.Payload.Semaphore.Name)
	require.Equal(t, semaphoreId, resp.Payload.Semaphore.Id)
	require.EqualValues(t, now.UnixNano(), resp.Payload.Semaphore.CreatedAt)
	require.EqualValues(t, now.UnixNano(), resp.Payload.Semaphore.UpdatedAt)

	return resp.Payload.Semaphore
}

func createSemaphoreWithError(t *testing.T, core *Core, semaphoreId *corepb.SemaphoreId, semaphoreName string, permits uint64, maxNumberOfSemaphoresPerNamespace int64, now time.Time) *monsterax.Error {
	t.Helper()

	resp, err := core.CreateSemaphore(&coreapis.CreateSemaphoreRequest{
		Payload: &corepb.CreateSemaphoreRequest{
			SemaphoreId:                       semaphoreId,
			Name:                              semaphoreName,
			Description:                       "test description",
			Permits:                           permits,
			Now:                               now.UnixNano(),
			MaxNumberOfSemaphoresPerNamespace: maxNumberOfSemaphoresPerNamespace,
		},
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.NotNil(t, resp.ApplicationError)
	require.Nil(t, resp.Payload)

	return resp.ApplicationError
}

func getSemaphore(t *testing.T, core *Core, semaphoreId *corepb.SemaphoreId, now time.Time) *corepb.Semaphore {
	t.Helper()

	resp, err := core.GetSemaphore(&coreapis.GetSemaphoreRequest{
		Payload: &corepb.GetSemaphoreRequest{
			SemaphoreId: semaphoreId,
			Now:         now.UnixNano(),
		},
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Nil(t, resp.ApplicationError)
	require.NotNil(t, resp.Payload)
	require.NotNil(t, resp.Payload.Semaphore)

	return resp.Payload.Semaphore
}

func getSemaphoreWithError(t *testing.T, core *Core, semaphoreId *corepb.SemaphoreId, now time.Time) *monsterax.Error {
	t.Helper()

	resp, err := core.GetSemaphore(&coreapis.GetSemaphoreRequest{
		Payload: &corepb.GetSemaphoreRequest{
			SemaphoreId: semaphoreId,
			Now:         now.UnixNano(),
		},
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.NotNil(t, resp.ApplicationError)
	require.Nil(t, resp.Payload)

	return resp.ApplicationError
}

func getSemaphoreByName(t *testing.T, core *Core, namespaceId *corepb.NamespaceId, semaphoreName string, now time.Time) *corepb.Semaphore {
	t.Helper()

	resp, err := core.GetSemaphoreByName(&coreapis.GetSemaphoreByNameRequest{
		Payload: &corepb.GetSemaphoreByNameRequest{
			NamespaceId:   namespaceId,
			SemaphoreName: semaphoreName,
			Now:           now.UnixNano(),
		},
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Nil(t, resp.ApplicationError)
	require.NotNil(t, resp.Payload)
	require.NotNil(t, resp.Payload.Semaphore)

	return resp.Payload.Semaphore
}

func getSemaphoreByNameWithError(t *testing.T, core *Core, namespaceId *corepb.NamespaceId, semaphoreName string, now time.Time) *monsterax.Error {
	t.Helper()

	resp, err := core.GetSemaphoreByName(&coreapis.GetSemaphoreByNameRequest{
		Payload: &corepb.GetSemaphoreByNameRequest{
			NamespaceId:   namespaceId,
			SemaphoreName: semaphoreName,
			Now:           now.UnixNano(),
		},
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.NotNil(t, resp.ApplicationError)
	require.Nil(t, resp.Payload)

	return resp.ApplicationError
}

func acquireSemaphoreWithError(t *testing.T, core *Core, namespaceId *corepb.NamespaceId, leaseId *corepb.LeaseId, semaphoreName string, weight uint64, now time.Time) *monsterax.Error {
	t.Helper()

	resp, err := core.AcquireSemaphore(&coreapis.AcquireSemaphoreRequest{
		Payload: &corepb.AcquireSemaphoreRequest{
			NamespaceId:   namespaceId,
			SemaphoreName: semaphoreName,
			Weight:        weight,
			Now:           now.UnixNano(),
			LeaseId:       leaseId.LeaseId,
		},
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Nil(t, resp.Payload)
	require.NotNil(t, resp.ApplicationError)

	return resp.ApplicationError
}

func listSemaphoreHolders(t *testing.T, core *Core, namespaceId *corepb.NamespaceId, semaphoreName string) *corepb.ListSemaphoreHoldersResponse {
	t.Helper()

	resp, err := core.ListSemaphoreHolders(&coreapis.ListSemaphoreHoldersRequest{
		Payload: &corepb.ListSemaphoreHoldersRequest{
			NamespaceId:   namespaceId,
			SemaphoreName: semaphoreName,
			Limit:         100,
		},
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Nil(t, resp.ApplicationError)
	require.NotNil(t, resp.Payload)

	return resp.Payload
}

func updateSemaphore(t *testing.T, core *Core, namespaceId *corepb.NamespaceId, semaphoreName string, description string, permits uint64, now time.Time) *corepb.Semaphore {
	t.Helper()

	resp, err := core.UpdateSemaphore(&coreapis.UpdateSemaphoreRequest{
		Payload: &corepb.UpdateSemaphoreRequest{
			NamespaceId:   namespaceId,
			SemaphoreName: semaphoreName,
			Description:   description,
			Permits:       permits,
			Now:           now.UnixNano(),
		},
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Nil(t, resp.ApplicationError)
	require.NotNil(t, resp.Payload)
	require.NotNil(t, resp.Payload.Semaphore)

	return resp.Payload.Semaphore
}

func updateSemaphoreWithError(t *testing.T, core *Core, namespaceId *corepb.NamespaceId, semaphoreName string, description string, permits uint64, now time.Time) *monsterax.Error {
	t.Helper()

	resp, err := core.UpdateSemaphore(&coreapis.UpdateSemaphoreRequest{
		Payload: &corepb.UpdateSemaphoreRequest{
			NamespaceId:   namespaceId,
			SemaphoreName: semaphoreName,
			Description:   description,
			Permits:       permits,
			Now:           now.UnixNano(),
		},
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Nil(t, resp.Payload)
	require.NotNil(t, resp.ApplicationError)

	return resp.ApplicationError
}
