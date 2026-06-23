package semaphores

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/evrblk/monstera/store"
	monsterax "github.com/evrblk/monstera/x"
	"github.com/samber/lo"
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

	t.Run("acquire with nonexistent lease", func(t *testing.T) {
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

		// Create semaphore but never create the lease referenced below
		_ = createSemaphore(t, core, semaphoreId, "test_semaphore", 5, now)
		fakeLease := &corepb.LeaseId{
			AccountId:   accountId,
			NamespaceId: namespaceId.NamespaceId,
			LeaseId:     rand.Uint64(),
		}

		appErr := acquireSemaphoreWithError(t, core, namespaceId, fakeLease, "test_semaphore", 1, now)
		require.Equal(t, monsterax.NotFound, appErr.Code)
		require.Contains(t, appErr.Message, "lease not found")

		// No holder should have been recorded.
		sem := getSemaphoreByName(t, core, namespaceId, "test_semaphore", now)
		require.EqualValues(t, 0, sem.ActiveHoldersCount)
		require.EqualValues(t, 0, sem.ActiveHolds)
	})

	t.Run("acquire with expired lease", func(t *testing.T) {
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

		_ = createSemaphore(t, core, semaphoreId, "test_semaphore", 5, now)

		// 1-minute TTL — call AcquireSemaphore at T+2m, after the lease has expired.
		lease := createLease(t, core, accountId, namespaceId.NamespaceId, "process_1", now, 1*time.Minute)
		appErr := acquireSemaphoreWithError(t, core, namespaceId, lease.Id, "test_semaphore", 1, now.Add(2*time.Minute))
		require.Equal(t, monsterax.NotFound, appErr.Code)
		require.Contains(t, appErr.Message, "lease not found")

		// No holder should have been recorded.
		sem := getSemaphoreByName(t, core, namespaceId, "test_semaphore", now.Add(2*time.Minute))
		require.EqualValues(t, 0, sem.ActiveHoldersCount)
		require.EqualValues(t, 0, sem.ActiveHolds)
	})

	t.Run("same lease re-acquires with a smaller weight - permits freed", func(t *testing.T) {
		core := newSemaphoresCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespaceId := &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: rand.Uint32(),
		}
		semaphoreId := &corepb.SemaphoreId{
			AccountId:   accountId,
			NamespaceId: namespaceId.NamespaceId,
			SemaphoreId: rand.Uint64(),
		}

		_ = createSemaphore(t, core, semaphoreId, "sem", 10, now)
		lease := createLease(t, core, accountId, namespaceId.NamespaceId, "process_1", now, 60*time.Minute)

		success, sem := acquireSemaphore(t, core, namespaceId, lease.Id, "sem", 5, now)
		require.True(t, success)
		require.EqualValues(t, 5, sem.ActiveHolds)
		require.EqualValues(t, 1, sem.ActiveHoldersCount)

		// Re-acquire with smaller weight at a later time
		success, sem = acquireSemaphore(t, core, namespaceId, lease.Id, "sem", 2, now.Add(time.Minute))
		require.True(t, success)
		require.EqualValues(t, 2, sem.ActiveHolds)
		require.EqualValues(t, 1, sem.ActiveHoldersCount)

		// Another lease can grab the freed permits
		lease2 := createLease(t, core, accountId, namespaceId.NamespaceId, "process_2", now, 60*time.Minute)
		success, sem = acquireSemaphore(t, core, namespaceId, lease2.Id, "sem", 8, now.Add(time.Minute))
		require.True(t, success)
		require.EqualValues(t, 10, sem.ActiveHolds)
		require.EqualValues(t, 2, sem.ActiveHoldersCount)
	})

	t.Run("same lease re-acquires with a larger weight - succeeds if permits fit", func(t *testing.T) {
		core := newSemaphoresCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespaceId := &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: rand.Uint32(),
		}
		semaphoreId := &corepb.SemaphoreId{
			AccountId:   accountId,
			NamespaceId: namespaceId.NamespaceId,
			SemaphoreId: rand.Uint64(),
		}

		_ = createSemaphore(t, core, semaphoreId, "sem", 10, now)
		lease := createLease(t, core, accountId, namespaceId.NamespaceId, "process_1", now, 60*time.Minute)

		success, _ := acquireSemaphore(t, core, namespaceId, lease.Id, "sem", 2, now)
		require.True(t, success)

		// A different lease is already holding 5; the same lease now wants to grow from 2 → 5.
		// 5 + 5 = 10 = permits — should fit.
		lease2 := createLease(t, core, accountId, namespaceId.NamespaceId, "process_2", now, 60*time.Minute)
		success, _ = acquireSemaphore(t, core, namespaceId, lease2.Id, "sem", 5, now)
		require.True(t, success)

		success, sem := acquireSemaphore(t, core, namespaceId, lease.Id, "sem", 5, now.Add(time.Minute))
		require.True(t, success)
		require.EqualValues(t, 10, sem.ActiveHolds)
		require.EqualValues(t, 2, sem.ActiveHoldersCount)

		// The grown holder's weight is persisted.
		listed := listSemaphoreHolders(t, core, namespaceId, "sem", now.Add(time.Minute))
		require.Len(t, listed.Holders, 2)
		holdersByLease := lo.KeyBy(listed.Holders, func(h *corepb.SemaphoreHolder) uint64 {
			return h.Id.LeaseId
		})
		require.EqualValues(t, 5, holdersByLease[lease.Id.LeaseId].Weight)
		require.EqualValues(t, 5, holdersByLease[lease2.Id.LeaseId].Weight)
	})

	t.Run("same lease re-acquires with a larger weight - fails when permits run out", func(t *testing.T) {
		core := newSemaphoresCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespaceId := &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: rand.Uint32(),
		}
		semaphoreId := &corepb.SemaphoreId{
			AccountId:   accountId,
			NamespaceId: namespaceId.NamespaceId,
			SemaphoreId: rand.Uint64(),
		}

		_ = createSemaphore(t, core, semaphoreId, "sem", 10, now)
		lease := createLease(t, core, accountId, namespaceId.NamespaceId, "process_1", now, 60*time.Minute)
		success, _ := acquireSemaphore(t, core, namespaceId, lease.Id, "sem", 2, now)
		require.True(t, success)

		// Another lease consumes most of the remaining permits.
		lease2 := createLease(t, core, accountId, namespaceId.NamespaceId, "process_2", now, 60*time.Minute)
		success, _ = acquireSemaphore(t, core, namespaceId, lease2.Id, "sem", 7, now)
		require.True(t, success)

		// Attempt to grow from 2 → 5: would need 5 + 7 = 12 > 10.
		success, sem := acquireSemaphore(t, core, namespaceId, lease.Id, "sem", 5, now.Add(time.Minute))
		require.False(t, success)
		// State is unchanged.
		require.EqualValues(t, 9, sem.ActiveHolds)
		require.EqualValues(t, 2, sem.ActiveHoldersCount)

		// Existing holder weight stays at 2, and its expiration was NOT refreshed.
		listed := listSemaphoreHolders(t, core, namespaceId, "sem", now)
		holdersByLease := lo.KeyBy(listed.Holders, func(h *corepb.SemaphoreHolder) uint64 {
			return h.Id.LeaseId
		})
		require.EqualValues(t, 2, holdersByLease[lease.Id.LeaseId].Weight)
		require.EqualValues(t, now.UnixNano(), holdersByLease[lease.Id.LeaseId].LockedAt)
	})

	t.Run("same lease re-acquires with the same weight - refreshes expiration only", func(t *testing.T) {
		core := newSemaphoresCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespaceId := &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: rand.Uint32(),
		}
		semaphoreId := &corepb.SemaphoreId{
			AccountId:   accountId,
			NamespaceId: namespaceId.NamespaceId,
			SemaphoreId: rand.Uint64(),
		}

		_ = createSemaphore(t, core, semaphoreId, "sem", 10, now)
		lease := createLease(t, core, accountId, namespaceId.NamespaceId, "process_1", now, 60*time.Minute)
		success, _ := acquireSemaphore(t, core, namespaceId, lease.Id, "sem", 3, now)
		require.True(t, success)

		laterLease := createLease(t, core, accountId, namespaceId.NamespaceId, "process_1", now.Add(time.Minute), 30*time.Minute)
		_ = laterLease // just to advance time consistently in the test

		// Re-acquire at T+1m with the same weight; ActiveHolds should not change.
		success, sem := acquireSemaphore(t, core, namespaceId, lease.Id, "sem", 3, now.Add(time.Minute))
		require.True(t, success)
		require.EqualValues(t, 3, sem.ActiveHolds)
		require.EqualValues(t, 1, sem.ActiveHoldersCount)

		// The single holder's LockedAt advanced.
		listed := listSemaphoreHolders(t, core, namespaceId, "sem", now.Add(time.Minute))
		require.Len(t, listed.Holders, 1)
		require.EqualValues(t, 3, listed.Holders[0].Weight)
		require.EqualValues(t, now.Add(time.Minute).UnixNano(), listed.Holders[0].LockedAt)
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

	t.Run("release with nonexistent lease", func(t *testing.T) {
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

		// Create semaphore and acquire it with a real lease so there is a holder to preserve.
		_ = createSemaphore(t, core, semaphoreId, "test_semaphore", 5, now)
		lease := createLease(t, core, accountId, namespaceId.NamespaceId, "process_1", now, 60*time.Minute)
		success, _ := acquireSemaphore(t, core, namespaceId, lease.Id, "test_semaphore", 2, now)
		require.True(t, success)

		// Releasing with a lease id that was never created must return NotFound and leave state alone.
		fakeLease := &corepb.LeaseId{
			AccountId:   accountId,
			NamespaceId: namespaceId.NamespaceId,
			LeaseId:     rand.Uint64(),
		}
		appErr := releaseSemaphoreWithError(t, core, namespaceId, "test_semaphore", fakeLease, now)
		require.Equal(t, monsterax.NotFound, appErr.Code)
		require.Contains(t, appErr.Message, "lease not found")

		// Original holder is untouched.
		sem := getSemaphoreByName(t, core, namespaceId, "test_semaphore", now)
		require.EqualValues(t, 1, sem.ActiveHoldersCount)
		require.EqualValues(t, 2, sem.ActiveHolds)
	})

	t.Run("release with expired lease", func(t *testing.T) {
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

		// Acquire under a short-TTL lease, then try to release after it has expired.
		_ = createSemaphore(t, core, semaphoreId, "test_semaphore", 5, now)
		lease := createLease(t, core, accountId, namespaceId.NamespaceId, "process_1", now, 1*time.Minute)
		success, _ := acquireSemaphore(t, core, namespaceId, lease.Id, "test_semaphore", 2, now)
		require.True(t, success)

		// T+2m: lease is now expired.
		appErr := releaseSemaphoreWithError(t, core, namespaceId, "test_semaphore", lease.Id, now.Add(2*time.Minute))
		require.Equal(t, monsterax.NotFound, appErr.Code)
		require.Contains(t, appErr.Message, "lease not found")
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
		_ = updateSemaphore(t, core, namespaceId, "test_semaphore", "updated description", 3, 1, now.Add(time.Minute))

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
		appErr := updateSemaphoreWithError(t, core, namespaceId, "test_semaphore", "updated description", 2, 1, now.Add(5*time.Minute))
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
		appErr := updateSemaphoreWithError(t, core, namespaceId, "nonexistent_semaphore", "updated description", 3, 1, now)
		require.Equal(t, monsterax.NotFound, appErr.Code)
	})

	t.Run("clears expirationRecords when the prune removes the last holder", func(t *testing.T) {
		// Without the reconciliation in UpdateSemaphore, a row would survive in
		// expirationRecords at the pruned holder's expiration time, which the GC sweep
		// then revisits forever.
		core := newSemaphoresCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespaceId := &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: rand.Uint32(),
		}
		semaphoreId := &corepb.SemaphoreId{
			AccountId:   accountId,
			NamespaceId: namespaceId.NamespaceId,
			SemaphoreId: rand.Uint64(),
		}
		_ = createSemaphore(t, core, semaphoreId, "sema", 3, now)

		lease := createLease(t, core, accountId, namespaceId.NamespaceId, "p", now, 1*time.Minute)
		success, sem := acquireSemaphore(t, core, namespaceId, lease.Id, "sema", 1, now)
		require.True(t, success)
		require.Equal(t, []int64{sem.EarliestHolderExpiresAt}, listExpirationRecords(t, core, semaphoreId))

		// T+2m: the only holder has expired. UpdateSemaphore prunes it and must clean up
		// the index row that was tracking it.
		_ = updateSemaphore(t, core, namespaceId, "sema", "updated", 3, 1, now.Add(2*time.Minute))
		require.Empty(t, listExpirationRecords(t, core, semaphoreId))

		stored, err := core.semaphores.Get(core.badgerStore.View(), semaphoreId)
		require.NoError(t, err)
		require.EqualValues(t, 0, stored.EarliestHolderExpiresAt)
	})

	t.Run("advances expirationRecords when only some holders are pruned", func(t *testing.T) {
		core := newSemaphoresCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespaceId := &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: rand.Uint32(),
		}
		semaphoreId := &corepb.SemaphoreId{
			AccountId:   accountId,
			NamespaceId: namespaceId.NamespaceId,
			SemaphoreId: rand.Uint64(),
		}
		_ = createSemaphore(t, core, semaphoreId, "sema", 5, now)

		// Earliest holder expires at T+1m, second at T+1h.
		shortLease := createLease(t, core, accountId, namespaceId.NamespaceId, "p_short", now, 1*time.Minute)
		success, _ := acquireSemaphore(t, core, namespaceId, shortLease.Id, "sema", 1, now)
		require.True(t, success)

		longLease := createLease(t, core, accountId, namespaceId.NamespaceId, "p_long", now, 1*time.Hour)
		success, semAfterLong := acquireSemaphore(t, core, namespaceId, longLease.Id, "sema", 1, now)
		require.True(t, success)
		require.Equal(t, shortLease.ExpiresAt, semAfterLong.EarliestHolderExpiresAt)
		require.Equal(t, []int64{shortLease.ExpiresAt}, listExpirationRecords(t, core, semaphoreId))

		// T+2m: the short-lived holder is gone. UpdateSemaphore prunes it and must move the
		// index row from the short lease's expiration to the long lease's.
		updated := updateSemaphore(t, core, namespaceId, "sema", "updated", 5, 1, now.Add(2*time.Minute))
		require.Equal(t, longLease.ExpiresAt, updated.EarliestHolderExpiresAt)
		require.Equal(t, []int64{longLease.ExpiresAt}, listExpirationRecords(t, core, semaphoreId))

		stored, err := core.semaphores.Get(core.badgerStore.View(), semaphoreId)
		require.NoError(t, err)
		require.Equal(t, longLease.ExpiresAt, stored.EarliestHolderExpiresAt)
	})

	t.Run("version increments on each successful update", func(t *testing.T) {
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

		// A freshly created semaphore starts at version 1
		created := createSemaphore(t, core, semaphoreId, "test_semaphore", 2, now)
		require.EqualValues(t, 1, created.Version)

		// Updating with the matching current version succeeds and bumps the version
		updated := updateSemaphore(t, core, namespaceId, "test_semaphore", "desc v2", 3, 1, now.Add(time.Minute))
		require.EqualValues(t, 2, updated.Version)

		// The next update must use the new version
		updated = updateSemaphore(t, core, namespaceId, "test_semaphore", "desc v3", 4, 2, now.Add(2*time.Minute))
		require.EqualValues(t, 3, updated.Version)
	})

	t.Run("update with stale version", func(t *testing.T) {
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

		_ = createSemaphore(t, core, semaphoreId, "test_semaphore", 2, now)

		// First update with version 1 succeeds (semaphore is now at version 2)
		_ = updateSemaphore(t, core, namespaceId, "test_semaphore", "desc v2", 3, 1, now.Add(time.Minute))

		// Reusing the stale version 1 is rejected with a version mismatch
		appErr := updateSemaphoreWithError(t, core, namespaceId, "test_semaphore", "should not apply", 5, 1, now.Add(2*time.Minute))
		require.Equal(t, monsterax.InvalidArgument, appErr.Code)
		require.Contains(t, appErr.Message, "version mismatch")

		// The rejected update did not change anything
		semaphore := getSemaphore(t, core, semaphoreId, now.Add(3*time.Minute))
		require.Equal(t, "desc v2", semaphore.Description)
		require.EqualValues(t, 3, semaphore.Permits)
		require.EqualValues(t, 2, semaphore.Version)
	})

	t.Run("update with future version", func(t *testing.T) {
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

		_ = createSemaphore(t, core, semaphoreId, "test_semaphore", 2, now)

		// Passing a version the semaphore has never reached is rejected
		appErr := updateSemaphoreWithError(t, core, namespaceId, "test_semaphore", "desc", 3, 99, now.Add(time.Minute))
		require.Equal(t, monsterax.InvalidArgument, appErr.Code)
		require.Contains(t, appErr.Message, "version mismatch")
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

	t.Run("max number of semaphores per namespace", func(t *testing.T) {
		core := newSemaphoresCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespaceId := rand.Uint32()
		const maxSemaphores = int64(3)

		// Create semaphores up to the limit using the same MaxNumberOfSemaphoresPerNamespace
		// throughout — each call must succeed.
		for i := 0; i < int(maxSemaphores); i++ {
			semaphoreId := &corepb.SemaphoreId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
				SemaphoreId: rand.Uint64(),
			}
			_ = createSemaphoreWithMax(t, core, semaphoreId, fmt.Sprintf("sem_%d", i), 5, maxSemaphores, now)
		}

		// One more must fail.
		overSemaphoreId := &corepb.SemaphoreId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			SemaphoreId: rand.Uint64(),
		}
		appErr := createSemaphoreWithError(t, core, overSemaphoreId, "sem_over", 5, maxSemaphores, now)
		require.Equal(t, monsterax.ResourceExhausted, appErr.Code)
		require.Contains(t, appErr.Message, "max number of semaphores per namespace reached")

		// Counter stayed at maxSemaphores — the failed call left no state behind.
		counters, err := core.counters.Get(core.badgerStore.View(), accountId, namespaceId)
		require.NoError(t, err)
		require.EqualValues(t, maxSemaphores, counters.NumberOfSemaphores)

		// The limit is per-namespace: a different namespace under the same account still
		// accepts new semaphores.
		_ = createSemaphoreWithMax(t, core, &corepb.SemaphoreId{
			AccountId:   accountId,
			NamespaceId: rand.Uint32(),
			SemaphoreId: rand.Uint64(),
		}, "other_ns_sem", 5, maxSemaphores, now)

		// And per-account: a different account is also unaffected.
		_ = createSemaphoreWithMax(t, core, &corepb.SemaphoreId{
			AccountId:   rand.Uint64(),
			NamespaceId: namespaceId,
			SemaphoreId: rand.Uint64(),
		}, "other_account_sem", 5, maxSemaphores, now)
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

func TestCore_SemaphoreMetadata(t *testing.T) {
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

	// T+0: Create semaphore with metadata.
	createMetadata := map[string]string{"team": "search"}
	createResp, err := core.CreateSemaphore(&coreapis.CreateSemaphoreRequest{
		Payload: &corepb.CreateSemaphoreRequest{
			SemaphoreId:                       semaphoreId,
			Name:                              "test_semaphore",
			Description:                       "test description",
			Permits:                           5,
			Now:                               now.UnixNano(),
			MaxNumberOfSemaphoresPerNamespace: 10000,
			Metadata:                          createMetadata,
		},
	})
	require.NoError(t, err)
	require.Nil(t, createResp.ApplicationError)
	require.NotNil(t, createResp.Payload)
	require.NotNil(t, createResp.Payload.Semaphore)
	require.Equal(t, createMetadata, createResp.Payload.Semaphore.Metadata)
	require.EqualValues(t, 5, createResp.Payload.Semaphore.Permits)

	// T+1m: Metadata is persisted and read back via GetSemaphore.
	semaphore := getSemaphore(t, core, semaphoreId, now.Add(time.Minute))
	require.Equal(t, createMetadata, semaphore.Metadata)

	// T+1m: Metadata is also read back via GetSemaphoreByName.
	semaphore = getSemaphoreByName(t, core, namespaceId, "test_semaphore", now.Add(time.Minute))
	require.Equal(t, createMetadata, semaphore.Metadata)

	// T+2m: Update semaphore with new metadata.
	updateMetadata := map[string]string{"team": "search", "env": "prod"}
	updateResp, err := core.UpdateSemaphore(&coreapis.UpdateSemaphoreRequest{
		Payload: &corepb.UpdateSemaphoreRequest{
			NamespaceId:     namespaceId,
			SemaphoreName:   "test_semaphore",
			Description:     "updated description",
			Permits:         5,
			Now:             now.Add(2 * time.Minute).UnixNano(),
			Metadata:        updateMetadata,
			ExpectedVersion: 1,
		},
	})
	require.NoError(t, err)
	require.Nil(t, updateResp.ApplicationError)
	require.NotNil(t, updateResp.Payload)
	require.NotNil(t, updateResp.Payload.Semaphore)
	require.Equal(t, updateMetadata, updateResp.Payload.Semaphore.Metadata)

	// T+3m: Updated metadata is persisted.
	semaphore = getSemaphore(t, core, semaphoreId, now.Add(3*time.Minute))
	require.Equal(t, updateMetadata, semaphore.Metadata)

	// T+4m: Create a lease and acquire the semaphore with holder metadata.
	holderMetadata := map[string]string{"host": "node-2"}
	lease := createLease(t, core, accountId, namespaceId.NamespaceId, "process_1", now.Add(4*time.Minute), 60*time.Minute)
	acquireResp, err := core.AcquireSemaphore(&coreapis.AcquireSemaphoreRequest{
		Payload: &corepb.AcquireSemaphoreRequest{
			NamespaceId:   namespaceId,
			SemaphoreName: "test_semaphore",
			Weight:        1,
			Now:           now.Add(4 * time.Minute).UnixNano(),
			LeaseId:       lease.Id.LeaseId,
			Metadata:      holderMetadata,
		},
	})
	require.NoError(t, err)
	require.Nil(t, acquireResp.ApplicationError)
	require.NotNil(t, acquireResp.Payload)
	require.True(t, acquireResp.Payload.Success)

	// T+5m: Holder metadata round-trips through ListSemaphoreHolders.
	list := listSemaphoreHolders(t, core, namespaceId, "test_semaphore", now.Add(5*time.Minute))
	require.Len(t, list.Holders, 1)
	require.Equal(t, holderMetadata, list.Holders[0].Metadata)
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
				RecordId:      rand.Uint64(),
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
				RecordId:      rand.Uint64(),
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
				RecordId:      rand.Uint64(),
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
				GcRecordHoldersPageSize:    100,
				MaxVisited:                 100,
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

	t.Run("queues a GC record so leftover holders can be drained later", func(t *testing.T) {
		core := newSemaphoresCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespaceId := &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: rand.Uint32(),
		}
		semaphoreId := &corepb.SemaphoreId{
			AccountId:   accountId,
			NamespaceId: namespaceId.NamespaceId,
			SemaphoreId: rand.Uint64(),
		}

		_ = createSemaphore(t, core, semaphoreId, "doomed", 5, now)

		// Acquire with three different leases — three holders for the same semaphore
		for i := 0; i < 3; i++ {
			lease := createLease(t, core, accountId, namespaceId.NamespaceId, fmt.Sprintf("process_%d", i), now, 60*time.Minute)
			success, _ := acquireSemaphore(t, core, namespaceId, lease.Id, "doomed", 1, now)
			require.True(t, success)
		}

		recordId := rand.Uint64()
		_, err := core.DeleteSemaphore(&coreapis.DeleteSemaphoreRequest{
			Payload: &corepb.DeleteSemaphoreRequest{
				NamespaceId:   namespaceId,
				SemaphoreName: "doomed",
				RecordId:      recordId,
			},
		})
		require.NoError(t, err)

		// The semaphore record is gone right away …
		_, err = core.semaphores.Get(core.badgerStore.View(), semaphoreId)
		require.ErrorIs(t, err, store.ErrNotFound)

		// … but the holders are still in the store until GC drains them.
		holdersResult, err := core.holders.List(core.badgerStore.View(), accountId, namespaceId.NamespaceId, semaphoreId.SemaphoreId, nil, 100)
		require.NoError(t, err)
		require.Len(t, holdersResult.holders, 3)

		// A GC record with the deleted semaphore_id was queued.
		gcRecords, err := core.gcRecords.List(core.badgerStore.View(), 100)
		require.NoError(t, err)
		require.Len(t, gcRecords, 1)
		require.Equal(t, recordId, gcRecords[0].Id)
		require.Equal(t, semaphoreId.SemaphoreId, gcRecords[0].GetSemaphoreId().SemaphoreId)
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
		list := listSemaphoreHolders(t, core, namespaceId, "test_semaphore", now)
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
				Now:           now.UnixNano(),
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
				Now:             now.UnixNano(),
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
		list := listSemaphoreHolders(t, core, namespaceId, "test_semaphore", now)
		require.Len(t, list.Holders, 0)
		require.Nil(t, list.NextPaginationToken)
	})

	t.Run("list holders for nonexistent semaphore", func(t *testing.T) {
		core := newSemaphoresCore(t)
		now := time.Now()
		namespaceId := &corepb.NamespaceId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
		}

		// Try to list holders for a nonexistent semaphore
		resp1, err := core.ListSemaphoreHolders(&coreapis.ListSemaphoreHoldersRequest{
			Payload: &corepb.ListSemaphoreHoldersRequest{
				NamespaceId:   namespaceId,
				SemaphoreName: "non_existing_semaphore",
				Now:           now.UnixNano(),
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
		list := listSemaphoreHolders(t, core, namespaceId, "test_semaphore", now)
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
		list := listSemaphoreHolders(t, core, namespaceId, "test_semaphore", now.Add(time.Minute))

		// Verify remaining holders - we expect 3 holders after releasing 2
		require.Len(t, list.Holders, 3)
	})

	t.Run("filters out holders whose leases have expired", func(t *testing.T) {
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

		_ = createSemaphore(t, core, semaphoreId, "test_semaphore", 5, now)

		// Short-lived lease expires in 1m, long-lived lease stays valid
		shortLease := createLease(t, core, accountId, namespaceId.NamespaceId, "process_short", now, 1*time.Minute)
		longLease := createLease(t, core, accountId, namespaceId.NamespaceId, "process_long", now, 60*time.Minute)

		success, _ := acquireSemaphore(t, core, namespaceId, shortLease.Id, "test_semaphore", 1, now)
		require.True(t, success)
		success, _ = acquireSemaphore(t, core, namespaceId, longLease.Id, "test_semaphore", 1, now)
		require.True(t, success)

		// T+0: both holders are returned
		list := listSemaphoreHolders(t, core, namespaceId, "test_semaphore", now)
		require.Len(t, list.Holders, 2)

		// T+2m: short lease has expired, only the long-lived holder is returned
		list = listSemaphoreHolders(t, core, namespaceId, "test_semaphore", now.Add(2*time.Minute))
		require.Len(t, list.Holders, 1)
		require.EqualValues(t, longLease.Id.LeaseId, list.Holders[0].Id.LeaseId)

		// Read-only call must not mutate state — the expired holder is still in the store
		storedHolder, err := core.holders.Get(core.badgerStore.View(), &corepb.SemaphoreHolderId{
			AccountId:   accountId,
			NamespaceId: namespaceId.NamespaceId,
			SemaphoreId: semaphoreId.SemaphoreId,
			LeaseId:     shortLease.Id.LeaseId,
		})
		require.NoError(t, err)
		require.Equal(t, shortLease.ExpiresAt, storedHolder.ExpiresAt)
	})
}

func TestCore_ListSemaphores(t *testing.T) {
	t.Run("lists semaphores in namespace", func(t *testing.T) {
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
				Now:         now.UnixNano(),
			},
		})

		require.NoError(t, err)
		require.NotNil(t, resp3)
		require.Nil(t, resp3.ApplicationError)
		require.NotNil(t, resp3.Payload)
		require.Len(t, resp3.Payload.Semaphores, 2)
	})

	t.Run("returns holder counters with expired holders filtered out", func(t *testing.T) {
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

		_ = createSemaphore(t, core, semaphoreId, "test_semaphore", 5, now)

		// Short-lived lease expires in 1m, long-lived lease stays valid
		shortLease := createLease(t, core, accountId, namespaceId.NamespaceId, "process_1", now, 1*time.Minute)
		longLease := createLease(t, core, accountId, namespaceId.NamespaceId, "process_2", now, 60*time.Minute)

		success, _ := acquireSemaphore(t, core, namespaceId, shortLease.Id, "test_semaphore", 2, now)
		require.True(t, success)
		success, _ = acquireSemaphore(t, core, namespaceId, longLease.Id, "test_semaphore", 1, now)
		require.True(t, success)

		// T+2m: short lease has expired
		listAt := now.Add(2 * time.Minute)
		resp, err := core.ListSemaphores(&coreapis.ListSemaphoresRequest{
			Payload: &corepb.ListSemaphoresRequest{
				NamespaceId: namespaceId,
				Now:         listAt.UnixNano(),
			},
		})
		require.NoError(t, err)
		require.Nil(t, resp.ApplicationError)
		require.Len(t, resp.Payload.Semaphores, 1)

		// Returned counters reflect only the still-valid holder
		listed := resp.Payload.Semaphores[0]
		require.EqualValues(t, 1, listed.ActiveHoldersCount)
		require.EqualValues(t, 1, listed.ActiveHolds)
		require.EqualValues(t, longLease.ExpiresAt, listed.EarliestHolderExpiresAt)

		// The view txn must not mutate state — stored counters are still the pre-expiration values
		storedSemaphore, err := core.semaphores.Get(core.badgerStore.View(), semaphoreId)
		require.NoError(t, err)
		require.EqualValues(t, 2, storedSemaphore.ActiveHoldersCount)
		require.EqualValues(t, 3, storedSemaphore.ActiveHolds)
	})

	t.Run("returns zero counters when all holders expired", func(t *testing.T) {
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

		_ = createSemaphore(t, core, semaphoreId, "test_semaphore", 5, now)

		lease := createLease(t, core, accountId, namespaceId.NamespaceId, "process_1", now, 1*time.Minute)
		success, _ := acquireSemaphore(t, core, namespaceId, lease.Id, "test_semaphore", 2, now)
		require.True(t, success)

		resp, err := core.ListSemaphores(&coreapis.ListSemaphoresRequest{
			Payload: &corepb.ListSemaphoresRequest{
				NamespaceId: namespaceId,
				Now:         now.Add(2 * time.Minute).UnixNano(),
			},
		})
		require.NoError(t, err)
		require.Nil(t, resp.ApplicationError)
		require.Len(t, resp.Payload.Semaphores, 1)

		listed := resp.Payload.Semaphores[0]
		require.EqualValues(t, 0, listed.ActiveHoldersCount)
		require.EqualValues(t, 0, listed.ActiveHolds)
		require.EqualValues(t, 0, listed.EarliestHolderExpiresAt)
	})
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
				GcRecordHoldersPageSize:    100,
				MaxVisited:                 1000,
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

		// Create more semaphores than MaxVisited to test the limit
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
				GcRecordHoldersPageSize:    100,
				MaxVisited:                 maxVisitedSemaphores,
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
				GcRecordHoldersPageSize:    100,
				MaxVisited:                 maxVisitedSemaphores,
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
				GcRecordHoldersPageSize:    100,
				MaxVisited:                 100,
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
				GcRecordHoldersPageSize:    100,
				MaxVisited:                 100,
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

	t.Run("drains holders left behind by DeleteSemaphore", func(t *testing.T) {
		core := newSemaphoresCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespaceId := &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: rand.Uint32(),
		}
		semaphoreId := &corepb.SemaphoreId{
			AccountId:   accountId,
			NamespaceId: namespaceId.NamespaceId,
			SemaphoreId: rand.Uint64(),
		}

		const numHolders = 5
		_ = createSemaphore(t, core, semaphoreId, "doomed", 100, now)
		leases := make([]*corepb.Lease, numHolders)
		for i := range numHolders {
			leases[i] = createLease(t, core, accountId, namespaceId.NamespaceId, fmt.Sprintf("process_%d", i), now, 60*time.Minute)
			success, _ := acquireSemaphore(t, core, namespaceId, leases[i].Id, "doomed", 1, now)
			require.True(t, success)
		}

		_, err := core.DeleteSemaphore(&coreapis.DeleteSemaphoreRequest{
			Payload: &corepb.DeleteSemaphoreRequest{
				NamespaceId:   namespaceId,
				SemaphoreName: "doomed",
				RecordId:      rand.Uint64(),
			},
		})
		require.NoError(t, err)

		// One GC pass with ample budget cleans up every leftover holder and the GC record.
		_, err = core.RunSemaphoresGarbageCollection(&coreapis.RunSemaphoresGarbageCollectionRequest{
			Payload: &corepb.RunSemaphoresGarbageCollectionRequest{
				Now:                        now.UnixNano(),
				GcRecordsPageSize:          100,
				GcRecordSemaphoresPageSize: 100,
				GcRecordHoldersPageSize:    100,
				MaxVisited:                 100,
			},
		})
		require.NoError(t, err)

		holdersResult, err := core.holders.List(core.badgerStore.View(), accountId, namespaceId.NamespaceId, semaphoreId.SemaphoreId, nil, 100)
		require.NoError(t, err)
		require.Empty(t, holdersResult.holders)

		gcRecords, err := core.gcRecords.List(core.badgerStore.View(), 100)
		require.NoError(t, err)
		require.Empty(t, gcRecords)

		// Lease-id index entries are gone too, so the deleted semaphore no longer shows up for any lease.
		for _, lease := range leases {
			result, err := core.semaphores.ListByLeaseId(core.badgerStore.View(), lease.Id, nil, 100)
			require.NoError(t, err)
			require.Empty(t, result.semaphores)
		}
	})

	t.Run("drains a deleted semaphore's holders across multiple bounded passes", func(t *testing.T) {
		core := newSemaphoresCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespaceId := &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: rand.Uint32(),
		}
		semaphoreId := &corepb.SemaphoreId{
			AccountId:   accountId,
			NamespaceId: namespaceId.NamespaceId,
			SemaphoreId: rand.Uint64(),
		}

		const numHolders = 7
		_ = createSemaphore(t, core, semaphoreId, "doomed", 100, now)
		for i := range numHolders {
			lease := createLease(t, core, accountId, namespaceId.NamespaceId, fmt.Sprintf("process_%d", i), now, 60*time.Minute)
			success, _ := acquireSemaphore(t, core, namespaceId, lease.Id, "doomed", 1, now)
			require.True(t, success)
		}

		_, err := core.DeleteSemaphore(&coreapis.DeleteSemaphoreRequest{
			Payload: &corepb.DeleteSemaphoreRequest{
				NamespaceId:   namespaceId,
				SemaphoreName: "doomed",
				RecordId:      rand.Uint64(),
			},
		})
		require.NoError(t, err)

		// Tight per-pass budget so it takes several passes to drain 7 holders.
		const passBudget = 3
		passes := 0
		for {
			passes++
			require.Less(t, passes, 20, "GC failed to converge")
			_, err = core.RunSemaphoresGarbageCollection(&coreapis.RunSemaphoresGarbageCollectionRequest{
				Payload: &corepb.RunSemaphoresGarbageCollectionRequest{
					Now:                        now.UnixNano(),
					GcRecordsPageSize:          10,
					GcRecordSemaphoresPageSize: 10,
					GcRecordHoldersPageSize:    passBudget,
					MaxVisited:                 passBudget,
				},
			})
			require.NoError(t, err)

			gcRecords, err := core.gcRecords.List(core.badgerStore.View(), 100)
			require.NoError(t, err)
			if len(gcRecords) == 0 {
				break
			}
		}
		require.Greater(t, passes, 1, "expected multiple passes to clear the holders")

		holdersResult, err := core.holders.List(core.badgerStore.View(), accountId, namespaceId.NamespaceId, semaphoreId.SemaphoreId, nil, 100)
		require.NoError(t, err)
		require.Empty(t, holdersResult.holders)
	})

	t.Run("namespace GC drains holders before deleting each semaphore", func(t *testing.T) {
		core := newSemaphoresCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespaceId := &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: rand.Uint32(),
		}

		const numSemaphores = 4
		const holdersPerSemaphore = 5
		semaphoreIds := make([]*corepb.SemaphoreId, numSemaphores)
		for i := range numSemaphores {
			semaphoreIds[i] = &corepb.SemaphoreId{
				AccountId:   accountId,
				NamespaceId: namespaceId.NamespaceId,
				SemaphoreId: rand.Uint64(),
			}
			_ = createSemaphore(t, core, semaphoreIds[i], fmt.Sprintf("sem_%d", i), 100, now)
			for j := 0; j < holdersPerSemaphore; j++ {
				lease := createLease(t, core, accountId, namespaceId.NamespaceId, fmt.Sprintf("proc_%d_%d", i, j), now, 60*time.Minute)
				success, _ := acquireSemaphore(t, core, namespaceId, lease.Id, fmt.Sprintf("sem_%d", i), 1, now)
				require.True(t, success)
			}
		}

		// A second namespace whose state must NOT be touched by the GC.
		survivorNamespaceId := &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: rand.Uint32(),
		}
		survivorSemaphoreId := &corepb.SemaphoreId{
			AccountId:   accountId,
			NamespaceId: survivorNamespaceId.NamespaceId,
			SemaphoreId: rand.Uint64(),
		}
		_ = createSemaphore(t, core, survivorSemaphoreId, "survivor", 1, now)
		survivorLease := createLease(t, core, accountId, survivorNamespaceId.NamespaceId, "survivor_proc", now, 60*time.Minute)
		success, _ := acquireSemaphore(t, core, survivorNamespaceId, survivorLease.Id, "survivor", 1, now)
		require.True(t, success)

		_, err := core.SemaphoresDeleteNamespace(&coreapis.SemaphoresDeleteNamespaceRequest{
			Payload: &corepb.SemaphoresDeleteNamespaceRequest{
				RecordId:    rand.Uint64(),
				NamespaceId: namespaceId,
				Now:         now.UnixNano(),
			},
		})
		require.NoError(t, err)

		// One pass with a tiny budget: it should drain a partial set of holders but cannot finish.
		const passBudget = 3
		_, err = core.RunSemaphoresGarbageCollection(&coreapis.RunSemaphoresGarbageCollectionRequest{
			Payload: &corepb.RunSemaphoresGarbageCollectionRequest{
				Now:                        now.UnixNano(),
				GcRecordsPageSize:          10,
				GcRecordSemaphoresPageSize: 10,
				GcRecordHoldersPageSize:    passBudget,
				MaxVisited:                 passBudget,
			},
		})
		require.NoError(t, err)

		// The GC record should still be around because we couldn't drain everything.
		gcRecords, err := core.gcRecords.List(core.badgerStore.View(), 100)
		require.NoError(t, err)
		require.Len(t, gcRecords, 1)

		// At least the first semaphore should still exist — its holders aren't fully drained yet,
		// so the semaphore record must remain so a future pass can finish the job.
		firstSemaphore, err := core.semaphores.Get(core.badgerStore.View(), semaphoreIds[0])
		require.NoError(t, err)
		require.NotNil(t, firstSemaphore)

		// Keep running passes until the GC record disappears.
		passes := 1
		for {
			passes++
			require.Less(t, passes, 100, "GC failed to converge")
			_, err = core.RunSemaphoresGarbageCollection(&coreapis.RunSemaphoresGarbageCollectionRequest{
				Payload: &corepb.RunSemaphoresGarbageCollectionRequest{
					Now:                        now.UnixNano(),
					GcRecordsPageSize:          10,
					GcRecordSemaphoresPageSize: 10,
					GcRecordHoldersPageSize:    passBudget,
					MaxVisited:                 passBudget,
				},
			})
			require.NoError(t, err)

			gcRecords, err := core.gcRecords.List(core.badgerStore.View(), 100)
			require.NoError(t, err)
			if len(gcRecords) == 0 {
				break
			}
		}
		require.Greater(t, passes, 2, "expected multi-pass GC to clear the deleted namespace")

		// All deleted-namespace semaphores and their holders are gone.
		for _, sid := range semaphoreIds {
			_, err := core.semaphores.Get(core.badgerStore.View(), sid)
			require.ErrorIs(t, err, store.ErrNotFound)
			holdersResult, err := core.holders.List(core.badgerStore.View(), accountId, namespaceId.NamespaceId, sid.SemaphoreId, nil, 100)
			require.NoError(t, err)
			require.Empty(t, holdersResult.holders)
		}

		// The unrelated namespace is untouched.
		survivor, err := core.semaphores.Get(core.badgerStore.View(), survivorSemaphoreId)
		require.NoError(t, err)
		require.EqualValues(t, 1, survivor.ActiveHoldersCount)
	})

	t.Run("reaps expired leases and releases their holders", func(t *testing.T) {
		core := newSemaphoresCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespaceId := &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: rand.Uint32(),
		}

		// One semaphore with a generous permit budget so every acquire below succeeds.
		semaphoreId := &corepb.SemaphoreId{
			AccountId:   accountId,
			NamespaceId: namespaceId.NamespaceId,
			SemaphoreId: rand.Uint64(),
		}
		_ = createSemaphore(t, core, semaphoreId, "sema", 10, now)

		// Three leases that all expire by GC time.
		expiredLeases := make([]*corepb.Lease, 3)
		for i := range expiredLeases {
			lease := createLease(t, core, accountId, namespaceId.NamespaceId, fmt.Sprintf("expired_%d", i), now, 1*time.Minute)
			success, _ := acquireSemaphore(t, core, namespaceId, lease.Id, "sema", 1, now)
			require.True(t, success)
			expiredLeases[i] = lease
		}

		// A lease that remains valid past GC time — must survive the sweep untouched.
		liveLease := createLease(t, core, accountId, namespaceId.NamespaceId, "live", now, 1*time.Hour)
		success, _ := acquireSemaphore(t, core, namespaceId, liveLease.Id, "sema", 1, now)
		require.True(t, success)

		// Before the sweep: four leases + four holders.
		preCounters, err := core.counters.Get(core.badgerStore.View(), accountId, namespaceId.NamespaceId)
		require.NoError(t, err)
		require.EqualValues(t, 4, preCounters.NumberOfLeases)

		gcTime := now.Add(2 * time.Minute)
		_, err = core.RunSemaphoresGarbageCollection(&coreapis.RunSemaphoresGarbageCollectionRequest{
			Payload: &corepb.RunSemaphoresGarbageCollectionRequest{
				Now:                        gcTime.UnixNano(),
				GcRecordsPageSize:          100,
				GcRecordSemaphoresPageSize: 100,
				GcRecordHoldersPageSize:    100,
				MaxVisited:                 100,
			},
		})
		require.NoError(t, err)

		// Each expired lease and its holder is gone.
		for _, lease := range expiredLeases {
			_, err := core.leases.Get(core.badgerStore.View(), lease.Id)
			require.ErrorIs(t, err, store.ErrNotFound)

			_, err = core.holders.Get(core.badgerStore.View(), &corepb.SemaphoreHolderId{
				AccountId:   accountId,
				NamespaceId: namespaceId.NamespaceId,
				SemaphoreId: semaphoreId.SemaphoreId,
				LeaseId:     lease.Id.LeaseId,
			})
			require.ErrorIs(t, err, store.ErrNotFound)
		}

		// The live lease and its holder survive.
		stillLive, err := core.leases.Get(core.badgerStore.View(), liveLease.Id)
		require.NoError(t, err)
		require.Equal(t, liveLease.ExpiresAt, stillLive.ExpiresAt)
		liveHolder, err := core.holders.Get(core.badgerStore.View(), &corepb.SemaphoreHolderId{
			AccountId:   accountId,
			NamespaceId: namespaceId.NamespaceId,
			SemaphoreId: semaphoreId.SemaphoreId,
			LeaseId:     liveLease.Id.LeaseId,
		})
		require.NoError(t, err)
		require.Equal(t, liveLease.ExpiresAt, liveHolder.ExpiresAt)

		// Counter is decremented by exactly the number of expired leases.
		postCounters, err := core.counters.Get(core.badgerStore.View(), accountId, namespaceId.NamespaceId)
		require.NoError(t, err)
		require.EqualValues(t, 1, postCounters.NumberOfLeases)

		// The surviving semaphore state reflects only the live holder.
		sem := getSemaphore(t, core, semaphoreId, gcTime)
		require.EqualValues(t, 1, sem.ActiveHoldersCount)
		require.EqualValues(t, 1, sem.ActiveHolds)
	})

	t.Run("expired-lease sweep is skipped when prior sweeps exhaust MaxVisited", func(t *testing.T) {
		// Several semaphores each have one expired holder, so the per-semaphore expiration
		// sweep alone exceeds the visit budget. Once that budget is gone the lease sweep
		// must not run — the expired leases are left in place for a subsequent pass.
		core := newSemaphoresCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespaceId := &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: rand.Uint32(),
		}

		const numSemaphores = 5
		semaphoreIds := make([]*corepb.SemaphoreId, numSemaphores)
		leases := make([]*corepb.Lease, numSemaphores)
		for i := range numSemaphores {
			semaphoreIds[i] = &corepb.SemaphoreId{
				AccountId:   accountId,
				NamespaceId: namespaceId.NamespaceId,
				SemaphoreId: rand.Uint64(),
			}
			_ = createSemaphore(t, core, semaphoreIds[i], fmt.Sprintf("sema_%d", i), 5, now)

			lease := createLease(t, core, accountId, namespaceId.NamespaceId, fmt.Sprintf("p_%d", i), now, 1*time.Minute)
			success, _ := acquireSemaphore(t, core, namespaceId, lease.Id, fmt.Sprintf("sema_%d", i), 1, now)
			require.True(t, success)
			leases[i] = lease
		}

		// A budget of 2 lets the per-semaphore sweep visit two records and then stop, well
		// before either the remaining semaphores or any lease row is touched.
		gcTime := now.Add(2 * time.Minute)
		_, err := core.RunSemaphoresGarbageCollection(&coreapis.RunSemaphoresGarbageCollectionRequest{
			Payload: &corepb.RunSemaphoresGarbageCollectionRequest{
				Now:                        gcTime.UnixNano(),
				GcRecordsPageSize:          100,
				GcRecordSemaphoresPageSize: 100,
				GcRecordHoldersPageSize:    100,
				MaxVisited:                 2,
			},
		})
		require.NoError(t, err)

		// At least three leases must survive the first pass — their semaphores were never
		// reached by the expiration sweep and the lease sweep had no remaining budget.
		survived := 0
		for _, lease := range leases {
			_, err := core.leases.Get(core.badgerStore.View(), lease.Id)
			if err == nil {
				survived++
				continue
			}
			require.ErrorIs(t, err, store.ErrNotFound)
		}
		require.GreaterOrEqual(t, survived, numSemaphores-2, "GC processed more work than MaxVisited allowed")

		// Subsequent bounded passes converge. Each pass reaps a small slice of work
		// (one semaphore + its expired holder, or two empty lease rows), so we cap the
		// loop generously rather than fixing the exact pass count.
		for range 20 {
			_, err := core.RunSemaphoresGarbageCollection(&coreapis.RunSemaphoresGarbageCollectionRequest{
				Payload: &corepb.RunSemaphoresGarbageCollectionRequest{
					Now:                        gcTime.UnixNano(),
					GcRecordsPageSize:          100,
					GcRecordSemaphoresPageSize: 100,
					GcRecordHoldersPageSize:    100,
					MaxVisited:                 2,
				},
			})
			require.NoError(t, err)
		}

		for _, lease := range leases {
			_, err := core.leases.Get(core.badgerStore.View(), lease.Id)
			require.ErrorIs(t, err, store.ErrNotFound)
		}
		postCounters, err := core.counters.Get(core.badgerStore.View(), accountId, namespaceId.NamespaceId)
		require.NoError(t, err)
		require.EqualValues(t, 0, postCounters.NumberOfLeases)
	})

	t.Run("tolerates stale expirationRecord pointing at a deleted semaphore", func(t *testing.T) {
		// Reproduces the poison-record case: an expirationRecord exists but the semaphore
		// it points to has been removed. The old sweep aborted with ErrNotFound on the
		// semaphores.Get and the same record poisoned every subsequent GC pass.
		core := newSemaphoresCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		ghostSemaphoreId := &corepb.SemaphoreId{
			AccountId:   accountId,
			NamespaceId: rand.Uint32(),
			SemaphoreId: rand.Uint64(),
		}
		staleAt := now.Add(-1 * time.Minute).UnixNano()

		// Inject a stale expirationRecord referencing a semaphore that does not exist.
		txn := core.badgerStore.Update()
		require.NoError(t, core.expirationRecords.Add(txn, staleAt, ghostSemaphoreId))
		require.NoError(t, txn.Commit())
		require.Equal(t, []int64{staleAt}, listExpirationRecords(t, core, ghostSemaphoreId))

		// Also create a live semaphore in a different namespace so we can verify the sweep
		// still processes valid work after stepping over the poison record.
		liveSemaphoreId := &corepb.SemaphoreId{
			AccountId:   accountId,
			NamespaceId: rand.Uint32(),
			SemaphoreId: rand.Uint64(),
		}
		liveNamespaceId := &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: liveSemaphoreId.NamespaceId,
		}
		_ = createSemaphore(t, core, liveSemaphoreId, "live", 1, now)
		lease := createLease(t, core, accountId, liveSemaphoreId.NamespaceId, "p", now, 1*time.Minute)
		success, _ := acquireSemaphore(t, core, liveNamespaceId, lease.Id, "live", 1, now)
		require.True(t, success)

		// T+2m: live semaphore's holder is expired. Run GC.
		resp, err := core.RunSemaphoresGarbageCollection(&coreapis.RunSemaphoresGarbageCollectionRequest{
			Payload: &corepb.RunSemaphoresGarbageCollectionRequest{
				Now:                        now.Add(2 * time.Minute).UnixNano(),
				GcRecordsPageSize:          100,
				GcRecordSemaphoresPageSize: 100,
				GcRecordHoldersPageSize:    100,
				MaxVisited:                 1000,
			},
		})
		require.NoError(t, err)
		require.Nil(t, resp.ApplicationError)

		// The poison row must be gone.
		require.Empty(t, listExpirationRecords(t, core, ghostSemaphoreId))

		// The live semaphore was processed in the same pass — its expired holder was pruned
		// and its expirationRecord cleared.
		require.Empty(t, listExpirationRecords(t, core, liveSemaphoreId))
		liveSem, err := core.semaphores.Get(core.badgerStore.View(), liveSemaphoreId)
		require.NoError(t, err)
		require.EqualValues(t, 0, liveSem.ActiveHoldersCount)
		require.EqualValues(t, 0, liveSem.EarliestHolderExpiresAt)
	})

	t.Run("removes duplicate expirationRecords for the same semaphore", func(t *testing.T) {
		// A duplicate expirationRecord at a timestamp different from
		// semaphore.EarliestHolderExpiresAt would survive the old sweep forever: the delete
		// targeted EarliestHolderExpiresAt, not the iterated record.
		core := newSemaphoresCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespaceId := &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: rand.Uint32(),
		}
		semaphoreId := &corepb.SemaphoreId{
			AccountId:   accountId,
			NamespaceId: namespaceId.NamespaceId,
			SemaphoreId: rand.Uint64(),
		}
		_ = createSemaphore(t, core, semaphoreId, "sema", 1, now)
		lease := createLease(t, core, accountId, namespaceId.NamespaceId, "p", now, 1*time.Minute)
		success, sem := acquireSemaphore(t, core, namespaceId, lease.Id, "sema", 1, now)
		require.True(t, success)
		canonical := sem.EarliestHolderExpiresAt
		require.NotZero(t, canonical)

		// Inject a stale duplicate at an earlier timestamp.
		staleAt := canonical - int64(30*time.Second)
		txn := core.badgerStore.Update()
		require.NoError(t, core.expirationRecords.Add(txn, staleAt, semaphoreId))
		require.NoError(t, txn.Commit())
		require.ElementsMatch(t, []int64{staleAt, canonical}, listExpirationRecords(t, core, semaphoreId))

		// Run GC at T+2m: both the stale duplicate and the canonical record are <= now.
		// The holder is also expired, so the pruned semaphore ends up with no record at all.
		resp, err := core.RunSemaphoresGarbageCollection(&coreapis.RunSemaphoresGarbageCollectionRequest{
			Payload: &corepb.RunSemaphoresGarbageCollectionRequest{
				Now:                        now.Add(2 * time.Minute).UnixNano(),
				GcRecordsPageSize:          100,
				GcRecordSemaphoresPageSize: 100,
				GcRecordHoldersPageSize:    100,
				MaxVisited:                 1000,
			},
		})
		require.NoError(t, err)
		require.Nil(t, resp.ApplicationError)

		require.Empty(t, listExpirationRecords(t, core, semaphoreId))
		stored, err := core.semaphores.Get(core.badgerStore.View(), semaphoreId)
		require.NoError(t, err)
		require.EqualValues(t, 0, stored.ActiveHoldersCount)
		require.EqualValues(t, 0, stored.EarliestHolderExpiresAt)
	})

	t.Run("removes stale expirationRecord when semaphore has no holders", func(t *testing.T) {
		// The old code skipped the delete entirely when semaphore.EarliestHolderExpiresAt
		// was zero, so a stale row would keep getting visited every GC pass forever.
		core := newSemaphoresCore(t)
		now := time.Now()
		semaphoreId := &corepb.SemaphoreId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
			SemaphoreId: rand.Uint64(),
		}
		_ = createSemaphore(t, core, semaphoreId, "sema", 1, now)

		// Inject a stale expirationRecord; the semaphore has no holders so its
		// EarliestHolderExpiresAt is zero.
		staleAt := now.Add(-1 * time.Minute).UnixNano()
		txn := core.badgerStore.Update()
		require.NoError(t, core.expirationRecords.Add(txn, staleAt, semaphoreId))
		require.NoError(t, txn.Commit())

		stored, err := core.semaphores.Get(core.badgerStore.View(), semaphoreId)
		require.NoError(t, err)
		require.EqualValues(t, 0, stored.EarliestHolderExpiresAt)
		require.Equal(t, []int64{staleAt}, listExpirationRecords(t, core, semaphoreId))

		resp, err := core.RunSemaphoresGarbageCollection(&coreapis.RunSemaphoresGarbageCollectionRequest{
			Payload: &corepb.RunSemaphoresGarbageCollectionRequest{
				Now:                        now.UnixNano(),
				GcRecordsPageSize:          100,
				GcRecordSemaphoresPageSize: 100,
				GcRecordHoldersPageSize:    100,
				MaxVisited:                 1000,
			},
		})
		require.NoError(t, err)
		require.Nil(t, resp.ApplicationError)

		require.Empty(t, listExpirationRecords(t, core, semaphoreId))

		// And a subsequent pass over an empty index is a no-op — proves the row is truly
		// gone, not just hidden behind the iterator's snapshot.
		resp, err = core.RunSemaphoresGarbageCollection(&coreapis.RunSemaphoresGarbageCollectionRequest{
			Payload: &corepb.RunSemaphoresGarbageCollectionRequest{
				Now:                        now.UnixNano(),
				GcRecordsPageSize:          100,
				GcRecordSemaphoresPageSize: 100,
				GcRecordHoldersPageSize:    100,
				MaxVisited:                 1000,
			},
		})
		require.NoError(t, err)
		require.Nil(t, resp.ApplicationError)
		require.Empty(t, listExpirationRecords(t, core, semaphoreId))
	})
}

func TestCore_CreateSemaphoreLease(t *testing.T) {
	t.Run("creates a lease", func(t *testing.T) {
		core := newSemaphoresCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		lease := createLeaseWithMax(t, core, accountId, namespaceId, "process-1", now, 30*time.Minute, 10)
		require.Equal(t, "process-1", lease.ProcessId)

		// Counters reflect the new lease.
		counters, err := core.counters.Get(core.badgerStore.View(), accountId, namespaceId)
		require.NoError(t, err)
		require.EqualValues(t, 1, counters.NumberOfLeases)
	})

	t.Run("max number of semaphore leases per namespace", func(t *testing.T) {
		core := newSemaphoresCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespaceId := rand.Uint32()
		const maxLeases = int64(3)

		// Create leases up to the limit using the same MaxNumberOfSemaphoreLeases throughout —
		// each call must succeed.
		for i := 0; i < int(maxLeases); i++ {
			_ = createLeaseWithMax(t, core, accountId, namespaceId, fmt.Sprintf("process_%d", i), now, 60*time.Second, maxLeases)
		}

		// The next attempt must be rejected with ResourceExhausted.
		appErr := createLeaseWithError(t, core, accountId, namespaceId, "process_over", now, 60*time.Second, maxLeases)
		require.Equal(t, monsterax.ResourceExhausted, appErr.Code)
		require.Contains(t, appErr.Message, "max number of semaphore leases per namespace reached")

		// Counter stayed at maxLeases — the failed call left no state behind.
		counters, err := core.counters.Get(core.badgerStore.View(), accountId, namespaceId)
		require.NoError(t, err)
		require.EqualValues(t, maxLeases, counters.NumberOfLeases)

		// The limit is per-namespace: a different namespace under the same account still accepts new leases.
		_ = createLeaseWithMax(t, core, accountId, rand.Uint32(), "process_other_ns", now, 60*time.Second, maxLeases)

		// And per-account: a different account is also unaffected.
		_ = createLeaseWithMax(t, core, rand.Uint64(), namespaceId, "process_other_account", now, 60*time.Second, maxLeases)
	})
}

func TestCore_GetSemaphoreLease(t *testing.T) {
	t.Run("returns the lease when still valid", func(t *testing.T) {
		core := newSemaphoresCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		lease := createLease(t, core, accountId, namespaceId, "process-1", now, 1*time.Minute)

		// T+30s: lease is still active
		resp, err := core.GetSemaphoreLease(&coreapis.GetSemaphoreLeaseRequest{
			Payload: &corepb.GetSemaphoreLeaseRequest{
				LeaseId: lease.Id,
				Now:     now.Add(30 * time.Second).UnixNano(),
			},
		})
		require.NoError(t, err)
		require.Nil(t, resp.ApplicationError)
		require.NotNil(t, resp.Payload)
		require.Equal(t, lease.ExpiresAt, resp.Payload.Lease.ExpiresAt)
	})

	t.Run("returns not found when the lease has expired by now", func(t *testing.T) {
		core := newSemaphoresCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		lease := createLease(t, core, accountId, namespaceId, "process-1", now, 1*time.Minute)

		// T+2m: lease has expired even though it's still in the store
		resp, err := core.GetSemaphoreLease(&coreapis.GetSemaphoreLeaseRequest{
			Payload: &corepb.GetSemaphoreLeaseRequest{
				LeaseId: lease.Id,
				Now:     now.Add(2 * time.Minute).UnixNano(),
			},
		})
		require.NoError(t, err)
		require.Nil(t, resp.Payload)
		require.NotNil(t, resp.ApplicationError)
		require.Equal(t, monsterax.NotFound, resp.ApplicationError.Code)

		// View txn must not mutate state — the expired lease is still in the store
		storedLease, err := core.leases.Get(core.badgerStore.View(), lease.Id)
		require.NoError(t, err)
		require.Equal(t, lease.ExpiresAt, storedLease.ExpiresAt)
	})

	t.Run("returns not found when the lease does not exist", func(t *testing.T) {
		core := newSemaphoresCore(t)
		now := time.Now()

		resp, err := core.GetSemaphoreLease(&coreapis.GetSemaphoreLeaseRequest{
			Payload: &corepb.GetSemaphoreLeaseRequest{
				LeaseId: &corepb.LeaseId{
					AccountId:   rand.Uint64(),
					NamespaceId: rand.Uint32(),
					LeaseId:     rand.Uint64(),
				},
				Now: now.UnixNano(),
			},
		})
		require.NoError(t, err)
		require.Nil(t, resp.Payload)
		require.NotNil(t, resp.ApplicationError)
		require.Equal(t, monsterax.NotFound, resp.ApplicationError.Code)
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
				Now:     now.UnixNano(),
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

	t.Run("returns not found when the lease does not exist", func(t *testing.T) {
		core := newSemaphoresCore(t)
		now := time.Now()

		resp, err := core.RevokeSemaphoreLease(&coreapis.RevokeSemaphoreLeaseRequest{
			Payload: &corepb.RevokeSemaphoreLeaseRequest{
				LeaseId: &corepb.LeaseId{
					AccountId:   rand.Uint64(),
					NamespaceId: rand.Uint32(),
					LeaseId:     rand.Uint64(),
				},
				Now: now.UnixNano(),
			},
		})
		require.NoError(t, err)
		require.Nil(t, resp.Payload)
		require.NotNil(t, resp.ApplicationError)
		require.Equal(t, monsterax.NotFound, resp.ApplicationError.Code)
	})

	t.Run("expirationRecords advances when the revoked lease held the earliest position", func(t *testing.T) {
		core := newSemaphoresCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespaceId := &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: rand.Uint32(),
		}
		semaphoreId := &corepb.SemaphoreId{
			AccountId:   accountId,
			NamespaceId: namespaceId.NamespaceId,
			SemaphoreId: rand.Uint64(),
		}
		_ = createSemaphore(t, core, semaphoreId, "sema", 5, now)

		// Two holders; the earliest is the short lease.
		shortLease := createLease(t, core, accountId, namespaceId.NamespaceId, "p_short", now, 1*time.Minute)
		success, _ := acquireSemaphore(t, core, namespaceId, shortLease.Id, "sema", 1, now)
		require.True(t, success)

		longLease := createLease(t, core, accountId, namespaceId.NamespaceId, "p_long", now, 1*time.Hour)
		success, _ = acquireSemaphore(t, core, namespaceId, longLease.Id, "sema", 1, now)
		require.True(t, success)

		require.Equal(t, []int64{shortLease.ExpiresAt}, listExpirationRecords(t, core, semaphoreId))

		// Revoke the short (earliest) lease. The index entry must move from the short
		// lease's expiration to the long one's.
		resp, err := core.RevokeSemaphoreLease(&coreapis.RevokeSemaphoreLeaseRequest{
			Payload: &corepb.RevokeSemaphoreLeaseRequest{
				LeaseId: shortLease.Id,
				Now:     now.UnixNano(),
			},
		})
		require.NoError(t, err)
		require.Nil(t, resp.ApplicationError)

		require.Equal(t, []int64{longLease.ExpiresAt}, listExpirationRecords(t, core, semaphoreId))

		stored, err := core.semaphores.Get(core.badgerStore.View(), semaphoreId)
		require.NoError(t, err)
		require.Equal(t, longLease.ExpiresAt, stored.EarliestHolderExpiresAt)
	})

	t.Run("expirationRecords unchanged when the revoked lease held a non-earliest position", func(t *testing.T) {
		// Historically the cleanup keyed off the removed holder's ExpiresAt rather than
		// the semaphore's prior earliest. In this scenario the old code would issue a
		// Delete that silently missed (key didn't exist) followed by an idempotent Add at
		// the unchanged earliest. With the captured-oldEarliest pattern the no-op path is
		// explicit: when oldEarliest == newEarliest no index work is done at all.
		core := newSemaphoresCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespaceId := &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: rand.Uint32(),
		}
		semaphoreId := &corepb.SemaphoreId{
			AccountId:   accountId,
			NamespaceId: namespaceId.NamespaceId,
			SemaphoreId: rand.Uint64(),
		}
		_ = createSemaphore(t, core, semaphoreId, "sema", 5, now)

		shortLease := createLease(t, core, accountId, namespaceId.NamespaceId, "p_short", now, 1*time.Minute)
		success, _ := acquireSemaphore(t, core, namespaceId, shortLease.Id, "sema", 1, now)
		require.True(t, success)

		longLease := createLease(t, core, accountId, namespaceId.NamespaceId, "p_long", now, 1*time.Hour)
		success, _ = acquireSemaphore(t, core, namespaceId, longLease.Id, "sema", 1, now)
		require.True(t, success)

		require.Equal(t, []int64{shortLease.ExpiresAt}, listExpirationRecords(t, core, semaphoreId))

		// Revoke the long (non-earliest) lease. The index entry must remain at the
		// short lease's expiration.
		resp, err := core.RevokeSemaphoreLease(&coreapis.RevokeSemaphoreLeaseRequest{
			Payload: &corepb.RevokeSemaphoreLeaseRequest{
				LeaseId: longLease.Id,
				Now:     now.UnixNano(),
			},
		})
		require.NoError(t, err)
		require.Nil(t, resp.ApplicationError)

		require.Equal(t, []int64{shortLease.ExpiresAt}, listExpirationRecords(t, core, semaphoreId))

		stored, err := core.semaphores.Get(core.badgerStore.View(), semaphoreId)
		require.NoError(t, err)
		require.Equal(t, shortLease.ExpiresAt, stored.EarliestHolderExpiresAt)
	})

	t.Run("expirationRecords is cleared when the revoked lease held the only holder", func(t *testing.T) {
		core := newSemaphoresCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespaceId := &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: rand.Uint32(),
		}
		semaphoreId := &corepb.SemaphoreId{
			AccountId:   accountId,
			NamespaceId: namespaceId.NamespaceId,
			SemaphoreId: rand.Uint64(),
		}
		_ = createSemaphore(t, core, semaphoreId, "sema", 5, now)

		lease := createLease(t, core, accountId, namespaceId.NamespaceId, "p", now, 1*time.Hour)
		success, _ := acquireSemaphore(t, core, namespaceId, lease.Id, "sema", 1, now)
		require.True(t, success)

		require.Equal(t, []int64{lease.ExpiresAt}, listExpirationRecords(t, core, semaphoreId))

		resp, err := core.RevokeSemaphoreLease(&coreapis.RevokeSemaphoreLeaseRequest{
			Payload: &corepb.RevokeSemaphoreLeaseRequest{
				LeaseId: lease.Id,
				Now:     now.UnixNano(),
			},
		})
		require.NoError(t, err)
		require.Nil(t, resp.ApplicationError)

		require.Empty(t, listExpirationRecords(t, core, semaphoreId))

		stored, err := core.semaphores.Get(core.badgerStore.View(), semaphoreId)
		require.NoError(t, err)
		require.EqualValues(t, 0, stored.EarliestHolderExpiresAt)
	})
}

func TestCore_RefreshSemaphoreLease(t *testing.T) {
	t.Run("nonexistent lease", func(t *testing.T) {
		core := newSemaphoresCore(t)
		now := time.Now()

		// Use a lease id that was never created.
		fakeLease := &corepb.LeaseId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
			LeaseId:     rand.Uint64(),
		}

		appErr := refreshSemaphoreLeaseWithError(t, core, fakeLease, 60, now)
		require.Equal(t, monsterax.NotFound, appErr.Code)
		require.Contains(t, appErr.Message, "lease not found")
	})

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
				Now:     futureTime.UnixNano(),
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
				Now:     futureTime.UnixNano(),
			},
		})
		require.NoError(t, err)
		require.NotNil(t, resp2)
		require.Nil(t, resp2.ApplicationError)
		require.NotNil(t, resp2.Payload)
		require.Equal(t, resp1.Payload.Lease.ExpiresAt, resp2.Payload.Lease.ExpiresAt)
	})

	t.Run("propagates new expiration to holders", func(t *testing.T) {
		core := newSemaphoresCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespaceId := &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: rand.Uint32(),
		}

		// Lease that will be refreshed
		lease := createLease(t, core, accountId, namespaceId.NamespaceId, "process-1", now, 1*time.Minute)
		originalLeaseExpiresAt := lease.ExpiresAt

		// A second lease that will outlive `lease` initially but will be overtaken once
		// `lease` is refreshed — used to verify EarliestHolderExpiresAt is recomputed.
		otherLease := createLease(t, core, accountId, namespaceId.NamespaceId, "process-2", now, 2*time.Minute)

		// Two semaphores: one held only by `lease`, one shared with `otherLease`
		soloSemaphoreId := &corepb.SemaphoreId{AccountId: accountId, NamespaceId: namespaceId.NamespaceId, SemaphoreId: rand.Uint64()}
		_ = createSemaphore(t, core, soloSemaphoreId, "solo", 5, now)
		success, _ := acquireSemaphore(t, core, namespaceId, lease.Id, "solo", 1, now)
		require.True(t, success)

		sharedSemaphoreId := &corepb.SemaphoreId{AccountId: accountId, NamespaceId: namespaceId.NamespaceId, SemaphoreId: rand.Uint64()}
		_ = createSemaphore(t, core, sharedSemaphoreId, "shared", 5, now)
		success, _ = acquireSemaphore(t, core, namespaceId, lease.Id, "shared", 1, now)
		require.True(t, success)
		success, sharedSem := acquireSemaphore(t, core, namespaceId, otherLease.Id, "shared", 1, now)
		require.True(t, success)
		// The shorter-lived lease drives EarliestHolderExpiresAt before the refresh
		require.Equal(t, originalLeaseExpiresAt, sharedSem.EarliestHolderExpiresAt)

		// T+30s: refresh `lease` to a 5 minute TTL — new expiration is later than `otherLease`'s
		refreshAt := now.Add(30 * time.Second)
		resp, err := core.RefreshSemaphoreLease(&coreapis.RefreshSemaphoreLeaseRequest{
			Payload: &corepb.RefreshSemaphoreLeaseRequest{
				LeaseId:    lease.Id,
				TtlSeconds: 300,
				Now:        refreshAt.UnixNano(),
			},
		})
		require.NoError(t, err)
		require.Nil(t, resp.ApplicationError)
		newLeaseExpiresAt := resp.Payload.Lease.ExpiresAt
		require.Greater(t, newLeaseExpiresAt, originalLeaseExpiresAt)

		// Holder on the solo semaphore now tracks the refreshed lease
		soloHolders, err := core.ListSemaphoreHolders(&coreapis.ListSemaphoreHoldersRequest{
			Payload: &corepb.ListSemaphoreHoldersRequest{
				NamespaceId:   namespaceId,
				SemaphoreName: "solo",
				Now:           refreshAt.UnixNano(),
			},
		})
		require.NoError(t, err)
		require.Len(t, soloHolders.Payload.Holders, 1)
		require.Equal(t, newLeaseExpiresAt, soloHolders.Payload.Holders[0].ExpiresAt)

		// Solo semaphore's earliest holder expiration follows
		soloStored, err := core.semaphores.Get(core.badgerStore.View(), soloSemaphoreId)
		require.NoError(t, err)
		require.Equal(t, newLeaseExpiresAt, soloStored.EarliestHolderExpiresAt)

		// On the shared semaphore the refreshed holder is no longer the earliest; `otherLease` wins
		sharedHolders, err := core.ListSemaphoreHolders(&coreapis.ListSemaphoreHoldersRequest{
			Payload: &corepb.ListSemaphoreHoldersRequest{
				NamespaceId:   namespaceId,
				SemaphoreName: "shared",
				Now:           refreshAt.UnixNano(),
			},
		})
		require.NoError(t, err)
		require.Len(t, sharedHolders.Payload.Holders, 2)
		holdersByLease := lo.KeyBy(sharedHolders.Payload.Holders, func(h *corepb.SemaphoreHolder) uint64 {
			return h.Id.LeaseId
		})
		require.Equal(t, newLeaseExpiresAt, holdersByLease[lease.Id.LeaseId].ExpiresAt)
		require.Equal(t, otherLease.ExpiresAt, holdersByLease[otherLease.Id.LeaseId].ExpiresAt)

		sharedStored, err := core.semaphores.Get(core.badgerStore.View(), sharedSemaphoreId)
		require.NoError(t, err)
		require.Equal(t, otherLease.ExpiresAt, sharedStored.EarliestHolderExpiresAt)
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
				Now:     now.UnixNano(),
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
				Now:     now.UnixNano(),
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
				Now:     now.UnixNano(),
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
				Now:     now.UnixNano(),
			},
		})
		require.NoError(t, err)
		require.NotNil(t, resp6)
		require.Nil(t, resp6.ApplicationError)
		require.NotNil(t, resp6.Payload)
		require.Len(t, resp6.Payload.Semaphores, 1)
		require.Equal(t, semaphoreId.SemaphoreId, resp6.Payload.Semaphores[0].Id.SemaphoreId)
	})

	t.Run("returns counters with expired holders filtered out", func(t *testing.T) {
		core := newSemaphoresCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespaceId := &corepb.NamespaceId{
			AccountId:   accountId,
			NamespaceId: rand.Uint32(),
		}
		semaphoreId := &corepb.SemaphoreId{
			AccountId:   accountId,
			NamespaceId: namespaceId.NamespaceId,
			SemaphoreId: rand.Uint64(),
		}

		_ = createSemaphore(t, core, semaphoreId, "test_semaphore", 5, now)

		// The lease we'll list by — stays valid past the listing time.
		longLease := createLease(t, core, accountId, namespaceId.NamespaceId, "process_long", now, 60*time.Minute)
		// A second short-lived lease that also acquires the same semaphore.
		shortLease := createLease(t, core, accountId, namespaceId.NamespaceId, "process_short", now, 1*time.Minute)

		success, _ := acquireSemaphore(t, core, namespaceId, longLease.Id, "test_semaphore", 1, now)
		require.True(t, success)
		success, _ = acquireSemaphore(t, core, namespaceId, shortLease.Id, "test_semaphore", 2, now)
		require.True(t, success)

		// T+2m: the short lease's holder has expired
		listAt := now.Add(2 * time.Minute)
		resp, err := core.ListSemaphoresByLeaseId(&coreapis.ListSemaphoresByLeaseIdRequest{
			Payload: &corepb.ListSemaphoresByLeaseIdRequest{
				LeaseId: longLease.Id,
				Now:     listAt.UnixNano(),
			},
		})
		require.NoError(t, err)
		require.Nil(t, resp.ApplicationError)
		require.Len(t, resp.Payload.Semaphores, 1)

		listed := resp.Payload.Semaphores[0]
		require.EqualValues(t, 1, listed.ActiveHoldersCount)
		require.EqualValues(t, 1, listed.ActiveHolds)
		require.EqualValues(t, longLease.ExpiresAt, listed.EarliestHolderExpiresAt)

		// View txn must not mutate state — stored counters are still the pre-expiration values
		stored, err := core.semaphores.Get(core.badgerStore.View(), semaphoreId)
		require.NoError(t, err)
		require.EqualValues(t, 2, stored.ActiveHoldersCount)
		require.EqualValues(t, 3, stored.ActiveHolds)
	})
}

func TestCore_LastActivityAt(t *testing.T) {
	t.Run("create sets it, acquire/release update it, update does not", func(t *testing.T) {
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

		// Create records the creation time as the initial activity.
		semaphore := createSemaphore(t, core, semaphoreId, "test_semaphore", 5, now)
		require.Equal(t, now.UnixNano(), semaphore.LastActivityAt)

		// Update* must NOT touch last_activity_at.
		updated := updateSemaphore(t, core, namespaceId, "test_semaphore", "new description", 5, 1, now.Add(time.Minute))
		require.Equal(t, now.UnixNano(), updated.LastActivityAt)

		lease := createLease(t, core, namespaceId.AccountId, namespaceId.NamespaceId, "process-1", now, time.Hour)

		// Acquiring updates the timestamp.
		acquireTime := now.Add(2 * time.Minute)
		ok, acquired := acquireSemaphore(t, core, namespaceId, lease.Id, "test_semaphore", 1, acquireTime)
		require.True(t, ok)
		require.Equal(t, acquireTime.UnixNano(), acquired.LastActivityAt)

		// Releasing updates the timestamp.
		releaseTime := now.Add(3 * time.Minute)
		released := releaseSemaphore(t, core, namespaceId, "test_semaphore", lease.Id, releaseTime)
		require.Equal(t, releaseTime.UnixNano(), released.LastActivityAt)
	})
}

func newSemaphoresCore(t *testing.T) *Core {
	t.Helper()

	store, err := store.NewBadgerInMemoryStore()
	require.NoError(t, err)
	return NewCore(store, []byte{0x1d, 0x36, 0x00, 0x00}, []byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})
}

// listExpirationRecords returns the expiration timestamps of every expirationRecords row
// that targets the given semaphore. The order matches the table's natural ordering
// (ascending by timestamp). Used by tests to detect stale or duplicated index entries.
func listExpirationRecords(t *testing.T, core *Core, semaphoreId *corepb.SemaphoreId) []int64 {
	t.Helper()

	txn := core.badgerStore.View()
	defer txn.Discard()

	var times []int64
	err := core.expirationRecords.List(txn, 0, math.MaxInt64, func(record *corepb.SemaphoresExpirationRecord) (bool, error) {
		if record.SemaphoreId.AccountId == semaphoreId.AccountId &&
			record.SemaphoreId.NamespaceId == semaphoreId.NamespaceId &&
			record.SemaphoreId.SemaphoreId == semaphoreId.SemaphoreId {
			times = append(times, record.ExpiresAt)
		}
		return true, nil
	})
	require.NoError(t, err)
	return times
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

func listSemaphoreHolders(t *testing.T, core *Core, namespaceId *corepb.NamespaceId, semaphoreName string, now time.Time) *corepb.ListSemaphoreHoldersResponse {
	t.Helper()

	resp, err := core.ListSemaphoreHolders(&coreapis.ListSemaphoreHoldersRequest{
		Payload: &corepb.ListSemaphoreHoldersRequest{
			NamespaceId:   namespaceId,
			SemaphoreName: semaphoreName,
			Now:           now.UnixNano(),
			Limit:         100,
		},
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Nil(t, resp.ApplicationError)
	require.NotNil(t, resp.Payload)

	return resp.Payload
}

func updateSemaphore(t *testing.T, core *Core, namespaceId *corepb.NamespaceId, semaphoreName string, description string, permits uint64, version uint64, now time.Time) *corepb.Semaphore {
	t.Helper()

	resp, err := core.UpdateSemaphore(&coreapis.UpdateSemaphoreRequest{
		Payload: &corepb.UpdateSemaphoreRequest{
			NamespaceId:     namespaceId,
			SemaphoreName:   semaphoreName,
			Description:     description,
			Permits:         permits,
			Now:             now.UnixNano(),
			ExpectedVersion: version,
		},
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Nil(t, resp.ApplicationError)
	require.NotNil(t, resp.Payload)
	require.NotNil(t, resp.Payload.Semaphore)

	return resp.Payload.Semaphore
}

func updateSemaphoreWithError(t *testing.T, core *Core, namespaceId *corepb.NamespaceId, semaphoreName string, description string, permits uint64, version uint64, now time.Time) *monsterax.Error {
	t.Helper()

	resp, err := core.UpdateSemaphore(&coreapis.UpdateSemaphoreRequest{
		Payload: &corepb.UpdateSemaphoreRequest{
			NamespaceId:     namespaceId,
			SemaphoreName:   semaphoreName,
			Description:     description,
			Permits:         permits,
			Now:             now.UnixNano(),
			ExpectedVersion: version,
		},
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Nil(t, resp.Payload)
	require.NotNil(t, resp.ApplicationError)

	return resp.ApplicationError
}

func createSemaphoreWithMax(t *testing.T, core *Core, semaphoreId *corepb.SemaphoreId, semaphoreName string, permits uint64, maxNumberOfSemaphoresPerNamespace int64, now time.Time) *corepb.Semaphore {
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
	require.Nil(t, resp.ApplicationError)
	require.NotNil(t, resp.Payload)
	require.NotNil(t, resp.Payload.Semaphore)
	return resp.Payload.Semaphore
}

func createLeaseWithMax(t *testing.T, core *Core, accountId uint64, namespaceId uint32, processId string, now time.Time, ttl time.Duration, maxNumberOfSemaphoreLeases int64) *corepb.Lease {
	t.Helper()

	resp, err := core.CreateSemaphoreLease(&coreapis.CreateSemaphoreLeaseRequest{
		Payload: &corepb.CreateSemaphoreLeaseRequest{
			LeaseId: &corepb.LeaseId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
				LeaseId:     rand.Uint64(),
			},
			ProcessId:                  processId,
			TtlSeconds:                 uint64(ttl.Seconds()),
			Now:                        now.UnixNano(),
			MaxNumberOfSemaphoreLeases: maxNumberOfSemaphoreLeases,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Nil(t, resp.ApplicationError)
	require.NotNil(t, resp.Payload)
	require.NotNil(t, resp.Payload.Lease)
	require.EqualValues(t, now.UnixNano(), resp.Payload.Lease.CreatedAt)
	require.EqualValues(t, now.Add(ttl).UnixNano(), resp.Payload.Lease.ExpiresAt)
	return resp.Payload.Lease
}

func createLeaseWithError(t *testing.T, core *Core, accountId uint64, namespaceId uint32, processId string, now time.Time, ttl time.Duration, maxNumberOfSemaphoreLeases int64) *monsterax.Error {
	t.Helper()

	resp, err := core.CreateSemaphoreLease(&coreapis.CreateSemaphoreLeaseRequest{
		Payload: &corepb.CreateSemaphoreLeaseRequest{
			LeaseId: &corepb.LeaseId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
				LeaseId:     rand.Uint64(),
			},
			ProcessId:                  processId,
			TtlSeconds:                 uint64(ttl.Seconds()),
			Now:                        now.UnixNano(),
			MaxNumberOfSemaphoreLeases: maxNumberOfSemaphoreLeases,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Nil(t, resp.Payload)
	require.NotNil(t, resp.ApplicationError)
	return resp.ApplicationError
}

func refreshSemaphoreLeaseWithError(t *testing.T, core *Core, leaseId *corepb.LeaseId, ttlSeconds uint64, now time.Time) *monsterax.Error {
	t.Helper()

	resp, err := core.RefreshSemaphoreLease(&coreapis.RefreshSemaphoreLeaseRequest{
		Payload: &corepb.RefreshSemaphoreLeaseRequest{
			LeaseId:    leaseId,
			TtlSeconds: ttlSeconds,
			Now:        now.UnixNano(),
		},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Nil(t, resp.Payload)
	require.NotNil(t, resp.ApplicationError)
	return resp.ApplicationError
}
