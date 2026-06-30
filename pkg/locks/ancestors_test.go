package locks

import (
	"bytes"
	"io"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/evrblk/grackle/pkg/coreapis"
	"github.com/evrblk/grackle/pkg/corepb"
)

func TestAncestors_ExclusiveLock(t *testing.T) {
	core := newLocksCore(t)
	now := time.Now()

	lockId := &corepb.LockId{
		AccountId:   rand.Uint64(),
		NamespaceId: rand.Uint64(),
		LockName:    "a/b/c",
	}

	// Create a lease
	lease := createLease(t, core, lockId.AccountId, lockId.NamespaceId, "proc1", now, time.Hour)

	// Acquire exclusive lock on a/b/c
	success, _ := acquireLock(t, core, lockId, lease.Id, true, now)
	require.True(t, success)

	// Ancestors "a" and "a/b" should have exclusive_count=1
	anc_a := getAncestor(t, core, ancestorId(lockId, "a"))
	require.EqualValues(t, 1, anc_a.ExclusiveCount)
	require.EqualValues(t, 0, anc_a.SharedCount)

	anc_ab := getAncestor(t, core, ancestorId(lockId, "a/b"))
	require.EqualValues(t, 1, anc_ab.ExclusiveCount)
	require.EqualValues(t, 0, anc_ab.SharedCount)

	// Release the lock
	_ = releaseLock(t, core, lockId, lease.Id, now.Add(time.Minute))

	// Ancestors should be gone (count=0)
	anc_a = getAncestor(t, core, ancestorId(lockId, "a"))
	require.EqualValues(t, 0, anc_a.ExclusiveCount)
	require.EqualValues(t, 0, anc_a.SharedCount)

	anc_ab = getAncestor(t, core, ancestorId(lockId, "a/b"))
	require.EqualValues(t, 0, anc_ab.ExclusiveCount)
	require.EqualValues(t, 0, anc_ab.SharedCount)
}

func TestAncestors_SharedLock(t *testing.T) {
	core := newLocksCore(t)
	now := time.Now()

	lockId := &corepb.LockId{
		AccountId:   rand.Uint64(),
		NamespaceId: rand.Uint64(),
		LockName:    "a/b/c",
	}

	// Create leases for two processes
	lease1 := createLease(t, core, lockId.AccountId, lockId.NamespaceId, "proc1", now, time.Hour)
	lease2 := createLease(t, core, lockId.AccountId, lockId.NamespaceId, "proc2", now, time.Hour)

	// Two processes acquire shared lock on a/b/c
	success, _ := acquireLock(t, core, lockId, lease1.Id, false, now)
	require.True(t, success)

	success, _ = acquireLock(t, core, lockId, lease2.Id, false, now)
	require.True(t, success)

	// Ancestor a/b has shared_count=1 (one lock record, two holders)
	anc_ab := getAncestor(t, core, ancestorId(lockId, "a/b"))
	require.EqualValues(t, 0, anc_ab.ExclusiveCount)
	require.EqualValues(t, 1, anc_ab.SharedCount)

	// Release proc1 — lock still held by proc2, ancestors unchanged
	_ = releaseLock(t, core, lockId, lease1.Id, now.Add(time.Minute))

	anc_ab = getAncestor(t, core, ancestorId(lockId, "a/b"))
	require.EqualValues(t, 1, anc_ab.SharedCount) // still held by proc2

	// Release proc2 — lock now fully released
	_ = releaseLock(t, core, lockId, lease2.Id, now.Add(2*time.Minute))

	anc_ab = getAncestor(t, core, ancestorId(lockId, "a/b"))
	require.EqualValues(t, 0, anc_ab.SharedCount)
	require.EqualValues(t, 0, anc_ab.ExclusiveCount)
}

func TestAncestors_MultipleLocksSameAncestor(t *testing.T) {
	core := newLocksCore(t)
	now := time.Now()

	accountId := rand.Uint64()
	namespaceId := rand.Uint64()

	lockId1 := &corepb.LockId{AccountId: accountId, NamespaceId: namespaceId, LockName: "a/b/c"}
	lockId2 := &corepb.LockId{AccountId: accountId, NamespaceId: namespaceId, LockName: "a/b/d"}
	lockId3 := &corepb.LockId{AccountId: accountId, NamespaceId: namespaceId, LockName: "a/x"}

	// Create leases for three processes
	lease1 := createLease(t, core, accountId, namespaceId, "proc1", now, time.Hour)
	lease2 := createLease(t, core, accountId, namespaceId, "proc2", now, time.Hour)
	lease3 := createLease(t, core, accountId, namespaceId, "proc3", now, time.Hour)

	// Acquire a/b/c exclusive
	success, _ := acquireLock(t, core, lockId1, lease1.Id, true, now)
	require.True(t, success)

	// Acquire a/b/d shared
	success, _ = acquireLock(t, core, lockId2, lease2.Id, false, now)
	require.True(t, success)

	// Acquire a/x exclusive
	success, _ = acquireLock(t, core, lockId3, lease3.Id, true, now)
	require.True(t, success)

	// Ancestor "a": exclusive_count=2 (a/b/c and a/x), shared_count=1 (a/b/d)
	anc_a := getAncestor(t, core, ancestorId(lockId1, "a"))
	require.EqualValues(t, 2, anc_a.ExclusiveCount)
	require.EqualValues(t, 1, anc_a.SharedCount)

	// Ancestor "a/b": exclusive_count=1 (a/b/c), shared_count=1 (a/b/d)
	anc_ab := getAncestor(t, core, ancestorId(lockId1, "a/b"))
	require.EqualValues(t, 1, anc_ab.ExclusiveCount)
	require.EqualValues(t, 1, anc_ab.SharedCount)

	// Delete a/b/c
	resp1, err := core.DeleteLock(&coreapis.DeleteLockRequest{
		Payload: &corepb.DeleteLockRequest{
			LockId: lockId1,
		},
	},
	)
	require.NoError(t, err)
	require.NotNil(t, resp1)
	require.NotNil(t, resp1.Payload)

	// Ancestor "a": exclusive_count=1 (a/x), shared_count=1 (a/b/d)
	anc_a = getAncestor(t, core, ancestorId(lockId1, "a"))
	require.EqualValues(t, 1, anc_a.ExclusiveCount)
	require.EqualValues(t, 1, anc_a.SharedCount)

	// Ancestor "a/b": exclusive_count=0, shared_count=1 (a/b/d)
	anc_ab = getAncestor(t, core, ancestorId(lockId1, "a/b"))
	require.EqualValues(t, 0, anc_ab.ExclusiveCount)
	require.EqualValues(t, 1, anc_ab.SharedCount)
}

func TestAncestors_FlatLockNoAncestors(t *testing.T) {
	core := newLocksCore(t)
	now := time.Now()

	lockId := &corepb.LockId{
		AccountId:   rand.Uint64(),
		NamespaceId: rand.Uint64(),
		LockName:    "flat_lock",
	}

	// Create a lease
	lease := createLease(t, core, lockId.AccountId, lockId.NamespaceId, "proc1", now, time.Hour)

	success, _ := acquireLock(t, core, lockId, lease.Id, true, now)
	require.True(t, success)

	// No ancestors exist for flat lock names
	txn := core.badgerStore.View()
	defer txn.Discard()
	// List all entries in ancestors table — should be empty for this namespace
	result, err := core.ancestors.table.ListPaginated(txn,
		core.ancestors.tablePK(lockId.AccountId, lockId.NamespaceId), nil, 100)
	require.NoError(t, err)
	require.Empty(t, result.Items)
}

func TestAncestors_ExpirationCleansUpAncestors(t *testing.T) {
	core := newLocksCore(t)
	now := time.Now()

	lockId := &corepb.LockId{
		AccountId:   rand.Uint64(),
		NamespaceId: rand.Uint64(),
		LockName:    "a/b/c",
	}

	// Create a lease with short expiry
	lease := createLease(t, core, lockId.AccountId, lockId.NamespaceId, "proc1", now, time.Minute)

	// Acquire with short expiry
	success, _ := acquireLock(t, core, lockId, lease.Id, true, now)
	require.True(t, success)

	anc_a := getAncestor(t, core, ancestorId(lockId, "a"))
	require.EqualValues(t, 1, anc_a.ExclusiveCount)

	// GetLock after expiry reports the lock as unlocked (expired holders are filtered out)...
	lock := getLock(t, core, lockId, now.Add(2*time.Minute))
	require.Equal(t, corepb.LockState_LOCK_STATE_UNLOCKED, lock.State)

	// ...but GetLock is read-only, so the ancestor counters are not cleaned up by it.
	// That cleanup is the GC's responsibility (see TestAncestors_GarbageCollectionCleansUpAncestors).
	anc_a = getAncestor(t, core, ancestorId(lockId, "a"))
	require.EqualValues(t, 1, anc_a.ExclusiveCount)

	// Run garbage collection to clean up the expired lock and its ancestors.
	gcResponse, err := core.RunLocksGarbageCollection(&coreapis.RunLocksGarbageCollectionRequest{
		Payload: &corepb.RunLocksGarbageCollectionRequest{
			GcRecordsPageSize:     100,
			GcRecordLocksPageSize: 100,
			MaxVisitedLocks:       100,
		},
		Now: now.Add(2 * time.Minute).UnixNano(),
	})
	require.NoError(t, err)
	require.NotNil(t, gcResponse)

	anc_a = getAncestor(t, core, ancestorId(lockId, "a"))
	require.EqualValues(t, 0, anc_a.ExclusiveCount)
	require.EqualValues(t, 0, anc_a.SharedCount)
}

func TestAncestors_GarbageCollectionCleansUpAncestors(t *testing.T) {
	core := newLocksCore(t)
	now := time.Now()

	lockId := &corepb.LockId{
		AccountId:   rand.Uint64(),
		NamespaceId: rand.Uint64(),
		LockName:    "x/y/z",
	}

	// Create a lease with short expiry
	lease := createLease(t, core, lockId.AccountId, lockId.NamespaceId, "proc1", now, time.Minute)

	// Acquire with short expiry
	success, _ := acquireLock(t, core, lockId, lease.Id, false, now)
	require.True(t, success)

	anc_x := getAncestor(t, core, ancestorId(lockId, "x"))
	require.EqualValues(t, 1, anc_x.SharedCount)

	// Run GC after expiry
	resp1, err := core.RunLocksGarbageCollection(&coreapis.RunLocksGarbageCollectionRequest{
		Payload: &corepb.RunLocksGarbageCollectionRequest{
			GcRecordsPageSize:     100,
			GcRecordLocksPageSize: 100,
			MaxVisitedLocks:       100,
		},
		Now: now.Add(2 * time.Minute).UnixNano(),
	})
	require.NoError(t, err)
	require.NotNil(t, resp1)
	require.NotNil(t, resp1.Payload)

	// Ancestors cleaned up
	anc_x = getAncestor(t, core, ancestorId(lockId, "x"))
	require.EqualValues(t, 0, anc_x.SharedCount)
	require.EqualValues(t, 0, anc_x.ExclusiveCount)
}

func TestAncestors_NamespaceGCCleansUpAncestors(t *testing.T) {
	core := newLocksCore(t)
	now := time.Now()
	namespaceId := &corepb.NamespaceId{
		AccountId:   rand.Uint64(),
		NamespaceId: rand.Uint64(),
	}
	lockId := &corepb.LockId{
		AccountId:   namespaceId.AccountId,
		NamespaceId: namespaceId.NamespaceId,
		LockName:    "a/b/c",
	}

	// Create a lease
	lease := createLease(t, core, namespaceId.AccountId, namespaceId.NamespaceId, "proc1", now, time.Hour)

	// Acquire lock
	success, _ := acquireLock(t, core, lockId, lease.Id, true, now)
	require.True(t, success)

	anc_a := getAncestor(t, core, ancestorId(lockId, "a"))
	require.EqualValues(t, 1, anc_a.ExclusiveCount)

	// Schedule namespace for deletion
	resp1, err := core.LocksDeleteNamespace(&coreapis.LocksDeleteNamespaceRequest{
		Payload: &corepb.LocksDeleteNamespaceRequest{
			NamespaceId: namespaceId,
			RecordId:    1,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, resp1)
	require.NotNil(t, resp1.Payload)

	// Run GC to delete the namespace's locks
	resp2, err := core.RunLocksGarbageCollection(&coreapis.RunLocksGarbageCollectionRequest{
		Payload: &corepb.RunLocksGarbageCollectionRequest{
			GcRecordsPageSize:     100,
			GcRecordLocksPageSize: 100,
			MaxVisitedLocks:       100,
		},
		Now: now.Add(time.Minute).UnixNano(),
	})
	require.NoError(t, err)
	require.NotNil(t, resp2)
	require.NotNil(t, resp2.Payload)

	// Ancestors should be cleaned up
	anc_a = getAncestor(t, core, ancestorId(lockId, "a"))
	require.EqualValues(t, 0, anc_a.ExclusiveCount)
	require.EqualValues(t, 0, anc_a.SharedCount)
}

func TestAncestors_ReacquireExpiredWithDifferentMode(t *testing.T) {
	core := newLocksCore(t)
	now := time.Now()

	lockId := &corepb.LockId{
		AccountId:   rand.Uint64(),
		NamespaceId: rand.Uint64(),
		LockName:    "a/b/c",
	}

	// Create leases for two processes
	lease1 := createLease(t, core, lockId.AccountId, lockId.NamespaceId, "proc1", now, time.Minute)
	lease2 := createLease(t, core, lockId.AccountId, lockId.NamespaceId, "proc2", now.Add(2*time.Minute), time.Minute)

	// Acquire shared lock
	success, _ := acquireLock(t, core, lockId, lease1.Id, false, now)
	require.True(t, success)

	anc_a := getAncestor(t, core, ancestorId(lockId, "a"))
	require.EqualValues(t, 0, anc_a.ExclusiveCount)
	require.EqualValues(t, 1, anc_a.SharedCount)

	// Re-acquire as exclusive after expiry (without GC running first)
	success, _ = acquireLock(t, core, lockId, lease2.Id, true, now.Add(2*time.Minute))
	require.True(t, success)

	// Ancestor mode should have swapped to exclusive
	anc_a = getAncestor(t, core, ancestorId(lockId, "a"))
	require.EqualValues(t, 1, anc_a.ExclusiveCount)
	require.EqualValues(t, 0, anc_a.SharedCount)
}

func TestAncestors_SnapshotRestore(t *testing.T) {
	core1 := newLocksCore(t)
	now := time.Now()

	lockId := &corepb.LockId{
		AccountId:   rand.Uint64(),
		NamespaceId: rand.Uint64(),
		LockName:    "a/b/c",
	}

	// Create a lease
	lease := createLease(t, core1, lockId.AccountId, lockId.NamespaceId, "proc1", now, time.Hour)

	success, _ := acquireLock(t, core1, lockId, lease.Id, true, now)
	require.True(t, success)

	// Take snapshot
	snapshot := core1.Snapshot()
	buf := bytes.NewBuffer(nil)
	err := snapshot.Write(buf)
	require.NoError(t, err)

	// Restore into a new core
	core2 := newLocksCore(t)
	err = core2.Restore(io.NopCloser(buf))
	require.NoError(t, err)

	// Ancestor should be present in restored core
	anc_a := getAncestor(t, core2, ancestorId(lockId, "a"))
	require.EqualValues(t, 1, anc_a.ExclusiveCount)
	require.EqualValues(t, 0, anc_a.SharedCount)
}

// getAncestor is a test helper to fetch an ancestor entry directly.
func getAncestor(t *testing.T, core *Core, lockId *corepb.LockId) *corepb.LockAncestor {
	t.Helper()
	txn := core.badgerStore.View()
	defer txn.Discard()
	ancestor, err := core.ancestors.Get(txn, lockId)
	require.NoError(t, err)
	return ancestor
}

func ancestorId(lockId *corepb.LockId, name string) *corepb.LockId {
	return &corepb.LockId{
		AccountId:   lockId.AccountId,
		NamespaceId: lockId.NamespaceId,
		LockName:    name,
	}
}
