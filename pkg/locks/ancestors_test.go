package locks

import (
	"bytes"
	"io"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/evrblk/grackle/pkg/corepb"
)

func TestAncestors_ExclusiveLock(t *testing.T) {
	core := newLocksCore(t)
	now := time.Now()

	lockId := &corepb.LockId{
		AccountId:   rand.Uint64(),
		NamespaceId: rand.Uint32(),
		LockName:    "a/b/c",
	}

	// Create a lease
	lease := createLease(t, core, lockId.AccountId, lockId.NamespaceId, "proc1", now, time.Hour)

	// Acquire exclusive lock on a/b/c
	resp, err := core.AcquireLock(&corepb.AcquireLockRequest{
		LockId:                       lockId,
		Now:                          now.UnixNano(),
		LeaseId:                      lease.Id.LeaseId,
		Exclusive:                    true,
		MaxNumberOfLocksPerNamespace: 100,
	})
	require.NoError(t, err)
	require.True(t, resp.Success)

	// Ancestors "a" and "a/b" should have exclusive_count=1
	anc_a := getAncestor(t, core, ancestorId(lockId, "a"))
	require.EqualValues(t, 1, anc_a.ExclusiveCount)
	require.EqualValues(t, 0, anc_a.SharedCount)

	anc_ab := getAncestor(t, core, ancestorId(lockId, "a/b"))
	require.EqualValues(t, 1, anc_ab.ExclusiveCount)
	require.EqualValues(t, 0, anc_ab.SharedCount)

	// Release the lock
	_, err = core.ReleaseLock(&corepb.ReleaseLockRequest{
		LockId:  lockId,
		LeaseId: lease.Id.LeaseId,
		Now:     now.Add(time.Minute).UnixNano(),
	})
	require.NoError(t, err)

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
		NamespaceId: rand.Uint32(),
		LockName:    "a/b/c",
	}

	// Create leases for two processes
	lease1 := createLease(t, core, lockId.AccountId, lockId.NamespaceId, "proc1", now, time.Hour)
	lease2 := createLease(t, core, lockId.AccountId, lockId.NamespaceId, "proc2", now, time.Hour)

	// Two processes acquire shared lock on a/b/c
	resp1, err := core.AcquireLock(&corepb.AcquireLockRequest{
		LockId:                       lockId,
		Now:                          now.UnixNano(),
		LeaseId:                      lease1.Id.LeaseId,
		Exclusive:                    false,
		MaxNumberOfLocksPerNamespace: 100,
	})
	require.NoError(t, err)
	require.True(t, resp1.Success)

	resp2, err := core.AcquireLock(&corepb.AcquireLockRequest{
		LockId:                       lockId,
		Now:                          now.UnixNano(),
		LeaseId:                      lease2.Id.LeaseId,
		Exclusive:                    false,
		MaxNumberOfLocksPerNamespace: 100,
	})
	require.NoError(t, err)
	require.True(t, resp2.Success)

	// Ancestor a/b has shared_count=1 (one lock record, two holders)
	anc_ab := getAncestor(t, core, ancestorId(lockId, "a/b"))
	require.EqualValues(t, 0, anc_ab.ExclusiveCount)
	require.EqualValues(t, 1, anc_ab.SharedCount)

	// Release proc1 — lock still held by proc2, ancestors unchanged
	_, err = core.ReleaseLock(&corepb.ReleaseLockRequest{
		LockId:  lockId,
		LeaseId: lease1.Id.LeaseId,
		Now:     now.Add(time.Minute).UnixNano(),
	})
	require.NoError(t, err)

	anc_ab = getAncestor(t, core, ancestorId(lockId, "a/b"))
	require.EqualValues(t, 1, anc_ab.SharedCount) // still held by proc2

	// Release proc2 — lock now fully released
	_, err = core.ReleaseLock(&corepb.ReleaseLockRequest{
		LockId:  lockId,
		LeaseId: lease2.Id.LeaseId,
		Now:     now.Add(2 * time.Minute).UnixNano(),
	})
	require.NoError(t, err)

	anc_ab = getAncestor(t, core, ancestorId(lockId, "a/b"))
	require.EqualValues(t, 0, anc_ab.SharedCount)
	require.EqualValues(t, 0, anc_ab.ExclusiveCount)
}

func TestAncestors_MultipleLocksSameAncestor(t *testing.T) {
	core := newLocksCore(t)
	now := time.Now()

	accountId := rand.Uint64()
	namespaceId := rand.Uint32()

	lockId1 := &corepb.LockId{AccountId: accountId, NamespaceId: namespaceId, LockName: "a/b/c"}
	lockId2 := &corepb.LockId{AccountId: accountId, NamespaceId: namespaceId, LockName: "a/b/d"}
	lockId3 := &corepb.LockId{AccountId: accountId, NamespaceId: namespaceId, LockName: "a/x"}

	// Create leases for three processes
	lease1 := createLease(t, core, accountId, namespaceId, "proc1", now, time.Hour)
	lease2 := createLease(t, core, accountId, namespaceId, "proc2", now, time.Hour)
	lease3 := createLease(t, core, accountId, namespaceId, "proc3", now, time.Hour)

	// Acquire a/b/c exclusive
	_, err := core.AcquireLock(&corepb.AcquireLockRequest{
		LockId: lockId1, Now: now.UnixNano(), LeaseId: lease1.Id.LeaseId,
		Exclusive: true, MaxNumberOfLocksPerNamespace: 100,
	})
	require.NoError(t, err)

	// Acquire a/b/d shared
	_, err = core.AcquireLock(&corepb.AcquireLockRequest{
		LockId: lockId2, Now: now.UnixNano(), LeaseId: lease2.Id.LeaseId,
		Exclusive: false, MaxNumberOfLocksPerNamespace: 100,
	})
	require.NoError(t, err)

	// Acquire a/x exclusive
	_, err = core.AcquireLock(&corepb.AcquireLockRequest{
		LockId: lockId3, Now: now.UnixNano(), LeaseId: lease3.Id.LeaseId,
		Exclusive: true, MaxNumberOfLocksPerNamespace: 100,
	})
	require.NoError(t, err)

	// Ancestor "a": exclusive_count=2 (a/b/c and a/x), shared_count=1 (a/b/d)
	anc_a := getAncestor(t, core, ancestorId(lockId1, "a"))
	require.EqualValues(t, 2, anc_a.ExclusiveCount)
	require.EqualValues(t, 1, anc_a.SharedCount)

	// Ancestor "a/b": exclusive_count=1 (a/b/c), shared_count=1 (a/b/d)
	anc_ab := getAncestor(t, core, ancestorId(lockId1, "a/b"))
	require.EqualValues(t, 1, anc_ab.ExclusiveCount)
	require.EqualValues(t, 1, anc_ab.SharedCount)

	// Delete a/b/c
	_, err = core.DeleteLock(&corepb.DeleteLockRequest{LockId: lockId1})
	require.NoError(t, err)

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
		NamespaceId: rand.Uint32(),
		LockName:    "flat_lock",
	}

	// Create a lease
	lease := createLease(t, core, lockId.AccountId, lockId.NamespaceId, "proc1", now, time.Hour)

	_, err := core.AcquireLock(&corepb.AcquireLockRequest{
		LockId: lockId, Now: now.UnixNano(), LeaseId: lease.Id.LeaseId,
		Exclusive: true, MaxNumberOfLocksPerNamespace: 100,
	})
	require.NoError(t, err)

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
		NamespaceId: rand.Uint32(),
		LockName:    "a/b/c",
	}

	// Create a lease with short expiry
	lease := createLease(t, core, lockId.AccountId, lockId.NamespaceId, "proc1", now, time.Minute)

	// Acquire with short expiry
	_, err := core.AcquireLock(&corepb.AcquireLockRequest{
		LockId: lockId, Now: now.UnixNano(), LeaseId: lease.Id.LeaseId,
		Exclusive: true, MaxNumberOfLocksPerNamespace: 100,
	})
	require.NoError(t, err)

	anc_a := getAncestor(t, core, ancestorId(lockId, "a"))
	require.EqualValues(t, 1, anc_a.ExclusiveCount)

	// GetLock after expiry — should lazy-delete the lock and clean up ancestors
	_, err = core.GetLock(&corepb.GetLockRequest{
		LockId: lockId,
		Now:    now.Add(2 * time.Minute).UnixNano(),
	})
	require.NoError(t, err)

	anc_a = getAncestor(t, core, ancestorId(lockId, "a"))
	require.EqualValues(t, 0, anc_a.ExclusiveCount)
	require.EqualValues(t, 0, anc_a.SharedCount)
}

func TestAncestors_GarbageCollectionCleansUpAncestors(t *testing.T) {
	core := newLocksCore(t)
	now := time.Now()

	lockId := &corepb.LockId{
		AccountId:   rand.Uint64(),
		NamespaceId: rand.Uint32(),
		LockName:    "x/y/z",
	}

	// Create a lease with short expiry
	lease := createLease(t, core, lockId.AccountId, lockId.NamespaceId, "proc1", now, time.Minute)

	// Acquire with short expiry
	_, err := core.AcquireLock(&corepb.AcquireLockRequest{
		LockId: lockId, Now: now.UnixNano(), LeaseId: lease.Id.LeaseId,
		Exclusive: false, MaxNumberOfLocksPerNamespace: 100,
	})
	require.NoError(t, err)

	anc_x := getAncestor(t, core, ancestorId(lockId, "x"))
	require.EqualValues(t, 1, anc_x.SharedCount)

	// Run GC after expiry
	_, err = core.RunLocksGarbageCollection(&corepb.RunLocksGarbageCollectionRequest{
		Now:                   now.Add(2 * time.Minute).UnixNano(),
		GcRecordsPageSize:     100,
		GcRecordLocksPageSize: 100,
		MaxVisitedLocks:       100,
	})
	require.NoError(t, err)

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
		NamespaceId: rand.Uint32(),
	}

	lockId := &corepb.LockId{
		AccountId:   namespaceId.AccountId,
		NamespaceId: namespaceId.NamespaceId,
		LockName:    "a/b/c",
	}

	// Create a lease
	lease := createLease(t, core, namespaceId.AccountId, namespaceId.NamespaceId, "proc1", now, time.Hour)

	// Acquire lock
	_, err := core.AcquireLock(&corepb.AcquireLockRequest{
		LockId: lockId, Now: now.UnixNano(), LeaseId: lease.Id.LeaseId,
		Exclusive: true, MaxNumberOfLocksPerNamespace: 100,
	})
	require.NoError(t, err)

	anc_a := getAncestor(t, core, ancestorId(lockId, "a"))
	require.EqualValues(t, 1, anc_a.ExclusiveCount)

	// Schedule namespace for deletion
	_, err = core.LocksDeleteNamespace(&corepb.LocksDeleteNamespaceRequest{
		NamespaceId: namespaceId,
		RecordId:    1,
	})
	require.NoError(t, err)

	// Run GC to delete the namespace's locks
	_, err = core.RunLocksGarbageCollection(&corepb.RunLocksGarbageCollectionRequest{
		Now:                   now.Add(time.Minute).UnixNano(),
		GcRecordsPageSize:     100,
		GcRecordLocksPageSize: 100,
		MaxVisitedLocks:       100,
	})
	require.NoError(t, err)

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
		NamespaceId: rand.Uint32(),
		LockName:    "a/b/c",
	}

	// Create leases for two processes
	lease1 := createLease(t, core, lockId.AccountId, lockId.NamespaceId, "proc1", now, time.Minute)
	lease2 := createLease(t, core, lockId.AccountId, lockId.NamespaceId, "proc2", now.Add(2*time.Minute), time.Minute)

	// Acquire shared lock
	_, err := core.AcquireLock(&corepb.AcquireLockRequest{
		LockId: lockId, Now: now.UnixNano(), LeaseId: lease1.Id.LeaseId,
		Exclusive: false, MaxNumberOfLocksPerNamespace: 100,
	})
	require.NoError(t, err)

	anc_a := getAncestor(t, core, ancestorId(lockId, "a"))
	require.EqualValues(t, 0, anc_a.ExclusiveCount)
	require.EqualValues(t, 1, anc_a.SharedCount)

	// Re-acquire as exclusive after expiry (without GC running first)
	_, err = core.AcquireLock(&corepb.AcquireLockRequest{
		LockId: lockId, Now: now.Add(2 * time.Minute).UnixNano(), LeaseId: lease2.Id.LeaseId,
		Exclusive: true, MaxNumberOfLocksPerNamespace: 100,
	})
	require.NoError(t, err)

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
		NamespaceId: rand.Uint32(),
		LockName:    "a/b/c",
	}

	// Create a lease
	lease := createLease(t, core1, lockId.AccountId, lockId.NamespaceId, "proc1", now, time.Hour)

	_, err := core1.AcquireLock(&corepb.AcquireLockRequest{
		LockId: lockId, Now: now.UnixNano(), LeaseId: lease.Id.LeaseId,
		Exclusive: true, MaxNumberOfLocksPerNamespace: 100,
	})
	require.NoError(t, err)

	// Take snapshot
	snapshot := core1.Snapshot()
	buf := bytes.NewBuffer(nil)
	err = snapshot.Write(buf)
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
