package grackle

import (
	"bytes"
	"fmt"
	"io"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/evrblk/grackle/pkg/corepb"
	"github.com/evrblk/monstera"
)

func TestAcquireWriteLock(t *testing.T) {
	require := require.New(t)

	locksCore := newLocksCore()

	now := time.Now()

	accountId := rand.Uint64()
	lockId := &corepb.LockId{
		AccountId:          accountId,
		NamespaceName:      "test_namespace",
		NamespaceCreatedAt: now.Unix(),
		LockName:           "test_lock",
	}

	// T+0: Acquire lock
	response1, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
		LockId:                       lockId,
		Now:                          now.UnixNano(),
		ProcessId:                    "process_1",
		WriteLock:                    true,
		ExpiresAt:                    now.Add(time.Hour).UnixNano(),
		MaxNumberOfLocksPerNamespace: 10,
	})

	require.NoError(err)
	require.NotNil(response1.Lock)
	require.Equal(true, response1.Success)
	require.EqualValues(now.UnixNano(), response1.Lock.LockedAt)
	require.Equal(corepb.LockState_WRITE_LOCKED, response1.Lock.State)
	require.Equal("process_1", response1.Lock.WriteLockHolder.ProcessId)
	require.EqualValues(now.Add(time.Hour).UnixNano(), response1.Lock.WriteLockHolder.ExpiresAt)
	require.EqualValues(now.UnixNano(), response1.Lock.WriteLockHolder.LockedAt)

	// T+1m: Get lock
	response2, err := locksCore.GetLock(&corepb.GetLockRequest{
		LockId: lockId,
		Now:    now.Add(time.Minute).UnixNano(),
	})

	require.NoError(err)
	require.NotNil(response2.Lock)
	require.Equal(corepb.LockState_WRITE_LOCKED, response1.Lock.State)

	// T+61m: Get lock
	response3, err := locksCore.GetLock(&corepb.GetLockRequest{
		LockId: lockId,
		Now:    now.Add(61 * time.Minute).UnixNano(),
	})

	require.NoError(err)
	require.NotNil(response3.Lock)
	require.Equal(corepb.LockState_UNLOCKED, response3.Lock.State)
}

func TestAcquireReadLock(t *testing.T) {
	require := require.New(t)

	locksCore := newLocksCore()

	now := time.Now()

	accountId := rand.Uint64()
	lockId := &corepb.LockId{
		AccountId:          accountId,
		NamespaceName:      "test_namespace",
		NamespaceCreatedAt: now.Unix(),
		LockName:           "test_lock",
	}

	// T+0: Acquire lock
	response1, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
		LockId:                       lockId,
		Now:                          now.UnixNano(),
		ProcessId:                    "process_1",
		WriteLock:                    false,
		ExpiresAt:                    now.Add(time.Hour).UnixNano(),
		MaxNumberOfLocksPerNamespace: 10,
	})

	require.NoError(err)
	require.NotNil(response1.Lock)
	require.Equal(true, response1.Success)
	require.EqualValues(now.UnixNano(), response1.Lock.LockedAt)
	require.Equal(corepb.LockState_READ_LOCKED, response1.Lock.State)
	require.Len(response1.Lock.ReadLockHolders, 1)
	require.Equal("process_1", response1.Lock.ReadLockHolders[0].ProcessId)
	require.EqualValues(now.Add(time.Hour).UnixNano(), response1.Lock.ReadLockHolders[0].ExpiresAt)
	require.EqualValues(now.UnixNano(), response1.Lock.ReadLockHolders[0].LockedAt)

	// T+1m: Get lock
	response2, err := locksCore.GetLock(&corepb.GetLockRequest{
		LockId: lockId,
		Now:    now.Add(time.Minute).UnixNano(),
	})

	require.NoError(err)
	require.NotNil(response2.Lock)
	require.Equal(corepb.LockState_READ_LOCKED, response1.Lock.State)

	// T+61m: Get lock
	response3, err := locksCore.GetLock(&corepb.GetLockRequest{
		LockId: lockId,
		Now:    now.Add(61 * time.Minute).UnixNano(),
	})

	require.NoError(err)
	require.NotNil(response3.Lock)
	require.Equal(corepb.LockState_UNLOCKED, response3.Lock.State)
}

func TestAcquireWriteLockRepeatedly(t *testing.T) {
	require := require.New(t)

	locksCore := newLocksCore()

	now := time.Now()

	accountId := rand.Uint64()
	lockId := &corepb.LockId{
		AccountId:          accountId,
		NamespaceName:      "test_namespace",
		NamespaceCreatedAt: now.Unix(),
		LockName:           "test_lock",
	}

	// T+0: Acquire lock
	response1, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
		LockId:                       lockId,
		Now:                          now.UnixNano(),
		ProcessId:                    "process_1",
		WriteLock:                    true,
		ExpiresAt:                    now.Add(time.Hour).UnixNano(),
		MaxNumberOfLocksPerNamespace: 10,
	})

	require.NoError(err)
	require.Equal(true, response1.Success)
	require.Equal(corepb.LockState_WRITE_LOCKED, response1.Lock.State)

	// T+1m: Acquire lock again
	response2, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
		LockId:                       lockId,
		Now:                          now.Add(time.Minute).UnixNano(),
		ProcessId:                    "process_1",
		WriteLock:                    true,
		ExpiresAt:                    now.Add(time.Minute).Add(time.Hour).UnixNano(),
		MaxNumberOfLocksPerNamespace: 10,
	})

	require.NoError(err)
	require.Equal(true, response2.Success)
	require.Equal(corepb.LockState_WRITE_LOCKED, response2.Lock.State)
	require.EqualValues(now.Add(time.Minute).Add(time.Hour).UnixNano(), response2.Lock.WriteLockHolder.ExpiresAt)
}

func TestAcquireReadLockRepeatedly(t *testing.T) {
	require := require.New(t)

	locksCore := newLocksCore()

	now := time.Now()

	accountId := rand.Uint64()
	lockId := &corepb.LockId{
		AccountId:          accountId,
		NamespaceName:      "test_namespace",
		NamespaceCreatedAt: now.Unix(),
		LockName:           "test_lock",
	}

	// T+0: Acquire lock
	response1, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
		LockId:                       lockId,
		Now:                          now.UnixNano(),
		ProcessId:                    "process_1",
		WriteLock:                    false,
		ExpiresAt:                    now.Add(time.Hour).UnixNano(),
		MaxNumberOfLocksPerNamespace: 10,
	})

	require.NoError(err)
	require.Equal(true, response1.Success)
	require.Equal(corepb.LockState_READ_LOCKED, response1.Lock.State)

	// T+1m: Acquire lock again
	response2, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
		LockId:                       lockId,
		Now:                          now.Add(time.Minute).UnixNano(),
		ProcessId:                    "process_1",
		WriteLock:                    false,
		ExpiresAt:                    now.Add(time.Minute).Add(time.Hour).UnixNano(),
		MaxNumberOfLocksPerNamespace: 10,
	})

	require.NoError(err)
	require.Equal(true, response2.Success)
	require.Equal(corepb.LockState_READ_LOCKED, response2.Lock.State)
	require.EqualValues(now.Add(time.Minute).Add(time.Hour).UnixNano(), response2.Lock.ReadLockHolders[0].ExpiresAt)
}

func TestAcquireLockWriteLockedByAnotherProcess(t *testing.T) {
	require := require.New(t)

	locksCore := newLocksCore()

	now := time.Now()

	accountId := rand.Uint64()
	lockId := &corepb.LockId{
		AccountId:          accountId,
		NamespaceName:      "test_namespace",
		NamespaceCreatedAt: now.Unix(),
		LockName:           "test_lock",
	}

	// T+0: Acquire lock
	response1, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
		LockId:                       lockId,
		Now:                          now.UnixNano(),
		ProcessId:                    "process_1",
		WriteLock:                    true,
		ExpiresAt:                    now.Add(time.Hour).UnixNano(),
		MaxNumberOfLocksPerNamespace: 10,
	})

	require.NoError(err)
	require.Equal(true, response1.Success)
	require.Equal(corepb.LockState_WRITE_LOCKED, response1.Lock.State)

	// T+1m: Acquire write lock by another process
	response2, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
		LockId:                       lockId,
		Now:                          now.Add(time.Minute).UnixNano(),
		ProcessId:                    "process_2",
		WriteLock:                    true,
		ExpiresAt:                    now.Add(time.Minute).Add(time.Hour).UnixNano(),
		MaxNumberOfLocksPerNamespace: 10,
	})

	require.NoError(err)
	require.Equal(false, response2.Success)
	require.Equal(corepb.LockState_WRITE_LOCKED, response2.Lock.State)
	require.Equal("process_1", response2.Lock.WriteLockHolder.ProcessId)

	// T+2m: Acquire read lock by another process
	response3, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
		LockId:                       lockId,
		Now:                          now.Add(2 * time.Minute).UnixNano(),
		ProcessId:                    "process_2",
		WriteLock:                    false,
		ExpiresAt:                    now.Add(2 * time.Minute).Add(time.Hour).UnixNano(),
		MaxNumberOfLocksPerNamespace: 10,
	})

	require.NoError(err)
	require.Equal(false, response3.Success)
	require.Equal(corepb.LockState_WRITE_LOCKED, response3.Lock.State)
	require.Equal("process_1", response3.Lock.WriteLockHolder.ProcessId)
}

func TestAcquireLockReadLockedByAnotherProcess(t *testing.T) {
	require := require.New(t)

	locksCore := newLocksCore()

	now := time.Now()

	accountId := rand.Uint64()
	lockId := &corepb.LockId{
		AccountId:          accountId,
		NamespaceName:      "test_namespace",
		NamespaceCreatedAt: now.Unix(),
		LockName:           "test_lock",
	}

	// T+0: Acquire lock
	response1, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
		LockId:                       lockId,
		Now:                          now.UnixNano(),
		ProcessId:                    "process_1",
		WriteLock:                    false,
		ExpiresAt:                    now.Add(time.Hour).UnixNano(),
		MaxNumberOfLocksPerNamespace: 10,
	})

	require.NoError(err)
	require.Equal(true, response1.Success)
	require.Equal(corepb.LockState_READ_LOCKED, response1.Lock.State)

	// T+1m: Acquire write lock by another process
	response2, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
		LockId:                       lockId,
		Now:                          now.Add(time.Minute).UnixNano(),
		ProcessId:                    "process_2",
		WriteLock:                    true,
		ExpiresAt:                    now.Add(time.Minute).Add(time.Hour).UnixNano(),
		MaxNumberOfLocksPerNamespace: 10,
	})

	require.NoError(err)
	require.Equal(false, response2.Success)
	require.Equal(corepb.LockState_READ_LOCKED, response2.Lock.State)
	require.Equal("process_1", response2.Lock.ReadLockHolders[0].ProcessId)

	// T+2m: Acquire read lock by another process
	response3, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
		LockId:                       lockId,
		Now:                          now.Add(2 * time.Minute).UnixNano(),
		ProcessId:                    "process_2",
		WriteLock:                    false,
		ExpiresAt:                    now.Add(2 * time.Minute).Add(time.Hour).UnixNano(),
		MaxNumberOfLocksPerNamespace: 10,
	})

	require.NoError(err)
	require.Equal(true, response3.Success)
	require.Equal(corepb.LockState_READ_LOCKED, response3.Lock.State)
	require.Len(response3.Lock.ReadLockHolders, 2)
	require.Equal("process_1", response3.Lock.ReadLockHolders[0].ProcessId)
	require.Equal("process_2", response3.Lock.ReadLockHolders[1].ProcessId)
}

func TestGetNonexistentLock(t *testing.T) {
	require := require.New(t)

	locksCore := newLocksCore()

	now := time.Now()

	accountId := rand.Uint64()
	lockId := &corepb.LockId{
		AccountId:          accountId,
		NamespaceName:      "test_namespace",
		NamespaceCreatedAt: now.Unix(),
		LockName:           "test_lock",
	}

	// Get lock
	response1, err := locksCore.GetLock(&corepb.GetLockRequest{
		LockId: lockId,
		Now:    now.UnixNano(),
	})

	require.NoError(err)
	require.NotNil(response1.Lock)
	require.Equal(corepb.LockState_UNLOCKED, response1.Lock.State)
}

func TestDeleteNonexistentLock(t *testing.T) {
	require := require.New(t)

	locksCore := newLocksCore()

	now := time.Now()

	accountId := rand.Uint64()
	lockId := &corepb.LockId{
		AccountId:          accountId,
		NamespaceName:      "test_namespace",
		NamespaceCreatedAt: now.Unix(),
		LockName:           "test_lock",
	}

	// Delete lock
	_, err := locksCore.DeleteLock(&corepb.DeleteLockRequest{
		LockId: lockId,
		Now:    now.UnixNano(),
	})

	require.NoError(err)
}

func TestReleaseNonexistentLock(t *testing.T) {
	require := require.New(t)

	locksCore := newLocksCore()

	now := time.Now()

	accountId := rand.Uint64()
	lockId := &corepb.LockId{
		AccountId:          accountId,
		NamespaceName:      "test_namespace",
		NamespaceCreatedAt: now.Unix(),
		LockName:           "test_lock",
	}

	// Release lock
	response1, err := locksCore.ReleaseLock(&corepb.ReleaseLockRequest{
		LockId:    lockId,
		ProcessId: "process_1",
		Now:       now.UnixNano(),
	})

	require.NoError(err)
	require.NotNil(response1.Lock)
	require.Equal(corepb.LockState_UNLOCKED, response1.Lock.State)
}

func TestDeleteAcquiredLock(t *testing.T) {
	require := require.New(t)

	locksCore := newLocksCore()

	now := time.Now()

	accountId := rand.Uint64()
	lockId := &corepb.LockId{
		AccountId:          accountId,
		NamespaceName:      "test_namespace",
		NamespaceCreatedAt: now.Unix(),
		LockName:           "test_lock",
	}

	// T+0: Acquire lock
	response1, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
		LockId:                       lockId,
		Now:                          now.UnixNano(),
		ProcessId:                    "process_1",
		WriteLock:                    true,
		ExpiresAt:                    now.Add(time.Hour).UnixNano(),
		MaxNumberOfLocksPerNamespace: 10,
	})

	require.NoError(err)
	require.NotNil(response1.Lock)
	require.Equal(true, response1.Success)
	require.Equal(corepb.LockState_WRITE_LOCKED, response1.Lock.State)

	// T+1m: Delete lock
	_, err = locksCore.DeleteLock(&corepb.DeleteLockRequest{
		LockId: lockId,
		Now:    now.Add(time.Minute).UnixNano(),
	})

	require.NoError(err)

	// T+2m: Acquire lock
	response3, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
		LockId:                       lockId,
		Now:                          now.Add(2 * time.Minute).UnixNano(),
		ProcessId:                    "process_2",
		WriteLock:                    true,
		ExpiresAt:                    now.Add(2 * time.Minute).Add(time.Hour).UnixNano(),
		MaxNumberOfLocksPerNamespace: 10,
	})

	require.NoError(err)
	require.NotNil(response3.Lock)
	require.Equal(true, response3.Success)
	require.Equal(corepb.LockState_WRITE_LOCKED, response3.Lock.State)
	require.Equal("process_2", response3.Lock.WriteLockHolder.ProcessId)
}

func TestReleaseWriteLock(t *testing.T) {
	require := require.New(t)

	locksCore := newLocksCore()

	now := time.Now()

	accountId := rand.Uint64()
	lockId := &corepb.LockId{
		AccountId:          accountId,
		NamespaceName:      "test_namespace",
		NamespaceCreatedAt: now.Unix(),
		LockName:           "test_lock",
	}

	// T+0: Acquire lock
	response1, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
		LockId:                       lockId,
		Now:                          now.UnixNano(),
		ProcessId:                    "process_1",
		WriteLock:                    true,
		ExpiresAt:                    now.Add(time.Hour).UnixNano(),
		MaxNumberOfLocksPerNamespace: 10,
	})

	require.NoError(err)
	require.NotNil(response1.Lock)
	require.Equal(true, response1.Success)
	require.Equal(corepb.LockState_WRITE_LOCKED, response1.Lock.State)

	// T+1m: Release lock with wrong process id
	response2, err := locksCore.ReleaseLock(&corepb.ReleaseLockRequest{
		LockId:    lockId,
		ProcessId: "process_2",
		Now:       now.Add(time.Minute).UnixNano(),
	})

	require.NoError(err)
	require.NotNil(response2.Lock)
	require.Equal(corepb.LockState_WRITE_LOCKED, response1.Lock.State)
	require.Equal("process_1", response1.Lock.WriteLockHolder.ProcessId)

	// T+2m: Release lock with correct process id
	response3, err := locksCore.ReleaseLock(&corepb.ReleaseLockRequest{
		LockId:    lockId,
		Now:       now.Add(2 * time.Minute).UnixNano(),
		ProcessId: "process_1",
	})

	require.NoError(err)
	require.NotNil(response3.Lock)
	require.Equal(corepb.LockState_UNLOCKED, response3.Lock.State)
}

func TestReleaseReadLock(t *testing.T) {
	require := require.New(t)

	locksCore := newLocksCore()

	now := time.Now()

	accountId := rand.Uint64()
	lockId := &corepb.LockId{
		AccountId:          accountId,
		NamespaceName:      "test_namespace",
		NamespaceCreatedAt: now.Unix(),
		LockName:           "test_lock",
	}

	// T+0: Acquire read lock
	response1, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
		LockId:                       lockId,
		Now:                          now.UnixNano(),
		ProcessId:                    "process_1",
		WriteLock:                    false,
		ExpiresAt:                    now.Add(time.Hour).UnixNano(),
		MaxNumberOfLocksPerNamespace: 10,
	})

	require.NoError(err)
	require.NotNil(response1.Lock)
	require.Equal(true, response1.Success)
	require.Equal(corepb.LockState_READ_LOCKED, response1.Lock.State)

	// T+1m: Acquire read lock from another process
	response2, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
		LockId:                       lockId,
		Now:                          now.Add(time.Minute).UnixNano(),
		ProcessId:                    "process_2",
		WriteLock:                    false,
		ExpiresAt:                    now.Add(time.Minute).Add(time.Hour).UnixNano(),
		MaxNumberOfLocksPerNamespace: 10,
	})

	require.NoError(err)
	require.NotNil(response2.Lock)
	require.Equal(true, response2.Success)
	require.Equal(corepb.LockState_READ_LOCKED, response2.Lock.State)

	// T+2m: Release lock with first process id
	response3, err := locksCore.ReleaseLock(&corepb.ReleaseLockRequest{
		LockId:    lockId,
		Now:       now.Add(2 * time.Minute).UnixNano(),
		ProcessId: "process_1",
	})

	require.NoError(err)
	require.NotNil(response3.Lock)
	require.Equal(corepb.LockState_READ_LOCKED, response3.Lock.State)
	require.Equal("process_2", response3.Lock.ReadLockHolders[0].ProcessId)

	// T+3m: Release lock with second process id
	response4, err := locksCore.ReleaseLock(&corepb.ReleaseLockRequest{
		LockId:    lockId,
		Now:       now.Add(3 * time.Minute).UnixNano(),
		ProcessId: "process_2",
	})

	require.NoError(err)
	require.NotNil(response4.Lock)
	require.Equal(corepb.LockState_UNLOCKED, response4.Lock.State)
	require.Len(response4.Lock.ReadLockHolders, 0)
}

func TestReleaseExpiredLock(t *testing.T) {
	require := require.New(t)

	locksCore := newLocksCore()

	now := time.Now()

	accountId := rand.Uint64()
	lockId := &corepb.LockId{
		AccountId:          accountId,
		NamespaceName:      "test_namespace",
		NamespaceCreatedAt: now.Unix(),
		LockName:           "test_lock",
	}

	// T+0: Acquire lock
	response1, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
		LockId:                       lockId,
		Now:                          now.UnixNano(),
		ProcessId:                    "process_1",
		WriteLock:                    true,
		ExpiresAt:                    now.Add(time.Hour).UnixNano(),
		MaxNumberOfLocksPerNamespace: 10,
	})

	require.NoError(err)
	require.NotNil(response1.Lock)
	require.Equal(true, response1.Success)
	require.Equal(corepb.LockState_WRITE_LOCKED, response1.Lock.State)

	// T+61m: Release lock after expiration time
	response2, err := locksCore.ReleaseLock(&corepb.ReleaseLockRequest{
		LockId:    lockId,
		Now:       now.Add(61 * time.Minute).UnixNano(),
		ProcessId: "process_1",
	})

	require.NoError(err)
	require.NotNil(response2.Lock)
	require.Equal(corepb.LockState_UNLOCKED, response2.Lock.State)
}

func TestSnapshotAndRestoreLocks(t *testing.T) {
	require := require.New(t)

	now := time.Now()

	accountId := rand.Uint64()
	lockId := &corepb.LockId{
		AccountId:          accountId,
		NamespaceName:      "test_namespace",
		NamespaceCreatedAt: now.Unix(),
		LockName:           "test_lock",
	}

	// Create two lock cores for testing snapshot and restore
	locksCore1 := newLocksCore()
	locksCore2 := newLocksCore()

	// T+0: Acquire write lock
	response1, err := locksCore1.AcquireLock(&corepb.AcquireLockRequest{
		LockId:                       lockId,
		Now:                          now.UnixNano(),
		ProcessId:                    "process_1",
		WriteLock:                    true,
		ExpiresAt:                    now.Add(time.Hour).UnixNano(),
		MaxNumberOfLocksPerNamespace: 10,
	})
	require.NoError(err)
	require.True(response1.Success)
	require.Equal(corepb.LockState_WRITE_LOCKED, response1.Lock.State)

	// Take snapshot at this point
	snapshot := locksCore1.Snapshot()

	// T+1m: Release the write lock (after snapshot)
	_, err = locksCore1.ReleaseLock(&corepb.ReleaseLockRequest{
		LockId:    lockId,
		ProcessId: "process_1",
		Now:       now.Add(time.Minute).UnixNano(),
	})
	require.NoError(err)

	// T+2m: Acquire read lock with different process (after snapshot)
	response2, err := locksCore1.AcquireLock(&corepb.AcquireLockRequest{
		LockId:                       lockId,
		Now:                          now.Add(2 * time.Minute).UnixNano(),
		ProcessId:                    "process_2",
		WriteLock:                    false,
		ExpiresAt:                    now.Add(2 * time.Minute).Add(time.Hour).UnixNano(),
		MaxNumberOfLocksPerNamespace: 10,
	})
	require.NoError(err)
	require.True(response2.Success)
	require.Equal(corepb.LockState_READ_LOCKED, response2.Lock.State)

	// Write snapshot to buffer
	buf := bytes.NewBuffer(nil)
	err = snapshot.Write(buf)
	require.NoError(err)

	// Restore snapshot to second core
	err = locksCore2.Restore(io.NopCloser(buf))
	require.NoError(err)

	// T+3m: Check that the restored state matches the snapshot state
	// The lock should exist with write lock held by process_1
	response3, err := locksCore2.GetLock(&corepb.GetLockRequest{
		LockId: lockId,
		Now:    now.Add(3 * time.Minute).UnixNano(),
	})
	require.NoError(err)
	require.NotNil(response3.Lock)
	require.Equal(corepb.LockState_WRITE_LOCKED, response3.Lock.State)
	require.Equal("process_1", response3.Lock.WriteLockHolder.ProcessId)

	// T+4m: Try to acquire write lock with different process in restored state (should fail)
	response4, err := locksCore2.AcquireLock(&corepb.AcquireLockRequest{
		LockId:                       lockId,
		Now:                          now.Add(4 * time.Minute).UnixNano(),
		ProcessId:                    "process_3",
		WriteLock:                    true,
		ExpiresAt:                    now.Add(4 * time.Minute).Add(time.Hour).UnixNano(),
		MaxNumberOfLocksPerNamespace: 10,
	})
	require.NoError(err)
	require.False(response4.Success)
	require.Equal(corepb.LockState_WRITE_LOCKED, response4.Lock.State)

	// T+5m: Try to acquire read lock with different process in restored state (should fail)
	response5, err := locksCore2.AcquireLock(&corepb.AcquireLockRequest{
		LockId:                       lockId,
		Now:                          now.Add(5 * time.Minute).UnixNano(),
		ProcessId:                    "process_3",
		WriteLock:                    false,
		ExpiresAt:                    now.Add(5 * time.Minute).Add(time.Hour).UnixNano(),
		MaxNumberOfLocksPerNamespace: 10,
	})
	require.NoError(err)
	require.False(response5.Success)
	require.Equal(corepb.LockState_WRITE_LOCKED, response5.Lock.State)

	// T+6m: Release the write lock in restored state
	_, err = locksCore2.ReleaseLock(&corepb.ReleaseLockRequest{
		LockId:    lockId,
		ProcessId: "process_1",
		Now:       now.Add(6 * time.Minute).UnixNano(),
	})
	require.NoError(err)

	// T+7m: Verify lock is unlocked in restored state
	response6, err := locksCore2.GetLock(&corepb.GetLockRequest{
		LockId: lockId,
		Now:    now.Add(7 * time.Minute).UnixNano(),
	})
	require.NoError(err)
	require.Equal(corepb.LockState_UNLOCKED, response6.Lock.State)

	// T+8m: Acquire read lock in restored state (should succeed)
	response7, err := locksCore2.AcquireLock(&corepb.AcquireLockRequest{
		LockId:                       lockId,
		Now:                          now.Add(8 * time.Minute).UnixNano(),
		ProcessId:                    "process_4",
		WriteLock:                    false,
		ExpiresAt:                    now.Add(8 * time.Minute).Add(time.Hour).UnixNano(),
		MaxNumberOfLocksPerNamespace: 10,
	})
	require.NoError(err)
	require.True(response7.Success)
	require.Equal(corepb.LockState_READ_LOCKED, response7.Lock.State)

	// Verify that the original core has different state (it should have a read lock from process_2)
	response8, err := locksCore1.GetLock(&corepb.GetLockRequest{
		LockId: lockId,
		Now:    now.Add(8 * time.Minute).UnixNano(),
	})
	require.NoError(err)
	require.Equal(corepb.LockState_READ_LOCKED, response8.Lock.State)
	require.Len(response8.Lock.ReadLockHolders, 1)
	require.Equal("process_2", response8.Lock.ReadLockHolders[0].ProcessId)
}

func TestListLocksEmptyNamespace(t *testing.T) {
	require := require.New(t)

	locksCore := newLocksCore()

	now := time.Now()

	namespaceId := &corepb.NamespaceId{
		AccountId:     rand.Uint64(),
		NamespaceName: "test_namespace",
	}

	// List locks in empty namespace
	response, err := locksCore.ListLocks(&corepb.ListLocksRequest{
		NamespaceTimestampedId: &corepb.NamespaceTimestampedId{
			AccountId:          namespaceId.AccountId,
			NamespaceName:      namespaceId.NamespaceName,
			NamespaceCreatedAt: now.Unix(),
		},
		Now: now.UnixNano(),
	})

	require.NoError(err)
	require.NotNil(response)
	require.Empty(response.Locks)
}

func TestListLocksWithActiveLocks(t *testing.T) {
	require := require.New(t)

	locksCore := newLocksCore()

	now := time.Now()

	accountId := rand.Uint64()
	namespaceId := &corepb.NamespaceId{
		AccountId:     accountId,
		NamespaceName: "test_namespace",
	}

	// Create multiple locks in the same namespace
	lockId1 := &corepb.LockId{
		AccountId:          accountId,
		NamespaceName:      "test_namespace",
		NamespaceCreatedAt: now.Unix(),
		LockName:           "lock_1",
	}

	lockId2 := &corepb.LockId{
		AccountId:          accountId,
		NamespaceName:      "test_namespace",
		NamespaceCreatedAt: now.Unix(),
		LockName:           "lock_2",
	}

	lockId3 := &corepb.LockId{
		AccountId:          accountId,
		NamespaceName:      "test_namespace",
		NamespaceCreatedAt: now.Unix(),
		LockName:           "lock_3",
	}

	// T+0: Acquire write lock for lock_1
	response1, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
		LockId:                       lockId1,
		Now:                          now.UnixNano(),
		ProcessId:                    "process_1",
		WriteLock:                    true,
		ExpiresAt:                    now.Add(time.Hour).UnixNano(),
		MaxNumberOfLocksPerNamespace: 10,
	})
	require.NoError(err)
	require.True(response1.Success)

	// T+1m: Acquire read lock for lock_2
	response2, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
		LockId:                       lockId2,
		Now:                          now.Add(time.Minute).UnixNano(),
		ProcessId:                    "process_2",
		WriteLock:                    false,
		ExpiresAt:                    now.Add(time.Minute).Add(time.Hour).UnixNano(),
		MaxNumberOfLocksPerNamespace: 10,
	})
	require.NoError(err)
	require.True(response2.Success)

	// T+2m: Acquire read lock for lock_3
	response3, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
		LockId:                       lockId3,
		Now:                          now.Add(2 * time.Minute).UnixNano(),
		ProcessId:                    "process_3",
		WriteLock:                    false,
		ExpiresAt:                    now.Add(2 * time.Minute).Add(time.Hour).UnixNano(),
		MaxNumberOfLocksPerNamespace: 10,
	})
	require.NoError(err)
	require.True(response3.Success)

	// T+3m: List locks
	response4, err := locksCore.ListLocks(&corepb.ListLocksRequest{
		NamespaceTimestampedId: &corepb.NamespaceTimestampedId{
			AccountId:          namespaceId.AccountId,
			NamespaceName:      namespaceId.NamespaceName,
			NamespaceCreatedAt: now.Unix(),
		},
		Now: now.Add(3 * time.Minute).UnixNano(),
	})

	require.NoError(err)
	require.NotNil(response4)
	require.Len(response4.Locks, 3)

	// Verify all locks are returned and are in the correct state
	lockMap := make(map[string]*corepb.Lock)
	for _, lock := range response4.Locks {
		lockMap[lock.Id.LockName] = lock
	}

	require.Contains(lockMap, "lock_1")
	require.Contains(lockMap, "lock_2")
	require.Contains(lockMap, "lock_3")

	require.Equal(corepb.LockState_WRITE_LOCKED, lockMap["lock_1"].State)
	require.Equal(corepb.LockState_READ_LOCKED, lockMap["lock_2"].State)
	require.Equal(corepb.LockState_READ_LOCKED, lockMap["lock_3"].State)
}

func TestListLocksWithExpiredLocks(t *testing.T) {
	require := require.New(t)

	locksCore := newLocksCore()

	now := time.Now()

	accountId := rand.Uint64()
	namespaceId := &corepb.NamespaceId{
		AccountId:     accountId,
		NamespaceName: "test_namespace",
	}

	// Create locks with different expiration times
	lockId1 := &corepb.LockId{
		AccountId:          accountId,
		NamespaceName:      "test_namespace",
		NamespaceCreatedAt: now.Unix(),
		LockName:           "lock_active",
	}

	lockId2 := &corepb.LockId{
		AccountId:          accountId,
		NamespaceName:      "test_namespace",
		NamespaceCreatedAt: now.Unix(),
		LockName:           "lock_expired",
	}

	// T+0: Acquire lock that will remain active
	response1, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
		LockId:                       lockId1,
		Now:                          now.UnixNano(),
		ProcessId:                    "process_1",
		WriteLock:                    true,
		ExpiresAt:                    now.Add(time.Hour).UnixNano(),
		MaxNumberOfLocksPerNamespace: 10,
	})
	require.NoError(err)
	require.True(response1.Success)

	// T+1m: Acquire lock that will expire
	response2, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
		LockId:                       lockId2,
		Now:                          now.Add(time.Minute).UnixNano(),
		ProcessId:                    "process_2",
		WriteLock:                    false,
		ExpiresAt:                    now.Add(2 * time.Minute).UnixNano(), // Will expire at T+3m
		MaxNumberOfLocksPerNamespace: 10,
	})
	require.NoError(err)
	require.True(response2.Success)

	// T+3m: List locks (after lock_2 has expired)
	response3, err := locksCore.ListLocks(&corepb.ListLocksRequest{
		NamespaceTimestampedId: &corepb.NamespaceTimestampedId{
			AccountId:          namespaceId.AccountId,
			NamespaceName:      namespaceId.NamespaceName,
			NamespaceCreatedAt: now.Unix(),
		},
		Now: now.Add(3 * time.Minute).UnixNano(),
	})

	require.NoError(err)
	require.NotNil(response3)
	require.Len(response3.Locks, 1) // Only the active lock should be returned

	require.Equal("lock_active", response3.Locks[0].Id.LockName)
	require.Equal(corepb.LockState_WRITE_LOCKED, response3.Locks[0].State)
}

func TestListLocksWithMultipleNamespaces(t *testing.T) {
	require := require.New(t)

	locksCore := newLocksCore()

	now := time.Now()

	accountId := rand.Uint64()

	// Create locks in different namespaces
	lockId1 := &corepb.LockId{
		AccountId:          accountId,
		NamespaceName:      "namespace_1",
		NamespaceCreatedAt: now.Unix(),
		LockName:           "lock_1",
	}

	lockId2 := &corepb.LockId{
		AccountId:          accountId,
		NamespaceName:      "namespace_2",
		NamespaceCreatedAt: now.Unix(),
		LockName:           "lock_2",
	}

	// T+0: Acquire lock in namespace_1
	response1, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
		LockId:                       lockId1,
		Now:                          now.UnixNano(),
		ProcessId:                    "process_1",
		WriteLock:                    true,
		ExpiresAt:                    now.Add(time.Hour).UnixNano(),
		MaxNumberOfLocksPerNamespace: 10,
	})
	require.NoError(err)
	require.True(response1.Success)

	// T+1m: Acquire lock in namespace_2
	response2, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
		LockId:                       lockId2,
		Now:                          now.Add(time.Minute).UnixNano(),
		ProcessId:                    "process_2",
		WriteLock:                    false,
		ExpiresAt:                    now.Add(time.Minute).Add(time.Hour).UnixNano(),
		MaxNumberOfLocksPerNamespace: 10,
	})
	require.NoError(err)
	require.True(response2.Success)

	// T+2m: List locks in namespace_1
	response3, err := locksCore.ListLocks(&corepb.ListLocksRequest{
		NamespaceTimestampedId: &corepb.NamespaceTimestampedId{
			AccountId:          accountId,
			NamespaceName:      "namespace_1",
			NamespaceCreatedAt: now.Unix(),
		},
		Now: now.Add(2 * time.Minute).UnixNano(),
	})

	require.NoError(err)
	require.NotNil(response3)
	require.Len(response3.Locks, 1)
	require.Equal("lock_1", response3.Locks[0].Id.LockName)
	require.Equal("namespace_1", response3.Locks[0].Id.NamespaceName)

	// T+3m: List locks in namespace_2
	response4, err := locksCore.ListLocks(&corepb.ListLocksRequest{
		NamespaceTimestampedId: &corepb.NamespaceTimestampedId{
			AccountId:          accountId,
			NamespaceName:      "namespace_2",
			NamespaceCreatedAt: now.Unix(),
		},
		Now: now.Add(3 * time.Minute).UnixNano(),
	})

	require.NoError(err)
	require.NotNil(response4)
	require.Len(response4.Locks, 1)
	require.Equal("lock_2", response4.Locks[0].Id.LockName)
	require.Equal("namespace_2", response4.Locks[0].Id.NamespaceName)

	// T+4m: List locks in non-existent namespace
	response5, err := locksCore.ListLocks(&corepb.ListLocksRequest{
		NamespaceTimestampedId: &corepb.NamespaceTimestampedId{
			AccountId:          accountId,
			NamespaceName:      "namespace_3",
			NamespaceCreatedAt: now.Unix(),
		},
		Now: now.Add(4 * time.Minute).UnixNano(),
	})

	require.NoError(err)
	require.NotNil(response5)
	require.Empty(response5.Locks)
}

func TestListLocksWithMixedLockStates(t *testing.T) {
	require := require.New(t)

	locksCore := newLocksCore()

	now := time.Now()

	accountId := rand.Uint64()
	namespaceId := &corepb.NamespaceId{
		AccountId:     accountId,
		NamespaceName: "test_namespace",
	}

	// Create locks with different states
	lockId1 := &corepb.LockId{
		AccountId:          accountId,
		NamespaceName:      "test_namespace",
		NamespaceCreatedAt: now.Unix(),
		LockName:           "write_lock",
	}

	lockId2 := &corepb.LockId{
		AccountId:          accountId,
		NamespaceName:      "test_namespace",
		NamespaceCreatedAt: now.Unix(),
		LockName:           "read_lock_single",
	}

	lockId3 := &corepb.LockId{
		AccountId:          accountId,
		NamespaceName:      "test_namespace",
		NamespaceCreatedAt: now.Unix(),
		LockName:           "read_lock_multiple",
	}

	// T+0: Acquire write lock
	response1, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
		LockId:                       lockId1,
		Now:                          now.UnixNano(),
		ProcessId:                    "process_1",
		WriteLock:                    true,
		ExpiresAt:                    now.Add(time.Hour).UnixNano(),
		MaxNumberOfLocksPerNamespace: 10,
	})
	require.NoError(err)
	require.True(response1.Success)

	// T+1m: Acquire read lock
	response2, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
		LockId:                       lockId2,
		Now:                          now.Add(time.Minute).UnixNano(),
		ProcessId:                    "process_2",
		WriteLock:                    false,
		ExpiresAt:                    now.Add(time.Minute).Add(time.Hour).UnixNano(),
		MaxNumberOfLocksPerNamespace: 10,
	})
	require.NoError(err)
	require.True(response2.Success)

	// T+2m: Acquire read lock with multiple holders
	response3, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
		LockId:                       lockId3,
		Now:                          now.Add(2 * time.Minute).UnixNano(),
		ProcessId:                    "process_3",
		WriteLock:                    false,
		ExpiresAt:                    now.Add(2 * time.Minute).Add(time.Hour).UnixNano(),
		MaxNumberOfLocksPerNamespace: 10,
	})
	require.NoError(err)
	require.True(response3.Success)

	// T+3m: Add another read lock holder
	response4, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
		LockId:                       lockId3,
		Now:                          now.Add(3 * time.Minute).UnixNano(),
		ProcessId:                    "process_4",
		WriteLock:                    false,
		ExpiresAt:                    now.Add(3 * time.Minute).Add(time.Hour).UnixNano(),
		MaxNumberOfLocksPerNamespace: 10,
	})
	require.NoError(err)
	require.True(response4.Success)

	// T+4m: List locks
	response5, err := locksCore.ListLocks(&corepb.ListLocksRequest{
		NamespaceTimestampedId: &corepb.NamespaceTimestampedId{
			AccountId:          namespaceId.AccountId,
			NamespaceName:      namespaceId.NamespaceName,
			NamespaceCreatedAt: now.Unix(),
		},
		Now: now.Add(4 * time.Minute).UnixNano(),
	})

	require.NoError(err)
	require.NotNil(response5)
	require.Len(response5.Locks, 3)

	// Verify lock states and holders
	lockMap := make(map[string]*corepb.Lock)
	for _, lock := range response5.Locks {
		lockMap[lock.Id.LockName] = lock
	}

	// Check write lock
	writeLock := lockMap["write_lock"]
	require.Equal(corepb.LockState_WRITE_LOCKED, writeLock.State)
	require.NotNil(writeLock.WriteLockHolder)
	require.Equal("process_1", writeLock.WriteLockHolder.ProcessId)
	require.Nil(writeLock.ReadLockHolders)

	// Check single read lock
	readLockSingle := lockMap["read_lock_single"]
	require.Equal(corepb.LockState_READ_LOCKED, readLockSingle.State)
	require.Nil(readLockSingle.WriteLockHolder)
	require.Len(readLockSingle.ReadLockHolders, 1)
	require.Equal("process_2", readLockSingle.ReadLockHolders[0].ProcessId)

	// Check multiple read lock
	readLockMultiple := lockMap["read_lock_multiple"]
	require.Equal(corepb.LockState_READ_LOCKED, readLockMultiple.State)
	require.Nil(readLockMultiple.WriteLockHolder)
	require.Len(readLockMultiple.ReadLockHolders, 2)
	require.Equal("process_3", readLockMultiple.ReadLockHolders[0].ProcessId)
	require.Equal("process_4", readLockMultiple.ReadLockHolders[1].ProcessId)
}

func TestListLocksWithAllExpiredLocks(t *testing.T) {
	require := require.New(t)

	locksCore := newLocksCore()

	now := time.Now()

	accountId := rand.Uint64()
	namespaceId := &corepb.NamespaceId{
		AccountId:     accountId,
		NamespaceName: "test_namespace",
	}

	// Create locks that will expire
	lockId1 := &corepb.LockId{
		AccountId:          accountId,
		NamespaceName:      "test_namespace",
		NamespaceCreatedAt: now.Unix(),
		LockName:           "lock_1",
	}

	lockId2 := &corepb.LockId{
		AccountId:          accountId,
		NamespaceName:      "test_namespace",
		NamespaceCreatedAt: now.Unix(),
		LockName:           "lock_2",
	}

	// T+0: Acquire locks with short expiration
	response1, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
		LockId:                       lockId1,
		Now:                          now.UnixNano(),
		ProcessId:                    "process_1",
		WriteLock:                    true,
		ExpiresAt:                    now.Add(time.Minute).UnixNano(), // Expires at T+1m
		MaxNumberOfLocksPerNamespace: 10,
	})
	require.NoError(err)
	require.True(response1.Success)

	response2, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
		LockId:                       lockId2,
		Now:                          now.UnixNano(),
		ProcessId:                    "process_2",
		WriteLock:                    false,
		ExpiresAt:                    now.Add(2 * time.Minute).UnixNano(), // Expires at T+2m
		MaxNumberOfLocksPerNamespace: 10,
	})
	require.NoError(err)
	require.True(response2.Success)

	// T+3m: List locks (after all locks have expired)
	response3, err := locksCore.ListLocks(&corepb.ListLocksRequest{
		NamespaceTimestampedId: &corepb.NamespaceTimestampedId{
			AccountId:          namespaceId.AccountId,
			NamespaceName:      namespaceId.NamespaceName,
			NamespaceCreatedAt: now.Unix(),
		},
		Now: now.Add(3 * time.Minute).UnixNano(),
	})

	require.NoError(err)
	require.NotNil(response3)
	require.Empty(response3.Locks) // No locks should be returned as they're all expired
}

func TestAcquireLockMaxNumberOfLocksPerNamespace(t *testing.T) {
	require := require.New(t)

	locksCore := newLocksCore()

	now := time.Now()
	accountId1 := rand.Uint64()
	accountId2 := rand.Uint64()
	maxLocksPerNamespace := int64(3)

	// Create locks up to the maximum limit
	for i := 0; i < int(maxLocksPerNamespace); i++ {
		lockId := &corepb.LockId{
			AccountId:          accountId1,
			NamespaceName:      "test_namespace",
			NamespaceCreatedAt: now.Unix(),
			LockName:           fmt.Sprintf("lock_%d", i),
		}

		response, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       lockId,
			Now:                          now.UnixNano(),
			ProcessId:                    fmt.Sprintf("process_%d", i),
			WriteLock:                    false,
			ExpiresAt:                    now.Add(time.Hour).UnixNano(),
			MaxNumberOfLocksPerNamespace: maxLocksPerNamespace,
		})

		require.NoError(err)
		require.NotNil(response.Lock)
		require.Equal(true, response.Success)
		require.Equal(corepb.LockState_READ_LOCKED, response.Lock.State)
	}

	// Try to acquire one more lock - this should fail
	lockId := &corepb.LockId{
		AccountId:          accountId1,
		NamespaceName:      "test_namespace",
		NamespaceCreatedAt: now.Unix(),
		LockName:           "lock_exceeding_limit",
	}

	response, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
		LockId:                       lockId,
		Now:                          now.Add(time.Second).UnixNano(),
		ProcessId:                    "process_exceeding_limit",
		WriteLock:                    false,
		ExpiresAt:                    now.Add(time.Second).Add(time.Hour).UnixNano(),
		MaxNumberOfLocksPerNamespace: maxLocksPerNamespace,
	})

	require.Error(err)
	require.Contains(err.Error(), "max number of locks per namespace reached")

	// Verify that the lock was not created
	getResponse, err := locksCore.GetLock(&corepb.GetLockRequest{
		LockId: lockId,
		Now:    now.Add(time.Second).UnixNano(),
	})
	require.NoError(err)
	require.NotNil(getResponse.Lock)
	require.Equal(corepb.LockState_UNLOCKED, getResponse.Lock.State)

	// Test that reusing an existing lock (even if expired) doesn't count against the limit
	existingLockId := &corepb.LockId{
		AccountId:          accountId1,
		NamespaceName:      "test_namespace",
		NamespaceCreatedAt: now.Unix(),
		LockName:           "lock_0",
	}

	// Now try to acquire the same lock again - this should succeed because it's reusing an existing lock
	response, err = locksCore.AcquireLock(&corepb.AcquireLockRequest{
		LockId:                       existingLockId,
		Now:                          now.Add(time.Second * 2).UnixNano(),
		ProcessId:                    "process_reuse",
		WriteLock:                    false,
		ExpiresAt:                    now.Add(time.Second * 2).Add(time.Hour).UnixNano(),
		MaxNumberOfLocksPerNamespace: maxLocksPerNamespace,
	})

	require.NoError(err)
	require.NotNil(response.Lock)
	require.Equal(true, response.Success)
	require.Equal(corepb.LockState_READ_LOCKED, response.Lock.State)

	// Let's release one of the existing locks (for both holders)
	_, err = locksCore.ReleaseLock(&corepb.ReleaseLockRequest{
		LockId:    existingLockId,
		ProcessId: "process_reuse",
		Now:       now.Add(time.Second * 3).UnixNano(),
	})
	require.NoError(err)
	_, err = locksCore.ReleaseLock(&corepb.ReleaseLockRequest{
		LockId:    existingLockId,
		ProcessId: "process_0",
		Now:       now.Add(time.Second * 3).UnixNano(),
	})
	require.NoError(err)

	// Now try to acquire another lock again
	response, err = locksCore.AcquireLock(&corepb.AcquireLockRequest{
		LockId: &corepb.LockId{
			AccountId:          accountId1,
			NamespaceName:      "test_namespace",
			NamespaceCreatedAt: now.Unix(),
			LockName:           "lock_4",
		},
		Now:                          now.Add(time.Second * 4).UnixNano(),
		ProcessId:                    "process_4",
		WriteLock:                    true,
		ExpiresAt:                    now.Add(time.Second * 4).Add(time.Hour).UnixNano(),
		MaxNumberOfLocksPerNamespace: maxLocksPerNamespace,
	})

	require.NoError(err)
	require.NotNil(response.Lock)
	require.Equal(true, response.Success)
	require.Equal(corepb.LockState_WRITE_LOCKED, response.Lock.State)

	// Test that creating a lock in a different namespace doesn't affect the limit
	differentNamespaceLockId := &corepb.LockId{
		AccountId:          accountId1,
		NamespaceName:      "different_namespace",
		NamespaceCreatedAt: now.Unix(),
		LockName:           "lock_different_namespace",
	}

	response, err = locksCore.AcquireLock(&corepb.AcquireLockRequest{
		LockId:                       differentNamespaceLockId,
		Now:                          now.UnixNano(),
		ProcessId:                    "process_different_namespace",
		WriteLock:                    true,
		ExpiresAt:                    now.Add(time.Hour).UnixNano(),
		MaxNumberOfLocksPerNamespace: maxLocksPerNamespace,
	})

	require.NoError(err)
	require.NotNil(response.Lock)
	require.Equal(true, response.Success)
	require.Equal(corepb.LockState_WRITE_LOCKED, response.Lock.State)

	// Test that creating a lock with a different account doesn't affect the limit
	differentAccountLockId := &corepb.LockId{
		AccountId:          accountId2,
		NamespaceName:      "test_namespace",
		NamespaceCreatedAt: now.Unix(),
		LockName:           "lock_different_account",
	}

	response, err = locksCore.AcquireLock(&corepb.AcquireLockRequest{
		LockId:                       differentAccountLockId,
		Now:                          now.UnixNano(),
		ProcessId:                    "process_different_account",
		WriteLock:                    true,
		ExpiresAt:                    now.Add(time.Hour).UnixNano(),
		MaxNumberOfLocksPerNamespace: maxLocksPerNamespace,
	})

	require.NoError(err)
	require.NotNil(response.Lock)
	require.Equal(true, response.Success)
	require.Equal(corepb.LockState_WRITE_LOCKED, response.Lock.State)
}

func TestRunLocksGarbageCollectionWithDeletedNamespace(t *testing.T) {
	require := require.New(t)

	locksCore := newLocksCore()

	now := time.Now()
	accountId := rand.Uint64()
	namespaceName := "test_namespace_for_gc"

	namespaceId := &corepb.NamespaceId{
		AccountId:     accountId,
		NamespaceName: namespaceName,
	}

	// Create some locks in the namespace
	lockIds := make([]*corepb.LockId, 10)
	for i := 0; i < 10; i++ {
		lockIds[i] = &corepb.LockId{
			AccountId:          accountId,
			NamespaceName:      namespaceName,
			NamespaceCreatedAt: now.Unix(),
			LockName:           fmt.Sprintf("lock_%d", i),
		}
	}

	// Acquire locks in the namespace
	for i, lockId := range lockIds {
		response, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       lockId,
			Now:                          now.UnixNano(),
			ProcessId:                    fmt.Sprintf("process_%d", i),
			WriteLock:                    i%2 == 0, // Alternate between write and read locks
			ExpiresAt:                    now.Add(time.Hour).UnixNano(),
			MaxNumberOfLocksPerNamespace: 100,
		})

		require.NoError(err)
		require.NotNil(response.Lock)
		require.Equal(true, response.Success)
	}

	// Verify that locks in a different namespace are accessible after GC
	differentNamespaceLockId := &corepb.LockId{
		AccountId:          accountId,
		NamespaceName:      "different_namespace",
		NamespaceCreatedAt: now.Unix(),
		LockName:           "different_lock",
	}

	// Acquire a lock in a different namespace
	acquireResponse, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
		LockId:                       differentNamespaceLockId,
		Now:                          now.UnixNano(),
		ProcessId:                    "process_different",
		WriteLock:                    true,
		ExpiresAt:                    now.Add(time.Hour).UnixNano(),
		MaxNumberOfLocksPerNamespace: 10,
	})

	require.NoError(err)
	require.NotNil(acquireResponse.Lock)
	require.Equal(true, acquireResponse.Success)

	// Verify locks exist by getting them
	for _, lockId := range lockIds {
		response, err := locksCore.GetLock(&corepb.GetLockRequest{
			LockId: lockId,
			Now:    now.UnixNano(),
		})

		require.NoError(err)
		require.NotNil(response.Lock)
		require.NotEqual(corepb.LockState_UNLOCKED, response.Lock.State)
	}

	// Mark the namespace as deleted using LocksDeleteNamespace
	deleteResponse, err := locksCore.LocksDeleteNamespace(&corepb.LocksDeleteNamespaceRequest{
		NamespaceTimestampedId: &corepb.NamespaceTimestampedId{
			AccountId:          namespaceId.AccountId,
			NamespaceName:      namespaceId.NamespaceName,
			NamespaceCreatedAt: now.Unix(),
		},
		Now: now.UnixNano(),
	})

	require.NoError(err)
	require.NotNil(deleteResponse)

	// Run garbage collection to clean up the deleted namespace
	gcResponse, err := locksCore.RunLocksGarbageCollection(&corepb.RunLocksGarbageCollectionRequest{
		Now:                   now.UnixNano(),
		GcRecordsPageSize:     100,
		GcRecordLocksPageSize: 100,
		MaxVisitedLocks:       1000,
	})

	require.NoError(err)
	require.NotNil(gcResponse)

	// Verify that locks in the deleted namespace are no longer accessible
	for _, lockId := range lockIds {
		response, err := locksCore.GetLock(&corepb.GetLockRequest{
			LockId: lockId,
			Now:    now.UnixNano(),
		})

		require.NoError(err)
		require.NotNil(response.Lock)
		require.Equal(corepb.LockState_UNLOCKED, response.Lock.State)
	}

	// Verify the different namespace lock still exists after GC
	getResponse, err := locksCore.GetLock(&corepb.GetLockRequest{
		LockId: differentNamespaceLockId,
		Now:    now.UnixNano(),
	})

	require.NoError(err)
	require.NotNil(getResponse.Lock)
	require.Equal(corepb.LockState_WRITE_LOCKED, getResponse.Lock.State)
}

func TestGetLockReadLockedWithMultipleHoldersBetweenExpirations(t *testing.T) {
	require := require.New(t)

	locksCore := newLocksCore()

	now := time.Now()

	accountId := rand.Uint64()
	lockId := &corepb.LockId{
		AccountId:          accountId,
		NamespaceName:      "test_namespace",
		NamespaceCreatedAt: now.Unix(),
		LockName:           "test_lock",
	}

	// T+0: Acquire read lock with process_1 (expires at T+30m)
	response1, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
		LockId:                       lockId,
		Now:                          now.UnixNano(),
		ProcessId:                    "process_1",
		WriteLock:                    false,
		ExpiresAt:                    now.Add(30 * time.Minute).UnixNano(),
		MaxNumberOfLocksPerNamespace: 10,
	})

	require.NoError(err)
	require.NotNil(response1.Lock)
	require.Equal(true, response1.Success)
	require.Equal(corepb.LockState_READ_LOCKED, response1.Lock.State)
	require.Len(response1.Lock.ReadLockHolders, 1)
	require.Equal("process_1", response1.Lock.ReadLockHolders[0].ProcessId)

	// T+1m: Acquire read lock with process_2 (expires at T+15m)
	response2, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
		LockId:                       lockId,
		Now:                          now.Add(time.Minute).UnixNano(),
		ProcessId:                    "process_2",
		WriteLock:                    false,
		ExpiresAt:                    now.Add(15 * time.Minute).UnixNano(),
		MaxNumberOfLocksPerNamespace: 10,
	})

	require.NoError(err)
	require.NotNil(response2.Lock)
	require.Equal(true, response2.Success)
	require.Equal(corepb.LockState_READ_LOCKED, response2.Lock.State)
	require.Len(response2.Lock.ReadLockHolders, 2)
	require.Equal("process_1", response2.Lock.ReadLockHolders[0].ProcessId)
	require.Equal("process_2", response2.Lock.ReadLockHolders[1].ProcessId)

	// T+2m: Acquire read lock with process_3 (expires at T+45m)
	response3, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
		LockId:                       lockId,
		Now:                          now.Add(2 * time.Minute).UnixNano(),
		ProcessId:                    "process_3",
		WriteLock:                    false,
		ExpiresAt:                    now.Add(45 * time.Minute).UnixNano(),
		MaxNumberOfLocksPerNamespace: 10,
	})

	require.NoError(err)
	require.NotNil(response3.Lock)
	require.Equal(true, response3.Success)
	require.Equal(corepb.LockState_READ_LOCKED, response3.Lock.State)
	require.Len(response3.Lock.ReadLockHolders, 3)
	require.Equal("process_1", response3.Lock.ReadLockHolders[0].ProcessId)
	require.Equal("process_2", response3.Lock.ReadLockHolders[1].ProcessId)
	require.Equal("process_3", response3.Lock.ReadLockHolders[2].ProcessId)

	// T+20m: Get lock at time between process_2 expiration (T+15m) and process_1 expiration (T+30m)
	// process_2 should have expired, but process_1 and process_3 should still be active
	response4, err := locksCore.GetLock(&corepb.GetLockRequest{
		LockId: lockId,
		Now:    now.Add(20 * time.Minute).UnixNano(),
	})

	require.NoError(err)
	require.NotNil(response4.Lock)
	require.Equal(corepb.LockState_READ_LOCKED, response4.Lock.State)
	require.Len(response4.Lock.ReadLockHolders, 2) // Only process_1 and process_3 should remain

	// Verify the remaining holders are process_1 and process_3
	holderProcessIds := make([]string, len(response4.Lock.ReadLockHolders))
	for i, holder := range response4.Lock.ReadLockHolders {
		holderProcessIds[i] = holder.ProcessId
	}
	require.Contains(holderProcessIds, "process_1")
	require.Contains(holderProcessIds, "process_3")
	require.NotContains(holderProcessIds, "process_2") // process_2 should have expired

	// Verify expiration times are correct for remaining holders
	for _, holder := range response4.Lock.ReadLockHolders {
		if holder.ProcessId == "process_1" {
			require.EqualValues(now.Add(30*time.Minute).UnixNano(), holder.ExpiresAt)
		} else if holder.ProcessId == "process_3" {
			require.EqualValues(now.Add(45*time.Minute).UnixNano(), holder.ExpiresAt)
		}
	}

	// T+50m: Get lock after all holders have expired
	response5, err := locksCore.GetLock(&corepb.GetLockRequest{
		LockId: lockId,
		Now:    now.Add(50 * time.Minute).UnixNano(),
	})

	require.NoError(err)
	require.NotNil(response5.Lock)
	require.Equal(corepb.LockState_UNLOCKED, response5.Lock.State)
	require.Len(response5.Lock.ReadLockHolders, 0)
	require.EqualValues(0, response5.Lock.LockedAt)
}

func TestRunLocksGarbageCollectionWithMultipleExpiringLocks(t *testing.T) {
	require := require.New(t)

	locksCore := newLocksCore()

	now := time.Now()
	accountId := rand.Uint64()
	namespaceName := "test_namespace_gc"

	// Create more locks than MaxVisitedLocks to test the limit
	const numLocks = 15
	const maxVisitedLocks = 10

	// Create locks with different scenarios:
	// - Locks 0-4: All holders will expire (should be deleted)
	// - Locks 5-9: Some holders will expire, some will remain (should be updated)
	// - Locks 10-14: All holders will remain (should be updated but not deleted)

	lockIds := make([]*corepb.LockId, numLocks)
	for i := range numLocks {
		lockIds[i] = &corepb.LockId{
			AccountId:          accountId,
			NamespaceName:      namespaceName,
			NamespaceCreatedAt: now.Unix(),
			LockName:           fmt.Sprintf("lock_%d", i),
		}
	}

	// Acquire locks with different expiration scenarios
	for i, lockId := range lockIds {
		if i < 5 {
			// Locks 0-4: All holders will expire
			response, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
				LockId:                       lockId,
				Now:                          now.UnixNano(),
				ProcessId:                    fmt.Sprintf("process_%d", i),
				WriteLock:                    i%2 == 0,                             // Alternate between write and read locks
				ExpiresAt:                    now.Add(30 * time.Minute).UnixNano(), // Will expire
				MaxNumberOfLocksPerNamespace: 100,
			})
			require.NoError(err)
			require.NotNil(response.Lock)
			require.Equal(true, response.Success)

			// Add a second holder for read locks that will also expire
			if response.Lock.WriteLockHolder == nil {
				response2, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
					LockId:                       lockId,
					Now:                          now.UnixNano(),
					ProcessId:                    fmt.Sprintf("process_%d_second", i),
					WriteLock:                    false,
					ExpiresAt:                    now.Add(30 * time.Minute).UnixNano(), // Will expire
					MaxNumberOfLocksPerNamespace: 100,
				})
				require.NoError(err)
				require.NotNil(response2.Lock)
				require.Equal(true, response2.Success)
			}
		} else if i < 10 {
			// Locks 5-9: Some holders will expire, some will remain
			// For this test, we'll make all locks 5-9 read locks to ensure we can have multiple holders
			response, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
				LockId:                       lockId,
				Now:                          now.UnixNano(),
				ProcessId:                    fmt.Sprintf("process_%d", i),
				WriteLock:                    false,                                // Make all read locks for consistency
				ExpiresAt:                    now.Add(30 * time.Minute).UnixNano(), // Will expire
				MaxNumberOfLocksPerNamespace: 100,
			})
			require.NoError(err)
			require.NotNil(response.Lock)
			require.Equal(true, response.Success)

			// Add a second holder that will remain
			response2, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
				LockId:                       lockId,
				Now:                          now.UnixNano(),
				ProcessId:                    fmt.Sprintf("process_%d_second", i),
				WriteLock:                    false,
				ExpiresAt:                    now.Add(2 * time.Hour).UnixNano(), // Will remain
				MaxNumberOfLocksPerNamespace: 100,
			})
			require.NoError(err)
			require.NotNil(response2.Lock)
			require.Equal(true, response2.Success)
		} else {
			// Locks 10-14: All holders will remain
			response, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
				LockId:                       lockId,
				Now:                          now.UnixNano(),
				ProcessId:                    fmt.Sprintf("process_%d", i),
				WriteLock:                    i%2 == 0,                          // Alternate between write and read locks
				ExpiresAt:                    now.Add(2 * time.Hour).UnixNano(), // Will remain
				MaxNumberOfLocksPerNamespace: 100,
			})
			require.NoError(err)
			require.NotNil(response.Lock)
			require.Equal(true, response.Success)

			// Add a second holder that will also remain
			if response.Lock.WriteLockHolder == nil {
				response2, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
					LockId:                       lockId,
					Now:                          now.UnixNano(),
					ProcessId:                    fmt.Sprintf("process_%d_second", i),
					WriteLock:                    false,
					ExpiresAt:                    now.Add(3 * time.Hour).UnixNano(), // Will remain
					MaxNumberOfLocksPerNamespace: 100,
				})
				require.NoError(err)
				require.NotNil(response2.Lock)
				require.Equal(true, response2.Success)
			}
		}
	}

	// Verify all locks exist and are locked before garbage collection
	for _, lockId := range lockIds {
		response, err := locksCore.GetLock(&corepb.GetLockRequest{
			LockId: lockId,
			Now:    now.UnixNano(),
		})
		require.NoError(err)
		require.NotNil(response.Lock)
		require.NotEqual(corepb.LockState_UNLOCKED, response.Lock.State)
	}

	// Run garbage collection at the moment when some locks expire (T+31 minutes)
	gcTime := now.Add(31 * time.Minute)
	gcResponse, err := locksCore.RunLocksGarbageCollection(&corepb.RunLocksGarbageCollectionRequest{
		Now:                   gcTime.UnixNano(),
		GcRecordsPageSize:     100,
		GcRecordLocksPageSize: 100,
		MaxVisitedLocks:       maxVisitedLocks,
	})

	require.NoError(err)
	require.NotNil(gcResponse)

	// Verify the state of locks after garbage collection
	// Note: We use the public GetLock method which internally calls checkLockExpiration
	// to verify the true state of the locks after garbage collection

	// Locks 0-4 should be unlocked (all holders expired)
	for i := 0; i < 5; i++ {
		response, err := locksCore.GetLock(&corepb.GetLockRequest{
			LockId: lockIds[i],
			Now:    gcTime.UnixNano(),
		})
		require.NoError(err)
		require.NotNil(response.Lock)
		require.Equal(corepb.LockState_UNLOCKED, response.Lock.State, "Lock %d should be unlocked", i)
	}

	// Locks 5-9 should still be locked but with fewer holders
	for i := 5; i < 10; i++ {
		response, err := locksCore.GetLock(&corepb.GetLockRequest{
			LockId: lockIds[i],
			Now:    gcTime.UnixNano(),
		})
		require.NoError(err)
		require.NotNil(response.Lock)
		require.NotEqual(corepb.LockState_UNLOCKED, response.Lock.State, "Lock %d should still be locked", i)

		// For read locks, verify only one holder remains
		if response.Lock.State == corepb.LockState_READ_LOCKED {
			require.Len(response.Lock.ReadLockHolders, 1, "Lock %d should have exactly one holder remaining", i)
			require.Equal(fmt.Sprintf("process_%d_second", i), response.Lock.ReadLockHolders[0].ProcessId)
		}
	}

	// Locks 10-14 should still be locked with all holders
	for i := 10; i < numLocks; i++ {
		response, err := locksCore.GetLock(&corepb.GetLockRequest{
			LockId: lockIds[i],
			Now:    gcTime.UnixNano(),
		})
		require.NoError(err)
		require.NotNil(response.Lock)
		require.NotEqual(corepb.LockState_UNLOCKED, response.Lock.State, "Lock %d should still be locked", i)

		// For read locks, verify both holders remain
		if response.Lock.State == corepb.LockState_READ_LOCKED {
			require.Len(response.Lock.ReadLockHolders, 2, "Lock %d should have both holders remaining", i)
			holderProcessIds := make([]string, len(response.Lock.ReadLockHolders))
			for j, holder := range response.Lock.ReadLockHolders {
				holderProcessIds[j] = holder.ProcessId
			}
			require.Contains(holderProcessIds, fmt.Sprintf("process_%d", i))
			require.Contains(holderProcessIds, fmt.Sprintf("process_%d_second", i))
		}
	}

	// Run garbage collection again to process the remaining locks
	// This should process locks 5-14 since locks 0-4 were already deleted
	gcResponse2, err := locksCore.RunLocksGarbageCollection(&corepb.RunLocksGarbageCollectionRequest{
		Now:                   gcTime.UnixNano(),
		GcRecordsPageSize:     100,
		GcRecordLocksPageSize: 100,
		MaxVisitedLocks:       maxVisitedLocks,
	})

	require.NoError(err)
	require.NotNil(gcResponse2)

	// Verify that locks 5-9 still have their remaining holders
	for i := 5; i < 10; i++ {
		response, err := locksCore.GetLock(&corepb.GetLockRequest{
			LockId: lockIds[i],
			Now:    gcTime.UnixNano(),
		})
		require.NoError(err)
		require.NotNil(response.Lock)
		require.NotEqual(corepb.LockState_UNLOCKED, response.Lock.State, "Lock %d should still be locked after second GC", i)
	}

	// Verify that locks 10-14 still have all their holders
	for i := 10; i < numLocks; i++ {
		response, err := locksCore.GetLock(&corepb.GetLockRequest{
			LockId: lockIds[i],
			Now:    gcTime.UnixNano(),
		})
		require.NoError(err)
		require.NotNil(response.Lock)
		require.NotEqual(corepb.LockState_UNLOCKED, response.Lock.State, "Lock %d should still be locked after second GC", i)
	}
}

func newLocksCore() *LocksCore {
	return NewLocksCore(monstera.NewBadgerInMemoryStore(), []byte{0x1d, 0x36, 0x00, 0x00}, []byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})
}
