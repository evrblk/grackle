package locks

import (
	"bytes"
	"fmt"
	"io"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/evrblk/grackle/pkg/corepb"
	"github.com/evrblk/monstera/store"
)

func TestCore_AcquireLock(t *testing.T) {
	t.Run("exclusive", func(t *testing.T) {
		locksCore := newLocksCore(t)

		now := time.Now()

		lockId := &corepb.LockId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
			LockName:    "test_lock",
		}

		// T+0: Acquire lock
		response1, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       lockId,
			Now:                          now.UnixNano(),
			ProcessId:                    "process_1",
			Exclusive:                    true,
			ExpiresAt:                    now.Add(time.Hour).UnixNano(),
			MaxNumberOfLocksPerNamespace: 10,
		})

		require.NoError(t, err)
		require.NotNil(t, response1.Lock)
		require.True(t, response1.Success)
		require.EqualValues(t, now.UnixNano(), response1.Lock.LockedAt)
		require.Equal(t, corepb.LockState_EXCLUSIVE_LOCKED, response1.Lock.State)
		require.Equal(t, "process_1", response1.Lock.LockHolders[0].ProcessId)
		require.EqualValues(t, now.Add(time.Hour).UnixNano(), response1.Lock.LockHolders[0].ExpiresAt)
		require.EqualValues(t, now.UnixNano(), response1.Lock.LockHolders[0].LockedAt)

		// T+1m: Get lock
		response2, err := locksCore.GetLock(&corepb.GetLockRequest{
			LockId: lockId,
			Now:    now.Add(time.Minute).UnixNano(),
		})

		require.NoError(t, err)
		require.NotNil(t, response2.Lock)
		require.Equal(t, corepb.LockState_EXCLUSIVE_LOCKED, response1.Lock.State)

		// T+61m: Get lock
		response3, err := locksCore.GetLock(&corepb.GetLockRequest{
			LockId: lockId,
			Now:    now.Add(61 * time.Minute).UnixNano(),
		})

		require.NoError(t, err)
		require.NotNil(t, response3.Lock)
		require.Equal(t, corepb.LockState_UNLOCKED, response3.Lock.State)
	})

	t.Run("shared lock", func(t *testing.T) {
		locksCore := newLocksCore(t)

		now := time.Now()

		lockId := &corepb.LockId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
			LockName:    "test_lock",
		}

		// T+0: Acquire lock
		response1, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       lockId,
			Now:                          now.UnixNano(),
			ProcessId:                    "process_1",
			Exclusive:                    false,
			ExpiresAt:                    now.Add(time.Hour).UnixNano(),
			MaxNumberOfLocksPerNamespace: 10,
		})

		require.NoError(t, err)
		require.NotNil(t, response1.Lock)
		require.True(t, response1.Success)
		require.EqualValues(t, now.UnixNano(), response1.Lock.LockedAt)
		require.Equal(t, corepb.LockState_SHARED_LOCKED, response1.Lock.State)
		require.Len(t, response1.Lock.LockHolders, 1)
		require.Equal(t, "process_1", response1.Lock.LockHolders[0].ProcessId)
		require.EqualValues(t, now.Add(time.Hour).UnixNano(), response1.Lock.LockHolders[0].ExpiresAt)
		require.EqualValues(t, now.UnixNano(), response1.Lock.LockHolders[0].LockedAt)

		// T+1m: Get lock
		response2, err := locksCore.GetLock(&corepb.GetLockRequest{
			LockId: lockId,
			Now:    now.Add(time.Minute).UnixNano(),
		})

		require.NoError(t, err)
		require.NotNil(t, response2.Lock)
		require.Equal(t, corepb.LockState_SHARED_LOCKED, response1.Lock.State)

		// T+61m: Get lock
		response3, err := locksCore.GetLock(&corepb.GetLockRequest{
			LockId: lockId,
			Now:    now.Add(61 * time.Minute).UnixNano(),
		})

		require.NoError(t, err)
		require.NotNil(t, response3.Lock)
		require.Equal(t, corepb.LockState_UNLOCKED, response3.Lock.State)
	})

	t.Run("exclusive lock repeatedly", func(t *testing.T) {
		locksCore := newLocksCore(t)

		now := time.Now()

		lockId := &corepb.LockId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
			LockName:    "test_lock",
		}

		// T+0: Acquire lock
		response1, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       lockId,
			Now:                          now.UnixNano(),
			ProcessId:                    "process_1",
			Exclusive:                    true,
			ExpiresAt:                    now.Add(time.Hour).UnixNano(),
			MaxNumberOfLocksPerNamespace: 10,
		})

		require.NoError(t, err)
		require.True(t, response1.Success)
		require.Equal(t, corepb.LockState_EXCLUSIVE_LOCKED, response1.Lock.State)

		// T+1m: Acquire lock again
		response2, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       lockId,
			Now:                          now.Add(time.Minute).UnixNano(),
			ProcessId:                    "process_1",
			Exclusive:                    true,
			ExpiresAt:                    now.Add(time.Minute).Add(time.Hour).UnixNano(),
			MaxNumberOfLocksPerNamespace: 10,
		})

		require.NoError(t, err)
		require.True(t, response2.Success)
		require.Equal(t, corepb.LockState_EXCLUSIVE_LOCKED, response2.Lock.State)
		require.EqualValues(t, now.Add(time.Minute).Add(time.Hour).UnixNano(), response2.Lock.LockHolders[0].ExpiresAt)
	})

	t.Run("shared lock repeatedly", func(t *testing.T) {
		locksCore := newLocksCore(t)

		now := time.Now()

		lockId := &corepb.LockId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
			LockName:    "test_lock",
		}

		// T+0: Acquire lock
		response1, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       lockId,
			Now:                          now.UnixNano(),
			ProcessId:                    "process_1",
			Exclusive:                    false,
			ExpiresAt:                    now.Add(time.Hour).UnixNano(),
			MaxNumberOfLocksPerNamespace: 10,
		})

		require.NoError(t, err)
		require.True(t, response1.Success)
		require.Equal(t, corepb.LockState_SHARED_LOCKED, response1.Lock.State)

		// T+1m: Acquire lock again
		response2, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       lockId,
			Now:                          now.Add(time.Minute).UnixNano(),
			ProcessId:                    "process_1",
			Exclusive:                    false,
			ExpiresAt:                    now.Add(time.Minute).Add(time.Hour).UnixNano(),
			MaxNumberOfLocksPerNamespace: 10,
		})

		require.NoError(t, err)
		require.True(t, response2.Success)
		require.Equal(t, corepb.LockState_SHARED_LOCKED, response2.Lock.State)
		require.EqualValues(t, now.Add(time.Minute).Add(time.Hour).UnixNano(), response2.Lock.LockHolders[0].ExpiresAt)
	})

	t.Run("exclusive locked by another process", func(t *testing.T) {
		locksCore := newLocksCore(t)

		now := time.Now()

		lockId := &corepb.LockId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
			LockName:    "test_lock",
		}

		// T+0: Acquire lock
		response1, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       lockId,
			Now:                          now.UnixNano(),
			ProcessId:                    "process_1",
			Exclusive:                    true,
			ExpiresAt:                    now.Add(time.Hour).UnixNano(),
			MaxNumberOfLocksPerNamespace: 10,
		})

		require.NoError(t, err)
		require.True(t, response1.Success)
		require.Equal(t, corepb.LockState_EXCLUSIVE_LOCKED, response1.Lock.State)

		// T+1m: Acquire write lock by another process
		response2, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       lockId,
			Now:                          now.Add(time.Minute).UnixNano(),
			ProcessId:                    "process_2",
			Exclusive:                    true,
			ExpiresAt:                    now.Add(time.Minute).Add(time.Hour).UnixNano(),
			MaxNumberOfLocksPerNamespace: 10,
		})

		require.NoError(t, err)
		require.False(t, response2.Success)
		require.Equal(t, corepb.LockState_EXCLUSIVE_LOCKED, response2.Lock.State)
		require.Equal(t, "process_1", response2.Lock.LockHolders[0].ProcessId)

		// T+2m: Acquire read lock by another process
		response3, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       lockId,
			Now:                          now.Add(2 * time.Minute).UnixNano(),
			ProcessId:                    "process_2",
			Exclusive:                    false,
			ExpiresAt:                    now.Add(2 * time.Minute).Add(time.Hour).UnixNano(),
			MaxNumberOfLocksPerNamespace: 10,
		})

		require.NoError(t, err)
		require.False(t, response3.Success)
		require.Equal(t, corepb.LockState_EXCLUSIVE_LOCKED, response3.Lock.State)
		require.Equal(t, "process_1", response3.Lock.LockHolders[0].ProcessId)
	})

	t.Run("shared locked by another process", func(t *testing.T) {
		locksCore := newLocksCore(t)

		now := time.Now()

		lockId := &corepb.LockId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
			LockName:    "test_lock",
		}

		// T+0: Acquire lock
		response1, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       lockId,
			Now:                          now.UnixNano(),
			ProcessId:                    "process_1",
			Exclusive:                    false,
			ExpiresAt:                    now.Add(time.Hour).UnixNano(),
			MaxNumberOfLocksPerNamespace: 10,
		})

		require.NoError(t, err)
		require.True(t, response1.Success)
		require.Equal(t, corepb.LockState_SHARED_LOCKED, response1.Lock.State)

		// T+1m: Acquire exclusive lock by another process
		response2, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       lockId,
			Now:                          now.Add(time.Minute).UnixNano(),
			ProcessId:                    "process_2",
			Exclusive:                    true,
			ExpiresAt:                    now.Add(time.Minute).Add(time.Hour).UnixNano(),
			MaxNumberOfLocksPerNamespace: 10,
		})

		require.NoError(t, err)
		require.False(t, response2.Success)
		require.Equal(t, corepb.LockState_SHARED_LOCKED, response2.Lock.State)
		require.Equal(t, "process_1", response2.Lock.LockHolders[0].ProcessId)

		// T+2m: Acquire shared lock by another process
		response3, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       lockId,
			Now:                          now.Add(2 * time.Minute).UnixNano(),
			ProcessId:                    "process_2",
			Exclusive:                    false,
			ExpiresAt:                    now.Add(2 * time.Minute).Add(time.Hour).UnixNano(),
			MaxNumberOfLocksPerNamespace: 10,
		})

		require.NoError(t, err)
		require.True(t, response3.Success)
		require.Equal(t, corepb.LockState_SHARED_LOCKED, response3.Lock.State)
		require.Len(t, response3.Lock.LockHolders, 2)
		require.Equal(t, "process_1", response3.Lock.LockHolders[0].ProcessId)
		require.Equal(t, "process_2", response3.Lock.LockHolders[1].ProcessId)
	})

	t.Run("maximum number of locks per namespace", func(t *testing.T) {
		locksCore := newLocksCore(t)

		now := time.Now()
		accountId1 := rand.Uint64()
		accountId2 := rand.Uint64()
		namespaceId1 := rand.Uint32()
		maxLocksPerNamespace := int64(3)

		// Create locks up to the maximum limit
		for i := 0; i < int(maxLocksPerNamespace); i++ {
			lockId := &corepb.LockId{
				AccountId:   accountId1,
				NamespaceId: namespaceId1,
				LockName:    fmt.Sprintf("lock_%d", i),
			}

			response, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
				LockId:                       lockId,
				Now:                          now.UnixNano(),
				ProcessId:                    fmt.Sprintf("process_%d", i),
				Exclusive:                    false,
				ExpiresAt:                    now.Add(time.Hour).UnixNano(),
				MaxNumberOfLocksPerNamespace: maxLocksPerNamespace,
			})

			require.NoError(t, err)
			require.NotNil(t, response.Lock)
			require.True(t, response.Success)
			require.Equal(t, corepb.LockState_SHARED_LOCKED, response.Lock.State)
		}

		// Try to acquire one more lock - this should fail
		lockId := &corepb.LockId{
			AccountId:   accountId1,
			NamespaceId: namespaceId1,
			LockName:    "lock_exceeding_limit",
		}

		response, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       lockId,
			Now:                          now.Add(time.Second).UnixNano(),
			ProcessId:                    "process_exceeding_limit",
			Exclusive:                    false,
			ExpiresAt:                    now.Add(time.Second).Add(time.Hour).UnixNano(),
			MaxNumberOfLocksPerNamespace: maxLocksPerNamespace,
		})

		require.Error(t, err)
		require.Contains(t, err.Error(), "max number of locks per namespace reached")

		// Verify that the lock was not created
		getResponse, err := locksCore.GetLock(&corepb.GetLockRequest{
			LockId: lockId,
			Now:    now.Add(time.Second).UnixNano(),
		})
		require.NoError(t, err)
		require.NotNil(t, getResponse.Lock)
		require.Equal(t, corepb.LockState_UNLOCKED, getResponse.Lock.State)

		// Test that reusing an existing lock (even if expired) doesn't count against the limit
		existingLockId := &corepb.LockId{
			AccountId:   accountId1,
			NamespaceId: namespaceId1,
			LockName:    "lock_0",
		}

		// Now try to acquire the same lock again - this should succeed because it's reusing an existing lock
		response, err = locksCore.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       existingLockId,
			Now:                          now.Add(time.Second * 2).UnixNano(),
			ProcessId:                    "process_reuse",
			Exclusive:                    false,
			ExpiresAt:                    now.Add(time.Second * 2).Add(time.Hour).UnixNano(),
			MaxNumberOfLocksPerNamespace: maxLocksPerNamespace,
		})

		require.NoError(t, err)
		require.NotNil(t, response.Lock)
		require.True(t, response.Success)
		require.Equal(t, corepb.LockState_SHARED_LOCKED, response.Lock.State)

		// Let's release one of the existing locks (for both holders)
		_, err = locksCore.ReleaseLock(&corepb.ReleaseLockRequest{
			LockId:    existingLockId,
			ProcessId: "process_reuse",
			Now:       now.Add(time.Second * 3).UnixNano(),
		})
		require.NoError(t, err)
		_, err = locksCore.ReleaseLock(&corepb.ReleaseLockRequest{
			LockId:    existingLockId,
			ProcessId: "process_0",
			Now:       now.Add(time.Second * 3).UnixNano(),
		})
		require.NoError(t, err)

		// Now try to acquire another lock again
		response, err = locksCore.AcquireLock(&corepb.AcquireLockRequest{
			LockId: &corepb.LockId{
				AccountId:   accountId1,
				NamespaceId: namespaceId1,
				LockName:    "lock_4",
			},
			Now:                          now.Add(time.Second * 4).UnixNano(),
			ProcessId:                    "process_4",
			Exclusive:                    true,
			ExpiresAt:                    now.Add(time.Second * 4).Add(time.Hour).UnixNano(),
			MaxNumberOfLocksPerNamespace: maxLocksPerNamespace,
		})

		require.NoError(t, err)
		require.NotNil(t, response.Lock)
		require.True(t, response.Success)
		require.Equal(t, corepb.LockState_EXCLUSIVE_LOCKED, response.Lock.State)

		// Test that creating a lock in a different namespace doesn't affect the limit
		differentNamespaceLockId := &corepb.LockId{
			AccountId:   accountId1,
			NamespaceId: rand.Uint32(),
			LockName:    "lock_different_namespace",
		}

		response, err = locksCore.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       differentNamespaceLockId,
			Now:                          now.UnixNano(),
			ProcessId:                    "process_different_namespace",
			Exclusive:                    true,
			ExpiresAt:                    now.Add(time.Hour).UnixNano(),
			MaxNumberOfLocksPerNamespace: maxLocksPerNamespace,
		})

		require.NoError(t, err)
		require.NotNil(t, response.Lock)
		require.True(t, response.Success)
		require.Equal(t, corepb.LockState_EXCLUSIVE_LOCKED, response.Lock.State)

		// Test that creating a lock with a different account doesn't affect the limit
		differentAccountLockId := &corepb.LockId{
			AccountId:   accountId2,
			NamespaceId: rand.Uint32(),
			LockName:    "lock_different_account",
		}

		response, err = locksCore.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       differentAccountLockId,
			Now:                          now.UnixNano(),
			ProcessId:                    "process_different_account",
			Exclusive:                    true,
			ExpiresAt:                    now.Add(time.Hour).UnixNano(),
			MaxNumberOfLocksPerNamespace: maxLocksPerNamespace,
		})

		require.NoError(t, err)
		require.NotNil(t, response.Lock)
		require.True(t, response.Success)
		require.Equal(t, corepb.LockState_EXCLUSIVE_LOCKED, response.Lock.State)
	})

	t.Run("hierarchical lock name", func(t *testing.T) {
		locksCore := newLocksCore(t)

		now := time.Now()

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		lockIdABC := &corepb.LockId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LockName:    "a/b/c",
		}
		lockIdABD := &corepb.LockId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LockName:    "a/b/d",
		}

		// T+0: Acquire a/b/c exclusive
		response1, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       lockIdABC,
			Now:                          now.UnixNano(),
			ProcessId:                    "process_1",
			Exclusive:                    true,
			ExpiresAt:                    now.Add(time.Hour).UnixNano(),
			MaxNumberOfLocksPerNamespace: 10,
		})

		require.NoError(t, err)
		require.True(t, response1.Success)
		require.Equal(t, corepb.LockState_EXCLUSIVE_LOCKED, response1.Lock.State)

		// Ancestors "a" and "a/b" must reflect the exclusive lock
		txn := locksCore.badgerStore.View()
		anc_a, err := locksCore.ancestors.Get(txn, &corepb.LockId{AccountId: accountId, NamespaceId: namespaceId, LockName: "a"})
		require.NoError(t, err)
		require.EqualValues(t, 1, anc_a.ExclusiveCount)
		require.EqualValues(t, 0, anc_a.SharedCount)

		anc_ab, err := locksCore.ancestors.Get(txn, &corepb.LockId{AccountId: accountId, NamespaceId: namespaceId, LockName: "a/b"})
		require.NoError(t, err)
		require.EqualValues(t, 1, anc_ab.ExclusiveCount)
		require.EqualValues(t, 0, anc_ab.SharedCount)
		txn.Discard()

		// T+1m: Acquire a/b/d shared
		response2, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       lockIdABD,
			Now:                          now.Add(time.Minute).UnixNano(),
			ProcessId:                    "process_2",
			Exclusive:                    false,
			ExpiresAt:                    now.Add(time.Hour).UnixNano(),
			MaxNumberOfLocksPerNamespace: 10,
		})

		require.NoError(t, err)
		require.True(t, response2.Success)
		require.Equal(t, corepb.LockState_SHARED_LOCKED, response2.Lock.State)

		// Ancestor "a" now has both exclusive (a/b/c) and shared (a/b/d) descendants
		txn = locksCore.badgerStore.View()
		anc_a, err = locksCore.ancestors.Get(txn, &corepb.LockId{AccountId: accountId, NamespaceId: namespaceId, LockName: "a"})
		require.NoError(t, err)
		require.EqualValues(t, 1, anc_a.ExclusiveCount)
		require.EqualValues(t, 1, anc_a.SharedCount)

		// Ancestor "a/b" has both as well
		anc_ab, err = locksCore.ancestors.Get(txn, &corepb.LockId{AccountId: accountId, NamespaceId: namespaceId, LockName: "a/b"})
		require.NoError(t, err)
		require.EqualValues(t, 1, anc_ab.ExclusiveCount)
		require.EqualValues(t, 1, anc_ab.SharedCount)
		txn.Discard()

		// T+2m: Release a/b/c
		response3, err := locksCore.ReleaseLock(&corepb.ReleaseLockRequest{
			LockId:    lockIdABC,
			ProcessId: "process_1",
			Now:       now.Add(2 * time.Minute).UnixNano(),
		})

		require.NoError(t, err)
		require.Equal(t, corepb.LockState_UNLOCKED, response3.Lock.State)

		// Ancestor "a" now only has the shared (a/b/d) descendant
		txn = locksCore.badgerStore.View()
		anc_a, err = locksCore.ancestors.Get(txn, &corepb.LockId{AccountId: accountId, NamespaceId: namespaceId, LockName: "a"})
		require.NoError(t, err)
		require.EqualValues(t, 0, anc_a.ExclusiveCount)
		require.EqualValues(t, 1, anc_a.SharedCount)

		anc_ab, err = locksCore.ancestors.Get(txn, &corepb.LockId{AccountId: accountId, NamespaceId: namespaceId, LockName: "a/b"})
		require.NoError(t, err)
		require.EqualValues(t, 0, anc_ab.ExclusiveCount)
		require.EqualValues(t, 1, anc_ab.SharedCount)
		txn.Discard()

		// T+3m: Release a/b/d
		response4, err := locksCore.ReleaseLock(&corepb.ReleaseLockRequest{
			LockId:    lockIdABD,
			ProcessId: "process_2",
			Now:       now.Add(3 * time.Minute).UnixNano(),
		})

		require.NoError(t, err)
		require.Equal(t, corepb.LockState_UNLOCKED, response4.Lock.State)

		// All ancestor entries should be gone
		txn = locksCore.badgerStore.View()
		anc_a, err = locksCore.ancestors.Get(txn, &corepb.LockId{AccountId: accountId, NamespaceId: namespaceId, LockName: "a"})
		require.NoError(t, err)
		require.EqualValues(t, 0, anc_a.ExclusiveCount)
		require.EqualValues(t, 0, anc_a.SharedCount)
		txn.Discard()
	})

	t.Run("acquire child while parent path is exclusively locked", func(t *testing.T) {
		locksCore := newLocksCore(t)

		now := time.Now()

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		lockIdAB := &corepb.LockId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LockName:    "a/b",
		}
		lockIdABC := &corepb.LockId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LockName:    "a/b/c",
		}

		// T+0: process_1 acquires a/b exclusively
		response1, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       lockIdAB,
			Now:                          now.UnixNano(),
			ProcessId:                    "process_1",
			Exclusive:                    true,
			ExpiresAt:                    now.Add(time.Hour).UnixNano(),
			MaxNumberOfLocksPerNamespace: 10,
		})

		require.NoError(t, err)
		require.True(t, response1.Success)
		require.Equal(t, corepb.LockState_EXCLUSIVE_LOCKED, response1.Lock.State)

		// Ancestor "a" reflects the a/b lock
		txn := locksCore.badgerStore.View()
		anc_a, err := locksCore.ancestors.Get(txn, &corepb.LockId{AccountId: accountId, NamespaceId: namespaceId, LockName: "a"})
		require.NoError(t, err)
		require.EqualValues(t, 1, anc_a.ExclusiveCount)
		require.EqualValues(t, 0, anc_a.SharedCount)
		txn.Discard()

		// T+1m: process_2 acquires a/b/c shared — succeeds because a/b and a/b/c are separate locks
		response2, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       lockIdABC,
			Now:                          now.Add(time.Minute).UnixNano(),
			ProcessId:                    "process_2",
			Exclusive:                    false,
			ExpiresAt:                    now.Add(time.Hour).UnixNano(),
			MaxNumberOfLocksPerNamespace: 10,
		})

		require.NoError(t, err)
		require.True(t, response2.Success)
		require.Equal(t, corepb.LockState_SHARED_LOCKED, response2.Lock.State)

		// Ancestor "a" now reflects both locks: a/b (exclusive) and a/b/c (shared)
		txn = locksCore.badgerStore.View()
		anc_a, err = locksCore.ancestors.Get(txn, &corepb.LockId{AccountId: accountId, NamespaceId: namespaceId, LockName: "a"})
		require.NoError(t, err)
		require.EqualValues(t, 1, anc_a.ExclusiveCount) // from a/b
		require.EqualValues(t, 1, anc_a.SharedCount)    // from a/b/c

		// Ancestor "a/b" in the ancestors table reflects only a/b/c (the a/b lock entry is in the locks table)
		anc_ab, err := locksCore.ancestors.Get(txn, &corepb.LockId{AccountId: accountId, NamespaceId: namespaceId, LockName: "a/b"})
		require.NoError(t, err)
		require.EqualValues(t, 0, anc_ab.ExclusiveCount)
		require.EqualValues(t, 1, anc_ab.SharedCount) // from a/b/c
		txn.Discard()

		// T+2m: process_1 releases a/b
		response3, err := locksCore.ReleaseLock(&corepb.ReleaseLockRequest{
			LockId:    lockIdAB,
			ProcessId: "process_1",
			Now:       now.Add(2 * time.Minute).UnixNano(),
		})

		require.NoError(t, err)
		require.Equal(t, corepb.LockState_UNLOCKED, response3.Lock.State)

		// a/b/c is still held; ancestor "a" retains only the shared count from a/b/c
		txn = locksCore.badgerStore.View()
		anc_a, err = locksCore.ancestors.Get(txn, &corepb.LockId{AccountId: accountId, NamespaceId: namespaceId, LockName: "a"})
		require.NoError(t, err)
		require.EqualValues(t, 0, anc_a.ExclusiveCount)
		require.EqualValues(t, 1, anc_a.SharedCount)
		txn.Discard()
	})
}

func TestCore_GetLock(t *testing.T) {
	t.Run("nonexistent", func(t *testing.T) {
		locksCore := newLocksCore(t)

		now := time.Now()

		lockId := &corepb.LockId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
			LockName:    "test_lock",
		}

		// Get lock
		response1, err := locksCore.GetLock(&corepb.GetLockRequest{
			LockId: lockId,
			Now:    now.UnixNano(),
		})

		require.NoError(t, err)
		require.NotNil(t, response1.Lock)
		require.Equal(t, corepb.LockState_UNLOCKED, response1.Lock.State)
	})

	t.Run("shared locked with multiple holders between expirations", func(t *testing.T) {
		locksCore := newLocksCore(t)

		now := time.Now()

		lockId := &corepb.LockId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
			LockName:    "test_lock",
		}

		// T+0: Acquire shared lock with process_1 (expires at T+30m)
		response1, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       lockId,
			Now:                          now.UnixNano(),
			ProcessId:                    "process_1",
			Exclusive:                    false,
			ExpiresAt:                    now.Add(30 * time.Minute).UnixNano(),
			MaxNumberOfLocksPerNamespace: 10,
		})

		require.NoError(t, err)
		require.NotNil(t, response1.Lock)
		require.True(t, response1.Success)
		require.Equal(t, corepb.LockState_SHARED_LOCKED, response1.Lock.State)
		require.Len(t, response1.Lock.LockHolders, 1)
		require.Equal(t, "process_1", response1.Lock.LockHolders[0].ProcessId)

		// T+1m: Acquire shared lock with process_2 (expires at T+15m)
		response2, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       lockId,
			Now:                          now.Add(time.Minute).UnixNano(),
			ProcessId:                    "process_2",
			Exclusive:                    false,
			ExpiresAt:                    now.Add(15 * time.Minute).UnixNano(),
			MaxNumberOfLocksPerNamespace: 10,
		})

		require.NoError(t, err)
		require.NotNil(t, response2.Lock)
		require.True(t, response2.Success)
		require.Equal(t, corepb.LockState_SHARED_LOCKED, response2.Lock.State)
		require.Len(t, response2.Lock.LockHolders, 2)
		require.Equal(t, "process_1", response2.Lock.LockHolders[0].ProcessId)
		require.Equal(t, "process_2", response2.Lock.LockHolders[1].ProcessId)

		// T+2m: Acquire shared lock with process_3 (expires at T+45m)
		response3, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       lockId,
			Now:                          now.Add(2 * time.Minute).UnixNano(),
			ProcessId:                    "process_3",
			Exclusive:                    false,
			ExpiresAt:                    now.Add(45 * time.Minute).UnixNano(),
			MaxNumberOfLocksPerNamespace: 10,
		})

		require.NoError(t, err)
		require.NotNil(t, response3.Lock)
		require.True(t, response3.Success)
		require.Equal(t, corepb.LockState_SHARED_LOCKED, response3.Lock.State)
		require.Len(t, response3.Lock.LockHolders, 3)
		require.Equal(t, "process_1", response3.Lock.LockHolders[0].ProcessId)
		require.Equal(t, "process_2", response3.Lock.LockHolders[1].ProcessId)
		require.Equal(t, "process_3", response3.Lock.LockHolders[2].ProcessId)

		// T+20m: Get lock at time between process_2 expiration (T+15m) and process_1 expiration (T+30m)
		// process_2 should have expired, but process_1 and process_3 should still be active
		response4, err := locksCore.GetLock(&corepb.GetLockRequest{
			LockId: lockId,
			Now:    now.Add(20 * time.Minute).UnixNano(),
		})

		require.NoError(t, err)
		require.NotNil(t, response4.Lock)
		require.Equal(t, corepb.LockState_SHARED_LOCKED, response4.Lock.State)
		require.Len(t, response4.Lock.LockHolders, 2) // Only process_1 and process_3 should remain

		// Verify the remaining holders are process_1 and process_3
		holderProcessIds := make([]string, len(response4.Lock.LockHolders))
		for i, holder := range response4.Lock.LockHolders {
			holderProcessIds[i] = holder.ProcessId
		}
		require.Contains(t, holderProcessIds, "process_1")
		require.Contains(t, holderProcessIds, "process_3")
		require.NotContains(t, holderProcessIds, "process_2") // process_2 should have expired

		// Verify expiration times are correct for remaining holders
		for _, holder := range response4.Lock.LockHolders {
			if holder.ProcessId == "process_1" {
				require.EqualValues(t, now.Add(30*time.Minute).UnixNano(), holder.ExpiresAt)
			} else if holder.ProcessId == "process_3" {
				require.EqualValues(t, now.Add(45*time.Minute).UnixNano(), holder.ExpiresAt)
			}
		}

		// T+50m: Get lock after all holders have expired
		response5, err := locksCore.GetLock(&corepb.GetLockRequest{
			LockId: lockId,
			Now:    now.Add(50 * time.Minute).UnixNano(),
		})

		require.NoError(t, err)
		require.NotNil(t, response5.Lock)
		require.Equal(t, corepb.LockState_UNLOCKED, response5.Lock.State)
		require.Len(t, response5.Lock.LockHolders, 0)
		require.EqualValues(t, 0, response5.Lock.LockedAt)
	})
}

func TestCore_DeleteLock(t *testing.T) {
	t.Run("nonexistent", func(t *testing.T) {
		locksCore := newLocksCore(t)

		now := time.Now()

		lockId := &corepb.LockId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
			LockName:    "test_lock",
		}

		// Delete lock
		_, err := locksCore.DeleteLock(&corepb.DeleteLockRequest{
			LockId: lockId,
			Now:    now.UnixNano(),
		})

		require.NoError(t, err)
	})

	t.Run("delete acquired lock", func(t *testing.T) {
		locksCore := newLocksCore(t)

		now := time.Now()

		lockId := &corepb.LockId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
			LockName:    "test_lock",
		}

		// T+0: Acquire lock
		response1, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       lockId,
			Now:                          now.UnixNano(),
			ProcessId:                    "process_1",
			Exclusive:                    true,
			ExpiresAt:                    now.Add(time.Hour).UnixNano(),
			MaxNumberOfLocksPerNamespace: 10,
		})

		require.NoError(t, err)
		require.NotNil(t, response1.Lock)
		require.True(t, response1.Success)
		require.Equal(t, corepb.LockState_EXCLUSIVE_LOCKED, response1.Lock.State)

		// T+1m: Delete lock
		_, err = locksCore.DeleteLock(&corepb.DeleteLockRequest{
			LockId: lockId,
			Now:    now.Add(time.Minute).UnixNano(),
		})

		require.NoError(t, err)

		// T+2m: Acquire lock
		response3, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       lockId,
			Now:                          now.Add(2 * time.Minute).UnixNano(),
			ProcessId:                    "process_2",
			Exclusive:                    true,
			ExpiresAt:                    now.Add(2 * time.Minute).Add(time.Hour).UnixNano(),
			MaxNumberOfLocksPerNamespace: 10,
		})

		require.NoError(t, err)
		require.NotNil(t, response3.Lock)
		require.True(t, response3.Success)
		require.Equal(t, corepb.LockState_EXCLUSIVE_LOCKED, response3.Lock.State)
		require.Equal(t, "process_2", response3.Lock.LockHolders[0].ProcessId)
	})
}

func TestCore_ReleaseLock(t *testing.T) {
	t.Run("nonexistent lock", func(t *testing.T) {
		locksCore := newLocksCore(t)

		now := time.Now()

		lockId := &corepb.LockId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
			LockName:    "test_lock",
		}

		// Release lock
		response1, err := locksCore.ReleaseLock(&corepb.ReleaseLockRequest{
			LockId:    lockId,
			ProcessId: "process_1",
			Now:       now.UnixNano(),
		})

		require.NoError(t, err)
		require.NotNil(t, response1.Lock)
		require.Equal(t, corepb.LockState_UNLOCKED, response1.Lock.State)
	})

	t.Run("exclusive lock", func(t *testing.T) {
		locksCore := newLocksCore(t)

		now := time.Now()

		lockId := &corepb.LockId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
			LockName:    "test_lock",
		}

		// T+0: Acquire lock
		response1, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       lockId,
			Now:                          now.UnixNano(),
			ProcessId:                    "process_1",
			Exclusive:                    true,
			ExpiresAt:                    now.Add(time.Hour).UnixNano(),
			MaxNumberOfLocksPerNamespace: 10,
		})

		require.NoError(t, err)
		require.NotNil(t, response1.Lock)
		require.True(t, response1.Success)
		require.Equal(t, corepb.LockState_EXCLUSIVE_LOCKED, response1.Lock.State)

		// T+1m: Release lock with wrong process id
		response2, err := locksCore.ReleaseLock(&corepb.ReleaseLockRequest{
			LockId:    lockId,
			ProcessId: "process_2",
			Now:       now.Add(time.Minute).UnixNano(),
		})

		require.NoError(t, err)
		require.NotNil(t, response2.Lock)
		require.Equal(t, corepb.LockState_EXCLUSIVE_LOCKED, response1.Lock.State)
		require.Equal(t, "process_1", response1.Lock.LockHolders[0].ProcessId)

		// T+2m: Release lock with correct process id
		response3, err := locksCore.ReleaseLock(&corepb.ReleaseLockRequest{
			LockId:    lockId,
			Now:       now.Add(2 * time.Minute).UnixNano(),
			ProcessId: "process_1",
		})

		require.NoError(t, err)
		require.NotNil(t, response3.Lock)
		require.Equal(t, corepb.LockState_UNLOCKED, response3.Lock.State)
	})

	t.Run("shared lock", func(t *testing.T) {
		locksCore := newLocksCore(t)

		now := time.Now()

		lockId := &corepb.LockId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
			LockName:    "test_lock",
		}

		// T+0: Acquire read lock
		response1, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       lockId,
			Now:                          now.UnixNano(),
			ProcessId:                    "process_1",
			Exclusive:                    false,
			ExpiresAt:                    now.Add(time.Hour).UnixNano(),
			MaxNumberOfLocksPerNamespace: 10,
		})

		require.NoError(t, err)
		require.NotNil(t, response1.Lock)
		require.True(t, response1.Success)
		require.Equal(t, corepb.LockState_SHARED_LOCKED, response1.Lock.State)

		// T+1m: Acquire read lock from another process
		response2, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       lockId,
			Now:                          now.Add(time.Minute).UnixNano(),
			ProcessId:                    "process_2",
			Exclusive:                    false,
			ExpiresAt:                    now.Add(time.Minute).Add(time.Hour).UnixNano(),
			MaxNumberOfLocksPerNamespace: 10,
		})

		require.NoError(t, err)
		require.NotNil(t, response2.Lock)
		require.True(t, response2.Success)
		require.Equal(t, corepb.LockState_SHARED_LOCKED, response2.Lock.State)

		// T+2m: Release lock with first process id
		response3, err := locksCore.ReleaseLock(&corepb.ReleaseLockRequest{
			LockId:    lockId,
			Now:       now.Add(2 * time.Minute).UnixNano(),
			ProcessId: "process_1",
		})

		require.NoError(t, err)
		require.NotNil(t, response3.Lock)
		require.Equal(t, corepb.LockState_SHARED_LOCKED, response3.Lock.State)
		require.Equal(t, "process_2", response3.Lock.LockHolders[0].ProcessId)

		// T+3m: Release lock with second process id
		response4, err := locksCore.ReleaseLock(&corepb.ReleaseLockRequest{
			LockId:    lockId,
			Now:       now.Add(3 * time.Minute).UnixNano(),
			ProcessId: "process_2",
		})

		require.NoError(t, err)
		require.NotNil(t, response4.Lock)
		require.Equal(t, corepb.LockState_UNLOCKED, response4.Lock.State)
		require.Len(t, response4.Lock.LockHolders, 0)
	})

	t.Run("expired lock", func(t *testing.T) {
		locksCore := newLocksCore(t)

		now := time.Now()

		lockId := &corepb.LockId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
			LockName:    "test_lock",
		}

		// T+0: Acquire lock
		response1, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       lockId,
			Now:                          now.UnixNano(),
			ProcessId:                    "process_1",
			Exclusive:                    true,
			ExpiresAt:                    now.Add(time.Hour).UnixNano(),
			MaxNumberOfLocksPerNamespace: 10,
		})

		require.NoError(t, err)
		require.NotNil(t, response1.Lock)
		require.True(t, response1.Success)
		require.Equal(t, corepb.LockState_EXCLUSIVE_LOCKED, response1.Lock.State)

		// T+61m: Release lock after expiration time
		response2, err := locksCore.ReleaseLock(&corepb.ReleaseLockRequest{
			LockId:    lockId,
			Now:       now.Add(61 * time.Minute).UnixNano(),
			ProcessId: "process_1",
		})

		require.NoError(t, err)
		require.NotNil(t, response2.Lock)
		require.Equal(t, corepb.LockState_UNLOCKED, response2.Lock.State)
	})
}

func TestCore_SnapshotAndRestore(t *testing.T) {
	now := time.Now()

	lockId := &corepb.LockId{
		AccountId:   rand.Uint64(),
		NamespaceId: rand.Uint32(),
		LockName:    "test_lock",
	}

	// Create two lock cores for testing snapshot and restore
	locksCore1 := newLocksCore(t)
	locksCore2 := newLocksCore(t)

	// T+0: Acquire exclusive lock
	response1, err := locksCore1.AcquireLock(&corepb.AcquireLockRequest{
		LockId:                       lockId,
		Now:                          now.UnixNano(),
		ProcessId:                    "process_1",
		Exclusive:                    true,
		ExpiresAt:                    now.Add(time.Hour).UnixNano(),
		MaxNumberOfLocksPerNamespace: 10,
	})
	require.NoError(t, err)
	require.True(t, response1.Success)
	require.Equal(t, corepb.LockState_EXCLUSIVE_LOCKED, response1.Lock.State)

	// Take snapshot at this point
	snapshot := locksCore1.Snapshot()

	// T+1m: Release the exclusive lock (after snapshot)
	_, err = locksCore1.ReleaseLock(&corepb.ReleaseLockRequest{
		LockId:    lockId,
		ProcessId: "process_1",
		Now:       now.Add(time.Minute).UnixNano(),
	})
	require.NoError(t, err)

	// T+2m: Acquire shared lock with different process (after snapshot)
	response2, err := locksCore1.AcquireLock(&corepb.AcquireLockRequest{
		LockId:                       lockId,
		Now:                          now.Add(2 * time.Minute).UnixNano(),
		ProcessId:                    "process_2",
		Exclusive:                    false,
		ExpiresAt:                    now.Add(2 * time.Minute).Add(time.Hour).UnixNano(),
		MaxNumberOfLocksPerNamespace: 10,
	})
	require.NoError(t, err)
	require.True(t, response2.Success)
	require.Equal(t, corepb.LockState_SHARED_LOCKED, response2.Lock.State)

	// Write snapshot to buffer
	buf := bytes.NewBuffer(nil)
	err = snapshot.Write(buf)
	require.NoError(t, err)

	// Restore snapshot to second core
	err = locksCore2.Restore(io.NopCloser(buf))
	require.NoError(t, err)

	// T+3m: Check that the restored state matches the snapshot state
	// The lock should exist with write lock held by process_1
	response3, err := locksCore2.GetLock(&corepb.GetLockRequest{
		LockId: lockId,
		Now:    now.Add(3 * time.Minute).UnixNano(),
	})
	require.NoError(t, err)
	require.NotNil(t, response3.Lock)
	require.Equal(t, corepb.LockState_EXCLUSIVE_LOCKED, response3.Lock.State)
	require.Equal(t, "process_1", response3.Lock.LockHolders[0].ProcessId)

	// T+4m: Try to acquire exclusive lock with different process in restored state (should fail)
	response4, err := locksCore2.AcquireLock(&corepb.AcquireLockRequest{
		LockId:                       lockId,
		Now:                          now.Add(4 * time.Minute).UnixNano(),
		ProcessId:                    "process_3",
		Exclusive:                    true,
		ExpiresAt:                    now.Add(4 * time.Minute).Add(time.Hour).UnixNano(),
		MaxNumberOfLocksPerNamespace: 10,
	})
	require.NoError(t, err)
	require.False(t, response4.Success)
	require.Equal(t, corepb.LockState_EXCLUSIVE_LOCKED, response4.Lock.State)

	// T+5m: Try to acquire shared lock with different process in restored state (should fail)
	response5, err := locksCore2.AcquireLock(&corepb.AcquireLockRequest{
		LockId:                       lockId,
		Now:                          now.Add(5 * time.Minute).UnixNano(),
		ProcessId:                    "process_3",
		Exclusive:                    false,
		ExpiresAt:                    now.Add(5 * time.Minute).Add(time.Hour).UnixNano(),
		MaxNumberOfLocksPerNamespace: 10,
	})
	require.NoError(t, err)
	require.False(t, response5.Success)
	require.Equal(t, corepb.LockState_EXCLUSIVE_LOCKED, response5.Lock.State)

	// T+6m: Release the exclusive lock in restored state
	_, err = locksCore2.ReleaseLock(&corepb.ReleaseLockRequest{
		LockId:    lockId,
		ProcessId: "process_1",
		Now:       now.Add(6 * time.Minute).UnixNano(),
	})
	require.NoError(t, err)

	// T+7m: Verify lock is unlocked in restored state
	response6, err := locksCore2.GetLock(&corepb.GetLockRequest{
		LockId: lockId,
		Now:    now.Add(7 * time.Minute).UnixNano(),
	})
	require.NoError(t, err)
	require.Equal(t, corepb.LockState_UNLOCKED, response6.Lock.State)

	// T+8m: Acquire shared lock in restored state (should succeed)
	response7, err := locksCore2.AcquireLock(&corepb.AcquireLockRequest{
		LockId:                       lockId,
		Now:                          now.Add(8 * time.Minute).UnixNano(),
		ProcessId:                    "process_4",
		Exclusive:                    false,
		ExpiresAt:                    now.Add(8 * time.Minute).Add(time.Hour).UnixNano(),
		MaxNumberOfLocksPerNamespace: 10,
	})
	require.NoError(t, err)
	require.True(t, response7.Success)
	require.Equal(t, corepb.LockState_SHARED_LOCKED, response7.Lock.State)

	// Verify that the original core has different state (it should have a read lock from process_2)
	response8, err := locksCore1.GetLock(&corepb.GetLockRequest{
		LockId: lockId,
		Now:    now.Add(8 * time.Minute).UnixNano(),
	})
	require.NoError(t, err)
	require.Equal(t, corepb.LockState_SHARED_LOCKED, response8.Lock.State)
	require.Len(t, response8.Lock.LockHolders, 1)
	require.Equal(t, "process_2", response8.Lock.LockHolders[0].ProcessId)
}

func TestCore_ListLocks(t *testing.T) {
	t.Run("empty namespace", func(t *testing.T) {
		locksCore := newLocksCore(t)

		now := time.Now()

		namespaceId := &corepb.NamespaceId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
		}

		// List locks in empty namespace
		response, err := locksCore.ListLocks(&corepb.ListLocksRequest{
			NamespaceId: namespaceId,
			Now:         now.UnixNano(),
		})

		require.NoError(t, err)
		require.NotNil(t, response)
		require.Empty(t, response.Locks)
	})

	t.Run("with active locks", func(t *testing.T) {
		locksCore := newLocksCore(t)

		now := time.Now()

		namespaceId := &corepb.NamespaceId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
		}

		// Create multiple locks in the same namespace
		lockId1 := &corepb.LockId{
			AccountId:   namespaceId.AccountId,
			NamespaceId: namespaceId.NamespaceId,
			LockName:    "lock_1",
		}

		lockId2 := &corepb.LockId{
			AccountId:   namespaceId.AccountId,
			NamespaceId: namespaceId.NamespaceId,
			LockName:    "lock_2",
		}

		lockId3 := &corepb.LockId{
			AccountId:   namespaceId.AccountId,
			NamespaceId: namespaceId.NamespaceId,
			LockName:    "lock_3",
		}

		// T+0: Acquire exclusive lock for lock_1
		response1, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       lockId1,
			Now:                          now.UnixNano(),
			ProcessId:                    "process_1",
			Exclusive:                    true,
			ExpiresAt:                    now.Add(time.Hour).UnixNano(),
			MaxNumberOfLocksPerNamespace: 10,
		})
		require.NoError(t, err)
		require.True(t, response1.Success)

		// T+1m: Acquire shared lock for lock_2
		response2, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       lockId2,
			Now:                          now.Add(time.Minute).UnixNano(),
			ProcessId:                    "process_2",
			Exclusive:                    false,
			ExpiresAt:                    now.Add(time.Minute).Add(time.Hour).UnixNano(),
			MaxNumberOfLocksPerNamespace: 10,
		})
		require.NoError(t, err)
		require.True(t, response2.Success)

		// T+2m: Acquire shared lock for lock_3
		response3, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       lockId3,
			Now:                          now.Add(2 * time.Minute).UnixNano(),
			ProcessId:                    "process_3",
			Exclusive:                    false,
			ExpiresAt:                    now.Add(2 * time.Minute).Add(time.Hour).UnixNano(),
			MaxNumberOfLocksPerNamespace: 10,
		})
		require.NoError(t, err)
		require.True(t, response3.Success)

		// T+3m: List locks
		response4, err := locksCore.ListLocks(&corepb.ListLocksRequest{
			NamespaceId: namespaceId,
			Now:         now.Add(3 * time.Minute).UnixNano(),
		})

		require.NoError(t, err)
		require.NotNil(t, response4)
		require.Len(t, response4.Locks, 3)

		// Verify all locks are returned and are in the correct state
		lockMap := make(map[string]*corepb.Lock)
		for _, lock := range response4.Locks {
			lockMap[lock.Id.LockName] = lock
		}

		require.Contains(t, lockMap, "lock_1")
		require.Contains(t, lockMap, "lock_2")
		require.Contains(t, lockMap, "lock_3")

		require.Equal(t, corepb.LockState_EXCLUSIVE_LOCKED, lockMap["lock_1"].State)
		require.Equal(t, corepb.LockState_SHARED_LOCKED, lockMap["lock_2"].State)
		require.Equal(t, corepb.LockState_SHARED_LOCKED, lockMap["lock_3"].State)
	})

	t.Run("with expired locks", func(t *testing.T) {
		locksCore := newLocksCore(t)

		now := time.Now()

		namespaceId := &corepb.NamespaceId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
		}

		// Create locks with different expiration times
		lockId1 := &corepb.LockId{
			AccountId:   namespaceId.AccountId,
			NamespaceId: namespaceId.NamespaceId,
			LockName:    "lock_active",
		}

		lockId2 := &corepb.LockId{
			AccountId:   namespaceId.AccountId,
			NamespaceId: namespaceId.NamespaceId,
			LockName:    "lock_expired",
		}

		// T+0: Acquire lock that will remain active
		response1, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       lockId1,
			Now:                          now.UnixNano(),
			ProcessId:                    "process_1",
			Exclusive:                    true,
			ExpiresAt:                    now.Add(time.Hour).UnixNano(),
			MaxNumberOfLocksPerNamespace: 10,
		})
		require.NoError(t, err)
		require.True(t, response1.Success)

		// T+1m: Acquire lock that will expire
		response2, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       lockId2,
			Now:                          now.Add(time.Minute).UnixNano(),
			ProcessId:                    "process_2",
			Exclusive:                    false,
			ExpiresAt:                    now.Add(2 * time.Minute).UnixNano(), // Will expire at T+3m
			MaxNumberOfLocksPerNamespace: 10,
		})
		require.NoError(t, err)
		require.True(t, response2.Success)

		// T+3m: List locks (after lock_2 has expired)
		response3, err := locksCore.ListLocks(&corepb.ListLocksRequest{
			NamespaceId: namespaceId,
			Now:         now.Add(3 * time.Minute).UnixNano(),
		})

		require.NoError(t, err)
		require.NotNil(t, response3)
		require.Len(t, response3.Locks, 1) // Only the active lock should be returned

		require.Equal(t, "lock_active", response3.Locks[0].Id.LockName)
		require.Equal(t, corepb.LockState_EXCLUSIVE_LOCKED, response3.Locks[0].State)
	})

	t.Run("with multiple namespaces", func(t *testing.T) {
		locksCore := newLocksCore(t)

		now := time.Now()

		accountId := rand.Uint64()

		// Create locks in different namespaces
		lockId1 := &corepb.LockId{
			AccountId:   accountId,
			NamespaceId: rand.Uint32(),
			LockName:    "lock_1",
		}

		lockId2 := &corepb.LockId{
			AccountId:   accountId,
			NamespaceId: rand.Uint32(),
			LockName:    "lock_2",
		}

		// T+0: Acquire lock in namespace_1
		response1, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       lockId1,
			Now:                          now.UnixNano(),
			ProcessId:                    "process_1",
			Exclusive:                    true,
			ExpiresAt:                    now.Add(time.Hour).UnixNano(),
			MaxNumberOfLocksPerNamespace: 10,
		})
		require.NoError(t, err)
		require.True(t, response1.Success)

		// T+1m: Acquire lock in namespace_2
		response2, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       lockId2,
			Now:                          now.Add(time.Minute).UnixNano(),
			ProcessId:                    "process_2",
			Exclusive:                    false,
			ExpiresAt:                    now.Add(time.Minute).Add(time.Hour).UnixNano(),
			MaxNumberOfLocksPerNamespace: 10,
		})
		require.NoError(t, err)
		require.True(t, response2.Success)

		// T+2m: List locks in namespace_1
		response3, err := locksCore.ListLocks(&corepb.ListLocksRequest{
			NamespaceId: &corepb.NamespaceId{
				AccountId:   accountId,
				NamespaceId: lockId1.NamespaceId,
			},
			Now: now.Add(2 * time.Minute).UnixNano(),
		})

		require.NoError(t, err)
		require.NotNil(t, response3)
		require.Len(t, response3.Locks, 1)
		require.Equal(t, "lock_1", response3.Locks[0].Id.LockName)
		require.Equal(t, lockId1.NamespaceId, response3.Locks[0].Id.NamespaceId)

		// T+3m: List locks in namespace_2
		response4, err := locksCore.ListLocks(&corepb.ListLocksRequest{
			NamespaceId: &corepb.NamespaceId{
				AccountId:   accountId,
				NamespaceId: lockId2.NamespaceId,
			},
			Now: now.Add(3 * time.Minute).UnixNano(),
		})

		require.NoError(t, err)
		require.NotNil(t, response4)
		require.Len(t, response4.Locks, 1)
		require.Equal(t, "lock_2", response4.Locks[0].Id.LockName)
		require.Equal(t, lockId2.NamespaceId, response4.Locks[0].Id.NamespaceId)

		// T+4m: List locks in nonexistent namespace
		response5, err := locksCore.ListLocks(&corepb.ListLocksRequest{
			NamespaceId: &corepb.NamespaceId{
				AccountId:   accountId,
				NamespaceId: rand.Uint32(),
			},
			Now: now.Add(4 * time.Minute).UnixNano(),
		})

		require.NoError(t, err)
		require.NotNil(t, response5)
		require.Empty(t, response5.Locks)
	})

	t.Run("with mixed lock states", func(t *testing.T) {
		locksCore := newLocksCore(t)

		now := time.Now()

		namespaceId := &corepb.NamespaceId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
		}

		// Create locks with different states
		lockId1 := &corepb.LockId{
			AccountId:   namespaceId.AccountId,
			NamespaceId: namespaceId.NamespaceId,
			LockName:    "write_lock",
		}

		lockId2 := &corepb.LockId{
			AccountId:   namespaceId.AccountId,
			NamespaceId: namespaceId.NamespaceId,
			LockName:    "read_lock_single",
		}

		lockId3 := &corepb.LockId{
			AccountId:   namespaceId.AccountId,
			NamespaceId: namespaceId.NamespaceId,
			LockName:    "read_lock_multiple",
		}

		// T+0: Acquire exclusive lock
		response1, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       lockId1,
			Now:                          now.UnixNano(),
			ProcessId:                    "process_1",
			Exclusive:                    true,
			ExpiresAt:                    now.Add(time.Hour).UnixNano(),
			MaxNumberOfLocksPerNamespace: 10,
		})
		require.NoError(t, err)
		require.True(t, response1.Success)

		// T+1m: Acquire shared lock
		response2, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       lockId2,
			Now:                          now.Add(time.Minute).UnixNano(),
			ProcessId:                    "process_2",
			Exclusive:                    false,
			ExpiresAt:                    now.Add(time.Minute).Add(time.Hour).UnixNano(),
			MaxNumberOfLocksPerNamespace: 10,
		})
		require.NoError(t, err)
		require.True(t, response2.Success)

		// T+2m: Acquire shared lock with multiple holders
		response3, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       lockId3,
			Now:                          now.Add(2 * time.Minute).UnixNano(),
			ProcessId:                    "process_3",
			Exclusive:                    false,
			ExpiresAt:                    now.Add(2 * time.Minute).Add(time.Hour).UnixNano(),
			MaxNumberOfLocksPerNamespace: 10,
		})
		require.NoError(t, err)
		require.True(t, response3.Success)

		// T+3m: Add another shared lock holder
		response4, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       lockId3,
			Now:                          now.Add(3 * time.Minute).UnixNano(),
			ProcessId:                    "process_4",
			Exclusive:                    false,
			ExpiresAt:                    now.Add(3 * time.Minute).Add(time.Hour).UnixNano(),
			MaxNumberOfLocksPerNamespace: 10,
		})
		require.NoError(t, err)
		require.True(t, response4.Success)

		// T+4m: List locks
		response5, err := locksCore.ListLocks(&corepb.ListLocksRequest{
			NamespaceId: namespaceId,
			Now:         now.Add(4 * time.Minute).UnixNano(),
		})

		require.NoError(t, err)
		require.NotNil(t, response5)
		require.Len(t, response5.Locks, 3)

		// Verify lock states and holders
		lockMap := make(map[string]*corepb.Lock)
		for _, lock := range response5.Locks {
			lockMap[lock.Id.LockName] = lock
		}

		// Check write lock
		writeLock := lockMap["write_lock"]
		require.Equal(t, corepb.LockState_EXCLUSIVE_LOCKED, writeLock.State)
		require.Len(t, writeLock.LockHolders, 1)
		require.Equal(t, "process_1", writeLock.LockHolders[0].ProcessId)

		// Check single read lock
		readLockSingle := lockMap["read_lock_single"]
		require.Equal(t, corepb.LockState_SHARED_LOCKED, readLockSingle.State)
		require.Len(t, readLockSingle.LockHolders, 1)
		require.Equal(t, "process_2", readLockSingle.LockHolders[0].ProcessId)

		// Check multiple read lock
		readLockMultiple := lockMap["read_lock_multiple"]
		require.Equal(t, corepb.LockState_SHARED_LOCKED, readLockMultiple.State)
		require.Len(t, readLockMultiple.LockHolders, 2)
		require.Equal(t, "process_3", readLockMultiple.LockHolders[0].ProcessId)
		require.Equal(t, "process_4", readLockMultiple.LockHolders[1].ProcessId)
	})

	t.Run("with all expired locks", func(t *testing.T) {
		locksCore := newLocksCore(t)

		now := time.Now()

		namespaceId := &corepb.NamespaceId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
		}

		// Create locks that will expire
		lockId1 := &corepb.LockId{
			AccountId:   namespaceId.AccountId,
			NamespaceId: namespaceId.NamespaceId,
			LockName:    "lock_1",
		}

		lockId2 := &corepb.LockId{
			AccountId:   namespaceId.AccountId,
			NamespaceId: namespaceId.NamespaceId,
			LockName:    "lock_2",
		}

		// T+0: Acquire locks with short expiration
		response1, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       lockId1,
			Now:                          now.UnixNano(),
			ProcessId:                    "process_1",
			Exclusive:                    true,
			ExpiresAt:                    now.Add(time.Minute).UnixNano(), // Expires at T+1m
			MaxNumberOfLocksPerNamespace: 10,
		})
		require.NoError(t, err)
		require.True(t, response1.Success)

		response2, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       lockId2,
			Now:                          now.UnixNano(),
			ProcessId:                    "process_2",
			Exclusive:                    false,
			ExpiresAt:                    now.Add(2 * time.Minute).UnixNano(), // Expires at T+2m
			MaxNumberOfLocksPerNamespace: 10,
		})
		require.NoError(t, err)
		require.True(t, response2.Success)

		// T+3m: List locks (after all locks have expired)
		response3, err := locksCore.ListLocks(&corepb.ListLocksRequest{
			NamespaceId: namespaceId,
			Now:         now.Add(3 * time.Minute).UnixNano(),
		})

		require.NoError(t, err)
		require.NotNil(t, response3)
		require.Empty(t, response3.Locks) // No locks should be returned as they're all expired
	})
}

func TestCore_RunLocksGarbageCollection(t *testing.T) {
	t.Run("with multiple expiring locks", func(t *testing.T) {
		locksCore := newLocksCore(t)

		now := time.Now()
		namespaceId := &corepb.NamespaceId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
		}

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
				AccountId:   namespaceId.AccountId,
				NamespaceId: namespaceId.NamespaceId,
				LockName:    fmt.Sprintf("lock_%d", i),
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
					Exclusive:                    i%2 == 0,                             // Alternate between exclusive and shared locks
					ExpiresAt:                    now.Add(30 * time.Minute).UnixNano(), // Will expire
					MaxNumberOfLocksPerNamespace: 100,
				})
				require.NoError(t, err)
				require.NotNil(t, response.Lock)
				require.True(t, response.Success)

				// Add a second holder for read locks that will also expire
				if len(response.Lock.LockHolders) == 0 {
					response2, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
						LockId:                       lockId,
						Now:                          now.UnixNano(),
						ProcessId:                    fmt.Sprintf("process_%d_second", i),
						Exclusive:                    false,
						ExpiresAt:                    now.Add(30 * time.Minute).UnixNano(), // Will expire
						MaxNumberOfLocksPerNamespace: 100,
					})
					require.NoError(t, err)
					require.NotNil(t, response2.Lock)
					require.True(t, response2.Success)
				}
			} else if i < 10 {
				// Locks 5-9: Some holders will expire, some will remain
				// For this test, we'll make all locks 5-9 read locks to ensure we can have multiple holders
				response, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
					LockId:                       lockId,
					Now:                          now.UnixNano(),
					ProcessId:                    fmt.Sprintf("process_%d", i),
					Exclusive:                    false,                                // Make all shared locks for consistency
					ExpiresAt:                    now.Add(30 * time.Minute).UnixNano(), // Will expire
					MaxNumberOfLocksPerNamespace: 100,
				})
				require.NoError(t, err)
				require.NotNil(t, response.Lock)
				require.True(t, response.Success)

				// Add a second holder that will remain
				response2, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
					LockId:                       lockId,
					Now:                          now.UnixNano(),
					ProcessId:                    fmt.Sprintf("process_%d_second", i),
					Exclusive:                    false,
					ExpiresAt:                    now.Add(2 * time.Hour).UnixNano(), // Will remain
					MaxNumberOfLocksPerNamespace: 100,
				})
				require.NoError(t, err)
				require.NotNil(t, response2.Lock)
				require.True(t, response2.Success)
			} else {
				// Locks 10-14: All holders will remain
				response, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
					LockId:                       lockId,
					Now:                          now.UnixNano(),
					ProcessId:                    fmt.Sprintf("process_%d", i),
					Exclusive:                    i%2 == 0,                          // Alternate between exclusive and shared locks
					ExpiresAt:                    now.Add(2 * time.Hour).UnixNano(), // Will remain
					MaxNumberOfLocksPerNamespace: 100,
				})
				require.NoError(t, err)
				require.NotNil(t, response.Lock)
				require.True(t, response.Success)

				// Add a second holder that will also remain
				if response.Lock.State == corepb.LockState_SHARED_LOCKED {
					response2, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
						LockId:                       lockId,
						Now:                          now.UnixNano(),
						ProcessId:                    fmt.Sprintf("process_%d_second", i),
						Exclusive:                    false,
						ExpiresAt:                    now.Add(3 * time.Hour).UnixNano(), // Will remain
						MaxNumberOfLocksPerNamespace: 100,
					})
					require.NoError(t, err)
					require.NotNil(t, response2.Lock)
					require.True(t, response2.Success)
				}
			}
		}

		// Verify all locks exist and are locked before garbage collection
		for _, lockId := range lockIds {
			response, err := locksCore.GetLock(&corepb.GetLockRequest{
				LockId: lockId,
				Now:    now.UnixNano(),
			})
			require.NoError(t, err)
			require.NotNil(t, response.Lock)
			require.NotEqual(t, corepb.LockState_UNLOCKED, response.Lock.State)
		}

		// Run garbage collection at the moment when some locks expire (T+31 minutes)
		gcTime := now.Add(31 * time.Minute)
		gcResponse, err := locksCore.RunLocksGarbageCollection(&corepb.RunLocksGarbageCollectionRequest{
			Now:                   gcTime.UnixNano(),
			GcRecordsPageSize:     100,
			GcRecordLocksPageSize: 100,
			MaxVisitedLocks:       maxVisitedLocks,
		})

		require.NoError(t, err)
		require.NotNil(t, gcResponse)

		// Verify the state of locks after garbage collection
		// Note: We use the public GetLock method which internally calls checkLockExpiration
		// to verify the true state of the locks after garbage collection

		// Locks 0-4 should be unlocked (all holders expired)
		for i := 0; i < 5; i++ {
			response, err := locksCore.GetLock(&corepb.GetLockRequest{
				LockId: lockIds[i],
				Now:    gcTime.UnixNano(),
			})
			require.NoError(t, err)
			require.NotNil(t, response.Lock)
			require.Equal(t, corepb.LockState_UNLOCKED, response.Lock.State, "Lock %d should be unlocked", i)
		}

		// Locks 5-9 should still be locked but with fewer holders
		for i := 5; i < 10; i++ {
			response, err := locksCore.GetLock(&corepb.GetLockRequest{
				LockId: lockIds[i],
				Now:    gcTime.UnixNano(),
			})
			require.NoError(t, err)
			require.NotNil(t, response.Lock)
			require.NotEqual(t, corepb.LockState_UNLOCKED, response.Lock.State, "Lock %d should still be locked", i)

			// For shared locks, verify only one holder remains
			if response.Lock.State == corepb.LockState_SHARED_LOCKED {
				require.Len(t, response.Lock.LockHolders, 1, "Lock %d should have exactly one holder remaining", i)
				require.Equal(t, fmt.Sprintf("process_%d_second", i), response.Lock.LockHolders[0].ProcessId)
			}
		}

		// Locks 10-14 should still be locked with all holders
		for i := 10; i < numLocks; i++ {
			response, err := locksCore.GetLock(&corepb.GetLockRequest{
				LockId: lockIds[i],
				Now:    gcTime.UnixNano(),
			})
			require.NoError(t, err)
			require.NotNil(t, response.Lock)
			require.NotEqual(t, corepb.LockState_UNLOCKED, response.Lock.State, "Lock %d should still be locked", i)

			// For shared locks, verify both holders remain
			if response.Lock.State == corepb.LockState_SHARED_LOCKED {
				require.Len(t, response.Lock.LockHolders, 2, "Lock %d should have both holders remaining", i)
				holderProcessIds := make([]string, len(response.Lock.LockHolders))
				for j, holder := range response.Lock.LockHolders {
					holderProcessIds[j] = holder.ProcessId
				}
				require.Contains(t, holderProcessIds, fmt.Sprintf("process_%d", i))
				require.Contains(t, holderProcessIds, fmt.Sprintf("process_%d_second", i))
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

		require.NoError(t, err)
		require.NotNil(t, gcResponse2)

		// Verify that locks 5-9 still have their remaining holders
		for i := 5; i < 10; i++ {
			response, err := locksCore.GetLock(&corepb.GetLockRequest{
				LockId: lockIds[i],
				Now:    gcTime.UnixNano(),
			})
			require.NoError(t, err)
			require.NotNil(t, response.Lock)
			require.NotEqual(t, corepb.LockState_UNLOCKED, response.Lock.State, "Lock %d should still be locked after second GC", i)
		}

		// Verify that locks 10-14 still have all their holders
		for i := 10; i < numLocks; i++ {
			response, err := locksCore.GetLock(&corepb.GetLockRequest{
				LockId: lockIds[i],
				Now:    gcTime.UnixNano(),
			})
			require.NoError(t, err)
			require.NotNil(t, response.Lock)
			require.NotEqual(t, corepb.LockState_UNLOCKED, response.Lock.State, "Lock %d should still be locked after second GC", i)
		}
	})

	t.Run("with deleted namespace", func(t *testing.T) {
		locksCore := newLocksCore(t)

		now := time.Now()

		namespaceId := &corepb.NamespaceId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
		}

		// Create some locks in the namespace
		lockIds := make([]*corepb.LockId, 10)
		for i := 0; i < 10; i++ {
			lockIds[i] = &corepb.LockId{
				AccountId:   namespaceId.AccountId,
				NamespaceId: namespaceId.NamespaceId,
				LockName:    fmt.Sprintf("lock_%d", i),
			}
		}

		// Acquire locks in the namespace
		for i, lockId := range lockIds {
			response, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
				LockId:                       lockId,
				Now:                          now.UnixNano(),
				ProcessId:                    fmt.Sprintf("process_%d", i),
				Exclusive:                    i%2 == 0, // Alternate between exclusive and shared locks
				ExpiresAt:                    now.Add(time.Hour).UnixNano(),
				MaxNumberOfLocksPerNamespace: 100,
			})

			require.NoError(t, err)
			require.NotNil(t, response.Lock)
			require.True(t, response.Success)
		}

		// Verify that locks in a different namespace are accessible after GC
		differentNamespaceLockId := &corepb.LockId{
			AccountId:   namespaceId.AccountId,
			NamespaceId: rand.Uint32(),
			LockName:    "different_lock",
		}

		// Acquire a lock in a different namespace
		acquireResponse, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       differentNamespaceLockId,
			Now:                          now.UnixNano(),
			ProcessId:                    "process_different",
			Exclusive:                    true,
			ExpiresAt:                    now.Add(time.Hour).UnixNano(),
			MaxNumberOfLocksPerNamespace: 10,
		})

		require.NoError(t, err)
		require.NotNil(t, acquireResponse.Lock)
		require.True(t, acquireResponse.Success)

		// Verify locks exist by getting them
		for _, lockId := range lockIds {
			response, err := locksCore.GetLock(&corepb.GetLockRequest{
				LockId: lockId,
				Now:    now.UnixNano(),
			})

			require.NoError(t, err)
			require.NotNil(t, response.Lock)
			require.NotEqual(t, corepb.LockState_UNLOCKED, response.Lock.State)
		}

		// Mark the namespace as deleted using LocksDeleteNamespace
		deleteResponse, err := locksCore.LocksDeleteNamespace(&corepb.LocksDeleteNamespaceRequest{
			NamespaceId: namespaceId,
			Now:         now.UnixNano(),
		})

		require.NoError(t, err)
		require.NotNil(t, deleteResponse)

		// Run garbage collection to clean up the deleted namespace
		gcResponse, err := locksCore.RunLocksGarbageCollection(&corepb.RunLocksGarbageCollectionRequest{
			Now:                   now.UnixNano(),
			GcRecordsPageSize:     100,
			GcRecordLocksPageSize: 100,
			MaxVisitedLocks:       1000,
		})

		require.NoError(t, err)
		require.NotNil(t, gcResponse)

		// Verify that locks in the deleted namespace are no longer accessible
		for _, lockId := range lockIds {
			response, err := locksCore.GetLock(&corepb.GetLockRequest{
				LockId: lockId,
				Now:    now.UnixNano(),
			})

			require.NoError(t, err)
			require.NotNil(t, response.Lock)
			require.Equal(t, corepb.LockState_UNLOCKED, response.Lock.State)
		}

		// Verify the different namespace lock still exists after GC
		getResponse, err := locksCore.GetLock(&corepb.GetLockRequest{
			LockId: differentNamespaceLockId,
			Now:    now.UnixNano(),
		})

		require.NoError(t, err)
		require.NotNil(t, getResponse.Lock)
		require.Equal(t, corepb.LockState_EXCLUSIVE_LOCKED, getResponse.Lock.State)
	})
}

func newLocksCore(t *testing.T) *Core {
	badgerStore, err := store.NewBadgerInMemoryStore()
	require.NoError(t, err)
	return NewCore(badgerStore, []byte{0x1d, 0x36, 0x00, 0x00}, []byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})
}
