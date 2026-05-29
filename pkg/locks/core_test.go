package locks

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

func TestCore_AcquireLock(t *testing.T) {
	t.Run("exclusive", func(t *testing.T) {
		locksCore := newLocksCore(t)

		now := time.Now()

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		lockId := &corepb.LockId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LockName:    "test_lock",
		}

		// Create lease with 60 minute TTL
		lease := createLease(t, locksCore, accountId, namespaceId, "process-1", now, 60*time.Minute)

		// T+0: Acquire lock
		response1, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       lockId,
			LeaseId:                      lease.Id.LeaseId,
			Now:                          now.UnixNano(),
			Exclusive:                    true,
			MaxNumberOfLocksPerNamespace: 10,
		})

		require.NoError(t, err)
		require.NotNil(t, response1.Lock)
		require.True(t, response1.Success)
		require.EqualValues(t, now.UnixNano(), response1.Lock.LockedAt)
		require.Equal(t, corepb.LockState_EXCLUSIVE_LOCKED, response1.Lock.State)
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

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		lockId := &corepb.LockId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LockName:    "test_lock",
		}

		// Create lease with 60 minute TTL
		lease := createLease(t, locksCore, accountId, namespaceId, "process-1", now, 60*time.Minute)

		// T+0: Acquire lock
		response1, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       lockId,
			LeaseId:                      lease.Id.LeaseId,
			Now:                          now.UnixNano(),
			Exclusive:                    false,
			MaxNumberOfLocksPerNamespace: 10,
		})

		require.NoError(t, err)
		require.NotNil(t, response1.Lock)
		require.True(t, response1.Success)
		require.EqualValues(t, now.UnixNano(), response1.Lock.LockedAt)
		require.Equal(t, corepb.LockState_SHARED_LOCKED, response1.Lock.State)
		require.Len(t, response1.Lock.LockHolders, 1)
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

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		lockId := &corepb.LockId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LockName:    "test_lock",
		}

		// Create lease with 60 minute TTL
		lease := createLease(t, locksCore, accountId, namespaceId, "process-1", now, 60*time.Minute)

		// T+0: Acquire lock
		response1, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       lockId,
			LeaseId:                      lease.Id.LeaseId,
			Now:                          now.UnixNano(),
			Exclusive:                    true,
			MaxNumberOfLocksPerNamespace: 10,
		})

		require.NoError(t, err)
		require.True(t, response1.Success)
		require.Equal(t, corepb.LockState_EXCLUSIVE_LOCKED, response1.Lock.State)

		// T+1m: Acquire lock again
		response2, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       lockId,
			LeaseId:                      lease.Id.LeaseId,
			Now:                          now.Add(time.Minute).UnixNano(),
			Exclusive:                    true,
			MaxNumberOfLocksPerNamespace: 10,
		})

		require.NoError(t, err)
		require.True(t, response2.Success)
		require.Equal(t, corepb.LockState_EXCLUSIVE_LOCKED, response2.Lock.State)
	})

	t.Run("shared lock repeatedly", func(t *testing.T) {
		locksCore := newLocksCore(t)

		now := time.Now()

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		lockId := &corepb.LockId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LockName:    "test_lock",
		}

		// Create lease with 60 minute TTL
		lease := createLease(t, locksCore, accountId, namespaceId, "process-1", now, 60*time.Minute)

		// T+0: Acquire lock
		response1, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       lockId,
			LeaseId:                      lease.Id.LeaseId,
			Now:                          now.UnixNano(),
			Exclusive:                    false,
			MaxNumberOfLocksPerNamespace: 10,
		})

		require.NoError(t, err)
		require.True(t, response1.Success)
		require.Equal(t, corepb.LockState_SHARED_LOCKED, response1.Lock.State)

		// T+1m: Acquire lock again
		response2, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       lockId,
			LeaseId:                      lease.Id.LeaseId,
			Now:                          now.Add(time.Minute).UnixNano(),
			Exclusive:                    false,
			MaxNumberOfLocksPerNamespace: 10,
		})

		require.NoError(t, err)
		require.True(t, response2.Success)
		require.Equal(t, corepb.LockState_SHARED_LOCKED, response2.Lock.State)
	})

	t.Run("exclusive locked by another process", func(t *testing.T) {
		locksCore := newLocksCore(t)

		now := time.Now()

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		lockId := &corepb.LockId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LockName:    "test_lock",
		}

		// Create leases for two different processes
		lease1 := createLease(t, locksCore, accountId, namespaceId, "process-1", now, 60*time.Minute)
		lease2 := createLease(t, locksCore, accountId, namespaceId, "process-2", now, 60*time.Minute)
		lease3 := createLease(t, locksCore, accountId, namespaceId, "process-3", now, 60*time.Minute)

		// T+0: Acquire lock
		response1, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       lockId,
			LeaseId:                      lease1.Id.LeaseId,
			Now:                          now.UnixNano(),
			Exclusive:                    true,
			MaxNumberOfLocksPerNamespace: 10,
		})

		require.NoError(t, err)
		require.True(t, response1.Success)
		require.Equal(t, corepb.LockState_EXCLUSIVE_LOCKED, response1.Lock.State)

		// T+1m: Acquire write lock by another process
		response2, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       lockId,
			LeaseId:                      lease2.Id.LeaseId,
			Now:                          now.Add(time.Minute).UnixNano(),
			Exclusive:                    true,
			MaxNumberOfLocksPerNamespace: 10,
		})

		require.NoError(t, err)
		require.False(t, response2.Success)
		require.Equal(t, corepb.LockState_EXCLUSIVE_LOCKED, response2.Lock.State)

		// T+2m: Acquire read lock by another process
		response3, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       lockId,
			LeaseId:                      lease3.Id.LeaseId,
			Now:                          now.Add(2 * time.Minute).UnixNano(),
			Exclusive:                    false,
			MaxNumberOfLocksPerNamespace: 10,
		})

		require.NoError(t, err)
		require.False(t, response3.Success)
		require.Equal(t, corepb.LockState_EXCLUSIVE_LOCKED, response3.Lock.State)
	})

	t.Run("shared locked by another process", func(t *testing.T) {
		locksCore := newLocksCore(t)

		now := time.Now()

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		lockId := &corepb.LockId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LockName:    "test_lock",
		}

		// Create leases for three different processes
		lease1 := createLease(t, locksCore, accountId, namespaceId, "process-1", now, 60*time.Minute)
		lease2 := createLease(t, locksCore, accountId, namespaceId, "process-2", now, 60*time.Minute)
		lease3 := createLease(t, locksCore, accountId, namespaceId, "process-3", now, 60*time.Minute)

		// T+0: Acquire lock
		response1, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       lockId,
			LeaseId:                      lease1.Id.LeaseId,
			Now:                          now.UnixNano(),
			Exclusive:                    false,
			MaxNumberOfLocksPerNamespace: 10,
		})

		require.NoError(t, err)
		require.True(t, response1.Success)
		require.Equal(t, corepb.LockState_SHARED_LOCKED, response1.Lock.State)

		// T+1m: Acquire exclusive lock by another process
		response2, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       lockId,
			LeaseId:                      lease2.Id.LeaseId,
			Now:                          now.Add(time.Minute).UnixNano(),
			Exclusive:                    true,
			MaxNumberOfLocksPerNamespace: 10,
		})

		require.NoError(t, err)
		require.False(t, response2.Success)
		require.Equal(t, corepb.LockState_SHARED_LOCKED, response2.Lock.State)

		// T+2m: Acquire shared lock by another process
		response3, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       lockId,
			LeaseId:                      lease3.Id.LeaseId,
			Now:                          now.Add(2 * time.Minute).UnixNano(),
			Exclusive:                    false,
			MaxNumberOfLocksPerNamespace: 10,
		})

		require.NoError(t, err)
		require.True(t, response3.Success)
		require.Equal(t, corepb.LockState_SHARED_LOCKED, response3.Lock.State)
		require.Len(t, response3.Lock.LockHolders, 2)
	})

	t.Run("maximum number of locks per namespace", func(t *testing.T) {
		locksCore := newLocksCore(t)

		now := time.Now()
		accountId1 := rand.Uint64()
		accountId2 := rand.Uint64()
		namespaceId1 := rand.Uint32()
		maxLocksPerNamespace := int64(3)

		// Create lease with 60 minute TTL
		lease := createLease(t, locksCore, accountId1, namespaceId1, "process-1", now, 60*time.Minute)

		// Create locks up to the maximum limit
		for i := 0; i < int(maxLocksPerNamespace); i++ {
			lockId := &corepb.LockId{
				AccountId:   accountId1,
				NamespaceId: namespaceId1,
				LockName:    fmt.Sprintf("lock_%d", i),
			}

			response, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
				LockId:                       lockId,
				LeaseId:                      lease.Id.LeaseId,
				Now:                          now.UnixNano(),
				Exclusive:                    false,
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

		_, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       lockId,
			LeaseId:                      lease.Id.LeaseId,
			Now:                          now.Add(time.Second).UnixNano(),
			Exclusive:                    false,
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
		response, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       existingLockId,
			LeaseId:                      lease.Id.LeaseId,
			Now:                          now.Add(time.Second * 2).UnixNano(),
			Exclusive:                    false,
			MaxNumberOfLocksPerNamespace: maxLocksPerNamespace,
		})

		require.NoError(t, err)
		require.NotNil(t, response.Lock)
		require.True(t, response.Success)
		require.Equal(t, corepb.LockState_SHARED_LOCKED, response.Lock.State)

		// Let's release one of the existing locks (for both holders)
		_, err = locksCore.ReleaseLock(&corepb.ReleaseLockRequest{
			LockId:  existingLockId,
			LeaseId: lease.Id.LeaseId,
			Now:     now.Add(time.Second * 3).UnixNano(),
		})
		require.NoError(t, err)
		_, err = locksCore.ReleaseLock(&corepb.ReleaseLockRequest{
			LockId:  existingLockId,
			LeaseId: lease.Id.LeaseId,
			Now:     now.Add(time.Second * 3).UnixNano(),
		})
		require.NoError(t, err)

		// Now try to acquire another lock again
		response, err = locksCore.AcquireLock(&corepb.AcquireLockRequest{
			LockId: &corepb.LockId{
				AccountId:   accountId1,
				NamespaceId: namespaceId1,
				LockName:    "lock_4",
			},
			LeaseId:                      lease.Id.LeaseId,
			Now:                          now.Add(time.Second * 4).UnixNano(),
			Exclusive:                    true,
			MaxNumberOfLocksPerNamespace: maxLocksPerNamespace,
		})

		require.NoError(t, err)
		require.NotNil(t, response.Lock)
		require.True(t, response.Success)
		require.Equal(t, corepb.LockState_EXCLUSIVE_LOCKED, response.Lock.State)

		// Test that creating a lock in a different namespace doesn't affect the limit
		differentNamespaceId := rand.Uint32()
		differentNamespaceLockId := &corepb.LockId{
			AccountId:   accountId1,
			NamespaceId: differentNamespaceId,
			LockName:    "lock_different_namespace",
		}

		lease2 := createLease(t, locksCore, accountId1, differentNamespaceId, "process-2", now, 60*time.Minute)

		response, err = locksCore.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       differentNamespaceLockId,
			LeaseId:                      lease2.Id.LeaseId,
			Now:                          now.UnixNano(),
			Exclusive:                    true,
			MaxNumberOfLocksPerNamespace: maxLocksPerNamespace,
		})

		require.NoError(t, err)
		require.NotNil(t, response.Lock)
		require.True(t, response.Success)
		require.Equal(t, corepb.LockState_EXCLUSIVE_LOCKED, response.Lock.State)

		// Test that creating a lock with a different account doesn't affect the limit
		differentAccountNamespaceId := rand.Uint32()
		differentAccountLockId := &corepb.LockId{
			AccountId:   accountId2,
			NamespaceId: differentAccountNamespaceId,
			LockName:    "lock_different_account",
		}

		lease3 := createLease(t, locksCore, accountId2, differentAccountNamespaceId, "process-3", now, 60*time.Minute)

		response, err = locksCore.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       differentAccountLockId,
			LeaseId:                      lease3.Id.LeaseId,
			Now:                          now.UnixNano(),
			Exclusive:                    true,
			MaxNumberOfLocksPerNamespace: maxLocksPerNamespace,
		})

		require.NoError(t, err)
		require.NotNil(t, response.Lock)
		require.True(t, response.Success)
		require.Equal(t, corepb.LockState_EXCLUSIVE_LOCKED, response.Lock.State)
	})

	t.Run("parent exclusive blocks descendant shared", func(t *testing.T) {
		core := newLocksCore(t)

		now := time.Now()

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		parentLock := &corepb.LockId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LockName:    "a/b",
		}
		childLock := &corepb.LockId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LockName:    "a/b/c",
		}

		// Create leases for two different processes
		lease1 := createLease(t, core, accountId, namespaceId, "process-1", now, 60*time.Minute)
		lease2 := createLease(t, core, accountId, namespaceId, "process-2", now, 60*time.Minute)

		// Acquire exclusive lock on parent
		resp1, err := core.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       parentLock,
			LeaseId:                      lease1.Id.LeaseId,
			Now:                          now.UnixNano(),
			Exclusive:                    true,
			MaxNumberOfLocksPerNamespace: 100,
		})
		require.NoError(t, err)
		require.True(t, resp1.Success)

		// Try to acquire shared lock on child - should BLOCK
		resp2, err := core.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       childLock,
			LeaseId:                      lease2.Id.LeaseId,
			Now:                          now.UnixNano(),
			Exclusive:                    false,
			MaxNumberOfLocksPerNamespace: 100,
		})
		require.NoError(t, err)
		require.False(t, resp2.Success) // BLOCKED by parent exclusive

		// Release parent
		_, err = core.ReleaseLock(&corepb.ReleaseLockRequest{
			LockId:  parentLock,
			LeaseId: lease1.Id.LeaseId,
			Now:     now.Add(time.Minute).UnixNano(),
		})
		require.NoError(t, err)

		// Now child lock should succeed
		resp3, err := core.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       childLock,
			LeaseId:                      lease2.Id.LeaseId,
			Now:                          now.Add(2 * time.Minute).UnixNano(),
			Exclusive:                    false,
			MaxNumberOfLocksPerNamespace: 100,
		})
		require.NoError(t, err)
		require.True(t, resp3.Success)
	})

	t.Run("parent exclusive blocks descendant exclusive", func(t *testing.T) {
		core := newLocksCore(t)

		now := time.Now()

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		parentLock := &corepb.LockId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LockName:    "a/b",
		}
		childLock := &corepb.LockId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LockName:    "a/b/c",
		}

		// Create leases for two different processes
		lease1 := createLease(t, core, accountId, namespaceId, "process-1", now, 60*time.Minute)
		lease2 := createLease(t, core, accountId, namespaceId, "process-2", now, 60*time.Minute)

		// Acquire exclusive lock on parent
		resp1, err := core.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       parentLock,
			LeaseId:                      lease1.Id.LeaseId,
			Now:                          now.UnixNano(),
			Exclusive:                    true,
			MaxNumberOfLocksPerNamespace: 100,
		})
		require.NoError(t, err)
		require.True(t, resp1.Success)

		// Try to acquire exclusive lock on child - should BLOCK
		resp2, err := core.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       childLock,
			LeaseId:                      lease2.Id.LeaseId,
			Now:                          now.UnixNano(),
			Exclusive:                    true,
			MaxNumberOfLocksPerNamespace: 100,
		})
		require.NoError(t, err)
		require.False(t, resp2.Success) // BLOCKED by parent exclusive
	})

	t.Run("parent shared allows descendant shared", func(t *testing.T) {
		core := newLocksCore(t)

		now := time.Now()

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		parentLock := &corepb.LockId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LockName:    "a/b",
		}
		childLock := &corepb.LockId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LockName:    "a/b/c",
		}

		// Create lease with 60 minute TTL
		lease := createLease(t, core, accountId, namespaceId, "process-1", now, 60*time.Minute)

		// Acquire shared lock on parent
		resp1, err := core.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       parentLock,
			LeaseId:                      lease.Id.LeaseId,
			Now:                          now.UnixNano(),
			Exclusive:                    false,
			MaxNumberOfLocksPerNamespace: 100,
		})
		require.NoError(t, err)
		require.True(t, resp1.Success)

		// Acquire shared lock on child - should ALLOW
		resp2, err := core.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       childLock,
			LeaseId:                      lease.Id.LeaseId,
			Now:                          now.UnixNano(),
			Exclusive:                    false,
			MaxNumberOfLocksPerNamespace: 100,
		})
		require.NoError(t, err)
		require.True(t, resp2.Success) // ALLOWED
	})

	t.Run("parent shared blocks descendant exclusive", func(t *testing.T) {
		core := newLocksCore(t)

		now := time.Now()

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		parentLock := &corepb.LockId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LockName:    "a/b",
		}
		childLock := &corepb.LockId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LockName:    "a/b/c",
		}

		// Create leases for two different processes
		lease1 := createLease(t, core, accountId, namespaceId, "process-1", now, 60*time.Minute)
		lease2 := createLease(t, core, accountId, namespaceId, "process-2", now, 60*time.Minute)

		// Acquire shared lock on parent
		resp1, err := core.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       parentLock,
			LeaseId:                      lease1.Id.LeaseId,
			Now:                          now.UnixNano(),
			Exclusive:                    false,
			MaxNumberOfLocksPerNamespace: 100,
		})
		require.NoError(t, err)
		require.True(t, resp1.Success)

		// Try to acquire exclusive lock on child - should BLOCK
		resp2, err := core.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       childLock,
			LeaseId:                      lease2.Id.LeaseId,
			Now:                          now.UnixNano(),
			Exclusive:                    true,
			MaxNumberOfLocksPerNamespace: 100,
		})
		require.NoError(t, err)
		require.False(t, resp2.Success) // BLOCKED by parent shared
	})

	t.Run("descendant exclusive blocks ancestor shared", func(t *testing.T) {
		core := newLocksCore(t)

		now := time.Now()

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		parentLock := &corepb.LockId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LockName:    "a/b",
		}
		childLock := &corepb.LockId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LockName:    "a/b/c",
		}

		// Create leases for two different processes
		lease1 := createLease(t, core, accountId, namespaceId, "process-1", now, 60*time.Minute)
		lease2 := createLease(t, core, accountId, namespaceId, "process-2", now, 60*time.Minute)

		// Acquire exclusive lock on child
		resp1, err := core.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       childLock,
			LeaseId:                      lease1.Id.LeaseId,
			Now:                          now.UnixNano(),
			Exclusive:                    true,
			MaxNumberOfLocksPerNamespace: 100,
		})
		require.NoError(t, err)
		require.True(t, resp1.Success)

		// Try to acquire shared lock on parent - should BLOCK
		resp2, err := core.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       parentLock,
			LeaseId:                      lease2.Id.LeaseId,
			Now:                          now.UnixNano(),
			Exclusive:                    false,
			MaxNumberOfLocksPerNamespace: 100,
		})
		require.NoError(t, err)
		require.False(t, resp2.Success) // BLOCKED by child exclusive
	})

	t.Run("descendant exclusive blocks ancestor exclusive", func(t *testing.T) {
		core := newLocksCore(t)

		now := time.Now()

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		parentLock := &corepb.LockId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LockName:    "a/b",
		}
		childLock := &corepb.LockId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LockName:    "a/b/c",
		}

		// Create leases for two different processes
		lease1 := createLease(t, core, accountId, namespaceId, "process-1", now, 60*time.Minute)
		lease2 := createLease(t, core, accountId, namespaceId, "process-2", now, 60*time.Minute)

		// Acquire exclusive lock on child
		resp1, err := core.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       childLock,
			LeaseId:                      lease1.Id.LeaseId,
			Now:                          now.UnixNano(),
			Exclusive:                    true,
			MaxNumberOfLocksPerNamespace: 100,
		})
		require.NoError(t, err)
		require.True(t, resp1.Success)

		// Try to acquire exclusive lock on parent - should BLOCK
		resp2, err := core.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       parentLock,
			LeaseId:                      lease2.Id.LeaseId,
			Now:                          now.UnixNano(),
			Exclusive:                    true,
			MaxNumberOfLocksPerNamespace: 100,
		})
		require.NoError(t, err)
		require.False(t, resp2.Success) // BLOCKED by child exclusive
	})

	t.Run("descendant shared allows ancestor shared", func(t *testing.T) {
		core := newLocksCore(t)

		now := time.Now()

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		parentLock := &corepb.LockId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LockName:    "a/b",
		}
		childLock := &corepb.LockId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LockName:    "a/b/c",
		}

		// Create lease with 60 minute TTL
		lease := createLease(t, core, accountId, namespaceId, "process-1", now, 60*time.Minute)

		// Acquire shared lock on child
		resp1, err := core.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       childLock,
			LeaseId:                      lease.Id.LeaseId,
			Now:                          now.UnixNano(),
			Exclusive:                    false,
			MaxNumberOfLocksPerNamespace: 100,
		})
		require.NoError(t, err)
		require.True(t, resp1.Success)

		// Acquire shared lock on parent - should ALLOW
		resp2, err := core.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       parentLock,
			LeaseId:                      lease.Id.LeaseId,
			Now:                          now.UnixNano(),
			Exclusive:                    false,
			MaxNumberOfLocksPerNamespace: 100,
		})
		require.NoError(t, err)
		require.True(t, resp2.Success) // ALLOWED
	})

	t.Run("descendant shared blocks ancestor exclusive", func(t *testing.T) {
		core := newLocksCore(t)

		now := time.Now()

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		parentLock := &corepb.LockId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LockName:    "a/b",
		}
		childLock := &corepb.LockId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LockName:    "a/b/c",
		}

		// Create leases for two different processes
		lease1 := createLease(t, core, accountId, namespaceId, "process-1", now, 60*time.Minute)
		lease2 := createLease(t, core, accountId, namespaceId, "process-2", now, 60*time.Minute)

		// Acquire shared lock on child
		resp1, err := core.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       childLock,
			LeaseId:                      lease1.Id.LeaseId,
			Now:                          now.UnixNano(),
			Exclusive:                    false,
			MaxNumberOfLocksPerNamespace: 100,
		})
		require.NoError(t, err)
		require.True(t, resp1.Success)

		// Try to acquire exclusive lock on parent - should BLOCK
		resp2, err := core.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       parentLock,
			LeaseId:                      lease2.Id.LeaseId,
			Now:                          now.UnixNano(),
			Exclusive:                    true,
			MaxNumberOfLocksPerNamespace: 100,
		})
		require.NoError(t, err)
		require.False(t, resp2.Success) // BLOCKED by child shared
	})

	t.Run("sibling paths independent", func(t *testing.T) {
		core := newLocksCore(t)

		now := time.Now()

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		lock1 := &corepb.LockId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LockName:    "a/b",
		}
		lock2 := &corepb.LockId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LockName:    "a/c",
		}

		// Create lease with 60 minute TTL
		lease := createLease(t, core, accountId, namespaceId, "process-1", now, 60*time.Minute)

		// Acquire exclusive lock on a/b
		resp1, err := core.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       lock1,
			LeaseId:                      lease.Id.LeaseId,
			Now:                          now.UnixNano(),
			Exclusive:                    true,
			MaxNumberOfLocksPerNamespace: 100,
		})
		require.NoError(t, err)
		require.True(t, resp1.Success)

		// Acquire exclusive lock on a/c - should ALLOW (siblings are independent)
		resp2, err := core.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       lock2,
			LeaseId:                      lease.Id.LeaseId,
			Now:                          now.UnixNano(),
			Exclusive:                    true,
			MaxNumberOfLocksPerNamespace: 100,
		})
		require.NoError(t, err)
		require.True(t, resp2.Success) // ALLOWED - siblings are independent
	})

	t.Run("deep hierarchy", func(t *testing.T) {
		core := newLocksCore(t)

		now := time.Now()

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		lock1 := &corepb.LockId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LockName:    "a",
		}
		lock2 := &corepb.LockId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LockName:    "a/b",
		}
		lock3 := &corepb.LockId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LockName:    "a/b/c",
		}
		lock4 := &corepb.LockId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LockName:    "a/b/c/d",
		}

		// Create leases for four different processes
		lease1 := createLease(t, core, accountId, namespaceId, "process-1", now, 60*time.Minute)
		lease2 := createLease(t, core, accountId, namespaceId, "process-2", now, 60*time.Minute)
		lease3 := createLease(t, core, accountId, namespaceId, "process-3", now, 60*time.Minute)
		lease4 := createLease(t, core, accountId, namespaceId, "process-4", now, 60*time.Minute)

		// Acquire exclusive lock on a/b
		resp1, err := core.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       lock2,
			LeaseId:                      lease1.Id.LeaseId,
			Now:                          now.UnixNano(),
			Exclusive:                    true,
			MaxNumberOfLocksPerNamespace: 100,
		})
		require.NoError(t, err)
		require.True(t, resp1.Success)

		// Try to acquire lock on a (ancestor) - should BLOCK
		resp2, err := core.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       lock1,
			LeaseId:                      lease2.Id.LeaseId,
			Now:                          now.UnixNano(),
			Exclusive:                    false,
			MaxNumberOfLocksPerNamespace: 100,
		})
		require.NoError(t, err)
		require.False(t, resp2.Success) // BLOCKED by descendant exclusive

		// Try to acquire lock on a/b/c (descendant) - should BLOCK
		resp3, err := core.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       lock3,
			LeaseId:                      lease3.Id.LeaseId,
			Now:                          now.UnixNano(),
			Exclusive:                    false,
			MaxNumberOfLocksPerNamespace: 100,
		})
		require.NoError(t, err)
		require.False(t, resp3.Success) // BLOCKED by parent exclusive

		// Try to acquire lock on a/b/c/d (deep descendant) - should BLOCK
		resp4, err := core.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       lock4,
			LeaseId:                      lease4.Id.LeaseId,
			Now:                          now.UnixNano(),
			Exclusive:                    false,
			MaxNumberOfLocksPerNamespace: 100,
		})
		require.NoError(t, err)
		require.False(t, resp4.Success) // BLOCKED by ancestor exclusive
	})

	t.Run("multiple shared locks", func(t *testing.T) {
		core := newLocksCore(t)

		now := time.Now()

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		lock1 := &corepb.LockId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LockName:    "a/b",
		}
		lock2 := &corepb.LockId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LockName:    "a/b/c",
		}
		lock3 := &corepb.LockId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LockName:    "a/b/c/d",
		}

		// Create leases for two different processes
		lease1 := createLease(t, core, accountId, namespaceId, "process-1", now, 60*time.Minute)
		lease2 := createLease(t, core, accountId, namespaceId, "process-2", now, 60*time.Minute)

		// Acquire shared lock on a/b
		resp1, err := core.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       lock1,
			LeaseId:                      lease1.Id.LeaseId,
			Now:                          now.UnixNano(),
			Exclusive:                    false,
			MaxNumberOfLocksPerNamespace: 100,
		})
		require.NoError(t, err)
		require.True(t, resp1.Success)

		// Acquire shared lock on a/b/c - should ALLOW
		resp2, err := core.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       lock2,
			LeaseId:                      lease1.Id.LeaseId,
			Now:                          now.UnixNano(),
			Exclusive:                    false,
			MaxNumberOfLocksPerNamespace: 100,
		})
		require.NoError(t, err)
		require.True(t, resp2.Success) // ALLOWED

		// Acquire shared lock on a/b/c/d - should ALLOW
		resp3, err := core.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       lock3,
			LeaseId:                      lease1.Id.LeaseId,
			Now:                          now.UnixNano(),
			Exclusive:                    false,
			MaxNumberOfLocksPerNamespace: 100,
		})
		require.NoError(t, err)
		require.True(t, resp3.Success) // ALLOWED

		// Try to acquire exclusive lock on any of them - should BLOCK
		resp4, err := core.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       lock2,
			LeaseId:                      lease2.Id.LeaseId,
			Now:                          now.UnixNano(),
			Exclusive:                    true,
			MaxNumberOfLocksPerNamespace: 100,
		})
		require.NoError(t, err)
		require.False(t, resp4.Success) // BLOCKED - has both ancestor and descendant shared
	})

	t.Run("flat lock no hierarchy", func(t *testing.T) {
		core := newLocksCore(t)

		now := time.Now()

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		lock1 := &corepb.LockId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LockName:    "flatlock1",
		}
		lock2 := &corepb.LockId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LockName:    "flatlock2",
		}

		// Create lease with 60 minute TTL
		lease := createLease(t, core, accountId, namespaceId, "process-1", now, 60*time.Minute)

		// Acquire exclusive lock
		resp1, err := core.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       lock1,
			LeaseId:                      lease.Id.LeaseId,
			Now:                          now.UnixNano(),
			Exclusive:                    true,
			MaxNumberOfLocksPerNamespace: 100,
		})
		require.NoError(t, err)
		require.True(t, resp1.Success)

		// Acquire different exclusive lock - should ALLOW (no hierarchy, independent locks)
		resp2, err := core.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       lock2,
			LeaseId:                      lease.Id.LeaseId,
			Now:                          now.UnixNano(),
			Exclusive:                    true,
			MaxNumberOfLocksPerNamespace: 100,
		})
		require.NoError(t, err)
		require.True(t, resp2.Success) // ALLOWED - different flat locks
	})

	t.Run("safe read pattern", func(t *testing.T) {
		core := newLocksCore(t)

		now := time.Now()

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		usersLock := &corepb.LockId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LockName:    "users",
		}
		user123Lock := &corepb.LockId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LockName:    "users/123",
		}
		user456Lock := &corepb.LockId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LockName:    "users/456",
		}

		// Create leases for four different processes
		leaseA := createLease(t, core, accountId, namespaceId, "process-a", now, 60*time.Minute)
		leaseB := createLease(t, core, accountId, namespaceId, "process-b", now, 60*time.Minute)
		leaseC := createLease(t, core, accountId, namespaceId, "process-c", now, 60*time.Minute)
		leaseD := createLease(t, core, accountId, namespaceId, "process-d", now, 60*time.Minute)

		// Client A: Acquire shared lock on users/
		resp1, err := core.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       usersLock,
			LeaseId:                      leaseA.Id.LeaseId,
			Now:                          now.UnixNano(),
			Exclusive:                    false,
			MaxNumberOfLocksPerNamespace: 100,
		})
		require.NoError(t, err)
		require.True(t, resp1.Success)

		// Client B: Acquire shared lock on users/123
		resp2, err := core.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       user123Lock,
			LeaseId:                      leaseB.Id.LeaseId,
			Now:                          now.UnixNano(),
			Exclusive:                    false,
			MaxNumberOfLocksPerNamespace: 100,
		})
		require.NoError(t, err)
		require.True(t, resp2.Success)

		// Client C: Acquire shared lock on users/456
		resp3, err := core.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       user456Lock,
			LeaseId:                      leaseC.Id.LeaseId,
			Now:                          now.UnixNano(),
			Exclusive:                    false,
			MaxNumberOfLocksPerNamespace: 100,
		})
		require.NoError(t, err)
		require.True(t, resp3.Success)

		// Client D: Acquire another shared lock on users/
		resp4, err := core.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       usersLock,
			LeaseId:                      leaseD.Id.LeaseId,
			Now:                          now.UnixNano(),
			Exclusive:                    false,
			MaxNumberOfLocksPerNamespace: 100,
		})
		require.NoError(t, err)
		require.True(t, resp4.Success)
	})

	t.Run("exclusive write pattern", func(t *testing.T) {
		core := newLocksCore(t)

		now := time.Now()

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		user123Lock := &corepb.LockId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LockName:    "users/123",
		}
		usersLock := &corepb.LockId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LockName:    "users",
		}

		// Create leases for three different processes
		leaseA := createLease(t, core, accountId, namespaceId, "process-a", now, 60*time.Minute)
		leaseB := createLease(t, core, accountId, namespaceId, "process-b", now, 60*time.Minute)
		leaseC := createLease(t, core, accountId, namespaceId, "process-c", now, 60*time.Minute)

		// Client A: Acquire exclusive lock on users/123
		resp1, err := core.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       user123Lock,
			LeaseId:                      leaseA.Id.LeaseId,
			Now:                          now.UnixNano(),
			Exclusive:                    true,
			MaxNumberOfLocksPerNamespace: 100,
		})
		require.NoError(t, err)
		require.True(t, resp1.Success)

		// Client B: Try to acquire shared lock on users/123 - BLOCKS
		resp2, err := core.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       user123Lock,
			LeaseId:                      leaseB.Id.LeaseId,
			Now:                          now.UnixNano(),
			Exclusive:                    false,
			MaxNumberOfLocksPerNamespace: 100,
		})
		require.NoError(t, err)
		require.False(t, resp2.Success)

		// Client C: Try to acquire shared lock on users/ - BLOCKS (descendant has exclusive)
		resp3, err := core.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       usersLock,
			LeaseId:                      leaseC.Id.LeaseId,
			Now:                          now.UnixNano(),
			Exclusive:                    false,
			MaxNumberOfLocksPerNamespace: 100,
		})
		require.NoError(t, err)
		require.False(t, resp3.Success)

		// Client A: Release lock on users/123
		_, err = core.ReleaseLock(&corepb.ReleaseLockRequest{
			LockId:  user123Lock,
			LeaseId: leaseA.Id.LeaseId,
			Now:     now.Add(time.Minute).UnixNano(),
		})
		require.NoError(t, err)

		// Client B: Acquire shared lock - SUCCESS
		resp4, err := core.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       user123Lock,
			LeaseId:                      leaseB.Id.LeaseId,
			Now:                          now.Add(2 * time.Minute).UnixNano(),
			Exclusive:                    false,
			MaxNumberOfLocksPerNamespace: 100,
		})
		require.NoError(t, err)
		require.True(t, resp4.Success)

		// Client C: Acquire shared lock - SUCCESS
		resp5, err := core.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       usersLock,
			LeaseId:                      leaseC.Id.LeaseId,
			Now:                          now.Add(2 * time.Minute).UnixNano(),
			Exclusive:                    false,
			MaxNumberOfLocksPerNamespace: 100,
		})
		require.NoError(t, err)
		require.True(t, resp5.Success)
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

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		lockId := &corepb.LockId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LockName:    "test_lock",
		}

		// Create leases with different TTLs
		lease1 := createLease(t, locksCore, accountId, namespaceId, "process-1", now, 30*time.Minute)
		lease2 := createLease(t, locksCore, accountId, namespaceId, "process-2", now.Add(time.Minute), 14*time.Minute)
		lease3 := createLease(t, locksCore, accountId, namespaceId, "process-3", now.Add(2*time.Minute), 43*time.Minute)

		// T+0: Acquire shared lock with process_1 (expires at T+30m)
		response1, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       lockId,
			LeaseId:                      lease1.Id.LeaseId,
			Now:                          now.UnixNano(),
			Exclusive:                    false,
			MaxNumberOfLocksPerNamespace: 10,
		})

		require.NoError(t, err)
		require.NotNil(t, response1.Lock)
		require.True(t, response1.Success)
		require.Equal(t, corepb.LockState_SHARED_LOCKED, response1.Lock.State)
		require.Len(t, response1.Lock.LockHolders, 1)

		// T+1m: Acquire shared lock with process_2 (expires at T+15m)
		response2, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       lockId,
			LeaseId:                      lease2.Id.LeaseId,
			Now:                          now.Add(time.Minute).UnixNano(),
			Exclusive:                    false,
			MaxNumberOfLocksPerNamespace: 10,
		})

		require.NoError(t, err)
		require.NotNil(t, response2.Lock)
		require.True(t, response2.Success)
		require.Equal(t, corepb.LockState_SHARED_LOCKED, response2.Lock.State)
		require.Len(t, response2.Lock.LockHolders, 2)

		// T+2m: Acquire shared lock with process_3 (expires at T+45m)
		response3, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       lockId,
			LeaseId:                      lease3.Id.LeaseId,
			Now:                          now.Add(2 * time.Minute).UnixNano(),
			Exclusive:                    false,
			MaxNumberOfLocksPerNamespace: 10,
		})

		require.NoError(t, err)
		require.NotNil(t, response3.Lock)
		require.True(t, response3.Success)
		require.Equal(t, corepb.LockState_SHARED_LOCKED, response3.Lock.State)
		require.Len(t, response3.Lock.LockHolders, 3)

		// T+20m: Get lock at time between process_2 expiration (T+15m) and process_1 expiration (T+30m)
		// process_2 should have expired, but process_1 and process_3 should still be active
		response4, err := locksCore.GetLock(&corepb.GetLockRequest{
			LockId: lockId,
			Now:    now.Add(20 * time.Minute).UnixNano(),
		})

		require.NoError(t, err)
		require.NotNil(t, response4.Lock)
		require.Equal(t, corepb.LockState_SHARED_LOCKED, response4.Lock.State)
		require.Len(t, response4.Lock.LockHolders, 2) // Only two holders should remain

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

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		lockId := &corepb.LockId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LockName:    "test_lock",
		}

		// Create lease with 60 minute TTL
		lease := createLease(t, locksCore, accountId, namespaceId, "process-1", now, 60*time.Minute)

		// T+0: Acquire lock
		response1, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       lockId,
			LeaseId:                      lease.Id.LeaseId,
			Now:                          now.UnixNano(),
			Exclusive:                    true,
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
			LeaseId:                      lease.Id.LeaseId,
			Now:                          now.Add(2 * time.Minute).UnixNano(),
			Exclusive:                    true,
			MaxNumberOfLocksPerNamespace: 10,
		})

		require.NoError(t, err)
		require.NotNil(t, response3.Lock)
		require.True(t, response3.Success)
		require.Equal(t, corepb.LockState_EXCLUSIVE_LOCKED, response3.Lock.State)
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
			LockId: lockId,
			Now:    now.UnixNano(),
		})

		require.NoError(t, err)
		require.NotNil(t, response1.Lock)
		require.Equal(t, corepb.LockState_UNLOCKED, response1.Lock.State)
	})

	t.Run("exclusive lock", func(t *testing.T) {
		locksCore := newLocksCore(t)

		now := time.Now()

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		lockId := &corepb.LockId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LockName:    "test_lock",
		}

		// Create leases for two different processes
		lease1 := createLease(t, locksCore, accountId, namespaceId, "process-1", now, 60*time.Minute)
		lease2 := createLease(t, locksCore, accountId, namespaceId, "process-2", now, 60*time.Minute)

		// T+0: Acquire lock
		response1, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       lockId,
			LeaseId:                      lease1.Id.LeaseId,
			Now:                          now.UnixNano(),
			Exclusive:                    true,
			MaxNumberOfLocksPerNamespace: 10,
		})

		require.NoError(t, err)
		require.NotNil(t, response1.Lock)
		require.True(t, response1.Success)
		require.Equal(t, corepb.LockState_EXCLUSIVE_LOCKED, response1.Lock.State)

		// T+1m: Release lock with wrong lease id
		response2, err := locksCore.ReleaseLock(&corepb.ReleaseLockRequest{
			LockId:  lockId,
			LeaseId: lease2.Id.LeaseId,
			Now:     now.Add(time.Minute).UnixNano(),
		})

		require.NoError(t, err)
		require.NotNil(t, response2.Lock)
		require.Equal(t, corepb.LockState_EXCLUSIVE_LOCKED, response1.Lock.State)

		// T+2m: Release lock with correct lease id
		response3, err := locksCore.ReleaseLock(&corepb.ReleaseLockRequest{
			LockId:  lockId,
			LeaseId: lease1.Id.LeaseId,
			Now:     now.Add(2 * time.Minute).UnixNano(),
		})

		require.NoError(t, err)
		require.NotNil(t, response3.Lock)
		require.Equal(t, corepb.LockState_UNLOCKED, response3.Lock.State)
	})

	t.Run("shared lock", func(t *testing.T) {
		locksCore := newLocksCore(t)

		now := time.Now()

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		lockId := &corepb.LockId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LockName:    "test_lock",
		}

		// Create leases for two different processes
		lease1 := createLease(t, locksCore, accountId, namespaceId, "process-1", now, 60*time.Minute)
		lease2 := createLease(t, locksCore, accountId, namespaceId, "process-2", now, 60*time.Minute)

		// T+0: Acquire read lock
		response1, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       lockId,
			LeaseId:                      lease1.Id.LeaseId,
			Now:                          now.UnixNano(),
			Exclusive:                    false,
			MaxNumberOfLocksPerNamespace: 10,
		})

		require.NoError(t, err)
		require.NotNil(t, response1.Lock)
		require.True(t, response1.Success)
		require.Equal(t, corepb.LockState_SHARED_LOCKED, response1.Lock.State)

		// T+1m: Acquire read lock from another process
		response2, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       lockId,
			LeaseId:                      lease2.Id.LeaseId,
			Now:                          now.Add(time.Minute).UnixNano(),
			Exclusive:                    false,
			MaxNumberOfLocksPerNamespace: 10,
		})

		require.NoError(t, err)
		require.NotNil(t, response2.Lock)
		require.True(t, response2.Success)
		require.Equal(t, corepb.LockState_SHARED_LOCKED, response2.Lock.State)

		// T+2m: Release lock with first lease id
		response3, err := locksCore.ReleaseLock(&corepb.ReleaseLockRequest{
			LockId:  lockId,
			LeaseId: lease1.Id.LeaseId,
			Now:     now.Add(2 * time.Minute).UnixNano(),
		})

		require.NoError(t, err)
		require.NotNil(t, response3.Lock)
		require.Equal(t, corepb.LockState_SHARED_LOCKED, response3.Lock.State)

		// T+3m: Release lock with second lease id
		response4, err := locksCore.ReleaseLock(&corepb.ReleaseLockRequest{
			LockId:  lockId,
			LeaseId: lease2.Id.LeaseId,
			Now:     now.Add(3 * time.Minute).UnixNano(),
		})

		require.NoError(t, err)
		require.NotNil(t, response4.Lock)
		require.Equal(t, corepb.LockState_UNLOCKED, response4.Lock.State)
		require.Len(t, response4.Lock.LockHolders, 0)
	})

	t.Run("expired lock", func(t *testing.T) {
		locksCore := newLocksCore(t)

		now := time.Now()

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		lockId := &corepb.LockId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LockName:    "test_lock",
		}

		// Create lease with 60 minute TTL
		lease := createLease(t, locksCore, accountId, namespaceId, "process-1", now, 60*time.Minute)

		// T+0: Acquire lock
		response1, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       lockId,
			LeaseId:                      lease.Id.LeaseId,
			Now:                          now.UnixNano(),
			Exclusive:                    true,
			MaxNumberOfLocksPerNamespace: 10,
		})

		require.NoError(t, err)
		require.NotNil(t, response1.Lock)
		require.True(t, response1.Success)
		require.Equal(t, corepb.LockState_EXCLUSIVE_LOCKED, response1.Lock.State)

		// T+61m: Release lock after expiration time
		response2, err := locksCore.ReleaseLock(&corepb.ReleaseLockRequest{
			LockId:  lockId,
			LeaseId: lease.Id.LeaseId,
			Now:     now.Add(61 * time.Minute).UnixNano(),
		})

		require.NoError(t, err)
		require.NotNil(t, response2.Lock)
		require.Equal(t, corepb.LockState_UNLOCKED, response2.Lock.State)
	})

	t.Run("expiration records cleanup", func(t *testing.T) {
		locksCore := newLocksCore(t)

		now := time.Now()
		accountId := rand.Uint64()
		namespaceId := rand.Uint32()
		lockId := &corepb.LockId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LockName:    "test_lock",
		}

		// Create leases for two different processes
		lease1 := createLease(t, locksCore, accountId, namespaceId, "process-1", now, 60*time.Minute)
		lease2 := createLease(t, locksCore, accountId, namespaceId, "process-2", now, 60*time.Minute)

		// T+0: Acquire shared lock with process_1
		response1, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       lockId,
			LeaseId:                      lease1.Id.LeaseId,
			Now:                          now.UnixNano(),
			Exclusive:                    false,
			MaxNumberOfLocksPerNamespace: 10,
		})
		require.NoError(t, err)
		require.NotNil(t, response1.Lock)
		require.True(t, response1.Success)
		require.Equal(t, corepb.LockState_SHARED_LOCKED, response1.Lock.State)
		require.Len(t, response1.Lock.LockHolders, 1)

		// T+1m: Acquire shared lock with process_2
		response2, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       lockId,
			LeaseId:                      lease2.Id.LeaseId,
			Now:                          now.Add(1 * time.Minute).UnixNano(),
			Exclusive:                    false,
			MaxNumberOfLocksPerNamespace: 10,
		})
		require.NoError(t, err)
		require.NotNil(t, response2.Lock)
		require.True(t, response2.Success)
		require.Equal(t, corepb.LockState_SHARED_LOCKED, response2.Lock.State)
		require.Len(t, response2.Lock.LockHolders, 2)

		// T+2m: Release lock from process_1
		// This is the critical operation that must properly delete the old expiration record
		response3, err := locksCore.ReleaseLock(&corepb.ReleaseLockRequest{
			LockId:  lockId,
			LeaseId: lease1.Id.LeaseId,
			Now:     now.Add(2 * time.Minute).UnixNano(),
		})
		require.NoError(t, err)
		require.NotNil(t, response3.Lock)
		require.Equal(t, corepb.LockState_SHARED_LOCKED, response3.Lock.State)
		require.Len(t, response3.Lock.LockHolders, 1)

		// T+3m: Run garbage collection
		// Before the fix, this would crash because the expiration record was corrupted
		// (pointing to a lock that still exists but with the wrong expiration time)
		gcResponse, err := locksCore.RunLocksGarbageCollection(&corepb.RunLocksGarbageCollectionRequest{
			Now:                   now.Add(3 * time.Minute).UnixNano(),
			GcRecordsPageSize:     100,
			GcRecordLocksPageSize: 100,
			MaxVisitedLocks:       100,
		})
		require.NoError(t, err)
		require.NotNil(t, gcResponse)

		// Verify lock still exists with process_2 holder
		response4, err := locksCore.GetLock(&corepb.GetLockRequest{
			LockId: lockId,
			Now:    now.Add(3 * time.Minute).UnixNano(),
		})
		require.NoError(t, err)
		require.NotNil(t, response4.Lock)
		require.Equal(t, corepb.LockState_SHARED_LOCKED, response4.Lock.State)
		require.Len(t, response4.Lock.LockHolders, 1)
	})
}

func TestCore_SnapshotAndRestore(t *testing.T) {
	now := time.Now()

	accountId := rand.Uint64()
	namespaceId := rand.Uint32()

	lockId := &corepb.LockId{
		AccountId:   accountId,
		NamespaceId: namespaceId,
		LockName:    "test_lock",
	}

	// Create two lock cores for testing snapshot and restore
	locksCore1 := newLocksCore(t)
	locksCore2 := newLocksCore(t)

	// Create leases for different processes
	lease1 := createLease(t, locksCore1, accountId, namespaceId, "process-1", now, 60*time.Minute)
	lease2 := createLease(t, locksCore1, accountId, namespaceId, "process-2", now, 60*time.Minute)

	// T+0: Acquire exclusive lock
	response1, err := locksCore1.AcquireLock(&corepb.AcquireLockRequest{
		LockId:                       lockId,
		LeaseId:                      lease1.Id.LeaseId,
		Now:                          now.UnixNano(),
		Exclusive:                    true,
		MaxNumberOfLocksPerNamespace: 10,
	})
	require.NoError(t, err)
	require.True(t, response1.Success)
	require.Equal(t, corepb.LockState_EXCLUSIVE_LOCKED, response1.Lock.State)

	// Take snapshot at this point
	snapshot := locksCore1.Snapshot()

	// T+1m: Release the exclusive lock (after snapshot)
	_, err = locksCore1.ReleaseLock(&corepb.ReleaseLockRequest{
		LockId:  lockId,
		LeaseId: lease1.Id.LeaseId,
		Now:     now.Add(time.Minute).UnixNano(),
	})
	require.NoError(t, err)

	// T+2m: Acquire shared lock with different process (after snapshot)
	response2, err := locksCore1.AcquireLock(&corepb.AcquireLockRequest{
		LockId:                       lockId,
		LeaseId:                      lease2.Id.LeaseId,
		Now:                          now.Add(2 * time.Minute).UnixNano(),
		Exclusive:                    false,
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

	// Create leases for locksCore2 (after restore)
	lease3 := createLease(t, locksCore2, accountId, namespaceId, "process-3", now, 60*time.Minute)
	lease4 := createLease(t, locksCore2, accountId, namespaceId, "process-4", now, 60*time.Minute)

	// T+4m: Try to acquire exclusive lock with different process in restored state (should fail)
	response4, err := locksCore2.AcquireLock(&corepb.AcquireLockRequest{
		LockId:                       lockId,
		LeaseId:                      lease3.Id.LeaseId,
		Now:                          now.Add(4 * time.Minute).UnixNano(),
		Exclusive:                    true,
		MaxNumberOfLocksPerNamespace: 10,
	})
	require.NoError(t, err)
	require.False(t, response4.Success)
	require.Equal(t, corepb.LockState_EXCLUSIVE_LOCKED, response4.Lock.State)

	// T+5m: Try to acquire shared lock with different process in restored state (should fail)
	response5, err := locksCore2.AcquireLock(&corepb.AcquireLockRequest{
		LockId:                       lockId,
		LeaseId:                      lease4.Id.LeaseId,
		Now:                          now.Add(5 * time.Minute).UnixNano(),
		Exclusive:                    false,
		MaxNumberOfLocksPerNamespace: 10,
	})
	require.NoError(t, err)
	require.False(t, response5.Success)
	require.Equal(t, corepb.LockState_EXCLUSIVE_LOCKED, response5.Lock.State)

	// T+6m: Release the exclusive lock in restored state
	// The lock is held by lease1 from locksCore1, so we need to use that lease ID
	_, err = locksCore2.ReleaseLock(&corepb.ReleaseLockRequest{
		LockId:  lockId,
		LeaseId: lease1.Id.LeaseId,
		Now:     now.Add(6 * time.Minute).UnixNano(),
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
		LeaseId:                      lease3.Id.LeaseId,
		Now:                          now.Add(8 * time.Minute).UnixNano(),
		Exclusive:                    false,
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

		// Create leases for three different processes
		lease1 := createLease(t, locksCore, namespaceId.AccountId, namespaceId.NamespaceId, "process-1", now, 60*time.Minute)
		lease2 := createLease(t, locksCore, namespaceId.AccountId, namespaceId.NamespaceId, "process-2", now, 60*time.Minute)
		lease3 := createLease(t, locksCore, namespaceId.AccountId, namespaceId.NamespaceId, "process-3", now, 60*time.Minute)

		// T+0: Acquire exclusive lock for lock_1
		response1, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       lockId1,
			LeaseId:                      lease1.Id.LeaseId,
			Now:                          now.UnixNano(),
			Exclusive:                    true,
			MaxNumberOfLocksPerNamespace: 10,
		})
		require.NoError(t, err)
		require.True(t, response1.Success)

		// T+1m: Acquire shared lock for lock_2
		response2, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       lockId2,
			LeaseId:                      lease2.Id.LeaseId,
			Now:                          now.Add(time.Minute).UnixNano(),
			Exclusive:                    false,
			MaxNumberOfLocksPerNamespace: 10,
		})
		require.NoError(t, err)
		require.True(t, response2.Success)

		// T+2m: Acquire shared lock for lock_3
		response3, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       lockId3,
			LeaseId:                      lease3.Id.LeaseId,
			Now:                          now.Add(2 * time.Minute).UnixNano(),
			Exclusive:                    false,
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

		// Create leases with different TTLs - one that stays active, one that expires
		lease1 := createLease(t, locksCore, namespaceId.AccountId, namespaceId.NamespaceId, "process-1", now, 60*time.Minute)
		lease2 := createLease(t, locksCore, namespaceId.AccountId, namespaceId.NamespaceId, "process-2", now.Add(time.Minute), 1*time.Minute)

		// T+0: Acquire lock that will remain active
		response1, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       lockId1,
			LeaseId:                      lease1.Id.LeaseId,
			Now:                          now.UnixNano(),
			Exclusive:                    true,
			MaxNumberOfLocksPerNamespace: 10,
		})
		require.NoError(t, err)
		require.True(t, response1.Success)

		// T+1m: Acquire lock that will expire
		response2, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       lockId2,
			LeaseId:                      lease2.Id.LeaseId,
			Now:                          now.Add(time.Minute).UnixNano(),
			Exclusive:                    false,
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

		// Create leases for two different processes in different namespaces
		lease1 := createLease(t, locksCore, accountId, lockId1.NamespaceId, "process-1", now, 60*time.Minute)
		lease2 := createLease(t, locksCore, accountId, lockId2.NamespaceId, "process-2", now, 60*time.Minute)

		// T+0: Acquire lock in namespace_1
		response1, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       lockId1,
			LeaseId:                      lease1.Id.LeaseId,
			Now:                          now.UnixNano(),
			Exclusive:                    true,
			MaxNumberOfLocksPerNamespace: 10,
		})
		require.NoError(t, err)
		require.True(t, response1.Success)

		// T+1m: Acquire lock in namespace_2
		response2, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       lockId2,
			LeaseId:                      lease2.Id.LeaseId,
			Now:                          now.Add(time.Minute).UnixNano(),
			Exclusive:                    false,
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

		// Create leases for different processes
		lease1 := createLease(t, locksCore, namespaceId.AccountId, namespaceId.NamespaceId, "process-1", now, 60*time.Minute)
		lease2 := createLease(t, locksCore, namespaceId.AccountId, namespaceId.NamespaceId, "process-2", now, 60*time.Minute)
		lease3 := createLease(t, locksCore, namespaceId.AccountId, namespaceId.NamespaceId, "process-3", now, 60*time.Minute)
		lease4 := createLease(t, locksCore, namespaceId.AccountId, namespaceId.NamespaceId, "process-4", now, 60*time.Minute)

		// T+0: Acquire exclusive lock
		response1, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       lockId1,
			LeaseId:                      lease1.Id.LeaseId,
			Now:                          now.UnixNano(),
			Exclusive:                    true,
			MaxNumberOfLocksPerNamespace: 10,
		})
		require.NoError(t, err)
		require.True(t, response1.Success)

		// T+1m: Acquire shared lock
		response2, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       lockId2,
			LeaseId:                      lease2.Id.LeaseId,
			Now:                          now.Add(time.Minute).UnixNano(),
			Exclusive:                    false,
			MaxNumberOfLocksPerNamespace: 10,
		})
		require.NoError(t, err)
		require.True(t, response2.Success)

		// T+2m: Acquire shared lock with multiple holders
		response3, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       lockId3,
			LeaseId:                      lease3.Id.LeaseId,
			Now:                          now.Add(2 * time.Minute).UnixNano(),
			Exclusive:                    false,
			MaxNumberOfLocksPerNamespace: 10,
		})
		require.NoError(t, err)
		require.True(t, response3.Success)

		// T+3m: Add another shared lock holder
		response4, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       lockId3,
			LeaseId:                      lease4.Id.LeaseId,
			Now:                          now.Add(3 * time.Minute).UnixNano(),
			Exclusive:                    false,
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

		// Check single read lock
		readLockSingle := lockMap["read_lock_single"]
		require.Equal(t, corepb.LockState_SHARED_LOCKED, readLockSingle.State)
		require.Len(t, readLockSingle.LockHolders, 1)

		// Check multiple read lock
		readLockMultiple := lockMap["read_lock_multiple"]
		require.Equal(t, corepb.LockState_SHARED_LOCKED, readLockMultiple.State)
		require.Len(t, readLockMultiple.LockHolders, 2)
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

		// Create leases with short TTL
		lease1 := createLease(t, locksCore, namespaceId.AccountId, namespaceId.NamespaceId, "process-1", now, 1*time.Minute)
		lease2 := createLease(t, locksCore, namespaceId.AccountId, namespaceId.NamespaceId, "process-2", now, 1*time.Minute)

		// T+0: Acquire locks with short expiration
		response1, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       lockId1,
			LeaseId:                      lease1.Id.LeaseId,
			Now:                          now.UnixNano(),
			Exclusive:                    true,
			MaxNumberOfLocksPerNamespace: 10,
		})
		require.NoError(t, err)
		require.True(t, response1.Success)

		response2, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       lockId2,
			LeaseId:                      lease2.Id.LeaseId,
			Now:                          now.UnixNano(),
			Exclusive:                    false,
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
				// Locks 0-4: All holders will expire (short TTL of 30 minutes, expires at T+30)
				lease1 := createLease(t, locksCore, namespaceId.AccountId, namespaceId.NamespaceId, fmt.Sprintf("process-%d-1", i), now, 30*time.Minute)

				response, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
					LockId:                       lockId,
					LeaseId:                      lease1.Id.LeaseId,
					Now:                          now.UnixNano(),
					Exclusive:                    i%2 == 0, // Alternate between exclusive and shared locks
					MaxNumberOfLocksPerNamespace: 100,
				})
				require.NoError(t, err)
				require.NotNil(t, response.Lock)
				require.True(t, response.Success)

				// Add a second holder for shared locks that will also expire
				if response.Lock.State == corepb.LockState_SHARED_LOCKED {
					lease2 := createLease(t, locksCore, namespaceId.AccountId, namespaceId.NamespaceId, fmt.Sprintf("process-%d-2", i), now, 30*time.Minute)
					response2, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
						LockId:                       lockId,
						LeaseId:                      lease2.Id.LeaseId,
						Now:                          now.UnixNano(),
						Exclusive:                    false,
						MaxNumberOfLocksPerNamespace: 100,
					})
					require.NoError(t, err)
					require.NotNil(t, response2.Lock)
					require.True(t, response2.Success)
				}
			} else if i < 10 {
				// Locks 5-9: Some holders will expire, some will remain
				// First holder expires (30 min TTL), second holder remains (60 min TTL)
				lease1 := createLease(t, locksCore, namespaceId.AccountId, namespaceId.NamespaceId, fmt.Sprintf("process-%d-1", i), now, 30*time.Minute)
				lease2 := createLease(t, locksCore, namespaceId.AccountId, namespaceId.NamespaceId, fmt.Sprintf("process-%d-2", i), now, 60*time.Minute)

				response, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
					LockId:                       lockId,
					LeaseId:                      lease1.Id.LeaseId,
					Now:                          now.UnixNano(),
					Exclusive:                    false, // Make all shared locks for consistency
					MaxNumberOfLocksPerNamespace: 100,
				})
				require.NoError(t, err)
				require.NotNil(t, response.Lock)
				require.True(t, response.Success)

				// Add a second holder that will remain
				response2, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
					LockId:                       lockId,
					LeaseId:                      lease2.Id.LeaseId,
					Now:                          now.UnixNano(),
					Exclusive:                    false,
					MaxNumberOfLocksPerNamespace: 100,
				})
				require.NoError(t, err)
				require.NotNil(t, response2.Lock)
				require.True(t, response2.Success)
			} else {
				// Locks 10-14: All holders will remain (60 min TTL)
				lease1 := createLease(t, locksCore, namespaceId.AccountId, namespaceId.NamespaceId, fmt.Sprintf("process-%d-1", i), now, 60*time.Minute)

				response, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
					LockId:                       lockId,
					LeaseId:                      lease1.Id.LeaseId,
					Now:                          now.UnixNano(),
					Exclusive:                    i%2 == 0, // Alternate between exclusive and shared locks
					MaxNumberOfLocksPerNamespace: 100,
				})
				require.NoError(t, err)
				require.NotNil(t, response.Lock)
				require.True(t, response.Success)

				// Add a second holder that will also remain
				if response.Lock.State == corepb.LockState_SHARED_LOCKED {
					lease2 := createLease(t, locksCore, namespaceId.AccountId, namespaceId.NamespaceId, fmt.Sprintf("process-%d-2", i), now, 60*time.Minute)
					response2, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
						LockId:                       lockId,
						LeaseId:                      lease2.Id.LeaseId,
						Now:                          now.UnixNano(),
						Exclusive:                    false,
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
		for i := range 5 {
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
		for i := range 10 {
			lockIds[i] = &corepb.LockId{
				AccountId:   namespaceId.AccountId,
				NamespaceId: namespaceId.NamespaceId,
				LockName:    fmt.Sprintf("lock_%d", i),
			}
		}

		// Create leases for acquiring locks
		leases := make([]*corepb.Lease, 10)
		for i := range 10 {
			leases[i] = createLease(t, locksCore, namespaceId.AccountId, namespaceId.NamespaceId, fmt.Sprintf("process-%d", i), now, 60*time.Minute)
		}

		// Acquire locks in the namespace
		for i, lockId := range lockIds {
			response, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
				LockId:                       lockId,
				LeaseId:                      leases[i].Id.LeaseId,
				Now:                          now.UnixNano(),
				Exclusive:                    i%2 == 0, // Alternate between exclusive and shared locks
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

		// Create a lease for the different namespace lock
		differentLease := createLease(t, locksCore, namespaceId.AccountId, differentNamespaceLockId.NamespaceId, "different-process", now, 60*time.Minute)

		// Acquire a lock in a different namespace
		acquireResponse, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
			LockId:                       differentNamespaceLockId,
			LeaseId:                      differentLease.Id.LeaseId,
			Now:                          now.UnixNano(),
			Exclusive:                    true,
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

func TestCore_LockAncestorNames(t *testing.T) {
	locksCore := newLocksCore(t)
	require.Nil(t, locksCore.lockAncestorNames("flat"))
	require.Nil(t, locksCore.lockAncestorNames(""))
	require.Equal(t, []string{"a"}, locksCore.lockAncestorNames("a/b"))
	require.Equal(t, []string{"a", "a/b"}, locksCore.lockAncestorNames("a/b/c"))
	require.Equal(t, []string{"a", "a/b", "a/b/c"}, locksCore.lockAncestorNames("a/b/c/d"))
}

func TestCore_RevokeLockLease(t *testing.T) {
	t.Run("revokes all locks with pagination", func(t *testing.T) {
		locksCore := newLocksCore(t)

		now := time.Now()
		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		// Create a lease
		lease := createLease(t, locksCore, accountId, namespaceId, "process-1", now, 60*time.Minute)

		// Acquire 1500 locks to test pagination
		numLocks := 1500
		lockIds := make([]*corepb.LockId, numLocks)
		for i := range numLocks {
			lockIds[i] = &corepb.LockId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
				LockName:    fmt.Sprintf("test_lock_%d", i),
			}

			// Acquire the lock
			response, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
				LockId:                       lockIds[i],
				LeaseId:                      lease.Id.LeaseId,
				Now:                          now.UnixNano(),
				Exclusive:                    true,
				MaxNumberOfLocksPerNamespace: 10000,
			})
			require.NoError(t, err)
			require.True(t, response.Success)
			require.Equal(t, corepb.LockState_EXCLUSIVE_LOCKED, response.Lock.State)
		}

		// Verify that counters show the correct number of locks and leases
		counters, err := locksCore.counters.Get(locksCore.badgerStore.View(), accountId, namespaceId)
		require.NoError(t, err)
		require.EqualValues(t, numLocks, counters.NumberOfLocks)
		require.EqualValues(t, 1, counters.NumberOfLeases)

		// Revoke the lease
		_, err = locksCore.RevokeLockLease(&corepb.RevokeLockLeaseRequest{
			LeaseId: lease.Id,
			Now:     now.UnixNano(),
		})
		require.NoError(t, err)

		// Verify that all locks are released
		for i := range numLocks {
			response, err := locksCore.GetLock(&corepb.GetLockRequest{
				LockId: lockIds[i],
				Now:    now.UnixNano(),
			})
			require.NoError(t, err)
			require.Equal(t, corepb.LockState_UNLOCKED, response.Lock.State)
		}

		// Verify that counters are updated correctly
		counters, err = locksCore.counters.Get(locksCore.badgerStore.View(), accountId, namespaceId)
		require.NoError(t, err)
		require.EqualValues(t, 0, counters.NumberOfLocks)
		require.EqualValues(t, 0, counters.NumberOfLeases)

		// Verify the lease is deleted
		_, err = locksCore.GetLockLease(&corepb.GetLockLeaseRequest{
			LeaseId: lease.Id,
		})
		require.Error(t, err)
	})
}

func TestCore_RevokeLockLease_ReleasesSharedLocks(t *testing.T) {
	locksCore := newLocksCore(t)

	now := time.Now()
	accountId := rand.Uint64()
	namespaceId := rand.Uint32()

	// Create two leases
	lease1 := createLease(t, locksCore, accountId, namespaceId, "process-1", now, 60*time.Minute)
	lease2 := createLease(t, locksCore, accountId, namespaceId, "process-2", now, 60*time.Minute)

	lockId := &corepb.LockId{
		AccountId:   accountId,
		NamespaceId: namespaceId,
		LockName:    "shared_lock",
	}

	// Acquire shared lock with lease1
	response1, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
		LockId:                       lockId,
		LeaseId:                      lease1.Id.LeaseId,
		Now:                          now.UnixNano(),
		Exclusive:                    false,
		MaxNumberOfLocksPerNamespace: 10000,
	})
	require.NoError(t, err)
	require.True(t, response1.Success)
	require.Equal(t, corepb.LockState_SHARED_LOCKED, response1.Lock.State)

	// Acquire shared lock with lease2
	response2, err := locksCore.AcquireLock(&corepb.AcquireLockRequest{
		LockId:                       lockId,
		LeaseId:                      lease2.Id.LeaseId,
		Now:                          now.UnixNano(),
		Exclusive:                    false,
		MaxNumberOfLocksPerNamespace: 10000,
	})
	require.NoError(t, err)
	require.True(t, response2.Success)
	require.Equal(t, corepb.LockState_SHARED_LOCKED, response2.Lock.State)
	require.Len(t, response2.Lock.LockHolders, 2)

	// Revoke lease1
	_, err = locksCore.RevokeLockLease(&corepb.RevokeLockLeaseRequest{
		LeaseId: lease1.Id,
		Now:     now.UnixNano(),
	})
	require.NoError(t, err)

	// Verify that the lock is still held by lease2
	response3, err := locksCore.GetLock(&corepb.GetLockRequest{
		LockId: lockId,
		Now:    now.UnixNano(),
	})
	require.NoError(t, err)
	require.Equal(t, corepb.LockState_SHARED_LOCKED, response3.Lock.State)
	require.Len(t, response3.Lock.LockHolders, 1)
	require.EqualValues(t, lease2.Id.LeaseId, response3.Lock.LockHolders[0].LeaseId)

	// Revoke lease2
	_, err = locksCore.RevokeLockLease(&corepb.RevokeLockLeaseRequest{
		LeaseId: lease2.Id,
		Now:     now.UnixNano(),
	})
	require.NoError(t, err)

	// Verify that the lock is now unlocked
	response4, err := locksCore.GetLock(&corepb.GetLockRequest{
		LockId: lockId,
		Now:    now.UnixNano(),
	})
	require.NoError(t, err)
	require.Equal(t, corepb.LockState_UNLOCKED, response4.Lock.State)
}

func newLocksCore(t *testing.T) *Core {
	badgerStore, err := store.NewBadgerInMemoryStore()
	require.NoError(t, err)
	return NewCore(badgerStore, []byte{0x1d, 0x36, 0x00, 0x00}, []byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})
}

// createLease is a test helper to create a lease
func createLease(t *testing.T, core *Core, accountId uint64, namespaceId uint32, processId string, now time.Time, ttl time.Duration) *corepb.Lease {
	t.Helper()
	leaseId := rand.Uint64()
	resp, err := core.CreateLockLease(&corepb.CreateLockLeaseRequest{
		LeaseId: &corepb.LeaseId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LeaseId:     leaseId,
		},
		ProcessId:             processId,
		TtlSeconds:            uint64(ttl.Seconds()),
		Now:                   now.UnixNano(),
		MaxNumberOfLockLeases: 100,
	})
	require.NoError(t, err)
	return resp.Lease
}
