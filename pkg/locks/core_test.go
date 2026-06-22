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

	"github.com/evrblk/grackle/pkg/coreapis"
	"github.com/evrblk/grackle/pkg/corepb"
	"github.com/evrblk/grackle/pkg/tables"
)

func init() {
	registry := monsterax.NewBaseTableRegistry(1)
	tables.RegisterGracklePrefixes(registry)
}

func TestCore_AcquireLock(t *testing.T) {
	t.Run("exclusive", func(t *testing.T) {
		core := newLocksCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespaceId := rand.Uint32()
		lockId := &corepb.LockId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LockName:    "test_lock",
		}

		// Create lease with 60 minute TTL
		lease := createLease(t, core, accountId, namespaceId, "process-1", now, 60*time.Minute)

		// T+0: Acquire lock
		success, lock := acquireLock(t, core, lockId, lease.Id, true, now)
		require.True(t, success)
		require.EqualValues(t, now.UnixNano(), lock.LockedAt)
		require.Equal(t, corepb.LockState_EXCLUSIVE_LOCKED, lock.State)
		require.EqualValues(t, now.UnixNano(), lock.LockHolders[0].LockedAt)

		// T+1m: Get lock
		resp2, err := core.GetLock(&coreapis.GetLockRequest{
			Payload: &corepb.GetLockRequest{
				LockId: lockId,
				Now:    now.Add(time.Minute).UnixNano(),
			},
		})

		require.NoError(t, err)
		require.Nil(t, resp2.ApplicationError)
		require.NotNil(t, resp2.Payload)
		require.NotNil(t, resp2.Payload.Lock)
		require.Equal(t, corepb.LockState_EXCLUSIVE_LOCKED, resp2.Payload.Lock.State)

		// T+61m: Get lock
		resp3, err := core.GetLock(&coreapis.GetLockRequest{
			Payload: &corepb.GetLockRequest{
				LockId: lockId,
				Now:    now.Add(61 * time.Minute).UnixNano(),
			},
		})

		require.NoError(t, err)
		require.Nil(t, resp3.ApplicationError)
		require.NotNil(t, resp3.Payload)
		require.NotNil(t, resp3.Payload.Lock)
		require.Equal(t, corepb.LockState_UNLOCKED, resp3.Payload.Lock.State)
	})

	t.Run("shared lock", func(t *testing.T) {
		core := newLocksCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespaceId := rand.Uint32()
		lockId := &corepb.LockId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LockName:    "test_lock",
		}

		// Create lease with 60 minute TTL
		lease := createLease(t, core, accountId, namespaceId, "process-1", now, 60*time.Minute)

		// T+0: Acquire lock
		success, lock := acquireLock(t, core, lockId, lease.Id, false, now)
		require.True(t, success)
		require.EqualValues(t, now.UnixNano(), lock.LockedAt)
		require.Equal(t, corepb.LockState_SHARED_LOCKED, lock.State)
		require.Len(t, lock.LockHolders, 1)
		require.EqualValues(t, now.UnixNano(), lock.LockHolders[0].LockedAt)

		// T+1m: Get lock
		lock = getLock(t, core, lockId, now.Add(time.Minute))
		require.Equal(t, corepb.LockState_SHARED_LOCKED, lock.State)

		// T+61m: Get lock
		lock = getLock(t, core, lockId, now.Add(61*time.Minute))
		require.Equal(t, corepb.LockState_UNLOCKED, lock.State)
	})

	t.Run("exclusive lock repeatedly", func(t *testing.T) {
		core := newLocksCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespaceId := rand.Uint32()
		lockId := &corepb.LockId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LockName:    "test_lock",
		}

		// Create lease with 60 minute TTL
		lease := createLease(t, core, accountId, namespaceId, "process-1", now, 60*time.Minute)

		// T+0: Acquire lock
		success, lock := acquireLock(t, core, lockId, lease.Id, true, now)
		require.True(t, success)
		require.Equal(t, corepb.LockState_EXCLUSIVE_LOCKED, lock.State)

		// T+1m: Acquire lock again
		success, lock = acquireLock(t, core, lockId, lease.Id, true, now.Add(time.Minute))
		require.True(t, success)
		require.Equal(t, corepb.LockState_EXCLUSIVE_LOCKED, lock.State)
	})

	t.Run("shared lock repeatedly", func(t *testing.T) {
		core := newLocksCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespaceId := rand.Uint32()
		lockId := &corepb.LockId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LockName:    "test_lock",
		}

		// Create lease with 60 minute TTL
		lease := createLease(t, core, accountId, namespaceId, "process-1", now, 60*time.Minute)

		// T+0: Acquire lock
		success, lock := acquireLock(t, core, lockId, lease.Id, false, now)
		require.True(t, success)
		require.Equal(t, corepb.LockState_SHARED_LOCKED, lock.State)

		// T+1m: Acquire lock again
		success, lock = acquireLock(t, core, lockId, lease.Id, false, now.Add(time.Minute))
		require.True(t, success)
		require.Equal(t, corepb.LockState_SHARED_LOCKED, lock.State)
	})

	t.Run("exclusive locked by another process", func(t *testing.T) {
		core := newLocksCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespaceId := rand.Uint32()
		lockId := &corepb.LockId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LockName:    "test_lock",
		}

		// Create leases for two different processes
		lease1 := createLease(t, core, accountId, namespaceId, "process-1", now, 60*time.Minute)
		lease2 := createLease(t, core, accountId, namespaceId, "process-2", now, 60*time.Minute)
		lease3 := createLease(t, core, accountId, namespaceId, "process-3", now, 60*time.Minute)

		// T+0: Acquire lock
		success, lock := acquireLock(t, core, lockId, lease1.Id, true, now)
		require.True(t, success)
		require.Equal(t, corepb.LockState_EXCLUSIVE_LOCKED, lock.State)

		// T+1m: Acquire write lock by another process
		success, lock = acquireLock(t, core, lockId, lease2.Id, true, now.Add(time.Minute))
		require.False(t, success)
		require.Equal(t, corepb.LockState_EXCLUSIVE_LOCKED, lock.State)

		// T+2m: Acquire read lock by another process
		success, lock = acquireLock(t, core, lockId, lease3.Id, false, now.Add(2*time.Minute))
		require.False(t, success)
		require.Equal(t, corepb.LockState_EXCLUSIVE_LOCKED, lock.State)
	})

	t.Run("shared locked by another process", func(t *testing.T) {
		core := newLocksCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespaceId := rand.Uint32()
		lockId := &corepb.LockId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LockName:    "test_lock",
		}

		// Create leases for three different processes
		lease1 := createLease(t, core, accountId, namespaceId, "process-1", now, 60*time.Minute)
		lease2 := createLease(t, core, accountId, namespaceId, "process-2", now, 60*time.Minute)
		lease3 := createLease(t, core, accountId, namespaceId, "process-3", now, 60*time.Minute)

		// T+0: Acquire lock
		success, lock := acquireLock(t, core, lockId, lease1.Id, false, now)
		require.True(t, success)
		require.Equal(t, corepb.LockState_SHARED_LOCKED, lock.State)

		// T+1m: Acquire exclusive lock by another process
		success, lock = acquireLock(t, core, lockId, lease2.Id, true, now.Add(time.Minute))
		require.False(t, success)
		require.Equal(t, corepb.LockState_SHARED_LOCKED, lock.State)

		// T+2m: Acquire shared lock by another process
		success, lock = acquireLock(t, core, lockId, lease3.Id, false, now.Add(2*time.Minute))
		require.True(t, success)
		require.Equal(t, corepb.LockState_SHARED_LOCKED, lock.State)
		require.Len(t, lock.LockHolders, 2)
	})

	t.Run("maximum number of locks per namespace", func(t *testing.T) {
		core := newLocksCore(t)
		now := time.Now()
		accountId1 := rand.Uint64()
		accountId2 := rand.Uint64()
		namespaceId1 := rand.Uint32()
		maxLocksPerNamespace := int64(3)

		// Create lease with 60 minute TTL
		lease := createLease(t, core, accountId1, namespaceId1, "process-1", now, 60*time.Minute)

		// Create locks up to the maximum limit
		for i := 0; i < int(maxLocksPerNamespace); i++ {
			lockId := &corepb.LockId{
				AccountId:   accountId1,
				NamespaceId: namespaceId1,
				LockName:    fmt.Sprintf("lock_%d", i),
			}

			response, err := core.AcquireLock(&coreapis.AcquireLockRequest{
				Payload: &corepb.AcquireLockRequest{
					LockId:                       lockId,
					LeaseId:                      lease.Id.LeaseId,
					Now:                          now.UnixNano(),
					Exclusive:                    false,
					MaxNumberOfLocksPerNamespace: maxLocksPerNamespace,
				},
			})

			require.NoError(t, err)
			require.Nil(t, response.ApplicationError)
			require.NotNil(t, response.Payload)
			require.NotNil(t, response.Payload.Lock)
			require.True(t, response.Payload.Success)
			require.Equal(t, corepb.LockState_SHARED_LOCKED, response.Payload.Lock.State)
		}

		// Try to acquire one more lock - this should fail
		lockId := &corepb.LockId{
			AccountId:   accountId1,
			NamespaceId: namespaceId1,
			LockName:    "lock_exceeding_limit",
		}

		resp1, err := core.AcquireLock(&coreapis.AcquireLockRequest{
			Payload: &corepb.AcquireLockRequest{
				LockId:                       lockId,
				LeaseId:                      lease.Id.LeaseId,
				Now:                          now.Add(time.Second).UnixNano(),
				Exclusive:                    false,
				MaxNumberOfLocksPerNamespace: maxLocksPerNamespace,
			},
		})

		require.NoError(t, err)
		require.NotNil(t, resp1)
		require.Nil(t, resp1.Payload)
		require.NotNil(t, resp1.ApplicationError)
		require.Equal(t, monsterax.ResourceExhausted, resp1.ApplicationError.Code)
		require.Contains(t, resp1.ApplicationError.Message, "max number of locks per namespace reached")

		// Verify that the lock was not created
		lock := getLock(t, core, lockId, now.Add(time.Second))
		require.Equal(t, corepb.LockState_UNLOCKED, lock.State)

		// Test that reusing an existing lock (even if expired) doesn't count against the limit
		existingLockId := &corepb.LockId{
			AccountId:   accountId1,
			NamespaceId: namespaceId1,
			LockName:    "lock_0",
		}

		// Now try to acquire the same lock again - this should succeed because it's reusing an existing lock
		response, err := core.AcquireLock(&coreapis.AcquireLockRequest{
			Payload: &corepb.AcquireLockRequest{
				LockId:                       existingLockId,
				LeaseId:                      lease.Id.LeaseId,
				Now:                          now.Add(time.Second * 2).UnixNano(),
				Exclusive:                    false,
				MaxNumberOfLocksPerNamespace: maxLocksPerNamespace,
			},
		})

		require.NoError(t, err)
		require.NotNil(t, response.Payload.Lock)
		require.True(t, response.Payload.Success)
		require.Equal(t, corepb.LockState_SHARED_LOCKED, response.Payload.Lock.State)

		// Let's release one of the existing locks (for both holders)
		_ = releaseLock(t, core, existingLockId, lease.Id, now.Add(time.Second*3))

		// Now try to acquire another lock again
		response, err = core.AcquireLock(&coreapis.AcquireLockRequest{
			Payload: &corepb.AcquireLockRequest{
				LockId: &corepb.LockId{
					AccountId:   accountId1,
					NamespaceId: namespaceId1,
					LockName:    "lock_4",
				},
				LeaseId:                      lease.Id.LeaseId,
				Now:                          now.Add(time.Second * 4).UnixNano(),
				Exclusive:                    true,
				MaxNumberOfLocksPerNamespace: maxLocksPerNamespace,
			},
		})

		require.NoError(t, err)
		require.NotNil(t, response.Payload.Lock)
		require.True(t, response.Payload.Success)
		require.Equal(t, corepb.LockState_EXCLUSIVE_LOCKED, response.Payload.Lock.State)

		// Test that creating a lock in a different namespace doesn't affect the limit
		differentNamespaceId := rand.Uint32()
		differentNamespaceLockId := &corepb.LockId{
			AccountId:   accountId1,
			NamespaceId: differentNamespaceId,
			LockName:    "lock_different_namespace",
		}

		lease2 := createLease(t, core, accountId1, differentNamespaceId, "process-2", now, 60*time.Minute)

		response, err = core.AcquireLock(&coreapis.AcquireLockRequest{
			Payload: &corepb.AcquireLockRequest{
				LockId:                       differentNamespaceLockId,
				LeaseId:                      lease2.Id.LeaseId,
				Now:                          now.UnixNano(),
				Exclusive:                    true,
				MaxNumberOfLocksPerNamespace: maxLocksPerNamespace,
			},
		})

		require.NoError(t, err)
		require.NotNil(t, response.Payload.Lock)
		require.True(t, response.Payload.Success)
		require.Equal(t, corepb.LockState_EXCLUSIVE_LOCKED, response.Payload.Lock.State)

		// Test that creating a lock with a different account doesn't affect the limit
		differentAccountNamespaceId := rand.Uint32()
		differentAccountLockId := &corepb.LockId{
			AccountId:   accountId2,
			NamespaceId: differentAccountNamespaceId,
			LockName:    "lock_different_account",
		}

		lease3 := createLease(t, core, accountId2, differentAccountNamespaceId, "process-3", now, 60*time.Minute)

		response, err = core.AcquireLock(&coreapis.AcquireLockRequest{
			Payload: &corepb.AcquireLockRequest{
				LockId:                       differentAccountLockId,
				LeaseId:                      lease3.Id.LeaseId,
				Now:                          now.UnixNano(),
				Exclusive:                    true,
				MaxNumberOfLocksPerNamespace: maxLocksPerNamespace,
			},
		})

		require.NoError(t, err)
		require.NotNil(t, response.Payload.Lock)
		require.True(t, response.Payload.Success)
		require.Equal(t, corepb.LockState_EXCLUSIVE_LOCKED, response.Payload.Lock.State)
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
		success, _ := acquireLock(t, core, parentLock, lease1.Id, true, now)
		require.True(t, success)

		// Try to acquire shared lock on child - should BLOCK
		success, _ = acquireLock(t, core, childLock, lease2.Id, false, now)
		require.False(t, success) // BLOCKED by parent exclusive

		// Release parent
		_ = releaseLock(t, core, parentLock, lease1.Id, now.Add(time.Minute))

		// Now child lock should succeed
		success, _ = acquireLock(t, core, childLock, lease2.Id, false, now.Add(2*time.Minute))
		require.True(t, success)
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
		success, _ := acquireLock(t, core, parentLock, lease1.Id, true, now)
		require.True(t, success)

		// Try to acquire exclusive lock on child - should BLOCK
		success, _ = acquireLock(t, core, childLock, lease2.Id, true, now)
		require.False(t, success) // BLOCKED by parent exclusive
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
		success, _ := acquireLock(t, core, parentLock, lease.Id, false, now)
		require.True(t, success)

		// Acquire shared lock on child - should ALLOW
		success, _ = acquireLock(t, core, childLock, lease.Id, false, now)
		require.True(t, success)
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
		success, _ := acquireLock(t, core, parentLock, lease1.Id, false, now)
		require.True(t, success)

		// Try to acquire exclusive lock on child - should BLOCK
		success, _ = acquireLock(t, core, childLock, lease2.Id, true, now)
		require.False(t, success) // BLOCKED by parent shared
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
		success, _ := acquireLock(t, core, childLock, lease1.Id, true, now)
		require.True(t, success)

		// Try to acquire shared lock on parent - should BLOCK
		success, _ = acquireLock(t, core, parentLock, lease2.Id, false, now)
		require.False(t, success) // BLOCKED by child exclusive
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
		success, _ := acquireLock(t, core, childLock, lease1.Id, true, now)
		require.True(t, success)

		// Try to acquire exclusive lock on parent - should BLOCK
		success, _ = acquireLock(t, core, parentLock, lease2.Id, true, now)
		require.False(t, success) // BLOCKED by child exclusive
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
		success, _ := acquireLock(t, core, childLock, lease.Id, false, now)
		require.True(t, success)

		// Acquire shared lock on parent - should ALLOW
		success, _ = acquireLock(t, core, parentLock, lease.Id, false, now)
		require.True(t, success)
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
		success, _ := acquireLock(t, core, childLock, lease1.Id, false, now)
		require.True(t, success)

		// Try to acquire exclusive lock on parent - should BLOCK
		success, _ = acquireLock(t, core, parentLock, lease2.Id, true, now)
		require.False(t, success) // BLOCKED by child shared
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
		success, _ := acquireLock(t, core, lock1, lease.Id, true, now)
		require.True(t, success)

		// Acquire exclusive lock on a/c - should ALLOW (siblings are independent)
		success, _ = acquireLock(t, core, lock2, lease.Id, true, now)
		require.True(t, success)
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
		success, _ := acquireLock(t, core, lock2, lease1.Id, true, now)
		require.True(t, success)

		// Try to acquire lock on a (ancestor) - should BLOCK
		success, _ = acquireLock(t, core, lock1, lease2.Id, false, now)
		require.False(t, success) // BLOCKED by descendant exclusive

		// Try to acquire lock on a/b/c (descendant) - should BLOCK
		success, _ = acquireLock(t, core, lock3, lease3.Id, false, now)
		require.False(t, success) // BLOCKED by parent exclusive

		// Try to acquire lock on a/b/c/d (deep descendant) - should BLOCK
		success, _ = acquireLock(t, core, lock4, lease4.Id, false, now)
		require.False(t, success) // BLOCKED by ancestor exclusive
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
		success, _ := acquireLock(t, core, lock1, lease1.Id, false, now)
		require.True(t, success)

		// Acquire shared lock on a/b/c - should ALLOW
		success, _ = acquireLock(t, core, lock2, lease1.Id, false, now)
		require.True(t, success)

		// Acquire shared lock on a/b/c/d - should ALLOW
		success, _ = acquireLock(t, core, lock3, lease1.Id, false, now)
		require.True(t, success)

		// Try to acquire exclusive lock on any of them - should BLOCK
		success, _ = acquireLock(t, core, lock2, lease2.Id, true, now)
		require.False(t, success) // BLOCKED - has both ancestor and descendant shared
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
		success, _ := acquireLock(t, core, lock1, lease.Id, true, now)
		require.True(t, success)

		// Acquire different exclusive lock - should ALLOW (no hierarchy, independent locks)
		success, _ = acquireLock(t, core, lock2, lease.Id, true, now)
		require.True(t, success)
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
		success, _ := acquireLock(t, core, usersLock, leaseA.Id, false, now)
		require.True(t, success)

		// Client B: Acquire shared lock on users/123
		success, _ = acquireLock(t, core, user123Lock, leaseB.Id, false, now)
		require.True(t, success)

		// Client C: Acquire shared lock on users/456
		success, _ = acquireLock(t, core, user456Lock, leaseC.Id, false, now)
		require.True(t, success)

		// Client D: Acquire another shared lock on users/
		success, _ = acquireLock(t, core, usersLock, leaseD.Id, false, now)
		require.True(t, success)
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
		success, _ := acquireLock(t, core, user123Lock, leaseA.Id, true, now)
		require.True(t, success)

		// Client B: Try to acquire shared lock on users/123 - BLOCKS
		success, _ = acquireLock(t, core, user123Lock, leaseB.Id, false, now)
		require.False(t, success)

		// Client C: Try to acquire shared lock on users/ - BLOCKS (descendant has exclusive)
		success, _ = acquireLock(t, core, usersLock, leaseC.Id, false, now)
		require.False(t, success)

		// Client A: Release lock on users/123
		_ = releaseLock(t, core, user123Lock, leaseA.Id, now.Add(time.Minute))

		// Client B: Acquire shared lock - SUCCESS
		success, _ = acquireLock(t, core, user123Lock, leaseB.Id, false, now.Add(2*time.Minute))
		require.True(t, success)

		// Client C: Acquire shared lock - SUCCESS
		success, _ = acquireLock(t, core, usersLock, leaseC.Id, false, now.Add(2*time.Minute))
		require.True(t, success)
	})

	t.Run("acquire with nonexistent lease", func(t *testing.T) {
		core := newLocksCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespaceId := rand.Uint32()
		lockId := &corepb.LockId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LockName:    "test_lock",
		}

		// Reference a lease that was never created.
		fakeLease := &corepb.LeaseId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LeaseId:     rand.Uint64(),
		}
		appErr := acquireLockWithError(t, core, lockId, fakeLease, true, now)
		require.Equal(t, monsterax.NotFound, appErr.Code)
		require.Contains(t, appErr.Message, "lease not found")

		// No lock row should have been created.
		lock := getLock(t, core, lockId, now)
		require.Equal(t, corepb.LockState_UNLOCKED, lock.State)
	})

	t.Run("acquire with expired lease", func(t *testing.T) {
		core := newLocksCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespaceId := rand.Uint32()
		lockId := &corepb.LockId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LockName:    "test_lock",
		}

		// 1-minute TTL lease, but the acquire happens at T+2m — after expiry.
		lease := createLease(t, core, accountId, namespaceId, "process-1", now, 1*time.Minute)
		appErr := acquireLockWithError(t, core, lockId, lease.Id, true, now.Add(2*time.Minute))
		require.Equal(t, monsterax.NotFound, appErr.Code)
		require.Contains(t, appErr.Message, "lease not found")

		// Lock row was not created.
		lock := getLock(t, core, lockId, now.Add(2*time.Minute))
		require.Equal(t, corepb.LockState_UNLOCKED, lock.State)
	})
}

func TestCore_LockHolderMetadata(t *testing.T) {
	core := newLocksCore(t)
	now := time.Now()
	accountId := rand.Uint64()
	namespaceId := rand.Uint32()
	lockId := &corepb.LockId{
		AccountId:   accountId,
		NamespaceId: namespaceId,
		LockName:    "test_lock",
	}

	// Create lease with 60 minute TTL
	lease := createLease(t, core, accountId, namespaceId, "process-1", now, 60*time.Minute)

	metadata := map[string]string{"host": "node-1", "pid": "1234"}

	// T+0: Acquire exclusive lock with metadata
	resp, err := core.AcquireLock(&coreapis.AcquireLockRequest{
		Payload: &corepb.AcquireLockRequest{
			LockId:                       lockId,
			LeaseId:                      lease.Id.LeaseId,
			Now:                          now.UnixNano(),
			Exclusive:                    true,
			Metadata:                     metadata,
			MaxNumberOfLocksPerNamespace: 2_000,
		},
	})

	require.NoError(t, err)
	require.Nil(t, resp.ApplicationError)
	require.NotNil(t, resp.Payload)
	require.NotNil(t, resp.Payload.Lock)
	require.True(t, resp.Payload.Success)
	require.Equal(t, corepb.LockState_EXCLUSIVE_LOCKED, resp.Payload.Lock.State)
	require.Len(t, resp.Payload.Lock.LockHolders, 1)

	// Metadata is present on the holder in the AcquireLock response.
	require.Equal(t, metadata, resp.Payload.Lock.LockHolders[0].Metadata)

	// And it is persisted: a subsequent GetLock returns the same holder metadata.
	lock := getLock(t, core, lockId, now.Add(time.Minute))
	require.Equal(t, corepb.LockState_EXCLUSIVE_LOCKED, lock.State)
	require.Len(t, lock.LockHolders, 1)
	require.Equal(t, metadata, lock.LockHolders[0].Metadata)
}

func TestCore_CreateLockLease(t *testing.T) {
	t.Run("creates a lease", func(t *testing.T) {
		core := newLocksCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		lease := createLeaseWithMax(t, core, accountId, namespaceId, "process-1", now, 30*time.Minute, 10)
		require.Equal(t, "process-1", lease.ProcessId)
		require.EqualValues(t, now.UnixNano(), lease.CreatedAt)
		require.EqualValues(t, now.Add(30*time.Minute).UnixNano(), lease.ExpiresAt)

		// Counters reflect the new lease.
		counters, err := core.counters.Get(core.badgerStore.View(), accountId, namespaceId)
		require.NoError(t, err)
		require.EqualValues(t, 1, counters.NumberOfLeases)
	})

	t.Run("max number of lock leases per namespace", func(t *testing.T) {
		core := newLocksCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespaceId := rand.Uint32()
		const maxLeases = int64(3)

		// Create leases up to the limit using the same MaxNumberOfLockLeases throughout —
		// each call must succeed.
		for i := 0; i < int(maxLeases); i++ {
			_ = createLeaseWithMax(t, core, accountId, namespaceId, fmt.Sprintf("process_%d", i), now, 60*time.Second, maxLeases)
		}

		// The next attempt must be rejected with ResourceExhausted.
		appErr := createLeaseWithError(t, core, accountId, namespaceId, "process_over", now, 60*time.Second, maxLeases)
		require.Equal(t, monsterax.ResourceExhausted, appErr.Code)
		require.Contains(t, appErr.Message, "max number of lock leases per namespace reached")

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

func TestCore_GetLockLease(t *testing.T) {
	t.Run("returns valid lease", func(t *testing.T) {
		core := newLocksCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		lease := createLease(t, core, accountId, namespaceId, "process-1", now, 60*time.Minute)

		got := getLockLease(t, core, lease.Id, now)
		require.Equal(t, lease.Id.LeaseId, got.Id.LeaseId)
		require.Equal(t, lease.ExpiresAt, got.ExpiresAt)
	})

	t.Run("nonexistent lease", func(t *testing.T) {
		core := newLocksCore(t)
		now := time.Now()

		appErr := getLockLeaseWithError(t, core, &corepb.LeaseId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
			LeaseId:     rand.Uint64(),
		}, now)
		require.Equal(t, monsterax.NotFound, appErr.Code)
		require.Contains(t, appErr.Message, "lease not found")
	})

	t.Run("expired lease", func(t *testing.T) {
		core := newLocksCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		// 1-minute TTL lease, read at T+2m.
		lease := createLease(t, core, accountId, namespaceId, "process-1", now, 1*time.Minute)
		appErr := getLockLeaseWithError(t, core, lease.Id, now.Add(2*time.Minute))
		require.Equal(t, monsterax.NotFound, appErr.Code)
		require.Contains(t, appErr.Message, "lease not found")
	})
}

func TestCore_GetLock(t *testing.T) {
	t.Run("nonexistent", func(t *testing.T) {
		core := newLocksCore(t)
		now := time.Now()
		lockId := &corepb.LockId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
			LockName:    "test_lock",
		}

		// Get lock
		lock := getLock(t, core, lockId, now)
		require.Equal(t, corepb.LockState_UNLOCKED, lock.State)
	})

	t.Run("shared locked with multiple holders between expirations", func(t *testing.T) {
		core := newLocksCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespaceId := rand.Uint32()
		lockId := &corepb.LockId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LockName:    "test_lock",
		}

		// Create leases with different TTLs
		lease1 := createLease(t, core, accountId, namespaceId, "process-1", now, 30*time.Minute)
		lease2 := createLease(t, core, accountId, namespaceId, "process-2", now.Add(time.Minute), 14*time.Minute)
		lease3 := createLease(t, core, accountId, namespaceId, "process-3", now.Add(2*time.Minute), 43*time.Minute)

		// T+0: Acquire shared lock with process_1 (expires at T+30m)
		success, lock := acquireLock(t, core, lockId, lease1.Id, false, now)
		require.True(t, success)
		require.Equal(t, corepb.LockState_SHARED_LOCKED, lock.State)
		require.Len(t, lock.LockHolders, 1)

		// T+1m: Acquire shared lock with process_2 (expires at T+15m)
		success, lock = acquireLock(t, core, lockId, lease2.Id, false, now.Add(time.Minute))
		require.True(t, success)
		require.Equal(t, corepb.LockState_SHARED_LOCKED, lock.State)
		require.Len(t, lock.LockHolders, 2)

		// T+2m: Acquire shared lock with process_3 (expires at T+45m)
		success, lock = acquireLock(t, core, lockId, lease3.Id, false, now.Add(2*time.Minute))
		require.True(t, success)
		require.Equal(t, corepb.LockState_SHARED_LOCKED, lock.State)
		require.Len(t, lock.LockHolders, 3)

		// T+20m: Get lock at time between process_2 expiration (T+15m) and process_1 expiration (T+30m)
		// process_2 should have expired, but process_1 and process_3 should still be active
		lock = getLock(t, core, lockId, now.Add(20*time.Minute))
		require.Equal(t, corepb.LockState_SHARED_LOCKED, lock.State)
		require.Len(t, lock.LockHolders, 2) // Only two holders should remain

		// T+50m: Get lock after all holders have expired
		lock = getLock(t, core, lockId, now.Add(50*time.Minute))
		require.Equal(t, corepb.LockState_UNLOCKED, lock.State)
		require.Len(t, lock.LockHolders, 0)
		require.EqualValues(t, 0, lock.LockedAt)
	})
}

func TestCore_DeleteLock(t *testing.T) {
	t.Run("nonexistent", func(t *testing.T) {
		core := newLocksCore(t)
		now := time.Now()
		lockId := &corepb.LockId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
			LockName:    "test_lock",
		}

		// Delete lock
		_, err := core.DeleteLock(&coreapis.DeleteLockRequest{
			Payload: &corepb.DeleteLockRequest{
				LockId: lockId,
				Now:    now.UnixNano(),
			},
		})

		require.NoError(t, err)
	})

	t.Run("delete acquired lock", func(t *testing.T) {
		core := newLocksCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespaceId := rand.Uint32()
		lockId := &corepb.LockId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LockName:    "test_lock",
		}

		// Create lease with 60 minute TTL
		lease := createLease(t, core, accountId, namespaceId, "process-1", now, 60*time.Minute)

		// T+0: Acquire lock
		success, lock := acquireLock(t, core, lockId, lease.Id, true, now)
		require.True(t, success)
		require.Equal(t, corepb.LockState_EXCLUSIVE_LOCKED, lock.State)

		// T+1m: Delete lock
		_, err := core.DeleteLock(&coreapis.DeleteLockRequest{
			Payload: &corepb.DeleteLockRequest{
				LockId: lockId,
				Now:    now.Add(time.Minute).UnixNano(),
			},
		})

		require.NoError(t, err)

		// T+2m: Acquire lock
		success, lock = acquireLock(t, core, lockId, lease.Id, true, now.Add(2*time.Minute))
		require.True(t, success)
		require.Equal(t, corepb.LockState_EXCLUSIVE_LOCKED, lock.State)
	})
}

func TestCore_ReleaseLock(t *testing.T) {
	t.Run("nonexistent lock", func(t *testing.T) {
		core := newLocksCore(t)
		now := time.Now()
		lockId := &corepb.LockId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
			LockName:    "test_lock",
		}
		leaseId := &corepb.LeaseId{
			AccountId:   lockId.AccountId,
			NamespaceId: lockId.NamespaceId,
			LeaseId:     rand.Uint64(),
		}

		// Release lock
		lock := releaseLock(t, core, lockId, leaseId, now)
		require.Equal(t, corepb.LockState_UNLOCKED, lock.State)
	})

	t.Run("exclusive lock", func(t *testing.T) {
		core := newLocksCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespaceId := rand.Uint32()
		lockId := &corepb.LockId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LockName:    "test_lock",
		}

		// Create leases for two different processes
		lease1 := createLease(t, core, accountId, namespaceId, "process-1", now, 60*time.Minute)
		lease2 := createLease(t, core, accountId, namespaceId, "process-2", now, 60*time.Minute)

		// T+0: Acquire lock
		success, lock := acquireLock(t, core, lockId, lease1.Id, true, now)
		require.True(t, success)
		require.Equal(t, corepb.LockState_EXCLUSIVE_LOCKED, lock.State)

		// T+1m: Release lock with wrong lease id
		lock = releaseLock(t, core, lockId, lease2.Id, now.Add(time.Minute))
		require.Equal(t, corepb.LockState_EXCLUSIVE_LOCKED, lock.State)

		// T+2m: Release lock with correct lease id
		lock = releaseLock(t, core, lockId, lease1.Id, now.Add(2*time.Minute))
		require.Equal(t, corepb.LockState_UNLOCKED, lock.State)
	})

	t.Run("shared lock", func(t *testing.T) {
		core := newLocksCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		lockId := &corepb.LockId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LockName:    "test_lock",
		}

		// Create leases for two different processes
		lease1 := createLease(t, core, accountId, namespaceId, "process-1", now, 60*time.Minute)
		lease2 := createLease(t, core, accountId, namespaceId, "process-2", now, 60*time.Minute)

		// T+0: Acquire read lock
		success, lock := acquireLock(t, core, lockId, lease1.Id, false, now)
		require.True(t, success)
		require.Equal(t, corepb.LockState_SHARED_LOCKED, lock.State)

		// T+1m: Acquire read lock from another process
		success, lock = acquireLock(t, core, lockId, lease2.Id, false, now.Add(time.Minute))
		require.True(t, success)
		require.Equal(t, corepb.LockState_SHARED_LOCKED, lock.State)

		// T+2m: Release lock with first lease id
		lock = releaseLock(t, core, lockId, lease1.Id, now.Add(2*time.Minute))
		require.Equal(t, corepb.LockState_SHARED_LOCKED, lock.State)

		// T+3m: Release lock with second lease id
		lock = releaseLock(t, core, lockId, lease2.Id, now.Add(3*time.Minute))
		require.Equal(t, corepb.LockState_UNLOCKED, lock.State)
		require.Len(t, lock.LockHolders, 0)
	})

	t.Run("expired lock", func(t *testing.T) {
		core := newLocksCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespaceId := rand.Uint32()
		lockId := &corepb.LockId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LockName:    "test_lock",
		}

		// Create lease with 60 minute TTL
		lease := createLease(t, core, accountId, namespaceId, "process-1", now, 60*time.Minute)

		// T+0: Acquire lock
		success, lock := acquireLock(t, core, lockId, lease.Id, true, now)
		require.True(t, success)
		require.Equal(t, corepb.LockState_EXCLUSIVE_LOCKED, lock.State)

		// T+61m: Release lock after expiration time
		lock = releaseLock(t, core, lockId, lease.Id, now.Add(61*time.Minute))
		require.Equal(t, corepb.LockState_UNLOCKED, lock.State)
	})

	t.Run("expiration records cleanup", func(t *testing.T) {
		core := newLocksCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespaceId := rand.Uint32()
		lockId := &corepb.LockId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LockName:    "test_lock",
		}

		// Create leases for two different processes
		lease1 := createLease(t, core, accountId, namespaceId, "process-1", now, 60*time.Minute)
		lease2 := createLease(t, core, accountId, namespaceId, "process-2", now, 60*time.Minute)

		// T+0: Acquire shared lock with process_1
		success, lock := acquireLock(t, core, lockId, lease1.Id, false, now)
		require.True(t, success)
		require.Equal(t, corepb.LockState_SHARED_LOCKED, lock.State)
		require.Len(t, lock.LockHolders, 1)

		// T+1m: Acquire shared lock with process_2
		success, lock = acquireLock(t, core, lockId, lease2.Id, false, now.Add(1*time.Minute))
		require.True(t, success)
		require.Equal(t, corepb.LockState_SHARED_LOCKED, lock.State)
		require.Len(t, lock.LockHolders, 2)

		// T+2m: Release lock from process_1
		// This is the critical operation that must properly delete the old expiration record
		lock = releaseLock(t, core, lockId, lease1.Id, now.Add(2*time.Minute))
		require.Equal(t, corepb.LockState_SHARED_LOCKED, lock.State)
		require.Len(t, lock.LockHolders, 1)

		// T+3m: Run garbage collection
		// Before the fix, this would crash because the expiration record was corrupted
		// (pointing to a lock that still exists but with the wrong expiration time)
		gcResponse, err := core.RunLocksGarbageCollection(&coreapis.RunLocksGarbageCollectionRequest{
			Payload: &corepb.RunLocksGarbageCollectionRequest{
				Now:                   now.Add(3 * time.Minute).UnixNano(),
				GcRecordsPageSize:     100,
				GcRecordLocksPageSize: 100,
				MaxVisitedLocks:       100,
			},
		})
		require.NoError(t, err)
		require.NotNil(t, gcResponse)

		// Verify lock still exists with process_2 holder
		lock = getLock(t, core, lockId, now.Add(3*time.Minute))
		require.Equal(t, corepb.LockState_SHARED_LOCKED, lock.State)
		require.Len(t, lock.LockHolders, 1)
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
	core1 := newLocksCore(t)
	core2 := newLocksCore(t)

	// Create leases for different processes
	lease1 := createLease(t, core1, accountId, namespaceId, "process-1", now, 60*time.Minute)
	lease2 := createLease(t, core1, accountId, namespaceId, "process-2", now, 60*time.Minute)

	// T+0: Acquire exclusive lock
	success, lock := acquireLock(t, core1, lockId, lease1.Id, true, now)
	require.True(t, success)
	require.Equal(t, corepb.LockState_EXCLUSIVE_LOCKED, lock.State)

	// Take snapshot at this point
	snapshot := core1.Snapshot()

	// T+1m: Release the exclusive lock (after snapshot)
	_ = releaseLock(t, core1, lockId, lease1.Id, now.Add(time.Minute))

	// T+2m: Acquire shared lock with different process (after snapshot)
	success, lock = acquireLock(t, core1, lockId, lease2.Id, false, now.Add(2*time.Minute))
	require.True(t, success)
	require.Equal(t, corepb.LockState_SHARED_LOCKED, lock.State)

	// Write snapshot to buffer
	buf := bytes.NewBuffer(nil)
	err := snapshot.Write(buf)
	require.NoError(t, err)

	// Restore snapshot to second core
	err = core2.Restore(io.NopCloser(buf))
	require.NoError(t, err)

	// T+3m: Check that the restored state matches the snapshot state
	// The lock should exist with write lock held by process_1
	lock = getLock(t, core2, lockId, now.Add(3*time.Minute))
	require.Equal(t, corepb.LockState_EXCLUSIVE_LOCKED, lock.State)

	// Create leases for core2 (after restore)
	lease3 := createLease(t, core2, accountId, namespaceId, "process-3", now, 60*time.Minute)
	lease4 := createLease(t, core2, accountId, namespaceId, "process-4", now, 60*time.Minute)

	// T+4m: Try to acquire exclusive lock with different process in restored state (should fail)
	success, lock = acquireLock(t, core2, lockId, lease3.Id, true, now.Add(4*time.Minute))
	require.False(t, success)
	require.Equal(t, corepb.LockState_EXCLUSIVE_LOCKED, lock.State)

	// T+5m: Try to acquire shared lock with different process in restored state (should fail)
	success, lock = acquireLock(t, core2, lockId, lease4.Id, false, now.Add(5*time.Minute))
	require.False(t, success)
	require.Equal(t, corepb.LockState_EXCLUSIVE_LOCKED, lock.State)

	// T+6m: Release the exclusive lock in restored state
	// The lock is held by lease1 from core1, so we need to use that lease ID
	_, err = core2.ReleaseLock(&coreapis.ReleaseLockRequest{
		Payload: &corepb.ReleaseLockRequest{
			LockId:  lockId,
			LeaseId: lease1.Id.LeaseId,
			Now:     now.Add(6 * time.Minute).UnixNano(),
		},
	})
	require.NoError(t, err)

	// T+7m: Verify lock is unlocked in restored state
	lock = getLock(t, core2, lockId, now.Add(7*time.Minute))
	require.Equal(t, corepb.LockState_UNLOCKED, lock.State)

	// T+8m: Acquire shared lock in restored state (should succeed)
	success, lock = acquireLock(t, core2, lockId, lease3.Id, false, now.Add(8*time.Minute))
	require.True(t, success)
	require.Equal(t, corepb.LockState_SHARED_LOCKED, lock.State)

	// Verify that the original core has different state (it should have a read lock from process_2)
	lock = getLock(t, core1, lockId, now.Add(8*time.Minute))
	require.Equal(t, corepb.LockState_SHARED_LOCKED, lock.State)
	require.Len(t, lock.LockHolders, 1)
}

func TestCore_ListLocks(t *testing.T) {
	t.Run("empty namespace", func(t *testing.T) {
		core := newLocksCore(t)
		now := time.Now()
		namespaceId := &corepb.NamespaceId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
		}

		// List locks in empty namespace
		response, err := core.ListLocks(&coreapis.ListLocksRequest{
			Payload: &corepb.ListLocksRequest{
				NamespaceId: namespaceId,
				Now:         now.UnixNano(),
			},
		})

		require.NoError(t, err)
		require.NotNil(t, response)
		require.Nil(t, response.ApplicationError)
		require.NotNil(t, response.Payload)
		require.Empty(t, response.Payload.Locks)
	})

	t.Run("with active locks", func(t *testing.T) {
		core := newLocksCore(t)
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
		lease1 := createLease(t, core, namespaceId.AccountId, namespaceId.NamespaceId, "process-1", now, 60*time.Minute)
		lease2 := createLease(t, core, namespaceId.AccountId, namespaceId.NamespaceId, "process-2", now, 60*time.Minute)
		lease3 := createLease(t, core, namespaceId.AccountId, namespaceId.NamespaceId, "process-3", now, 60*time.Minute)

		// T+0: Acquire exclusive lock for lock_1
		success, _ := acquireLock(t, core, lockId1, lease1.Id, true, now)
		require.True(t, success)

		// T+1m: Acquire shared lock for lock_2
		success, _ = acquireLock(t, core, lockId2, lease2.Id, false, now.Add(time.Minute))
		require.True(t, success)

		// T+2m: Acquire shared lock for lock_3
		success, _ = acquireLock(t, core, lockId3, lease3.Id, false, now.Add(2*time.Minute))
		require.True(t, success)

		// T+3m: List locks
		response4, err := core.ListLocks(&coreapis.ListLocksRequest{
			Payload: &corepb.ListLocksRequest{
				NamespaceId: namespaceId,
				Now:         now.Add(3 * time.Minute).UnixNano(),
			},
		})

		require.NoError(t, err)
		require.NotNil(t, response4)
		require.Nil(t, response4.ApplicationError)
		require.NotNil(t, response4.Payload)
		require.Len(t, response4.Payload.Locks, 3)

		// Verify all locks are returned and are in the correct state
		lockMap := make(map[string]*corepb.Lock)
		for _, lock := range response4.Payload.Locks {
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
		core := newLocksCore(t)
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
		lease1 := createLease(t, core, namespaceId.AccountId, namespaceId.NamespaceId, "process-1", now, 60*time.Minute)
		lease2 := createLease(t, core, namespaceId.AccountId, namespaceId.NamespaceId, "process-2", now.Add(time.Minute), 1*time.Minute)

		// T+0: Acquire lock that will remain active
		success, _ := acquireLock(t, core, lockId1, lease1.Id, true, now)
		require.True(t, success)

		// T+1m: Acquire lock that will expire
		success, _ = acquireLock(t, core, lockId2, lease2.Id, true, now.Add(time.Minute))
		require.True(t, success)

		// T+3m: List locks (after lock_2 has expired)
		response3, err := core.ListLocks(&coreapis.ListLocksRequest{
			Payload: &corepb.ListLocksRequest{
				NamespaceId: namespaceId,
				Now:         now.Add(3 * time.Minute).UnixNano(),
			},
		})

		require.NoError(t, err)
		require.NotNil(t, response3)
		require.Nil(t, response3.ApplicationError)
		require.NotNil(t, response3.Payload)
		require.Len(t, response3.Payload.Locks, 1) // Only the active lock should be returned

		require.Equal(t, "lock_active", response3.Payload.Locks[0].Id.LockName)
		require.Equal(t, corepb.LockState_EXCLUSIVE_LOCKED, response3.Payload.Locks[0].State)
	})

	t.Run("with multiple namespaces", func(t *testing.T) {
		core := newLocksCore(t)
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
		lease1 := createLease(t, core, accountId, lockId1.NamespaceId, "process-1", now, 60*time.Minute)
		lease2 := createLease(t, core, accountId, lockId2.NamespaceId, "process-2", now, 60*time.Minute)

		// T+0: Acquire lock in namespace_1
		success, _ := acquireLock(t, core, lockId1, lease1.Id, true, now)
		require.True(t, success)

		// T+1m: Acquire lock in namespace_2
		success, _ = acquireLock(t, core, lockId2, lease2.Id, false, now.Add(time.Minute))
		require.True(t, success)

		// T+2m: List locks in namespace_1
		response3, err := core.ListLocks(&coreapis.ListLocksRequest{
			Payload: &corepb.ListLocksRequest{
				NamespaceId: &corepb.NamespaceId{
					AccountId:   accountId,
					NamespaceId: lockId1.NamespaceId,
				},
				Now: now.Add(2 * time.Minute).UnixNano(),
			},
		})

		require.NoError(t, err)
		require.NotNil(t, response3)
		require.Nil(t, response3.ApplicationError)
		require.NotNil(t, response3.Payload)
		require.Len(t, response3.Payload.Locks, 1)
		require.Equal(t, "lock_1", response3.Payload.Locks[0].Id.LockName)
		require.Equal(t, lockId1.NamespaceId, response3.Payload.Locks[0].Id.NamespaceId)

		// T+3m: List locks in namespace_2
		response4, err := core.ListLocks(&coreapis.ListLocksRequest{
			Payload: &corepb.ListLocksRequest{
				NamespaceId: &corepb.NamespaceId{
					AccountId:   accountId,
					NamespaceId: lockId2.NamespaceId,
				},
				Now: now.Add(3 * time.Minute).UnixNano(),
			},
		})

		require.NoError(t, err)
		require.NotNil(t, response4)
		require.Nil(t, response4.ApplicationError)
		require.NotNil(t, response4.Payload)
		require.Len(t, response4.Payload.Locks, 1)
		require.Equal(t, "lock_2", response4.Payload.Locks[0].Id.LockName)
		require.Equal(t, lockId2.NamespaceId, response4.Payload.Locks[0].Id.NamespaceId)

		// T+4m: List locks in nonexistent namespace
		response5, err := core.ListLocks(&coreapis.ListLocksRequest{
			Payload: &corepb.ListLocksRequest{
				NamespaceId: &corepb.NamespaceId{
					AccountId:   accountId,
					NamespaceId: rand.Uint32(),
				},
				Now: now.Add(4 * time.Minute).UnixNano(),
			},
		})

		require.NoError(t, err)
		require.NotNil(t, response5)
		require.Empty(t, response5.Payload.Locks)
	})

	t.Run("with mixed lock states", func(t *testing.T) {
		core := newLocksCore(t)
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
		lease1 := createLease(t, core, namespaceId.AccountId, namespaceId.NamespaceId, "process-1", now, 60*time.Minute)
		lease2 := createLease(t, core, namespaceId.AccountId, namespaceId.NamespaceId, "process-2", now, 60*time.Minute)
		lease3 := createLease(t, core, namespaceId.AccountId, namespaceId.NamespaceId, "process-3", now, 60*time.Minute)
		lease4 := createLease(t, core, namespaceId.AccountId, namespaceId.NamespaceId, "process-4", now, 60*time.Minute)

		// T+0: Acquire exclusive lock
		success, _ := acquireLock(t, core, lockId1, lease1.Id, true, now)
		require.True(t, success)

		// T+1m: Acquire shared lock
		success, _ = acquireLock(t, core, lockId2, lease2.Id, false, now.Add(time.Minute))
		require.True(t, success)

		// T+2m: Acquire shared lock with multiple holders
		success, _ = acquireLock(t, core, lockId3, lease3.Id, false, now.Add(2*time.Minute))
		require.True(t, success)

		// T+3m: Add another shared lock holder
		success, _ = acquireLock(t, core, lockId3, lease4.Id, false, now.Add(3*time.Minute))
		require.True(t, success)

		// T+4m: List locks
		response5, err := core.ListLocks(&coreapis.ListLocksRequest{
			Payload: &corepb.ListLocksRequest{
				NamespaceId: namespaceId,
				Now:         now.Add(4 * time.Minute).UnixNano(),
			},
		})

		require.NoError(t, err)
		require.NotNil(t, response5)
		require.Len(t, response5.Payload.Locks, 3)

		// Verify lock states and holders
		lockMap := make(map[string]*corepb.Lock)
		for _, lock := range response5.Payload.Locks {
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
		core := newLocksCore(t)
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
		lease1 := createLease(t, core, namespaceId.AccountId, namespaceId.NamespaceId, "process-1", now, 1*time.Minute)
		lease2 := createLease(t, core, namespaceId.AccountId, namespaceId.NamespaceId, "process-2", now, 1*time.Minute)

		// T+0: Acquire locks with short expiration
		success, _ := acquireLock(t, core, lockId1, lease1.Id, true, now)
		require.True(t, success)

		success, _ = acquireLock(t, core, lockId2, lease2.Id, false, now)
		require.True(t, success)

		// T+3m: List locks (after all locks have expired)
		response3, err := core.ListLocks(&coreapis.ListLocksRequest{
			Payload: &corepb.ListLocksRequest{
				NamespaceId: namespaceId,
				Now:         now.Add(3 * time.Minute).UnixNano(),
			},
		})

		require.NoError(t, err)
		require.NotNil(t, response3)
		require.Empty(t, response3.Payload.Locks) // No locks should be returned as they're all expired
	})
}

func TestCore_RunLocksGarbageCollection(t *testing.T) {
	t.Run("with multiple expiring locks", func(t *testing.T) {
		core := newLocksCore(t)
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
				lease1 := createLease(t, core, namespaceId.AccountId, namespaceId.NamespaceId, fmt.Sprintf("process-%d-1", i), now, 30*time.Minute)

				success, lock := acquireLock(t, core, lockId, lease1.Id, i%2 == 0, now) // Alternate between exclusive and shared locks
				require.True(t, success)

				// Add a second holder for shared locks that will also expire
				if lock.State == corepb.LockState_SHARED_LOCKED {
					lease2 := createLease(t, core, namespaceId.AccountId, namespaceId.NamespaceId, fmt.Sprintf("process-%d-2", i), now, 30*time.Minute)
					success, _ := acquireLock(t, core, lockId, lease2.Id, false, now)
					require.True(t, success)
				}
			} else if i < 10 {
				// Locks 5-9: Some holders will expire, some will remain
				// First holder expires (30 min TTL), second holder remains (60 min TTL)
				lease1 := createLease(t, core, namespaceId.AccountId, namespaceId.NamespaceId, fmt.Sprintf("process-%d-1", i), now, 30*time.Minute)
				lease2 := createLease(t, core, namespaceId.AccountId, namespaceId.NamespaceId, fmt.Sprintf("process-%d-2", i), now, 60*time.Minute)

				success, _ := acquireLock(t, core, lockId, lease1.Id, false, now) // Make all shared locks for consistency
				require.True(t, success)

				// Add a second holder that will remain
				success, _ = acquireLock(t, core, lockId, lease2.Id, false, now)
				require.True(t, success)
			} else {
				// Locks 10-14: All holders will remain (60 min TTL)
				lease1 := createLease(t, core, namespaceId.AccountId, namespaceId.NamespaceId, fmt.Sprintf("process-%d-1", i), now, 60*time.Minute)

				success, lock := acquireLock(t, core, lockId, lease1.Id, i%2 == 0, now) // Alternate between exclusive and shared locks
				require.True(t, success)

				// Add a second holder that will also remain
				if lock.State == corepb.LockState_SHARED_LOCKED {
					lease2 := createLease(t, core, namespaceId.AccountId, namespaceId.NamespaceId, fmt.Sprintf("process-%d-2", i), now, 60*time.Minute)
					success, _ := acquireLock(t, core, lockId, lease2.Id, false, now)
					require.True(t, success)
				}
			}
		}

		// Verify all locks exist and are locked before garbage collection
		for _, lockId := range lockIds {
			lock := getLock(t, core, lockId, now)
			require.NotEqual(t, corepb.LockState_UNLOCKED, lock.State)
		}

		// Run garbage collection at the moment when some locks expire (T+31 minutes)
		gcTime := now.Add(31 * time.Minute)
		gcResponse, err := core.RunLocksGarbageCollection(&coreapis.RunLocksGarbageCollectionRequest{
			Payload: &corepb.RunLocksGarbageCollectionRequest{
				Now:                   gcTime.UnixNano(),
				GcRecordsPageSize:     100,
				GcRecordLocksPageSize: 100,
				MaxVisitedLocks:       maxVisitedLocks,
			},
		})

		require.NoError(t, err)
		require.NotNil(t, gcResponse)

		// Verify the state of locks after garbage collection
		// Note: We use the public GetLock method which internally calls checkLockExpiration
		// to verify the true state of the locks after garbage collection

		// Locks 0-4 should be unlocked (all holders expired)
		for i := range 5 {
			lock := getLock(t, core, lockIds[i], gcTime)
			require.Equal(t, corepb.LockState_UNLOCKED, lock.State, "Lock %d should be unlocked", i)
		}

		// Locks 5-9 should still be locked but with fewer holders
		for i := 5; i < 10; i++ {
			lock := getLock(t, core, lockIds[i], gcTime)
			require.NotEqual(t, corepb.LockState_UNLOCKED, lock.State, "Lock %d should still be locked", i)

			// For shared locks, verify only one holder remains
			if lock.State == corepb.LockState_SHARED_LOCKED {
				require.Len(t, lock.LockHolders, 1, "Lock %d should have exactly one holder remaining", i)
			}
		}

		// Locks 10-14 should still be locked with all holders
		for i := 10; i < numLocks; i++ {
			lock := getLock(t, core, lockIds[i], gcTime)
			require.NotEqual(t, corepb.LockState_UNLOCKED, lock.State, "Lock %d should still be locked", i)

			// For shared locks, verify both holders remain
			if lock.State == corepb.LockState_SHARED_LOCKED {
				require.Len(t, lock.LockHolders, 2, "Lock %d should have both holders remaining", i)
			}
		}

		// Run garbage collection again to process the remaining locks
		// This should process locks 5-14 since locks 0-4 were already deleted
		gcResponse2, err := core.RunLocksGarbageCollection(&coreapis.RunLocksGarbageCollectionRequest{
			Payload: &corepb.RunLocksGarbageCollectionRequest{
				Now:                   gcTime.UnixNano(),
				GcRecordsPageSize:     100,
				GcRecordLocksPageSize: 100,
				MaxVisitedLocks:       maxVisitedLocks,
			},
		})

		require.NoError(t, err)
		require.NotNil(t, gcResponse2)

		// Verify that locks 5-9 still have their remaining holders
		for i := 5; i < 10; i++ {
			lock := getLock(t, core, lockIds[i], gcTime)
			require.NotEqual(t, corepb.LockState_UNLOCKED, lock.State, "Lock %d should still be locked after second GC", i)
		}

		// Verify that locks 10-14 still have all their holders
		for i := 10; i < numLocks; i++ {
			lock := getLock(t, core, lockIds[i], gcTime)
			require.NotEqual(t, corepb.LockState_UNLOCKED, lock.State, "Lock %d should still be locked after second GC", i)
		}
	})

	t.Run("with deleted namespace", func(t *testing.T) {
		core := newLocksCore(t)
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
			leases[i] = createLease(t, core, namespaceId.AccountId, namespaceId.NamespaceId, fmt.Sprintf("process-%d", i), now, 60*time.Minute)
		}

		// Acquire locks in the namespace
		for i, lockId := range lockIds {
			success, _ := acquireLock(t, core, lockId, leases[i].Id, i%2 == 0, now) // Alternate between exclusive and shared locks
			require.True(t, success)
		}

		// Verify that locks in a different namespace are accessible after GC
		differentNamespaceLockId := &corepb.LockId{
			AccountId:   namespaceId.AccountId,
			NamespaceId: rand.Uint32(),
			LockName:    "different_lock",
		}

		// Create a lease for the different namespace lock
		differentLease := createLease(t, core, namespaceId.AccountId, differentNamespaceLockId.NamespaceId, "different-process", now, 60*time.Minute)

		// Acquire a lock in a different namespace
		success, _ := acquireLock(t, core, differentNamespaceLockId, differentLease.Id, true, now)
		require.True(t, success)

		// Verify locks exist by getting them
		for _, lockId := range lockIds {
			lock := getLock(t, core, lockId, now)
			require.NotEqual(t, corepb.LockState_UNLOCKED, lock.State)
		}

		// Mark the namespace as deleted using LocksDeleteNamespace
		deleteResponse, err := core.LocksDeleteNamespace(&coreapis.LocksDeleteNamespaceRequest{
			Payload: &corepb.LocksDeleteNamespaceRequest{
				NamespaceId: namespaceId,
				Now:         now.UnixNano(),
			},
		})

		require.NoError(t, err)
		require.NotNil(t, deleteResponse)

		// Run garbage collection to clean up the deleted namespace
		gcResponse, err := core.RunLocksGarbageCollection(&coreapis.RunLocksGarbageCollectionRequest{
			Payload: &corepb.RunLocksGarbageCollectionRequest{
				Now:                   now.UnixNano(),
				GcRecordsPageSize:     100,
				GcRecordLocksPageSize: 100,
				MaxVisitedLocks:       1000,
			},
		})

		require.NoError(t, err)
		require.NotNil(t, gcResponse)

		// Verify that locks in the deleted namespace are no longer accessible
		for _, lockId := range lockIds {
			lock := getLock(t, core, lockId, now)
			require.Equal(t, corepb.LockState_UNLOCKED, lock.State)
		}

		// Verify the different namespace lock still exists after GC
		lock := getLock(t, core, differentNamespaceLockId, now)
		require.Equal(t, corepb.LockState_EXCLUSIVE_LOCKED, lock.State)
	})
}

func TestCore_LockAncestorNames(t *testing.T) {
	core := newLocksCore(t)
	require.Nil(t, core.lockAncestorNames("flat"))
	require.Nil(t, core.lockAncestorNames(""))
	require.Equal(t, []string{"a"}, core.lockAncestorNames("a/b"))
	require.Equal(t, []string{"a", "a/b"}, core.lockAncestorNames("a/b/c"))
	require.Equal(t, []string{"a", "a/b", "a/b/c"}, core.lockAncestorNames("a/b/c/d"))
}

func TestCore_RevokeLockLease(t *testing.T) {
	t.Run("revokes all locks with pagination", func(t *testing.T) {
		core := newLocksCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		// Create a lease
		lease := createLease(t, core, accountId, namespaceId, "process-1", now, 60*time.Minute)

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
			success, lock := acquireLock(t, core, lockIds[i], lease.Id, true, now)
			require.True(t, success)
			require.Equal(t, corepb.LockState_EXCLUSIVE_LOCKED, lock.State)
		}

		// Verify that counters show the correct number of locks and leases
		counters, err := core.counters.Get(core.badgerStore.View(), accountId, namespaceId)
		require.NoError(t, err)
		require.EqualValues(t, numLocks, counters.NumberOfLocks)
		require.EqualValues(t, 1, counters.NumberOfLeases)

		// Revoke the lease
		_, err = core.RevokeLockLease(&coreapis.RevokeLockLeaseRequest{
			Payload: &corepb.RevokeLockLeaseRequest{
				LeaseId: lease.Id,
				Now:     now.UnixNano(),
			},
		})
		require.NoError(t, err)

		// Verify that all locks are released
		for i := range numLocks {
			lock := getLock(t, core, lockIds[i], now)
			require.Equal(t, corepb.LockState_UNLOCKED, lock.State)
		}

		// Verify that counters are updated correctly
		counters, err = core.counters.Get(core.badgerStore.View(), accountId, namespaceId)
		require.NoError(t, err)
		require.EqualValues(t, 0, counters.NumberOfLocks)
		require.EqualValues(t, 0, counters.NumberOfLeases)

		// Verify the lease is deleted
		resp, err := core.GetLockLease(&coreapis.GetLockLeaseRequest{
			Payload: &corepb.GetLockLeaseRequest{
				LeaseId: lease.Id,
			},
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Nil(t, resp.Payload)
		require.NotNil(t, resp.ApplicationError)
		require.Equal(t, monsterax.NotFound, resp.ApplicationError.Code)
	})

	t.Run("nonexistent lease", func(t *testing.T) {
		core := newLocksCore(t)
		now := time.Now()

		// Revoking a lease that was never created is a no-op (idempotent success).
		revokeLockLease(t, core, &corepb.LeaseId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
			LeaseId:     rand.Uint64(),
		}, now)
	})

	t.Run("expired lease", func(t *testing.T) {
		core := newLocksCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespaceId := rand.Uint32()
		lockId := &corepb.LockId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LockName:    "test_lock",
		}

		// 1-minute TTL lease holding an exclusive lock.
		lease := createLease(t, core, accountId, namespaceId, "process-1", now, 1*time.Minute)
		success, _ := acquireLock(t, core, lockId, lease.Id, true, now)
		require.True(t, success)

		counters, err := core.counters.Get(core.badgerStore.View(), accountId, namespaceId)
		require.NoError(t, err)
		require.EqualValues(t, 1, counters.NumberOfLocks)
		require.EqualValues(t, 1, counters.NumberOfLeases)

		// T+2m: lease has expired. Revoke still proceeds and releases the held lock.
		revokeLockLease(t, core, lease.Id, now.Add(2*time.Minute))

		// Lease is gone.
		appErr := getLockLeaseWithError(t, core, lease.Id, now.Add(2*time.Minute))
		require.Equal(t, monsterax.NotFound, appErr.Code)

		// Lock held by the expired lease is released; counters reflect the revocation.
		lock := getLock(t, core, lockId, now.Add(2*time.Minute))
		require.Equal(t, corepb.LockState_UNLOCKED, lock.State)

		counters, err = core.counters.Get(core.badgerStore.View(), accountId, namespaceId)
		require.NoError(t, err)
		require.EqualValues(t, 0, counters.NumberOfLocks)
		require.EqualValues(t, 0, counters.NumberOfLeases)
	})
}

func TestCore_RevokeLockLease_ReleasesSharedLocks(t *testing.T) {
	core := newLocksCore(t)
	now := time.Now()
	accountId := rand.Uint64()
	namespaceId := rand.Uint32()
	lockId := &corepb.LockId{
		AccountId:   accountId,
		NamespaceId: namespaceId,
		LockName:    "shared_lock",
	}

	// Create two leases
	lease1 := createLease(t, core, accountId, namespaceId, "process-1", now, 60*time.Minute)
	lease2 := createLease(t, core, accountId, namespaceId, "process-2", now, 60*time.Minute)

	// Acquire shared lock with lease1
	success, lock := acquireLock(t, core, lockId, lease1.Id, false, now)
	require.True(t, success)
	require.Equal(t, corepb.LockState_SHARED_LOCKED, lock.State)

	// Acquire shared lock with lease2
	success, lock = acquireLock(t, core, lockId, lease2.Id, false, now)
	require.True(t, success)
	require.Equal(t, corepb.LockState_SHARED_LOCKED, lock.State)
	require.Len(t, lock.LockHolders, 2)

	// Revoke lease1
	_, err := core.RevokeLockLease(&coreapis.RevokeLockLeaseRequest{
		Payload: &corepb.RevokeLockLeaseRequest{
			LeaseId: lease1.Id,
			Now:     now.UnixNano(),
		},
	})
	require.NoError(t, err)

	// Verify that the lock is still held by lease2
	lock = getLock(t, core, lockId, now)
	require.Equal(t, corepb.LockState_SHARED_LOCKED, lock.State)
	require.Len(t, lock.LockHolders, 1)
	require.EqualValues(t, lease2.Id.LeaseId, lock.LockHolders[0].LeaseId)

	// Revoke lease2
	_, err = core.RevokeLockLease(&coreapis.RevokeLockLeaseRequest{
		Payload: &corepb.RevokeLockLeaseRequest{
			LeaseId: lease2.Id,
			Now:     now.UnixNano(),
		},
	})
	require.NoError(t, err)

	// Verify that the lock is now unlocked
	lock = getLock(t, core, lockId, now)
	require.Equal(t, corepb.LockState_UNLOCKED, lock.State)
}

func TestCore_RefreshLockLease(t *testing.T) {
	t.Run("refreshes a valid lease", func(t *testing.T) {
		core := newLocksCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		lease := createLease(t, core, accountId, namespaceId, "process-1", now, 1*time.Minute)

		// T+30s: refresh with a fresh 5 minute TTL
		refreshAt := now.Add(30 * time.Second)
		resp, err := core.RefreshLockLease(&coreapis.RefreshLockLeaseRequest{
			Payload: &corepb.RefreshLockLeaseRequest{
				LeaseId:    lease.Id,
				TtlSeconds: uint64((5 * time.Minute).Seconds()),
				Now:        refreshAt.UnixNano(),
			},
		})
		require.NoError(t, err)
		require.Nil(t, resp.ApplicationError)
		require.NotNil(t, resp.Payload)
		require.NotNil(t, resp.Payload.Lease)
		require.EqualValues(t, refreshAt.Add(5*time.Minute).UnixNano(), resp.Payload.Lease.ExpiresAt)

		// Lease is still readable at T+4m after refresh
		getResp, err := core.GetLockLease(&coreapis.GetLockLeaseRequest{
			Payload: &corepb.GetLockLeaseRequest{
				LeaseId: lease.Id,
				Now:     refreshAt.Add(4 * time.Minute).UnixNano(),
			},
		})
		require.NoError(t, err)
		require.Nil(t, getResp.ApplicationError)
		require.EqualValues(t, refreshAt.Add(5*time.Minute).UnixNano(), getResp.Payload.Lease.ExpiresAt)
	})

	t.Run("returns not found for missing lease", func(t *testing.T) {
		core := newLocksCore(t)
		now := time.Now()

		resp, err := core.RefreshLockLease(&coreapis.RefreshLockLeaseRequest{
			Payload: &corepb.RefreshLockLeaseRequest{
				LeaseId: &corepb.LeaseId{
					AccountId:   rand.Uint64(),
					NamespaceId: rand.Uint32(),
					LeaseId:     rand.Uint64(),
				},
				TtlSeconds: 60,
				Now:        now.UnixNano(),
			},
		})
		require.NoError(t, err)
		require.Nil(t, resp.Payload)
		require.NotNil(t, resp.ApplicationError)
		require.Equal(t, monsterax.NotFound, resp.ApplicationError.Code)
	})

	t.Run("revokes an expired lease and releases its locks", func(t *testing.T) {
		core := newLocksCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		// Lease with 1 minute TTL
		lease := createLease(t, core, accountId, namespaceId, "process-1", now, 1*time.Minute)

		// Acquire an exclusive and a shared lock under that lease
		exclusiveLockId := &corepb.LockId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LockName:    "exclusive_lock",
		}
		sharedLockId := &corepb.LockId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LockName:    "shared_lock",
		}

		success, _ := acquireLock(t, core, exclusiveLockId, lease.Id, true, now)
		require.True(t, success)
		success, _ = acquireLock(t, core, sharedLockId, lease.Id, false, now)
		require.True(t, success)

		counters, err := core.counters.Get(core.badgerStore.View(), accountId, namespaceId)
		require.NoError(t, err)
		require.EqualValues(t, 2, counters.NumberOfLocks)
		require.EqualValues(t, 1, counters.NumberOfLeases)

		// T+2m: refresh after the lease has expired
		refreshAt := now.Add(2 * time.Minute)
		resp, err := core.RefreshLockLease(&coreapis.RefreshLockLeaseRequest{
			Payload: &corepb.RefreshLockLeaseRequest{
				LeaseId:    lease.Id,
				TtlSeconds: 60,
				Now:        refreshAt.UnixNano(),
			},
		})
		require.NoError(t, err)
		require.Nil(t, resp.Payload)
		require.NotNil(t, resp.ApplicationError)
		require.Equal(t, monsterax.NotFound, resp.ApplicationError.Code)

		// Lease is deleted
		getResp, err := core.GetLockLease(&coreapis.GetLockLeaseRequest{
			Payload: &corepb.GetLockLeaseRequest{
				LeaseId: lease.Id,
				Now:     refreshAt.UnixNano(),
			},
		})
		require.NoError(t, err)
		require.Nil(t, getResp.Payload)
		require.NotNil(t, getResp.ApplicationError)
		require.Equal(t, monsterax.NotFound, getResp.ApplicationError.Code)

		// Locks held by the lease are released
		require.Equal(t, corepb.LockState_UNLOCKED, getLock(t, core, exclusiveLockId, refreshAt).State)
		require.Equal(t, corepb.LockState_UNLOCKED, getLock(t, core, sharedLockId, refreshAt).State)

		// Counters reflect the revocation
		counters, err = core.counters.Get(core.badgerStore.View(), accountId, namespaceId)
		require.NoError(t, err)
		require.EqualValues(t, 0, counters.NumberOfLocks)
		require.EqualValues(t, 0, counters.NumberOfLeases)
	})

	t.Run("revokes an expired lease while preserving shared locks held by others", func(t *testing.T) {
		core := newLocksCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespaceId := rand.Uint32()
		lockId := &corepb.LockId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LockName:    "shared_lock",
		}

		// Lease 1 expires in 1m, lease 2 stays valid for 1h
		expiringLease := createLease(t, core, accountId, namespaceId, "process-1", now, 1*time.Minute)
		validLease := createLease(t, core, accountId, namespaceId, "process-2", now, 60*time.Minute)

		success, _ := acquireLock(t, core, lockId, expiringLease.Id, false, now)
		require.True(t, success)
		success, lock := acquireLock(t, core, lockId, validLease.Id, false, now)
		require.True(t, success)
		require.Len(t, lock.LockHolders, 2)

		// T+2m: refresh the expired lease — it should be revoked
		refreshAt := now.Add(2 * time.Minute)
		resp, err := core.RefreshLockLease(&coreapis.RefreshLockLeaseRequest{
			Payload: &corepb.RefreshLockLeaseRequest{
				LeaseId:    expiringLease.Id,
				TtlSeconds: 60,
				Now:        refreshAt.UnixNano(),
			},
		})
		require.NoError(t, err)
		require.Nil(t, resp.Payload)
		require.NotNil(t, resp.ApplicationError)
		require.Equal(t, monsterax.NotFound, resp.ApplicationError.Code)

		// The shared lock is still held by the valid lease
		lock = getLock(t, core, lockId, refreshAt)
		require.Equal(t, corepb.LockState_SHARED_LOCKED, lock.State)
		require.Len(t, lock.LockHolders, 1)
		require.EqualValues(t, validLease.Id.LeaseId, lock.LockHolders[0].LeaseId)

		counters, err := core.counters.Get(core.badgerStore.View(), accountId, namespaceId)
		require.NoError(t, err)
		require.EqualValues(t, 1, counters.NumberOfLocks)
		require.EqualValues(t, 1, counters.NumberOfLeases)
	})
}

func TestCore_ListLockLeases(t *testing.T) {
	t.Run("lists multiple leases", func(t *testing.T) {
		core := newLocksCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		lease1 := createLease(t, core, accountId, namespaceId, "process-1", now, 60*time.Minute)
		lease2 := createLease(t, core, accountId, namespaceId, "process-2", now, 60*time.Minute)
		lease3 := createLease(t, core, accountId, namespaceId, "process-3", now, 60*time.Minute)

		resp := listLockLeases(t, core, &corepb.NamespaceId{AccountId: accountId, NamespaceId: namespaceId}, now)
		require.Len(t, resp.Leases, 3)

		leaseIds := make(map[uint64]bool)
		for _, lease := range resp.Leases {
			leaseIds[lease.Id.LeaseId] = true
		}
		require.True(t, leaseIds[lease1.Id.LeaseId])
		require.True(t, leaseIds[lease2.Id.LeaseId])
		require.True(t, leaseIds[lease3.Id.LeaseId])
	})

	t.Run("filters out expired leases", func(t *testing.T) {
		core := newLocksCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		expiringLease := createLease(t, core, accountId, namespaceId, "process-1", now, 1*time.Minute)
		lease2 := createLease(t, core, accountId, namespaceId, "process-2", now, 5*time.Minute)
		lease3 := createLease(t, core, accountId, namespaceId, "process-3", now, 10*time.Minute)

		// T+2m: the 1-minute lease has expired; the other two are still alive.
		futureTime := now.Add(2 * time.Minute)
		resp := listLockLeases(t, core, &corepb.NamespaceId{AccountId: accountId, NamespaceId: namespaceId}, futureTime)
		require.Len(t, resp.Leases, 2)

		leaseIds := make(map[uint64]bool)
		for _, lease := range resp.Leases {
			leaseIds[lease.Id.LeaseId] = true
		}
		require.False(t, leaseIds[expiringLease.Id.LeaseId])
		require.True(t, leaseIds[lease2.Id.LeaseId])
		require.True(t, leaseIds[lease3.Id.LeaseId])
	})

	t.Run("scoped to namespace", func(t *testing.T) {
		core := newLocksCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		nsA := rand.Uint32()
		nsB := rand.Uint32()

		ownLease := createLease(t, core, accountId, nsA, "process-1", now, 60*time.Minute)
		otherLease := createLease(t, core, accountId, nsB, "process-2", now, 60*time.Minute)

		resp := listLockLeases(t, core, &corepb.NamespaceId{AccountId: accountId, NamespaceId: nsA}, now)
		require.Len(t, resp.Leases, 1)
		require.Equal(t, ownLease.Id.LeaseId, resp.Leases[0].Id.LeaseId)
		require.NotEqual(t, otherLease.Id.LeaseId, resp.Leases[0].Id.LeaseId)
	})

	t.Run("returns empty list for namespace with no leases", func(t *testing.T) {
		core := newLocksCore(t)
		now := time.Now()

		resp := listLockLeases(t, core, &corepb.NamespaceId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
		}, now)
		require.Empty(t, resp.Leases)
	})
}

func TestCore_ListLockLeasesByProcessId(t *testing.T) {
	t.Run("lists leases for specific process", func(t *testing.T) {
		core := newLocksCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		// Two leases for process-1, one lease for process-2.
		lease1a := createLease(t, core, accountId, namespaceId, "process-1", now, 60*time.Minute)
		lease1b := createLease(t, core, accountId, namespaceId, "process-1", now, 60*time.Minute)
		lease2 := createLease(t, core, accountId, namespaceId, "process-2", now, 60*time.Minute)

		resp := listLockLeasesByProcessId(t, core, &corepb.NamespaceId{AccountId: accountId, NamespaceId: namespaceId}, "process-1", now)
		require.Len(t, resp.Leases, 2)

		leaseIds := make(map[uint64]bool)
		for _, lease := range resp.Leases {
			require.Equal(t, "process-1", lease.ProcessId)
			leaseIds[lease.Id.LeaseId] = true
		}
		require.True(t, leaseIds[lease1a.Id.LeaseId])
		require.True(t, leaseIds[lease1b.Id.LeaseId])
		require.False(t, leaseIds[lease2.Id.LeaseId])
	})

	t.Run("filters out expired leases", func(t *testing.T) {
		core := newLocksCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		_ = createLease(t, core, accountId, namespaceId, "process-1", now, 1*time.Minute) // expires by T+2m
		stillAlive := createLease(t, core, accountId, namespaceId, "process-1", now, 5*time.Minute)

		futureTime := now.Add(2 * time.Minute)
		resp := listLockLeasesByProcessId(t, core, &corepb.NamespaceId{AccountId: accountId, NamespaceId: namespaceId}, "process-1", futureTime)
		require.Len(t, resp.Leases, 1)
		require.Equal(t, stillAlive.Id.LeaseId, resp.Leases[0].Id.LeaseId)
	})

	t.Run("returns empty list for process with no leases", func(t *testing.T) {
		core := newLocksCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		// A lease exists for a different process.
		_ = createLease(t, core, accountId, namespaceId, "process-1", now, 60*time.Minute)

		resp := listLockLeasesByProcessId(t, core, &corepb.NamespaceId{AccountId: accountId, NamespaceId: namespaceId}, "process-other", now)
		require.Empty(t, resp.Leases)
	})
}

func TestCore_ListLocksByLeaseId(t *testing.T) {
	t.Run("lists locks held by lease", func(t *testing.T) {
		core := newLocksCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		lease1 := createLease(t, core, accountId, namespaceId, "process-1", now, 60*time.Minute)
		lease2 := createLease(t, core, accountId, namespaceId, "process-2", now, 60*time.Minute)

		// lease1 acquires 3 locks, lease2 acquires 2.
		lockIds := make([]*corepb.LockId, 5)
		for i := 0; i < 5; i++ {
			lockIds[i] = &corepb.LockId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
				LockName:    fmt.Sprintf("test_lock_%d", i),
			}
		}
		for i := 0; i < 3; i++ {
			success, _ := acquireLock(t, core, lockIds[i], lease1.Id, true, now)
			require.True(t, success)
		}
		for i := 3; i < 5; i++ {
			success, _ := acquireLock(t, core, lockIds[i], lease2.Id, true, now)
			require.True(t, success)
		}

		resp := listLocksByLeaseId(t, core, lease1.Id, now)
		require.Len(t, resp.Locks, 3)

		lockNames := make(map[string]bool)
		for _, lock := range resp.Locks {
			lockNames[lock.Id.LockName] = true
		}
		require.True(t, lockNames["test_lock_0"])
		require.True(t, lockNames["test_lock_1"])
		require.True(t, lockNames["test_lock_2"])
		require.False(t, lockNames["test_lock_3"])
		require.False(t, lockNames["test_lock_4"])
	})

	t.Run("returns shared lock held by lease alongside others", func(t *testing.T) {
		core := newLocksCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespaceId := rand.Uint32()
		lockId := &corepb.LockId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LockName:    "shared_lock",
		}

		lease1 := createLease(t, core, accountId, namespaceId, "process-1", now, 60*time.Minute)
		lease2 := createLease(t, core, accountId, namespaceId, "process-2", now, 60*time.Minute)

		// Both leases acquire a shared lock.
		success, _ := acquireLock(t, core, lockId, lease1.Id, false, now)
		require.True(t, success)
		success, _ = acquireLock(t, core, lockId, lease2.Id, false, now)
		require.True(t, success)

		// lease1's view returns the shared lock.
		resp := listLocksByLeaseId(t, core, lease1.Id, now)
		require.Len(t, resp.Locks, 1)
		require.Equal(t, "shared_lock", resp.Locks[0].Id.LockName)

		// After lease1 releases, its view becomes empty; lease2 still sees the lock.
		_ = releaseLock(t, core, lockId, lease1.Id, now)

		resp1After := listLocksByLeaseId(t, core, lease1.Id, now)
		require.Empty(t, resp1After.Locks)

		resp2 := listLocksByLeaseId(t, core, lease2.Id, now)
		require.Len(t, resp2.Locks, 1)
	})

	t.Run("filters out expired locks", func(t *testing.T) {
		core := newLocksCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespaceId := rand.Uint32()
		lockId := &corepb.LockId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LockName:    "test_lock",
		}

		// 1-minute lease, acquire a lock under it, then list at T+2m.
		lease := createLease(t, core, accountId, namespaceId, "process-1", now, 1*time.Minute)
		success, _ := acquireLock(t, core, lockId, lease.Id, true, now)
		require.True(t, success)

		// The lock holders expired with the lease; ListLocksByLeaseId filters out unlocked rows.
		resp := listLocksByLeaseId(t, core, lease.Id, now.Add(2*time.Minute))
		require.Empty(t, resp.Locks)
	})

	t.Run("returns empty list for lease with no locks", func(t *testing.T) {
		core := newLocksCore(t)
		now := time.Now()
		accountId := rand.Uint64()
		namespaceId := rand.Uint32()

		// Lease exists but never acquired any locks.
		lease := createLease(t, core, accountId, namespaceId, "process-1", now, 60*time.Minute)

		resp := listLocksByLeaseId(t, core, lease.Id, now)
		require.Empty(t, resp.Locks)
	})
}

func newLocksCore(t *testing.T) *Core {
	badgerStore, err := store.NewBadgerInMemoryStore()
	require.NoError(t, err)
	return NewCore(badgerStore, []byte{0x1d, 0x36, 0x00, 0x00}, []byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})
}

func createLease(t *testing.T, core *Core, accountId uint64, namespaceId uint32, processId string, now time.Time, ttl time.Duration) *corepb.Lease {
	t.Helper()

	leaseId := rand.Uint64()
	resp, err := core.CreateLockLease(&coreapis.CreateLockLeaseRequest{
		Payload: &corepb.CreateLockLeaseRequest{
			LeaseId: &corepb.LeaseId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
				LeaseId:     leaseId,
			},
			ProcessId:             processId,
			TtlSeconds:            uint64(ttl.Seconds()),
			Now:                   now.UnixNano(),
			MaxNumberOfLockLeases: 100,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Nil(t, resp.ApplicationError)
	require.NotNil(t, resp.Payload)
	require.NotNil(t, resp.Payload.Lease)
	return resp.Payload.Lease
}

func acquireLock(t *testing.T, core *Core, lockId *corepb.LockId, leaseId *corepb.LeaseId, exclusive bool, now time.Time) (bool, *corepb.Lock) {
	t.Helper()

	resp, err := core.AcquireLock(&coreapis.AcquireLockRequest{
		Payload: &corepb.AcquireLockRequest{
			LockId:                       lockId,
			LeaseId:                      leaseId.LeaseId,
			Now:                          now.UnixNano(),
			Exclusive:                    exclusive,
			MaxNumberOfLocksPerNamespace: 2_000,
		},
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Nil(t, resp.ApplicationError)
	require.NotNil(t, resp.Payload)
	require.NotNil(t, resp.Payload.Lock)

	return resp.Payload.Success, resp.Payload.Lock
}

func releaseLock(t *testing.T, core *Core, lockId *corepb.LockId, leaseId *corepb.LeaseId, now time.Time) *corepb.Lock {
	t.Helper()

	resp, err := core.ReleaseLock(&coreapis.ReleaseLockRequest{
		Payload: &corepb.ReleaseLockRequest{
			LockId:  lockId,
			LeaseId: leaseId.LeaseId,
			Now:     now.UnixNano(),
		},
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Nil(t, resp.ApplicationError)
	require.NotNil(t, resp.Payload)
	require.NotNil(t, resp.Payload.Lock)

	return resp.Payload.Lock
}

func getLock(t *testing.T, core *Core, lockId *corepb.LockId, now time.Time) *corepb.Lock {
	t.Helper()

	resp, err := core.GetLock(&coreapis.GetLockRequest{
		Payload: &corepb.GetLockRequest{
			LockId: lockId,
			Now:    now.UnixNano(),
		},
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Nil(t, resp.ApplicationError)
	require.NotNil(t, resp.Payload)
	require.NotNil(t, resp.Payload.Lock)

	return resp.Payload.Lock
}

func acquireLockWithError(t *testing.T, core *Core, lockId *corepb.LockId, leaseId *corepb.LeaseId, exclusive bool, now time.Time) *monsterax.Error {
	t.Helper()

	resp, err := core.AcquireLock(&coreapis.AcquireLockRequest{
		Payload: &corepb.AcquireLockRequest{
			LockId:                       lockId,
			LeaseId:                      leaseId.LeaseId,
			Now:                          now.UnixNano(),
			Exclusive:                    exclusive,
			MaxNumberOfLocksPerNamespace: 2_000,
		},
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Nil(t, resp.Payload)
	require.NotNil(t, resp.ApplicationError)

	return resp.ApplicationError
}

func createLeaseWithMax(t *testing.T, core *Core, accountId uint64, namespaceId uint32, processId string, now time.Time, ttl time.Duration, maxNumberOfLockLeases int64) *corepb.Lease {
	t.Helper()

	resp, err := core.CreateLockLease(&coreapis.CreateLockLeaseRequest{
		Payload: &corepb.CreateLockLeaseRequest{
			LeaseId: &corepb.LeaseId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
				LeaseId:     rand.Uint64(),
			},
			ProcessId:             processId,
			TtlSeconds:            uint64(ttl.Seconds()),
			Now:                   now.UnixNano(),
			MaxNumberOfLockLeases: maxNumberOfLockLeases,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Nil(t, resp.ApplicationError)
	require.NotNil(t, resp.Payload)
	require.NotNil(t, resp.Payload.Lease)
	return resp.Payload.Lease
}

func createLeaseWithError(t *testing.T, core *Core, accountId uint64, namespaceId uint32, processId string, now time.Time, ttl time.Duration, maxNumberOfLockLeases int64) *monsterax.Error {
	t.Helper()

	resp, err := core.CreateLockLease(&coreapis.CreateLockLeaseRequest{
		Payload: &corepb.CreateLockLeaseRequest{
			LeaseId: &corepb.LeaseId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
				LeaseId:     rand.Uint64(),
			},
			ProcessId:             processId,
			TtlSeconds:            uint64(ttl.Seconds()),
			Now:                   now.UnixNano(),
			MaxNumberOfLockLeases: maxNumberOfLockLeases,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Nil(t, resp.Payload)
	require.NotNil(t, resp.ApplicationError)
	return resp.ApplicationError
}

func getLockLease(t *testing.T, core *Core, leaseId *corepb.LeaseId, now time.Time) *corepb.Lease {
	t.Helper()

	resp, err := core.GetLockLease(&coreapis.GetLockLeaseRequest{
		Payload: &corepb.GetLockLeaseRequest{
			LeaseId: leaseId,
			Now:     now.UnixNano(),
		},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Nil(t, resp.ApplicationError)
	require.NotNil(t, resp.Payload)
	require.NotNil(t, resp.Payload.Lease)
	return resp.Payload.Lease
}

func getLockLeaseWithError(t *testing.T, core *Core, leaseId *corepb.LeaseId, now time.Time) *monsterax.Error {
	t.Helper()

	resp, err := core.GetLockLease(&coreapis.GetLockLeaseRequest{
		Payload: &corepb.GetLockLeaseRequest{
			LeaseId: leaseId,
			Now:     now.UnixNano(),
		},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Nil(t, resp.Payload)
	require.NotNil(t, resp.ApplicationError)
	return resp.ApplicationError
}

func revokeLockLease(t *testing.T, core *Core, leaseId *corepb.LeaseId, now time.Time) {
	t.Helper()

	resp, err := core.RevokeLockLease(&coreapis.RevokeLockLeaseRequest{
		Payload: &corepb.RevokeLockLeaseRequest{
			LeaseId: leaseId,
			Now:     now.UnixNano(),
		},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Nil(t, resp.ApplicationError)
	require.NotNil(t, resp.Payload)
}

func listLockLeases(t *testing.T, core *Core, namespaceId *corepb.NamespaceId, now time.Time) *corepb.ListLockLeasesResponse {
	t.Helper()

	resp, err := core.ListLockLeases(&coreapis.ListLockLeasesRequest{
		Payload: &corepb.ListLockLeasesRequest{
			NamespaceId: namespaceId,
			Now:         now.UnixNano(),
		},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Nil(t, resp.ApplicationError)
	require.NotNil(t, resp.Payload)
	return resp.Payload
}

func listLockLeasesByProcessId(t *testing.T, core *Core, namespaceId *corepb.NamespaceId, processId string, now time.Time) *corepb.ListLockLeasesByProcessIdResponse {
	t.Helper()

	resp, err := core.ListLockLeasesByProcessId(&coreapis.ListLockLeasesByProcessIdRequest{
		Payload: &corepb.ListLockLeasesByProcessIdRequest{
			NamespaceId: namespaceId,
			ProcessId:   processId,
			Now:         now.UnixNano(),
		},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Nil(t, resp.ApplicationError)
	require.NotNil(t, resp.Payload)
	return resp.Payload
}

func listLocksByLeaseId(t *testing.T, core *Core, leaseId *corepb.LeaseId, now time.Time) *corepb.ListLocksByLeaseIdResponse {
	t.Helper()

	resp, err := core.ListLocksByLeaseId(&coreapis.ListLocksByLeaseIdRequest{
		Payload: &corepb.ListLocksByLeaseIdRequest{
			LeaseId: leaseId,
			Now:     now.UnixNano(),
		},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Nil(t, resp.ApplicationError)
	require.NotNil(t, resp.Payload)
	return resp.Payload
}
