# Hierarchical Lock Semantics

This document defines the behavior for hierarchical shared/exclusive locks.

## Lock Types

- **Shared Lock (S)**: Multiple readers can hold shared locks simultaneously. Used for read-only operations.
- **Exclusive Lock (X)**: Only one holder can have an exclusive lock. Used for write operations.

## Hierarchical Lock Compatibility Table

The table below describes what should happen when attempting to acquire a lock, given existing locks in the hierarchy.

| Existing Lock | Requested Lock | Result | Reasoning |
|---------------|----------------|--------|-----------|
| `a/b` (X) | `a/b/c` (S) | ❌ BLOCK | Parent exclusive lock prevents all descendant locks |
| `a/b` (X) | `a/b/c` (X) | ❌ BLOCK | Parent exclusive lock prevents all descendant locks |
| `a/b` (S) | `a/b/c` (S) | ✅ ALLOW | Parent shared lock allows descendant shared locks |
| `a/b` (S) | `a/b/c` (X) | ❌ BLOCK | Parent shared lock prevents descendant exclusive locks |
| `a/b/c` (X) | `a/b` (S) | ❌ BLOCK | Descendant exclusive lock prevents ancestor locks (intent conflict) |
| `a/b/c` (X) | `a/b` (X) | ❌ BLOCK | Descendant exclusive lock prevents ancestor locks (intent conflict) |
| `a/b/c` (S) | `a/b` (S) | ✅ ALLOW | Descendant shared lock allows ancestor shared locks |
| `a/b/c` (S) | `a/b` (X) | ❌ BLOCK | Descendant shared lock prevents ancestor exclusive locks |
| `a/b` (X) | `a/b` (S) | ❌ BLOCK | Same-level exclusive lock blocks all other locks |
| `a/b` (X) | `a/b` (X) | ❌ BLOCK | Same-level exclusive lock blocks all other locks |
| `a/b` (S) | `a/b` (S) | ✅ ALLOW | Multiple shared locks on same path are allowed |
| `a/b` (S) | `a/b` (X) | ❌ BLOCK | Shared lock prevents exclusive lock on same path |
| `a` (X) | `a/b` (S) | ❌ BLOCK | Parent exclusive lock prevents all descendant locks |
| `a` (X) | `a/b` (X) | ❌ BLOCK | Parent exclusive lock prevents all descendant locks |
| `a` (S) | `a/b` (S) | ✅ ALLOW | Parent shared lock allows descendant shared locks |
| `a` (S) | `a/b` (X) | ❌ BLOCK | Parent shared lock prevents descendant exclusive locks |
| `a/b/c/d` (X) | `a/b` (S) | ❌ BLOCK | Deep descendant exclusive lock prevents ancestor locks |
| `a/b/c/d` (X) | `a/b` (X) | ❌ BLOCK | Deep descendant exclusive lock prevents ancestor locks |
| `a/b/c/d` (S) | `a/b` (S) | ✅ ALLOW | Deep descendant shared lock allows ancestor shared locks |
| `a/b/c/d` (S) | `a/b` (X) | ❌ BLOCK | Deep descendant shared lock prevents ancestor exclusive locks |

---

## Multiple Existing Locks

When multiple locks exist in the hierarchy, the most restrictive rule applies.

| Existing Locks | Requested Lock Path | Result | Reasoning |
|----------------|---------------------|--------|-----------|
| `a/b` (S), `a/b/c` (S) | `a/b/c/d` (S) | ✅ ALLOW | All ancestors are shared, allowing descendant shared lock |
| `a/b` (S), `a/b/c` (S) | `a/b/c/d` (X) | ❌ BLOCK | Ancestor shared locks prevent descendant exclusive lock |
| `a/b/c` (S), `a/b/c/d` (S) | `a/b` (S) | ✅ ALLOW | All descendants are shared, allowing ancestor shared lock |
| `a/b/c` (S), `a/b/c/d` (S) | `a/b` (X) | ❌ BLOCK | Descendant shared locks prevent ancestor exclusive lock |

---

## Sibling Paths (No Hierarchical Relationship)

Locks on sibling paths should **not** interfere with each other:

| Existing Lock | Requested Lock | Result | Reasoning |
|---------------|----------------|--------|-----------|
| `a/b` (X) | `a/c` (S) | ✅ ALLOW | Sibling paths are independent |
| `a/b` (X) | `a/c` (X) | ✅ ALLOW | Sibling paths are independent |
| `a/b` (S) | `a/c` (S) | ✅ ALLOW | Sibling paths are independent |
| `a/b` (S) | `a/c` (X) | ✅ ALLOW | Sibling paths are independent |
| `a/b/c` (X) | `a/d/e` (X) | ✅ ALLOW | Sibling paths are independent |

---

## Core Principles

### 1. Exclusive Lock Semantics
- An **exclusive lock** at any level in the hierarchy grants exclusive access to that path and all its descendants
- No other locks (shared or exclusive) can be acquired on:
  - The same path
  - Any descendant paths
  - Any ancestor paths (to prevent intent conflicts)

### 2. Shared Lock Semantics
- A **shared lock** allows multiple readers at the same level
- Shared locks at a parent level allow shared locks at descendant levels
- Shared locks prevent exclusive locks from being acquired on:
  - The same path
  - Any descendant paths
  - Any ancestor paths

### 3. Intent Conflict Prevention
- If a descendant path has **any lock** (shared or exclusive), acquiring an **exclusive lock** on an ancestor should block
  - This prevents "upgrading" an ancestor lock that would invalidate active descendant locks
- If a descendant path has a **shared lock**, acquiring a **shared lock** on an ancestor is allowed
  - This is safe because shared locks are compatible

### 4. Lock Ordering
To prevent deadlocks in hierarchical locking, clients should generally:
- Acquire locks from **root to leaf** (parent before child)
- Release locks from **leaf to root** (child before parent)

### 5. Path Independence
- Paths that are not in the same hierarchy (siblings or unrelated paths) should not interfere with each other
- Example: `a/b` and `a/c` are siblings and their locks don't conflict

---

## Examples

### Example 1: Safe Read Pattern
```
1. Client A: Acquire Shared lock on `users/` → SUCCESS
2. Client B: Acquire Shared lock on `users/123` → SUCCESS
3. Client C: Acquire Shared lock on `users/456` → SUCCESS
4. Client D: Acquire Shared lock on `users/` → SUCCESS

All clients can read concurrently.
```

### Example 2: Exclusive Write Pattern
```
1. Client A: Acquire Exclusive lock on `users/123` → SUCCESS
2. Client B: Acquire Shared lock on `users/123` → BLOCKS (waits for A)
3. Client C: Acquire Shared lock on `users/` → BLOCKS (descendant has exclusive lock)
4. Client A: Release lock on `users/123` → SUCCESS
5. Client B: Acquires Shared lock → SUCCESS
6. Client C: Acquires Shared lock → SUCCESS
```

### Example 3: Parent Lock Prevention
```
1. Client A: Acquire Shared lock on `users/123/profile` → SUCCESS
2. Client B: Acquire Exclusive lock on `users/` → BLOCKS (descendant has shared lock)
3. Client C: Acquire Shared lock on `users/` → SUCCESS (compatible with descendant shared)
```

### Example 4: Sibling Independence
```
1. Client A: Acquire Exclusive lock on `users/123` → SUCCESS
2. Client B: Acquire Exclusive lock on `users/456` → SUCCESS (different user)
3. Client C: Acquire Shared lock on `orders/789` → SUCCESS (unrelated path)
```
