package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	grackle "github.com/evrblk/evrblk-go/grackle/v1beta"
)

// LeaseHandle represents a created lease
type LeaseHandle struct {
	Namespace string
	LeaseID   string
	CreatedAt time.Time
	TTL       time.Duration
	Type      string // "lock" or "semaphore"
}

// LockHandle represents an acquired lock
type LockHandle struct {
	Namespace string
	LockName  string
	LeaseID   string
	Exclusive bool
}

// SemaphoreHandle represents an acquired semaphore
type SemaphoreHandle struct {
	Namespace     string
	SemaphoreName string
	LeaseID       string
	Weight        int64
}

// ResourcePool manages all pre-created resources and tracks acquisitions
type ResourcePool struct {
	// Pre-created resource names / state
	namespaces []string
	locks      map[string][]string          // namespace -> lock names
	semaphores map[string][]string          // namespace -> semaphore names
	waitGroups map[string][]*WaitGroupState // namespace -> wait group state
	barriers   map[string][]*BarrierState   // namespace -> barrier state

	// Flattened views for uniform random selection across namespaces.
	allWaitGroups []*WaitGroupState
	allBarriers   []*BarrierState

	// Per-worker tracking of leases and acquired resources. Blocking acquires
	// run on background goroutines, so multiple goroutines may touch the same
	// worker's entries concurrently; mu guards all four maps.
	mu                 sync.Mutex
	lockLeases         map[int][]LeaseHandle
	semaphoreLeases    map[int][]LeaseHandle
	acquiredLocks      map[int][]LockHandle
	acquiredSemaphores map[int][]SemaphoreHandle

	// Config for resource creation
	config *Config
}

// SetupResources creates all namespaces and primitives
func SetupResources(ctx context.Context, client grackle.GrackleApi, config *Config) (*ResourcePool, error) {
	log.Println("Setting up resources...")

	pool := &ResourcePool{
		namespaces:         make([]string, 0, config.Namespaces),
		locks:              make(map[string][]string),
		semaphores:         make(map[string][]string),
		waitGroups:         make(map[string][]*WaitGroupState),
		barriers:           make(map[string][]*BarrierState),
		lockLeases:         make(map[int][]LeaseHandle),
		semaphoreLeases:    make(map[int][]LeaseHandle),
		acquiredLocks:      make(map[int][]LockHandle),
		acquiredSemaphores: make(map[int][]SemaphoreHandle),
		config:             config,
	}

	// Create namespaces
	log.Printf("Creating %d namespaces...", config.Namespaces)
	for i := 0; i < config.Namespaces; i++ {
		nsName := fmt.Sprintf("load-test-ns-%d", i)
		_, err := client.CreateNamespace(ctx, &grackle.CreateNamespaceRequest{
			Name: nsName,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create namespace %s: %w", nsName, err)
		}
		pool.namespaces = append(pool.namespaces, nsName)
	}
	log.Printf("Created %d namespaces", config.Namespaces)

	// Create semaphores (need to be created before use)
	if config.SemaphoresPerNS > 0 {
		log.Printf("Creating %d semaphores per namespace...", config.SemaphoresPerNS)
		for _, ns := range pool.namespaces {
			semNames := make([]string, 0, config.SemaphoresPerNS)
			for i := 0; i < config.SemaphoresPerNS; i++ {
				semName := fmt.Sprintf("sem-%d", i)
				_, err := client.CreateSemaphore(ctx, &grackle.CreateSemaphoreRequest{
					NamespaceName: ns,
					SemaphoreName: semName,
					Permits:       int64(config.SemaphorePermits),
				})
				if err != nil {
					return nil, fmt.Errorf("failed to create semaphore %s in namespace %s: %w", semName, ns, err)
				}
				semNames = append(semNames, semName)
			}
			pool.semaphores[ns] = semNames
		}
		log.Printf("Created %d semaphores per namespace", config.SemaphoresPerNS)
	}

	// Create wait groups (need to be created before use)
	if config.WaitGroupsPerNS > 0 {
		log.Printf("Creating %d wait groups per namespace...", config.WaitGroupsPerNS)
		for _, ns := range pool.namespaces {
			states := make([]*WaitGroupState, 0, config.WaitGroupsPerNS)
			for i := 0; i < config.WaitGroupsPerNS; i++ {
				wgName := fmt.Sprintf("wg-%d", i)
				state := NewWaitGroupState(ns, wgName, int64(config.WaitGroupInitialCounter), config)
				if err := state.Create(ctx, client); err != nil {
					return nil, fmt.Errorf("failed to create wait group %s in namespace %s: %w", wgName, ns, err)
				}
				states = append(states, state)
				pool.allWaitGroups = append(pool.allWaitGroups, state)
			}
			pool.waitGroups[ns] = states
		}
		log.Printf("Created %d wait groups per namespace", config.WaitGroupsPerNS)
	}

	// Create barriers (need to be created before use)
	if config.BarriersPerNS > 0 {
		log.Printf("Creating %d barriers per namespace...", config.BarriersPerNS)
		for _, ns := range pool.namespaces {
			states := make([]*BarrierState, 0, config.BarriersPerNS)
			for i := 0; i < config.BarriersPerNS; i++ {
				barrierName := fmt.Sprintf("barrier-%d", i)
				state := NewBarrierState(ns, barrierName, int64(config.BarrierExpectedProcesses), config)
				if err := state.Create(ctx, client); err != nil {
					return nil, fmt.Errorf("failed to create barrier %s in namespace %s: %w", barrierName, ns, err)
				}
				states = append(states, state)
				pool.allBarriers = append(pool.allBarriers, state)
			}
			pool.barriers[ns] = states
		}
		log.Printf("Created %d barriers per namespace", config.BarriersPerNS)
	}

	// Pre-populate lock names (locks don't need to be created, just named)
	if config.LocksPerNS > 0 {
		for _, ns := range pool.namespaces {
			lockNames := make([]string, 0, config.LocksPerNS)
			for i := 0; i < config.LocksPerNS; i++ {
				lockNames = append(lockNames, fmt.Sprintf("lock-%d", i))
			}
			pool.locks[ns] = lockNames
		}
		log.Printf("Prepared %d lock names per namespace", config.LocksPerNS)
	}

	log.Println("Resource setup complete!")
	return pool, nil
}

// CleanupResources deletes all created resources
func CleanupResources(ctx context.Context, client grackle.GrackleApi, pool *ResourcePool) {
	log.Println("Cleaning up resources...")

	// Revoke all lock leases (releases the locks they hold in one shot)
	for _, lease := range pool.allLeases("lock") {
		_, err := client.RevokeLockLease(ctx, &grackle.RevokeLockLeaseRequest{
			NamespaceName: lease.Namespace,
			LeaseId:       lease.LeaseID,
		})
		if err != nil {
			log.Printf("Warning: failed to revoke lock lease %s/%s: %v", lease.Namespace, lease.LeaseID, err)
		}
	}

	// Revoke all semaphore leases
	for _, lease := range pool.allLeases("semaphore") {
		_, err := client.RevokeSemaphoreLease(ctx, &grackle.RevokeSemaphoreLeaseRequest{
			NamespaceName: lease.Namespace,
			LeaseId:       lease.LeaseID,
		})
		if err != nil {
			log.Printf("Warning: failed to revoke semaphore lease %s/%s: %v", lease.Namespace, lease.LeaseID, err)
		}
	}

	// Delete wait groups
	for ns, states := range pool.waitGroups {
		for _, wg := range states {
			_, err := client.DeleteWaitGroup(ctx, &grackle.DeleteWaitGroupRequest{
				NamespaceName: ns,
				WaitGroupName: wg.Name(),
			})
			if err != nil {
				log.Printf("Warning: failed to delete wait group %s/%s: %v", ns, wg.Name(), err)
			}
		}
	}

	// Delete barriers
	for ns, states := range pool.barriers {
		for _, b := range states {
			_, err := client.DeleteBarrier(ctx, &grackle.DeleteBarrierRequest{
				NamespaceName: ns,
				BarrierName:   b.Name(),
			})
			if err != nil {
				log.Printf("Warning: failed to delete barrier %s/%s: %v", ns, b.Name(), err)
			}
		}
	}

	// Delete semaphores
	for ns, semNames := range pool.semaphores {
		for _, semName := range semNames {
			_, err := client.DeleteSemaphore(ctx, &grackle.DeleteSemaphoreRequest{
				NamespaceName: ns,
				SemaphoreName: semName,
			})
			if err != nil {
				log.Printf("Warning: failed to delete semaphore %s/%s: %v", ns, semName, err)
			}
		}
	}

	// Delete locks
	for ns, lockNames := range pool.locks {
		for _, lockName := range lockNames {
			_, err := client.DeleteLock(ctx, &grackle.DeleteLockRequest{
				NamespaceName: ns,
				LockName:      lockName,
			})
			if err != nil {
				log.Printf("Warning: failed to delete lock %s/%s: %v", ns, lockName, err)
			}
		}
	}

	// Delete namespaces
	for _, ns := range pool.namespaces {
		_, err := client.DeleteNamespace(ctx, &grackle.DeleteNamespaceRequest{
			NamespaceName: ns,
		})
		if err != nil {
			log.Printf("Warning: failed to delete namespace %s: %v", ns, err)
		}
	}

	log.Println("Cleanup complete")
}

// ---------------------------------------------------------------------------
// Random selection helpers
// ---------------------------------------------------------------------------

// GetRandomWaitGroup returns a uniformly random wait group, or nil if none.
func (p *ResourcePool) GetRandomWaitGroup(rng *rand.Rand) *WaitGroupState {
	if len(p.allWaitGroups) == 0 {
		return nil
	}
	return p.allWaitGroups[rng.Intn(len(p.allWaitGroups))]
}

// GetRandomBarrier returns a uniformly random barrier, or nil if none.
func (p *ResourcePool) GetRandomBarrier(rng *rand.Rand) *BarrierState {
	if len(p.allBarriers) == 0 {
		return nil
	}
	return p.allBarriers[rng.Intn(len(p.allBarriers))]
}

// ---------------------------------------------------------------------------
// Acquired-resource tracking
// ---------------------------------------------------------------------------

// TrackAcquiredLock tracks a lock acquisition for a worker
func (p *ResourcePool) TrackAcquiredLock(workerID int, handle LockHandle) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.acquiredLocks[workerID] = append(p.acquiredLocks[workerID], handle)
}

// GetAndRemoveAcquiredLock retrieves and removes a lock handle for a worker
func (p *ResourcePool) GetAndRemoveAcquiredLock(workerID int) *LockHandle {
	p.mu.Lock()
	defer p.mu.Unlock()
	handles := p.acquiredLocks[workerID]
	if len(handles) == 0 {
		return nil
	}
	handle := handles[0]
	p.acquiredLocks[workerID] = handles[1:]
	return &handle
}

// TrackAcquiredSemaphore tracks a semaphore acquisition for a worker
func (p *ResourcePool) TrackAcquiredSemaphore(workerID int, handle SemaphoreHandle) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.acquiredSemaphores[workerID] = append(p.acquiredSemaphores[workerID], handle)
}

// GetAndRemoveAcquiredSemaphore retrieves and removes a semaphore handle for a worker
func (p *ResourcePool) GetAndRemoveAcquiredSemaphore(workerID int) *SemaphoreHandle {
	p.mu.Lock()
	defer p.mu.Unlock()
	handles := p.acquiredSemaphores[workerID]
	if len(handles) == 0 {
		return nil
	}
	handle := handles[0]
	p.acquiredSemaphores[workerID] = handles[1:]
	return &handle
}

// CountAcquiredLocks returns total acquired locks across all workers
func (p *ResourcePool) CountAcquiredLocks() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	count := 0
	for _, handles := range p.acquiredLocks {
		count += len(handles)
	}
	return count
}

// CountAcquiredSemaphores returns total acquired semaphores across all workers
func (p *ResourcePool) CountAcquiredSemaphores() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	count := 0
	for _, handles := range p.acquiredSemaphores {
		count += len(handles)
	}
	return count
}

// ---------------------------------------------------------------------------
// Lease tracking
// ---------------------------------------------------------------------------

// leaseMap returns the per-worker lease map for the given type. Caller holds mu.
func (p *ResourcePool) leaseMap(leaseType string) map[int][]LeaseHandle {
	if leaseType == "lock" {
		return p.lockLeases
	}
	return p.semaphoreLeases
}

// TrackLease tracks a lease for a worker
func (p *ResourcePool) TrackLease(workerID int, lease LeaseHandle) {
	p.mu.Lock()
	defer p.mu.Unlock()
	m := p.leaseMap(lease.Type)
	m[workerID] = append(m[workerID], lease)
}

// GetRandomLease returns a random lease for a worker of the specified type
func (p *ResourcePool) GetRandomLease(workerID int, leaseType string, rng *rand.Rand) *LeaseHandle {
	p.mu.Lock()
	defer p.mu.Unlock()
	leases := p.leaseMap(leaseType)[workerID]
	if len(leases) == 0 {
		return nil
	}
	lease := leases[rng.Intn(len(leases))]
	return &lease
}

// GetAllLeases returns a snapshot copy of all leases for a worker of the
// specified type.
func (p *ResourcePool) GetAllLeases(workerID int, leaseType string) []LeaseHandle {
	p.mu.Lock()
	defer p.mu.Unlock()
	leases := p.leaseMap(leaseType)[workerID]
	if len(leases) == 0 {
		return nil
	}
	out := make([]LeaseHandle, len(leases))
	copy(out, leases)
	return out
}

// allLeases returns a snapshot of every lease of the given type across all
// workers, used during cleanup.
func (p *ResourcePool) allLeases(leaseType string) []LeaseHandle {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []LeaseHandle
	for _, leases := range p.leaseMap(leaseType) {
		out = append(out, leases...)
	}
	return out
}
