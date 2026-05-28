package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	grackle "github.com/evrblk/evrblk-go/grackle/preview"
)

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
	Weight        uint64
}

// ResourcePool manages all pre-created resources and tracks acquisitions
type ResourcePool struct {
	// Pre-created resource names
	namespaces []string
	locks      map[string][]string // namespace -> lock names
	semaphores map[string][]string // namespace -> semaphore names
	waitGroups map[string][]string // namespace -> waitgroup names

	// Tracking acquired resources per worker
	acquiredLocks      sync.Map // workerID -> []LockHandle
	acquiredSemaphores sync.Map // workerID -> []SemaphoreHandle

	// Config for resource creation
	config *Config
}

// SetupResources creates all namespaces and primitives
func SetupResources(ctx context.Context, client grackle.GrackleApi, config *Config) (*ResourcePool, error) {
	log.Println("Setting up resources...")

	pool := &ResourcePool{
		namespaces: make([]string, 0, config.Namespaces),
		locks:      make(map[string][]string),
		semaphores: make(map[string][]string),
		waitGroups: make(map[string][]string),
		config:     config,
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
					Permits:       uint64(config.SemaphorePermits),
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
			wgNames := make([]string, 0, config.WaitGroupsPerNS)
			for i := 0; i < config.WaitGroupsPerNS; i++ {
				wgName := fmt.Sprintf("wg-%d", i)
				expiresAt := time.Now().Add(1 * time.Hour).UnixNano()
				_, err := client.CreateWaitGroup(ctx, &grackle.CreateWaitGroupRequest{
					NamespaceName: ns,
					WaitGroupName: wgName,
					Counter:       uint64(config.WaitGroupInitialCounter),
					ExpiresAt:     expiresAt,
				})
				if err != nil {
					return nil, fmt.Errorf("failed to create wait group %s in namespace %s: %w", wgName, ns, err)
				}
				wgNames = append(wgNames, wgName)
			}
			pool.waitGroups[ns] = wgNames
		}
		log.Printf("Created %d wait groups per namespace", config.WaitGroupsPerNS)
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

	// Delete wait groups
	for ns, wgNames := range pool.waitGroups {
		for _, wgName := range wgNames {
			_, err := client.DeleteWaitGroup(ctx, &grackle.DeleteWaitGroupRequest{
				NamespaceName: ns,
				WaitGroupName: wgName,
			})
			if err != nil {
				log.Printf("Warning: failed to delete wait group %s/%s: %v", ns, wgName, err)
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

	// Delete locks (release first, then delete)
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

// TrackAcquiredLock tracks a lock acquisition for a worker
func (p *ResourcePool) TrackAcquiredLock(workerID int, handle LockHandle) {
	val, _ := p.acquiredLocks.LoadOrStore(workerID, []LockHandle{})
	handles := val.([]LockHandle)
	handles = append(handles, handle)
	p.acquiredLocks.Store(workerID, handles)
}

// GetAndRemoveAcquiredLock retrieves and removes a lock handle for a worker
func (p *ResourcePool) GetAndRemoveAcquiredLock(workerID int) *LockHandle {
	val, ok := p.acquiredLocks.Load(workerID)
	if !ok {
		return nil
	}
	handles := val.([]LockHandle)
	if len(handles) == 0 {
		return nil
	}
	// Remove the first handle
	handle := handles[0]
	remaining := handles[1:]
	if len(remaining) == 0 {
		p.acquiredLocks.Delete(workerID)
	} else {
		p.acquiredLocks.Store(workerID, remaining)
	}
	return &handle
}

// TrackAcquiredSemaphore tracks a semaphore acquisition for a worker
func (p *ResourcePool) TrackAcquiredSemaphore(workerID int, handle SemaphoreHandle) {
	val, _ := p.acquiredSemaphores.LoadOrStore(workerID, []SemaphoreHandle{})
	handles := val.([]SemaphoreHandle)
	handles = append(handles, handle)
	p.acquiredSemaphores.Store(workerID, handles)
}

// GetAndRemoveAcquiredSemaphore retrieves and removes a semaphore handle for a worker
func (p *ResourcePool) GetAndRemoveAcquiredSemaphore(workerID int) *SemaphoreHandle {
	val, ok := p.acquiredSemaphores.Load(workerID)
	if !ok {
		return nil
	}
	handles := val.([]SemaphoreHandle)
	if len(handles) == 0 {
		return nil
	}
	// Remove the first handle
	handle := handles[0]
	remaining := handles[1:]
	if len(remaining) == 0 {
		p.acquiredSemaphores.Delete(workerID)
	} else {
		p.acquiredSemaphores.Store(workerID, remaining)
	}
	return &handle
}

// CountAcquiredLocks returns total acquired locks across all workers
func (p *ResourcePool) CountAcquiredLocks() int {
	count := 0
	p.acquiredLocks.Range(func(key, value interface{}) bool {
		handles := value.([]LockHandle)
		count += len(handles)
		return true
	})
	return count
}

// CountAcquiredSemaphores returns total acquired semaphores across all workers
func (p *ResourcePool) CountAcquiredSemaphores() int {
	count := 0
	p.acquiredSemaphores.Range(func(key, value interface{}) bool {
		handles := value.([]SemaphoreHandle)
		count += len(handles)
		return true
	})
	return count
}
