package tables

import monsterax "github.com/evrblk/monstera/x"

// Grackle is the global map containing all Grackle table prefixes.
var Grackle map[string]monsterax.TablePrefix

func RegisterGracklePrefixes(registry monsterax.TableRegistry) {
	Grackle = map[string]monsterax.TablePrefix{
		// Locks
		"Grackle.LocksCore.Locks.Table":                    registry.RegisterPrefix([]byte{0x01}),
		"Grackle.LocksCore.Locks.LeaseIdIndex":             registry.RegisterPrefix([]byte{0x02}),
		"Grackle.LocksCore.Counters.Table":                 registry.RegisterPrefix([]byte{0x03}),
		"Grackle.LocksCore.GarbageCollectionRecords.Table": registry.RegisterPrefix([]byte{0x04}),
		"Grackle.LocksCore.ExpirationRecords.Table":        registry.RegisterPrefix([]byte{0x05}),
		"Grackle.LocksCore.Ancestors.Table":                registry.RegisterPrefix([]byte{0x06}),
		"Grackle.LocksCore.Leases.Table":                   registry.RegisterPrefix([]byte{0x07}),
		"Grackle.LocksCore.Leases.ProcessIdIndex":          registry.RegisterPrefix([]byte{0x08}),
		"Grackle.LocksCore.Leases.ExpirationIndex":         registry.RegisterPrefix([]byte{0x09}),

		// Semaphores
		"Grackle.SemaphoresCore.Semaphores.Table":               registry.RegisterPrefix([]byte{0x20}),
		"Grackle.SemaphoresCore.Semaphores.NamesIndex":          registry.RegisterPrefix([]byte{0x21}),
		"Grackle.SemaphoresCore.Semaphores.LeaseIdIndex":        registry.RegisterPrefix([]byte{0x22}),
		"Grackle.SemaphoresCore.Counters.Table":                 registry.RegisterPrefix([]byte{0x23}),
		"Grackle.SemaphoresCore.GarbageCollectionRecords.Table": registry.RegisterPrefix([]byte{0x24}),
		"Grackle.SemaphoresCore.ExpirationRecords.Table":        registry.RegisterPrefix([]byte{0x25}),
		"Grackle.SemaphoresCore.Holders.Table":                  registry.RegisterPrefix([]byte{0x26}),
		"Grackle.SemaphoresCore.Holders.ExpirationIndex":        registry.RegisterPrefix([]byte{0x27}),
		"Grackle.SemaphoresCore.Leases.Table":                   registry.RegisterPrefix([]byte{0x28}),
		"Grackle.SemaphoresCore.Leases.ProcessIdIndex":          registry.RegisterPrefix([]byte{0x29}),
		"Grackle.SemaphoresCore.Leases.ExpirationIndex":         registry.RegisterPrefix([]byte{0x2a}),

		// WaitGroups
		"Grackle.WaitGroupsCore.WaitGroups.Table":               registry.RegisterPrefix([]byte{0x40}),
		"Grackle.WaitGroupsCore.WaitGroups.NamesIndex":          registry.RegisterPrefix([]byte{0x41}),
		"Grackle.WaitGroupsCore.Jobs.Table":                     registry.RegisterPrefix([]byte{0x42}),
		"Grackle.WaitGroupsCore.Counters.Table":                 registry.RegisterPrefix([]byte{0x43}),
		"Grackle.WaitGroupsCore.GarbageCollectionRecords.Table": registry.RegisterPrefix([]byte{0x44}),
		"Grackle.WaitGroupsCore.ExpirationRecords.Table":        registry.RegisterPrefix([]byte{0x45}),

		// Namespaces
		"Grackle.NamespacesCore.Namespaces.Table":      registry.RegisterPrefix([]byte{0x50}),
		"Grackle.NamespacesCore.Namespaces.NamesIndex": registry.RegisterPrefix([]byte{0x51}),
		"Grackle.NamespacesCore.Counters.Table":        registry.RegisterPrefix([]byte{0x52}),

		// Barriers
		"Grackle.BarriersCore.Barriers.Table":                 registry.RegisterPrefix([]byte{0x60}),
		"Grackle.BarriersCore.Barriers.NamesIndex":            registry.RegisterPrefix([]byte{0x61}),
		"Grackle.BarriersCore.Counters.Table":                 registry.RegisterPrefix([]byte{0x62}),
		"Grackle.BarriersCore.GarbageCollectionRecords.Table": registry.RegisterPrefix([]byte{0x63}),
		"Grackle.BarriersCore.ExpirationRecords.Table":        registry.RegisterPrefix([]byte{0x64}),
		"Grackle.BarriersCore.Participants.Table":             registry.RegisterPrefix([]byte{0x65}),
	}
}
