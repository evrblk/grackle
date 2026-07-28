package semaphores

// Table prefixes for the SemaphoresCore. Each is a one-byte id for one table,
// following this core's node-local replica prefix (assigned by
// honey.ReplicaPrefixRegistry) in every key: [replica prefix][table prefix]
// [record]. They only need to be unique WITHIN this core — every SemaphoresCore
// instance owns a distinct replica prefix, so rows from different cores never
// collide even when they reuse the same table prefix byte.
//
// Treat these as constants; never mutate the returned slices.
var (
	tablePrefixSemaphores             = []byte{0x00}
	tablePrefixSemaphoresNamesIndex   = []byte{0x01}
	tablePrefixCounters               = []byte{0x03}
	tablePrefixGCRecords              = []byte{0x04}
	tablePrefixExpirationRecords      = []byte{0x05}
	tablePrefixHolders                = []byte{0x06}
	tablePrefixHoldersExpirationIndex = []byte{0x07}
	tablePrefixHoldersLeaseIdIndex    = []byte{0x08}
	tablePrefixLeases                 = []byte{0x09}
	tablePrefixLeasesProcessIdIndex   = []byte{0x0a}
	tablePrefixLeasesExpirationIndex  = []byte{0x0b}
)
