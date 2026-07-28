package locks

// Table prefixes for the LocksCore. Each is a one-byte id for one table,
// following this core's node-local replica prefix (assigned by
// honey.ReplicaPrefixRegistry) in every key: [replica prefix][table prefix]
// [record]. They only need to be unique WITHIN this core — every LocksCore
// instance owns a distinct replica prefix, so rows from different cores never
// collide even when they reuse the same table prefix byte.
//
// Treat these as constants; never mutate the returned slices.
//
// 0x05 is reserved for a future locks ExpirationRecords table and is not yet
// used.
var (
	tablePrefixLocks                 = []byte{0x01}
	tablePrefixLocksLeaseIdIndex     = []byte{0x02}
	tablePrefixCounters              = []byte{0x03}
	tablePrefixGCRecords             = []byte{0x04}
	tablePrefixAncestors             = []byte{0x05}
	tablePrefixLeases                = []byte{0x06}
	tablePrefixLeasesProcessIdIndex  = []byte{0x07}
	tablePrefixLeasesExpirationIndex = []byte{0x08}
)
