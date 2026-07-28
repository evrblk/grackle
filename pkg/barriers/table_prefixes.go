package barriers

// Table prefixes for the BarriersCore. Each is a one-byte id for one table,
// following this core's node-local replica prefix (assigned by
// honey.ReplicaPrefixRegistry) in every key: [replica prefix][table prefix]
// [record]. They only need to be unique WITHIN this core — every BarriersCore
// instance owns a distinct replica prefix, so rows from different cores never
// collide even when they reuse the same table prefix byte.
//
// Treat these as constants; never mutate the returned slices.
var (
	tablePrefixBarriers           = []byte{0x00}
	tablePrefixBarriersNamesIndex = []byte{0x01}
	tablePrefixCounters           = []byte{0x02}
	tablePrefixGCRecords          = []byte{0x03}
	tablePrefixDeletionRecords    = []byte{0x04}
	tablePrefixParticipants       = []byte{0x05}
)
