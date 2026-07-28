package namespaces

// Table prefixes for the NamespacesCore. Each is a one-byte id for one table,
// following this core's node-local replica prefix (assigned by
// honey.ReplicaPrefixRegistry) in every key: [replica prefix][table prefix]
// [record]. They only need to be unique WITHIN this core — every NamespacesCore
// instance owns a distinct replica prefix, so rows from different cores never
// collide even when they reuse the same table prefix byte.
//
// Treat these as constants; never mutate the returned slices.
var (
	tablePrefixNamespaces           = []byte{0x00}
	tablePrefixNamespacesNamesIndex = []byte{0x01}
	tablePrefixCounters             = []byte{0x02}
)
