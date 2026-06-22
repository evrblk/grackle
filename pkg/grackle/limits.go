package grackle

type ServiceLimits struct {
	MaxNumberOfNamespaces             int64
	MaxNumberOfWaitGroupsPerNamespace int64
	MaxNumberOfLocksPerNamespace      int64
	MaxNumberOfSemaphoresPerNamespace int64
	MaxNumberOfBarriersPerNamespace   int64
	MaxNumberOfSharedLockHolders      int64
	MaxNumberOfLockLeases             int64
	MaxNumberOfSemaphoreHolders       int64
	MaxNumberOfSemaphoreLeases        int64
	MaxWaitGroupSize                  int64
	MaxNumberOfBarrierParticipants    int64
	ControlPlaneReadRequestRate       int64
	ControlPlaneUpdateRequestRate     int64
	DataPlaneRequestRate              int64
}

var (
	DefaultServiceLimits = ServiceLimits{
		MaxNumberOfNamespaces:             100_000,
		MaxNumberOfWaitGroupsPerNamespace: 1_000_000,
		MaxNumberOfLocksPerNamespace:      1_000_000,
		MaxNumberOfSemaphoresPerNamespace: 1_000_000,
		MaxNumberOfBarriersPerNamespace:   1_000_000,
		MaxNumberOfSharedLockHolders:      1_000,
		MaxNumberOfSemaphoreHolders:       1_000,
		MaxNumberOfLockLeases:             1_000_000,
		MaxNumberOfSemaphoreLeases:        1_000_000,
		MaxWaitGroupSize:                  100_000_000,
		MaxNumberOfBarrierParticipants:    1_000_000,
	}
)
