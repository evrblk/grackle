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
