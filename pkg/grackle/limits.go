package grackle

type GrackleServiceLimits struct {
	MaxNumberOfNamespaces             int64
	MaxNumberOfWaitGroupsPerNamespace int64
	MaxNumberOfLocksPerNamespace      int64
	MaxNumberOfSemaphoresPerNamespace int64
	MaxNumberOfReadLockHolders        int64
	MaxNumberOfSemaphoreHolders       int64
	MaxWaitGroupSize                  int64
	ControlPlaneReadRequestRate       int64
	ControlPlaneUpdateRequestRate     int64
	DataPlaneRequestRate              int64
}
