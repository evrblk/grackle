package grackle

var (
	GrackleLocksTableId                      = []byte{0x05, 0x00}
	GrackleSemaphoresTableId                 = []byte{0x05, 0x01}
	GrackleWaitGroupsTableId                 = []byte{0x05, 0x02}
	GrackleWaitGroupJobsTableId              = []byte{0x05, 0x03}
	GrackleNamespacesTableId                 = []byte{0x05, 0x04}
	GrackleNamespacesCountersTableId         = []byte{0x05, 0x05}
	GrackleSemaphoresCountersTableId         = []byte{0x05, 0x06}
	GrackleWaitGroupsCountersTableId         = []byte{0x05, 0x07}
	GrackleLocksCountersTableId              = []byte{0x05, 0x08}
	GrackleLocksGCRecordsGlobalIndexId       = []byte{0x05, 0x09}
	GrackleSemaphoresGCRecordsGlobalIndexId  = []byte{0x05, 0x0a}
	GrackleLocksExpirationGlobalIndexId      = []byte{0x05, 0x0b}
	GrackleSemaphoresExpirationGlobalIndexId = []byte{0x05, 0x0c}
	GrackleWaitGroupsGCRecordsGlobalIndexId  = []byte{0x05, 0x0d}
	GrackleWaitGroupsExpirationGlobalIndexId = []byte{0x05, 0x0e}
)
