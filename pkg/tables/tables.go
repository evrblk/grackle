package tables

var (
	GrackleLocksTableId                         = []byte{0x05, 0x01}
	GrackleLocksCountersTableId                 = []byte{0x05, 0x02}
	GrackleLocksGarbageCollectionRecordsTableId = []byte{0x05, 0x03}
	GrackleLocksExpirationRecordsTableId        = []byte{0x05, 0x04}
	GrackleLocksAncestorsTableId                = []byte{0x05, 0x1a}

	GrackleSemaphoresTableId                         = []byte{0x05, 0x05}
	GrackleSemaphoresNamesIndexId                    = []byte{0x05, 0x06}
	GrackleSemaphoresCountersTableId                 = []byte{0x05, 0x07}
	GrackleSemaphoresGarbageCollectionRecordsTableId = []byte{0x05, 0x08}
	GrackleSemaphoresExpirationrecordsTableId        = []byte{0x05, 0x09}
	GrackleSemaphoreHoldersTableId                   = []byte{0x05, 0x18}
	GrackleSemaphoreHoldersExpirationIndexId         = []byte{0x05, 0x19}

	GrackleWaitGroupsTableId                         = []byte{0x05, 0x0a}
	GrackleWaitGroupsJobsTableId                     = []byte{0x05, 0x0b}
	GrackleWaitGroupsCountersTableId                 = []byte{0x05, 0x0c}
	GrackleWaitGroupsGarbageCollectionRecordsTableId = []byte{0x05, 0x0d}
	GrackleWaitGroupsExpirationRecordsTableId        = []byte{0x05, 0x0e}
	GrackleWaitGroupsNamesIndexId                    = []byte{0x05, 0x0f}

	GrackleNamespacesTableId         = []byte{0x05, 0x10}
	GrackleNamespacesNamesIndexId    = []byte{0x05, 0x11}
	GrackleNamespacesCountersTableId = []byte{0x05, 0x12}

	GrackleBarriersTableId                         = []byte{0x05, 0x13}
	GrackleBarriersNamesIndexId                    = []byte{0x05, 0x14}
	GrackleBarriersCountersTableId                 = []byte{0x05, 0x15}
	GrackleBarriersGarbageCollectionRecordsTableId = []byte{0x05, 0x16}
	GrackleBarriersExpirationRecordsTableId        = []byte{0x05, 0x17}
	GrackleBarrierParticipantsTableId              = []byte{0x05, 0x18}
)
