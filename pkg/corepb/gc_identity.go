package corepb

// Identity methods for garbage collection records: the (account id, namespace
// id) of the entity a record targets, regardless of which oneof variant it
// carries. Portable snapshot restores filter records by it (see
// tables.GCRecordsTable). ok is false when the record carries no target — an
// invalid record.

func (r *LocksGarbageCollectionRecord) Identity() (accountId uint64, namespaceId uint64, ok bool) {
	if r.NamespaceId == nil {
		return 0, 0, false
	}
	return r.NamespaceId.AccountId, r.NamespaceId.NamespaceId, true
}

func (r *SemaphoresGarbageCollectionRecord) Identity() (accountId uint64, namespaceId uint64, ok bool) {
	switch rec := r.Record.(type) {
	case *SemaphoresGarbageCollectionRecord_NamespaceId:
		return rec.NamespaceId.AccountId, rec.NamespaceId.NamespaceId, true
	case *SemaphoresGarbageCollectionRecord_SemaphoreId:
		return rec.SemaphoreId.AccountId, rec.SemaphoreId.NamespaceId, true
	}
	return 0, 0, false
}

func (r *WaitGroupsGarbageCollectionRecord) Identity() (accountId uint64, namespaceId uint64, ok bool) {
	switch rec := r.Record.(type) {
	case *WaitGroupsGarbageCollectionRecord_NamespaceId:
		return rec.NamespaceId.AccountId, rec.NamespaceId.NamespaceId, true
	case *WaitGroupsGarbageCollectionRecord_WaitGroupId:
		return rec.WaitGroupId.AccountId, rec.WaitGroupId.NamespaceId, true
	}
	return 0, 0, false
}

func (r *BarriersGarbageCollectionRecord) Identity() (accountId uint64, namespaceId uint64, ok bool) {
	switch rec := r.Record.(type) {
	case *BarriersGarbageCollectionRecord_NamespaceId:
		return rec.NamespaceId.AccountId, rec.NamespaceId.NamespaceId, true
	case *BarriersGarbageCollectionRecord_BarrierId:
		return rec.BarrierId.AccountId, rec.BarrierId.NamespaceId, true
	}
	return 0, 0, false
}
