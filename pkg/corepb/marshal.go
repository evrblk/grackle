package corepb

import "google.golang.org/protobuf/proto"

func (m *Semaphore) UnmarshalBinary(data []byte) error {
	return proto.Unmarshal(data, m)
}

func (m *Semaphore) MarshalBinary() (data []byte, err error) {
	return proto.Marshal(m)
}

func (m *Barrier) UnmarshalBinary(data []byte) error {
	return proto.Unmarshal(data, m)
}

func (m *Barrier) MarshalBinary() (data []byte, err error) {
	return proto.Marshal(m)
}

func (m *SemaphoreHolder) UnmarshalBinary(data []byte) error {
	return proto.Unmarshal(data, m)
}

func (m *SemaphoreHolder) MarshalBinary() (data []byte, err error) {
	return proto.Marshal(m)
}

func (m *SemaphoresGarbageCollectionRecord) UnmarshalBinary(data []byte) error {
	return proto.Unmarshal(data, m)
}

func (m *SemaphoresGarbageCollectionRecord) MarshalBinary() (data []byte, err error) {
	return proto.Marshal(m)
}

func (m *SemaphoresExpirationRecord) UnmarshalBinary(data []byte) error {
	return proto.Unmarshal(data, m)
}

func (m *SemaphoresExpirationRecord) MarshalBinary() (data []byte, err error) {
	return proto.Marshal(m)
}

func (m *SemaphoresCounter) UnmarshalBinary(data []byte) error {
	return proto.Unmarshal(data, m)
}

func (m *SemaphoresCounter) MarshalBinary() (data []byte, err error) {
	return proto.Marshal(m)
}

func (m *WaitGroupsCounter) UnmarshalBinary(data []byte) error {
	return proto.Unmarshal(data, m)
}

func (m *WaitGroupsCounter) MarshalBinary() (data []byte, err error) {
	return proto.Marshal(m)
}

func (m *WaitGroup) UnmarshalBinary(data []byte) error {
	return proto.Unmarshal(data, m)
}

func (m *WaitGroup) MarshalBinary() (data []byte, err error) {
	return proto.Marshal(m)
}

func (m *WaitGroupJob) UnmarshalBinary(data []byte) error {
	return proto.Unmarshal(data, m)
}

func (m *WaitGroupJob) MarshalBinary() (data []byte, err error) {
	return proto.Marshal(m)
}

func (m *WaitGroupsGarbageCollectionRecord) UnmarshalBinary(data []byte) error {
	return proto.Unmarshal(data, m)
}

func (m *WaitGroupsGarbageCollectionRecord) MarshalBinary() (data []byte, err error) {
	return proto.Marshal(m)
}

func (m *WaitGroupsExpirationRecord) UnmarshalBinary(data []byte) error {
	return proto.Unmarshal(data, m)
}

func (m *WaitGroupsExpirationRecord) MarshalBinary() (data []byte, err error) {
	return proto.Marshal(m)
}

func (m *Namespace) UnmarshalBinary(data []byte) error {
	return proto.Unmarshal(data, m)
}

func (m *Namespace) MarshalBinary() (data []byte, err error) {
	return proto.Marshal(m)
}

func (m *NamespacesCounter) UnmarshalBinary(data []byte) error {
	return proto.Unmarshal(data, m)
}

func (m *NamespacesCounter) MarshalBinary() (data []byte, err error) {
	return proto.Marshal(m)
}

func (m *Lock) UnmarshalBinary(data []byte) error {
	return proto.Unmarshal(data, m)
}

func (m *Lock) MarshalBinary() (data []byte, err error) {
	return proto.Marshal(m)
}

func (m *LocksGarbageCollectionRecord) UnmarshalBinary(data []byte) error {
	return proto.Unmarshal(data, m)
}

func (m *LocksGarbageCollectionRecord) MarshalBinary() (data []byte, err error) {
	return proto.Marshal(m)
}

func (m *LocksExpirationRecord) UnmarshalBinary(data []byte) error {
	return proto.Unmarshal(data, m)
}

func (m *LocksExpirationRecord) MarshalBinary() (data []byte, err error) {
	return proto.Marshal(m)
}

func (m *LocksCounter) UnmarshalBinary(data []byte) error {
	return proto.Unmarshal(data, m)
}

func (m *LocksCounter) MarshalBinary() (data []byte, err error) {
	return proto.Marshal(m)
}

func (m *LockAncestor) UnmarshalBinary(data []byte) error {
	return proto.Unmarshal(data, m)
}

func (m *LockAncestor) MarshalBinary() (data []byte, err error) {
	return proto.Marshal(m)
}

func (m *BarrierParticipant) UnmarshalBinary(data []byte) error {
	return proto.Unmarshal(data, m)
}

func (m *BarrierParticipant) MarshalBinary() (data []byte, err error) {
	return proto.Marshal(m)
}

func (m *BarriersCounter) UnmarshalBinary(data []byte) error {
	return proto.Unmarshal(data, m)
}

func (m *BarriersCounter) MarshalBinary() (data []byte, err error) {
	return proto.Marshal(m)
}

func (m *BarriersGarbageCollectionRecord) UnmarshalBinary(data []byte) error {
	return proto.Unmarshal(data, m)
}

func (m *BarriersGarbageCollectionRecord) MarshalBinary() (data []byte, err error) {
	return proto.Marshal(m)
}

func (m *BarriersExpirationRecord) UnmarshalBinary(data []byte) error {
	return proto.Unmarshal(data, m)
}

func (m *BarriersExpirationRecord) MarshalBinary() (data []byte, err error) {
	return proto.Marshal(m)
}
