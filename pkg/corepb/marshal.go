package corepb

import (
	"encoding"
)

// Semaphore

var _ encoding.BinaryMarshaler = (*Semaphore)(nil)
var _ encoding.BinaryUnmarshaler = (*Semaphore)(nil)

func (m *Semaphore) UnmarshalBinary(data []byte) error {
	return m.UnmarshalVT(data)
}

func (m *Semaphore) MarshalBinary() (data []byte, err error) {
	return m.MarshalVT()
}

// Barrier

var _ encoding.BinaryMarshaler = (*Barrier)(nil)
var _ encoding.BinaryUnmarshaler = (*Barrier)(nil)

func (m *Barrier) UnmarshalBinary(data []byte) error {
	return m.UnmarshalVT(data)
}

func (m *Barrier) MarshalBinary() (data []byte, err error) {
	return m.MarshalVT()
}

// SemaphoreHolder

var _ encoding.BinaryMarshaler = (*SemaphoreHolder)(nil)
var _ encoding.BinaryUnmarshaler = (*SemaphoreHolder)(nil)

func (m *SemaphoreHolder) UnmarshalBinary(data []byte) error {
	return m.UnmarshalVT(data)
}

func (m *SemaphoreHolder) MarshalBinary() (data []byte, err error) {
	return m.MarshalVT()
}

// SemaphoresGarbageCollectionRecord

var _ encoding.BinaryMarshaler = (*SemaphoresGarbageCollectionRecord)(nil)
var _ encoding.BinaryUnmarshaler = (*SemaphoresGarbageCollectionRecord)(nil)

func (m *SemaphoresGarbageCollectionRecord) UnmarshalBinary(data []byte) error {
	return m.UnmarshalVT(data)
}

func (m *SemaphoresGarbageCollectionRecord) MarshalBinary() (data []byte, err error) {
	return m.MarshalVT()
}

// SemaphoresExpirationRecord

var _ encoding.BinaryMarshaler = (*SemaphoresExpirationRecord)(nil)
var _ encoding.BinaryUnmarshaler = (*SemaphoresExpirationRecord)(nil)

func (m *SemaphoresExpirationRecord) UnmarshalBinary(data []byte) error {
	return m.UnmarshalVT(data)
}

func (m *SemaphoresExpirationRecord) MarshalBinary() (data []byte, err error) {
	return m.MarshalVT()
}

// SemaphoresCounter

var _ encoding.BinaryMarshaler = (*SemaphoresCounter)(nil)
var _ encoding.BinaryUnmarshaler = (*SemaphoresCounter)(nil)

func (m *SemaphoresCounter) UnmarshalBinary(data []byte) error {
	return m.UnmarshalVT(data)
}

func (m *SemaphoresCounter) MarshalBinary() (data []byte, err error) {
	return m.MarshalVT()
}

// WaitGroupsCounter

var _ encoding.BinaryMarshaler = (*WaitGroupsCounter)(nil)
var _ encoding.BinaryUnmarshaler = (*WaitGroupsCounter)(nil)

func (m *WaitGroupsCounter) UnmarshalBinary(data []byte) error {
	return m.UnmarshalVT(data)
}

func (m *WaitGroupsCounter) MarshalBinary() (data []byte, err error) {
	return m.MarshalVT()
}

// WaitGroup

var _ encoding.BinaryMarshaler = (*WaitGroup)(nil)
var _ encoding.BinaryUnmarshaler = (*WaitGroup)(nil)

func (m *WaitGroup) UnmarshalBinary(data []byte) error {
	return m.UnmarshalVT(data)
}

func (m *WaitGroup) MarshalBinary() (data []byte, err error) {
	return m.MarshalVT()
}

// WaitGroupJob

var _ encoding.BinaryMarshaler = (*WaitGroupJob)(nil)
var _ encoding.BinaryUnmarshaler = (*WaitGroupJob)(nil)

func (m *WaitGroupJob) UnmarshalBinary(data []byte) error {
	return m.UnmarshalVT(data)
}

func (m *WaitGroupJob) MarshalBinary() (data []byte, err error) {
	return m.MarshalVT()
}

// WaitGroupsGarbageCollectionRecord

var _ encoding.BinaryMarshaler = (*WaitGroupsGarbageCollectionRecord)(nil)
var _ encoding.BinaryUnmarshaler = (*WaitGroupsGarbageCollectionRecord)(nil)

func (m *WaitGroupsGarbageCollectionRecord) UnmarshalBinary(data []byte) error {
	return m.UnmarshalVT(data)
}

func (m *WaitGroupsGarbageCollectionRecord) MarshalBinary() (data []byte, err error) {
	return m.MarshalVT()
}

// WaitGroupsExpirationRecord

var _ encoding.BinaryMarshaler = (*WaitGroupsExpirationRecord)(nil)
var _ encoding.BinaryUnmarshaler = (*WaitGroupsExpirationRecord)(nil)

func (m *WaitGroupsExpirationRecord) UnmarshalBinary(data []byte) error {
	return m.UnmarshalVT(data)
}

func (m *WaitGroupsExpirationRecord) MarshalBinary() (data []byte, err error) {
	return m.MarshalVT()
}

// Namespace

var _ encoding.BinaryMarshaler = (*Namespace)(nil)
var _ encoding.BinaryUnmarshaler = (*Namespace)(nil)

func (m *Namespace) UnmarshalBinary(data []byte) error {
	return m.UnmarshalVT(data)
}

func (m *Namespace) MarshalBinary() (data []byte, err error) {
	return m.MarshalVT()
}

// NamespacesCounter

var _ encoding.BinaryMarshaler = (*NamespacesCounter)(nil)
var _ encoding.BinaryUnmarshaler = (*NamespacesCounter)(nil)

func (m *NamespacesCounter) UnmarshalBinary(data []byte) error {
	return m.UnmarshalVT(data)
}

func (m *NamespacesCounter) MarshalBinary() (data []byte, err error) {
	return m.MarshalVT()
}

// Lock

var _ encoding.BinaryMarshaler = (*Lock)(nil)
var _ encoding.BinaryUnmarshaler = (*Lock)(nil)

func (m *Lock) UnmarshalBinary(data []byte) error {
	return m.UnmarshalVT(data)
}

func (m *Lock) MarshalBinary() (data []byte, err error) {
	return m.MarshalVT()
}

// LocksGarbageCollectionRecord

var _ encoding.BinaryMarshaler = (*LocksGarbageCollectionRecord)(nil)
var _ encoding.BinaryUnmarshaler = (*LocksGarbageCollectionRecord)(nil)

func (m *LocksGarbageCollectionRecord) UnmarshalBinary(data []byte) error {
	return m.UnmarshalVT(data)
}

func (m *LocksGarbageCollectionRecord) MarshalBinary() (data []byte, err error) {
	return m.MarshalVT()
}

// LocksCounter

var _ encoding.BinaryMarshaler = (*LocksCounter)(nil)
var _ encoding.BinaryUnmarshaler = (*LocksCounter)(nil)

func (m *LocksCounter) UnmarshalBinary(data []byte) error {
	return m.UnmarshalVT(data)
}

func (m *LocksCounter) MarshalBinary() (data []byte, err error) {
	return m.MarshalVT()
}

// LockAncestor

var _ encoding.BinaryMarshaler = (*LockAncestor)(nil)
var _ encoding.BinaryUnmarshaler = (*LockAncestor)(nil)

func (m *LockAncestor) UnmarshalBinary(data []byte) error {
	return m.UnmarshalVT(data)
}

func (m *LockAncestor) MarshalBinary() (data []byte, err error) {
	return m.MarshalVT()
}

// BarrierParticipant

var _ encoding.BinaryMarshaler = (*BarrierParticipant)(nil)
var _ encoding.BinaryUnmarshaler = (*BarrierParticipant)(nil)

func (m *BarrierParticipant) UnmarshalBinary(data []byte) error {
	return m.UnmarshalVT(data)
}

func (m *BarrierParticipant) MarshalBinary() (data []byte, err error) {
	return m.MarshalVT()
}

// BarriersCounter

var _ encoding.BinaryMarshaler = (*BarriersCounter)(nil)
var _ encoding.BinaryUnmarshaler = (*BarriersCounter)(nil)

func (m *BarriersCounter) UnmarshalBinary(data []byte) error {
	return m.UnmarshalVT(data)
}

func (m *BarriersCounter) MarshalBinary() (data []byte, err error) {
	return m.MarshalVT()
}

// BarriersGarbageCollectionRecord

var _ encoding.BinaryMarshaler = (*BarriersGarbageCollectionRecord)(nil)
var _ encoding.BinaryUnmarshaler = (*BarriersGarbageCollectionRecord)(nil)

func (m *BarriersGarbageCollectionRecord) UnmarshalBinary(data []byte) error {
	return m.UnmarshalVT(data)
}

func (m *BarriersGarbageCollectionRecord) MarshalBinary() (data []byte, err error) {
	return m.MarshalVT()
}

// BarriersExpirationRecord

var _ encoding.BinaryMarshaler = (*BarriersExpirationRecord)(nil)
var _ encoding.BinaryUnmarshaler = (*BarriersExpirationRecord)(nil)

func (m *BarriersExpirationRecord) UnmarshalBinary(data []byte) error {
	return m.UnmarshalVT(data)
}

func (m *BarriersExpirationRecord) MarshalBinary() (data []byte, err error) {
	return m.MarshalVT()
}

// Lease

var _ encoding.BinaryMarshaler = (*Lease)(nil)
var _ encoding.BinaryUnmarshaler = (*Lease)(nil)

func (m *Lease) UnmarshalBinary(data []byte) error {
	return m.UnmarshalVT(data)
}

func (m *Lease) MarshalBinary() (data []byte, err error) {
	return m.MarshalVT()
}
