package ids

import (
	"errors"
	"regexp"

	"github.com/evrblk/grackle/pkg/corepb"
)

var (
	ErrInvalidId = errors.New("invalid id")

	namespaceIdRegex = regexp.MustCompile("^ns_[0-9a-zA-Z]+$")
	waitGroupIdRegex = regexp.MustCompile("^wg_[0-9a-zA-Z]+$")
	semaphoreIdRegex = regexp.MustCompile("^sem_[0-9a-zA-Z]+$")
	barrierIdRegex   = regexp.MustCompile("^bar_[0-9a-zA-Z]+$")
	leaseIdRegex     = regexp.MustCompile("^ls_[0-9a-zA-Z]+$")
)

type Encoder interface {
	DecodeNamespaceId(s string) (*corepb.NamespaceId, error)
	EncodeNamespaceId(id *corepb.NamespaceId) string
	DecodeWaitGroupId(s string) (*corepb.WaitGroupId, error)
	EncodeWaitGroupId(id *corepb.WaitGroupId) string
	DecodeSemaphoreId(s string) (*corepb.SemaphoreId, error)
	EncodeSemaphoreId(id *corepb.SemaphoreId) string
	DecodeBarrierId(s string) (*corepb.BarrierId, error)
	EncodeBarrierId(id *corepb.BarrierId) string
	DecodeLeaseId(s string) (*corepb.LeaseId, error)
	EncodeLeaseId(id *corepb.LeaseId) string
}

var DefaultEncoder Encoder = &SingleTenantIDsEncoder{}

func DecodeNamespaceId(s string) (*corepb.NamespaceId, error) {
	return DefaultEncoder.DecodeNamespaceId(s)
}

func EncodeNamespaceId(id *corepb.NamespaceId) string {
	return DefaultEncoder.EncodeNamespaceId(id)
}

func DecodeWaitGroupId(s string) (*corepb.WaitGroupId, error) {
	return DefaultEncoder.DecodeWaitGroupId(s)
}

func EncodeWaitGroupId(id *corepb.WaitGroupId) string {
	return DefaultEncoder.EncodeWaitGroupId(id)
}

func DecodeSemaphoreId(s string) (*corepb.SemaphoreId, error) {
	return DefaultEncoder.DecodeSemaphoreId(s)
}

func EncodeSemaphoreId(id *corepb.SemaphoreId) string {
	return DefaultEncoder.EncodeSemaphoreId(id)
}

func DecodeBarrierId(s string) (*corepb.BarrierId, error) {
	return DefaultEncoder.DecodeBarrierId(s)
}

func EncodeBarrierId(id *corepb.BarrierId) string {
	return DefaultEncoder.EncodeBarrierId(id)
}

func DecodeLeaseId(s string) (*corepb.LeaseId, error) {
	return DefaultEncoder.DecodeLeaseId(s)
}

func EncodeLeaseId(id *corepb.LeaseId) string {
	return DefaultEncoder.EncodeLeaseId(id)
}
