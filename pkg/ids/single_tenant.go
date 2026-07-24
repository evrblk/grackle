package ids

import (
	"encoding/binary"
	"fmt"
	"strings"

	"github.com/evrblk/yellowstone-common/encoding/base62"

	"github.com/evrblk/grackle/pkg/corepb"
)

type SingleTenantIDsEncoder struct {
}

var _ Encoder = (*SingleTenantIDsEncoder)(nil)

func (SingleTenantIDsEncoder) DecodeNamespaceId(s string) (*corepb.NamespaceId, error) {
	if !namespaceIdRegex.MatchString(s) {
		return nil, ErrInvalidId
	}

	b, err := base62.DecodeString(strings.TrimPrefix(s, "ns_"))
	if err != nil {
		return nil, ErrInvalidId
	}

	if len(b) != 8 {
		return nil, ErrInvalidId
	}

	return &corepb.NamespaceId{
		AccountId:   0,
		NamespaceId: binary.BigEndian.Uint64(b[0:8]),
	}, nil
}

func (SingleTenantIDsEncoder) EncodeNamespaceId(id *corepb.NamespaceId) string {
	src := make([]byte, 8)
	binary.BigEndian.PutUint64(src[0:8], id.NamespaceId)
	return fmt.Sprintf("ns_%s", base62.Encode(src))
}

func (SingleTenantIDsEncoder) DecodeWaitGroupId(s string) (*corepb.WaitGroupId, error) {
	if !waitGroupIdRegex.MatchString(s) {
		return nil, ErrInvalidId
	}

	b, err := base62.DecodeString(strings.TrimPrefix(s, "wg_"))
	if err != nil {
		return nil, ErrInvalidId
	}

	if len(b) != 8+8 {
		return nil, ErrInvalidId
	}

	return &corepb.WaitGroupId{
		AccountId:   0,
		NamespaceId: binary.BigEndian.Uint64(b[0:8]),
		WaitGroupId: binary.BigEndian.Uint64(b[8 : 8+8]),
	}, nil
}

func (SingleTenantIDsEncoder) EncodeWaitGroupId(id *corepb.WaitGroupId) string {
	src := make([]byte, 8+8)
	binary.BigEndian.PutUint64(src[0:8], id.NamespaceId)
	binary.BigEndian.PutUint64(src[8:8+8], id.WaitGroupId)
	return fmt.Sprintf("wg_%s", base62.Encode(src))
}

func (SingleTenantIDsEncoder) DecodeSemaphoreId(s string) (*corepb.SemaphoreId, error) {
	if !semaphoreIdRegex.MatchString(s) {
		return nil, ErrInvalidId
	}

	b, err := base62.DecodeString(strings.TrimPrefix(s, "sem_"))
	if err != nil {
		return nil, ErrInvalidId
	}

	if len(b) != 8+8 {
		return nil, ErrInvalidId
	}

	return &corepb.SemaphoreId{
		NamespaceId: binary.BigEndian.Uint64(b[0:8]),
		SemaphoreId: binary.BigEndian.Uint64(b[8 : 8+8]),
	}, nil
}

func (SingleTenantIDsEncoder) EncodeSemaphoreId(id *corepb.SemaphoreId) string {
	src := make([]byte, 8+8)
	binary.BigEndian.PutUint64(src[0:8], id.NamespaceId)
	binary.BigEndian.PutUint64(src[8:8+8], id.SemaphoreId)
	return fmt.Sprintf("sem_%s", base62.Encode(src))
}

func (SingleTenantIDsEncoder) DecodeBarrierId(s string) (*corepb.BarrierId, error) {
	if !barrierIdRegex.MatchString(s) {
		return nil, ErrInvalidId
	}

	b, err := base62.DecodeString(strings.TrimPrefix(s, "bar_"))
	if err != nil {
		return nil, ErrInvalidId
	}

	if len(b) != 8+8 {
		return nil, ErrInvalidId
	}

	return &corepb.BarrierId{
		AccountId:   0,
		NamespaceId: binary.BigEndian.Uint64(b[0:8]),
		BarrierId:   binary.BigEndian.Uint64(b[8 : 8+8]),
	}, nil
}

func (SingleTenantIDsEncoder) EncodeBarrierId(id *corepb.BarrierId) string {
	src := make([]byte, 8+8)
	binary.BigEndian.PutUint64(src[0:8], id.NamespaceId)
	binary.BigEndian.PutUint64(src[8:8+8], id.BarrierId)
	return fmt.Sprintf("bar_%s", base62.Encode(src))
}

func (SingleTenantIDsEncoder) DecodeLeaseId(s string) (*corepb.LeaseId, error) {
	if !leaseIdRegex.MatchString(s) {
		return nil, ErrInvalidId
	}

	b, err := base62.DecodeString(strings.TrimPrefix(s, "ls_"))
	if err != nil {
		return nil, ErrInvalidId
	}

	if len(b) != 8+8 {
		return nil, ErrInvalidId
	}

	return &corepb.LeaseId{
		AccountId:   0,
		NamespaceId: binary.BigEndian.Uint64(b[0:8]),
		LeaseId:     binary.BigEndian.Uint64(b[8 : 8+8]),
	}, nil
}

func (SingleTenantIDsEncoder) EncodeLeaseId(id *corepb.LeaseId) string {
	src := make([]byte, 8+8)
	binary.BigEndian.PutUint64(src[0:8], id.NamespaceId)
	binary.BigEndian.PutUint64(src[8:8+8], id.LeaseId)
	return fmt.Sprintf("ls_%s", base62.Encode(src))
}
