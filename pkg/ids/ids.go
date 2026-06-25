package ids

import (
	"encoding/binary"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/evrblk/yellowstone-common/encoding/base62"

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

func DecodeNamespaceId(s string) (*corepb.NamespaceId, error) {
	if !namespaceIdRegex.MatchString(s) {
		return nil, ErrInvalidId
	}

	b, err := base62.DecodeString(strings.TrimPrefix(s, "ns_"))
	if err != nil {
		return nil, ErrInvalidId
	}

	if len(b) != 8+8 {
		return nil, ErrInvalidId
	}

	return &corepb.NamespaceId{
		AccountId:   binary.BigEndian.Uint64(b[0:8]),
		NamespaceId: binary.BigEndian.Uint64(b[8 : 8+8]),
	}, nil
}

func EncodeNamespaceId(id *corepb.NamespaceId) string {
	src := make([]byte, 8+8)
	binary.BigEndian.PutUint64(src[0:8], id.AccountId)
	binary.BigEndian.PutUint64(src[8:8+8], id.NamespaceId)
	return fmt.Sprintf("ns_%s", base62.Encode(src))
}

func DecodeWaitGroupId(s string) (*corepb.WaitGroupId, error) {
	if !waitGroupIdRegex.MatchString(s) {
		return nil, ErrInvalidId
	}

	b, err := base62.DecodeString(strings.TrimPrefix(s, "wg_"))
	if err != nil {
		return nil, ErrInvalidId
	}

	if len(b) != 8+8+8 {
		return nil, ErrInvalidId
	}

	return &corepb.WaitGroupId{
		AccountId:   binary.BigEndian.Uint64(b[0:8]),
		NamespaceId: binary.BigEndian.Uint64(b[8 : 8+8]),
		WaitGroupId: binary.BigEndian.Uint64(b[8+8 : 8+8+8]),
	}, nil
}

func EncodeWaitGroupId(id *corepb.WaitGroupId) string {
	src := make([]byte, 8+8+8)
	binary.BigEndian.PutUint64(src[0:8], id.AccountId)
	binary.BigEndian.PutUint64(src[8:8+8], id.NamespaceId)
	binary.BigEndian.PutUint64(src[8+8:8+8+8], id.WaitGroupId)
	return fmt.Sprintf("wg_%s", base62.Encode(src))
}

func DecodeSemaphoreId(s string) (*corepb.SemaphoreId, error) {
	if !semaphoreIdRegex.MatchString(s) {
		return nil, ErrInvalidId
	}

	b, err := base62.DecodeString(strings.TrimPrefix(s, "sem_"))
	if err != nil {
		return nil, ErrInvalidId
	}

	if len(b) != 8+8+8 {
		return nil, ErrInvalidId
	}

	return &corepb.SemaphoreId{
		AccountId:   binary.BigEndian.Uint64(b[0:8]),
		NamespaceId: binary.BigEndian.Uint64(b[8 : 8+8]),
		SemaphoreId: binary.BigEndian.Uint64(b[8+8 : 8+8+8]),
	}, nil
}

func EncodeSemaphoreId(id *corepb.SemaphoreId) string {
	src := make([]byte, 8+8+8)
	binary.BigEndian.PutUint64(src[0:8], id.AccountId)
	binary.BigEndian.PutUint64(src[8:8+8], id.NamespaceId)
	binary.BigEndian.PutUint64(src[8+8:8+8+8], id.SemaphoreId)
	return fmt.Sprintf("sem_%s", base62.Encode(src))
}

func DecodeBarrierId(s string) (*corepb.BarrierId, error) {
	if !barrierIdRegex.MatchString(s) {
		return nil, ErrInvalidId
	}

	b, err := base62.DecodeString(strings.TrimPrefix(s, "bar_"))
	if err != nil {
		return nil, ErrInvalidId
	}

	if len(b) != 8+8+8 {
		return nil, ErrInvalidId
	}

	return &corepb.BarrierId{
		AccountId:   binary.BigEndian.Uint64(b[0:8]),
		NamespaceId: binary.BigEndian.Uint64(b[8 : 8+8]),
		BarrierId:   binary.BigEndian.Uint64(b[8+8 : 8+8+8]),
	}, nil
}

func EncodeBarrierId(id *corepb.BarrierId) string {
	src := make([]byte, 8+8+8)
	binary.BigEndian.PutUint64(src[0:8], id.AccountId)
	binary.BigEndian.PutUint64(src[8:8+8], id.NamespaceId)
	binary.BigEndian.PutUint64(src[8+8:8+8+8], id.BarrierId)
	return fmt.Sprintf("bar_%s", base62.Encode(src))
}

func DecodeLeaseId(s string) (*corepb.LeaseId, error) {
	if !leaseIdRegex.MatchString(s) {
		return nil, ErrInvalidId
	}

	b, err := base62.DecodeString(strings.TrimPrefix(s, "ls_"))
	if err != nil {
		return nil, ErrInvalidId
	}

	if len(b) != 8+8+8 {
		return nil, ErrInvalidId
	}

	return &corepb.LeaseId{
		AccountId:   binary.BigEndian.Uint64(b[0:8]),
		NamespaceId: binary.BigEndian.Uint64(b[8 : 8+8]),
		LeaseId:     binary.BigEndian.Uint64(b[8+8 : 8+8+8]),
	}, nil
}

func EncodeLeaseId(id *corepb.LeaseId) string {
	src := make([]byte, 8+8+8)
	binary.BigEndian.PutUint64(src[0:8], id.AccountId)
	binary.BigEndian.PutUint64(src[8:8+8], id.NamespaceId)
	binary.BigEndian.PutUint64(src[8+8:8+8+8], id.LeaseId)
	return fmt.Sprintf("ls_%s", base62.Encode(src))
}
