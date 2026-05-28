package monsteragen

import (
	"github.com/evrblk/grackle/pkg/corepb"
)

// GrackleReadRequestProtoCodec is a GrackleReadRequestCodec codec that uses VT protobuf.
type GrackleReadRequestProtoCodec struct {
}

var _ GrackleReadRequestCodec = (*GrackleReadRequestProtoCodec)(nil)

func (c *GrackleReadRequestProtoCodec) Encode(p *corepb.GrackleReadRequest) ([]byte, error) {
	return p.MarshalVT()
}

func (c *GrackleReadRequestProtoCodec) Decode(data []byte, out *corepb.GrackleReadRequest) error {
	return out.UnmarshalVT(data)
}

// GrackleReadResponseProtoCodec is a GrackleReadResponseCodec codec that uses VT protobuf.
type GrackleReadResponseProtoCodec struct {
}

var _ GrackleReadResponseCodec = (*GrackleReadResponseProtoCodec)(nil)

func (c *GrackleReadResponseProtoCodec) Encode(p *corepb.GrackleReadResponse) ([]byte, error) {
	return p.MarshalVT()
}

func (c *GrackleReadResponseProtoCodec) Decode(data []byte, out *corepb.GrackleReadResponse) error {
	return out.UnmarshalVT(data)
}

// GrackleUpdateRequestProtoCodec is a GrackleUpdateRequestCodec codec that uses VT protobuf.
type GrackleUpdateRequestProtoCodec struct {
}

var _ GrackleUpdateRequestCodec = (*GrackleUpdateRequestProtoCodec)(nil)

func (c *GrackleUpdateRequestProtoCodec) Encode(p *corepb.GrackleUpdateRequest) ([]byte, error) {
	return p.MarshalVT()
}

func (c *GrackleUpdateRequestProtoCodec) Decode(data []byte, out *corepb.GrackleUpdateRequest) error {
	return out.UnmarshalVT(data)
}

// GrackleUpdateResponseProtoCodec is a GrackleUpdateResponseCodec codec that uses VT protobuf.
type GrackleUpdateResponseProtoCodec struct {
}

var _ GrackleUpdateResponseCodec = (*GrackleUpdateResponseProtoCodec)(nil)

func (c *GrackleUpdateResponseProtoCodec) Encode(p *corepb.GrackleUpdateResponse) ([]byte, error) {
	return p.MarshalVT()
}

func (c *GrackleUpdateResponseProtoCodec) Decode(data []byte, out *corepb.GrackleUpdateResponse) error {
	return out.UnmarshalVT(data)
}
