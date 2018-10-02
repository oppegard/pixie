// Code generated by protoc-gen-gogo. DO NOT EDIT.
// source: services/common/proto/jwt.proto

package jwt // import "pixielabs.ai/pixielabs/services/common"

import proto "github.com/gogo/protobuf/proto"
import fmt "fmt"
import math "math"

import strings "strings"
import reflect "reflect"

import io "io"

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.GoGoProtoPackageIsVersion2 // please upgrade the proto package

type JWTClaims struct {
	Audience             string   `protobuf:"bytes,1,opt,name=audience,proto3" json:"audience,omitempty"`
	ExpiresAt            int64    `protobuf:"varint,2,opt,name=expires_at,json=expiresAt,proto3" json:"expires_at,omitempty"`
	Id                   string   `protobuf:"bytes,3,opt,name=id,proto3" json:"id,omitempty"`
	IssuedT              int64    `protobuf:"varint,4,opt,name=issued_t,json=issuedT,proto3" json:"issued_t,omitempty"`
	Issuer               string   `protobuf:"bytes,5,opt,name=issuer,proto3" json:"issuer,omitempty"`
	NotBefore            int64    `protobuf:"varint,6,opt,name=not_before,json=notBefore,proto3" json:"not_before,omitempty"`
	Subject              string   `protobuf:"bytes,7,opt,name=subject,proto3" json:"subject,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *JWTClaims) Reset()      { *m = JWTClaims{} }
func (*JWTClaims) ProtoMessage() {}
func (*JWTClaims) Descriptor() ([]byte, []int) {
	return fileDescriptor_jwt_267850023ff5d713, []int{0}
}
func (m *JWTClaims) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *JWTClaims) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_JWTClaims.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalTo(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (dst *JWTClaims) XXX_Merge(src proto.Message) {
	xxx_messageInfo_JWTClaims.Merge(dst, src)
}
func (m *JWTClaims) XXX_Size() int {
	return m.Size()
}
func (m *JWTClaims) XXX_DiscardUnknown() {
	xxx_messageInfo_JWTClaims.DiscardUnknown(m)
}

var xxx_messageInfo_JWTClaims proto.InternalMessageInfo

func (m *JWTClaims) GetAudience() string {
	if m != nil {
		return m.Audience
	}
	return ""
}

func (m *JWTClaims) GetExpiresAt() int64 {
	if m != nil {
		return m.ExpiresAt
	}
	return 0
}

func (m *JWTClaims) GetId() string {
	if m != nil {
		return m.Id
	}
	return ""
}

func (m *JWTClaims) GetIssuedT() int64 {
	if m != nil {
		return m.IssuedT
	}
	return 0
}

func (m *JWTClaims) GetIssuer() string {
	if m != nil {
		return m.Issuer
	}
	return ""
}

func (m *JWTClaims) GetNotBefore() int64 {
	if m != nil {
		return m.NotBefore
	}
	return 0
}

func (m *JWTClaims) GetSubject() string {
	if m != nil {
		return m.Subject
	}
	return ""
}

func init() {
	proto.RegisterType((*JWTClaims)(nil), "common.JWTClaims")
}
func (this *JWTClaims) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	that1, ok := that.(*JWTClaims)
	if !ok {
		that2, ok := that.(JWTClaims)
		if ok {
			that1 = &that2
		} else {
			return false
		}
	}
	if that1 == nil {
		return this == nil
	} else if this == nil {
		return false
	}
	if this.Audience != that1.Audience {
		return false
	}
	if this.ExpiresAt != that1.ExpiresAt {
		return false
	}
	if this.Id != that1.Id {
		return false
	}
	if this.IssuedT != that1.IssuedT {
		return false
	}
	if this.Issuer != that1.Issuer {
		return false
	}
	if this.NotBefore != that1.NotBefore {
		return false
	}
	if this.Subject != that1.Subject {
		return false
	}
	return true
}
func (this *JWTClaims) GoString() string {
	if this == nil {
		return "nil"
	}
	s := make([]string, 0, 11)
	s = append(s, "&jwt.JWTClaims{")
	s = append(s, "Audience: "+fmt.Sprintf("%#v", this.Audience)+",\n")
	s = append(s, "ExpiresAt: "+fmt.Sprintf("%#v", this.ExpiresAt)+",\n")
	s = append(s, "Id: "+fmt.Sprintf("%#v", this.Id)+",\n")
	s = append(s, "IssuedT: "+fmt.Sprintf("%#v", this.IssuedT)+",\n")
	s = append(s, "Issuer: "+fmt.Sprintf("%#v", this.Issuer)+",\n")
	s = append(s, "NotBefore: "+fmt.Sprintf("%#v", this.NotBefore)+",\n")
	s = append(s, "Subject: "+fmt.Sprintf("%#v", this.Subject)+",\n")
	s = append(s, "}")
	return strings.Join(s, "")
}
func valueToGoStringJwt(v interface{}, typ string) string {
	rv := reflect.ValueOf(v)
	if rv.IsNil() {
		return "nil"
	}
	pv := reflect.Indirect(rv).Interface()
	return fmt.Sprintf("func(v %v) *%v { return &v } ( %#v )", typ, typ, pv)
}
func (m *JWTClaims) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalTo(dAtA)
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *JWTClaims) MarshalTo(dAtA []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	if len(m.Audience) > 0 {
		dAtA[i] = 0xa
		i++
		i = encodeVarintJwt(dAtA, i, uint64(len(m.Audience)))
		i += copy(dAtA[i:], m.Audience)
	}
	if m.ExpiresAt != 0 {
		dAtA[i] = 0x10
		i++
		i = encodeVarintJwt(dAtA, i, uint64(m.ExpiresAt))
	}
	if len(m.Id) > 0 {
		dAtA[i] = 0x1a
		i++
		i = encodeVarintJwt(dAtA, i, uint64(len(m.Id)))
		i += copy(dAtA[i:], m.Id)
	}
	if m.IssuedT != 0 {
		dAtA[i] = 0x20
		i++
		i = encodeVarintJwt(dAtA, i, uint64(m.IssuedT))
	}
	if len(m.Issuer) > 0 {
		dAtA[i] = 0x2a
		i++
		i = encodeVarintJwt(dAtA, i, uint64(len(m.Issuer)))
		i += copy(dAtA[i:], m.Issuer)
	}
	if m.NotBefore != 0 {
		dAtA[i] = 0x30
		i++
		i = encodeVarintJwt(dAtA, i, uint64(m.NotBefore))
	}
	if len(m.Subject) > 0 {
		dAtA[i] = 0x3a
		i++
		i = encodeVarintJwt(dAtA, i, uint64(len(m.Subject)))
		i += copy(dAtA[i:], m.Subject)
	}
	return i, nil
}

func encodeVarintJwt(dAtA []byte, offset int, v uint64) int {
	for v >= 1<<7 {
		dAtA[offset] = uint8(v&0x7f | 0x80)
		v >>= 7
		offset++
	}
	dAtA[offset] = uint8(v)
	return offset + 1
}
func (m *JWTClaims) Size() (n int) {
	var l int
	_ = l
	l = len(m.Audience)
	if l > 0 {
		n += 1 + l + sovJwt(uint64(l))
	}
	if m.ExpiresAt != 0 {
		n += 1 + sovJwt(uint64(m.ExpiresAt))
	}
	l = len(m.Id)
	if l > 0 {
		n += 1 + l + sovJwt(uint64(l))
	}
	if m.IssuedT != 0 {
		n += 1 + sovJwt(uint64(m.IssuedT))
	}
	l = len(m.Issuer)
	if l > 0 {
		n += 1 + l + sovJwt(uint64(l))
	}
	if m.NotBefore != 0 {
		n += 1 + sovJwt(uint64(m.NotBefore))
	}
	l = len(m.Subject)
	if l > 0 {
		n += 1 + l + sovJwt(uint64(l))
	}
	return n
}

func sovJwt(x uint64) (n int) {
	for {
		n++
		x >>= 7
		if x == 0 {
			break
		}
	}
	return n
}
func sozJwt(x uint64) (n int) {
	return sovJwt(uint64((x << 1) ^ uint64((int64(x) >> 63))))
}
func (this *JWTClaims) String() string {
	if this == nil {
		return "nil"
	}
	s := strings.Join([]string{`&JWTClaims{`,
		`Audience:` + fmt.Sprintf("%v", this.Audience) + `,`,
		`ExpiresAt:` + fmt.Sprintf("%v", this.ExpiresAt) + `,`,
		`Id:` + fmt.Sprintf("%v", this.Id) + `,`,
		`IssuedT:` + fmt.Sprintf("%v", this.IssuedT) + `,`,
		`Issuer:` + fmt.Sprintf("%v", this.Issuer) + `,`,
		`NotBefore:` + fmt.Sprintf("%v", this.NotBefore) + `,`,
		`Subject:` + fmt.Sprintf("%v", this.Subject) + `,`,
		`}`,
	}, "")
	return s
}
func valueToStringJwt(v interface{}) string {
	rv := reflect.ValueOf(v)
	if rv.IsNil() {
		return "nil"
	}
	pv := reflect.Indirect(rv).Interface()
	return fmt.Sprintf("*%v", pv)
}
func (m *JWTClaims) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowJwt
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: JWTClaims: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: JWTClaims: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Audience", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowJwt
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				stringLen |= (uint64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthJwt
			}
			postIndex := iNdEx + intStringLen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Audience = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		case 2:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field ExpiresAt", wireType)
			}
			m.ExpiresAt = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowJwt
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.ExpiresAt |= (int64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 3:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Id", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowJwt
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				stringLen |= (uint64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthJwt
			}
			postIndex := iNdEx + intStringLen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Id = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		case 4:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field IssuedT", wireType)
			}
			m.IssuedT = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowJwt
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.IssuedT |= (int64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 5:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Issuer", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowJwt
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				stringLen |= (uint64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthJwt
			}
			postIndex := iNdEx + intStringLen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Issuer = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		case 6:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field NotBefore", wireType)
			}
			m.NotBefore = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowJwt
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.NotBefore |= (int64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 7:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Subject", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowJwt
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				stringLen |= (uint64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthJwt
			}
			postIndex := iNdEx + intStringLen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Subject = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipJwt(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthJwt
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func skipJwt(dAtA []byte) (n int, err error) {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return 0, ErrIntOverflowJwt
			}
			if iNdEx >= l {
				return 0, io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		wireType := int(wire & 0x7)
		switch wireType {
		case 0:
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowJwt
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				iNdEx++
				if dAtA[iNdEx-1] < 0x80 {
					break
				}
			}
			return iNdEx, nil
		case 1:
			iNdEx += 8
			return iNdEx, nil
		case 2:
			var length int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowJwt
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				length |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			iNdEx += length
			if length < 0 {
				return 0, ErrInvalidLengthJwt
			}
			return iNdEx, nil
		case 3:
			for {
				var innerWire uint64
				var start int = iNdEx
				for shift := uint(0); ; shift += 7 {
					if shift >= 64 {
						return 0, ErrIntOverflowJwt
					}
					if iNdEx >= l {
						return 0, io.ErrUnexpectedEOF
					}
					b := dAtA[iNdEx]
					iNdEx++
					innerWire |= (uint64(b) & 0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				innerWireType := int(innerWire & 0x7)
				if innerWireType == 4 {
					break
				}
				next, err := skipJwt(dAtA[start:])
				if err != nil {
					return 0, err
				}
				iNdEx = start + next
			}
			return iNdEx, nil
		case 4:
			return iNdEx, nil
		case 5:
			iNdEx += 4
			return iNdEx, nil
		default:
			return 0, fmt.Errorf("proto: illegal wireType %d", wireType)
		}
	}
	panic("unreachable")
}

var (
	ErrInvalidLengthJwt = fmt.Errorf("proto: negative length found during unmarshaling")
	ErrIntOverflowJwt   = fmt.Errorf("proto: integer overflow")
)

func init() {
	proto.RegisterFile("services/common/proto/jwt.proto", fileDescriptor_jwt_267850023ff5d713)
}

var fileDescriptor_jwt_267850023ff5d713 = []byte{
	// 279 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x5c, 0x90, 0x3f, 0x4e, 0xf3, 0x40,
	0x14, 0xc4, 0xfd, 0x92, 0xef, 0xb3, 0xe3, 0x2d, 0x28, 0xb6, 0x40, 0x0b, 0x12, 0x8f, 0x88, 0x2a,
	0xa2, 0x88, 0x0b, 0x4a, 0x2a, 0x42, 0x47, 0x19, 0x45, 0x02, 0xd1, 0x58, 0xfe, 0xf3, 0x90, 0xd6,
	0x8a, 0xbd, 0x96, 0x77, 0x4d, 0x52, 0x72, 0x04, 0x8e, 0xc1, 0x39, 0xa8, 0x28, 0x53, 0x52, 0xe2,
	0xa5, 0xa1, 0xcc, 0x11, 0x10, 0xeb, 0x90, 0x82, 0x6e, 0x7e, 0xa3, 0x19, 0x8d, 0x34, 0xec, 0x54,
	0x53, 0xf3, 0x28, 0x33, 0xd2, 0x51, 0xa6, 0xca, 0x52, 0x55, 0x51, 0xdd, 0x28, 0xa3, 0xa2, 0x62,
	0x65, 0xa6, 0x4e, 0x71, 0xbf, 0xf7, 0xcf, 0x5e, 0x81, 0x85, 0x37, 0xb7, 0x8b, 0xeb, 0x65, 0x22,
	0x4b, 0xcd, 0x8f, 0xd9, 0x28, 0x69, 0x73, 0x49, 0x55, 0x46, 0x02, 0xc6, 0x30, 0x09, 0xe7, 0x7b,
	0xe6, 0x27, 0x8c, 0xd1, 0xba, 0x96, 0x0d, 0xe9, 0x38, 0x31, 0x62, 0x30, 0x86, 0xc9, 0x70, 0x1e,
	0xee, 0x9c, 0x2b, 0xc3, 0x0f, 0xd8, 0x40, 0xe6, 0x62, 0xe8, 0x4a, 0x03, 0x99, 0xf3, 0x23, 0x36,
	0x92, 0x5a, 0xb7, 0x94, 0xc7, 0x46, 0xfc, 0x73, 0xe1, 0xa0, 0xe7, 0x05, 0x3f, 0x64, 0xbe, 0x93,
	0x8d, 0xf8, 0xef, 0xe2, 0x3b, 0xfa, 0x59, 0xa8, 0x94, 0x89, 0x53, 0x7a, 0x50, 0x0d, 0x09, 0xbf,
	0x5f, 0xa8, 0x94, 0x99, 0x39, 0x83, 0x0b, 0x16, 0xe8, 0x36, 0x2d, 0x28, 0x33, 0x22, 0x70, 0xbd,
	0x5f, 0x9c, 0xdd, 0x6d, 0x3a, 0xf4, 0xde, 0x3b, 0xf4, 0xb6, 0x1d, 0xc2, 0x93, 0x45, 0x78, 0xb1,
	0x08, 0x6f, 0x16, 0x61, 0x63, 0x11, 0x3e, 0x2c, 0xc2, 0x97, 0x45, 0x6f, 0x6b, 0x11, 0x9e, 0x3f,
	0xd1, 0xbb, 0x3f, 0xaf, 0xe5, 0x5a, 0xd2, 0x32, 0x49, 0xf5, 0x34, 0x91, 0xd1, 0x1e, 0xa2, 0x3f,
	0x97, 0x5d, 0x16, 0x2b, 0x93, 0xfa, 0xee, 0xad, 0x8b, 0xef, 0x00, 0x00, 0x00, 0xff, 0xff, 0xca,
	0x48, 0x7a, 0xa8, 0x50, 0x01, 0x00, 0x00,
}
