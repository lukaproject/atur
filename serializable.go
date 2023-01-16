package atur

import "encoding/binary"

type Serializable interface {
	Serialize() []byte
	Unserialize(s []byte) (err error)
}

type (
	Int32 struct{ V int32 }
	Int64 struct{ V int64 }
)

func (i32 *Int32) Serialize() (b []byte) {
	b = make([]byte, 4)
	binary.BigEndian.PutUint32(b, uint32(i32.V))
	return
}

func (i32 *Int32) Unserialize(s []byte) (err error) {
	i32.V = int32(binary.BigEndian.Uint32(s))
	return
}

func (i64 *Int64) Serialize() (b []byte) {
	b = make([]byte, 8)
	binary.BigEndian.PutUint32(b, uint32(i64.V))
	return
}

func (i64 *Int64) Unserialize(s []byte) (err error) {
	i64.V = int64(binary.BigEndian.Uint64(s))
	return
}
