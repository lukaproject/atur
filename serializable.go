package atur

import (
	"encoding/binary"
)

type Serializable interface {
	// This function support to serialize the object to []byte
	// in order to write down to the disk
	// this fuction **MUST** be no error, this rule must be
	// obeyed by the implementor.
	Serialize() []byte

	// Unserialize the []byte to Object.
	Unserialize(s []byte) (err error)
}

type (
	Int32  struct{ V int32 }
	Int64  struct{ V int64 }
	Uint32 struct{ V uint32 }
	Uint64 struct{ V uint64 }
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
	binary.BigEndian.PutUint64(b, uint64(i64.V))
	return
}

func (i64 *Int64) Unserialize(s []byte) (err error) {
	i64.V = int64(binary.BigEndian.Uint64(s))
	return
}

func (ui32 *Uint32) Serialize() (b []byte) {
	b = make([]byte, 4)
	binary.BigEndian.PutUint32(b, ui32.V)
	return
}

func (ui32 *Uint32) Unserialize(s []byte) (err error) {
	ui32.V = binary.BigEndian.Uint32(s)
	return
}

func (ui64 *Uint64) Serialize() (b []byte) {
	b = make([]byte, 8)
	binary.BigEndian.PutUint64(b, ui64.V)
	return
}

func (ui64 *Uint64) Unserialize(s []byte) (err error) {
	ui64.V = binary.BigEndian.Uint64(s)
	return
}

type TtlObject interface {
	// return the last time (unix ms) this Object can keep.
	GetTTLTime() int64
}

type TTLSerializable interface {
	TtlObject
	Serializable
}
