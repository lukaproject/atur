package atur

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
)

var (
	Err_ObjectIdExist = errors.New("atur: Object id exist in db")
	Err_TableClosed   = errors.New("atur: table closed")
)

type tableImpl struct {
	Name   string
	Shards int

	kv       KvStore
	isClosed atomic.Value
}

func (ti *tableImpl) InsertCtx(ctx context.Context, obj Object) error {
	if ti.closed() {
		return Err_TableClosed
	}
	if ti.kv.Exist(ctx, obj.GetId()) {
		// exist, return error
		return Err_ObjectIdExist
	}
	return ti.kv.SetCtx(ctx, obj.GetId(), obj)
}

func (ti *tableImpl) FindCtx(ctx context.Context, id []byte, obj Object) (err error) {
	if ti.closed() {
		return Err_TableClosed
	}
	return ti.kv.GetCtx(ctx, id, obj)
}

func (ti *tableImpl) DeleteCtx(ctx context.Context, id []byte) (err error) {
	if ti.closed() {
		return Err_TableClosed
	}
	return ti.kv.DelCtx(ctx, id)
}

func (ti *tableImpl) UpdateCtx(ctx context.Context, obj Object) (err error) {
	if ti.closed() {
		return Err_TableClosed
	}
	if !ti.kv.Exist(ctx, obj.GetId()) {
		// not exist, return error
		return Err_NotFound
	}
	ti.kv.SetCtx(ctx, obj.GetId(), obj)
	return
}

func (ti *tableImpl) Close() {
	if ti.closed() {
		return
	}
	ti.kv.Close()
	ti.isClosed.Store(true)
}

func (ti *tableImpl) Serialize() []byte {
	var b []byte
	b, err := json.Marshal(ti)
	if err != nil {
		panic(err)
	}
	return b
}
func (ti *tableImpl) Unserialize(s []byte) (err error) {
	return json.Unmarshal(s, ti)
}

func (ti *tableImpl) closed() bool {
	return InterfaceTo[bool](ti.isClosed.Load())
}

func NewTable(dir string, Name string, shards int) (ret Table, err error) {
	ti := &tableImpl{
		Name:   Name,
		Shards: shards,
	}
	ti.isClosed.Store(false)
	kvconf := NewKvConfig(SetShards(shards), SetDir(dir))
	ti.kv, err = NewKvStore(kvconf)
	if err != nil {
		return
	}
	return ti, nil
}
