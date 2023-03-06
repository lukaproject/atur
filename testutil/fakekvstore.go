package testutil

import (
	"context"
	"sync"
	"time"

	"github.com/lukaproject/atur"
)

type fakeKvStore struct {
	mp *sync.Map // map[string, string] key -> value by string([]byte)
}

// Set kv pair to KVstore
// input:
// ctx : context
// key : []byte
// value : a Serializable Object, must be a point
//
// return :
// error
func (kv *fakeKvStore) SetCtx(ctx context.Context, key []byte, value atur.Serializable) (err error) {
	kv.mp.Store(string(key), string(value.Serialize()))
	return
}

// Delete the key from KVstore
// input:
// ctx : context
// key : []byte
//
// return:
// error
func (kv *fakeKvStore) DelCtx(ctx context.Context, key []byte) (err error) {
	kv.mp.Delete(string(key))
	return
}

// Get kv pair from KVstore
// input:
// ctx : context
// key : []byte
// value : a Serializable Object, must be an initialized point,
// 				 a null point is not availiable.
//
// return:
// error
func (kv *fakeKvStore) GetCtx(ctx context.Context, key []byte, value atur.Serializable) (err error) {
	valueByte, ok := kv.mp.Load(string(key))
	if !ok {
		return atur.Err_NotFound
	}
	err = value.Unserialize([]byte(valueByte.(string)))
	if err != nil {
		return
	}
	return
}

// Set a TTL kv pair to KVstore
// input:
// ctx : context
// key : []byte
// value : a Serializable Object, must be a TTLSerializable point
//
// return:
// error
func (kv *fakeKvStore) SetTTLCtx(ctx context.Context, key []byte, value atur.TTLSerializable) (err error) {
	kv.mp.Store(string(key), string(value.Serialize()))
	return
}

// Get TTL kv pair from KVstore
// input:
// ctx : context
// key : []byte
// value : a TTLSerializable Object, must be an initialized point,
// 				 a null point is not availiable.
//
// return:
// error
func (kv *fakeKvStore) GetTTLCtx(ctx context.Context, key []byte, value atur.TTLSerializable) (err error) {
	valueByte, ok := kv.mp.Load(string(key))
	if !ok {
		return atur.Err_NotFound
	}
	err = value.Unserialize([]byte(valueByte.(string)))
	if err != nil {
		return
	}
	if time.Now().UnixMilli() > value.GetTTLTime() {
		return atur.Err_NotFound
	}
	return
}

// Check if the key exist or not
// input:
// ctx : context
// key : []byte
//
// return:
// exist bool
func (kv *fakeKvStore) Exist(ctx context.Context, key []byte) (exist bool) {
	_, exist = kv.mp.Load(string(key))
	return exist
}

// maybe only used by leveldbKvStore
func (kv *fakeKvStore) GetFull() map[string]string {
	ret := make(map[string]string)
	kv.mp.Range(func(key, value any) bool {
		ret[key.(string)] = value.(string)
		return true
	})
	return ret
}

func (kv *fakeKvStore) Close() (err error) {
	return
}

func NewFakeKvStore() atur.KvStore {
	return &fakeKvStore{
		mp: &sync.Map{},
	}
}
