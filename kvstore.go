package atur

import (
	"context"
	"errors"
	"os"
	"path"
	"strconv"
	"time"

	"github.com/spaolacci/murmur3"
	"github.com/syndtr/goleveldb/leveldb"
	leveldbOpt "github.com/syndtr/goleveldb/leveldb/opt"
)

var (
	Err_NotFound = errors.New("atur: not found")
	Err_TimeOut  = errors.New("atur: timeout")
)

/**
-----------------------------KvStore---------------------------
some notices:
  1. You should implement the Serializable and TTLSerializable
 		by your self.
	2. TTLSerializable and the Serializable Object can be used
	  in the save KvStore.
---------------------------------------------------------------
**/

type KvStore interface {

	// Set kv pair to KVstore
	// input:
	// ctx : context
	// key : []byte
	// value : a Serializable Object, must be a point
	//
	// return :
	// error
	SetCtx(ctx context.Context, key []byte, value Serializable) (err error)

	// Delete the key from KVstore
	// input:
	// ctx : context
	// key : []byte
	//
	// return:
	// error
	DelCtx(ctx context.Context, key []byte) (err error)

	// Get kv pair from KVstore
	// input:
	// ctx : context
	// key : []byte
	// value : a Serializable Object, must be an initialized point,
	// 				 a null point is not availiable.
	//
	// return:
	// error
	GetCtx(ctx context.Context, key []byte, value Serializable) (err error)

	// Set a TTL kv pair to KVstore
	// input:
	// ctx : context
	// key : []byte
	// value : a Serializable Object, must be a TTLSerializable point
	//
	// return:
	// error
	SetTTLCtx(ctx context.Context, key []byte, value TTLSerializable) (err error)

	// Get TTL kv pair from KVstore
	// input:
	// ctx : context
	// key : []byte
	// value : a TTLSerializable Object, must be an initialized point,
	// 				 a null point is not availiable.
	//
	// return:
	// error
	GetTTLCtx(ctx context.Context, key []byte, value TTLSerializable) (err error)

	Close() (err error)
}

func NewKvStore(kvConf *KvStoreConfig) (KvStore, error) {
	ret := &levelDBKvStore{
		Dbs:    make([]*leveldb.DB, 0),
		Opts:   kvConf.LeveldbOptions,
		Shards: kvConf.Shards,
	}
	for i := 0; i < kvConf.Shards; i++ {
		dir := path.Join(kvConf.Dir, strconv.Itoa(i))
		os.Mkdir(dir, os.ModePerm)
		db, err := leveldb.OpenFile(dir, kvConf.LeveldbOptions)
		if err != nil {
			return nil, err
		}
		ret.Dbs = append(ret.Dbs, db)
	}

	return ret, nil
}

type levelDBKvStore struct {
	Dbs    []*leveldb.DB
	Opts   *leveldbOpt.Options
	Shards int
}

func calcIdx(key []byte, shard int) uint32 {
	m3 := murmur3.New32()
	m3.Write(key)
	return m3.Sum32() % uint32(shard)
}

func (lkv *levelDBKvStore) SetCtx(ctx context.Context, key []byte, value Serializable) (err error) {
	idx := calcIdx(key, lkv.Shards)
	return lkv.Dbs[idx].Put(key, value.Serialize(), nil)
}

func (lkv *levelDBKvStore) DelCtx(ctx context.Context, key []byte) (err error) {
	idx := calcIdx(key, lkv.Shards)
	return lkv.Dbs[idx].Delete(key, nil)
}

func (lkv *levelDBKvStore) GetCtx(ctx context.Context, key []byte, value Serializable) (err error) {
	idx := calcIdx(key, lkv.Shards)
	var v []byte
	v, err = lkv.Dbs[idx].Get(key, nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			err = Err_NotFound
		}
		return
	}
	err = value.Unserialize(v)
	if err != nil {
		return
	}
	return
}

func (lkv *levelDBKvStore) SetTTLCtx(ctx context.Context, key []byte, value TTLSerializable) (err error) {
	panic("")
}
func (lkv *levelDBKvStore) GetTTLCtx(ctx context.Context, key []byte, value TTLSerializable) (err error) {
	idx := calcIdx(key, lkv.Shards)
	var v []byte
	v, err = lkv.Dbs[idx].Get(key, nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return Err_NotFound
		}
		return
	}
	err = value.Unserialize(v)
	if err != nil {
		return
	}
	// check if timeout in get the value.
	if value.GetTTLTime() < time.Now().UnixMilli() {
		return Err_TimeOut
	}
	return
}

func (lkv *levelDBKvStore) Close() (err error) {
	for _, v := range lkv.Dbs {
		err = v.Close()
		if err != nil {
			return
		}
	}
	return
}
