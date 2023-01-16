package atur

import (
	"context"
	"errors"

	"github.com/syndtr/goleveldb/leveldb"
	leveldbOpt "github.com/syndtr/goleveldb/leveldb/opt"
)

var (
	Err_NotFound = errors.New("atur: not found")
)

type KvStore interface {
	SetCtx(ctx context.Context, key []byte, value Serializable) (err error)
	DelCtx(ctx context.Context, key []byte) (err error)
	GetCtx(ctx context.Context, key []byte, value Serializable) (err error)

	Close() (err error)
}

func NewKvStore(dir string, opts *leveldbOpt.Options) (ret KvStore, err error) {
	db, err := leveldb.OpenFile(dir, opts)
	if err != nil {
		return
	}
	ret = &levelDBKvStore{
		Db:   db,
		Opts: opts,
	}
	return
}

type levelDBKvStore struct {
	Db   *leveldb.DB
	Opts *leveldbOpt.Options
}

func (lkv *levelDBKvStore) SetCtx(ctx context.Context, key []byte, value Serializable) (err error) {
	return lkv.Db.Put(key, value.Serialize(), nil)
}

func (lkv *levelDBKvStore) DelCtx(ctx context.Context, key []byte) (err error) {
	return lkv.Db.Delete(key, nil)
}

func (lkv *levelDBKvStore) GetCtx(ctx context.Context, key []byte, value Serializable) (err error) {
	var v []byte
	v, err = lkv.Db.Get(key, nil)
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

func (lkv *levelDBKvStore) Close() (err error) {
	return lkv.Db.Close()
}
