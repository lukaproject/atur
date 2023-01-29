package atur

import (
	"context"
	"os"
	"sync"
)

type Table interface {
	Serializable
	InsertCtx(ctx context.Context, obj Object) error
	FindCtx(ctx context.Context, id []byte, obj Object) error
	DeleteCtx(ctx context.Context, id []byte) error
	UpdateCtx(ctx context.Context, obj Object) error
	Close()
}

type DB interface {
	Create(tableName string, shards int) error

	DropTable(tableName string) error
	GetTable(tableName string) (Table, error)

	Close()
}

func Open(dir string) (DB, error) {
	var err error
	err = os.MkdirAll(dir, os.ModePerm)
	if err != nil && err != os.ErrExist {
		return nil, err
	}
	ret := &dBImpl{
		Dir:        dir,
		Name2table: &sync.Map{},
	}
	ret.isClosed.Store(false)
	err = ret.loadConfig()
	if err != nil {
		return nil, err
	}
	return ret, nil
}

func MustOpen(dir string) DB {
	db, err := Open(dir)
	if err != nil {
		panic(err)
	}
	return db
}
