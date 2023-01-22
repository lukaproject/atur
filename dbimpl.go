package atur

import (
	"context"
	"errors"
	"path"
	"sync"
	"sync/atomic"
)

var (
	Err_TableExist    = errors.New("atur: table exist")
	Err_TableNotExist = errors.New("atur: table not exist")
	Err_DBClosed      = errors.New("atur: DB closed")
)

const (
	TableNamePrefix = "T_"
	ConfigTableName = "C_CONFIG"
)

type dBImpl struct {
	// directory of atur working in.
	Dir string
	// tableName to Table
	Name2table *sync.Map

	createAndDropMut sync.Mutex

	// shards awlays equal to 1
	config   KvStore
	isClosed atomic.Value
}

func (db *dBImpl) Create(tableName string, shards int) (err error) {
	if db.closed() {
		return Err_DBClosed
	}
	if _, ok := db.Name2table.Load(tableName); ok {
		// table exist.
		return Err_TableExist
	}

	db.createAndDropMut.Lock()
	defer db.createAndDropMut.Unlock()
	table, err := NewTable(path.Join(db.Dir, TableNamePrefix+tableName), tableName, shards)
	if err != nil {
		return
	}
	// write table define to disk.
	err = db.config.SetCtx(context.Background(), []byte(tableName), table)
	if err != nil {
		return
	}

	db.Name2table.Store(tableName, table)
	return
}

func (db *dBImpl) DropTable(tableName string) error {
	if db.closed() {
		return Err_DBClosed
	}
	if _, ok := db.Name2table.Load(tableName); !ok {
		return Err_TableNotExist
	}
	db.createAndDropMut.Lock()
	defer db.createAndDropMut.Unlock()
	table, _ := InterfaceToWithBool[Table](db.Name2table.LoadAndDelete(tableName))
	table.Close()
	return nil
}

func (db *dBImpl) GetTable(tableName string) (t Table, err error) {
	if db.closed() {
		return nil, Err_DBClosed
	}
	tInterface, ok := db.Name2table.Load(tableName)
	if !ok {
		return nil, Err_TableNotExist
	}
	t = tInterface.(Table)
	return
}

func (db *dBImpl) Close() {
	if db.closed() {
		return
	}
	db.isClosed.Store(true)
	db.config.Close()
}

func (db *dBImpl) closed() bool {
	return InterfaceTo[bool](db.isClosed.Load())
}

func (db *dBImpl) loadConfig() (err error) {
	configKvConf := NewKvConfig(SetDir(path.Join(db.Dir, ConfigTableName)), SetShards(1))
	db.config, err = NewKvStore(configKvConf)
	if err != nil {
		return
	}
	ret := db.config.GetFull()
	for _, v := range ret {
		tableObj := &tableImpl{}
		tableObj.Unserialize(v)
		table, err := NewTable(path.Join(db.Dir, TableNamePrefix+tableObj.Name), tableObj.Name, tableObj.Shards)
		if err != nil {
			return err
		}
		db.Name2table.Store(tableObj.Name, table)
	}
	return
}
