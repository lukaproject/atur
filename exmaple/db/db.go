package db

import (
	"path"

	"github.com/lukaproject/atur"
)

var (
	Dir       = path.Join("testDir", "user_info_cli")
	TableName = "TableInfo"
)

var (
	Db            atur.DB
	UserInfoTable atur.Table
)

func Init() (err error) {
	Db = atur.Open(Dir)
loop:
	UserInfoTable, err = Db.GetTable(TableName)
	if UserInfoTable == nil {
		err = Db.Create(TableName, 3)
		if err != nil {
			return
		}
		goto loop
	}
	return
}
