package atur_test

import (
	"context"
	"encoding/json"
	"math/rand"
	"os"
	"path"
	"sync"

	"testing"

	"github.com/google/uuid"
	"github.com/lukaproject/atur"
	"github.com/stretchr/testify/require"
)

type TestObj struct {
	Id        string
	ObjName   string
	RandomInt int
}

func (tobj *TestObj) GetId() []byte {
	return []byte(tobj.Id)
}

func (tobj *TestObj) Serialize() []byte {
	b, err := json.Marshal(tobj)
	if err != nil {
		panic(err)
	}
	return b
}

func (tobj *TestObj) Unserialize(s []byte) (err error) {
	return json.Unmarshal(s, tobj)
}

func randomTestObj() *TestObj {
	return &TestObj{
		Id:        uuid.NewString(),
		ObjName:   "ObjName" + uuid.NewString(),
		RandomInt: rand.Int(),
	}
}

func insertObjs(table atur.Table, cnt int, result *map[string]string) {
	for i := 0; i < cnt; i++ {
		tObj := randomTestObj()
		table.InsertCtx(context.Background(), tObj)
		(*result)[string(tObj.GetId())] = string(tObj.Serialize())
	}
}

func runInserts(t *testing.T, table atur.Table, batchSize int) (err error) {
	wg := &sync.WaitGroup{}
	wg.Add(batchSize)
	eachCount := 1000
	totalResult := make(map[string]string)
	resultCh := make(chan map[string]string, batchSize)
	for i := 0; i < batchSize; i++ {
		go func(idx int) {
			defer wg.Done()
			t.Logf("batch count=%d", idx)
			result := make(map[string]string)
			insertObjs(table, eachCount, &result)
			resultCh <- result
		}(i)
	}
	wg.Wait()
	t.Log("finished")
	// merge the result
	for i := 0; i < batchSize; i++ {
		result := <-resultCh
		for k, v := range result {
			totalResult[k] = v
		}
	}
	t.Logf("inserts test finished, batch_size=%d", batchSize)
	for k, v := range totalResult {
		testObj := &TestObj{}
		testObj.Unserialize([]byte(v))
		fromTable := &TestObj{}
		err = table.FindCtx(context.Background(), []byte(k), fromTable)
		require.Nil(t, err)
		require.Equal(t, testObj.Id, fromTable.Id)
		require.Equal(t, testObj.ObjName, fromTable.ObjName)
		require.Equal(t, testObj.RandomInt, fromTable.RandomInt)
	}
	return
}

func TestAtur(t *testing.T) {
	tableName := "TestObj"
	testDir := "testDir"
	os.RemoveAll(testDir)
	aturDb := atur.Open(path.Join(testDir, t.Name()))
	err := aturDb.Create(tableName, 5)
	require.Nil(t, err, "create table error")
	table, err := aturDb.GetTable(tableName)
	require.Nil(t, err, "get table %s error", tableName)
	require.Nil(t, runInserts(t, table, 10), "run inserts error")
}
