package atur_test

import (
	"context"
	"encoding/json"
	"math/rand"
	"os"
	"path"
	"strconv"
	"testing"

	"github.com/lukaproject/atur"
	"github.com/lukaproject/atur/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testDir     = "testDir"
	letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
)

func randString(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Int63()%int64(len(letterBytes))]
	}
	return string(b)
}

type TestStruct struct {
	Key  string
	P    int
	Test string
}

func (ts *TestStruct) Serialize() (b []byte) {
	b, _ = json.Marshal(ts)
	return
}

func (ts *TestStruct) Unserialize(s []byte) (err error) {
	return json.Unmarshal(s, ts)
}

func (ts *TestStruct) Equal(other *TestStruct) bool {
	return string(ts.Serialize()) == string(other.Serialize())
}

func randomTestStructs(num int) (r []*TestStruct) {
	r = make([]*TestStruct, 0)
	for i := 0; i < num; i++ {
		r = append(r, &TestStruct{
			Key:  "test_" + strconv.Itoa(i),
			P:    rand.Int(),
			Test: randString(100),
		})
	}
	return
}

func set_get_del_test_cases(kvs atur.KvStore, t *testing.T) {
	var err error
	items := randomTestStructs(50)
	for _, v := range items {
		kvs.SetCtx(context.Background(), []byte(v.Key), v)
	}
	for _, v := range items {
		fromdb := &TestStruct{}
		err = kvs.GetCtx(context.Background(), []byte(v.Key), fromdb)
		require.Nilf(t, err, "get item key=%s wrong %v", v.Key, err)
		assert.Truef(
			t, v.Equal(fromdb),
			"key=%s, expect=%s, but=%s",
			string(v.Key), string(v.Serialize()), string(fromdb.Serialize()))
	}

	del_idxs := map[int]bool{
		1:  true,
		3:  true,
		5:  true,
		7:  true,
		9:  true,
		11: true,
	}
	for k := range del_idxs {
		err = kvs.DelCtx(context.Background(), []byte(items[k].Key))
		require.Nilf(t, err, "del item key=%s wrong %v", items[k].Key, err)
	}
	for idx, v := range items {
		fromdb := &TestStruct{}
		if _, ok := del_idxs[idx]; ok {
			err = kvs.GetCtx(context.Background(), []byte(v.Key), fromdb)
			require.Equal(t, err,
				atur.Err_NotFound,
				"this item key=%s, must be not found",
				v.Key)
		} else {
			err = kvs.GetCtx(context.Background(), []byte(v.Key), fromdb)
			require.Nilf(t, err, "get item key=%s wrong %v", v.Key, err)
			assert.Truef(
				t, v.Equal(fromdb),
				"key=%s, expect=%s, but=%s",
				string(v.Key), string(v.Serialize()), string(fromdb.Serialize()))
		}
	}
}

func Test_KvStore_for_set_get_del(t *testing.T) {
	var err error
	dir := path.Join(testDir, t.Name())
	kvconf := atur.NewKvConfig(atur.SetDir(dir), atur.SetShards(3))
	kvconf.Dir = dir
	err = os.MkdirAll(dir, os.ModePerm)
	require.Nilf(t, err, "mkdir %s wrong %v", dir, err)
	kvs, err := atur.NewKvStore(kvconf)
	require.Nilf(t, err, "new kvstore %s wrong %v", dir, err)
	defer func() {
		kvs.Close()
		_ = os.RemoveAll(dir)
	}()
	set_get_del_test_cases(kvs, t)
}

func Test_fakeKvStore_for_set_get_del(t *testing.T) {
	kvs := testutil.NewFakeKvStore()
	defer kvs.Close()
	set_get_del_test_cases(kvs, t)
}
