package que_test

import (
	"encoding/json"
	"os"
	"path"
	"testing"

	"github.com/lukaproject/atur/que"
	"github.com/stretchr/testify/require"
)

type TestDiskQue struct {
	Name    string
	TestInt int
}

func (tdq *TestDiskQue) Serialize() []byte {
	b, _ := json.Marshal(tdq)
	return b
}

func (tdq *TestDiskQue) Unserialize(b []byte) error {
	return json.Unmarshal(b, tdq)
}

func TestDiskQue_reInit(t *testing.T) {
	var err error
	dqPath := path.Join("testDir", t.Name())
	os.MkdirAll(dqPath, os.ModePerm)
	dq := que.DefaultDiskQue(t.Name(), dqPath)
	dq.New()
	tdq := &TestDiskQue{
		Name:    "haha",
		TestInt: 12222,
	}
	for i := 0; i < 10; i++ {
		err = dq.Push(tdq)
		require.Nil(t, err, "push is error")
	}
	dq.Close()

	dq2 := que.DefaultDiskQue(t.Name(), dqPath)
	dq2.New()
	tdq2 := &TestDiskQue{}
	err = dq2.Pop(tdq2)
	require.Nil(t, err, "pop is error")
	t.Log(string(tdq2.Serialize()))
}
