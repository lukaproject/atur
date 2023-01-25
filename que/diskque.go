package que

import (
	"errors"
	"time"

	"github.com/lukaproject/atur"
	"github.com/nsqio/go-diskqueue"
	"github.com/sirupsen/logrus"
)

var (
	Err_Timeout = errors.New("atur: timeout")
	Err_Empty   = errors.New("atur: empty")
)

type Que interface {
	Push(atur.Serializable) error
	Pop(atur.Serializable) error
}

type DiskQue struct {
	Name            string
	DirPath         string
	MaxBytesPerFile int64
	MinMsgSize      int32
	MaxMsgSize      int32
	SyncEvery       int64
	SyncTimeout     time.Duration
	Logf            diskqueue.AppLogFunc
	PopTimeout      time.Duration

	diskQ diskqueue.Interface
}

func (dq *DiskQue) Push(item atur.Serializable) error {
	err := dq.diskQ.Put(item.Serialize())
	if err != nil {
		return err
	}
	return nil
}

func (dq *DiskQue) Pop(item atur.Serializable) error {
	if dq.diskQ.Depth() == 0 {
		return Err_Empty
	}
	select {
	case <-time.After(dq.PopTimeout):

		return Err_Timeout
	case retBytes := <-dq.diskQ.ReadChan():
		err := item.Unserialize(retBytes)
		if err != nil {
			return err
		}
	}
	return nil
}

func (dq *DiskQue) New() {
	dq.diskQ = diskqueue.New(
		dq.Name,
		dq.DirPath,
		dq.MaxBytesPerFile,
		dq.MinMsgSize,
		dq.MaxMsgSize,
		dq.SyncEvery,
		dq.SyncTimeout,
		dq.Logf,
	)
}

func (dq *DiskQue) Close() error {
	return dq.diskQ.Close()
}

func DefaultLogf(lvl diskqueue.LogLevel, f string, args ...interface{}) {
	logrusLevel := logrus.Level(lvl)
	logrus.StandardLogger().Logf(logrusLevel, f, args...)
}

func DefaultDiskQue(name string, dirPath string) *DiskQue {
	return &DiskQue{
		Name:            name,
		DirPath:         dirPath,
		MaxBytesPerFile: 1 << 10,
		MinMsgSize:      1 << 2,
		MaxMsgSize:      1 << 10,
		SyncEvery:       2500,
		SyncTimeout:     3 * time.Second,
		PopTimeout:      1 * time.Second,
		Logf:            DefaultLogf,
	}
}
