package atur

import (
	leveldbOpt "github.com/syndtr/goleveldb/leveldb/opt"
)

type KvStoreConfig struct {
	Dir            string
	Shards         int
	LeveldbOptions *leveldbOpt.Options
}

func DefaultKvConfig() *KvStoreConfig {
	return &KvStoreConfig{
		Dir:            "dir",
		Shards:         1,
		LeveldbOptions: nil,
	}
}

type KvOption func(*KvStoreConfig)

func SetDir(dir string) KvOption {
	return func(ksc *KvStoreConfig) {
		ksc.Dir = dir
	}
}

func SetShards(shards int) KvOption {
	return func(ksc *KvStoreConfig) {
		ksc.Shards = shards
	}
}

func NewKvConfig(opts ...KvOption) (ksc *KvStoreConfig) {
	ksc = &KvStoreConfig{}
	for _, opt := range opts {
		opt(ksc)
	}
	return
}
