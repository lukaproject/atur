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

type Option func(*KvStoreConfig)

func SetDir(dir string) Option {
	return func(ksc *KvStoreConfig) {
		ksc.Dir = dir
	}
}

func SetShards(shards int) Option {
	return func(ksc *KvStoreConfig) {
		ksc.Shards = shards
	}
}

func SetLeveldbOptions(ldbOpt *leveldbOpt.Options) Option {
	return func(ksc *KvStoreConfig) {
		ksc.LeveldbOptions = ldbOpt
	}
}

func NewKvConfig(opts ...Option) (ksc *KvStoreConfig) {
	ksc = &KvStoreConfig{}
	for _, opt := range opts {
		opt(ksc)
	}
	return
}
