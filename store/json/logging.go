package json

import (
	"github.com/docker/libkv/store"
	"github.com/sirupsen/logrus"
)

type logging struct {
	ds *Store
}

func (l logging) Put(key string, value []byte, options *store.WriteOptions) error {
	err := l.ds.Put(key, value, options)
	logrus.Debugf("PUT: %s, %s: %v", key, value, err)
	return err
}

func (l logging) Get(key string) (*store.KVPair, error) {
	kv, err := l.ds.Get(key)
	logrus.Debugf("GET: %s: %v, %v", key, kv, err)
	return kv, err
}

func (l logging) Delete(key string) error {
	err := l.ds.Delete(key)
	logrus.Debugf("DELETE: %s: %v", key, err)
	return err
}

func (l logging) Exists(key string) (bool, error) {
	kv, err := l.ds.Exists(key)
	logrus.Debugf("EXISTS: %s: %v, %v", key, kv, err)
	return kv, err
}

func (l logging) Watch(key string, stopCh <-chan struct{}) (<-chan *store.KVPair, error) {
	kv, err := l.ds.Watch(key, stopCh)
	logrus.Debugf("WATCH: %s: %v, %v", key, kv, err)
	return kv, err
}

func (l logging) WatchTree(directory string, stopCh <-chan struct{}) (<-chan []*store.KVPair, error) {
	kv, err := l.ds.WatchTree(directory, stopCh)
	logrus.Debugf("WATCHTREE: %s: %v, %v", directory, kv, err)
	return kv, err
}

func (l logging) NewLock(key string, options *store.LockOptions) (store.Locker, error) {
	kv, err := l.ds.NewLock(key, options)
	logrus.Debugf("NEWLOCK: %s: %v, %v", key, kv, err)
	return kv, err
}

func (l logging) List(directory string) ([]*store.KVPair, error) {
	kv, err := l.ds.List(directory)
	logrus.Debugf("LIST: %s: %v, %v", directory, kv, err)
	return kv, err
}

func (l logging) DeleteTree(directory string) error {
	err := l.ds.DeleteTree(directory)
	logrus.Debugf("DELETETREE: %s: %v", directory, err)
	return err
}

func (l logging) AtomicPut(key string, value []byte, previous *store.KVPair, options *store.WriteOptions) (bool, *store.KVPair, error) {
	ok, kv, err := l.ds.AtomicPut(key, value, previous, options)
	logrus.Debugf("ATOMICPUT: %s, %s, %v, %v: %v, %v, %v", key, value, previous, options, ok, kv, err)
	return ok, kv, err
}

func (l logging) AtomicDelete(key string, previous *store.KVPair) (bool, error) {
	kv, err := l.ds.AtomicDelete(key, previous)
	logrus.Debugf("ATOMICDELETE: %s, %v: %v, %v", key, previous, kv, err)
	return kv, err
}

func (l logging) Close() {
	l.ds.Close()
	logrus.Debug("CLOSE:")
}
