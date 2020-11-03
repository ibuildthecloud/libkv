package json

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/docker/libkv"
	"github.com/docker/libkv/store"
	"github.com/docker/libkv/store/json/broadcast"
	"github.com/moby/locker"
)

var (
	emptyOptions store.WriteOptions
	emptyKV      store.KVPair
)

// Register registers boltdb to libkv
func Register() {
	libkv.AddStore(store.JSON, New)
}

func New(addrs []string, _ *store.Config) (store.Store, error) {
	if len(addrs) != 1 {
		return nil, fmt.Errorf("missing path as first addr")
	}
	dir := filepath.Dir(addrs[0])
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to mkdir %s: %w", dir, err)
	}

	s := &Store{
		file:    addrs[0],
		changes: make(chan interface{}),
	}
	err := s.load()
	if err != nil {
		return nil, err
	}

	go func() {
		s.WatchTree("", context.Background().Done())
	}()

	return logging{ds: s}, nil
}

type data struct {
	store.KVPair
	TTL time.Time
}

type Store struct {
	lock sync.Mutex

	file      string
	broadcast broadcast.Broadcaster
	changes   chan interface{}
	Data      map[string]data
	locker    locker.Locker
}

func xnormalizeKey(key string, isDir bool) string {
	for strings.HasSuffix(key, "/") {
		key = strings.TrimSuffix(key, "/")
	}
	if isDir {
		key += "/"
	}
	return key
}

func (s *Store) load() error {
	s.Data = map[string]data{}
	f, err := os.Open(s.file)
	if os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return err
	}
	defer f.Close()

	return json.NewDecoder(f).Decode(&s.Data)
}

func (s *Store) Put(key string, value []byte, options *store.WriteOptions) error {
	_, _, err := s.put(key, value, nil, false, options)
	return err
}

func (s *Store) checkTTL(key string, kv data) {
	if kv.TTL.IsZero() {
		return
	}
	go func() {
		now := time.Now()
		if kv.TTL.Before(now) {
			_, _ = s.AtomicDelete(key, &kv.KVPair)
			return
		}
		time.Sleep(kv.TTL.Sub(now))
		_, _ = s.AtomicDelete(key, &kv.KVPair)
	}()
}

func (s *Store) persist() error {
	tmp, err := os.Create(s.file + ".tmp")
	if err != nil {
		return err
	}
	defer tmp.Close()

	if err := json.NewEncoder(tmp).Encode(s.Data); err != nil {
		return err
	}

	if err := tmp.Close(); err != nil {
		return err
	}

	return os.Rename(s.file+".tmp", s.file)
}

func (s *Store) Get(key string) (*store.KVPair, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	data, ok := s.Data[key]
	if !ok {
		return nil, store.ErrKeyNotFound
	}
	return &data.KVPair, nil
}

func (s *Store) Delete(key string) error {
	_, err := s.delete(key, false, nil)
	return err
}

func (s *Store) Exists(key string) (bool, error) {
	_, err := s.Get(key)
	if err == store.ErrKeyNotFound {
		return false, nil
	}
	return true, err
}

func (s *Store) connect() (chan interface{}, error) {
	return s.changes, nil
}

func (s *Store) Watch(key string, stopCh <-chan struct{}) (<-chan *store.KVPair, error) {
	ctx, cancel := context.WithCancel(context.Background())
	c, err := s.broadcast.Subscribe(ctx, s.connect)
	if err != nil {
		cancel()
		return nil, err
	}

	result := make(chan *store.KVPair)

	go func() {
		<-stopCh
		cancel()
	}()

	go func() {
		defer close(result)
		for v := range c {
			if kv, ok := v.(*store.KVPair); ok && kv.Key == key {
				result <- kv
			}
		}
	}()

	return result, nil
}

func (s *Store) WatchTree(directory string, stopCh <-chan struct{}) (<-chan []*store.KVPair, error) {
	ctx, cancel := context.WithCancel(context.Background())
	c, err := s.broadcast.Subscribe(ctx, s.connect)
	if err != nil {
		cancel()
		return nil, err
	}

	result := make(chan []*store.KVPair)

	go func() {
		<-stopCh
		cancel()
	}()

	go func() {
		defer close(result)
		for v := range c {
			if kv, ok := v.(*store.KVPair); ok && strings.HasPrefix(kv.Key, directory) {
				result <- []*store.KVPair{kv}
			}
		}
	}()

	return result, nil
}

func (s *Store) NewLock(key string, _ *store.LockOptions) (store.Locker, error) {
	return &keyLock{
		key: key,
		s:   s,
	}, nil
}

type keyLock struct {
	key        string
	lockChan   chan struct{}
	unlockLock sync.Mutex
	s          *Store
}

func (k *keyLock) Lock(stopChan chan struct{}) (<-chan struct{}, error) {
	k.s.locker.Lock(k.key)
	k.lockChan = make(chan struct{})
	if stopChan != nil {
		go func() {
			select {
			case <-stopChan:
				k.Unlock()
			case <-k.lockChan:
			}
		}()
	}
	return k.lockChan, nil
}

func (k *keyLock) Unlock() error {
	k.unlockLock.Lock()
	defer k.unlockLock.Unlock()
	if k.lockChan == nil {
		return nil
	}

	if err := k.s.locker.Unlock(k.key); err != nil {
		return err
	}
	close(k.lockChan)
	return nil
}

func (s *Store) List(directory string) (result []*store.KVPair, _ error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	for k, v := range s.Data {
		if strings.HasPrefix(k, directory) {
			result = append(result, &v.KVPair)
		}
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Key < result[j].Key
	})

	return result, nil
}

func (s *Store) DeleteTree(directory string) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	deleted := map[string]data{}
	for k, v := range s.Data {
		if strings.HasPrefix(k, directory) {
			deleted[k] = v
			delete(s.Data, k)
		}
	}

	if err := s.persist(); err != nil {
		for k, v := range deleted {
			s.Data[k] = v
		}
		return err
	}

	return nil
}

func (s *Store) AtomicPut(key string, value []byte, previous *store.KVPair, options *store.WriteOptions) (bool, *store.KVPair, error) {
	return s.put(key, value, previous, true, options)
}

func (s *Store) put(key string, value []byte, previous *store.KVPair, atomic bool, options *store.WriteOptions) (bool, *store.KVPair, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if options == nil {
		options = &emptyOptions
	}

	if previous == nil {
		previous = &emptyKV
	}

	oldValue, exists := s.Data[key]
	newValue := data{
		KVPair: store.KVPair{
			Key:       key,
			Value:     value,
			LastIndex: oldValue.LastIndex + 1,
		},
	}
	if options.TTL > 0 {
		newValue.TTL = time.Now().Add(options.TTL)
	}

	if atomic {
		if previous.LastIndex != oldValue.LastIndex {
			return false, &oldValue.KVPair, store.ErrKeyModified
		}
	}

	s.Data[key] = newValue

	if err := s.persist(); err != nil {
		if exists {
			s.Data[key] = oldValue
		} else {
			delete(s.Data, key)
		}
		return false, nil, err
	}

	s.checkTTL(key, newValue)
	s.changes <- &newValue.KVPair
	return true, &newValue.KVPair, nil
}

func (s *Store) AtomicDelete(key string, previous *store.KVPair) (bool, error) {
	return s.delete(key, true, previous)
}

func (s *Store) delete(key string, atomic bool, previous *store.KVPair) (bool, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if previous == nil {
		previous = &emptyKV
	}

	data, ok := s.Data[key]
	if atomic {
		if previous.LastIndex != data.LastIndex {
			return false, store.ErrKeyModified
		}
	}

	if !ok {
		return true, nil
	}

	delete(s.Data, key)
	if err := s.persist(); err != nil {
		s.Data[key] = data
		return false, err
	}
	return true, nil
}

func (s *Store) Close() {
	s.lock.Lock()
	close(s.changes)
	s.changes = nil
	s.lock.Unlock()
}
