package hmkv

import (
	"github.com/juju/errors"
	tidb_bytes  "github.com/pingcap/tidb/util/bytes"
	"bytes"
	"reflect"
	"unsafe"
	"github.com/pingcap/tidb/store/localstore/engine"
	"github.com/ngaut/log"
)

func BytesToString(b []byte) string {
	bh := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	sh := reflect.StringHeader{bh.Data, bh.Len}
	return *(*string)(unsafe.Pointer(&sh))
}

func StringToBytes(s string) []byte {
	sh := (*reflect.StringHeader)(unsafe.Pointer(&s))
	bh := reflect.SliceHeader{sh.Data, sh.Len, 0}
	return *(*[]byte)(unsafe.Pointer(&bh))
}

type errs struct {
	errStr string
}

func (e errs) Error() string {
	return e.errStr
}

type MMap struct {
	maps map[string][]byte
}

func (mmap *MMap) get(key []byte) ([]byte, bool) {
	value, err :=  mmap.maps[BytesToString(key)]
	log.Debugf("MMap get key:%v-->%v", key, value)
	return value, err
}

func (mmap *MMap) set(key []byte, value []byte)  {
	log.Debugf("MMap set key:%v-->%v", key, value)
	mmap.maps[BytesToString(key)] = value
}

func (mmap *MMap) delete(key []byte)  {
	//log.Debugf("MMap delete key:%v", key)
	delete(mmap.maps, BytesToString(key))
}

type Driver struct {
}

type DB struct {
	maps MMap
}

func (driver Driver) Open(schema string) (engine.DB, error) {
	return &DB{maps:MMap{maps:make(map[string][]byte)}}, nil
}

// Get gets the associated value with key, returns (nil, ErrNotFound) if no value found.
func (db *DB) Get(key []byte) ([]byte, error) {
	log.Debugf("hmkv Get %v", key)
	value, err := db.maps.get(key)
	if err == false {
		return nil, errs{errStr:"not found key"}
	}
	return tidb_bytes.CloneBytes(value), nil
}

// Seek searches for the first key in the engine which is >= key in byte order, returns (nil, nil, ErrNotFound)
// if such key is not found.
func (db *DB) Seek(key []byte) ([]byte, []byte, error) {
	retKey := key
	var retValue []byte
	foundKeyFlag := false
	for sk, v := range db.maps.maps {
		k := StringToBytes(sk)
		if bytes.Compare(k, key) >= 0 {
			if !foundKeyFlag {
				retKey = k
				retValue = v
				foundKeyFlag = true
				continue
			}
			if bytes.Compare(k, retKey)  <= 0 {
				retKey = k
				retValue = v
			}
		}
	}
	if foundKeyFlag {
		//log.Debugf("hmkv Seek ret, key:%v, %v", retKey, retValue)
		return tidb_bytes.CloneBytes(retKey), tidb_bytes.CloneBytes(retValue), nil
	} else {
		log.Debugf("hmkv Seek ret no found, key:%v, %v", key, nil)
		return nil, nil, errors.Trace(engine.ErrNotFound)
	}
}
// SeekReverse searches the engine in backward order for the first key-value pair which key is less than the key
// in byte order, returns (nil, nil, ErrNotFound) if such key is not found. If key is nil, the last key is returned.
func (db *DB) SeekReverse(key []byte) ([]byte, []byte, error) {
	log.Debugf("hmkv SeekReverse %v", key)
	if key == nil {
		var retKey []byte
		var retValue []byte
		for sk, v := range db.maps.maps {
			k := StringToBytes(sk)
			if retKey == nil {
				retKey = k
				retValue = v
				continue
			}
			if bytes.Compare(k, retKey) > 0 {
				retKey = k
				retValue = v
			}
		}
		if retKey == nil {
			return nil, nil, errors.Trace(engine.ErrNotFound)
		} else {
			return tidb_bytes.CloneBytes(retKey), tidb_bytes.CloneBytes(retValue), nil
		}
	}

	retKey := key
	var retValue []byte
	foundKeyFlag := false
	for sk, v := range db.maps.maps {
		k := StringToBytes(sk)
		if bytes.Compare(k, key)  < 0 {
			if !foundKeyFlag {
				retKey = k
				retValue = v
				foundKeyFlag = true
				continue
			}
			if bytes.Compare(k, retKey)  > 0 {
				retKey = k
				retValue = v
			}
		}
	}
	if foundKeyFlag {
		return tidb_bytes.CloneBytes(retKey), tidb_bytes.CloneBytes(retValue), nil
	} else {
		return nil, nil, errors.Trace(engine.ErrNotFound)
	}
}
// NewBatch creates a Batch for writing.
func (db *DB) NewBatch() engine.Batch {
	return &MBatch{maps:MMap{maps:make(map[string][]byte)}}
}
// Commit writes the changed data in Batch.
func (db *DB) Commit(b engine.Batch) error {
	bt, ok := b.(*MBatch)
	if !ok {
		return errs{errStr:"commit not type MBatch"}
	}
	for _, w := range bt.writes {
		if w.isDelete {
			db.maps.delete(w.key)
		} else {
			db.maps.set(w.key, w.value)
		}
	}
	return nil
}
// Close closes database.
func (db *DB) Close() error {
	return nil
}

type write struct {
	key []byte
	value []byte
	isDelete bool
}

type MBatch struct {
	maps MMap
	writes []write
}
// Put appends 'put operation' of the key/value to the batch.
func (b *MBatch) Put(key []byte, value []byte) {
	b.maps.set(key, value)
	w := write{
		key:key,
		value:value,
	}
	b.writes = append(b.writes, w)
}
// Delete appends 'delete operation' of the key/value to the batch.
func (b *MBatch) Delete(key []byte) {
	//delete(b.maps, key)
	b.maps.delete(key)
	w := write{
		key:key,
		isDelete:true,
	}
	b.writes = append(b.writes, w)
}
// Len return length of the batch
func (b *MBatch) Len() int {
	//return len(b.maps.maps)
	return len(b.writes)
}
