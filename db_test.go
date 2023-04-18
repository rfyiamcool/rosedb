package rosedb

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/flower-corp/rosedb/logger"
	"github.com/stretchr/testify/assert"
)

func TestOpen(t *testing.T) {
	path := filepath.Join("/tmp", "rosedb")
	t.Run("default", func(t *testing.T) {
		opts := DefaultOptions(path)
		db, err := Open(opts)
		defer destroyDB(db)
		assert.Nil(t, err)
		assert.NotNil(t, db)
	})

	t.Run("mmap", func(t *testing.T) {
		opts := DefaultOptions(path)
		opts.IoType = MMap
		db, err := Open(opts)
		defer destroyDB(db)
		assert.Nil(t, err)
		assert.NotNil(t, db)
	})
}

func TestLogFileGC(t *testing.T) {
	path := filepath.Join("/tmp", "rosedb")
	opts := DefaultOptions(path)
	opts.LogFileGCInterval = time.Second * 7
	opts.LogFileGCRatio = 0.00001
	db, err := Open(opts)
	defer destroyDB(db)
	if err != nil {
		t.Error("open db err ", err)
	}

	writeCount := 800000
	for i := 0; i < writeCount; i++ {
		err := db.Set(GetKey(i), GetValue16B())
		assert.Nil(t, err)
	}

	var deleted [][]byte
	rand.Seed(time.Now().Unix())
	for i := 0; i < 100000; i++ {
		k := rand.Intn(writeCount)
		key := GetKey(k)
		err := db.Delete(key)
		assert.Nil(t, err)
		deleted = append(deleted, key)
	}

	time.Sleep(time.Second * 12)
	for _, key := range deleted {
		_, err := db.Get(key)
		assert.Equal(t, err, ErrKeyNotFound)
	}
}

func TestRoseDB_Backup(t *testing.T) {
	path := filepath.Join("/tmp", "rosedb")
	opts := DefaultOptions(path)
	db, err := Open(opts)
	defer destroyDB(db)
	if err != nil {
		t.Error("open db err ", err)
	}

	for i := 0; i < 10; i++ {
		err := db.Set(GetKey(i), GetValue128B())
		assert.Nil(t, err)
	}

	backupPath := filepath.Join("/tmp", "rosedb-backup")
	err = db.Backup(backupPath)
	assert.Nil(t, err)

	// open the backup database
	opts2 := DefaultOptions(backupPath)
	db2, err := Open(opts2)
	assert.Nil(t, err)
	defer destroyDB(db2)
	val, err := db2.Get(GetKey(4))
	assert.Nil(t, err)
	assert.NotNil(t, val)
}

func destroyDB(db *RoseDB) {
	if db != nil {
		_ = db.Close()
		if runtime.GOOS == "windows" {
			time.Sleep(time.Millisecond * 100)
		}
		err := os.RemoveAll(db.opts.DBPath)
		if err != nil {
			logger.Errorf("destroy db err: %v", err)
		}
	}
}

const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"

func init() {
	rand.Seed(time.Now().Unix())
}

// GetKey length: 32 Bytes
func GetKey(n int) []byte {
	return []byte("kvstore-bench-key------" + fmt.Sprintf("%09d", n))
}

func GetValue16B() []byte {
	return GetValue(16)
}

func GetValue128B() []byte {
	return GetValue(128)
}

func GetValue4K() []byte {
	return GetValue(4096)
}

func GetValue(n int) []byte {
	var str bytes.Buffer
	for i := 0; i < n; i++ {
		str.WriteByte(alphabet[rand.Int()%36])
	}
	return str.Bytes()
}

func TestRoseDB_syncDeleteExpireEntry(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		testRosedbSyncDeleteExpireEntry(t, FileIO, KeyOnlyMemMode)
	})

	t.Run("mmap", func(t *testing.T) {
		testRosedbSyncDeleteExpireEntry(t, MMap, KeyOnlyMemMode)
	})

	t.Run("key-val-mem-mode", func(t *testing.T) {
		testRosedbSyncDeleteExpireEntry(t, FileIO, KeyValueMemMode)
	})
}

func testRosedbSyncDeleteExpireEntry(t *testing.T, ioType IOType, mode DataIndexMode) {
	path := filepath.Join("/tmp", "rosedb")
	opts := DefaultOptions(path)
	opts.IoType = ioType
	opts.IndexMode = mode
	db, err := Open(opts)
	assert.Nil(t, err)
	defer destroyDB(db)

	type args struct {
		key      []byte
		value    []byte
		expire   float32
		dataType DataType
	}

	tests := []struct {
		name string
		args args
	}{
		{
			"sync expire 0.1", args{key: []byte("k1"), value: []byte("v1"), expire: 0.2, dataType: String},
		},
		{
			"sync expire 0.2", args{key: []byte("k2"), value: []byte("v2"), expire: 0.3, dataType: String},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dur := time.Duration(tt.args.expire) * time.Second
			err := db.SetEX(tt.args.key, tt.args.value, dur)
			assert.Equal(t, nil, err)

			idxNode, err := db.getIndexNode(db.strIndex.idxTree, tt.args.key)
			assert.Equal(t, nil, err)

			// wait for data to expire
			dur = time.Duration(tt.args.expire+0.1) * time.Second
			time.Sleep(dur)

			// delete expired entry
			db.syncDeleteExpireEntry(tt.args.key, tt.args.dataType, idxNode)

			// double check
			_, err = db.getIndexNode(db.strIndex.idxTree, tt.args.key)
			assert.Equal(t, ErrKeyNotFound, err)
		})
	}
}

func TestRoseDB_ayncDeleteExpireEntry(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		testRosedbAyncDeleteExpireEntry(t, FileIO, KeyOnlyMemMode)
	})

	t.Run("mmap", func(t *testing.T) {
		testRosedbAyncDeleteExpireEntry(t, MMap, KeyOnlyMemMode)
	})

	t.Run("key-val-mem-mode", func(t *testing.T) {
		testRosedbAyncDeleteExpireEntry(t, FileIO, KeyValueMemMode)
	})
}

func testRosedbAyncDeleteExpireEntry(t *testing.T, ioType IOType, mode DataIndexMode) {
	path := filepath.Join("/tmp", "rosedb")
	opts := DefaultOptions(path)
	opts.IoType = ioType
	opts.IndexMode = mode
	db, err := Open(opts)
	assert.Nil(t, err)
	defer destroyDB(db)

	type args struct {
		key      []byte
		value    []byte
		expire   float32
		dataType DataType
	}

	tests := []struct {
		name string
		args args
	}{
		{
			"async expire 0.4", args{key: []byte("k1"), value: []byte("v1"), expire: 0.4, dataType: String},
		},
		{
			"async expire 0.5", args{key: []byte("k2"), value: []byte("v2"), expire: 0.5, dataType: String},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// set kv with ttl
			dur := time.Duration(tt.args.expire) * time.Second
			err := db.SetEX(tt.args.key, tt.args.value, dur)
			assert.Equal(t, nil, err)

			// wait for the entry to expire
			dur = time.Duration(tt.args.expire+0.1) * time.Second
			time.Sleep(dur)

			// check if the data is expired，if expired, async delete expired entry.
			_, err = db.Get(tt.args.key)
			assert.Equal(t, ErrKeyNotFound, err)

			// wait for passiveExpireHandler to delete the entry.
			time.Sleep(100 * time.Millisecond)

			// the key is deleted by passiveExpireHandler, so can't get val from str radix tree.
			_, err = db.getIndexNode(db.strIndex.idxTree, tt.args.key)
			assert.Equal(t, ErrKeyNotFound, err)
		})
	}
}
