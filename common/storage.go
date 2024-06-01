package common

import (
	"fmt"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/errors"
	"github.com/vmihailenco/msgpack"
	"go-mysql-elasticsearch/config"
	"go.etcd.io/bbolt"
	"path"
	"strings"
)

type PositionStorage interface {
	InitPosition() error
	Save(pos mysql.Position) error
	Get() (mysql.Position, error)
}

type BoltPositionStorage struct {
	Name string
	Pos  uint32
}

var (
	positionBucket = []byte("Position")
	positionId     = Uint64ToBytes(uint64(1))
	boltClient     *bbolt.DB
)

func NewPositionStorage(storageCfg config.StorageConfig) (boltPosStore PositionStorage, err error) {
	var filepath string
	boltPosStore = &BoltPositionStorage{}
	if !strings.HasSuffix(storageCfg.FilePath, ".db") {
		err = errors.New(fmt.Sprintf("storage file must has '.db' suffix"))
		return
	}
	// 如果是绝对路径
	if strings.HasPrefix(storageCfg.FilePath, "/") {
		filepath = storageCfg.FilePath
	} else {
		// 相对路径
		filepath = path.Join(config.Config.ExecPath, storageCfg.FilePath)
	}
	if !CreateFile(filepath) {
		err = errors.New(fmt.Sprintf("create bolt file: %s failed", filepath))
		return
	}
	if boltClient, err = bbolt.Open(filepath, 0666, bbolt.DefaultOptions); err != nil {
		return
	}
	// 创建 bucket
	err = boltClient.Update(func(tx *bbolt.Tx) error {
		_, err = tx.CreateBucketIfNotExists(positionBucket)
		return nil
	})
	return
}

func (boltPos *BoltPositionStorage) InitPosition() (err error) {
	var (
		bytes []byte
		bkt   *bbolt.Bucket
	)
	// 指定 position
	err = boltClient.Update(func(tx *bbolt.Tx) error {
		bkt = tx.Bucket(positionBucket)
		if bytes = bkt.Get(positionId); bytes != nil {
			return nil
		}
		if bytes, err = msgpack.Marshal(mysql.Position{}); err != nil {
			return err
		}
		return bkt.Put(positionId, bytes)
	})
	return
}

func CloseStorage() {
	if boltClient != nil {
		_ = boltClient.Close()
	}
}

func (boltPos *BoltPositionStorage) Save(pos mysql.Position) (err error) {
	var (
		bytes []byte
		bkt   *bbolt.Bucket
	)
	return boltClient.Update(func(tx *bbolt.Tx) (err error) {
		bkt = tx.Bucket(positionBucket)
		if bytes, err = msgpack.Marshal(pos); err != nil {
			return err
		}
		return bkt.Put(positionId, bytes)
	})
}

func (boltPos *BoltPositionStorage) Get() (pos mysql.Position, err error) {
	var (
		bytes []byte
		bkt   *bbolt.Bucket
	)
	err = boltClient.View(func(tx *bbolt.Tx) error {
		bkt = tx.Bucket(positionBucket)
		if bytes = bkt.Get(positionId); bytes == nil {
			err = errors.New("position storage not found")
			return err
		}
		return msgpack.Unmarshal(bytes, &pos)
	})
	return
}
