package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	db   *badger.DB
	conf *config.Config
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	return &StandAloneStorage{nil, conf}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	opts := badger.DefaultOptions
	opts.Dir = "/tmp/badger"
	opts.ValueDir = "/tmp/badger_log"

	db, err := badger.Open(opts)
	s.db = db
	return err
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return s.db.Close()
}

// StorageReader supports key/value's point get and scan operations on a snapshot.
func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return &StandaloneStorageReader{s.db.NewTransaction(false)}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	var err error
	for _, single := range batch {
		switch data := single.Data.(type) {
		case storage.Put:
			err = engine_util.PutCF(s.db, data.Cf, data.Key, data.Value)
		case storage.Delete:
			err = engine_util.DeleteCF(s.db, data.Cf, data.Key)
		}
		if err != nil {
			return err
		}
	}
	return err
}

type StandaloneStorageReader struct {
	txn *badger.Txn
}

func (reader *StandaloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	return engine_util.GetCFFromTxn(reader.txn, cf, key)
}

func (reader *StandaloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	// 返回一个cf prefix的iterators
	return engine_util.NewCFIterator(cf, reader.txn)
}

func (reader *StandaloneStorageReader) Close() {
	reader.txn.Discard()
}
