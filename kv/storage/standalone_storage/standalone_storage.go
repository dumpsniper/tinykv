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
	*badger.DB
	opts badger.Options
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	opts := badger.DefaultOptions
	opts.Dir  conf.DBPath
	opts.ValueDir = conf.DBPath

	return &StandAloneStorage{
		opts: opts,
	}
}

func (s *StandAloneStorage) Start() error {
	db, err := badger.Open(s.opts)
	if err == nil {
		s.DB = db
	}
	return err
}

func (s *StandAloneStorage) Stop() error {
	return s.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	return &standAloneReader{txn: s.NewTransaction(false)}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	var err error
	for _, m := range batch {
		switch data := m.Data.(type) {
		case storage.Put:
			err = engine_util.PutCF(s.DB, data.Cf, data.Key, data.Value)
		case storage.Delete:
			err = engine_util.DeleteCF(s.DB, data.Cf, data.Key)
		}
	}
	return err
}

type standAloneReader struct {
	txn *badger.Txn
}

func (sr *standAloneReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, _ := engine_util.GetCFFromTxn(sr.txn, cf, key)
	return val, nil
}

func (sr *standAloneReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, sr.txn)
}

func (sr *standAloneReader) Close() {
	sr.txn.Discard()
}
