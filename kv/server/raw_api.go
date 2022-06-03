package server

import (
	"context"
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	reader, _ := server.storage.Reader(nil)
	defer reader.Close()

	resp := kvrpcpb.RawGetResponse{}
	value, err := reader.GetCF(req.GetCf(), req.GetKey())
	resp.Value = value
	if err == badger.ErrKeyNotFound {
		resp.NotFound = true
		err = nil
	}
	return &resp, err
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	var err = server.storage.Write(nil, []storage.Modify{
		{
			Data: storage.Put{
				Key:   req.GetKey(),
				Value: req.GetValue(),
				Cf:    req.GetCf(),
			},
		},
	})
	return nil, err
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	var err = server.storage.Write(nil, []storage.Modify{
		{
			Data: storage.Delete{
				Key: req.GetKey(),
				Cf:  req.GetCf(),
			},
		},
	})
	return nil, err
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	resp := &kvrpcpb.RawScanResponse{}
	resp.Kvs = make([]*kvrpcpb.KvPair, 0)

	reader, _ := server.storage.Reader(nil)
	defer reader.Close()
	iter := reader.IterCF(req.GetCf())
	defer iter.Close()
	iter.Seek(req.GetStartKey())
	for i := uint32(0); iter.Valid() && i < req.GetLimit(); i++ {
		var item = iter.Item()
		value, err := item.Value()
		if err != nil {
			return resp, err
		}
		resp.Kvs = append(resp.Kvs, &kvrpcpb.KvPair{Key: item.Key(), Value: value})
		iter.Next()
	}

	return resp, nil
}
