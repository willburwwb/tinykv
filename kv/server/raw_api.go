package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	cf := req.GetCf()
	key := req.GetKey()
	reader, err := server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}
	val, err := reader.GetCF(cf, key)
	if err != nil {
		return nil, err
	}
	resp := &kvrpcpb.RawGetResponse{}
	if val != nil {
		resp.Value = val
	} else {
		resp.NotFound = true
	}

	return resp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Hint: Consider using Storage.Modify to store data to be modified
	cf := req.GetCf()
	key := req.GetKey()
	val := req.GetValue()

	var modify []storage.Modify
	modify = append(modify, storage.Modify{
		Data: storage.Put{
			Cf:    cf,
			Key:   key,
			Value: val,
		},
	})

	err := server.storage.Write(nil, modify)
	resp := &kvrpcpb.RawPutResponse{}
	if err != nil {
		resp.Error = err.Error()
	}
	return resp, err
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Hint: Consider using Storage.Modify to store data to be deleted
	cf := req.GetCf()
	key := req.GetKey()

	var modify []storage.Modify
	modify = append(modify, storage.Modify{
		Data: storage.Delete{
			Cf:  cf,
			Key: key,
		},
	})

	err := server.storage.Write(nil, modify)
	resp := &kvrpcpb.RawDeleteResponse{}
	if err != nil {
		resp.Error = err.Error()
	}
	return resp, err
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Hint: Consider using reader.IterCF
	startKey := req.GetStartKey()
	limit := req.GetLimit()
	cf := req.GetCf()

	reader, err := server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}

	resp := &kvrpcpb.RawScanResponse{}

	iter := reader.IterCF(cf)
	iter.Seek(startKey)

	for i := uint32(0); i < limit; i++ {
		if !iter.Valid() {
			iter.Close()
			return resp, nil
		}
		item := iter.Item()
		key := item.Key()
		val, err := item.Value()
		if err != nil {
			iter.Close()
			return nil, err
		}
		pair := &kvrpcpb.KvPair{
			Key:   key,
			Value: val,
		}
		resp.Kvs = append(resp.Kvs, pair)
		iter.Next()
	}

	return resp, nil
}
