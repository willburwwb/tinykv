package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).

	// 这里 req.Version 就是 startTimeStamp
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	txn := mvcc.NewMvccTxn(reader, req.Version)

	// 1. 对 key 上锁, 记得 release
	server.Latches.AcquireLatches([][]byte{req.Key})
	defer server.Latches.ReleaseLatches([][]byte{req.Key})

	// 2. 查看是否有 lock 的 ts < txn.Ts, 如果存在就 abort
	lock, err := txn.GetLock(req.GetKey())
	if err != nil {
		return nil, err
	}
	if lock != nil && lock.Ts < txn.StartTS {
		resp := &kvrpcpb.GetResponse{
			Error: &kvrpcpb.KeyError{
				Locked: &kvrpcpb.LockInfo{
					PrimaryLock: lock.Primary,
					LockVersion: lock.Ts,
					LockTtl:     lock.Ttl,
					Key:         req.GetKey(),
				},
			},
		}
		return resp, nil
	}

	// 3. 直接调用 txn 的 GetValue 查找对应 key 的 value
	value, err := txn.GetValue(req.Key)
	if err != nil {
		return nil, err
	}
	if value == nil {
		resp := &kvrpcpb.GetResponse{
			NotFound: true,
		}
		return resp, nil
	}

	resp := &kvrpcpb.GetResponse{
		Value: value,
	}
	return resp, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)

	keys := [][]byte{}
	for _, mutation := range req.Mutations {
		keys = append(keys, mutation.Key)
	}

	// 1. 给所有的 preWrite 的 Key 都加锁
	server.Latches.AcquireLatches(keys)
	defer server.Latches.ReleaseLatches(keys)

	resp := &kvrpcpb.PrewriteResponse{
		Errors: []*kvrpcpb.KeyError{},
	}

	// 2. 检查该 key 最近的 Write (也就是 Commit ),看其 commitTs 是否大于 txn.Ts, 如果大于可能存在写冲突
	// 3. 检查该 key 是否存在 Lock, 有 lock 也回退
	for _, mutation := range req.Mutations {
		_, u, err := txn.MostRecentWrite(mutation.Key)
		if err != nil {
			return nil, err
		}

		// 	假如 commitTs 大于 txn.Ts, 可能存在写冲突,
		if u != 0 && u > txn.StartTS {
			resp.Errors = append(resp.Errors, &kvrpcpb.KeyError{
				Conflict: &kvrpcpb.WriteConflict{
					StartTs:    txn.StartTS,
					ConflictTs: u,
					Key:        mutation.Key,
					Primary:    req.PrimaryLock,
				},
			})
			continue
		}

		lock, err := txn.GetLock(mutation.Key)
		if err != nil {
			return nil, err
		}

		// 假如该 key 存在锁, 可能存在写冲突,
		if lock != nil {
			resp.Errors = append(resp.Errors, &kvrpcpb.KeyError{
				Conflict: &kvrpcpb.WriteConflict{
					StartTs:    txn.StartTS,
					ConflictTs: u,
					Key:        mutation.Key,
					Primary:    req.PrimaryLock,
				},
			})
			continue
		}

	}

	if len(resp.Errors) != 0 {
		return resp, nil
	}

	for _, mutation := range req.Mutations {
		switch mutation.Op {
		case kvrpcpb.Op_Put:
			txn.PutValue(mutation.Key, mutation.Value)
		case kvrpcpb.Op_Del:
			txn.DeleteValue(mutation.Key)
		}
		txn.PutLock(mutation.Key, &mvcc.Lock{
			Primary: req.PrimaryLock,
			Ts:      txn.StartTS,
			Ttl:     req.LockTtl,
			Kind:    mvcc.WriteKindPut,
		})
	}

	// 记得要写进存储里面
	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)

	// 1. 给所有的 commit 的 Key 都加锁
	server.Latches.AcquireLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)

	resp := &kvrpcpb.CommitResponse{
		Error: nil,
	}

	// 2. 假如 key 对应的锁不存在了, 就abort
	for _, key := range req.Keys {
		lock, err := txn.GetLock(key)
		if err != nil {
			return nil, err
		}
		// 当锁不存在时，检查是否回滚
		if lock == nil {
			write, _, err := txn.CurrentWrite(key)
			if err != nil {
				return nil, err
			}

			if write != nil && write.Kind == mvcc.WriteKindRollback {
				resp.Error = &kvrpcpb.KeyError{
					Retryable: "true", //这里设置 retry ?
				}
				return resp, nil
			}
			continue
		}
		if lock.Ts != txn.StartTS {
			resp.Error = &kvrpcpb.KeyError{
				Retryable: "true", //这里设置 retry ?
			}
			return resp, nil
		}
	}

	for _, key := range req.Keys {
		write, _, err := txn.CurrentWrite(key)
		if err != nil {
			return nil, err
		}
		if write != nil {
			continue
		}

		txn.PutWrite(key, req.CommitVersion, &mvcc.Write{
			StartTS: req.StartVersion,
			Kind:    mvcc.WriteKindPut,
		})
		txn.DeleteLock(key)
	}

	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}

	resp := &kvrpcpb.ScanResponse{
		Pairs: []*kvrpcpb.KvPair{},
	}

	txn := mvcc.NewMvccTxn(reader, req.Version)
	scanner := mvcc.NewScanner(req.StartKey, txn)
	defer scanner.Close()
	if scanner == nil {
		return resp, nil
	}

	var pairs []*kvrpcpb.KvPair

	for i := uint32(1); i <= req.Limit; i++ {
		if !scanner.Iter().Valid() {
			break
		}

		key, value, err := scanner.Next()
		if err != nil {
			return nil, err
		}
		if key == nil {
			break
		}

		// 还是跟 get value 时一样，如果存在 lock.Ts < startTs resp返回error
		lock, err := txn.GetLock(key)
		if err != nil {
			return nil, err
		}
		if lock != nil && lock.Ts < txn.StartTS {
			pairs = append(pairs, &kvrpcpb.KvPair{
				Error: &kvrpcpb.KeyError{
					Locked: &kvrpcpb.LockInfo{
						PrimaryLock: lock.Primary,
						LockVersion: lock.Ts,
						Key:         key,
						LockTtl:     lock.Ttl,
					},
				},
			})
			continue
		}

		pairs = append(pairs, &kvrpcpb.KvPair{
			Key:   key,
			Value: value,
		})
	}

	resp = &kvrpcpb.ScanResponse{
		Pairs: pairs,
	}

	return resp, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}

	txn := mvcc.NewMvccTxn(reader, req.LockTs)
	write, commitTs, err := txn.CurrentWrite(req.PrimaryKey)
	if err != nil {
		return nil, err
	}
	lock, err := txn.GetLock(req.PrimaryKey)

	resp := &kvrpcpb.CheckTxnStatusResponse{}
	// 获取该事务的 PrimaryKey 对应的 Write，如果存在并且不是回滚类型，表明 PrimaryKey 已经提交, 如果存在回滚类型，表明已经回滚
	if write != nil {
		if write.Kind != mvcc.WriteKindRollback {
			resp.CommitVersion = commitTs
			resp.Action = kvrpcpb.Action_NoAction
			return resp, nil
		} else {
			resp.Action = kvrpcpb.Action_NoAction
			return resp, nil
		}
	}

	// 假如 PrimaryKey 没有锁，那么回滚 PrimaryKey，即加入 Rollback Write
	if lock == nil {
		write := &mvcc.Write{
			StartTS: req.LockTs,
			Kind:    mvcc.WriteKindRollback,
		}
		txn.PutWrite(req.PrimaryKey, req.LockTs, write)
		err := server.storage.Write(req.Context, txn.Writes())
		if err != nil {
			return nil, err
		}

		resp.Action = kvrpcpb.Action_LockNotExistRollback
		return resp, nil
	}

	// 假如锁存在，看下锁是否超时
	currTime := mvcc.PhysicalTime(req.CurrentTs)
	lockStartTime := mvcc.PhysicalTime(lock.Ts)

	if currTime > lockStartTime+lock.Ttl {
		resp.Action = kvrpcpb.Action_TTLExpireRollback
		resp.LockTtl = lock.Ttl
		// 锁超时后，将锁删除，并加入 RollBack 类型的 Write
		write := &mvcc.Write{
			StartTS: req.LockTs,
			Kind:    mvcc.WriteKindRollback,
		}
		txn.PutWrite(req.PrimaryKey, req.LockTs, write)
		txn.DeleteLock(req.PrimaryKey)
		txn.DeleteValue(req.PrimaryKey)

		err := server.storage.Write(req.Context, txn.Writes())
		if err != nil {
			return nil, err
		}
	} else {
		// 未超时，直接返回
		resp.Action = kvrpcpb.Action_NoAction
	}

	return resp, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}

	txn := mvcc.NewMvccTxn(reader, req.StartVersion)

	resp := &kvrpcpb.BatchRollbackResponse{}

	// 检查 keys 中是否有已经提交的 key, 如果有就不能回滚
	for _, key := range req.Keys {
		write, _, err := txn.CurrentWrite(key)
		if err != nil {
			return nil, err
		}
		if write != nil && write.Kind != mvcc.WriteKindRollback {
			// 证明已经提交不能回滚
			resp.Error = &kvrpcpb.KeyError{
				Abort: "true",
			}
			return resp, nil
		}
	}

	for _, key := range req.Keys {
		write, _, err := txn.CurrentWrite(key)
		if err != nil {
			return nil, err
		}

		// 已经回滚了
		if write != nil && write.Kind == mvcc.WriteKindRollback {
			continue
		}

		lock, err := txn.GetLock(key)
		if err != nil {
			return nil, err
		}

		// 其他事务给这个 key 上的锁，也要写入 RollbackWrite
		if lock != nil && lock.Ts != txn.StartTS {
			rollBackWrite := &mvcc.Write{
				StartTS: req.StartVersion,
				Kind:    mvcc.WriteKindRollback,
			}
			txn.PutWrite(key, req.StartVersion, rollBackWrite)
		} else {
			// 即使没有 prewrite 也就是 lock 为空的时候也要 插入 rollBack
			txn.DeleteLock(key)
			txn.DeleteValue(key)
			rollBackWrite := &mvcc.Write{
				StartTS: req.StartVersion,
				Kind:    mvcc.WriteKindRollback,
			}
			txn.PutWrite(key, req.StartVersion, rollBackWrite)
		}

		err = server.storage.Write(req.Context, txn.Writes())
		if err != nil {
			return nil, err
		}
	}
	return resp, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}

	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	iter := reader.IterCF(engine_util.CfLock)
	var keys [][]byte
	// 遍历所有的 lock，找到其中 lock.Ts == txn.StartTs
	for iter.Valid() {
		key := iter.Item().Key()
		value, err := iter.Item().Value()
		if err != nil {
			return nil, err
		}
		lock, err := mvcc.ParseLock(value)
		if err != nil {
			return nil, err
		}

		if lock.Ts == txn.StartTS {
			keys = append(keys, key)
		}
		iter.Next()
	}

	resp := &kvrpcpb.ResolveLockResponse{}

	if req.CommitVersion != 0 {
		commit, err := server.KvCommit(context.Background(), &kvrpcpb.CommitRequest{
			Context:       req.Context,
			StartVersion:  req.StartVersion,
			Keys:          keys,
			CommitVersion: req.CommitVersion,
		})
		if err != nil {
			return nil, err
		}
		resp.Error = commit.Error
		resp.RegionError = commit.RegionError
	} else {
		rollback, err := server.KvBatchRollback(context.Background(), &kvrpcpb.BatchRollbackRequest{
			Context:      req.Context,
			StartVersion: req.StartVersion,
			Keys:         keys,
		})
		if err != nil {
			return nil, err
		}
		resp.Error = rollback.Error
		resp.RegionError = rollback.RegionError
	}
	return resp, nil
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}
