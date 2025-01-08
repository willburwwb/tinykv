package raftstore

import (
	"fmt"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"reflect"
	"time"

	"github.com/Connor1996/badger/y"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/btree"
	"github.com/pingcap/errors"
)

type PeerTick int

const (
	PeerTickRaft               PeerTick = 0
	PeerTickRaftLogGC          PeerTick = 1
	PeerTickSplitRegionCheck   PeerTick = 2
	PeerTickSchedulerHeartbeat PeerTick = 3
)

type peerMsgHandler struct {
	*peer
	ctx *GlobalContext
}

func newPeerMsgHandler(peer *peer, ctx *GlobalContext) *peerMsgHandler {
	return &peerMsgHandler{
		peer: peer,
		ctx:  ctx,
	}
}

func (d *peerMsgHandler) HandleRaftReady() {
	if d.stopped {
		return
	}
	// Your Code Here (2B).

	if !d.RaftGroup.HasReady() {
		return
	}

	ready := d.RaftGroup.Ready()

	// 1. 将 ready 中需要持久化的内容持久化 unstable entries, hard state
	applySnapResult, err := d.peerStorage.SaveReadyState(&ready)
	if err != nil {
		log.Panic(err)
	}

	if applySnapResult != nil {
		if !reflect.DeepEqual(applySnapResult.PrevRegion, applySnapResult.Region) {
			d.peerStorage.SetRegion(applySnapResult.Region)
			d.ctx.storeMeta.Lock()
			d.ctx.storeMeta.regions[applySnapResult.Region.Id] = applySnapResult.Region
			d.ctx.storeMeta.regionRanges.Delete(&regionItem{region: applySnapResult.PrevRegion})
			d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: applySnapResult.Region})
			d.ctx.storeMeta.Unlock()
		}
	}

	// 2. 将ready中的msg发送出去
	d.Send(d.ctx.trans, ready.Messages)

	// 3. apply ready 中的 committedEntries
	for _, entry := range ready.CommittedEntries {
		kvWB := &engine_util.WriteBatch{}
		raftWB := &engine_util.WriteBatch{}
		if entry.EntryType == eraftpb.EntryType_EntryNormal {
			d.applyEntry(&entry, kvWB, raftWB)
		} else if entry.EntryType == eraftpb.EntryType_EntryConfChange {
			d.applyConfChangeEntry(&entry, kvWB, raftWB)
		}

		// 在 apply 了 delete 操作的 applyConfChangeEntry 后，会销毁节点以及底层存储的数据也被扔掉，但是如果这里没有根据 d.stop return 的话，
		// 会再次向底层存储写入 ApplyStateKey ，测试样例中貌似后续添加节点时会复用原来删掉节点的存储，会出现在初始化添加节点时 lastIndex < appliedIndex，
		// 因为 ApplyState 在节点被删除后依然被更新。
		// panic: [region 1] 2 unexpected raft log index: lastIndex 0 < appliedIndex 7
		if d.stopped {
			return
		}

		d.peerStorage.applyState.AppliedIndex = entry.Index
		err := kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
		if err != nil {
			log.Panic(err)
		}

		err = kvWB.WriteToDB(d.peerStorage.Engines.Kv)
		if err != nil {
			log.Panic(err)
		}

		err = raftWB.WriteToDB(d.peerStorage.Engines.Raft)
		if err != nil {
			log.Panic(err)
		}
	}
	// 4. advance
	d.RaftGroup.Advance(ready)
}

func (d *peerMsgHandler) applyEntry(entry *eraftpb.Entry, kvWB *engine_util.WriteBatch, raftWB *engine_util.WriteBatch) {
	req := new(raft_cmdpb.RaftCmdRequest)
	err := req.Unmarshal(entry.Data)
	if err != nil {
		log.Error(err)
		return
	}

	if req.Header != nil {
		fromEpoch := req.GetHeader().GetRegionEpoch()
		if fromEpoch != nil {
			if util.IsEpochStale(fromEpoch, d.Region().RegionEpoch) {
				resp := ErrResp(&util.ErrEpochNotMatch{})
				d.handleProposal(resp, entry, false)
				return
			}
		}
	}

	//if len(req.Requests) == 0 && req.AdminRequest == nil {
	//	log.Infof("empty request %+v", req)
	//}
	if len(req.Requests) > 0 {
		for _, request := range req.Requests {
			switch request.CmdType {
			case raft_cmdpb.CmdType_Invalid:
			case raft_cmdpb.CmdType_Get:
				d.applyGetEntry(entry, request)
			case raft_cmdpb.CmdType_Delete:
				d.applyDeleteEntry(entry, request, kvWB)
			case raft_cmdpb.CmdType_Put:
				d.applyPutEntry(entry, request, kvWB)
			case raft_cmdpb.CmdType_Snap:
				d.applySnapEntry(entry, request)
			}
		}
	} else if req.AdminRequest != nil {
		switch req.AdminRequest.CmdType {
		case raft_cmdpb.AdminCmdType_CompactLog:
			d.applyCompactEntry(entry, req.AdminRequest, kvWB)
		case raft_cmdpb.AdminCmdType_Split:
			d.applySplitEntry(entry, req, kvWB)
		}
	}
}
func (d *peerMsgHandler) applySplitEntry(entry *eraftpb.Entry, req *raft_cmdpb.RaftCmdRequest, kvWB *engine_util.WriteBatch) {
	if req.Header.RegionId != d.regionId {
		resp := ErrResp(&util.ErrRegionNotFound{RegionId: req.Header.RegionId})
		d.handleProposal(resp, entry, false)
		return
	}
	err := util.CheckRegionEpoch(req, d.Region(), true)
	if err != nil {
		resp := ErrResp(err)
		d.handleProposal(resp, entry, false)
		return
	}
	err = util.CheckKeyInRegion(req.AdminRequest.Split.SplitKey, d.Region())
	if err != nil {
		resp := ErrResp(err)
		d.handleProposal(resp, entry, false)
		return
	}
	if len(d.Region().Peers) != len(req.AdminRequest.Split.NewPeerIds) {
		resp := ErrResp(errors.Errorf("len of newPeers != len of req.Split.NewPeerIds"))
		d.handleProposal(resp, entry, false)
		return
	}

	log.Infof("region %d peer %d begin to split", d.regionId, d.PeerId())

	// 复制新的 region 的 prs
	newPeers := make([]*metapb.Peer, 0)

	for i, pr := range d.Region().Peers {
		newPeers = append(newPeers, &metapb.Peer{
			Id:      req.AdminRequest.Split.NewPeerIds[i],
			StoreId: pr.StoreId,
		})
	}

	newRegion := &metapb.Region{
		Id:       req.AdminRequest.Split.NewRegionId,
		StartKey: req.AdminRequest.Split.SplitKey,
		EndKey:   d.Region().EndKey,
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: 0,
			Version: 0,
		},
		Peers: newPeers,
	}

	// 修改 regionState, 注意加锁
	d.ctx.storeMeta.Lock()
	// 修改原来的 region
	d.Region().RegionEpoch.Version++
	d.Region().EndKey = req.AdminRequest.Split.SplitKey
	// 注意要修改新 region 的 Version, 不是 confVersion
	newRegion.RegionEpoch.Version++

	d.ctx.storeMeta.regions[req.AdminRequest.Split.NewRegionId] = newRegion
	d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{d.Region()})
	d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{newRegion})
	meta.WriteRegionState(kvWB, newRegion, rspb.PeerState_Normal)
	meta.WriteRegionState(kvWB, d.Region(), rspb.PeerState_Normal)

	d.ctx.storeMeta.Unlock()

	// 创建新的节点
	newPeer, err := createPeer(d.storeID(), d.ctx.cfg, d.ctx.regionTaskSender, d.ctx.engine, newRegion)
	if err != nil {
		log.Panic(err)
	}
	newPeer.peerStorage.SetRegion(newRegion)
	d.ctx.router.register(newPeer)
	err = d.ctx.router.send(newRegion.Id, message.Msg{
		RegionID: newRegion.Id,
		Type:     message.MsgTypeStart,
	})
	if err != nil {
		log.Panic(err)
	}

	resp := &raft_cmdpb.RaftCmdResponse{
		Header: &raft_cmdpb.RaftResponseHeader{},
		AdminResponse: &raft_cmdpb.AdminResponse{
			CmdType: raft_cmdpb.AdminCmdType_Split,
			Split: &raft_cmdpb.SplitResponse{
				Regions: []*metapb.Region{d.Region(), newRegion},
			},
		},
	}
	d.handleProposal(resp, entry, false)

	// 刷新 scheduler 的 region 缓存，刷新两次
	d.notifyHeartbeatScheduler(d.Region(), d.peer)
	d.notifyHeartbeatScheduler(newRegion, newPeer)
}

func (d *peerMsgHandler) applyConfChangeEntry(entry *eraftpb.Entry, kvWB *engine_util.WriteBatch, raftWB *engine_util.WriteBatch) {
	cc := new(eraftpb.ConfChange)
	err := cc.Unmarshal(entry.Data)
	if err != nil {
		log.Panic(err)
	}

	// cc.Context 为 RaftCmdRequest 序列化后的内容
	req := new(raft_cmdpb.RaftCmdRequest)
	err = req.Unmarshal(cc.Context)
	if err != nil {
		log.Panic(err)
	}

	switch cc.ChangeType {
	case eraftpb.ConfChangeType_AddNode:
		//	log.Infof("apply conf change entry, add node %+v", req.AdminRequest.ChangePeer.Peer)
		d.applyAddNode(entry, req, cc, kvWB, raftWB)
	case eraftpb.ConfChangeType_RemoveNode:
		//	log.Infof("apply conf change entry, delete node %+v", req.AdminRequest.ChangePeer.Peer)
		d.applyRemoveNode(entry, req, cc, kvWB, raftWB)
	}

	return
}

func (d *peerMsgHandler) applyAddNode(entry *eraftpb.Entry, req *raft_cmdpb.RaftCmdRequest, cc *eraftpb.ConfChange, kvWB *engine_util.WriteBatch, raftWB *engine_util.WriteBatch) {
	if !d.isNodeExist(cc.NodeId) {
		d.ctx.storeMeta.Lock()
		d.Region().RegionEpoch.ConfVer++

		peer := &metapb.Peer{
			Id:      req.AdminRequest.ChangePeer.Peer.Id,
			StoreId: req.AdminRequest.ChangePeer.Peer.StoreId,
		}
		d.Region().Peers = append(d.Region().Peers, peer)

		// 将新的 region 信息写入 kvWB 中
		meta.WriteRegionState(kvWB, d.Region(), rspb.PeerState_Normal) // 这里第三个参数 PeerState_Normal，表示正常响应

		d.insertPeerCache(peer)

		d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: d.Region()})
		d.ctx.storeMeta.Unlock()
		// !!!! 不要忘记 appy ConfChange 来修改 Raft 模块
		d.RaftGroup.ApplyConfChange(*cc)
	}

	resp := &raft_cmdpb.RaftCmdResponse{
		Header: &raft_cmdpb.RaftResponseHeader{},
		AdminResponse: &raft_cmdpb.AdminResponse{
			CmdType: raft_cmdpb.AdminCmdType_ChangePeer,
			ChangePeer: &raft_cmdpb.ChangePeerResponse{
				Region: d.Region(),
			},
		},
	}

	// 同理还是交给 handleProposal 处理回调
	d.handleProposal(resp, entry, false)

	d.notifyHeartbeatScheduler(d.Region(), d.peer)
}

func (d *peerMsgHandler) applyRemoveNode(entry *eraftpb.Entry, req *raft_cmdpb.RaftCmdRequest, cc *eraftpb.ConfChange, kvWB *engine_util.WriteBatch, raftWB *engine_util.WriteBatch) {
	nodeId := req.AdminRequest.ChangePeer.Peer.Id
	// 	如果要移除的是自己，调用 destroyPeer 后直接 return
	if d.peer.RaftGroup.Raft.GetRaftId() == nodeId {
		// !!!!一定要记得删除该节点的 applyState 状态
		//kvnewWB := &engine_util.WriteBatch{}
		//kvnewWB.DeleteMeta(meta.ApplyStateKey(d.regionId))
		//err := kvnewWB.WriteToDB(d.peerStorage.Engines.Kv)
		//if err != nil {
		//	log.Error(err)
		//}
		//raftWB.DeleteMeta(meta.RaftStateKey(d.regionId))
		d.destroyPeer()

	} else if d.isNodeExist(nodeId) {
		d.ctx.storeMeta.Lock()

		d.Region().RegionEpoch.ConfVer++

		// 在 region 中移除 nodeId
		d.removeNodeInRegion(nodeId)

		// 将新的 region 信息写入 kvWB 中
		meta.WriteRegionState(kvWB, d.Region(), rspb.PeerState_Normal) // 这里第三个参数 PeerState_Normal，表示正常响应

		// peerCache 用于 send msg 时获取对应节点的 store id，这里需要删除对应记录
		d.removePeerCache(nodeId)

		d.ctx.storeMeta.Unlock()

		// 修改 raft 模块中的内容，这里就是调用 raft.RemoveNode 方法
		d.RaftGroup.ApplyConfChange(*cc)
	}

	resp := &raft_cmdpb.RaftCmdResponse{
		Header: &raft_cmdpb.RaftResponseHeader{},
		AdminResponse: &raft_cmdpb.AdminResponse{
			CmdType: raft_cmdpb.AdminCmdType_ChangePeer,
			ChangePeer: &raft_cmdpb.ChangePeerResponse{
				Region: d.Region(),
			},
		},
	}

	// 同理还是交给 handleProposal 处理回调
	d.handleProposal(resp, entry, false)

	d.notifyHeartbeatScheduler(d.Region(), d.peer)
}

func (d *peerMsgHandler) notifyHeartbeatScheduler(region *metapb.Region, peer *peer) {
	cloneRegion := &metapb.Region{}

	err := util.CloneMsg(region, cloneRegion)
	if err != nil {
		return
	}

	d.ctx.schedulerTaskSender <- &runner.SchedulerRegionHeartbeatTask{
		Region:          cloneRegion,
		Peer:            peer.Meta,
		PendingPeers:    peer.CollectPendingPeers(),
		ApproximateSize: peer.ApproximateSize,
	}
}

func (d *peerMsgHandler) applyGetEntry(entry *eraftpb.Entry, req *raft_cmdpb.Request) {
	value, _ := engine_util.GetCF(d.peerStorage.Engines.Kv, req.Get.GetCf(), req.Get.GetKey())

	resp := &raft_cmdpb.RaftCmdResponse{
		Header: &raft_cmdpb.RaftResponseHeader{},
		Responses: []*raft_cmdpb.Response{
			{
				CmdType: raft_cmdpb.CmdType_Get,
				Get: &raft_cmdpb.GetResponse{
					Value: value,
				},
			},
		},
	}

	d.handleProposal(resp, entry, false)
	return
}

func (d *peerMsgHandler) applyDeleteEntry(entry *eraftpb.Entry, req *raft_cmdpb.Request, kvWB *engine_util.WriteBatch) {
	kvWB.DeleteCF(req.Delete.Cf, req.Delete.Key)

	resp := &raft_cmdpb.RaftCmdResponse{
		Header: &raft_cmdpb.RaftResponseHeader{},
		Responses: []*raft_cmdpb.Response{
			{
				CmdType: raft_cmdpb.CmdType_Delete,
				Delete:  &raft_cmdpb.DeleteResponse{},
			},
		},
	}

	d.handleProposal(resp, entry, false)
	return
}
func (d *peerMsgHandler) applyPutEntry(entry *eraftpb.Entry, req *raft_cmdpb.Request, kvWB *engine_util.WriteBatch) {
	kvWB.SetCF(req.Put.Cf, req.Put.Key, req.Put.Value)
	// log.Infof("put %v %v %v", req.Put.Cf, string(req.Put.Key), string(req.Put.Value))
	resp := &raft_cmdpb.RaftCmdResponse{
		Header: &raft_cmdpb.RaftResponseHeader{},
		Responses: []*raft_cmdpb.Response{
			{
				CmdType: raft_cmdpb.CmdType_Put,
				Put:     &raft_cmdpb.PutResponse{},
			},
		},
	}

	d.handleProposal(resp, entry, false)
	return
}

func (d *peerMsgHandler) applySnapEntry(entry *eraftpb.Entry, req *raft_cmdpb.Request) {
	resp := &raft_cmdpb.RaftCmdResponse{
		Header: &raft_cmdpb.RaftResponseHeader{},
		Responses: []*raft_cmdpb.Response{
			{
				CmdType: raft_cmdpb.CmdType_Snap,
				Snap: &raft_cmdpb.SnapResponse{
					Region: d.Region(),
				},
			},
		},
	}
	d.handleProposal(resp, entry, true)
}

func (d *peerMsgHandler) applyCompactEntry(entry *eraftpb.Entry, req *raft_cmdpb.AdminRequest, kvWB *engine_util.WriteBatch) {
	compactIndex := req.CompactLog.CompactIndex
	compactTerm := req.CompactLog.CompactTerm
	if compactIndex > d.peerStorage.applyState.TruncatedState.Index {
		d.peerStorage.applyState.TruncatedState.Index = compactIndex
		d.peerStorage.applyState.TruncatedState.Term = compactTerm

		err := kvWB.SetMeta(meta.ApplyStateKey(d.Region().GetId()), d.peerStorage.applyState)
		if err != nil {
			log.Panic(err)
		}

		d.ScheduleCompactLog(compactIndex)
	}
}

func (d *peerMsgHandler) handleProposal(resp *raft_cmdpb.RaftCmdResponse, entry *eraftpb.Entry, isApplySnap bool) {
	// Your Code Here (2B).
	proposal := d.findProposal(entry.Index, entry.Term)
	if proposal == nil {
		//err := fmt.Errorf("node %v find proposal is nil, %+v", d.RaftGroup.Raft.GetRaftId(), entry)
		//log.Error(err)
		return
	}
	// log.Infof("node %v return resp %+v", d.RaftGroup.Raft.GetRaftId(), resp)

	if isApplySnap {
		proposal.cb.Txn = d.peerStorage.Engines.Kv.NewTransaction(false)
	}
	// 构造响应
	proposal.cb.Done(resp)
}

func (d *peerMsgHandler) findProposal(index uint64, term uint64) *proposal {
	for len(d.proposals) != 0 {
		firstProposal := d.proposals[0]
		d.proposals = d.proposals[1:]
		if firstProposal.index < index {
			NotifyStaleReq(d.Term(), firstProposal.cb)
		} else if firstProposal.index == index {
			if firstProposal.term == term {
				return firstProposal
			} else {
				return nil
			}
		} else {
			break
		}
	}
	return nil
}

func (d *peerMsgHandler) HandleMsg(msg message.Msg) {
	switch msg.Type {
	case message.MsgTypeRaftMessage:
		raftMsg := msg.Data.(*rspb.RaftMessage)
		if err := d.onRaftMsg(raftMsg); err != nil {
			log.Errorf("%s handle raft message error %v", d.Tag, err)
		}
	case message.MsgTypeRaftCmd:
		raftCMD := msg.Data.(*message.MsgRaftCmd)
		d.proposeRaftCommand(raftCMD.Request, raftCMD.Callback)
	case message.MsgTypeTick:
		d.onTick()
	case message.MsgTypeSplitRegion:
		split := msg.Data.(*message.MsgSplitRegion)
		log.Infof("%s on split with %v", d.Tag, split.SplitKey)
		d.onPrepareSplitRegion(split.RegionEpoch, split.SplitKey, split.Callback)
	case message.MsgTypeRegionApproximateSize:
		d.onApproximateRegionSize(msg.Data.(uint64))
	case message.MsgTypeGcSnap:
		gcSnap := msg.Data.(*message.MsgGCSnap)
		d.onGCSnap(gcSnap.Snaps)
	case message.MsgTypeStart:
		d.startTicker()
	}
}

func (d *peerMsgHandler) preProposeRaftCommand(req *raft_cmdpb.RaftCmdRequest) error {
	// Check store_id, make sure that the msg is dispatched to the right place.
	if err := util.CheckStoreID(req, d.storeID()); err != nil {
		return err
	}

	// Check whether the store has the right peer to handle the request.
	regionID := d.regionId
	leaderID := d.LeaderId()
	if !d.IsLeader() {
		leader := d.getPeerFromCache(leaderID)
		return &util.ErrNotLeader{RegionId: regionID, Leader: leader}
	}
	// peer_id must be the same as peer's.
	if err := util.CheckPeerID(req, d.PeerId()); err != nil {
		return err
	}
	// Check whether the term is stale.
	if err := util.CheckTerm(req, d.Term()); err != nil {
		return err
	}
	err := util.CheckRegionEpoch(req, d.Region(), true)
	if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
		// Attach the region which might be split from the current region. But it doesn't
		// matter if the region is not split from the current region. If the region meta
		// received by the TiKV driver is newer than the meta cached in the driver, the meta is
		// updated.
		siblingRegion := d.findSiblingRegion()
		if siblingRegion != nil {
			errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
		}
		return errEpochNotMatching
	}
	return err
}

func (d *peerMsgHandler) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	err := d.preProposeRaftCommand(msg)
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}

	// Your Code Here (2B).
	if len(msg.Requests) != 0 {
		// 1. 将 msg 序列化
		data, err := msg.Marshal()
		if err != nil {
			log.Panic(err)
		}

		for _, v := range msg.Requests {
			var key []byte
			if v.CmdType == raft_cmdpb.CmdType_Get {
				key = v.Get.Key
			} else if v.CmdType == raft_cmdpb.CmdType_Put {
				key = v.Put.Key
			} else if v.CmdType == raft_cmdpb.CmdType_Delete {
				key = v.Delete.Key
			} else {
				continue
			}
			if err := util.CheckKeyInRegion(key, d.Region()); err != nil {
				cb.Done(ErrResp(err))
				return
			}
		}

		// 2. 设置该 request 的回调
		d.appendProposal(d.nextProposalIndex(), d.Term(), cb)
		// 3. 将该 request 提交到 raft group 中

		// 		log.Infof("node %d propose msg %+v", d.RaftGroup.Raft.GetRaftId(), msg.Requests)
		err = d.RaftGroup.Propose(data)
		if err != nil {
			cb.Done(ErrResp(err))
			return
		}
	} else {
		//log.Infof("handle admin msg %+v", msg)

		switch msg.AdminRequest.CmdType {
		case raft_cmdpb.AdminCmdType_CompactLog:
			data, err := msg.Marshal()
			if err != nil {
				log.Panic(err)
			}
			err = d.RaftGroup.Propose(data)
			if err != nil {
				log.Panic(err)
			}
		case raft_cmdpb.AdminCmdType_TransferLeader:
			// TransferLeader 不需要将该 cmd 同步到其他节点，直接应用即可
			d.RaftGroup.TransferLeader(msg.AdminRequest.TransferLeader.Peer.Id)

			// 所以这里当场就回复消息，因为不需要同步，所以不用等到 handleReady 的时候再处理
			resp := &raft_cmdpb.RaftCmdResponse{
				Header: &raft_cmdpb.RaftResponseHeader{},
				AdminResponse: &raft_cmdpb.AdminResponse{
					CmdType:        raft_cmdpb.AdminCmdType_TransferLeader,
					TransferLeader: &raft_cmdpb.TransferLeaderResponse{},
				},
			}
			//			log.Infof("node %v return transfer leader to %d", d.RaftGroup.Raft.GetRaftId(), msg.AdminRequest.TransferLeader.Peer.Id)
			cb.Done(resp)

		case raft_cmdpb.AdminCmdType_ChangePeer:
			data, err := msg.Marshal()
			if err != nil {
				log.Panic(err)
			}

			d.appendProposal(d.nextProposalIndex(), d.Term(), cb)

			err = d.RaftGroup.ProposeConfChange(eraftpb.ConfChange{
				ChangeType: msg.AdminRequest.ChangePeer.ChangeType,
				NodeId:     msg.AdminRequest.ChangePeer.Peer.Id,
				Context:    data, // 这里的 Context 为 data
			})

			if err != nil {
				log.Error(err)
				cb.Done(ErrResp(err))
				return
			}

		case raft_cmdpb.AdminCmdType_Split:
			data, err := msg.Marshal()
			if err != nil {
				log.Panic(err)
			}

			err = util.CheckKeyInRegion(msg.AdminRequest.Split.SplitKey, d.Region())
			if err != nil {
				cb.Done(ErrResp(err))
				return
			}

			d.appendProposal(d.nextProposalIndex(), d.Term(), cb)

			err = d.RaftGroup.Propose(data)
			if err != nil {
				log.Panic(err)
			}
		}
	}
}

func (d *peerMsgHandler) appendProposal(index uint64, term uint64, cb *message.Callback) {
	p := &proposal{
		index: index,
		term:  term,
		cb:    cb,
	}
	d.proposals = append(d.proposals, p)
}

func (d *peerMsgHandler) onTick() {
	if d.stopped {
		return
	}
	d.ticker.tickClock()
	if d.ticker.isOnTick(PeerTickRaft) {
		d.onRaftBaseTick()
	}
	if d.ticker.isOnTick(PeerTickRaftLogGC) {
		d.onRaftGCLogTick()
	}
	if d.ticker.isOnTick(PeerTickSchedulerHeartbeat) {
		d.onSchedulerHeartbeatTick()
	}
	if d.ticker.isOnTick(PeerTickSplitRegionCheck) {
		d.onSplitRegionCheckTick()
	}
	d.ctx.tickDriverSender <- d.regionId
}

func (d *peerMsgHandler) startTicker() {
	d.ticker = newTicker(d.regionId, d.ctx.cfg)
	d.ctx.tickDriverSender <- d.regionId
	d.ticker.schedule(PeerTickRaft)
	d.ticker.schedule(PeerTickRaftLogGC)
	d.ticker.schedule(PeerTickSplitRegionCheck)
	d.ticker.schedule(PeerTickSchedulerHeartbeat)
}

func (d *peerMsgHandler) onRaftBaseTick() {
	d.RaftGroup.Tick()
	d.ticker.schedule(PeerTickRaft)
}

func (d *peerMsgHandler) ScheduleCompactLog(truncatedIndex uint64) {
	raftLogGCTask := &runner.RaftLogGCTask{
		RaftEngine: d.ctx.engine.Raft,
		RegionID:   d.regionId,
		StartIdx:   d.LastCompactedIdx,
		EndIdx:     truncatedIndex + 1,
	}
	d.LastCompactedIdx = raftLogGCTask.EndIdx
	d.ctx.raftLogGCTaskSender <- raftLogGCTask
}

func (d *peerMsgHandler) onRaftMsg(msg *rspb.RaftMessage) error {
	log.Debugf("%s handle raft message %s from %d to %d",
		d.Tag, msg.GetMessage().GetMsgType(), msg.GetFromPeer().GetId(), msg.GetToPeer().GetId())
	if !d.validateRaftMessage(msg) {
		return nil
	}
	if d.stopped {
		return nil
	}
	if msg.GetIsTombstone() {
		// we receive a message tells us to remove self.
		d.handleGCPeerMsg(msg)
		return nil
	}
	if d.checkMessage(msg) {
		return nil
	}
	key, err := d.checkSnapshot(msg)
	if err != nil {
		return err
	}
	if key != nil {
		// If the snapshot file is not used again, then it's OK to
		// delete them here. If the snapshot file will be reused when
		// receiving, then it will fail to pass the check again, so
		// missing snapshot files should not be noticed.
		s, err1 := d.ctx.snapMgr.GetSnapshotForApplying(*key)
		if err1 != nil {
			return err1
		}
		d.ctx.snapMgr.DeleteSnapshot(*key, s, false)
		return nil
	}
	d.insertPeerCache(msg.GetFromPeer())
	err = d.RaftGroup.Step(*msg.GetMessage())
	if err != nil {
		return err
	}
	if d.AnyNewPeerCatchUp(msg.FromPeer.Id) {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
	return nil
}

// return false means the message is invalid, and can be ignored.
func (d *peerMsgHandler) validateRaftMessage(msg *rspb.RaftMessage) bool {
	regionID := msg.GetRegionId()
	from := msg.GetFromPeer()
	to := msg.GetToPeer()
	log.Debugf("[region %d] handle raft message %s from %d to %d", regionID, msg, from.GetId(), to.GetId())
	if to.GetStoreId() != d.storeID() {
		log.Warnf("[region %d] store not match, to store id %d, mine %d, ignore it",
			regionID, to.GetStoreId(), d.storeID())
		return false
	}
	if msg.RegionEpoch == nil {
		log.Errorf("[region %d] missing epoch in raft message, ignore it", regionID)
		return false
	}
	return true
}

// / Checks if the message is sent to the correct peer.
// /
// / Returns true means that the message can be dropped silently.
func (d *peerMsgHandler) checkMessage(msg *rspb.RaftMessage) bool {
	fromEpoch := msg.GetRegionEpoch()
	isVoteMsg := util.IsVoteMessage(msg.Message)
	fromStoreID := msg.FromPeer.GetStoreId()

	// Let's consider following cases with three nodes [1, 2, 3] and 1 is leader:
	// a. 1 removes 2, 2 may still send MsgAppendResponse to 1.
	//  We should ignore this stale message and let 2 remove itself after
	//  applying the ConfChange log.
	// b. 2 is isolated, 1 removes 2. When 2 rejoins the cluster, 2 will
	//  send stale MsgRequestVote to 1 and 3, at this time, we should tell 2 to gc itself.
	// c. 2 is isolated but can communicate with 3. 1 removes 3.
	//  2 will send stale MsgRequestVote to 3, 3 should ignore this message.
	// d. 2 is isolated but can communicate with 3. 1 removes 2, then adds 4, remove 3.
	//  2 will send stale MsgRequestVote to 3, 3 should tell 2 to gc itself.
	// e. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader.
	//  After 2 rejoins the cluster, 2 may send stale MsgRequestVote to 1 and 3,
	//  1 and 3 will ignore this message. Later 4 will send messages to 2 and 2 will
	//  rejoin the raft group again.
	// f. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader, and 4 removes 2.
	//  unlike case e, 2 will be stale forever.
	// TODO: for case f, if 2 is stale for a long time, 2 will communicate with scheduler and scheduler will
	// tell 2 is stale, so 2 can remove itself.
	region := d.Region()
	if util.IsEpochStale(fromEpoch, region.RegionEpoch) && util.FindPeer(region, fromStoreID) == nil {
		// The message is stale and not in current region.
		handleStaleMsg(d.ctx.trans, msg, region.RegionEpoch, isVoteMsg)
		return true
	}
	target := msg.GetToPeer()
	if target.Id < d.PeerId() {
		log.Infof("%s target peer ID %d is less than %d, msg maybe stale", d.Tag, target.Id, d.PeerId())
		return true
	} else if target.Id > d.PeerId() {
		if d.MaybeDestroy() {
			log.Infof("%s is stale as received a larger peer %s, destroying", d.Tag, target)
			d.destroyPeer()
			d.ctx.router.sendStore(message.NewMsg(message.MsgTypeStoreRaftMessage, msg))
		}
		return true
	}
	return false
}

func handleStaleMsg(trans Transport, msg *rspb.RaftMessage, curEpoch *metapb.RegionEpoch,
	needGC bool) {
	regionID := msg.RegionId
	fromPeer := msg.FromPeer
	toPeer := msg.ToPeer
	msgType := msg.Message.GetMsgType()

	if !needGC {
		log.Infof("[region %d] raft message %s is stale, current %v ignore it",
			regionID, msgType, curEpoch)
		return
	}
	gcMsg := &rspb.RaftMessage{
		RegionId:    regionID,
		FromPeer:    toPeer,
		ToPeer:      fromPeer,
		RegionEpoch: curEpoch,
		IsTombstone: true,
	}
	if err := trans.Send(gcMsg); err != nil {
		log.Errorf("[region %d] send message failed %v", regionID, err)
	}
}

func (d *peerMsgHandler) handleGCPeerMsg(msg *rspb.RaftMessage) {
	fromEpoch := msg.RegionEpoch
	if !util.IsEpochStale(d.Region().RegionEpoch, fromEpoch) {
		return
	}
	if !util.PeerEqual(d.Meta, msg.ToPeer) {
		log.Infof("%s receive stale gc msg, ignore", d.Tag)
		return
	}
	log.Infof("%s peer %s receives gc message, trying to remove", d.Tag, msg.ToPeer)
	if d.MaybeDestroy() {
		d.destroyPeer()
	}
}

// Returns `None` if the `msg` doesn't contain a snapshot or it contains a snapshot which
// doesn't conflict with any other snapshots or regions. Otherwise a `snap.SnapKey` is returned.
func (d *peerMsgHandler) checkSnapshot(msg *rspb.RaftMessage) (*snap.SnapKey, error) {
	if msg.Message.Snapshot == nil {
		return nil, nil
	}
	regionID := msg.RegionId
	snapshot := msg.Message.Snapshot
	key := snap.SnapKeyFromRegionSnap(regionID, snapshot)
	snapData := new(rspb.RaftSnapshotData)
	err := snapData.Unmarshal(snapshot.Data)
	if err != nil {
		return nil, err
	}
	snapRegion := snapData.Region
	peerID := msg.ToPeer.Id
	var contains bool
	for _, peer := range snapRegion.Peers {
		if peer.Id == peerID {
			contains = true
			break
		}
	}
	if !contains {
		log.Infof("%s %s doesn't contains peer %d, skip", d.Tag, snapRegion, peerID)
		return &key, nil
	}
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	if !util.RegionEqual(meta.regions[d.regionId], d.Region()) {
		if !d.isInitialized() {
			log.Infof("%s stale delegate detected, skip", d.Tag)
			return &key, nil
		} else {
			panic(fmt.Sprintf("%s meta corrupted %s != %s", d.Tag, meta.regions[d.regionId], d.Region()))
		}
	}

	existRegions := meta.getOverlapRegions(snapRegion)
	for _, existRegion := range existRegions {
		if existRegion.GetId() == snapRegion.GetId() {
			continue
		}
		log.Infof("%s region overlapped %s %s", d.Tag, existRegion, snapRegion)
		return &key, nil
	}

	// check if snapshot file exists.
	_, err = d.ctx.snapMgr.GetSnapshotForApplying(key)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (d *peerMsgHandler) destroyPeer() {
	log.Infof("%s starts destroy", d.Tag)
	regionID := d.regionId
	// We can't destroy a peer which is applying snapshot.
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	isInitialized := d.isInitialized()
	if err := d.Destroy(d.ctx.engine, false); err != nil {
		// If not panic here, the peer will be recreated in the next restart,
		// then it will be gc again. But if some overlap region is created
		// before restarting, the gc action will delete the overlap region's
		// data too.
		panic(fmt.Sprintf("%s destroy peer %v", d.Tag, err))
	}
	d.ctx.router.close(regionID)
	d.stopped = true
	if isInitialized && meta.regionRanges.Delete(&regionItem{region: d.Region()}) == nil {
		panic(d.Tag + " meta corruption detected")
	}
	if _, ok := meta.regions[regionID]; !ok {
		panic(d.Tag + " meta corruption detected")
	}
	delete(meta.regions, regionID)
}

func (d *peerMsgHandler) findSiblingRegion() (result *metapb.Region) {
	meta := d.ctx.storeMeta
	meta.RLock()
	defer meta.RUnlock()
	item := &regionItem{region: d.Region()}
	meta.regionRanges.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		result = i.(*regionItem).region
		return true
	})
	return
}

func (d *peerMsgHandler) onRaftGCLogTick() {
	d.ticker.schedule(PeerTickRaftLogGC)
	if !d.IsLeader() {
		return
	}

	appliedIdx := d.peerStorage.AppliedIndex()
	firstIdx, _ := d.peerStorage.FirstIndex()
	var compactIdx uint64
	if appliedIdx > firstIdx && appliedIdx-firstIdx >= d.ctx.cfg.RaftLogGcCountLimit {
		compactIdx = appliedIdx
	} else {
		return
	}

	y.Assert(compactIdx > 0)
	compactIdx -= 1
	if compactIdx < firstIdx {
		// In case compact_idx == first_idx before subtraction.
		return
	}

	term, err := d.RaftGroup.Raft.RaftLog.Term(compactIdx)
	if err != nil {
		log.Fatalf("appliedIdx: %d, firstIdx: %d, compactIdx: %d", appliedIdx, firstIdx, compactIdx)
		panic(err)
	}

	// Create a compact log request and notify directly.
	regionID := d.regionId
	request := newCompactLogRequest(regionID, d.Meta, compactIdx, term)
	d.proposeRaftCommand(request, nil)
}

func (d *peerMsgHandler) onSplitRegionCheckTick() {
	d.ticker.schedule(PeerTickSplitRegionCheck)
	// To avoid frequent scan, we only add new scan tasks if all previous tasks
	// have finished.
	if len(d.ctx.splitCheckTaskSender) > 0 {
		return
	}

	if !d.IsLeader() {
		return
	}
	if d.ApproximateSize != nil && d.SizeDiffHint < d.ctx.cfg.RegionSplitSize/8 {
		return
	}
	d.ctx.splitCheckTaskSender <- &runner.SplitCheckTask{
		Region: d.Region(),
	}
	d.SizeDiffHint = 0
}

func (d *peerMsgHandler) onPrepareSplitRegion(regionEpoch *metapb.RegionEpoch, splitKey []byte, cb *message.Callback) {
	if err := d.validateSplitRegion(regionEpoch, splitKey); err != nil {
		cb.Done(ErrResp(err))
		return
	}
	region := d.Region()
	d.ctx.schedulerTaskSender <- &runner.SchedulerAskSplitTask{
		Region:   region,
		SplitKey: splitKey,
		Peer:     d.Meta,
		Callback: cb,
	}
}

func (d *peerMsgHandler) validateSplitRegion(epoch *metapb.RegionEpoch, splitKey []byte) error {
	if len(splitKey) == 0 {
		err := errors.Errorf("%s split key should not be empty", d.Tag)
		log.Error(err)
		return err
	}

	if !d.IsLeader() {
		// region on this store is no longer leader, skipped.
		log.Infof("%s not leader, skip", d.Tag)
		return &util.ErrNotLeader{
			RegionId: d.regionId,
			Leader:   d.getPeerFromCache(d.LeaderId()),
		}
	}

	region := d.Region()
	latestEpoch := region.GetRegionEpoch()

	// This is a little difference for `check_region_epoch` in region split case.
	// Here we just need to check `version` because `conf_ver` will be update
	// to the latest value of the peer, and then send to Scheduler.
	if latestEpoch.Version != epoch.Version {
		log.Infof("%s epoch changed, retry later, prev_epoch: %s, epoch %s",
			d.Tag, latestEpoch, epoch)
		return &util.ErrEpochNotMatch{
			Message: fmt.Sprintf("%s epoch changed %s != %s, retry later", d.Tag, latestEpoch, epoch),
			Regions: []*metapb.Region{region},
		}
	}
	return nil
}

func (d *peerMsgHandler) onApproximateRegionSize(size uint64) {
	d.ApproximateSize = &size
}

func (d *peerMsgHandler) onSchedulerHeartbeatTick() {
	d.ticker.schedule(PeerTickSchedulerHeartbeat)

	if !d.IsLeader() {
		return
	}
	d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
}

func (d *peerMsgHandler) onGCSnap(snaps []snap.SnapKeyWithSending) {
	compactedIdx := d.peerStorage.truncatedIndex()
	compactedTerm := d.peerStorage.truncatedTerm()
	for _, snapKeyWithSending := range snaps {
		key := snapKeyWithSending.SnapKey
		if snapKeyWithSending.IsSending {
			snap, err := d.ctx.snapMgr.GetSnapshotForSending(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			if key.Term < compactedTerm || key.Index < compactedIdx {
				log.Infof("%s snap file %s has been compacted, delete", d.Tag, key)
				d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
			} else if fi, err1 := snap.Meta(); err1 == nil {
				modTime := fi.ModTime()
				if time.Since(modTime) > 4*time.Hour {
					log.Infof("%s snap file %s has been expired, delete", d.Tag, key)
					d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
				}
			}
		} else if key.Term <= compactedTerm &&
			(key.Index < compactedIdx || key.Index == compactedIdx) {
			log.Infof("%s snap file %s has been applied, delete", d.Tag, key)
			a, err := d.ctx.snapMgr.GetSnapshotForApplying(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			d.ctx.snapMgr.DeleteSnapshot(key, a, false)
		}
	}
}

func newAdminRequest(regionID uint64, peer *metapb.Peer) *raft_cmdpb.RaftCmdRequest {
	return &raft_cmdpb.RaftCmdRequest{
		Header: &raft_cmdpb.RaftRequestHeader{
			RegionId: regionID,
			Peer:     peer,
		},
	}
}

func newCompactLogRequest(regionID uint64, peer *metapb.Peer, compactIndex, compactTerm uint64) *raft_cmdpb.RaftCmdRequest {
	req := newAdminRequest(regionID, peer)
	req.AdminRequest = &raft_cmdpb.AdminRequest{
		CmdType: raft_cmdpb.AdminCmdType_CompactLog,
		CompactLog: &raft_cmdpb.CompactLogRequest{
			CompactIndex: compactIndex,
			CompactTerm:  compactTerm,
		},
	}
	return req
}

func (d *peerMsgHandler) isNodeExist(nodeId uint64) bool {
	for _, peer := range d.Region().Peers {
		if peer.Id == nodeId {
			return true
		}
	}
	return false
}

func (d *peerMsgHandler) removeNodeInRegion(nodeId uint64) {
	for i, peer := range d.Region().Peers {
		if peer.Id == nodeId {
			d.Region().Peers = append(d.Region().Peers[:i], d.Region().Peers[i+1:]...)
			break
		}
	}
}
