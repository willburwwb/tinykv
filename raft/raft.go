// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"github.com/pingcap-incubator/tinykv/log"
	"math/rand"
	"sort"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int

	electionTimeOutBase int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).

	hardState, confState, err := c.Storage.InitialState()
	if err != nil {
		panic(err)
	}

	raftLog := newLog(c.Storage)

	prs := make(map[uint64]*Progress)
	if len(c.peers) != 0 {
		for _, id := range c.peers {
			prs[id] = &Progress{
				Match: 0,
				Next:  raftLog.LastIndex() + 1,
			}
		}
	} else {
		for _, id := range confState.Nodes {
			prs[id] = &Progress{
				Match: 0,
				Next:  raftLog.LastIndex() + 1,
			}
		}
	}

	votes := make(map[uint64]bool)

	r := &Raft{
		id:                  c.ID,
		Term:                hardState.Term,
		Vote:                hardState.Vote,
		RaftLog:             raftLog,
		Prs:                 prs,
		State:               StateFollower,
		votes:               votes,
		msgs:                nil,
		Lead:                None,
		heartbeatTimeout:    c.HeartbeatTick,
		electionTimeOutBase: c.ElectionTick,
		electionTimeout:     c.ElectionTick + rand.Int()%c.ElectionTick,
		heartbeatElapsed:    0,
		electionElapsed:     0,
		leadTransferee:      0,
		PendingConfIndex:    0,
	}

	//log.Infof("init raft %+v", r)
	return r
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).

	// append entries 时，preLogIndex 为 r.Prs[to].next - 1
	preLogIndex := r.Prs[to].Next - 1
	preLogTerm, err := r.RaftLog.Term(preLogIndex)
	//	log.Infof("peer %d sendAppend to %d, preLogIndex: %d, preLogTerm: %d", r.id, to, preLogIndex, preLogTerm)

	if err != nil {
		//	log.Errorf("get term failed when sendAppend, err: %v", err)
		return false
	}

	entries, err := r.RaftLog.GetEntriesFromIndex(r.Prs[to].Next)
	if err != nil {
		//	log.Errorf("get entries failed when sendAppend, err: %v", err)
		return false
	}
	//	log.Infof("peer %d sendAppend to %d, entries: %v", r.id, to, entries)

	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		LogTerm: preLogTerm,
		Index:   preLogIndex,
		Entries: entries,
		Commit:  r.RaftLog.committed,
	})
	return true
}

// index 为 当前节点的最后一个日志的 index，用于优化
func (r *Raft) sendAppendResp(reject bool, to uint64, index uint64) {
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Reject:  reject,
		Index:   index,
	})
	return
}

func (r *Raft) sendRequestVote(to uint64) {
	// 保证安全性，只有拥有最新提交日志的节点才能成为 leader
	lastLogIndex, lastLogTerm, err := r.RaftLog.GetLastIndexAndTerm()
	if err != nil {
		//log.Errorf("get last log index and term failed when sendRequestVote, err: %v", err)
		return
	}

	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Index:   lastLogIndex,
		LogTerm: lastLogTerm,
	})
}

func (r *Raft) sendRequestVoteResp(reject bool, to uint64) {
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Reject:  reject,
	})
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		From:    r.id,
		To:      to,
		Term:    r.Term,
	})
}

func (r *Raft) sendHeartbeatResponse(to uint64) {
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Commit:  r.RaftLog.committed, // heartBeat 返回 commitIndex，为了方便 leader 更新 follower 的 commitIndex
	})
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		fallthrough
	case StateCandidate:
		r.electionElapsed++
		if r.electionElapsed >= r.electionTimeout && r.promotable(r.id) {
			err := r.Step(pb.Message{From: r.id, MsgType: pb.MessageType_MsgHup})
			if err != nil {
				log.Errorf("Candidate %d failed to start election: %v", r.id, err)
				return
			}
			r.electionElapsed = 0
		}
	case StateLeader:
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout && r.promotable(r.id) {
			err := r.Step(pb.Message{From: r.id, MsgType: pb.MessageType_MsgBeat})
			if err != nil {
				log.Errorf("Leader %d failed to send heartbeat: %v", r.id, err)
				return
			}
			r.heartbeatElapsed = 0
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.State = StateFollower
	r.reset(term)
	r.Lead = lead
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.State = StateCandidate
	r.reset(r.Term + 1)
	r.Vote = r.id
	r.votes[r.id] = true
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.reset(r.Term)
	r.Lead = r.id
	for id := range r.Prs {
		if id == r.id {
			r.Prs[id] = &Progress{
				Next:  r.RaftLog.LastIndex() + 1,
				Match: r.RaftLog.LastIndex(),
			}
		} else {
			r.Prs[id] = &Progress{
				Next:  r.RaftLog.LastIndex() + 1,
				Match: 0,
			}
		}
	}

	// 安全性保证，leader 在选举成功后，需要propose一个空的 msg
	_ = r.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		From:    r.id,
		To:      r.id,
		Term:    r.Term,
		Entries: []*pb.Entry{
			{
				EntryType: pb.EntryType_EntryNormal,
				Term:      0,
				Index:     0,
				Data:      nil,
			},
		},
	})

	// 更新 commitIndex, 有可能只有leader一个节点
	r.updateCommitIndex()
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	var err error

	// 判断节点是否在分区中
	if !r.promotable(r.id) {
		log.Errorf("peer %d is not promotable", r.id)
		return nil
	}

	switch r.State {
	case StateFollower:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			r.handleHup(m)
		case pb.MessageType_MsgBeat:
		case pb.MessageType_MsgPropose:
			err = ErrProposalDropped
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgAppendResponse:
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		case pb.MessageType_MsgRequestVoteResponse:
		case pb.MessageType_MsgSnapshot:
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		case pb.MessageType_MsgHeartbeatResponse:
		}
	case StateCandidate:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			r.handleHup(m)
		case pb.MessageType_MsgBeat:
		case pb.MessageType_MsgPropose:
			err = ErrProposalDropped
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgAppendResponse:
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		case pb.MessageType_MsgRequestVoteResponse:
			r.handleRequestVoteResp(m)
		case pb.MessageType_MsgSnapshot:
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		case pb.MessageType_MsgHeartbeatResponse:

		}
	case StateLeader:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
		case pb.MessageType_MsgBeat:
			r.handleBeat(m)
		case pb.MessageType_MsgPropose:
			r.handlePropose(m)
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgAppendResponse:
			r.handleAppendEntriesResponse(m)
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		case pb.MessageType_MsgRequestVoteResponse:
		case pb.MessageType_MsgSnapshot:
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		case pb.MessageType_MsgHeartbeatResponse:
			r.handleHeartbeatResponse(m)
		}
	}

	if err != nil {
		log.Errorf("step message failed, err: %v", err)
	}
	return err
}

func (r *Raft) handleHup(m pb.Message) {
	// 1. 成为 candidate
	r.becomeCandidate()

	// 2. 发送 RequestVote RPC
	for id := range r.Prs {
		if id == r.id {
			continue
		}
		r.sendRequestVote(id)
	}

	// 根据测试用例可知，当 len(prs) == 1时，直接成为 leader
	if len(r.Prs) == 1 {
		r.becomeLeader()
	}
	return
}

func (r *Raft) handleBeat(m pb.Message) {
	// 1. 发送心跳
	for id := range r.Prs {
		if id == r.id {
			continue
		}
		r.sendHeartbeat(id)
	}
}

func (r *Raft) handlePropose(m pb.Message) {
	// 1. 更新本节点日志, 并更新本节点的 match 和 next
	r.RaftLog.Propose(r.Term, m.Entries) // 注意这里的entries没有term和index, 与AppendEntries不同
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1

	// 2. 发送 AppendEntries RPC
	for id := range r.Prs {
		if id == r.id {
			continue
		}
		r.sendAppend(id)
	}

	// 当只有一个节点时，要直接更新 commitIndex
	if len(r.Prs) == 1 {
		r.updateCommitIndex()
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	// 1. 如果 term < currentTerm 返回 false
	if m.Term < r.Term {
		r.sendAppendResp(true, m.From, r.RaftLog.LastIndex())
		return
	}

	// 对于所有 server，如果 term >= currentTerm，转为 Follower，将electionElapsed置为0
	if m.Term >= r.Term {
		// 	根据测试用例1，可以知道当 m.Term >= currentTerm 时，需要提升term，同时非follower节点需要转为follower
		//  follower 不需要重新转换状态
		r.Term = m.Term
		r.Lead = m.From
		if r.State == StateCandidate || r.State == StateLeader {
			r.becomeFollower(m.Term, m.From)
		}
	}

	// 2. 如果 preLogIndex 对应的那条日志的 term != preLogTerm 返回 false
	if m.Index > r.RaftLog.LastIndex() {
		r.sendAppendResp(true, m.From, r.RaftLog.LastIndex())
		return
	}

	term, err := r.RaftLog.Term(m.Index)
	if err != nil {
		//log.Infof("get term failed when handleAppendEntries, err: %v", err)
		return
	}
	if term != m.LogTerm {
		r.sendAppendResp(true, m.From, r.RaftLog.LastIndex())
		return
	}

	// 3. 如果 r.RaftLog.entries 与新的日志 m.Entries 冲突（index 相同但是 term 不同），删除这条日志和它之后的所有日志，然后添加新的日志
	for i := uint64(0); i < uint64(len(m.Entries)); i++ {
		term, err := r.RaftLog.Term(i + m.Index + 1)
		if err != nil || term != m.Entries[i].Term {
			r.RaftLog.Release(i + m.Index + 1) // i + m.Index + 1 不匹配，删除这条日志和它之后的所有日志
			r.RaftLog.Append(m.Entries[i:])    // 添加新的日志
			break
		}
	}

	// 4. 如果 leaderCommit > commitIndex，设置 commitIndex = min(leaderCommit, m.Index + len(m.Entries))
	r.RaftLog.UpdateCommit(m.Commit, m.Index, uint64(len(m.Entries)))
	r.sendAppendResp(false, m.From, r.RaftLog.LastIndex())
}

func (r *Raft) handleAppendEntriesResponse(m pb.Message) {
	if m.Reject {
		// 1. 如果 AppendEntries RPC 被拒绝，减小 next (m.Index 是此时 from 节点的index)
		r.Prs[m.From].Next = min(r.Prs[m.From].Next-1, m.Index+1)
		r.sendAppend(m.From)
		return
	}

	r.Prs[m.From].Match = m.Index
	r.Prs[m.From].Next = m.Index + 1

	oldCommit := r.RaftLog.committed
	r.updateCommitIndex()

	// 2. 如果 AppendEntries RPC 被接受，更新 commitIndex，然后发送 AppendEntries RPC 更新其他节点的 commitIndex
	if r.RaftLog.committed > oldCommit {
		for id := range r.Prs {
			if id == r.id {
				continue
			}
			r.sendAppend(id)
		}
	}
}

func (r *Raft) updateCommitIndex() uint64 {
	// 1. 计算大多数节点的 commitIndex
	//log.Infof("===peer %d update commitIndex===", r.id)
	matchIndex := make([]uint64, 0)
	for id := range r.Prs {
		matchIndex = append(matchIndex, r.Prs[id].Match)
	}

	// 2. 对 matchIndex 进行排序
	sort.Slice(matchIndex, func(i, j int) bool {
		return matchIndex[i] < matchIndex[j]
	})

	// 3. 计算大多数节点的 commitIndex
	n := len(r.Prs)
	commitIndex := uint64(0)
	if n%2 == 0 {
		commitIndex, _ = r.RaftLog.TryCommit(matchIndex[n/2-1], r.Term)
	} else {
		commitIndex, _ = r.RaftLog.TryCommit(matchIndex[n/2], r.Term)
	}

	// r.RaftLog.UpdateStabled()
	// log.Infof("peer %d update commitIndex to %d", r.id, commitIndex)
	return commitIndex
}

func (r *Raft) handleRequestVote(m pb.Message) {
	// 1. 如果 term < currentTerm 返回 false
	if m.Term < r.Term {
		r.sendRequestVoteResp(true, m.From)
		return
	}

	// 对于所有 server，如果 term > currentTerm，表明新 term 的到来以前的vote作废，所以全部节点转为 Follower
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None) // 转为 follower, 但是不一定会发送投票，follower 不一定是 from
		r.Vote = m.From
	}

	// 2. 如果 votedFor 不为空，并且不为 candidateId，表明 term 没有变过，返回 false
	if r.Vote != None && r.Vote != m.From {
		r.sendRequestVoteResp(true, m.From)
		return
	}

	// 3. 如果 votedFor 为空或者为 candidateId，并且候选者的日志至少和自己一样新，投票给候选者
	lastLogIndex, lastLogTerm, err := r.RaftLog.GetLastIndexAndTerm()
	if err != nil {
		log.Errorf("get term failed when handleRequestVote, err: %v", err)
		return
	}

	if m.LogTerm > lastLogTerm || (m.LogTerm == lastLogTerm && m.Index >= lastLogIndex) {
		r.becomeFollower(m.Term, None) // 当前节点投票给候选者，转为 follower
		r.Vote = m.From
		r.sendRequestVoteResp(false, m.From)
		return
	} else {
		r.sendRequestVoteResp(true, m.From)
		return
	}
}

func (r *Raft) handleRequestVoteResp(m pb.Message) {
	if m.Reject {
		r.votes[m.From] = false
	} else {
		r.votes[m.From] = true
	}

	accept := 0
	reject := 0
	for id := range r.votes {
		if r.votes[id] {
			accept++
		} else {
			reject++
		}
	}

	if accept*2 > len(r.Prs) {
		r.becomeLeader()
	} else if reject*2 >= len(r.Prs) {
		r.becomeFollower(r.Term, None)
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	// 1. 如果 term < currentTerm 直接返回，用 currentTerm 更新 leader, 使其转换为 follower
	if m.Term < r.Term {
		r.sendHeartbeatResponse(m.From)
		return
	}

	// 2. 如果 term > currentTerm，转为 Follower
	r.becomeFollower(m.Term, m.From)
	r.sendHeartbeatResponse(m.From)
	return
}

func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	if m.Term > r.Term {
		r.Term = m.Term
		if r.State == StateCandidate || r.State == StateLeader {
			r.becomeFollower(m.Term, m.From)
		}
		return
	}

	if r.RaftLog.committed > m.Commit {
		r.sendAppend(m.From)
	}
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}

// promotable 是否可以提升为 leader
func (r *Raft) promotable(i uint64) bool {
	_, ok := r.Prs[i]
	return ok
}

func (r *Raft) reset(term uint64) {
	r.Term = term
	r.Vote = None
	//r.Prs = make(map[uint64]*Progress)
	prs := make(map[uint64]*Progress)
	for id := range r.Prs {
		prs[id] = nil
	}
	r.Prs = prs

	r.votes = make(map[uint64]bool)
	r.msgs = make([]pb.Message, 0)
	r.Lead = None
	r.heartbeatElapsed = 0
	r.electionElapsed = 0
	r.electionTimeout = r.electionTimeOutBase + rand.Int()%r.electionTimeOutBase
}
