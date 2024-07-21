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
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//	snapshot/first.....applied....committed....stabled.....last
//	--------|------------------------------------------------|
//	                          log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry // 包含了所有的entries，持久化+非持久化

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	firstIndex uint64 // 用于标记entries[0] 在 storage 中的 index
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	firstIndex, err := storage.FirstIndex()
	if err != nil {
		panic(err)
	}

	lastIndex, err := storage.LastIndex()
	if err != nil {
		panic(err)
	}

	entries, err := storage.Entries(firstIndex, lastIndex+1) // [firstIndex, lastIndex+1)
	if err != nil {
		panic(err)
	}

	hardState, _, err := storage.InitialState()
	if err != nil {
		panic(err)
	}

	l := &RaftLog{
		storage:         storage,
		committed:       hardState.Commit,
		applied:         firstIndex - 1,
		stabled:         lastIndex,
		entries:         entries,
		pendingSnapshot: nil,
		firstIndex:      firstIndex,
	}

	return l
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
func (l *RaftLog) allEntries() []pb.Entry {
	// Your Code Here (2A).
	return l.entries
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if l.stabled < l.firstIndex {
		return l.entries
	}

	if l.stabled+1-l.firstIndex >= uint64(len(l.entries)) {
		return []pb.Entry{}
	}

	return l.entries[l.stabled+1-l.firstIndex:]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	// [applied + 1 ..... committed + 1)
	return l.entries[l.applied-l.firstIndex+1 : l.committed-l.firstIndex+1]
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	return l.firstIndex + uint64(len(l.entries)) - 1
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	// 为了解决 i 为 0 的情况
	if i == 0 {
		return 0, nil
	}

	if i < l.firstIndex {
		return 0, ErrCompacted
	}

	if i > l.LastIndex() {
		return 0, ErrUnavailable
	}

	return l.entries[i-l.firstIndex].Term, nil
}

func (l *RaftLog) Append(entries []*pb.Entry) {
	//index := l.LastIndex()
	for _, entry := range entries {
		//	index++
		l.entries = append(l.entries, pb.Entry{
			EntryType: entry.EntryType,
			Term:      entry.Term,
			Index:     entry.Index,
			Data:      entry.Data,
		})
	}
}

// Release 释放掉 index 及之后的所有 entries
func (l *RaftLog) Release(index uint64) {
	if index < l.firstIndex {
		return
	}

	if index > l.LastIndex() {
		return
	}

	l.entries = l.entries[:index-l.firstIndex]

	l.stabled = l.LastIndex()
}

func (l *RaftLog) UpdateStabled(index uint64) {
	l.stabled = index
}

func (l *RaftLog) UpdateApplied(index uint64) {
	l.applied = index
}

func (l *RaftLog) UpdateCommit(commitIndex uint64, logIndex uint64, entriesLen uint64) {
	if commitIndex > l.committed {
		// 限制 commitIndex 不超过 logIndex+entriesLen
		l.committed = min(commitIndex, logIndex+entriesLen)
	}
}

func (l *RaftLog) Propose(term uint64, entries []*pb.Entry) {
	index := l.LastIndex()
	for _, entry := range entries {
		index++
		l.entries = append(l.entries, pb.Entry{
			EntryType: entry.EntryType,
			Term:      term,
			Index:     index,
			Data:      entry.Data,
		})
	}
}

func (l *RaftLog) GetEntriesFromIndex(index uint64) ([]*pb.Entry, error) {
	reEntries := make([]*pb.Entry, 0)
	if index < l.firstIndex {
		log.Infof("the log is compacted %d %d", index, l.firstIndex)
		return nil, ErrCompacted
	}

	entries := l.entries[index-l.firstIndex:]
	for i := uint64(0); i < uint64(len(entries)); i++ {
		reEntries = append(reEntries, &entries[i])
	}
	return reEntries, nil
}

func (l *RaftLog) GetLastIndexAndTerm() (uint64, uint64, error) {
	lastLogIndex := l.LastIndex()
	if lastLogIndex == 0 {
		return 0, 0, nil
	}
	lastLogTerm, err := l.Term(lastLogIndex)
	if err != nil {
		//		log.Errorf("get term failed when sendRequestVote, err: %v", err)
		return 0, 0, err
	}
	return lastLogIndex, lastLogTerm, nil
}

// TryCommit 更新 committed，只能提交当前term的entry
func (l *RaftLog) TryCommit(index uint64, term uint64) (uint64, error) {
	if logTerm, _ := l.Term(index); logTerm != term {
		return l.committed, nil
	}
	l.committed = max(index, l.committed)
	return l.committed, nil
}
