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
	"math/rand"

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

	Term  uint64
	Vote  uint64
	Peers []uint64

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
	// baseline of election interval
	electionTimeout int
	// randomized timeout in each election
	etRandomized int
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
	var raft Raft
	raft.heartbeatTimeout = c.HeartbeatTick
	raft.electionTimeout = c.ElectionTick
	raft.etRandomized = c.ElectionTick + rand.Intn(c.ElectionTick)
	raft.Peers = c.peers
	raft.id = c.ID
	raft.Term = 0
	raft.RaftLog = newLog(c.Storage)
	hardState, _, _ := c.Storage.InitialState()
	raft.Vote = hardState.Vote

	raft.Prs = make(map[uint64]*Progress)
	for _, id := range raft.Peers {
		raft.Prs[id] = &Progress{}
	}
	return &raft
}

func (r *Raft) prepareEntriesForMsg(m *pb.Message, nextIndex uint64) {
	// Detemine the entries to send
	offset := r.RaftLog.Offset()
	// Get the previous entry before the ones that are about to be sent
	startIdx := 0
	if nextIndex > offset {
		startIdx = int(nextIndex - offset)
		prevEntry := r.RaftLog.allEntries()[startIdx-1]
		m.LogTerm = prevEntry.Term
		m.Index = prevEntry.Index
	}
	for i := startIdx; i < len(r.RaftLog.entries); i++ {
		m.Entries = append(m.Entries, &r.RaftLog.entries[i])
	}
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Return true if a message was sent.
//
// The tests assume that once the leader advances its commit index,
// it will broadcast the commit index by `MessageType_MsgAppend` messages.
//
// We send the appendEntries message even when there are no entries to be sent
func (r *Raft) sendAppend(to uint64) bool {
	m := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Commit:  r.RaftLog.committed,
		LogTerm: 0,
		Index:   0,
		Entries: []*pb.Entry{},
	}
	r.prepareEntriesForMsg(&m, r.Prs[to].Next)
	r.msgs = append(r.msgs, m)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	m := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Commit:  r.RaftLog.committed,
	}
	r.msgs = append(r.msgs, m)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		r.electionElapsed++
		if r.electionElapsed == r.etRandomized {
			r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
		}
	case StateCandidate:
		r.electionElapsed++
		// Failed to get enough votes
		if r.electionElapsed == r.etRandomized {
			r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
		}
	case StateLeader:
		r.heartbeatElapsed++
		if r.heartbeatElapsed == r.heartbeatTimeout {
			for _, serverID := range r.Peers {
				if serverID != r.id {
					r.sendHeartbeat(serverID)
				}
			}
			r.heartbeatElapsed = 0
		}
	}

}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.Term = term
	r.State = StateFollower
	r.Vote = 0
	r.Lead = lead
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.State = StateCandidate
	r.electionElapsed = 0
	// Randomize election timeout
	r.etRandomized = r.electionTimeout + rand.Intn(r.electionTimeout)
	r.Term++
}

func (r *Raft) startElection() {
	// Reset votes
	r.votes = make(map[uint64]bool)
	// Vote for itself
	r.Vote = r.id
	r.votes[r.id] = true
	// If there is only one people
	if len(r.Peers) == 1 {
		r.becomeLeader()
		return
	}
	// Send requestVotes for other servers
	for _, serverID := range r.Peers {
		if serverID == r.id {
			continue
		}
		lastIdx := r.RaftLog.LastIndex()
		lastTerm, _ := r.RaftLog.Term(lastIdx)
		m := pb.Message{
			From:    r.id,
			To:      serverID,
			MsgType: pb.MessageType_MsgRequestVote,
			Term:    r.Term,
			Index:   lastIdx,
			LogTerm: lastTerm,
		}
		r.msgs = append(r.msgs, m)
	}
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	if r.State == StateLeader {
		return
	}
	r.State = StateLeader
	r.heartbeatElapsed = 0
	// Log replication
	r.RaftLog.committed = 0
	r.RaftLog.applied = 0
	for _, id := range r.Peers {
		r.Prs[id].Next = r.RaftLog.LastIndex() + 1
		r.Prs[id].Match = 0
	}
	// NOTE: Leader should propose a noop entry on its term
	m := pb.Message{MsgType: pb.MessageType_MsgPropose, From: r.id, To: r.id, Entries: []*pb.Entry{{}}}
	r.Step(m)
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	if m.Term > r.Term {
		r.becomeFollower(m.Term, 0)
	}
	switch m.MsgType {
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendEntriesResp(m)
	case pb.MessageType_MsgHeartbeatResponse:
		r.handleHeartbeatResp(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleRequstVoteResponse(m)
	// Local messages
	case pb.MessageType_MsgHup:
		r.handleHup(m)
	case pb.MessageType_MsgBeat:
		r.handleBeat(m)
	case pb.MessageType_MsgPropose:
		r.handlePropose(m)
	}

	return nil
}

func (r *Raft) handlePropose(m pb.Message) {
	if r.State == StateCandidate {
		return
	}
	if r.State == StateFollower {
		/*
			When passed to follower, 'MessageType_MsgPropose' is stored in follower's mailbox(msgs)
			by the send method. It is stored with sender's ID and later forwarded to the leader by
			rafthttp package.
		*/
		r.msgs = append(r.msgs, m)
		return
	}
	for _, entry := range m.Entries {
		entry.Index = r.RaftLog.LastIndex() + 1
		entry.Term = r.Term
		r.RaftLog.entries = append(r.RaftLog.entries, *entry)
	}
	newLastIdx := r.RaftLog.LastIndex()
	r.Prs[r.id].Match = newLastIdx
	r.Prs[r.id].Next = newLastIdx + 1
	if len(r.Peers) == 1 {
		// Handle leader only situation
		r.RaftLog.committed = newLastIdx
		return
	}
	for _, id := range r.Peers {
		if id != r.id {
			r.sendAppend(id)
		}
	}
}

func (r *Raft) handleHup(_ pb.Message) {
	if r.State == StateFollower || r.State == StateCandidate {
		r.becomeCandidate()
		r.startElection()
	}
}

func (r *Raft) handleRequstVoteResponse(m pb.Message) {
	r.votes[m.From] = !m.Reject
	currentVotes := 0
	currentRejects := 0
	for _, val := range r.votes {
		if val {
			currentVotes++
		} else {
			currentRejects++
		}
	}
	// If we get the majority of votes
	if currentVotes > len(r.Peers)/2 {
		r.becomeLeader()
	}
	// This is specified by the tests, not the paper
	if currentRejects > len(r.Peers)/2 {
		r.becomeFollower(r.Term, r.Lead)
	}
}

func (r *Raft) handleRequestVote(m pb.Message) {
	resp := pb.Message{MsgType: pb.MessageType_MsgRequestVoteResponse}
	resp.From = r.id
	resp.To = m.From
	resp.Term = r.Term
	resp.Reject = true
	if m.Term < r.Term {
		r.msgs = append(r.msgs, resp)
		return
	}
	if r.State == StateFollower {
		lastIdx := r.RaftLog.LastIndex()
		lastTerm, _ := r.RaftLog.Term(lastIdx)
		candidateUpToDate := false
		if m.LogTerm == lastTerm {
			// If the logs end with the same term,
			// then whichever log is longer is more up-to-date.
			candidateUpToDate = m.Index >= lastIdx
		} else {
			// If the logs have last entries with different terms,
			// then the log with the later term is more up-to-date.
			candidateUpToDate = m.LogTerm >= lastTerm
		}
		// If votedFor is null or candidateId,
		// and candidate’s log is at least as up-to-date as receiver’s log, grant vote
		if candidateUpToDate && (r.Vote == 0 || r.Vote == m.From) {
			r.Vote = m.From
			resp.Reject = false
		}
	}
	r.msgs = append(r.msgs, resp)
	return
}

func (r *Raft) handleAppendOrHeartbeat(m pb.Message) *pb.Message {
	// Your Code Here (2A).
	resp := &pb.Message{
		From: r.id,
		To:   m.From,
		Term: r.Term,
	}
	if m.Term < r.Term {
		resp.Reject = true
		return resp
	}
	// For Candidate or Follower, accept the new leader
	if r.State == StateCandidate || r.State == StateFollower {
		r.becomeFollower(m.Term, m.From)
	}
	// Reject if prev not match
	if m.Index != 0 {
		logTerm, err := r.RaftLog.Term(m.Index)
		if err != nil || logTerm != m.LogTerm {
			resp.Reject = true
			return resp
		}
	}
	for _, entry := range m.Entries {
		li := r.RaftLog.LastIndex()
		if entry.Index <= li {
			// such index exist
			term, _ := r.RaftLog.Term(entry.Index)
			if term == entry.Term {
				// No conflict
				continue
			}
			// Term conflicts, discard
			offset := r.RaftLog.Offset()
			conflictIdx := entry.Index - offset
			r.RaftLog.entries = r.RaftLog.entries[:conflictIdx]
			// The conflict entries are not stabled anymore
			r.RaftLog.stabled = min(r.RaftLog.stabled, entry.Index-1)
		}
		r.RaftLog.entries = append(r.RaftLog.entries, *entry)
	}
	resp.Index = r.RaftLog.LastIndex()
	if m.Commit > r.RaftLog.committed {
		// If leaderCommit > commitIndex
		// set commitIndex = min(leaderCommit, index of last new entry)
		// If there are no new entries, fallback to the lastIndex of the message. See TestHandleMessageType_MsgAppend2AB
		lastNewIndex := m.Index
		if len(m.Entries) > 0 {
			lastNewIndex = m.Entries[len(m.Entries)-1].Index
		}
		r.RaftLog.committed = min(m.Commit, lastNewIndex)
	}
	resp.Reject = false
	return resp
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	resp := r.handleAppendOrHeartbeat(m)
	resp.MsgType = pb.MessageType_MsgAppendResponse
	r.msgs = append(r.msgs, *resp)
}

func (r *Raft) handleHeartbeatResp(m pb.Message) {
	// If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
	if r.RaftLog.LastIndex() > m.Index {
		r.sendAppend(m.From)
	}
}

func (r *Raft) handleAppendEntriesResp(m pb.Message) {
	if m.Reject {
		// If AppendEntries fails because of log inconsistency: decrement nextIndex and retry (§5.3)
		r.Prs[m.From].Next--
		r.sendAppend(m.From)
		return
	}
	// If successful: update nextIndex and matchIndex for follower (§5.3)
	newMatchIdx := m.Index
	r.Prs[m.From].Next = newMatchIdx + 1
	r.Prs[m.From].Match = newMatchIdx
	// If there exists an N such that N > commitIndex,
	// a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm:
	// set commitIndex = N (§5.3, §5.4).
	if newMatchIdx <= r.RaftLog.committed {
		return
	}
	committedPeer := 0
	for _, progress := range r.Prs {
		if progress.Match >= newMatchIdx {
			committedPeer++
		}
	}
	if committedPeer > len(r.Peers)/2 {
		term, err := r.RaftLog.Term(newMatchIdx)
		if err == nil && term == r.Term {
			r.RaftLog.committed = newMatchIdx
			// Once the leader advances its commit index,
			// it will broadcast the commit index by `MessageType_MsgAppend` messages.
			for _, id := range r.Peers {
				if id != r.id {
					r.sendAppend(id)
				}
			}
		}
	}
}

func (r *Raft) handleBeat(_ pb.Message) {
	if r.State == StateLeader {
		for _, serverID := range r.Peers {
			if serverID != r.id {
				r.sendHeartbeat(serverID)
			}
		}
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	r.heartbeatTimeout = 0
	resp := r.handleAppendOrHeartbeat(m)
	resp.MsgType = pb.MessageType_MsgHeartbeatResponse
	r.msgs = append(r.msgs, *resp)
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
