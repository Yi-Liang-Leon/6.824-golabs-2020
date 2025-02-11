package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"lab/src/labgob"
	"lab/src/labrpc"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
)

// import "bytes"
// import "lab/src/labgob"

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.

type state int

const (
	follower state = iota
	candidate
	leader
	minTimeout       int          = 200 // Milliseconds
	maxTimeout       int          = 400
	heartbeatTimeout int          = 150
	logLevel         logrus.Level = logrus.TraceLevel
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Term    int
	Command any
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on ALL servers
	currentTerm int        // Latest term server has seen
	votedFor    int        // CandidateId that received vote in current term, -1 for not voted
	log         []LogEntry // Log entries
	state       state      // Follower, candidate or leader

	// Volatile state on ALL servers
	commitIndex int // Index of highest log entry known to be committed
	lastApplied int // Index of highest log entry applied to state machine

	// Volatile state on LEADERs (reinitialized after election)
	nextIndex  []int // For each server, index of the next log entry to send
	matchindex []int // For each server, index of highest log entry known to be replicated on server

	// Election Logic
	electionTimer   *time.Timer
	electionTimeout time.Duration // Randomly picked during election
	turnLeaderCh    chan struct{}
	turnFollowerCh  chan struct{} // Term is passed through this channel

	// Heartbeat Logic
	heartbeatTimer   *time.Timer // Should be reset upon elected, and stopped when turning into follower
	heartbeatTimeout time.Duration

	// Apply Message
	applyCh chan ApplyMsg

	// Kill Signal
	killSig chan struct{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (term int, isLeader bool) {

	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isLeader = (rf.state == leader)
	return
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// persist() must be inside critical zone to guarentee the correctness
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.state)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	rf.DebugLogNoLock("Read persist")
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	var state state
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&state) != nil {
		panic("Read persist error!")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.state = state
		for i := range rf.nextIndex {
			rf.nextIndex[i] = len(rf.log)
		}
	}
}

// Append entries and heartbeat RPC

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command any) (index int, term int, isLeader bool) {
	rf.DebugLogNoLock("Start(%v)", command)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index = -1
	term = -1
	isLeader = true

	// If current is not leader, return false.
	if rf.state != leader {
		isLeader = false
		return
	}

	rf.log = append(rf.log, LogEntry{rf.currentTerm, command})
	index = len(rf.log) - 1
	term = rf.currentTerm
	rf.matchindex[rf.me] = len(rf.log) - 1

	// Append entries immediately.
	rf.persist()
	rf.mu.Unlock()
	rf.startAppendEntries()
	rf.mu.Lock()
	rf.heartbeatTimer.Reset(rf.heartbeatTimeout)

	return
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	rf.DebugLogNoLock("Killed!")
	rf.mu.Lock()
	defer rf.mu.Unlock()
	atomic.StoreInt32(&rf.dead, 1)
	rf.persist()
	close(rf.killSig)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	initLog()
	// Elelction
	rf.electionTimeout = newTimeout()
	rf.electionTimer = time.NewTimer(rf.electionTimeout)
	rf.turnFollowerCh = make(chan struct{}, 1)
	rf.turnLeaderCh = make(chan struct{}, 1)
	// Heartbeat
	rf.heartbeatTimer = time.NewTimer(time.Hour)
	rf.heartbeatTimer.Stop() // Stopped when init. Only invoked when turning into leader
	rf.heartbeatTimeout = time.Duration(heartbeatTimeout) * time.Millisecond
	// Log
	rf.log = make([]LogEntry, 1) // Log[0] is not used
	rf.nextIndex = make([]int, len(peers))
	rf.matchindex = make([]int, len(peers))
	// Apply Message
	rf.applyCh = applyCh
	// Kill Signal
	rf.killSig = make(chan struct{})

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.stateHandler()
	go rf.electionHandler()
	go rf.heartbeatHandler()

	return rf
}

func (rf *Raft) stateHandler() {
	for {
		select {
		case <-rf.turnFollowerCh:
			rf.DebugLogNoLock("Turn follower")
			rf.mu.Lock()
			rf.state = follower
			rf.votedFor = -1
			rf.electionTimer.Reset(rf.electionTimeout)
			rf.heartbeatTimer.Stop()
			rf.persist()
			rf.mu.Unlock()
		case <-rf.turnLeaderCh:
			rf.DebugLogNoLock("Turn leader")
			rf.mu.Lock()
			rf.state = leader
			rf.electionTimer.Stop()
			rf.heartbeatTimer.Reset(0) // Immediately send heartbeat
			for i := range len(rf.peers) {
				rf.nextIndex[i] = len(rf.log)
				rf.matchindex[i] = 0
			}
			rf.persist()
			rf.mu.Unlock()
		case <-rf.killSig:
			return
		}
	}
}

// must be inside critical zone to prevent from out-of-order call
func (rf *Raft) apply() {
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		rf.applyCh <- ApplyMsg{true, rf.log[i].Command, i}
	}
	if rf.lastApplied < rf.commitIndex {
		from, to := rf.lastApplied+1, rf.commitIndex
		rf.DebugLogWithLock("Applied %d ~ %d", from, to)
	}
	rf.lastApplied = rf.commitIndex
}
