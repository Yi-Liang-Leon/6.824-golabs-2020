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
	"fmt"
	"lab/src/labrpc"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
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
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// persistent
	currentTerm int
	votedFor    int
	log         []entry

	// volatile
	state       int
	received    bool // whether received heartbeat
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int
}

const (
	FOLLOWER  = 0
	CANDIDATE = 1
	LEADER    = 2
)

type entry struct {
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == LEADER
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.received = true
	if args.Term > rf.currentTerm { // update term
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
	}
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		return
	} else {
		if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
		}
	}
	fmt.Printf("%d(%d): RequestVote from %d: %t\n", rf.me, rf.currentTerm, args.CandidateId, reply.VoteGranted)
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.received = true
	if args.Term > rf.currentTerm { // update term
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
	}
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
	} else {
		reply.Success = true
	}
	fmt.Printf("%d(%d): AppendEntries from %d: %t\n", rf.me, rf.currentTerm, args.LeaderId, reply.Success)
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
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
	atomic.StoreInt32(&rf.dead, 1)
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

	rf.currentTerm = 1
	rf.votedFor = -1
	rf.log = make([]entry, 0)
	rf.state = FOLLOWER
	rf.received = false
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, 0)
	rf.matchIndex = make([]int, 0)

	go rf.timeout()   // election timeout
	go rf.heartbeat() // leader heartbeat

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func (rf *Raft) timeout() {
	time.Sleep(time.Duration(rand.Int63n(300)+300) * time.Millisecond) // sleep at beginning to prevent split
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for {
		rf.received = false
		sleepTime := time.Duration(rand.Int63n(300)+300) * time.Millisecond // range: [300ms, 600ms)
		if !rf.received && rf.state != LEADER {                             // start new election
			fmt.Printf("%d(%d): Timed out! Electing.\n", rf.me, rf.currentTerm)
			rf.mu.Unlock() // child goroutine should run without lock
			go rf.startElection(sleepTime)
			rf.mu.Lock()
		}
		rf.mu.Unlock() // release lock during wait time
		time.Sleep(sleepTime)
		rf.mu.Lock()
	}
}

func (rf *Raft) startElection(sleepTime time.Duration) {
	timer := time.After(sleepTime) // set timer

	// convert to candidate
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = CANDIDATE
	rf.currentTerm += 1
	rf.votedFor = rf.me

	// send vote request
	votes := 0 // received votes
	for i, peer := range rf.peers {
		var args RequestVoteArgs
		var reply RequestVoteReply
		args.CandidateId = rf.me
		args.Term = rf.currentTerm
		fmt.Printf("%d(%d): Requesting vote from %d\n", rf.me, rf.currentTerm, i)
		rf.mu.Unlock()
		go rf.sendRequestVote(&args, &reply, peer, &votes)
		rf.mu.Lock()

	}

	for {
		if 2*votes > len(rf.peers) { // convert to leader
			rf.state = LEADER
			fmt.Printf("%d(%d): Converted to leader now\n", rf.me, rf.currentTerm)
			return
		}
		select {
		case <-timer:
			fmt.Printf("%d(%d): Election timed out\n", rf.me, rf.currentTerm)
			return
		default:
			rf.mu.Unlock()
			time.Sleep(time.Millisecond)
			rf.mu.Lock()
			continue
		}
	}
}

func (rf *Raft) sendRequestVote(args *RequestVoteArgs, reply *RequestVoteReply, peer *labrpc.ClientEnd, votes *int) {
	peer.Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm { // update term
		rf.currentTerm = reply.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
		return
	}
	if reply.VoteGranted {
		*votes++
	}
}

func (rf *Raft) heartbeat() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for {
		rf.mu.Unlock()
		time.Sleep(100 * time.Millisecond) // 10 times per second
		rf.mu.Lock()
		if rf.state == LEADER {
			fmt.Printf("%d(%d): Leader time out, sending heartbeat\n", rf.me, rf.currentTerm)
			for i, peer := range rf.peers {
				fmt.Printf("%d(%d): Sending heartbeat to %d\n", rf.me, rf.currentTerm, i)
				var args AppendEntriesArgs
				var reply AppendEntriesReply
				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				rf.mu.Unlock()
				go rf.sendAppendEntries(&args, &reply, peer)
				rf.mu.Lock()
			}
		}
	}
}

func (rf *Raft) sendAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply, peer *labrpc.ClientEnd) {
	peer.Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm { // update term
		rf.currentTerm = reply.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
	}
}
