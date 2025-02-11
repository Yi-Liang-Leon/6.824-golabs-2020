package raft

import (
	"sync/atomic"
	"time"
)

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // Candidate's term
	CandidateID  int // Candidate requesting vote
	LastLogIndex int // Index of last log entry
	LastLogTerm  int // Term of last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if rf.currentTerm > args.Term { // If call from an older candidate, reply false
		return
	} else if rf.currentTerm < args.Term { // If call from a newer candidate, update term
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	if rf.isNewerThan(args.LastLogIndex, args.LastLogTerm) { // If this is newer than candidate, reply false
		return
	}
	if rf.votedFor == -1 || rf.votedFor == args.CandidateID {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID
		rf.electionTimer.Reset(rf.electionTimeout)
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	rf.DebugLogNoLock("sendRequestVote(%d,%v)", server, *args)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply) // Put RPC call outside the critical zone for async IO
	rf.DebugLogNoLock("Received RequestVote from %d (%t)", server, reply.VoteGranted)
	return ok
}

func (rf *Raft) electionHandler() {
	for {
		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			if rf.state != leader {
				rf.mu.Unlock()
				rf.startElection()
				rf.mu.Lock()
			}
			rf.electionTimer.Reset(rf.electionTimeout)
			rf.mu.Unlock()
		case <-rf.killSig:
			return
		}
	}
}

func (rf *Raft) startElection() {
	rf.DebugLogNoLock("startElection()")
	rf.mu.Lock()
	rf.electionTimeout = newTimeout()
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.state = candidate
	var args RequestVoteArgs
	// Init RequestVoteArgs, same args for all vote requests
	if len(rf.log) > 0 {
		args = RequestVoteArgs{
			rf.currentTerm,
			rf.me,
			len(rf.log) - 1,
			rf.log[len(rf.log)-1].Term,
		}
	} else {
		args = RequestVoteArgs{
			rf.currentTerm,
			rf.me,
			0,
			0,
		}
	}
	rf.persist()
	rf.mu.Unlock()

	// Send out vote request
	doneCh := make(chan struct{}, len(rf.peers))
	var voteNum atomic.Int32
	voteNum.Store(1) // One vote from self
	for i := range rf.peers {
		if i != rf.me {
			go func() {
				reply := RequestVoteReply{}
				if !rf.sendRequestVote(i, &args, &reply) {
					return
				}
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.turnFollowerCh <- struct{}{}
				} else if reply.VoteGranted {
					voteNum.Add(1)
				}
				doneCh <- struct{}{}
				rf.persist()
				rf.mu.Unlock()
			}()
		}
	}

	// Collect vote results
	go func() {
		timeout := time.After(time.Duration(maxTimeout) * time.Millisecond)
		doneTaskNum := 0
		for {
			select {
			case <-doneCh:
				rf.DebugLogNoLock("Received %d/%d votes", voteNum.Load(), len(rf.peers))
				doneTaskNum++
				if int(voteNum.Load()) >= len(rf.peers)/2+1 {
					rf.turnLeaderCh <- struct{}{}
					rf.DebugLogNoLock("Win vote! Turning into leader...")
					return
				}
				if doneTaskNum >= len(rf.peers)-1 {
					return
				}
			case <-timeout:
				rf.DebugLogNoLock("Election timeout!")
				return
			}
		}
	}()
}
