package raft

type AppendEntriesArgs struct {
	Term         int        // Leader's term
	LeaderId     int        // To redirect followers
	PrevLogIndex int        // Index of log entry immediately preceding new ones
	PrevLogTerm  int        // Term of PrevLogIndex entry
	Entries      []LogEntry // Log entries to store
	LeaderCommit int        // Leader's commitIndex
}

type AppendEntriesReply struct {
	Term    int  // Current term, for leader to update
	Success bool // True if follower contained entry matching PrevLogIndex and PrevLogTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.DebugLog("AppendEntries(%v)", *args)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.electionTimer.Reset(rf.electionTimeout)
	reply.Term = rf.currentTerm
	reply.Success = false

	// If call come from an old leader, disregard.
	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		rf.DebugLog("Call from old leader, reply false.")
		rf.mu.Lock()
		return
	}
	rf.currentTerm = args.Term

	if rf.state != follower {
		rf.turnFollowerCh <- struct{}{}
	}

	// If previous entry does not match, reply false.
	if args.PrevLogIndex >= len(rf.log) || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		rf.mu.Unlock()
		rf.DebugLog("Previous entry does not match, reply false.")
		rf.mu.Lock()
		return
	}

	// Update log.
	reply.Success = true
	index := args.PrevLogIndex + 1
	for _, e := range args.Entries { // Skip matching entries
		if len(rf.log) <= index || rf.log[index].Term != e.Term {
			break
		}
		index++
	}
	rf.log = rf.log[:index] // Remove all entries starting from index
	rf.log = append(rf.log, args.Entries[index-args.PrevLogIndex-1:]...)
	if rf.commitIndex < args.LeaderCommit {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
	}

	rf.mu.Unlock()
	rf.DebugLog("Updated log starting from %d", index)
	rf.apply()
	rf.mu.Lock()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	rf.DebugLog("sendAppendEntries(%d, %v)", server, *args)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) startAppendEntries() {
	for i := range rf.peers {
		if i != rf.me {
			go func() {
				args := AppendEntriesArgs{}
				reply := AppendEntriesReply{}

				for {
					rf.mu.Lock()
					args.LeaderId = rf.me
					args.Term = rf.currentTerm
					args.LeaderCommit = rf.commitIndex
					args.PrevLogIndex = rf.nextIndex[i] - 1
					args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
					args.Entries = rf.log[args.PrevLogIndex+1:]
					rf.mu.Unlock()

					if !rf.sendAppendEntries(i, &args, &reply) {
						return
					}

					if reply.Term > args.Term {
						rf.turnFollowerCh <- struct{}{}
						break
					}
					if reply.Success {
						rf.DebugLog("Received successful AppendEntriesReply from %d", i)
						rf.mu.Lock()
						rf.matchindex[i] = len(rf.log) - 1 // All logs are updated successfully
						rf.nextIndex[i] = len(rf.log)
						rf.mu.Unlock()
						break
					} else {
						rf.DebugLog("Received failed AppendEntriesReply from %d, retry.", i)
						rf.mu.Lock()
						rf.nextIndex[i]-- // Decrease and retry
						rf.mu.Unlock()
					}
				}
				rf.mu.Lock()
				rf.commitIndex = getCommittedIndex(rf.matchindex)
				rf.mu.Unlock()
				rf.apply()
			}()
		}
	}
}
