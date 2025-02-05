package raft

func (rf *Raft) heartbeatHandler() {
	for range rf.heartbeatTimer.C {
		rf.mu.Lock()
		if rf.state == leader {
			rf.mu.Unlock()
			rf.startAppendEntries()
			rf.mu.Lock()
		}
		rf.heartbeatTimer.Reset(rf.heartbeatTimeout)
		rf.mu.Unlock()
	}

}

func (rf *Raft) startHeartbeat() {
	for i := range rf.peers {
		if i != rf.me {
			go func() {
				args := AppendEntriesArgs{}
				reply := AppendEntriesReply{}

				args.LeaderId = rf.me
				args.Term = rf.currentTerm
				args.LeaderCommit = rf.commitIndex
				rf.sendAppendEntries(i, &args, &reply)
			}()
		}
	}
}
