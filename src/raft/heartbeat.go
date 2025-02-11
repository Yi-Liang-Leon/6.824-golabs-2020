package raft

func (rf *Raft) heartbeatHandler() {
	for {
		select {
		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			if rf.state == leader {
				rf.mu.Unlock()
				rf.startAppendEntries()
				rf.mu.Lock()
			}
			rf.heartbeatTimer.Reset(rf.heartbeatTimeout)
			rf.mu.Unlock()
		case <-rf.killSig:
			return
		}
	}
}
