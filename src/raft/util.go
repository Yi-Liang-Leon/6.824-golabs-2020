package raft

import (
	"log"
	"math/rand"
	"sort"
	"time"
)

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func newTimeout() time.Duration {
	return time.Duration(rand.Intn(maxTimeout-minTimeout)+minTimeout) * time.Millisecond
}

func getCommittedIndex(matchIndex []int) int {
	cloned := make([]int, len(matchIndex))
	copy(cloned, matchIndex)
	sort.Ints(cloned)
	return cloned[len(cloned)/2] // return matched indices of majority servers
}

func (rf *Raft) isNewerThan(index int, term int) bool {
	last := rf.log[len(rf.log)-1]
	if last.Term > term {
		return true
	} else if last.Term < term {
		return false
	} else if len(rf.log)-1 > index {
		return true
	}
	return false
}
