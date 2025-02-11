package raft

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path"
	"runtime"

	"github.com/sirupsen/logrus"
)

type raftFormatter struct{}

const (
	LEADER_COLOR    = "\x1b[91m"
	CANDIDATE_COLOR = "\x1b[92m"
	FOLLOWER_COLOR  = "\x1b[0m"
	NORMAL_COLOR    = "\x1b[0m"
)

func (formatter *raftFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	var b bytes.Buffer
	fmt.Fprintf(&b, "%s\n", entry.Message)
	return b.Bytes(), nil
}

func initLog() {
	fd, err := os.OpenFile("raft.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		panic("Can't open log file")
	}
	logrus.SetLevel(logLevel)
	logrus.SetOutput(io.MultiWriter(os.Stdout, fd))
	logrus.SetReportCaller(true)
	logrus.SetFormatter(&raftFormatter{})
}

func (rf *Raft) DebugLogNoLock(format string, args ...interface{}) {
	rf.mu.Lock()
	rf.DebugLogWithLock(format, args...)
	rf.mu.Unlock()
}

func (rf *Raft) DebugLogWithLock(format string, args ...interface{}) {
	color_scheme := FOLLOWER_COLOR
	switch rf.state {
	case leader:
		color_scheme = LEADER_COLOR
	case candidate:
		color_scheme = CANDIDATE_COLOR
	}
	_, file, line, _ := runtime.Caller(1)
	logrus.Debugf("%s:%d\t%s%d(%d)%s:%s", path.Base(file), line, color_scheme, rf.me, rf.currentTerm, NORMAL_COLOR, fmt.Sprintf(format, args...))
	logrus.Tracef("%v", rf.log)
}
