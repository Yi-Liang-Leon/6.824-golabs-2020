package raft

import (
	"bytes"
	"fmt"
	"io"
	"os"

	"github.com/sirupsen/logrus"
)

type raftFormatter struct{}

func (formatter *raftFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	var b bytes.Buffer
	fmt.Fprintf(&b, "%s:%d: %s\n", entry.Caller.File, entry.Caller.Line, entry.Message)
	return b.Bytes(), nil
}

func initLog() {
	fd, err := os.OpenFile("raft.log", os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		panic("Can't open log file")
	}
	logrus.SetLevel(logLevel)
	logrus.SetOutput(io.MultiWriter(os.Stdout, fd))
	logrus.SetReportCaller(true)
	logrus.SetFormatter(&raftFormatter{})
}
