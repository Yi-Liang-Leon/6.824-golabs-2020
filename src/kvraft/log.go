package kvraft

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path"
	"runtime"

	"github.com/sirupsen/logrus"
)

const logLevel = logrus.InfoLevel

type kvFormater struct{}

func (formatter *kvFormater) Format(entry *logrus.Entry) ([]byte, error) {
	var b bytes.Buffer
	fmt.Fprintf(&b, "%s\n", entry.Message)
	return b.Bytes(), nil
}

func initLog() {
	fd, err := os.OpenFile("kvraft.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		panic("Can't open log file")
	}
	logrus.SetLevel(logLevel)
	logrus.SetOutput(io.MultiWriter(os.Stdout, fd))
	logrus.SetReportCaller(true)
	logrus.SetFormatter(&kvFormater{})
}

func (kv *KVServer) DebugLogServerNoLock(format string, args ...interface{}) {
	kv.mu.Lock()
	kv.DebugLogServerWithLock(format, args...)
	kv.mu.Unlock()
}

func (kv *KVServer) DebugLogServerWithLock(format string, args ...interface{}) {
	_, file, line, _ := runtime.Caller(1)
	logrus.Debugf("%s:%d\t%d: %s", path.Base(file), line, kv.me, fmt.Sprintf(format, args...))
	logrus.Tracef("%v", kv.db)
}
