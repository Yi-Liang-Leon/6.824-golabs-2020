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

const (
	KV_LOG_LEVEL = logrus.TraceLevel
	CK_LOG_LEVEL = logrus.DebugLevel
)

type kvFormater struct{}

func (formatter *kvFormater) Format(entry *logrus.Entry) ([]byte, error) {
	var b bytes.Buffer
	fmt.Fprintf(&b, "%s\n", entry.Message)
	return b.Bytes(), nil
}

func (kv *KVServer) initLog() {
	fd, err := os.OpenFile("kvraft.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		panic("Can't open log file")
	}
	kv.logger.SetLevel(KV_LOG_LEVEL)
	kv.logger.SetOutput(io.MultiWriter(os.Stdout, fd))
	kv.logger.SetReportCaller(true)
	kv.logger.SetFormatter(&kvFormater{})
}

func (kv *KVServer) LogServerNoLock(format string, args ...interface{}) {
	kv.mu.Lock()
	_, file, line, _ := runtime.Caller(1)
	kv.logger.Debugf("%s:%d\t%d: %s", path.Base(file), line, kv.me, fmt.Sprintf(format, args...))
	kv.logger.Tracef("%v", kv.db)
	kv.mu.Unlock()
}

func (kv *KVServer) LogServerWithLock(format string, args ...interface{}) {
	_, file, line, _ := runtime.Caller(1)
	kv.logger.Debugf("%s:%d\t%d: %s", path.Base(file), line, kv.me, fmt.Sprintf(format, args...))
	kv.logger.Tracef("%v", kv.db)
}

func (ck *Clerk) initLog() {
	fd, err := os.OpenFile("clerk.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		panic("Can't open log file")
	}
	ck.logger.SetLevel(CK_LOG_LEVEL)
	ck.logger.SetOutput(io.MultiWriter(os.Stdout, fd))
	ck.logger.SetReportCaller(true)
	ck.logger.SetFormatter(&kvFormater{})
}

func (ck *Clerk) LogClientNoLock(format string, args ...interface{}) {
	ck.mu.Lock()
	_, file, line, _ := runtime.Caller(1)
	ck.logger.Debugf("%s:%d\tclient: %s", path.Base(file), line, fmt.Sprintf(format, args...))
	ck.mu.Unlock()
}

func (ck *Clerk) LogClientWithLock(format string, args ...interface{}) {
	_, file, line, _ := runtime.Caller(1)
	ck.logger.Debugf("%s:%d\tclient: %s", path.Base(file), line, fmt.Sprintf(format, args...))
}
