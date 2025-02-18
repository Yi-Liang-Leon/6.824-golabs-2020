package kvraft

import (
	"crypto/rand"
	"lab/src/labrpc"
	"math/big"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// TODO: You will have to modify this struct.
	mu     sync.Mutex
	leader int
	callId ClientCallID

	logger logrus.Logger
	once   sync.Once
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// TODO: You'll have to add code here.
	ck.leader = 0
	ck.callId = ClientCallID(nrand())
	ck.once.Do(ck.initLog)
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// TODO: You will have to modify this function.
	ck.mu.Lock()
	ck.LogClientWithLock("Get(%s)", key)
	ck.callId++
	args := GetArgs{key, ck.callId}
	reply := GetReply{}
	ck.mu.Unlock()
	for i := ck.leader; ; i = (i + 1) % len(ck.servers) { // Send requests to all servers starting from leader last time
		// ck.LogClientNoLock("Sending Get(%s) RPC call to server %d", key, i)
		time.Sleep(SLEEP_TIME)
		ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			ck.LogClientNoLock("Get(%s) failed: %s", key, reply.Err)
			continue
		}

		// OK to return
		ck.mu.Lock()
		defer ck.mu.Unlock()
		ck.LogClientWithLock("Get(%s)=%s success. leader=%d", key, reply.Value, i)
		ck.leader = i
		if reply.Err == ErrNoKey {
			return ""
		}
		return reply.Value
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// TODO: You will have to modify this function.
	ck.mu.Lock()
	ck.LogClientWithLock("PutAppend(%s, %s)", key, value)
	args := PutAppendArgs{key, value, Op(op), ck.callId}
	reply := PutAppendReply{}
	ck.mu.Unlock()
	for i := ck.leader; ; i = (i + 1) % len(ck.servers) { // Send requests to all servers starting from leader last time
		// ck.LogClientNoLock("Sending PutAppend(%s, %s) RPC call to server %d", key, value, i)
		time.Sleep(SLEEP_TIME)
		ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
		if !ok || reply.Err != OK {
			ck.LogClientNoLock("PutAppend(%s, %s) failed: %s", key, value, reply.Err)
			continue
		}

		// OK to return
		ck.mu.Lock()
		defer ck.mu.Unlock()
		ck.LogClientWithLock("PutAppend(%s, %s) success. Leader=%d", key, value, i)
		ck.leader = i
		return
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
