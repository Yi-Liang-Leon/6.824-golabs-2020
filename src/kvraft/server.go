package kvraft

import (
	"lab/src/labgob"
	"lab/src/labrpc"
	"lab/src/raft"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
)

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()
	killSig chan struct{}

	maxraftstate int // snapshot if log grows this big

	// TODO: Your definitions here.
	db map[string]string // will be updated and read when applyCh arrives

	// Channel dispatcher to certain client call based on log index
	clientCallChs map[int]chan ClientMsg

	// For idempotent calling
	previousGetReplys map[ClientCallID]string
	previousPutAppend map[ClientCallID]struct{}

	// For logging
	logger logrus.Logger
	once   sync.Once
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// TODO: Your code here.
	kv.mu.Lock()
	kv.LogServerWithLock("Get(%v)", args)

	// If replied before
	if prevReply, ok := kv.previousGetReplys[args.Id]; ok {
		reply.Err = OK
		reply.Value = prevReply
		return
	}
	command := Command{GET, args.Key, ""}
	index, _, isLeader := kv.rf.Start(command)
	if !isLeader { // If isn't leader return directly
		reply.Err = ErrWrongLeader
		return
	}
	timer := time.NewTimer(MAX_TIMEOUT)
	callCh := make(chan ClientMsg)
	kv.clientCallChs[index] = callCh // Attach channel to dispatcher
	kv.mu.Unlock()

	select {
	case msg := <-callCh:
		reply.Err = msg.Err
		reply.Value = msg.Value
		kv.previousGetReplys[args.Id] = msg.Value
		delete(kv.clientCallChs, index)
		return
	case <-timer.C:
		reply.Err = ErrTimeout
		return
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// TODO: Your code here.
	kv.mu.Lock()
	kv.LogServerWithLock("PutAppend(%v)", args)

	// If replied before
	if _, ok := kv.previousPutAppend[args.Id]; ok {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	command := Command{args.Op, args.Key, args.Value}
	index, _, isLeader := kv.rf.Start(command)
	if !isLeader { // If isn't leader return directly
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	timer := time.NewTimer(MAX_TIMEOUT)
	callCh := make(chan ClientMsg)
	kv.clientCallChs[index] = callCh // Attach channel to dispatcher
	kv.mu.Unlock()

	select {
	case msg := <-callCh:
		kv.LogServerNoLock("PutAppend(%v) ok to return", args)
		reply.Err = msg.Err
		delete(kv.clientCallChs, index)
		return
	case <-timer.C:
		kv.LogServerNoLock("PutAppend(%v) timeout", args)
		reply.Err = ErrTimeout
		delete(kv.clientCallChs, index)
		return
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.LogServerNoLock("KILLED!")
	close(kv.killSig)
	kv.rf.Kill()
	// TODO: Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall
	labgob.Register(Command{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.db = make(map[string]string)
	kv.previousGetReplys = make(map[ClientCallID]string)
	kv.clientCallChs = make(map[int]chan ClientMsg)

	// TODO: You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.killSig = make(chan struct{})

	// You may need initialization code here.
	kv.once.Do(kv.initLog)
	kv.LogServerNoLock("Starting server %d", me)

	go kv.applyHandler()

	return kv
}

func (kv *KVServer) applyHandler() {
	for {
		select {
		case <-kv.killSig:
			return
		case applyMsg := <-kv.applyCh:
			command := applyMsg.Command.(Command)
			var msg ClientMsg
			switch command.Op {
			case GET:
				kv.mu.Lock()
				kv.LogServerWithLock("Received applyMsg GET(%s)", command.Key)
				value, ok := kv.db[command.Key]
				if !ok {
					msg.Err = ErrNoKey
				} else {
					msg.Err = OK
					msg.Value = value
				}
				if _, ok := kv.clientCallChs[applyMsg.CommandIndex]; ok {
					kv.clientCallChs[applyMsg.CommandIndex] <- msg
				}
				kv.mu.Unlock()
			case PUT, APPEND:
				kv.mu.Lock()
				kv.LogServerWithLock("Received applyMsg %s(%s,%s)", command.Op, command.Key, command.Value)
				if command.Op == PUT {
					kv.db[command.Key] = command.Value
				} else {
					kv.db[command.Key] += command.Value
				}
				msg.Err = OK
				if _, ok := kv.clientCallChs[applyMsg.CommandIndex]; ok {
					kv.clientCallChs[applyMsg.CommandIndex] <- msg
				}
				kv.mu.Unlock()
			}
		}
	}
}
