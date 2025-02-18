package kvraft

import "time"

const (
	OK             Err = "OK"
	ErrNoKey       Err = "ErrNoKey"
	ErrWrongLeader Err = "ErrWrongLeader"
	ErrTimeout     Err = "ErrTimeout"
	GET            Op  = "Get"
	PUT            Op  = "Put"
	APPEND         Op  = "Append"
	MAX_TIMEOUT        = 1 * time.Second
	SLEEP_TIME         = 100 * time.Millisecond
)

type Err string
type Op string
type ClientCallID int64

type Command struct {
	// TODO: Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op    Op
	Key   string
	Value string
}

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    Op // "Put" or "Append"
	// TODO: You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Id ClientCallID
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// TODO: You'll have to add definitions here.
	Id ClientCallID
}

type GetReply struct {
	Err   Err
	Value string
}

type ClientMsg struct {
	Value string
	Err   Err
}
