package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout 	   = "ErrTimeout"
	ErrSituation   = "ErrSituation"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	ClientId int64
	CommandId int
	OKey string
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	ClientId int64
	CommandId int
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}
