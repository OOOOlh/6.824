package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"
	"../labrpc"
	// "fmt"
	// "../util"
	// "strconv"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	mu                   sync.Mutex //锁
	lastLeader           int        //上一次RPC发现的主机id
	clientId             int64      //client唯一id
	lastAppliedCommandId int        //Command的唯一id
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
	ck.lastLeader = 0
    ck.clientId = nrand()
    ck.lastAppliedCommandId = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
func (ck *Clerk) Get(key string) string {
	commandId := ck.lastAppliedCommandId + 1
	args := GetArgs{
	   Key:       key,
	   ClientId:  ck.clientId,
	   CommandId: commandId,
	}
	reply := GetReply{}
	serverId := ck.lastLeader
	for{
		ok := ck.servers[serverId].Call("KVServer.Get", &args, &reply)
		if !ok || reply.Err == ErrTimeout || reply.Err == ErrWrongLeader|| reply.Err == ErrSituation{
			serverId = (serverId + 1) % len(ck.servers)
			continue
		}
		ck.lastLeader = serverId
		ck.lastAppliedCommandId = commandId
		if reply.Err == ErrNoKey {
			return ""
		}
      	return reply.Value
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	commandId := ck.lastAppliedCommandId + 1
	args := PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
		ClientId:  ck.clientId,
		CommandId: commandId,
	}
	reply := PutAppendReply{}
	serverId := ck.lastLeader
	for{
		// args.OKey = strconv.Itoa(serverId)
		// util.Debug(util.DClient, "S%d <- Client", serverId)
		ok := ck.servers[serverId].Call("KVServer.PutAppend", &args, &reply)
		// util.Debug(util.DClient, "S%d <- Client ok:%v err:%v", serverId, ok, reply.Err)
		if !ok || reply.Err == ErrTimeout || reply.Err == ErrWrongLeader || reply.Err == ErrSituation{
			serverId = (serverId + 1) % len(ck.servers)
			continue
		}
		ck.lastLeader = serverId
		ck.lastAppliedCommandId = commandId
		return
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
