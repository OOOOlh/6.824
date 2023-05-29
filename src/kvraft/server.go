package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"
	"bytes"
	// "fmt"

	"../labgob"
	"../labrpc"
	"../raft"
	"../util"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	CommandType string
	Key         string
	Value       string
	ClientId    int64
	CommandId   int
}


type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	toClientReply    map[int64]Commands          //clientId-Commands
    // kvDataBase     KvDataBase                  //数据库,可自行定义和更换
    // storeInterface store                       //数据库接口
	kvData 			 map[string]string
    replyChMap       map[int]chan ApplyNotifyMsg //某index的响应的chan
    lastApplied      int                         //上一条应用的log的index,防止快照导致回退
}


type ApplyNotifyMsg struct {
    Err   Err
    Value string
    Term int
}

type Commands struct {
    CommandId int            
    Reply   ApplyNotifyMsg
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
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
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	//all commited log will be send to applych 
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.toClientReply = make(map[int64]Commands)
	kv.kvData = make(map[string]string)
	kv.replyChMap =  make(map[int]chan ApplyNotifyMsg) 
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	util.Debug(util.DServer, "S%d start", kv.me)
	
	go kv.ReceiveApplyMsg()

	time.Sleep(300 * time.Millisecond)
	return kv
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	//if the command has not been executed, send command to raft
	op := Op{
	   CommandType: "Get",
	   Key:         args.Key,
	   ClientId:    args.ClientId,
	   CommandId:   args.CommandId,
	}

	// util.Debug(util.DServer, "S%d %s K:%s ClId:%d CoId:%d", kv.me, op.CommandType, op.Key, op.ClientId, op.CommandId)

	cmdIndex, term, isLeader := kv.rf.Start(op)
	//return if not leader
	if !isLeader {
	   reply.Err = ErrWrongLeader
	//    util.Debug(util.DServer, "S%d %s NLeader", kv.me, op.CommandType)
	   return
	}

	kv.mu.Lock()
	//the command whether is executed
	if commandContext, ok := kv.toClientReply[args.ClientId]; ok {
	   if commandContext.CommandId >= args.CommandId {
		  util.Debug(util.DServer, "S%d get used cmd R:%v", kv.me, commandContext)
		  reply.Err = commandContext.Reply.Err
		  reply.Value = commandContext.Reply.Value
		  kv.mu.Unlock()
		  return
	   }
	}
	kv.mu.Unlock()

	replyCh := make(chan ApplyNotifyMsg, 1)
	kv.mu.Lock()
	//update command receive channel
	kv.replyChMap[cmdIndex] = replyCh
	kv.mu.Unlock()

	util.Debug(util.DServer, "S%d %s K:%s ClId:%d CoId:%d WFC", kv.me, op.CommandType, op.Key, op.ClientId, op.CommandId)
	//wait for raft log commit
	select {
	case replyMsg := <-replyCh:
	   if term == replyMsg.Term {
		  reply.Err = replyMsg.Err
		  reply.Value = replyMsg.Value
	   } else {
		  reply.Err = ErrSituation
	   }
	case <-time.After(500 * time.Millisecond):
	   reply.Err = ErrTimeout
	   util.Debug(util.DServer, "S%d %s K:%s ClId:%d CoId:%d WFC TO", kv.me, op.CommandType, op.Key, op.ClientId, op.CommandId)
	}
	// //clear channel
	// go kv.CloseChan(index)
}

// func (kv *KVServer) CloseChan(index int) {
// 	ch := kv.replyChMap[index]
// 	for len(ch) > 0 {
// 		<-ch
// 	}
// 	ch.close()
// }

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	// util.Debug(util.DServer, "S%d <- PA", kv.me)
	//the command whether is executed
	if commandContext, ok := kv.toClientReply[args.ClientId]; ok {
	   if commandContext.CommandId == args.CommandId {
		util.Debug(util.DServer, "S%d PA used cmd R:%v", kv.me, commandContext)
		  reply.Err = commandContext.Reply.Err
		  kv.mu.Unlock()
		  return
	   }
	}
	kv.mu.Unlock()

	op := Op{
	   CommandType: args.Op,
	   Key:         args.Key,
	   Value:       args.Value,
	   ClientId:    args.ClientId,
	   CommandId:   args.CommandId,
	}
	index, term, isLeader := kv.rf.Start(op)

	util.Debug(util.DServer, "S%d %s K:%s ClId:%d CoId:%d WFC", kv.me, op.CommandType, op.Key, op.ClientId, op.CommandId)
	if !isLeader {
		util.Debug(util.DServer, "S%d %s NLeader", kv.me, op.CommandType)
	   reply.Err = ErrWrongLeader
	   return
	}

	replyCh := make(chan ApplyNotifyMsg, 1)
	kv.mu.Lock()
	kv.replyChMap[index] = replyCh
	kv.mu.Unlock()

	select {
	case replyMsg := <-replyCh:
	   if term == replyMsg.Term {
		  reply.Err = replyMsg.Err
	   } else {
		  reply.Err = ErrSituation
	   }
	case <-time.After(500 * time.Millisecond):
	   reply.Err = ErrTimeout
	   util.Debug(util.DServer, "S%d %s K:%s ClId:%d CoId:%d WFC TO", kv.me, op.CommandType, op.Key, op.ClientId, op.CommandId)
	}
	// go kv.CloseChan(index)
}

func (kv *KVServer) ReceiveApplyMsg() {
	for !kv.killed() {
	   	  applyMsg := <-kv.applyCh
		  if applyMsg.CommandValid {
			 util.Debug(util.DServer, "S%d rec applyMsg CI:%d CT:%d", kv.me, applyMsg.CommandIndex, applyMsg.CommandTerm)
			 kv.ApplyCommand(applyMsg)
		  } else if applyMsg.SnapshotValid {
			 kv.ApplySnapshot(applyMsg)
		  } else {
			 util.Debug(util.DServer, "S%d rec applyMsg invalid %v", kv.me, applyMsg)
		  }
	}
 }


 //when raft commit log, state machine will apply it first,then reply to client
 func (kv *KVServer) ApplyCommand(applyMsg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	var commonReply ApplyNotifyMsg

	//get command
	op := applyMsg.Command.(Op)
	// util.Debug(util.DServer, "S%d OP:%v", kv.me, op)
	cmdIndex := applyMsg.CommandIndex
	//当命令已经被应用过了
	if commands, ok := kv.toClientReply[op.ClientId]; ok && commands.CommandId >= op.CommandId {
		util.Debug(util.DServer, "S%d used cmd R:%v", kv.me, commonReply)
		commonReply = commands.Reply
	   return
	}

	//当命令未被应用过
	if op.CommandType == "Get" {
		if value, ok := kv.kvData[op.Key]; ok{
			commonReply = ApplyNotifyMsg{OK, value, applyMsg.CommandTerm}
		}else{
			commonReply = ApplyNotifyMsg{ErrNoKey, value, applyMsg.CommandTerm}
		}
		util.Debug(util.DServer, "S%d get cmd apply R:%v", kv.me, commonReply)
	//    //Get请求时
	//    if value, ok := kv.storeInterface.Get(op.Key); ok {
	// 	  //有该数据时
	// 	  commonReply = ApplyNotifyMsg{OK, value, applyMsg.CommandTerm}
	//    } else {
	// 	  //当没有数据时
	// 	  commonReply = ApplyNotifyMsg{ErrNoKey, value, applyMsg.CommandTerm}
	//    }
	} else if op.CommandType == "Put" {
	//    //Put请求时
	//    value := kv.storeInterface.Put(op.Key, op.Value)
	//    commonReply = ApplyNotifyMsg{OK, value, applyMsg.CommandTerm}
		kv.kvData[op.Key] = op.Value
		commonReply = ApplyNotifyMsg{OK, op.Value, applyMsg.CommandTerm}
		util.Debug(util.DServer, "S%d put cmd apply R:%v", kv.me, commonReply)
	} else if op.CommandType == "Append" {
	//    //Append请求时
	//    newValue := kv.storeInterface.Append(op.Key, op.Value)
	//    commonReply = ApplyNotifyMsg{OK, newValue, applyMsg.CommandTerm}

		if value, ok := kv.kvData[op.Key]; ok{
			kv.kvData[op.Key] = value + op.Value
		}else{
			kv.kvData[op.Key] = op.Value
		}
		commonReply = ApplyNotifyMsg{OK, op.Value, applyMsg.CommandTerm}
		util.Debug(util.DServer, "S%d append cmd apply R:%v", kv.me, commonReply)
	}

	//reply to client
	if replyCh, ok := kv.replyChMap[cmdIndex]; ok {
		util.Debug(util.DServer, "S%d RTClient R:%v", kv.me, commonReply)
	   replyCh <- commonReply
	}

	// value, _ := kv.storeInterface.Get(op.Key)
	// DPrintf("kvserver[%d]: 此时key=[%v],value=[%v]\n", kv.me, op.Key, value)

	kv.toClientReply[op.ClientId] = Commands{op.CommandId, commonReply}
	kv.lastApplied = applyMsg.CommandIndex
	//判断是否需要快照
	if kv.needSnapshot() {
	   kv.startSnapshot(applyMsg.CommandIndex)
	}
 }

 //判断当前是否需要进行snapshot(90%则需要快照)
func (kv *KVServer) needSnapshot() bool {
	if kv.maxraftstate == -1 {
	   return false
	}
	proportion := float32(kv.rf.GetRaftStateSize() / kv.maxraftstate)
	return proportion > 0.9
 }

 //生成server的状态的snapshot
func (kv *KVServer) createSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	//编码kv数据
	err := e.Encode(kv.kvData)
	if err != nil {
	   log.Fatalf("kvserver[%d]: encode kvData error: %v\n", kv.me, err)
	}
	//编码clientReply(为了去重)
	err = e.Encode(kv.toClientReply)
	if err != nil {
	   log.Fatalf("kvserver[%d]: encode clientReply error: %v\n", kv.me, err)
	}
	snapshotData := w.Bytes()
	return snapshotData
 }

 //主动开始snapshot(由leader在maxRaftState不为-1,而且目前接近阈值的时候调用)
func (kv *KVServer) startSnapshot(index int) {
	DPrintf("kvserver[%d]: 容量接近阈值,进行快照,rateStateSize=[%d],maxRaftState=[%d]\n", kv.me, kv.rf.GetRaftStateSize(), kv.maxraftstate)
	snapshot := kv.createSnapshot()
	DPrintf("kvserver[%d]: 完成service层快照\n", kv.me)
	//通知Raft进行快照
	go kv.rf.Snapshot(index, snapshot)
 }

 // ApplySnapshot 被动应用snapshot
func (kv *KVServer) ApplySnapshot(msg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("kvserver[%d]: 接收到leader的快照\n", kv.me)
	// if msg.SnapshotIndex < kv.lastApplied {
	//    DPrintf("kvserver[%d]: 接收到旧的日志,snapshotIndex=[%d],状态机的lastApplied=[%d]\n", kv.me, msg.SnapshotIndex, kv.lastApplied)
	//    return
	// }
	// if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
	//    kv.lastApplied = msg.SnapshotIndex
	//    //将快照中的service层数据进行加载
	//    kv.readSnapshot(msg.Snapshot)
	//    DPrintf("kvserver[%d]: 完成service层快照\n", kv.me)
	// }
 }

 func (kv *KVServer) readSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
	   return
	}
	// r := bytes.NewBuffer(snapshot)
	// d := labgob.NewDecoder(r)
	// var kvDataBase KvDataBase
	// var clientReply map[int64]Commands
	// if d.Decode(&kvDataBase) != nil || d.Decode(&clientReply) != nil {
	//    DPrintf("kvserver[%d]: decode error\n", kv.me)
	// } else {
	//    kv.kvDataBase = kvDataBase
	//    kv.clientReply = clientReply
	//    kv.storeInterface = &kvDataBase
	// }
 }
 
 
