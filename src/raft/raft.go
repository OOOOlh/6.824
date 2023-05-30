package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"math/rand"

	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
	"../util"
)

// func init() {
// 	t := time.Now().Unix()
// 	serverName = strconv.FormatInt(t, 10)
// 	file := "./" + serverName + "message" + ".log"
// 	logFile, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0766)
// 	if err != nil {
// 		panic(err)
// 	}
// 	log.SetOutput(logFile) // 将文件设置为log输出的文件
// 	log.SetPrefix("[raft]")
// 	log.SetFlags(log.LstdFlags | log.Lshortfile | log.LUTC)
// 	return
// }

// // Retrieve the verbosity level from an environment variable
// func getVerbosity() int {
// 	v := os.Getenv("VERBOSE")
// 	level := 0
// 	if v != "" {
// 		var err error
// 		level, err = strconv.Atoi(v)
// 		if err != nil {
// 			log.Fatalf("Invalid verbosity %v", v)
// 		}
// 	}
// 	return level
// }

// type logTopic string

// const (
// 	dClient  logTopic = "CLNT"
// 	dCommit  logTopic = "CMIT"
// 	dDrop    logTopic = "DROP"
// 	dError   logTopic = "ERRO"
// 	dInfo    logTopic = "INFO"
// 	dLeader  logTopic = "LEAD"
// 	dFoll    logTopic = "FOLL"
// 	dCand    logTopic = "CAND"
// 	dLog     logTopic = "LOG1"
// 	dLog2    logTopic = "LOG2"
// 	dPersist logTopic = "PERS"
// 	dSnap    logTopic = "SNAP"
// 	dTerm    logTopic = "TERM"
// 	dTest    logTopic = "TEST"
// 	dTimer   logTopic = "TIMR"
// 	dTrace   logTopic = "TRCE"
// 	dVote    logTopic = "VOTE"
// 	dAppE    logTopic = "APPE"
// 	dWarn    logTopic = "WARN"
// )

// var debugStart time.Time
// var debugVerbosity int

// func init() {
// 	debugVerbosity = getVerbosity()
// 	debugStart = time.Now()
// 	// t := time.Now().Unix()
// 	// serverName = strconv.FormatInt(t, 10)
// 	// file := "./" + serverName + "message" + ".log"
// 	// logFile, err := os.OpenFile(file, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
// 	// if err != nil {
// 	// 	panic(err)
// 	// }
// 	// log.SetOutput(logFile) // 将文件设置为log输出的文件
// 	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
// }

// func Debug(topic logTopic, format string, a ...interface{}) {
// 	if debugVerbosity >= 1 {
// 		time := time.Since(debugStart).Microseconds()
// 		time /= 100
// 		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
// 		format = prefix + format
// 		log.Printf(format, a...)
// 	}
// }

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int

	Snapshot []byte
	SnapshotValid bool
	SnapshotIndex int
	SnapshotTerm int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	applyCh   chan ApplyMsg
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	CurrentTerm int
	VotedFor    int
	Log         []Entry
	Snapshots    []byte

	commitIndex int
	lastApplied int

	lastLogIndex int
	// LastSnapSIndex int

	nextIndex  []int
	matchIndex []int

	//0: follower
	//1: candidate
	//2: leader
	role int

	follower  Follower
	candidate Candidate
	kill      bool
}

type PersistentState struct {
	CurrentTerm int
	VotedFor    int
	Log         []Entry
}

type Entry struct {
	Command interface{}
	Term    int
}

type Follower struct {
	//channel for AppendEntries
	recAE chan int

	//channel for ReqiestVote
	recRV chan int
}

type Candidate struct {
	//channel for AppendEntries
	recAE chan int
}

func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	rf.mu.Lock()
	if rf.role == 2 {
		isleader = true
	}
	term = rf.CurrentTerm
	rf.mu.Unlock()
	return term, isleader
}

func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	ps := &PersistentState{
		CurrentTerm: rf.CurrentTerm,
		VotedFor:    rf.VotedFor,
		Log:         rf.Log,
	}
	e.Encode(ps)
	data := w.Bytes()
	rf.persister.SaveStateAndSnapshot(data, rf.Snapshots)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var persistentState PersistentState
	if err := d.Decode(&persistentState); err != nil {
		util.Debug(util.DError, "S%d T:%d R:%d -> readPersist", rf.me, rf.CurrentTerm, rf.role)
	} else {
		rf.CurrentTerm = persistentState.CurrentTerm
		rf.VotedFor = persistentState.VotedFor
		rf.Log = persistentState.Log
	}
	util.Debug(util.DInfo, "S%d T:%d R:%d -> readPersist VF:%d CT:%d L0:%v L:%v",
		rf.me, rf.CurrentTerm, rf.role, rf.VotedFor, rf.CurrentTerm, rf.Log[1], rf.Log[len(rf.Log)-2])
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	XTerm      int
	XIndex     int
	XLastIndex int
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.Snapshot", args, reply)
	return ok
}

func (rf *Raft) Snapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer func() {
		reply.Term = rf.CurrentTerm
	}()

	if args.Term < rf.CurrentTerm {
		return 
	}

	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.VotedFor = -1
		rf.persist()
	}

	rf.role = 0
	clearChannel(rf.follower.recRV)
	rf.follower.recRV <- 1

	//snapshot expire
	if args.LastIncludedIndex <= rf.commitIndex {
		return 
	}

	util.Debug(util.DFoll, "S%d T:%d R:%d <- snapshot", rf.me, rf.CurrentTerm, rf.role)
	//applyCh -> service
	applyMsg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotIndex: args.LastIncludedIndex,
		SnapshotTerm:  args.LastIncludedTerm,
	}

	go func(msg ApplyMsg) {
		rf.applyCh <- msg
	}(applyMsg)
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	rf.mu.Lock()
	isLeader := true
	term := rf.CurrentTerm

	if rf.role == 2 {
		entry := Entry{command, rf.CurrentTerm}

		util.Debug(util.DClient, "S%d T:%d R:%d <- C:%v", rf.me, rf.CurrentTerm, rf.role, entry.Command)
		
		//update log, lastLogIndex, nextIndex
		rf.Log[rf.lastLogIndex+1 - rf.getLastSnapsIndex()] = entry
		rf.Log = append(rf.Log, Entry{nil, -1})
		rf.persist()
		util.Debug(util.DPersist, "S%d T:%d R:%d -> S%d P L0:%v L:%v",
			rf.me, rf.CurrentTerm, rf.role, rf.me, rf.Log[1], rf.Log[len(rf.Log)-1])

		rf.lastLogIndex = len(rf.Log) - 2 + rf.getLastSnapsIndex()
		//some followers may don't catch the leader, so the nextIndex is not equals to rf.lastLogIndex,
		//actually, these followers will return false
		for index := range rf.nextIndex {
			rf.nextIndex[index] = rf.lastLogIndex + 1
		}
		//if there are many request from client at the same time, then there will start many goroutine to request entries
	} else {
		isLeader = false
	}
	index = rf.lastLogIndex
	rf.mu.Unlock()
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		applyCh:      applyCh,
		peers:        peers,
		persister:    persister,
		me:           me,
		role:         0,
		CurrentTerm:  0,
		VotedFor:     -1,
		Log:          make([]Entry, 0),
		commitIndex:  0,
		nextIndex:    make([]int, len(peers)),
		matchIndex:   make([]int, len(peers)),
	}
	//fill the index 0, because the log index begin with 1
	rf.Log = append(rf.Log, Entry{nil, -1})
	//pre append
	rf.Log = append(rf.Log, Entry{nil, -1})

	rf.follower.recAE = make(chan int, 1)
	rf.follower.recRV = make(chan int, 1)
	rf.candidate.recAE = make(chan int, 1)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.Snapshots = persister.ReadSnapshot()
	rf.lastLogIndex = len(rf.Log) - 2 + rf.getLastSnapsIndex()

	rf.commitIndex = rf.getLastSnapsIndex()
	rf.lastApplied = rf.getLastSnapsIndex()

	go StateListening(rf)
	go rf.applyLog()

	return rf
}

func (rf *Raft) getLastSnapsIndex() int{
	lastSnapshotIndex := 0
	if rf.Log[0].Command != nil{
		lastSnapshotIndex = rf.Log[0].Command.(int)
	}
	return lastSnapshotIndex
}

func (rf *Raft) applyLog() {
	delay := false
	for !rf.killed() {
		if delay {
			time.Sleep(10 * time.Millisecond)
		}
		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			delay = true
			if rf.commitIndex > rf.lastApplied {
				rf.lastApplied++
				applyMsg := ApplyMsg{
					CommandValid: true,
					Command:      rf.Log[rf.lastApplied - rf.getLastSnapsIndex()].Command,
					CommandIndex: rf.lastApplied,
					CommandTerm:  rf.Log[rf.lastApplied - rf.getLastSnapsIndex()].Term,
				}

				rf.applyCh <- applyMsg
				util.Debug(util.DCommit, "S%d T:%d R:%d -> RCI:%d RCIL:%v",
					rf.me, rf.CurrentTerm, rf.role, rf.lastApplied, rf.Log[rf.lastApplied - rf.getLastSnapsIndex()])
				delay = false
			}
		}()
	}
}

func StateListening(rf *Raft) {
	var role int
	var me int
	var term int
	var c0 interface{}
	var c interface{}
	for !rf.killed() {
		rf.mu.Lock()
		role = rf.role
		me = rf.me
		term = rf.CurrentTerm
		c0 = rf.Log[1].Command
		c = rf.Log[len(rf.Log)-2].Command
		rf.mu.Unlock()

		if role == 0 {
			util.Debug(util.DFoll, "S%d T:%d R:%d -> Follower S%d L0:%v L:%v", me, term, role, me, c0, c)

			var followerElectionTimeoutChan chan int = make(chan int, 1)
			go electionTimeout(followerElectionTimeoutChan)

			select {
			case <-followerElectionTimeoutChan:
				rf.mu.Lock()
				//convert to candidate
				rf.role = 1
				util.Debug(util.DFoll, "S%d T:%d R:%d -> S%d CTo R%d for ETO", rf.me, rf.CurrentTerm, 0, rf.me, rf.role)
				rf.VotedFor = rf.me
				rf.CurrentTerm++
				rf.persist()
				// Debug(dPersist, "S%d T:%d R:%d -> S%d P L0:%v L:%v",
				// 	rf.me, rf.CurrentTerm, rf.role, rf.me, rf.Log[1], rf.Log[len(rf.Log)-2])
				rf.mu.Unlock()
				break
			case <-rf.follower.recAE:
				util.Debug(util.DFoll, "S%d T:%d R:%d -> reset ETO for AE", me, term, role)
				//receive AppendEntries Request, reset timeout
				break
			case <-rf.follower.recRV:
				util.Debug(util.DFoll, "S%d T:%d R:%d -> reset ETO for RV", me, term, role)
				//receive RequestVote Request, reset timeout
				break
			}
		} else if role == 1 {
			util.Debug(util.DCand, "S%d T:%d R:%d -> Candi S%d L0:%v L:%v", me, term, role, me, c0, c)

			var attemptRequestVoteChan chan int = make(chan int, 1)
			var candidateTimeoutChan chan int = make(chan int, 1)
			go candidateRequestTicker(candidateTimeoutChan)

			var electionTimeoutChan chan int = make(chan int, 1)
			go electionTimeout(electionTimeoutChan)
			tim := time.Now().Nanosecond()

		Loop:
			for {
				go rf.AttemptRequestVote(attemptRequestVoteChan)
				select {
				case <-candidateTimeoutChan:
				case <-rf.candidate.recAE:
					//receive AppendEntries Request and leader term >= candidate term, reset timeout
					break Loop
				case <-attemptRequestVoteChan:
					//request vote and receive votes, maybe convert to leader, if there is one server timeout, this channel will wait
					break Loop
				case <-electionTimeoutChan:
					rf.mu.Lock()
					util.Debug(util.DInfo, "S%d T:%d R:%d -> time:%v ",
						rf.me, rf.CurrentTerm, rf.role, time.Now().Nanosecond()-tim)
					rf.VotedFor = rf.me
					rf.CurrentTerm++
					rf.persist()
					// Debug(dPersist, "S%d T:%d R:%d -> S%d P L0:%v L:%v",
					// 	rf.me, rf.CurrentTerm, rf.role, rf.me, c0, c)
					util.Debug(util.DCand, "S%d T:%d R:%d -> S%d term++", me, rf.CurrentTerm, role, me)
					rf.mu.Unlock()
					break Loop
				}
			}

		} else {
			util.Debug(util.DLeader, "S%d T:%d R:%d -> Leader S%d L0:%v L:%v", me, term, role, me, c0, c)
			go rf.AttemptSendAppendEntries()

			//wait heartbeat timeout, 200ms can pass TestFigure82C, 180ms can't
			var heartbeatTimeout time.Duration = time.Duration(60) * time.Millisecond
			time.Sleep(heartbeatTimeout)
		}
		if rf.killed() {
			break
		}
	}
}

// func (rf *Raft) convertToFollower(term int) {
// 	rf.mu.Lock()
// 	util.Debug(util.DInfo, "S%d T:%d R:%d -> S%d CTF", rf.me, rf.CurrentTerm, rf.role, rf.me)
// 	rf.role = 0
// 	rf.VotedFor = -1
// 	rf.CurrentTerm = term
// 	rf.persist()
// 	util.Debug(util.DPersist, "S%d T:%d R:%d -> S%d P L0:%v L:%v",
// 		rf.me, rf.CurrentTerm, rf.role, rf.me, rf.Log[1], rf.Log[len(rf.Log)-2])
// 	rf.mu.Unlock()
// }

func (rf *Raft) AttemptSendAppendEntries() {
	argeeNums := 1
	var wg sync.WaitGroup
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		wg.Add(1)
		go rf.SendAppendEntriesToOneServer(i, &argeeNums, &wg)
	}
	wg.Wait()
}

func (rf *Raft) SendAppendEntriesToOneServer(server int, argeeNums *int, wg *sync.WaitGroup) {
	defer func() {
		wg.Done()
	}()

	reply := &AppendEntriesReply{}

	rf.mu.Lock()
	if rf.role != 2 {
		rf.mu.Unlock()
		return
	}

	util.Debug(util.DSnap, "S%d T:%d R:%d -> S%d snap NI:%d LSI:%d", 
	rf.me, rf.CurrentTerm, rf.role, server, rf.nextIndex[server], rf.getLastSnapsIndex())

	if rf.nextIndex[server] <= rf.getLastSnapsIndex(){
		args := InstallSnapshotArgs{
			Term:              rf.CurrentTerm,
			LeaderId:          rf.me,
			LastIncludedIndex: rf.getLastSnapsIndex(),
			LastIncludedTerm:  rf.Log[0].Term,
			Data:              rf.Snapshots,
		}

		reply := InstallSnapshotReply{}
		util.Debug(util.DSnap, "S%d T:%d R:%d -> S%d snap I:%d T:%d", 
			rf.me, rf.CurrentTerm, rf.role, server, args.LastIncludedIndex, args.LastIncludedTerm)
		rf.mu.Unlock()

		ok := rf.sendSnapshot(server, &args, &reply)

		rf.mu.Lock()

		if rf.role != 2 || args.Term != rf.CurrentTerm || !ok{
			rf.mu.Unlock()
			return
		}

		if reply.Term > rf.CurrentTerm {
			rf.CurrentTerm = reply.Term
			rf.role = 0
			rf.VotedFor = -1
			rf.persist()
			rf.mu.Unlock()
			return
		}

		rf.matchIndex[server] = args.LastIncludedIndex
		rf.nextIndex[server] = rf.matchIndex[server] + 1
		rf.mu.Unlock()
		return
	}

	//send heartbeats
	args := &AppendEntriesArgs{
		LeaderCommit: rf.commitIndex,
		LeaderId:     rf.me,
		PrevLogIndex: rf.nextIndex[server] - 1,
		PrevLogTerm:  rf.Log[rf.nextIndex[server]-1 - rf.getLastSnapsIndex()].Term,
		Term:         rf.CurrentTerm,
	}
	
	var entries = make([]Entry, 0)
	entries = append(entries, rf.Log[rf.nextIndex[server] - rf.getLastSnapsIndex():]...)
	args.Entries = entries
	util.Debug(util.DAppE, "S%d T:%d R:%d -> S%d AE PLI: %d PLT: %d LC: %d",
		rf.me, rf.CurrentTerm, rf.role, server, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit)

	rf.mu.Unlock()

	ok := rf.sendAppendEntries(server, args, reply)

	rf.mu.Lock()
	if !ok || rf.role != 2 || rf.CurrentTerm != args.Term {
		// Debug(dAppE, "S%d T:%d R:%d <- S%d expired rpc AE", rf.me, rf.CurrentTerm, rf.role, server)
		rf.mu.Unlock()
		return
	}

	//ok
	if reply.Success {
		(*argeeNums)++
		util.Debug(util.DAppE, "S%d T:%d R:%d -> S%d AE RSucc AN:%d", rf.me, rf.CurrentTerm, rf.role, server, *argeeNums)
		rf.nextIndex[server] = args.PrevLogIndex + len(entries)
		rf.matchIndex[server] = rf.nextIndex[server] - 1

		for commitN := rf.lastLogIndex; commitN > rf.commitIndex; commitN-- {
			num := 1
			for i, index := range rf.matchIndex {
				if i == rf.me {
					continue
				}
				if index >= commitN {
					num++
				}
			}
			if num > len(rf.peers)/2 && rf.Log[commitN - rf.getLastSnapsIndex()].Term == rf.CurrentTerm {
				rf.commitIndex = commitN
				break
			}
		}
	} else {
		//if not success, check the term, XTerm, XIndex, XLastIndex
		if rf.CurrentTerm < reply.Term {
			util.Debug(util.DAppE, "S%d T:%d R:%d <- S%d AE R RT:%d > CT:%d",
				rf.me, rf.CurrentTerm, rf.role, server, reply.Term, rf.CurrentTerm)

			util.Debug(util.DInfo, "S%d T:%d R:%d <- S%d AE R CToF", rf.me, rf.CurrentTerm, rf.role, rf.me)
			rf.role = 0
			rf.VotedFor = -1
			rf.CurrentTerm = reply.Term
			rf.persist()
			// Debug(dPersist, "S%d T:%d R:%d -> S%d P L0:%v L:%v",
			// 	rf.me, rf.CurrentTerm, rf.role, rf.me, rf.Log[1], rf.Log[len(rf.Log)-2])
			rf.mu.Unlock()
			return
		} else if reply.XTerm != -1 {
			rf.nextIndex[server] = reply.XIndex
			util.Debug(util.DAppE, "S%d T:%d  R:%d <- S%d AE R RXI:%d",
				rf.me, rf.CurrentTerm, rf.role, server, reply.XIndex)
		} else {
			//rf.nextIndex[server]> reply.XLastIndex, so it won't cross the border
			rf.nextIndex[server] = reply.XLastIndex
			util.Debug(util.DAppE, "S%d T:%d  R:%d -> S%d AE R RXL:%d",
				rf.me, rf.CurrentTerm, rf.role, server, reply.XLastIndex)
		}
	}
	rf.mu.Unlock()

}

//follower receive
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	reply.Success = false
	
	lastSnapSIndex := rf.getLastSnapsIndex()
	util.Debug(util.DAppE, "S%d T:%d R:%d <- S%d AE PLI:%d LLI:%d LSS:%d", rf.me, rf.CurrentTerm, rf.role, args.LeaderId, args.PrevLogIndex, rf.lastLogIndex, lastSnapSIndex)

	if args.Term < rf.CurrentTerm {
		util.Debug(util.DAppE, "S%d T:%d R:%d -> S%d AE RCTerm:%d > ATerm:%d", rf.me, rf.CurrentTerm, rf.role, args.LeaderId, rf.CurrentTerm, args.Term)
		reply.Term = rf.CurrentTerm
		rf.mu.Unlock()
		return
	} else if args.PrevLogIndex > rf.lastLogIndex {
		reply.XTerm = -1
		reply.XIndex = -1
		reply.XLastIndex = rf.lastLogIndex + 1
		util.Debug(util.DAppE, "S%d T:%d R:%d -> S%d AE APL:%d > RLLI:%d RXLI:%d",
			rf.me, rf.CurrentTerm, rf.role, args.LeaderId, args.PrevLogIndex, rf.lastLogIndex, reply.XLastIndex)
	} else if rf.lastLogIndex != 0 && args.PrevLogIndex > lastSnapSIndex && rf.Log[args.PrevLogIndex - lastSnapSIndex].Term != args.PrevLogTerm {
		reply.XTerm = rf.Log[args.PrevLogIndex - lastSnapSIndex].Term
		index := args.PrevLogIndex - lastSnapSIndex
		for index >= 1 && rf.Log[index].Term != 0 && rf.Log[index].Term == rf.Log[args.PrevLogIndex - lastSnapSIndex].Term {
			reply.XIndex = index
			index--
		}

		util.Debug(util.DAppE, "S%d T:%d R:%d -> S%d AE RLT:%d != APLT:%d RXT:%d RXI:%d",
			rf.me, rf.CurrentTerm, rf.role, args.LeaderId, rf.Log[args.PrevLogIndex - lastSnapSIndex].Term, args.PrevLogTerm, reply.XTerm, reply.XLastIndex)
	} else{
		reply.Success = true
		//if leadear send empty command to follower,and they has same log, then don't need to update
		if args.Entries[0].Command != nil {
			//delete after rf.lastLogIndex
			// rf.Log = rf.Log[:args.PrevLogIndex+1 - lastSnapSIndex]
			// rf.Log = append(rf.Log, args.Entries...)
			// rf.lastLogIndex = len(rf.Log) - 2 + lastSnapSIndex
			// rf.persist()
			// Debug(dAppE, "S%d T:%d  R:%d -> S%d AP success PLI: %d PLT: %d LC: %d ",
			// 	rf.me, rf.CurrentTerm, rf.role, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit)
			
			//追加
			for i, logEntry := range args.Entries {
				index := args.PrevLogIndex + i + 1
				if index > rf.lastLogIndex {
					rf.Log = append(rf.Log[:len(rf.Log) - 1], logEntry)
				} else if index <= rf.getLastSnapsIndex() {
					//当追加的日志处于快照部分,那么直接跳过不处理该日志
					continue
				} else {
					if rf.Log[index - lastSnapSIndex].Term != logEntry.Term {
						rf.Log = rf.Log[:index - lastSnapSIndex]
						rf.Log = append(rf.Log, logEntry)						
					}
					// term一样啥也不用做，继续向后比对Log
				}
				if rf.Log[len(rf.Log) - 1].Command != nil{
					rf.Log = append(rf.Log, Entry{nil, -1})
				}
				rf.lastLogIndex = len(rf.Log) - 2 + lastSnapSIndex
				rf.persist()
			}
		}


		if rf.commitIndex < args.LeaderCommit {
			minIndex := args.LeaderCommit
			if args.LeaderCommit > rf.lastLogIndex {
				minIndex = rf.lastLogIndex
			}
			rf.commitIndex = minIndex
		}
	}

	//update term
	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.persist()
		// Debug(dPersist, "S%d T:%d R:%d -> S%d P L0:%v L:%v",
		// 	rf.me, rf.CurrentTerm, rf.role, rf.me, rf.Log[1], rf.Log[len(rf.Log)-2])
	}
	reply.Term = rf.CurrentTerm

	//if follower receive AppendEntries, reset election timeout
	if rf.role == 0 {
		clearChannel(rf.follower.recAE)
		rf.follower.recAE <- 1
		//if candidate receive AppendEntries, if leader term >= candidate term, candidate convert to follower,
	} else {
		// rf.convertToFollower(args.Term)
		util.Debug(util.DInfo, "S%d T:%d R:%d -> S%d AE CToF", rf.me, rf.CurrentTerm, rf.role, rf.me)
		rf.role = 0
		rf.VotedFor = -1
		rf.CurrentTerm = args.Term
		rf.persist()
		// Debug(dPersist, "S%d T:%d R:%d -> S%d P L0:%v L:%v",
		// 	rf.me, rf.CurrentTerm, rf.role, rf.me, rf.Log[1], rf.Log[len(rf.Log)-2])
		if rf.role == 1 {
			clearChannel(rf.candidate.recAE)
			rf.candidate.recAE <- 1
		}
	}
	rf.mu.Unlock()
}

func (rf *Raft) AttemptRequestVote(ch chan int) {
	var votes int = 1
	var wg sync.WaitGroup
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		wg.Add(1)
		go func(server int) {
			defer func() {
				wg.Done()
			}()

			rf.mu.Lock()
			args := &RequestVoteArgs{
				CandidateId:  rf.me,
				LastLogIndex: rf.lastLogIndex,
				LastLogTerm:  rf.Log[rf.lastLogIndex - rf.getLastSnapsIndex()].Term,
				Term:         rf.CurrentTerm,
			}
			if rf.role != 1 {
				rf.mu.Unlock()
				return
			}
			util.Debug(util.DVote, "S%d T:%d R:%d -> S%d RV LLI: %d LLT: %d",
				rf.me, rf.CurrentTerm, rf.role, server, args.LastLogIndex, args.LastLogTerm)
			rf.mu.Unlock()

			reply := &RequestVoteReply{}

			ok := rf.sendRequestVote(server, args, reply)

			//receive reply
			rf.mu.Lock()
			if !ok || rf.role != 1 || rf.CurrentTerm != args.Term {
				// Debug(dVote, "S%d T:%d R:%d <- S%d expired rpc RV", rf.me, rf.CurrentTerm, rf.role, server)
				rf.mu.Unlock()
				return
			}

			if reply.Term > rf.CurrentTerm {
				util.Debug(util.DVote, "S%d T:%d R:%d <- S%d RV R RT:%d > CT:%d",
					rf.me, rf.CurrentTerm, rf.role, server, reply.Term, rf.CurrentTerm)
				util.Debug(util.DInfo, "S%d T:%d R:%d <- S%d RV R CToF", rf.me, rf.CurrentTerm, rf.role, rf.me)
				rf.role = 0
				rf.VotedFor = -1
				rf.CurrentTerm = args.Term
				rf.persist()
				// Debug(dPersist, "S%d T:%d R:%d -> S%d P L0:%v L:%v",
				// 	rf.me, rf.CurrentTerm, rf.role, rf.me, rf.Log[1], rf.Log[len(rf.Log)-2])
				rf.mu.Unlock()
				return
			}

			if reply.VoteGranted {
				votes++
				util.Debug(util.DVote, "S%d T:%d R:%d <- S%d RV R RVG votes:%d all:%d", rf.me, rf.CurrentTerm, rf.role, server, votes, len(rf.peers))
				if votes > len(rf.peers)/2 {
					rf.role = 2
					for i := range rf.peers {
						rf.nextIndex[i] = len(rf.Log) - 1 + rf.getLastSnapsIndex()
						rf.matchIndex[i] = -1
					}
					if rf.Log[len(rf.Log)-1].Command != nil {
						rf.Log = append(rf.Log, Entry{nil, -1})
						rf.persist()
					}
					//if get enough votes, convert to leader immediately
					ch <- 1
					rf.mu.Unlock()
					return
				}
			} else {
				util.Debug(util.DVote, "S%d T:%d R:%d <- S%d RV R RVNG votes:%d all:%d", rf.me, rf.CurrentTerm, rf.role, server, votes, len(rf.peers))
			}
			rf.mu.Unlock()
		}(i)
	}
	wg.Wait()
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	util.Debug(util.DVote, "S%d T:%d R:%d <- S%d RV vote req", rf.me, rf.CurrentTerm, rf.role, args.CandidateId)

	if args.Term > rf.CurrentTerm {
		util.Debug(util.DVote, "S%d T:%d R:%d -> S%d RV vote granted ", rf.me, rf.CurrentTerm, rf.role, args.CandidateId)
		// rf.convertToFollower(args.Term)
		if rf.role != 0 {
			util.Debug(util.DVote, "S%d T:%d R:%d -> S%d RV CToF", rf.me, rf.CurrentTerm, rf.role, rf.me)
			rf.role = 0
		}
		rf.CurrentTerm = args.Term
		rf.VotedFor = -1
		rf.persist()
		// Debug(dPersist, "S%d T:%d R:%d -> S%d P L0:%v L:%v",
		// 	rf.me, rf.CurrentTerm, rf.role, rf.me, rf.Log[1], rf.Log[len(rf.Log)-2])
	}

	lastSnapSIndex := rf.getLastSnapsIndex()
	if rf.CurrentTerm == args.Term && (rf.VotedFor == -1 || rf.VotedFor == args.CandidateId) &&
		(args.LastLogTerm > rf.Log[rf.lastLogIndex - lastSnapSIndex].Term ||
			(args.LastLogTerm == rf.Log[rf.lastLogIndex - lastSnapSIndex].Term && args.LastLogIndex >= rf.lastLogIndex)) {
				util.Debug(util.DVote, "S%d T:%d R:%d -> S%d RV vote for", rf.me, rf.CurrentTerm, rf.role, args.CandidateId)
		if rf.role == 0 {
			clearChannel(rf.follower.recRV)
			rf.follower.recRV <- 1
		}
		rf.VotedFor = args.CandidateId
		rf.persist()
		// Debug(dPersist, "S%d T:%d R:%d -> S%d P L0:%v L:%v",
		// 	rf.me, rf.CurrentTerm, rf.role, rf.me, rf.Log[1], rf.Log[len(rf.Log)-2])
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}

	if rf.CurrentTerm < args.Term {
		rf.CurrentTerm = args.Term
		rf.persist()
		// Debug(dPersist, "S%d T:%d R:%d -> S%d P L0:%v L:%v",
		// 	rf.me, rf.CurrentTerm, rf.role, rf.me, rf.Log[1], rf.Log[len(rf.Log)-2])
	}
	reply.Term = rf.CurrentTerm
	rf.mu.Unlock()
}


func electionTimeout(ch chan int) {
	//random timeout
	rand.Seed(time.Now().UnixNano())
	r := rand.Intn(100) + 250

	var electionTimeout time.Duration = time.Duration(r) * time.Millisecond
	time.Sleep(electionTimeout)
	ch <- 1
}

func candidateRequestTicker(ch chan int) {
	var electionTimeout time.Duration = time.Duration(70) * time.Millisecond
	time.Sleep(electionTimeout)
	ch <- 1
}

func clearChannel(ch chan int) {
	for len(ch) > 0 {
		<-ch
	}
}

func (rf *Raft) GetSnapshot() []byte {
	return rf.persister.ReadSnapshot()
}

func (rf *Raft) GetRaftStateSize() int {
	return rf.persister.RaftStateSize()
}

//index: lastCommitIndex
func (rf *Raft) Actsnapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index > rf.lastLogIndex || index < 1 || index > rf.commitIndex || index <= rf.getLastSnapsIndex(){
		return
	}
	util.Debug(util.DSnap, "S%d T:%d R:%d -> actively Snap index:%d", rf.me, rf.CurrentTerm, rf.role, index)
	//[1, index]
	term := rf.Log[index - rf.getLastSnapsIndex()].Term
	entry0 := rf.Log[:1]
	entry1 := rf.Log[index + 1 - rf.getLastSnapsIndex():]
	rf.Log = entry0;
	rf.Log = append(rf.Log, entry1...)

	rf.Log[0].Command = index
	rf.Log[0].Term = term
	rf.Snapshots = snapshot
	util.Debug(util.DSnap, "S%d T:%d R:%d -> Log0:%v, Log1:%v", rf.me, rf.CurrentTerm, rf.role, entry0, rf.Log[1])
	//persist state
	rf.persist()
}

//update follower's snapshot
func (rf *Raft) CondInstallSnapshot(lastSnapshotTerm int, lastSnapshotIndex int, snapshot []byte) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if lastSnapshotIndex <= rf.commitIndex {
		return false
	}

	util.Debug(util.DSnap, "S%d T:%d R:%d -> snap passi SI:%d, rf.SI:%d", 
		rf.me, rf.CurrentTerm, rf.role, lastSnapshotIndex, rf.getLastSnapsIndex())
	// term := rf.Log[lastSnapshotIndex - rf.getLastSnapsIndex()].Term
	if rf.lastLogIndex < lastSnapshotIndex {
		rf.Log = rf.Log[:1]
		rf.lastLogIndex = lastSnapshotIndex
		rf.Log = append(rf.Log, Entry{nil, -1})
	} else {
		rf.Log = append(rf.Log[:1], rf.Log[lastSnapshotIndex + 1 - rf.getLastSnapsIndex():]...)
	}
	rf.persist()

	rf.Snapshots = snapshot

	rf.Log[0].Command = lastSnapshotIndex
	rf.Log[0].Term = lastSnapshotTerm

	rf.commitIndex = lastSnapshotIndex
	rf.lastApplied = lastSnapshotIndex

	rf.persist()
	util.Debug(util.DSnap, "S%d T:%d R:%d ->Follower Snap LSI:%d", rf.me, rf.CurrentTerm, rf.role, lastSnapshotIndex)
	return true
}

