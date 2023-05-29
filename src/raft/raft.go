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
	SnapshotValid bool
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

	commitIndex int
	lastApplied int

	lastLogIndex int

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

	electionTimeout time.Duration
	// heartbeatTimeout time.Duration
}

type Candidate struct {
	//channel for AppendEntries
	recAE chan int

	//channel for ReqiestVote
	recRV chan int

	electionTimeout time.Duration
}

// return currentTerm and whether this server
// believes it is the leader.
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

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
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
	rf.persister.SaveRaftState(data)
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
		rf.lastLogIndex = len(rf.Log) - 2
	}
	util.Debug(util.DInfo, "S%d T:%d R:%d -> readPersist VF:%d CT:%d L0:%v L:%v",
		rf.me, rf.CurrentTerm, rf.role, rf.VotedFor, rf.CurrentTerm, rf.Log[1], rf.Log[len(rf.Log)-2])
	// log.Printf("server%d: restart VotedFor%d, CurrentTerm %d, log %v ", rf.me, rf.VotedFor, rf.CurrentTerm, rf.Log)
}

type InstallSnapShotArgs struct {
	Term              int
	LeaderId          int
	lastIncludedIndex int
	lastIncludedTerm  int
	offert            int
	data              []byte
	done              bool
}

type InstallSnapShotReply struct {
	Term int
}

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
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

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	rf.mu.Lock()
	isLeader := true
	term := rf.CurrentTerm

	if rf.role == 2 {
		// log.Printf("server %d role %d term %d: receive command %v from client", rf.me, rf.role, rf.CurrentTerm, command)
		entry := Entry{command, rf.CurrentTerm}

		util.Debug(util.DClient, "S%d T:%d R:%d <- C:%v", rf.me, rf.CurrentTerm, rf.role, entry.Command)
		//update log, lastLogIndex, nextIndex
		rf.Log[rf.lastLogIndex+1] = entry
		util.Debug(util.DPersist, "S%d T:%d R:%d -> S%d P L0:%v L:%v",
			rf.me, rf.CurrentTerm, rf.role, rf.me, rf.Log[1], rf.Log[len(rf.Log)-2])
		rf.Log = append(rf.Log, Entry{nil, -1})
		rf.persist()
		rf.lastLogIndex = len(rf.Log) - 2
		//some followers may don't catch the leader, so the nextIndex is not equals to rf.lastLogIndex,
		//actually, these followers will return false
		for index, nextIndex := range rf.nextIndex {
			if nextIndex == rf.lastLogIndex {
				rf.nextIndex[index] = rf.lastLogIndex + 1
			}
		}
		// log.Printf("server %d role %d term %d: Leader receive client cmd, new nextIndex %v, log %v", rf.me, rf.role, rf.CurrentTerm, rf.nextIndex, rf.Log)

		//if there are many request from client at the same time, then there will start many goroutine to request entries
		// go rf.AttemptSendAppendEntries()
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
var serverName string

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
		lastLogIndex: 0,
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
	rf.lastLogIndex = len(rf.Log) - 2

	go StateListening(rf)
	go rf.applyLog()

	return rf
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
					Command:      rf.Log[rf.lastApplied].Command,
					CommandIndex: rf.lastApplied,
					CommandTerm:  rf.Log[rf.lastApplied].Term,
				}
				rf.applyCh <- applyMsg
				util.Debug(util.DCommit, "S%d T:%d R:%d -> RCI:%d RCIL:%v",
					rf.me, rf.CurrentTerm, rf.role, rf.lastApplied, rf.Log[rf.lastApplied])
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
			// log.Printf("candid %d term %d: StateListening Candidate, request vote, log: %v", rf.me, rf.CurrentTerm, rf.Log)
			var attemptRequestVoteChan chan int = make(chan int, 1)
			// go rf.AttemptRequestVote(candidateAttemptRequestVoteChan)

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
			// log.Printf("leader %d term %d: StateListening Leader, nextIndex: %v, log: %v, commitIndex: %d", rf.me, rf.CurrentTerm, rf.nextIndex, rf.Log, rf.commitIndex)
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

func (rf *Raft) convertToFollower(term int) {
	rf.mu.Lock()
	util.Debug(util.DInfo, "S%d T:%d R:%d -> S%d CTF", rf.me, rf.CurrentTerm, rf.role, rf.me)
	rf.role = 0
	rf.VotedFor = -1
	rf.CurrentTerm = term
	rf.persist()
	util.Debug(util.DPersist, "S%d T:%d R:%d -> S%d P L0:%v L:%v",
		rf.me, rf.CurrentTerm, rf.role, rf.me, rf.Log[1], rf.Log[len(rf.Log)-2])
	rf.mu.Unlock()
}

func (rf *Raft) AttemptSendAppendEntries() {
	argeeNums := 1
	var wg sync.WaitGroup
	for i, _ := range rf.peers {
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
		rf.mu.Unlock()
		wg.Done()
	}()

	reply := &AppendEntriesReply{}

	rf.mu.Lock()
	if rf.lastLogIndex == len(rf.Log)-1 && rf.Log[len(rf.Log)-1].Term == -1 {
		rf.Log[len(rf.Log)-1].Term = rf.CurrentTerm
	}

	//send heartbeats
	args := &AppendEntriesArgs{
		LeaderCommit: rf.commitIndex,
		LeaderId:     rf.me,
		PrevLogIndex: rf.nextIndex[server] - 1,
		PrevLogTerm:  rf.Log[rf.nextIndex[server]-1].Term,
		Term:         rf.CurrentTerm,
	}
	// entries := rf.Log[rf.nextIndex[server]:]
	var entries = make([]Entry, 0)
	entries = append(entries, rf.Log[rf.nextIndex[server]:]...)
	args.Entries = entries
	util.Debug(util.DAppE, "S%d T:%d R:%d -> S%d Sending PLI: %d PLT: %d LC: %d",
		rf.me, rf.CurrentTerm, rf.role, server, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit)

	if rf.role != 2 {
		return
	}
	rf.mu.Unlock()

	ok := rf.sendAppendEntries(server, args, reply)

	rf.mu.Lock()
	if !ok || rf.role != 2 || rf.CurrentTerm != args.Term {
		// Debug(dAppE, "S%d T:%d R:%d <- S%d expired rpc AE", rf.me, rf.CurrentTerm, rf.role, server)
		return
	}

	//ok
	if reply.Success {
		(*argeeNums)++
		util.Debug(util.DAppE, "S%d T:%d R:%d -> S%d RSucc AN:%d", rf.me, rf.CurrentTerm, rf.role, server, *argeeNums)
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
			// && rf.Log[commitN].Term == rf.CurrentTerm
			if num > len(rf.peers)/2 && rf.Log[commitN].Term == rf.CurrentTerm {
				// nums := commitN - rf.commitIndex
				// for i := 1; i <= nums; i++ {
				// 	applyMsg := &ApplyMsg{true, rf.Log[rf.commitIndex+i].Command, rf.commitIndex + i}
				// 	rf.applyCh <- *applyMsg
				// 	Debug(dCommit, "S%d T:%d R:%d -> RCI:%d RCIL:%v for AE",
				// 		rf.me, rf.CurrentTerm, rf.role, rf.commitIndex+i, rf.Log[rf.commitIndex+i])
				// }
				rf.commitIndex = commitN
				break
			}
		}
	} else {
		//if not success, check the term, XTerm, XIndex, XLastIndex
		if rf.CurrentTerm < reply.Term {
			util.Debug(util.DAppE, "S%d T:%d R:%d -> S%d RT:%d > CT:%d for AE",
				rf.me, rf.CurrentTerm, rf.role, server, reply.Term, rf.CurrentTerm)
			// rf.convertToFollower(reply.Term)
			util.Debug(util.DInfo, "S%d T:%d R:%d -> S%d CToF", rf.me, rf.CurrentTerm, rf.role, rf.me)
			rf.role = 0
			rf.VotedFor = -1
			rf.CurrentTerm = reply.Term
			rf.persist()
			// Debug(dPersist, "S%d T:%d R:%d -> S%d P L0:%v L:%v",
			// 	rf.me, rf.CurrentTerm, rf.role, rf.me, rf.Log[1], rf.Log[len(rf.Log)-2])
			return
		} else if reply.XTerm != -1 {
			//leader nextIndex term conflict with follower
			//reply.XIndex <= follower.lastLogIndex == args.PreLogIndex < rf.nextIndex[server], so it won't cross the border
			// for i, t := range rf.Log {

			// }
			rf.nextIndex[server] = reply.XIndex
			util.Debug(util.DAppE, "S%d T:%d  R:%d -> S%d RXI:%d for AE",
				rf.me, rf.CurrentTerm, rf.role, server, reply.XIndex)
		} else {
			//rf.nextIndex[server]> reply.XLastIndex, so it won't cross the border
			rf.nextIndex[server] = reply.XLastIndex
			util.Debug(util.DAppE, "S%d T:%d  R:%d -> S%d RXL:%d for AE",
				rf.me, rf.CurrentTerm, rf.role, server, reply.XLastIndex)
		}
	}

}

//follower receive
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	util.Debug(util.DAppE, "S%d T:%d R:%d <- S%d AE", rf.me, rf.CurrentTerm, rf.role, args.LeaderId)
	reply.Success = false

	if args.Term < rf.CurrentTerm {
		// log.Printf("server %d role %d term %d: bigger term, append entries from %d", rf.me, rf.role, rf.CurrentTerm, args.LeaderId)
		util.Debug(util.DAppE, "S%d T:%d R:%d -> S%d RCTerm:%d > ATerm:%d", rf.me, rf.CurrentTerm, rf.role, args.LeaderId, rf.CurrentTerm, args.Term)
		reply.Term = rf.CurrentTerm
		rf.mu.Unlock()
		return
	} else if args.PrevLogIndex > rf.lastLogIndex {
		reply.XTerm = -1
		reply.XIndex = -1
		reply.XLastIndex = rf.lastLogIndex + 1
		util.Debug(util.DAppE, "S%d T:%d R:%d -> S%d APL:%d > RLLI:%d RXLI:%d",
			rf.me, rf.CurrentTerm, rf.role, args.LeaderId, args.PrevLogIndex, rf.lastLogIndex, reply.XLastIndex)
	} else if rf.lastLogIndex != 0 && rf.Log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.XTerm = rf.Log[args.PrevLogIndex].Term
		index := args.PrevLogIndex
		for index >= 1 && rf.Log[index].Term != 0 && rf.Log[index].Term == rf.Log[args.PrevLogIndex].Term {
			reply.XIndex = index
			index--
		}
		//if same index and different term, delete args.PrevLogIndex and following entries
		// rf.Log = rf.Log[:args.PrevLogIndex]
		// rf.lastLogIndex = args.PrevLogIndex - 1
		util.Debug(util.DAppE, "S%d T:%d R:%d -> S%d RLT:%d != APLT:%d RXT:%d RXI:%d",
			rf.me, rf.CurrentTerm, rf.role, args.LeaderId, rf.Log[args.PrevLogIndex].Term, args.PrevLogTerm, reply.XTerm, reply.XLastIndex)
	} else if rf.lastLogIndex == 0 || rf.Log[args.PrevLogIndex].Term == args.PrevLogTerm {
		reply.Success = true
		//if leadear send empty command to follower,and they has same log, then don't need to update
		if args.Entries[0].Command != nil {
			//delete after rf.lastLogIndex
			rf.Log = rf.Log[:args.PrevLogIndex+1]
			rf.Log = append(rf.Log, args.Entries...)
			rf.lastLogIndex = len(rf.Log) - 2
			rf.persist()
			// Debug(dAppE, "S%d T:%d  R:%d -> S%d AP success PLI: %d PLT: %d LC: %d ",
			// 	rf.me, rf.CurrentTerm, rf.role, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit)
		}
		if args.Entries[0].Command == nil && args.Entries[0].Term >= rf.CurrentTerm {

		}

		if rf.commitIndex < args.LeaderCommit {
			minIndex := args.LeaderCommit
			if args.LeaderCommit > rf.lastLogIndex {
				minIndex = rf.lastLogIndex
			}
			// nums := minIndex - rf.commitIndex
			// for i := 1; i <= nums; i++ {
			// 	applyMsg := &ApplyMsg{true, rf.Log[rf.commitIndex+i].Command, rf.commitIndex + i}
			// 	rf.applyCh <- *applyMsg
			// 	Debug(dCommit, "S%d T:%d R:%d -> RCI:%d RCIL:%v for AE",
			// 		rf.me, rf.CurrentTerm, rf.role, rf.commitIndex+i, rf.Log[rf.commitIndex+i])
			// 	// log.Printf("server %d role %d term %d: follower commit %d %v", rf.me, rf.role, rf.CurrentTerm, rf.commitIndex+i, rf.Log[rf.commitIndex+i])
			// }
			rf.commitIndex = minIndex
			// log.Printf("server %d role %d term %d: follower commitIndex %d %v", rf.me, rf.role, rf.CurrentTerm, minIndex, rf.Log[minIndex])
		}
	}
	// if rf.VotedFor != args.LeaderId {
	// 	rf.VotedFor = args.LeaderId
	// }

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
		util.Debug(util.DInfo, "S%d T:%d R:%d -> S%d CToF", rf.me, rf.CurrentTerm, rf.role, rf.me)
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
	for i, _ := range rf.peers {
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
				LastLogTerm:  rf.Log[rf.lastLogIndex].Term,
				Term:         rf.CurrentTerm,
			}
			if rf.role != 1 {
				rf.mu.Unlock()
				return
			}
			// fmt.Println(server)
			// fmt.Printf("S%d T:%d R:%d -> S%d Sending LLI: %d LLT: %d",
			// 	rf.me, rf.CurrentTerm, rf.role, server, args.LastLogIndex, args.LastLogTerm)
			util.Debug(util.DVote, "S%d T:%d R:%d -> S%d Sending LLI: %d LLT: %d",
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
				// log.Printf("candid %d term %d: all votes %d, but term >= candidate term, reply %v", rf.me, rf.CurrentTerm, votes, reply)
				util.Debug(util.DVote, "S%d T:%d R:%d -> S%d RT:%d > CT:%d for RV",
					rf.me, rf.CurrentTerm, rf.role, server, reply.Term, rf.CurrentTerm)
				// rf.convertToFollower(reply.Term)
				util.Debug(util.DInfo, "S%d T:%d R:%d -> S%d CToF", rf.me, rf.CurrentTerm, rf.role, rf.me)
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
				util.Debug(util.DVote, "S%d T:%d R:%d <- S%d RVG votes:%d all:%d", rf.me, rf.CurrentTerm, rf.role, server, votes, len(rf.peers))
				// log.Printf("candid %d term %d: get vote from %d, all votes %d", rf.me, rf.CurrentTerm, server, votes)
				if votes > len(rf.peers)/2 {
					rf.role = 2
					for i, _ := range rf.peers {
						rf.nextIndex[i] = rf.lastLogIndex + 1
						rf.matchIndex[i] = -1
					}
					if rf.Log[len(rf.Log)-1].Command != nil {
						rf.Log = append(rf.Log, Entry{nil, -1})
						rf.persist()
					}
					// //commit before logs
					// nums := rf.lastLogIndex - rf.commitIndex
					// for i := 1; i <= nums; i++ {
					// 	applyMsg := &ApplyMsg{true, rf.Log[rf.commitIndex+i].Command, rf.commitIndex + i}
					// 	rf.applyCh <- *applyMsg
					// 	Debug(dCommit, "S%d T:%d R:%d -> RCI:%d RCIL:%v for AE",
					// 		rf.me, rf.CurrentTerm, rf.role, rf.commitIndex+i, rf.Log[rf.commitIndex+i])
					// }
					// rf.commitIndex = rf.lastLogIndex

					// log.Printf("candid %d term %d: become leader, log: %v, nextIndex: %v", rf.me, rf.CurrentTerm, rf.Log, rf.nextIndex)
					//if get enough votes, convert to leader immediately
					ch <- 1
					rf.mu.Unlock()
					return
				}
			} else {
				util.Debug(util.DVote, "S%d T:%d R:%d <- S%d RVNG votes:%d all:%d", rf.me, rf.CurrentTerm, rf.role, server, votes, len(rf.peers))
				// log.Printf("candid %d term %d: can't get vote from %d, all votes %d", rf.me, rf.CurrentTerm, server, votes)
			}
			rf.mu.Unlock()
		}(i)
	}
	wg.Wait()
	// log.Printf("candid %d term %d: get votes: %d, log: %v, nextIndex: %v", rf.me, rf.CurrentTerm, votes, rf.Log, rf.nextIndex)
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	util.Debug(util.DVote, "S%d T:%d R:%d <- S%d vote req", rf.me, rf.CurrentTerm, rf.role, args.CandidateId)

	// if (rf.Log[rf.lastLogIndex].Term < args.LastLogTerm || (rf.Log[rf.lastLogIndex].Term == args.LastLogTerm && rf.lastLogIndex <= args.LastLogIndex)) &&
	// 	args.Term > rf.CurrentTerm {
	// 	Debug(dVote, "S%d T:%d R:%d -> S%d vote granted ", rf.me, rf.CurrentTerm, rf.role, args.CandidateId)
	// 	// rf.convertToFollower(args.Term)
	// 	if rf.role != 0 {
	// 		Debug(dVote, "S%d T:%d R:%d -> S%d CToF", rf.me, rf.CurrentTerm, rf.role, rf.me)
	// 		rf.role = 0
	// 	}
	// 	rf.CurrentTerm = args.Term
	// 	rf.VotedFor = args.CandidateId
	// 	rf.persist()
	// 	Debug(dPersist, "S%d T:%d R:%d -> S%d P L0:%v L:%v",
	// 		rf.me, rf.CurrentTerm, rf.role, rf.me, rf.Log[1], rf.Log[len(rf.Log)-2])
	// 	reply.Term = rf.CurrentTerm
	// 	reply.VoteGranted = true

	// 	if rf.role == 1 {
	// 		clearChannel(rf.candidate.recRV)
	// 		rf.candidate.recRV <- 1
	// 	}
	// 	rf.mu.Unlock()
	// 	return
	// }
	if args.Term > rf.CurrentTerm {
		util.Debug(util.DVote, "S%d T:%d R:%d -> S%d vote granted ", rf.me, rf.CurrentTerm, rf.role, args.CandidateId)
		// rf.convertToFollower(args.Term)
		if rf.role != 0 {
			util.Debug(util.DVote, "S%d T:%d R:%d -> S%d CToF", rf.me, rf.CurrentTerm, rf.role, rf.me)
			rf.role = 0
		}
		rf.CurrentTerm = args.Term
		rf.VotedFor = -1
		rf.persist()
		// Debug(dPersist, "S%d T:%d R:%d -> S%d P L0:%v L:%v",
		// 	rf.me, rf.CurrentTerm, rf.role, rf.me, rf.Log[1], rf.Log[len(rf.Log)-2])
	}

	if rf.CurrentTerm == args.Term && (rf.VotedFor == -1 || rf.VotedFor == args.CandidateId) &&
		(args.LastLogTerm > rf.Log[rf.lastLogIndex].Term ||
			(args.LastLogTerm == rf.Log[rf.lastLogIndex].Term && args.LastLogIndex >= rf.lastLogIndex)) {
				util.Debug(util.DVote, "S%d T:%d R:%d -> S%d vote for", rf.me, rf.CurrentTerm, rf.role, args.CandidateId)
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

	// if rf.CurrentTerm > args.Term {
	// 	reply.VoteGranted = false
	// 	Debug(dVote, "S%d T:%d R:%d -> S%d RCTerm:%d > ATerm:%d", rf.me, rf.CurrentTerm, rf.role, args.CandidateId, rf.CurrentTerm, args.Term)
	// 	// log.Printf("server %d role %d term %d: reject vote to %d, term args %+v, rf %d", rf.me, rf.role, rf.CurrentTerm, args.CandidateId, args.Term, rf.CurrentTerm)

	// } else if rf.VotedFor != -1 && rf.VotedFor != args.CandidateId {
	// 	reply.VoteGranted = false
	// 	Debug(dVote, "S%d T:%d R:%d -> S%d RVF:%d", rf.me, rf.CurrentTerm, rf.role, args.CandidateId, rf.VotedFor)
	// 	// log.Printf("server %d role %d term %d: reject vote to %d, voteTo %d", rf.me, rf.role, rf.CurrentTerm, args.CandidateId, rf.VotedFor)

	// } else if rf.Log[rf.lastLogIndex].Term > args.LastLogTerm {
	// 	reply.VoteGranted = false
	// 	Debug(dVote, "S%d T:%d R:%d -> S%d RLT:%d > ALLT:%d", rf.me, rf.CurrentTerm, rf.role, args.CandidateId, rf.Log[rf.lastLogIndex].Term, args.LastLogTerm)
	// 	// log.Printf("server %d role %d term %d: reject vote to %d, logTerm args %d rf %d", rf.me, rf.role, rf.CurrentTerm, args.CandidateId, args.LastLogTerm, rf.Log[rf.lastLogIndex].Term)

	// } else if rf.Log[rf.lastLogIndex].Term == args.LastLogTerm && rf.lastLogIndex > args.LastLogIndex {
	// 	reply.VoteGranted = false
	// 	Debug(dVote, "S%d T:%d R:%d -> S%d RLLI:%d > ALLI:%d", rf.me, rf.CurrentTerm, rf.role, args.CandidateId, rf.lastLogIndex, args.LastLogIndex)
	// 	// log.Printf("server %d role %d term %d: reject vote to %d, logIndex args %d rf %d", rf.me, rf.role, rf.CurrentTerm, args.CandidateId, args.LastLogIndex, rf.lastLogIndex)

	// } else {
	// 	Debug(dVote, "S%d T:%d R:%d -> S%d vote for", rf.me, rf.CurrentTerm, rf.role, args.CandidateId)
	// 	if rf.role == 0 {
	// 		clearChannel(rf.follower.recRV)
	// 		rf.follower.recRV <- 1
	// 	}
	// 	rf.VotedFor = args.CandidateId
	// 	rf.CurrentTerm = args.Term
	// 	rf.persist()
	// 	Debug(dPersist, "S%d T:%d R:%d -> S%d P L0:%v L:%v",
	// 		rf.me, rf.CurrentTerm, rf.role, rf.me, rf.Log[1], rf.Log[len(rf.Log)-2])
	// 	reply.VoteGranted = true
	// }
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

func (rf *Raft) GetRaftStateSize() int {
	return rf.persister.RaftStateSize()
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// if index > rf.lastLog().Index || index < rf.logEntries[0].Index || index > rf.commitIndex {
	// 	return
	// }
	// //1.获取需要压缩末尾日志的数组内索引
	// realIndex := rf.binaryFindRealIndexInArrayByIndex(index)
	// lastLogEntry := rf.logEntries[realIndex]

	// //2.清除log中[1,realIndex]之间的数据
	// rf.logEntries = append(rf.logEntries[:1], rf.logEntries[realIndex+1:]...)
	// //3.保存三项快照数据
	// rf.snapshotData = snapshot
	// //4.更改日志占位节点
	// rf.logEntries[0].Index = lastLogEntry.Index
	// rf.logEntries[0].Term = lastLogEntry.Term
	// //5.持久化
	// rf.persistStateAndSnapshot()
}

