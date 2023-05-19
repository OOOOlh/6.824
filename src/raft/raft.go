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
	"log"
	"math/rand"
	"os"
	"strconv"

	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
)

func init() {
	t := time.Now().Unix()
	serverName = strconv.FormatInt(t, 10)
	file := "./" + serverName + "message" + ".log"
	logFile, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0766)
	if err != nil {
		panic(err)
	}
	log.SetOutput(logFile) // 将文件设置为log输出的文件
	log.SetPrefix("[raft]")
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.LUTC)
	return
}

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

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	persistentState PersistentState
	commitIndex     int
	lastApplied     int

	lastLogIndex int

	nextIndex  []int
	matchIndex []int

	//0: follower
	//1: candidate
	//2: leader
	role int

	follower  Follower
	candidate Candidate
	leader    Leader
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

type Leader struct {
	//crashed follower list
	// crashedFollowersChan chan int

	// finishedChan chan int

	//server agreement nums
	// argeeNums int
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
	if rf.role == 2 {
		isleader = true
	}
	term = rf.persistentState.CurrentTerm
	// Your code here (2A).
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.persistentState)
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
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var persistentState PersistentState
	if err := d.Decode(&persistentState); err != nil {
		log.Fatal()
	} else {
		rf.persistentState = persistentState
	}
	log.Printf("server%d: restart VotedFor%d, CurrentTerm %d, log %v ", rf.me, rf.persistentState.VotedFor, rf.persistentState.CurrentTerm, rf.persistentState.Log)
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

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	log.Printf("server%d: role:%d receive vote request from %d, args %+v, rf.LastLogTerm %v", rf.me, rf.role, args.CandidateId, args, rf.persistentState.Log[rf.lastLogIndex].Term)
	rf.mu.Lock()
	if rf.persistentState.CurrentTerm > args.Term {
		reply.VoteGranted = false
		log.Printf("server%d: role:%d reject vote to %d, term args %+v, rf %d", rf.me, rf.role, args.CandidateId, args.Term, rf.persistentState.CurrentTerm)
	} else if rf.persistentState.VotedFor != -1 && rf.persistentState.VotedFor != args.CandidateId && rf.persistentState.CurrentTerm == args.Term {
		reply.VoteGranted = false
		log.Printf("server%d: role:%d reject vote to %d, voteTo %d", rf.me, rf.role, args.CandidateId, rf.persistentState.VotedFor)
	} else if rf.persistentState.Log[rf.lastLogIndex].Term > args.LastLogTerm {
		reply.VoteGranted = false
		log.Printf("server%d: role:%d reject vote to %d, logTerm args %d rf %d", rf.me, rf.role, args.CandidateId, args.LastLogTerm, rf.persistentState.Log[rf.lastLogIndex].Term)
	} else if rf.persistentState.Log[rf.lastLogIndex].Term == args.LastLogTerm && rf.lastLogIndex > args.LastLogIndex {
		reply.VoteGranted = false
		log.Printf("server%d: role:%d reject vote to %d, logIndex args %d rf %d", rf.me, rf.role, args.CandidateId, args.LastLogIndex, rf.lastLogIndex)
	} else {
		log.Printf("server%d: role:%d vote to %d", rf.me, rf.role, args.CandidateId)
		if rf.role == 0 {
			clearChannel(rf.follower.recRV)
			rf.follower.recRV <- 1
		} else if rf.role == 1 {
			// if args.Term > rf.persistentState.CurrentTerm {
			// 	rf.role = 0
			// }
			clearChannel(rf.candidate.recRV)
			rf.candidate.recRV <- 1
		}
		rf.persistentState.VotedFor = args.CandidateId
		rf.persistentState.CurrentTerm = args.Term
		rf.persist()
		reply.VoteGranted = true
	}
	reply.Term = rf.persistentState.CurrentTerm
	rf.mu.Unlock()
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//follower receive
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	log.Printf("server%d: role:%d receive append from %d, argsTerm is %v, term is %v, args is %v, argsPrevLogIndex is %v, rf.lastLogIndex is %v, rf.log is %v",
		rf.me, rf.role, args.LeaderId, args.Term, rf.persistentState.CurrentTerm, args, args.PrevLogIndex, rf.lastLogIndex, rf.persistentState.Log)
	reply.Success = false
	rf.mu.Lock()

	//if follower's or candidate's term < leader's term, then update their term
	if args.Term >= rf.persistentState.CurrentTerm {
		rf.persistentState.CurrentTerm = args.Term
		//when receive from leader, refresh rf.votedFor
		// rf.persistentState.VotedFor = -1
		rf.persist()
	}
	//return false if leader's term < follower's term(1) or if log doesn’t contain an entry at prevLogIndex
	//whose term matches prevLogTerm(2)
	//a. different term
	//b. different index
	//c. different log term
	if args.Term < rf.persistentState.CurrentTerm {
		log.Printf("server%d: role:%d bigger term, append entries from %d", rf.me, rf.role, args.LeaderId)
	} else if args.PrevLogIndex > rf.lastLogIndex {
		log.Printf("server%d: role:%d logger index: %d, %d, append entries from %d", rf.me, rf.role, args.PrevLogIndex, rf.lastLogIndex, args.LeaderId)
		reply.XTerm = -1
		reply.XIndex = -1
		reply.XLastIndex = rf.lastLogIndex + 1
	} else if rf.lastLogIndex != 0 && rf.persistentState.Log[args.PrevLogIndex].Term != args.PrevLogTerm {
		log.Printf("server%d: role:%d not equals log term %d, %d, append entries from %d", rf.me, rf.role, args.PrevLogTerm, rf.persistentState.Log[args.PrevLogIndex].Term, args.LeaderId)
		reply.XTerm = rf.persistentState.Log[args.PrevLogIndex].Term
		index := args.PrevLogIndex
		for index >= 0 && rf.persistentState.Log[index].Term != 0 && rf.persistentState.Log[index].Term == rf.persistentState.Log[args.PrevLogIndex].Term {
			reply.XIndex = index
			index--
		}
	} else if args.PrevLogIndex <= rf.lastLogIndex && (rf.lastLogIndex == 0 || (rf.lastLogIndex != 0 && rf.persistentState.Log[args.PrevLogIndex].Term == args.PrevLogTerm)) {
		reply.Success = true
		//if leadear send empty command to follower,and they has same log, then don't need to update
		if args.Entries[0].Command != nil {
			//delete after rf.lastLogIndex
			rf.persistentState.Log = rf.persistentState.Log[:args.PrevLogIndex+1]
			rf.persistentState.Log = append(rf.persistentState.Log, args.Entries...)
			rf.lastLogIndex = len(rf.persistentState.Log) - 2
			log.Printf("server%d: role:%d successfully append entries new log %v rf.lastLogIndex %v", rf.me, rf.role, rf.persistentState.Log, rf.lastLogIndex)
		}
		log.Printf("server%d: role:%d rf.commitIndex %d args.LeaderCommit %d", rf.me, rf.role, rf.commitIndex, args.LeaderCommit)
		if rf.commitIndex < args.LeaderCommit {
			nums := args.LeaderCommit - rf.commitIndex
			for i := 1; i <= nums; i++ {
				applyMsg := &ApplyMsg{true, rf.persistentState.Log[rf.commitIndex+i].Command, rf.commitIndex + i}
				rf.applyCh <- *applyMsg
				log.Printf("server%d: follower commit %d %v", rf.me, rf.commitIndex+i, rf.persistentState.Log[rf.commitIndex+i])
			}
			rf.commitIndex = args.LeaderCommit
		}
		rf.persistentState.VotedFor = -1
		rf.persist()
	}

	//if follower receive AppendEntries, reset election timeout
	if rf.role == 0 {
		clearChannel(rf.follower.recAE)
		rf.follower.recAE <- 1
		//if candidate receive AppendEntries, if leader term >= candidate term, candidate convert to follower,
	} else if rf.role == 1 {
		if args.Term >= rf.persistentState.CurrentTerm {
			rf.role = 0
			// rf.persistentState.VotedFor = -1
			rf.persist()
			clearChannel(rf.candidate.recAE)
			rf.candidate.recAE <- 1
		}
		//if there are over 1 leader at the same time, then other leaders must convert to vandidate, elect again
	} else if rf.role == 2 {
		if args.Term >= rf.persistentState.CurrentTerm {
			rf.role = 0
			// rf.persistentState.VotedFor = -1
			rf.persistentState.CurrentTerm = args.Term
			rf.persist()
		}
	}

	reply.Term = rf.persistentState.CurrentTerm
	rf.mu.Unlock()
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := rf.persistentState.CurrentTerm
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	if rf.role == 2 {
		log.Printf("server%d: role%d receive command %v from client", rf.me, rf.role, command)
		entry := Entry{command, rf.persistentState.CurrentTerm}

		//update log, lastLogIndex, nextIndex
		rf.persistentState.Log[rf.lastLogIndex+1] = entry
		rf.persist()
		rf.persistentState.Log = append(rf.persistentState.Log, Entry{})
		rf.lastLogIndex = len(rf.persistentState.Log) - 2
		//some followers may don't catch the leader, so the nextIndex is not equals to rf.lastLogIndex,
		//actually, these followers will return false
		for index, nextIndex := range rf.nextIndex {
			if nextIndex == rf.lastLogIndex {
				rf.nextIndex[index] = rf.lastLogIndex + 1
			}
		}
		log.Printf("server%d: Leader receive client cmd, new nextIndex %v, log %v", rf.me, rf.nextIndex, rf.persistentState.Log)

		//if there are many request from client at the same time, then there will start many goroutine to request entries
		// go rf.AttemptSendAppendEntries()
	} else {
		isLeader = false
	}
	rf.mu.Unlock()
	index = rf.lastLogIndex
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
	// Your code here, if desired.

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
	rf := &Raft{}
	rf.applyCh = applyCh
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.persistentState.Log = make([]Entry, 0)
	//fill the index 0, because the log index begin with 1
	rf.persistentState.Log = append(rf.persistentState.Log, Entry{})
	//pre append
	rf.persistentState.Log = append(rf.persistentState.Log, Entry{})
	rf.lastLogIndex = 0
	rf.commitIndex = 0
	rf.persistentState.VotedFor = -1
	rf.follower.recAE = make(chan int, 1)
	rf.follower.recRV = make(chan int, 1)
	rf.candidate.recAE = make(chan int, 1)
	rf.candidate.recRV = make(chan int, 1)

	rf.readPersist(rf.persister.raftstate)
	rf.lastLogIndex = len(rf.persistentState.Log) - 2

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// t := time.Now().Unix()
	// serverName = strconv.Itoa()
	go StateListening(rf)

	return rf
}

func StateListening(rf *Raft) {
	for {
		//follower's task is to reply the leader's or candidate's request, and monitor the leader's heartbeat
		if rf.role == 0 {
			log.Printf("server%d: StateListening Follower, current Term: %d, log: %v", rf.me, rf.persistentState.CurrentTerm, rf.persistentState.Log)
			var followerElectionTimeoutChan chan int = make(chan int, 1)
			go electionTimeout(followerElectionTimeoutChan)

			select {
			case <-followerElectionTimeoutChan:
				rf.mu.Lock()
				//convert to candidate
				rf.role = 1
				rf.persistentState.VotedFor = rf.me
				rf.persistentState.CurrentTerm++
				rf.persist()
				log.Printf("server%d: convert to candidate, current Term: %d, log: %v", rf.me, rf.persistentState.CurrentTerm, rf.persistentState.Log)
				rf.mu.Unlock()
				break
			case <-rf.follower.recAE:
				//receive AppendEntries Request, reset timeout
				break
			case <-rf.follower.recRV:
				//receive RequestVote Request, reset timeout
				break
			}
		}

		//candidate's task is to request votes, and maybe convert to leader
		if rf.role == 1 {
			log.Printf("server%d: StateListening Candidate, request vote current Term: %d, log: %v", rf.me, rf.persistentState.CurrentTerm, rf.persistentState.Log)
			var candidateAttemptRequestVoteChan chan int = make(chan int, 1)
			go rf.AttemptRequestVote(candidateAttemptRequestVoteChan)

			var candidateTimeoutChan chan int = make(chan int, 1)
			go candidateTimeout(candidateTimeoutChan)

			//during this time,a candidate may convert to follower
			select {
			//when election timeout, candidate should request vote again
			case <-candidateTimeoutChan:
				rf.mu.Lock()
				rf.persistentState.VotedFor = rf.me
				rf.persist()
				rf.persistentState.CurrentTerm++
				rf.mu.Unlock()
				break
			case <-rf.candidate.recAE:
				//receive AppendEntries Request and leader term >= candidate term, reset timeout
				break
			case <-candidateAttemptRequestVoteChan:

				//request vote and receive votes, maybe convert to leader, if there is one server timeout, this channel will wait
				break
			}
		}

		//leader's task is to send heartbeat to follower
		if rf.role == 2 {
			log.Printf("server%d: StateListening Leader, current Term: %d, nextIndex: %v, log: %v, commitIndex: %d", rf.me, rf.persistentState.CurrentTerm, rf.nextIndex, rf.persistentState.Log, rf.commitIndex)
			// beginTime := time.Now().UnixNano() / 1e6
			finish := false
			go rf.AttemptSendAppendEntries(&finish)

			//wait heartbeat timeout, 200ms can pass TestFigure82C, 180ms can't
			var heartbeatTimeout time.Duration = time.Duration(100) * time.Millisecond
			time.Sleep(heartbeatTimeout)
			rf.mu.Lock()
			finish = true
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) AttemptSendAppendEntries(finish *bool) {
	argeeNums := 1
	startTime := time.Now()
	var wg sync.WaitGroup
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		wg.Add(1)
		// log.Printf("server%d: leader ready to send to %d", rf.me, i)
		go rf.SendAppendEntriesToOneServer(i, &argeeNums, finish, &wg)
	}
	wg.Wait()
	log.Println(time.Now().Sub(startTime).Microseconds())
}

func (rf *Raft) SendAppendEntriesToOneServer(server int, argeeNums *int, finish *bool, wg *sync.WaitGroup) {
	defer func() {
		rf.mu.Unlock()
		wg.Done()
	}()

	reply := &AppendEntriesReply{}

	rf.mu.Lock()
	//send heartbeats
	args := &AppendEntriesArgs{}
	args.LeaderCommit = rf.commitIndex
	args.LeaderId = rf.me
	args.PrevLogIndex = rf.nextIndex[server] - 1
	args.PrevLogTerm = rf.persistentState.Log[rf.nextIndex[server]-1].Term
	args.Term = rf.persistentState.CurrentTerm
	entries := rf.persistentState.Log[rf.nextIndex[server]:]
	log.Printf("server%d: ready to send %d entries is %v, rf.nextIndex is %v", rf.me, server, entries, rf.nextIndex)
	rf.mu.Unlock()
	args.Entries = entries

	if rf.role != 2 {
		rf.mu.Lock()
		return
	}

	ok := rf.sendAppendEntries(server, args, reply)
	//receive reply
	//if the server is healthy, it will return reply immediately, and next heartbeat will send up-to-date info, don't need to wait other servers
	rf.mu.Lock()
	if ok && !*finish {
		if reply.Success {
			(*argeeNums)++
			log.Printf("server%d: receive success reply from %d, current agreeNum %d / %d, entries %v", rf.me, server, *argeeNums, len(rf.peers), entries)
			// fmt.Println("++++", *argeeNums)
			rf.nextIndex[server] = rf.lastLogIndex + 1
		} else {
			//if not success, check the term, XTerm, XIndex, XLastIndex
			if rf.persistentState.CurrentTerm < reply.Term {
				//convert to a follower and update term
				rf.role = 0
				// rf.persistentState.VotedFor = -1
				log.Printf("server%d: update term to %d from follower %d", rf.me, reply.Term, server)
				rf.persistentState.CurrentTerm = reply.Term
				rf.persist()
			} else if reply.XTerm != -1 {
				//leader nextIndex term conflict with follower
				//reply.XIndex <= follower.lastLogIndex == args.PreLogIndex < rf.nextIndex[server], so it won't cross the border
				rf.nextIndex[server] = reply.XIndex
				log.Printf("server%d: update by XIndex nextIndex to %d from follower %d", rf.me, reply.XIndex, server)
			} else {
				//rf.nextIndex[server] - 1 > reply.XLastIndex - 1, so it won't cross the border
				rf.nextIndex[server] = reply.XLastIndex
				log.Printf("server%d: update nextIndex to %d from follower %d", rf.me, reply.XLastIndex, server)
			}
		}
		//if leader has command not been commited
		//there maybe client request leader as a result rf.lastLogIndex is being changed, so need to add a lock
		log.Printf("server%d: rf.nextIndex[server]-1:%d args.PrevLogIndex:%d from follower %d", rf.me, rf.nextIndex[server]-1, args.PrevLogIndex, server)
		if rf.nextIndex[server]-1 == args.PrevLogIndex && rf.commitIndex < rf.lastLogIndex {
			if *argeeNums > len(rf.peers)/2 {
				nums := rf.lastLogIndex - rf.commitIndex
				for i := 1; i <= nums; i++ {
					applyMsg := &ApplyMsg{true, rf.persistentState.Log[rf.commitIndex+i].Command, rf.commitIndex + i}
					rf.applyCh <- *applyMsg
					log.Printf("server%d: leader commit %d %v", rf.me, rf.commitIndex+i, rf.persistentState.Log[rf.commitIndex+i])
				}
				rf.commitIndex = rf.lastLogIndex
				rf.persist()
				//must update, else will affect other results
				*argeeNums = 0
			}
		}
	}
}

// func (rf *Raft) HandleCrashedFollowers() {
// 	for {
// 		select {
// 		case server := <-rf.leader.crashedFollowersChan:
// 			//recyclely send leader's log to crashed followers until receive the reply
// 			rf.mu.Lock()
// 			args := &AppendEntriesArgs{}
// 			args.LeaderCommit = rf.commitIndex
// 			args.LeaderId = rf.me
// 			args.PrevLogIndex = rf.lastLogIndex
// 			args.PrevLogTerm = rf.currentTerm
// 			args.Term = rf.currentTerm
// 			args.Entries = entries

// 			reply := &AppendEntriesReply{}

// 			rf.leader.argeeNums = 0
// 			var wg sync.WaitGroup
// 			wg.Add(1)
// 			go rf.SendAppendEntriesToOneServer(server, args, reply, &wg)
// 			wg.Wait()
// 		}
// 	}
// }

func (rf *Raft) AttemptRequestVote(ch chan int) {
	var votes int = 1
	var term int = rf.persistentState.CurrentTerm
	var wg sync.WaitGroup
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		wg.Add(1)
		go func(server int) {
			var startTime time.Time
			defer func() {
				log.Printf("server%d: role:%d request from %d, function finished, all time %d", rf.me, rf.role, server, time.Now().Sub(startTime).Milliseconds())
				wg.Done()
			}()
			rf.mu.Lock()
			args := &RequestVoteArgs{}
			args.CandidateId = rf.me
			args.LastLogIndex = rf.lastLogIndex
			args.LastLogTerm = rf.persistentState.Log[rf.lastLogIndex].Term
			args.Term = rf.persistentState.CurrentTerm
			rf.mu.Unlock()

			reply := &RequestVoteReply{}
			log.Printf("server%d: role:%d request vote from %d, all votes %d", rf.me, rf.role, server, votes)
			if rf.role != 1 {
				return
			}
			startTime = time.Now()
			ok := rf.sendRequestVote(server, args, reply)
			//receive reply
			rf.mu.Lock()
			if ok && rf.role == 1 {
				if reply.VoteGranted {
					votes++
					log.Printf("server%d: role:%d get vote from %d, all votes %d", rf.me, rf.role, server, votes)
					// if rf.role == 0 {
					// 	log.Printf("server%d: role:%d get expired vote from %d, all votes %d", rf.me, rf.role, server, votes)
					// 	rf.mu.Unlock()
					// 	return
					// }
					if votes > len(rf.peers)/2 {
						rf.role = 2
						// rf.persistentState.VotedFor = -1
						rf.persist()
						rf.nextIndex = make([]int, len(rf.peers))
						for i, _ := range rf.peers {
							rf.nextIndex[i] = rf.lastLogIndex + 1
						}
						if rf.persistentState.Log[len(rf.persistentState.Log)-1].Command != nil {
							rf.persistentState.Log = append(rf.persistentState.Log, Entry{})
						}
						log.Printf("server%d: become leader current Term: %d, log: %v, nextIndex: %v", rf.me, rf.persistentState.CurrentTerm, rf.persistentState.Log, rf.nextIndex)
						//if get enough votes, convert to leader immediately
						ch <- 1

					}
				} else {
					log.Printf("server%d: role:%d can't get vote from %d, all votes %d", rf.me, rf.role, server, votes)
					//spend some time to fix here
					if reply.Term > rf.persistentState.CurrentTerm && rf.role == 1 {
						log.Printf("server%d: role:%d get vote from %d, all votes %d, but term >= candidate term, reply %v, currentTerm %d", rf.me, rf.role, server, votes, reply, rf.persistentState.CurrentTerm)
						rf.role = 0
						rf.persistentState.CurrentTerm = reply.Term
						// rf.persistentState.VotedFor = -1
						rf.persist()
					}
					if term < reply.Term {
						term = reply.Term
					}
				}
			} else {
				log.Printf("server%d: role:%d get vote from %d, not ok %v, time: %d, all votes %d", rf.me, rf.role, server, ok, time.Now().Sub(startTime).Milliseconds(), votes)
			}
			rf.mu.Unlock()
		}(i)
	}
	wg.Wait()
	rf.mu.Lock()
	if rf.role == 1 {
		//if there are no enough votes, convert to follower
		log.Printf("server%d: role:%d can't get enough votes, all votes %d", rf.me, rf.role, votes)
		rf.role = 0
		rf.persistentState.CurrentTerm = term
		// rf.persistentState.VotedFor = -1
		rf.persist()
	}
	rf.mu.Unlock()
	// rf.mu.Lock()
	// if rf.role == 1 {
	// 	log.Printf("server%d: role:%d don't get enough vote %d, convert to follower", rf.me, rf.role, votes)
	// 	rf.persistentState.CurrentTerm = term
	// 	rf.persist()
	// 	rf.role = 0
	// }
	// rf.mu.Unlock()
	//convert to leader
	log.Printf("server%d: get votes: %d Term: %d, log: %v, nextIndex: %v", rf.me, votes, rf.persistentState.CurrentTerm, rf.persistentState.Log, rf.nextIndex)
	// ch <- 1
}

func electionTimeout(ch chan int) {
	//random timeout
	rand.Seed(time.Now().UnixNano())
	r := rand.Intn(100) + 300

	var electionTimeout time.Duration = time.Duration(r) * time.Millisecond
	time.Sleep(electionTimeout)
	ch <- 1
}

func candidateTimeout(ch chan int) {
	//random timeout
	rand.Seed(time.Now().UnixNano())
	r := rand.Intn(100) + 100

	var electionTimeout time.Duration = time.Duration(r) * time.Millisecond
	time.Sleep(electionTimeout)
	ch <- 1
}

func clearChannel(ch chan int) {
	for len(ch) > 0 {
		<-ch
	}
}
