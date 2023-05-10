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
	"log"
	"math/rand"
	"os"

	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

// import "bytes"
// import "../labgob"

func init() {
	file := "./" + "message" + ".log"
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
	currentTerm int

	votedFor int
	log      []Entry

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
	leader    Leader
}

type Entry struct {
	Command interface{}
	Term    int
}

type Leader struct {
	//crashed follower list
	// crashedFollowersChan chan int

	//server agreement nums
	argeeNums int
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
	term = rf.currentTerm
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
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
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
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
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

	//if follower's term > candidate's term, reject vote
	//if follower's term equals to candidate's term, and follower's lastLogIndex > candidate's lastLogIndex,
	//reject vote
	log.Printf("server%d: role:%d receive vote request from %d, args %v, rf.LastLogTerm %v", rf.me, rf.role, args.CandidateId, args, rf.log[rf.lastLogIndex].Term)
	rf.mu.Lock()
	reply.Term = rf.currentTerm
	if rf.votedFor != -1 || rf.currentTerm > args.Term || rf.log[rf.lastLogIndex].Term > args.LastLogTerm ||
		(rf.log[rf.lastLogIndex].Term == args.LastLogTerm && rf.lastLogIndex > args.LastLogIndex) {
		reply.VoteGranted = false
		log.Printf("server%d: role:%d reject vote to %d, args %v", rf.me, rf.role, args.CandidateId, args)
	} else {
		if rf.role == 0 {
			clearChannel(rf.follower.recRV)
			rf.follower.recRV <- 1
		} else if rf.role == 1 {
			clearChannel(rf.candidate.recRV)
			rf.candidate.recRV <- 1
		}
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
	}
	rf.mu.Unlock()
	// if rf.votedFor != -1 || args.LastLogTerm < rf.currentTerm {
	// 	reply.VoteGranted = false
	// } else if args.LastLogIndex >= rf.lastLogIndex {
	// 	clearChannel(rf.follower.recRV)
	// 	rf.follower.recRV <- 1
	// 	reply.VoteGranted = true
	// 	rf.votedFor = args.CandidateId
	// }
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
		rf.me, rf.role, args.LeaderId, args.Term, rf.currentTerm, args, args.PrevLogIndex, rf.lastLogIndex, rf.log)
	reply.Success = false
	rf.mu.Lock()
	//when receive from leader, refresh rf.votedFor
	rf.votedFor = -1

	//if follower's or candidate's term < leader's term, then update their term
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
	}

	//return false if leader's term < follower's term(1) or if log doesn’t contain an entry at prevLogIndex
	//whose term matches prevLogTerm(2)
	//a. different term
	//b. different index
	//c. different log term
	if args.Term < rf.currentTerm {
		log.Printf("server%d: role:%d bigger term, append entries from %d", rf.me, rf.role, args.LeaderId)
	} else if args.PrevLogIndex > rf.lastLogIndex {
		log.Printf("server%d: role:%d logger index: %d, %d, append entries from %d", rf.me, rf.role, args.PrevLogIndex, rf.lastLogIndex, args.LeaderId)
		reply.XTerm = -1
		reply.XIndex = -1
		reply.XLastIndex = rf.lastLogIndex + 1
	} else if rf.lastLogIndex != 0 && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		log.Printf("server%d: role:%d not equals log term %d, %d, append entries from %d", rf.me, rf.role, args.PrevLogTerm, rf.log[args.PrevLogIndex].Term, args.LeaderId)
		reply.XTerm = rf.log[args.PrevLogIndex].Term
		index := args.PrevLogIndex
		for index >= 0 && rf.log[index].Term != 0 && rf.log[index].Term == rf.log[args.PrevLogIndex].Term {
			reply.XIndex = index
			index--
		}
	} else if args.PrevLogIndex <= rf.lastLogIndex && (rf.lastLogIndex == 0 || (rf.lastLogIndex != 0 && rf.log[args.PrevLogIndex].Term == args.PrevLogTerm)) {
		reply.Success = true
		//if leadear send empty command to follower,and they has same log, then don't need to update
		if args.Entries[0].Command != nil {
			//delete after rf.lastLogIndex
			rf.log = rf.log[:args.PrevLogIndex+1]
			rf.log = append(rf.log, args.Entries...)
			rf.lastLogIndex = len(rf.log) - 2
			log.Printf("server%d: role:%d successfully append entries new log %v rf.lastLogIndex %v", rf.me, rf.role, rf.log, rf.lastLogIndex)
		}
		log.Printf("server%d: role:%d rf.commitIndex %d args.LeaderCommit %d", rf.me, rf.role, rf.commitIndex, args.LeaderCommit)
		if rf.commitIndex < args.LeaderCommit {
			nums := args.LeaderCommit - rf.commitIndex
			for i := 1; i <= nums; i++ {
				applyMsg := &ApplyMsg{true, rf.log[rf.commitIndex+i].Command, rf.commitIndex + i}
				rf.applyCh <- *applyMsg
				log.Printf("server%d: follower commit %d %v", rf.me, rf.commitIndex+i, rf.log[rf.commitIndex+i])
			}
			rf.commitIndex = args.LeaderCommit
		}
	}

	// //follower or candidate append leader's log
	// if len(args.Entries) != 0 {
	// 	for _, command := range args.Entries {
	// 		rf.log = append(rf.log, Entry{command: command, term: args.Term})
	// 	}

	// 	rf.lastLogIndex = len(rf.log)
	// }

	//if follower receive AppendEntries, reset election timeout
	if rf.role == 0 {
		clearChannel(rf.follower.recAE)
		rf.follower.recAE <- 1
		//if candidate receive AppendEntries, if leader term >= candidate term, candidate convert to follower,
	} else if rf.role == 1 {
		if args.Term >= rf.currentTerm {
			rf.role = 0
			clearChannel(rf.candidate.recAE)
			rf.candidate.recAE <- 1
		}
		//if there are over 1 leader at the same time, then other leaders must convert to vandidate, elect again
	} else if rf.role == 2 {
		if args.Term >= rf.currentTerm {
			rf.role = 0
			rf.currentTerm = args.Term
		}
	}

	reply.Term = rf.currentTerm
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
	term := rf.currentTerm
	isLeader := true

	// Your code here (2B).
	if rf.role == 2 {
		log.Printf("server%d: Leader receive command %v from client", rf.me, command)
		entry := Entry{command, rf.currentTerm}
		rf.mu.Lock()
		//update log, lastLogIndex, nextIndex
		rf.log[rf.lastLogIndex+1] = entry
		rf.log = append(rf.log, Entry{})
		rf.lastLogIndex = len(rf.log) - 2
		//some followers may don't catch the leader, so the nextIndex is not equals to rf.lastLogIndex,
		//actually, these followers will return false
		for index, nextIndex := range rf.nextIndex {
			if nextIndex == rf.lastLogIndex {
				rf.nextIndex[index] = rf.lastLogIndex + 1
			}
			log.Printf("server%d: Leader new nextIndex %v, log %v", rf.me, rf.nextIndex, rf.log)
		}
		rf.mu.Unlock()

		//if there are many request from client at the same time, then there will start many goroutine to request entries
		// go rf.AttemptSendAppendEntries()
	} else {
		isLeader = false
	}
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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.applyCh = applyCh
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.log = make([]Entry, 0)
	//fill the index 0, because the log index begin with 1
	rf.log = append(rf.log, Entry{})
	//pre append
	rf.log = append(rf.log, Entry{})
	rf.lastLogIndex = 0
	rf.commitIndex = 0
	rf.votedFor = -1
	rf.follower.recAE = make(chan int, 1)
	rf.follower.recRV = make(chan int, 1)
	rf.candidate.recAE = make(chan int, 1)
	rf.candidate.recRV = make(chan int, 1)

	// rf.leader.crashedFollowersChan = make(chan int, len(peers))

	// Your initialization code here (2A, 2B, 2C).
	go StateListening(rf)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func StateListening(rf *Raft) {
	for {
		//follower's task is to reply the leader's or candidate's request, and monitor the leader's heartbeat
		if rf.role == 0 {
			log.Printf("server%d: Follower, current Term: %d, log: %v", rf.me, rf.currentTerm, rf.log)
			var followerElectionTimeoutChan chan int = make(chan int, 1)
			go electionTimeout(followerElectionTimeoutChan)

			select {
			case <-followerElectionTimeoutChan:
				rf.mu.Lock()
				// if rf.votedFor == -1 {
				//convert to candidate
				rf.role = 1
				rf.votedFor = -1
				rf.currentTerm++
				log.Printf("server%d: convert to candidate, current Term: %d, log: %v", rf.me, rf.currentTerm, rf.log)
				// }
				rf.mu.Unlock()
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
			log.Printf("server%d: Candidate, current Term: %d, log: %v", rf.me, rf.currentTerm, rf.log)

			log.Printf("server%d: Candidate, request vote current Term: %d, log: %v", rf.me, rf.currentTerm, rf.log)
			var candidateAttemptRequestVoteChan chan int = make(chan int, 1)
			go rf.AttemptRequestVote(candidateAttemptRequestVoteChan)

			var candidateElectionTimeoutChan chan int = make(chan int, 1)
			go electionTimeout(candidateElectionTimeoutChan)

			//during this time,a candidate may convert to follower
			select {
			case <-candidateElectionTimeoutChan:
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
			log.Printf("server%d: Leader, current Term: %d, nextIndex: %v, log: %v", rf.me, rf.currentTerm, rf.nextIndex, rf.log)
			// beginTime := time.Now().UnixNano() / 1e6
			go rf.AttemptSendAppendEntries()
			// duration := time.Now().UnixNano()/1e6 - beginTime
			// log.Printf("server%d: Leader, duration %v", rf.me, duration)

			//wait heartbeat timeout
			var heartbeatTimeout time.Duration = time.Duration(100) * time.Millisecond
			time.Sleep(heartbeatTimeout)
		}
	}
}

func (rf *Raft) AttemptSendAppendEntries() {
	rf.leader.argeeNums = 1
	var wg sync.WaitGroup
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		wg.Add(1)
		log.Printf("server%d: leader ready to send to %d", rf.me, i)
		go rf.SendAppendEntriesToOneServer(i, &wg)
	}
	wg.Wait()

}

func (rf *Raft) SendAppendEntriesToOneServer(server int, wg *sync.WaitGroup) {
	defer wg.Done()
	reply := &AppendEntriesReply{}

	rf.mu.Lock()
	//send heartbeats
	args := &AppendEntriesArgs{}
	args.LeaderCommit = rf.commitIndex
	args.LeaderId = rf.me
	args.PrevLogIndex = rf.nextIndex[server] - 1
	args.PrevLogTerm = rf.log[rf.nextIndex[server]-1].Term
	args.Term = rf.currentTerm
	entries := rf.log[rf.nextIndex[server]:]
	log.Printf("server%d: ready to send %d entries is %v, rf.nextIndex is %v", rf.me, server, entries, rf.nextIndex)
	rf.mu.Unlock()
	args.Entries = entries

	ok := rf.sendAppendEntries(server, args, reply)
	//receive reply
	//if the server is healthy, it will return reply immediately, and next heartbeat will send up-to-date info, don't need to wait other servers
	if ok {
		if reply.Success {
			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				rf.role = 0
			} else {
				rf.leader.argeeNums++
				rf.nextIndex[server] = rf.lastLogIndex + 1
			}
			rf.mu.Unlock()
		} else {
			//if not success, check the term, XTerm, XIndex, XLastIndex
			rf.mu.Lock()
			if rf.currentTerm < reply.Term {
				//convert to a follower and update term
				rf.role = 0
				log.Printf("server%d: update term to %d from follower %d", rf.me, reply.Term, server)
				rf.currentTerm = reply.Term
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
			rf.mu.Unlock()
		}
		//if leader has command not been commited
		//there maybe client request leader as a result rf.lastLogIndex is being changed, so need to add a lock
		rf.mu.Lock()
		if rf.commitIndex < rf.lastLogIndex {
			if rf.leader.argeeNums > len(rf.peers)/2 {
				nums := rf.lastLogIndex - rf.commitIndex
				for i := 1; i <= nums; i++ {
					applyMsg := &ApplyMsg{true, rf.log[rf.commitIndex+i].Command, rf.commitIndex + i}
					rf.applyCh <- *applyMsg
					log.Printf("server%d: leader commit %d %v", rf.me, rf.commitIndex+i, rf.log[rf.commitIndex+i])
				}
				rf.commitIndex = rf.lastLogIndex
			}
		}
		rf.mu.Unlock()
	}
	// else {
	// 	//if follower reply timeout, will retry later
	// 	rf.leader.crashedFollowersChan <- server
	// }
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
	var term int = 0
	var wg sync.WaitGroup
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		wg.Add(1)
		go func(server int) {
			defer func() {
				log.Printf("server%d: role:%d request from %d, function finished", rf.me, rf.role, server)
				wg.Done()
			}()
			args := &RequestVoteArgs{}
			args.CandidateId = rf.me
			args.LastLogIndex = rf.lastLogIndex
			args.LastLogTerm = rf.log[rf.lastLogIndex].Term
			args.Term = rf.currentTerm

			reply := &RequestVoteReply{}
			ok := rf.sendRequestVote(server, args, reply)
			//receive reply
			if ok {
				if reply.VoteGranted {
					if reply.Term > rf.currentTerm {
						log.Printf("server%d: role:%d get vote from %d, all votes %d, but term >= candidate term, reply %v, currentTerm %d", rf.me, rf.role, server, votes, reply, rf.currentTerm)
						rf.role = 0
						return
					}
					rf.mu.Lock()
					votes++
					log.Printf("server%d: role:%d get vote from %d, all votes %d", rf.me, rf.role, server, votes)
					if rf.role != 2 && votes > len(rf.peers)/2 {
						rf.role = 2
						rf.nextIndex = make([]int, len(rf.peers))
						for i, _ := range rf.peers {
							rf.nextIndex[i] = rf.lastLogIndex + 1
						}
						if rf.log[len(rf.log)-1].Command != nil {
							rf.log = append(rf.log, Entry{})
						}
						log.Printf("server%d: become leader current Term: %d, log: %v, nextIndex: %v", rf.me, rf.currentTerm, rf.log, rf.nextIndex)
					}
					rf.mu.Unlock()

				} else {
					log.Printf("server%d: role:%d can't get vote from %d, all votes %d", rf.me, rf.role, server, votes)
					if term < reply.Term {
						term = reply.Term
					}
				}
			} else {
				log.Printf("server%d: role:%d get vote from %d, not ok, all votes %d", rf.me, rf.role, server, votes)
			}
		}(i)
	}
	wg.Wait()
	rf.mu.Lock()
	if rf.role != 2 {
		log.Printf("server%d: role:%d don't get enough vote %d, convert to follower", rf.me, rf.role, votes)
		rf.currentTerm = term
		rf.role = 0
	}
	rf.mu.Unlock()
	//convert to leader
	log.Printf("server%d: get votes: %d Term: %d, log: %v, nextIndex: %v", rf.me, votes, rf.currentTerm, rf.log, rf.nextIndex)
	ch <- 1
}

func electionTimeout(ch chan int) {
	//random timeout
	rand.Seed(time.Now().UnixNano())
	r := rand.Intn(100) + 200

	var electionTimeout time.Duration = time.Duration(r) * time.Millisecond
	time.Sleep(electionTimeout)
	ch <- 1
}

func clearChannel(ch chan int) {
	for len(ch) > 0 {
		<-ch
	}
}
