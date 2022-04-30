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
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"
	//	"6.824/labgob"
	"6.824/labrpc"
	"math/rand"
	crand "crypto/rand"
	"math/big"
)


//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	lastReceivedCommunication time.Time
	currentTerm int
	votedFor int
	isLeader bool
	logs map[int]Log
	applyCh chan ApplyMsg
	commitIndex int
	lastApplied int
	nextIndexPerPeer []int
	matchIndexPerPeer []int

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

type Log struct {
	Index int
	Command interface{}
	Term int
}

type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries []Log
	LeaderCommitIndex int
}

type AppendEntriesReply struct {
	Term int
	Success bool
	ConflictingTerm int
	FirstIndexForConflictingTerm int
	LengthLog int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.isLeader
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


//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term int
	CandidateId int

	// Your data here (2A, 2B).
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf(dVote, "S%d Received Vote, Term %v, Other Raft: %v", rf.me, rf.currentTerm, args.CandidateId)
	if args.Term < rf.currentTerm {
		DPrintf(dTimer, "S%d Rejected Vote, Term %v, Other Raft: %v", rf.me, rf.currentTerm, args.CandidateId)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	} else if args.Term == rf.currentTerm && rf.votedFor == -1 {
		DPrintf(dVote, "S%d Granted Vote, Term %v, Other Raft: %v", rf.me, rf.currentTerm, args.CandidateId)
		rf.lastReceivedCommunication = time.Now()
		rf.votedFor = args.CandidateId
		rf.isLeader = false
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		return
	} else if args.Term == rf.currentTerm && rf.votedFor != -1 {
		if rf.votedFor == rf.me {
			DPrintf(dVote, "S%d Vote Rejected - I am already the leader for this term: %v, Other Raft: %v", rf.me, rf.currentTerm, args.CandidateId)
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
			return
		} else if rf.votedFor == args.CandidateId {
			DPrintf(dVote, "S%d Vote Granted Again Term %v, Other Raft: %v", rf.me, rf.currentTerm, args.CandidateId)
			rf.lastReceivedCommunication = time.Now()
			rf.votedFor = args.CandidateId
			rf.isLeader = false
			reply.VoteGranted = true
			reply.Term = rf.currentTerm
			return
		} else {
			DPrintf(dVote, "S%d (Rejected - Leader already elected) Term %v, Other Raft: %v", rf.me, rf.currentTerm, args.CandidateId)
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
			return
		}
	} else if args.Term > rf.currentTerm {
		DPrintf(dVote, "S%d (Granted - New Term), Term %v, Other Raft: %v", rf.me, args.Term, args.CandidateId)
		rf.lastReceivedCommunication = time.Now()
		rf.votedFor = args.CandidateId
		rf.currentTerm = args.Term
		rf.isLeader = false
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		return
	} 
	// Your code here (2A, 2B).
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// invalid append entries term
	if args.Term < rf.currentTerm {
		DPrintf(dTimer, "S%d Append Entries (Failure): Term %v, Other Raft: %v", rf.me, rf.currentTerm, args.LeaderId)
		reply.Term = rf.currentTerm // TODO sender should stop? anything else?
		reply.Success = false
		return 
	}

	DPrintf(dTimer, "S%d Append Entries: Term %v, Other Raft: %v", rf.me, rf.currentTerm, args.LeaderId)
	rf.lastReceivedCommunication = time.Now()
	rf.isLeader = false
	rf.currentTerm = args.Term
	reply.Term = args.Term
	if len(args.Entries) == 0 { // heartbeat from leader
		reply.Success = true
		return
	} 
	
	// entries from leader with no previous log index conflict
	_, isPrevLogIndexInLogs := rf.logs[args.PrevLogIndex] 
	if args.PrevLogIndex == -1 || (isPrevLogIndexInLogs == true  && rf.logs[args.PrevLogIndex].Term == args.Term) {
		lastLogIndex := len(rf.logs) - 1
		for i := lastLogIndex; i > args.PrevLogIndex; i-- {
			delete(rf.logs, i)
		} 
		for i := 0; i < len(args.Entries); i++ {
			newLogIndex := len(rf.logs)
			rf.logs[newLogIndex] = args.Entries[i]
		}
		rf.commitIndex = min(args.LeaderCommitIndex, len(rf.logs) - 1)
		reply.Success = true
		reply.LengthLog = len(rf.logs)	
		return
	}
	
	// conflicting prev log index from leader
	reply.Success = false
	reply.LengthLog = len(rf.logs)			
	if reply.LengthLog == 0 {
		return
	}
	reply.ConflictingTerm = rf.logs[args.PrevLogIndex].Term
	firstIndexForConflictingTerm := args.PrevLogIndex
	for rf.logs[firstIndexForConflictingTerm].Term == reply.ConflictingTerm {
		if firstIndexForConflictingTerm != args.PrevLogIndex {
			firstIndexForConflictingTerm--
		}
	}
	reply.FirstIndexForConflictingTerm = firstIndexForConflictingTerm
	reply.Success = false
	return
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.isLeader == false {
		return -1, -1, false
	} else {
		newLogIndex := len(rf.logs)
		log := Log{Command: command, Term: rf.currentTerm, Index: newLogIndex}
		rf.logs[newLogIndex] = log
		return newLogIndex, rf.currentTerm, rf.isLeader
	}
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

func (rf *Raft) applyLogs(){
	for rf.killed() == false {
		go rf.applyLog()
		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) applyLog(){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.commitIndex > rf.lastApplied {
		for i := rf.lastApplied; i <= rf.commitIndex; i++ {
			applyMsg := ApplyMsg{CommandValid: true, Command: rf.logs[i].Command, CommandIndex: i}
			rf.applyCh <- applyMsg
		}
		rf.lastApplied = rf.commitIndex
	}
}

func (rf *Raft) heartbeats(){
	for rf.killed() == false {
		go rf.startHeartbeat()
		time.Sleep(100 * time.Millisecond)
	}
}

func (rf *Raft) startHeartbeat(){
	rf.mu.Lock()
	if rf.isLeader == true {
		startedHeartbeatsTerm := rf.currentTerm
		appendEntriesArgs := &AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me}
		peersLength := len(rf.peers)
		peersReplied:= 0 
		cond := sync.NewCond(&rf.mu)
		rf.mu.Unlock()
		for i:=0; i < peersLength && rf.killed() == false; i++{
			if i != rf.me {
				go func(i int){
					reply := &AppendEntriesReply{}
					rf.sendAppendEntries(i, appendEntriesArgs, reply)
					cond.L.Lock()
					peersReplied++
					if reply.Term > rf.currentTerm {
						DPrintf(dLeader, "S%d Heartbeat (Failure): Old Term %v, New Term %v", rf.me, rf.currentTerm, reply.Term)
						rf.isLeader = false
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						cond.Broadcast()
					} else if startedHeartbeatsTerm != rf.currentTerm {
						DPrintf(dLeader, "S%d Heartbeat (Invalid - Term changed): Term %v", rf.me, rf.currentTerm)
						cond.Broadcast()
					} else if peersReplied == peersLength - 1 {
						DPrintf(dLeader, "S%d Heartbeat (Finished): Term %v, peersReplied %v", rf.me, rf.currentTerm, peersReplied)
						cond.Broadcast()
					}
					cond.L.Unlock()
				}(i)
			}
		}
		cond.L.Lock()
		cond.Wait()
		cond.L.Unlock()
	} else {
		rf.mu.Unlock()
	}
	
}

func (rf *Raft) startAppendEntries(){
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.isLeader == true {
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me {
					go rf.startAppendEntriesPerPeer(i)
				} else {
					//TODO Alan check last commited index based on majority? send to applyMsg channel?
				}
			} 
		}
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) startAppendEntriesPerPeer(peerIndex int){
	rf.mu.Lock()
	lastLogIndex := len(rf.logs) - 1
	nextPeerIndex := rf.nextIndexPerPeer[peerIndex]
	if lastLogIndex >= nextPeerIndex {
		entries:= []Log{}
		for i := nextPeerIndex; i <= lastLogIndex; i++ {
			entries = append(entries, rf.logs[i])
		}
		prevLogIndex:= nextPeerIndex - 1;
		prevLogTerm := rf.logs[nextPeerIndex-1].Term
		appendEntriesArgs := &AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me, Entries: entries, PrevLogIndex: prevLogIndex, PrevLogTerm: prevLogTerm, LeaderCommitIndex: rf.commitIndex }
		reply := &AppendEntriesReply{}
		peerReplied := rf.sendAppendEntries(peerIndex, appendEntriesArgs, reply)
		if peerReplied == false {
			return	
		}
		rf.mu.Lock()
		//TODO considerations around term changes on leader since sending request?
		if reply.Term > rf.currentTerm {
			DPrintf(dLeader, "S%d Append Entries (Term Failure): Old Term %v, New Term %v", rf.me, rf.currentTerm, reply.Term)
			rf.isLeader = false
			rf.currentTerm = reply.Term
			rf.votedFor = -1
		} else if nextPeerIndex != rf.nextIndexPerPeer[peerIndex] {
			DPrintf(dLeader, "S%d Append Entries (Next Index Changed, Ignoring result): Index before: %v, Index after: %v", nextPeerIndex, rf.nextIndexPerPeer[peerIndex])
		} else if reply.Success == true {
			DPrintf(dLeader, "S%d Append Entries (Success): Last Log index %v", lastLogIndex)
			rf.nextIndexPerPeer[peerIndex] = lastLogIndex + 1
			rf.matchIndexPerPeer[peerIndex] = lastLogIndex
		} else {
			DPrintf(dLeader, "S%d Append Entries (Failure): Last Log index %v", lastLogIndex)
			if reply.LengthLog == 0 {
				rf.nextIndexPerPeer[peerIndex] = 0
				rf.matchIndexPerPeer[peerIndex] = -1
			} else {
				if rf.logs[reply.FirstIndexForConflictingTerm].Term == reply.ConflictingTerm {
					rf.nextIndexPerPeer[peerIndex] = reply.FirstIndexForConflictingTerm + 1
				} else if reply.ConflictingTerm == 0 {
					rf.nextIndexPerPeer[peerIndex] = reply.LengthLog
				} else {
					rf.nextIndexPerPeer[peerIndex] = reply.FirstIndexForConflictingTerm
				}
			}
		}
		rf.mu.Unlock()
	} else {
		rf.mu.Unlock()
	}
}

func (rf *Raft) elections() {
	for rf.killed() == false {
		rand.Seed(raftSeed())
		electionTimeoutDuration := time.Duration(350 + (rand.Int63() % 100)) * time.Millisecond
		now := time.Now()
		rf.mu.Lock()
		if now.Sub(rf.lastReceivedCommunication) > electionTimeoutDuration && rf.isLeader == false {
			DPrintf(dTimer, "S%d Election (Timeout): PriorTerm %v, Timeout(ms) %v", rf.me, rf.currentTerm, electionTimeoutDuration)
			go rf.startElection()
		}	
		rf.mu.Unlock()
		time.Sleep(electionTimeoutDuration)
	}
}

func (rf *Raft) startElection(){
	rf.mu.Lock()
	rf.lastReceivedCommunication = time.Now()
	rf.currentTerm++
	rf.votedFor = rf.me
	termAtElectionStart := rf.currentTerm
	DPrintf(dVote, "S%d Election (Starting): Term %v", rf.me, rf.currentTerm)	
	requestVoteArgs := &RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me}
	peersLength := len(rf.peers)
	majority:= (peersLength / 2) + 1
	cond := sync.NewCond(&rf.mu)
	votesGranted := 1
	votesTaken := 1
	rf.mu.Unlock()
	for i:=0; i < peersLength && rf.killed() == false; i++{
		if i != rf.me {
			go func(i int){
				reply := &RequestVoteReply{}
				rf.sendRequestVote(i, requestVoteArgs, reply)
				cond.L.Lock()
				if reply.Term > rf.currentTerm {
					DPrintf(dVote, "S%d Election (Failure): Old Term %v, New Term %v", rf.me, rf.currentTerm, reply.Term)
					rf.isLeader = false
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					cond.Broadcast()
				} else if termAtElectionStart != rf.currentTerm {
					DPrintf(dVote, "S%d Election (Invalid - Term/Vote changed): Term %v, Election Start Term %v, voted for %v", rf.me, rf.currentTerm, termAtElectionStart, rf.votedFor)
					cond.Broadcast()
				} else {
					votesTaken++
					if reply.VoteGranted == true {
						votesGranted++
						if votesGranted >= majority {
							DPrintf(dLeader, "S%d Election (Success): Term %v, Majority %v, Votes Granted %v, Votes Taken %v", rf.me, rf.currentTerm, majority, votesGranted, votesTaken)
							rf.isLeader = true
							for i:= 0; i < len(rf.peers); i++ {
								rf.nextIndexPerPeer[i] = len(rf.logs)
								rf.matchIndexPerPeer[i] = -1
							}
							go rf.startHeartbeat()
							go rf.startAppendEntries()
							cond.Broadcast()
						}
					} else if peersLength == votesTaken{
						DPrintf(dVote, "S%d Election (Finished/Failed): Term %v, Majority %v, Votes Granted %v, Votes Taken %v", rf.me, rf.currentTerm, majority, votesGranted, votesTaken)
						cond.Broadcast()
					}
					
				}
				cond.L.Unlock()
			}(i)
		}
	}
	cond.L.Lock()
	cond.Wait()
	cond.L.Unlock()
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
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.lastReceivedCommunication = time.Now()
	rf.isLeader = false
	rf.votedFor = -1
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.logs = make(map[int]Log)
	rf.nextIndexPerPeer = make([]int, len(rf.peers))
	rf.matchIndexPerPeer = make([]int, len(rf.peers))
	rf.applyCh = applyCh
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	
	for i:= 0; i < len(rf.peers); i++ {
		rf.nextIndexPerPeer[i] = len(rf.logs)
		rf.matchIndexPerPeer[i] = -1
	}

	go rf.elections()
	go rf.heartbeats()
	go rf.applyLogs()

	return rf
}

func raftSeed() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := crand.Int(crand.Reader, max)
	x := bigx.Int64()
	return x
}

func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}