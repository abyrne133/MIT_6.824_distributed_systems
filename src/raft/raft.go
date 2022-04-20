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

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

type AppendEntriesArgs struct {
	Term int
	LeaderId int
}

type AppendEntriesReply struct {
	Term int
	Success bool
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
	DPrintf("Vote Request (Received): Raft %v, Term %v, Other Raft: %v", rf.me, rf.currentTerm, args.CandidateId)
	if args.Term < rf.currentTerm {
		DPrintf("Vote Request (Rejected): Raft %v, Term %v, Other Raft: %v", rf.me, rf.currentTerm, args.CandidateId)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	} else if args.Term == rf.currentTerm && rf.votedFor == -1 {
		DPrintf("Vote Request (Granted): Raft %v, Term %v, Other Raft: %v", rf.me, rf.currentTerm, args.CandidateId)
		rf.lastReceivedCommunication = time.Now()
		rf.votedFor = args.CandidateId
		rf.isLeader = false
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		return
	} else if args.Term == rf.currentTerm && rf.votedFor != -1 {
		if rf.votedFor == rf.me {
			DPrintf("Vote Request (Rejected - I am already the leader for this term): Raft %v, Term %v, Other Raft: %v", rf.me, rf.currentTerm, args.CandidateId)
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
			return
		} else if rf.votedFor == args.CandidateId {
			DPrintf("Vote Request (Granted Again): Raft %v, Term %v, Other Raft: %v", rf.me, rf.currentTerm, args.CandidateId)
			rf.lastReceivedCommunication = time.Now()
			rf.votedFor = args.CandidateId
			rf.isLeader = false
			reply.VoteGranted = true
			reply.Term = rf.currentTerm
			return
		} else {
			DPrintf("Vote Request (Rejected - Leader already elected): Raft %v, Term %v, Other Raft: %v", rf.me, rf.currentTerm, args.CandidateId)
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
			return
		}
	} else if args.Term > rf.currentTerm {
		DPrintf("Vote Request (Granted - New Term): Raft %v, Term %v, Other Raft: %v", rf.me, args.Term, args.CandidateId)
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
	if args.Term < rf.currentTerm {
		DPrintf("Append Entries (Failure): Raft %v, Term %v, Other Raft: %v", rf.me, rf.currentTerm, args.LeaderId)
		reply.Term = rf.currentTerm
		reply.Success = false
		return 
	} else {
		DPrintf("Append Entries (Success): Raft %v, Term %v, Other Raft: %v", rf.me, rf.currentTerm, args.LeaderId)
		rf.lastReceivedCommunication = time.Now()
		rf.isLeader = false
		rf.currentTerm = args.Term
		reply.Term = args.Term
		reply.Success = true
		if args.Term > rf.currentTerm {
			rf.votedFor = -1
		}
		return
	} 
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


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

func (rf *Raft) heartbeats(){
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.isLeader == true {
			startedHeartbeatsTerm := rf.currentTerm
			var waitGroup sync.WaitGroup
			replies := make([]*AppendEntriesReply, len(rf.peers))
			appendEntriesArgs := &AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me}
			for i:=0; i < len(rf.peers) && rf.killed() == false; i++{
				if i != rf.me {
					waitGroup.Add(1)
					go func(i int){
						defer waitGroup.Done()
						reply := &AppendEntriesReply{}
						rf.sendAppendEntries(i, appendEntriesArgs, reply)
						replies[i] = reply
					}(i)
				}
			
			}
			DPrintf("Heartbeat Request (Waiting for all Peers): Raft %v, Term %v", rf.me, rf.currentTerm)
			rf.mu.Unlock()
			waitGroup.Wait()
			rf.mu.Lock()
			if startedHeartbeatsTerm != rf.currentTerm {
				DPrintf("Heartbeat Request (Invalid - Term changed): Raft %v, Term %v", rf.me, rf.currentTerm)
			} else {
				DPrintf("Heartbeat Request (All Peers Responded): Raft %v, Term %v", rf.me, rf.currentTerm)
				for index, reply := range replies {
					if index != rf.me && reply.Term > rf.currentTerm  {
						rf.isLeader = false
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						DPrintf("Heartbeat Request (New Leader): Raft %v, Term %v, Other Raft: %v", rf.me, rf.currentTerm, index)
						break
					} 
				}	
			}
		}
		rf.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}	
}

func (rf *Raft) elections() {
	for rf.killed() == false {
		rand.Seed(raftSeed())
		electionTimeoutDuration := time.Duration(350 + (rand.Int63() % 100)) * time.Millisecond
		now := time.Now()
		rf.mu.Lock()
		if now.Sub(rf.lastReceivedCommunication) > electionTimeoutDuration && rf.isLeader == false {
			DPrintf("Election (Timeout): Raft %v, PriorTerm %v, Timeout(ms) %v", rf.me, rf.currentTerm, electionTimeoutDuration)
			rf.mu.Unlock()
			rf.startElection()
		}else {
			rf.mu.Unlock()
		}
		time.Sleep(electionTimeoutDuration)
	}
}

func (rf *Raft) startElection(){
	rf.mu.Lock()
	rf.lastReceivedCommunication = time.Now()
	rf.currentTerm++
	rf.votedFor = rf.me
	termAtElectionStart := rf.currentTerm
	DPrintf("Election (Starting): Raft %v, Term %v", rf.me, rf.currentTerm)	
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
					DPrintf("Election (Failure): Raft %v, Old Term %v, New Term %v", rf.me, rf.currentTerm, reply.Term)
					rf.isLeader = false
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					cond.Broadcast()
				} else if termAtElectionStart != rf.currentTerm || rf.votedFor != rf.me {
					DPrintf("Election (Invalid - Term/Vote changed): Raft %v, Term %v, Election Start Term %v, voted for %v", rf.me, rf.currentTerm, termAtElectionStart, rf.votedFor)
					rf.votedFor = -1
					cond.Broadcast()
				} else {
					votesTaken++
					if reply.VoteGranted == true {
						votesGranted++
						if votesGranted >= majority {
							DPrintf("Election (Success): Raft %v, Term %v, Majority %v, Votes Granted %v, Votes Taken %v", rf.me, rf.currentTerm, majority, votesGranted, votesTaken)
							rf.isLeader = true
							cond.Broadcast()
						}
					} else if peersLength == votesTaken{
						DPrintf("Election (Finished/Failed): Raft %v, Term %v, Majority %v, Votes Granted %v, Votes Taken %v", rf.me, rf.currentTerm, majority, votesGranted, votesTaken)
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
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.elections()
	go rf.heartbeats()

	return rf
}

func raftSeed() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := crand.Int(crand.Reader, max)
	x := bigx.Int64()
	return x
}
