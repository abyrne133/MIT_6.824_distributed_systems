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
	"errors"
	"bytes"
	"sync"
	"sync/atomic"
	"time"
	"6.824/labgob"
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

type PersistedRaft struct {
	CurrentTerm int
	VotedFor int
	Logs map[int]Log
	BaseLogIndex int
	LastSnapshotTerm int
} 

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	lastReceivedCommunication time.Time
	CurrentTerm int
	VotedFor int
	votesGranted int
	isLeader bool
	Logs map[int]Log
	baseLogIndex int
	lastSnapshotTerm int
	applyCh chan ApplyMsg
	commitIndex int
	lastApplied int
	nextIndexPerPeer []int
	matchIndexPerPeer []int
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
	XTerm int
	XIndex int
	XLength int
}

type InstallSnapshotArgs struct {
	Term int
	LastIncludedIndex int
	LastIncludedTerm int
	Snapshot []byte
}

type InstallSnapshotReply struct {
	Term int
	Success bool
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.CurrentTerm, rf.isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	data, err := rf.encodeRaftState()
	if err != nil {
		DPrintf(dError, "Error encoding raft state")
	} else {
		rf.persister.SaveRaftState(data)
	}
}

func (rf *Raft) encodeRaftState() ([]byte, error) {
	writer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(writer)
	err := encoder.Encode(PersistedRaft{CurrentTerm: rf.CurrentTerm, VotedFor: rf.VotedFor, Logs: rf.Logs, BaseLogIndex: rf.baseLogIndex, LastSnapshotTerm: rf.lastSnapshotTerm})
	if err != nil {
		return nil, errors.New("Error encoding persistent state")
	} else {
		return writer.Bytes(), nil
	}
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	reader := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(reader)
	var raft PersistedRaft
	err := decoder.Decode(&raft)
	if err != nil {
		DPrintf(dError, "Error decoding persistent state")
	} else {
		rf.CurrentTerm = raft.CurrentTerm
		rf.VotedFor = raft.VotedFor
		rf.Logs = raft.Logs
		rf.lastSnapshotTerm = raft.LastSnapshotTerm
		rf.baseLogIndex = raft.BaseLogIndex
	}
	for i:=1; i<=rf.baseLogIndex; i++ {
		delete(rf.Logs,i)
	}
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.snapshotLockAlreadyObtained(index, snapshot)
}

func (rf *Raft) snapshotLockAlreadyObtained(index int, snapshot []byte){
	rf.lastSnapshotTerm = rf.Logs[index].Term
	for i := 1; i <= index; i++ {
		delete(rf.Logs, i)
	}
	rf.baseLogIndex = index	
	raftState, err := rf.encodeRaftState()
	if err != nil {
		DPrintf(dError, "Error encoding raft state")
	} else {
		rf.persister.SaveStateAndSnapshot(raftState, snapshot)
	}
}


type RequestVoteArgs struct {
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

type RequestVoteReply struct {
	Term int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf(dVote, "S%d Received Vote, Term %v, Other Raft: %v", rf.me, rf.CurrentTerm, args.CandidateId)
	if args.Term < rf.CurrentTerm {
		DPrintf(dTimer, "S%d Rejected Vote, Term %v, Other Raft: %v, Other Raft Term: %v", rf.me, rf.CurrentTerm, args.CandidateId, args.Term)
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
		return
	} 

	if args.Term > rf.CurrentTerm {
		rf.VotedFor = -1
		rf.isLeader = false
		rf.CurrentTerm = args.Term
		rf.persist()
	}
	
	lastLogIndex := rf.logLength()
	lastLogTerm := rf.Logs[lastLogIndex].Term
	if (rf.VotedFor == -1 || rf.VotedFor == args.CandidateId) && isCandidateLogUpToDate(lastLogIndex, lastLogTerm, args.LastLogIndex, args.LastLogTerm){
		DPrintf(dVote, "S%d Granted Vote, Term %v, Other Raft: %v", rf.me, rf.CurrentTerm, args.CandidateId)
		rf.lastReceivedCommunication = time.Now()
		rf.VotedFor = args.CandidateId
		rf.isLeader = false
		rf.CurrentTerm = args.Term
		reply.VoteGranted = true
		reply.Term = rf.CurrentTerm
		rf.persist()
		return
	} else {
		DPrintf(dVote, "S%d Rejected Vote, Term %v, Other Raft: %v", rf.me, rf.CurrentTerm, args.CandidateId)
		reply.VoteGranted = false
		reply.Term = rf.CurrentTerm
		return
	}
}

func isCandidateLogUpToDate(lastLogIndex int, lastLogTerm int, candidateLastLogIndex int, candidateLastLogTerm int) bool {
	if lastLogTerm > candidateLastLogTerm {
		return false
	} else if lastLogTerm < candidateLastLogTerm {
		return true
	} else { // equal terms
		if lastLogIndex > candidateLastLogIndex {
			return false
		} else {
			return true
		}
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// leader has invalid term
	if args.Term < rf.CurrentTerm {
		DPrintf(dTimer, "S%d Append Entries (Failure): Term %v, Other Raft: %v", rf.me, rf.CurrentTerm, args.LeaderId)
		reply.Term = rf.CurrentTerm
		reply.Success = false
		return 
	}

	DPrintf(dTimer, "S%d Append Entries: Term %v, Other Raft: %v", rf.me, rf.CurrentTerm, args.LeaderId)
	rf.lastReceivedCommunication = time.Now()
	rf.isLeader = false
	rf.CurrentTerm = args.Term
	reply.Term = args.Term
	rf.persist()

	prevLog, isPrevLogIndexInLogs := rf.Logs[args.PrevLogIndex]
	if args.PrevLogIndex != 0 && isPrevLogIndexInLogs == false {
		DPrintf(dLog, "S%v Conflict log too short", rf.me)
		reply.Success = false
		reply.XLength = rf.logLength()	
		reply.XTerm = 0
		reply.XIndex = 0
		return 
	}	

	if args.PrevLogIndex != 0 && isPrevLogIndexInLogs == true  && prevLog.Term != args.PrevLogTerm  {
		reply.Success = false
		for i:=rf.baseLogIndex + 1; i <= rf.logLength(); i++{
			if rf.Logs[i].Term == prevLog.Term {
				reply.XTerm = prevLog.Term
				reply.XIndex = i
				DPrintf(dLog, "S%v Conflicting index %v", rf.me, reply.XIndex)
				return
			}
		}

		panic("First instance of an already found conflicting term not found in logs")
		return
	}

	// if an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it
	for i := 0; i < len(args.Entries); i++ {
		index := args.Entries[i].Index
		term  := args.Entries[i].Term
		existingLog, containsLog := rf.Logs[index]
		if containsLog && term != existingLog.Term {
			for j := index; j <= rf.logLength(); j++ {
				delete(rf.Logs, j)
			}
		}
	}

	// append any entries not already in the log
	DPrintf(dLog, "S%v Appending %v entries", rf.me, len(args.Entries))
	for i := 0; i < len(args.Entries); i++ {
		index := args.Entries[i].Index
		_, containsLog := rf.Logs[index]
		if containsLog == false {
			rf.Logs[index] = args.Entries[i]
		}
	}

	rf.persist()
	
	if args.LeaderCommitIndex > rf.commitIndex {
		indexOfLastNewEntry := args.LeaderCommitIndex
		if len(args.Entries) > 0 {
			indexOfLastNewEntry = args.Entries[len(args.Entries)-1].Index
		}
		rf.commitIndex = min(args.LeaderCommitIndex, indexOfLastNewEntry)
	}

	reply.Success = true	
	DPrintf(dLog, "S%v Leader commit index %v last log index %v, commit index %v", rf.me, args.LeaderCommitIndex, rf.logLength(), rf.commitIndex)	
	return


	
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// leader has invalid term
	if args.Term < rf.CurrentTerm {
		DPrintf(dTimer, "S%d Install Snapshot (Failure): Term %v", rf.me, rf.CurrentTerm)
		reply.Term = rf.CurrentTerm
		reply.Success = false
		return 
	}

	DPrintf(dTimer, "S%d Install Snapshot: Term %v", rf.me, rf.CurrentTerm)
	rf.lastReceivedCommunication = time.Now()
	rf.isLeader = false
	rf.CurrentTerm = args.Term
	reply.Term = args.Term
	rf.snapshotLockAlreadyObtained(args.LastIncludedIndex, args.Snapshot)

	// snapshot overlaps with existing log, delete overlap
	prevLog, isLastIncludedIndexInLogs := rf.Logs[args.LastIncludedIndex]
	if args.LastIncludedIndex != 0 && isLastIncludedIndexInLogs == true  && prevLog.Term == args.LastIncludedTerm  {
		reply.Success = true
		for i:= args.LastIncludedIndex; i >= 1; i-- {
			delete(rf.Logs, i)
		}
		return
	}

	rf.Logs = make(map[int]Log)
	rf.persist()
	applyMsg := ApplyMsg{CommandValid: false, SnapshotValid: true, Snapshot: args.Snapshot, SnapshotTerm: args.LastIncludedTerm, SnapshotIndex: args.LastIncludedIndex}
	rf.applyCh <- applyMsg
	reply.Success = true
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs) {
	reply := &RequestVoteReply{}
	peerReplied := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if peerReplied == false {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.CurrentTerm {
		DPrintf(dVote, "S%d Election (Failure): Old Term %v, New Term %v", rf.me, rf.CurrentTerm, reply.Term)
		rf.isLeader = false
		rf.CurrentTerm = reply.Term
		rf.VotedFor = -1
		rf.persist()
		return
	} else if args.Term != rf.CurrentTerm {
		DPrintf(dVote, "S%d Election (Invalid - Term/Vote changed): Term %v, Election Start Term %v, voted for %v", rf.me, rf.CurrentTerm, args.Term, rf.VotedFor)
		return
	} else if reply.VoteGranted == true {
		rf.votesGranted++
		majority:= (len(rf.peers) / 2) + 1
		if rf.votesGranted >= majority && rf.isLeader == false {
			DPrintf(dLeader, "S%d Election (Success): Term %v, Majority %v, Votes Granted %v", rf.me, rf.CurrentTerm, majority, rf.votesGranted)
			rf.isLeader = true
			for i:= 0; i < len(rf.peers); i++ {
				if i != rf.me {
					rf.nextIndexPerPeer[i] = rf.logLength() + 1
					rf.matchIndexPerPeer[i] = 0
					rf.startAppendEntriesPerPeer(i)
				} 
			}
		}
	}
}

func (rf *Raft) sendAppendEntries(peerIndex int, args *AppendEntriesArgs, lastLogIndex int) {
	reply := &AppendEntriesReply{}
	peerReplied := rf.peers[peerIndex].Call("Raft.AppendEntries", args, reply)
	if peerReplied == false {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	if reply.Term > rf.CurrentTerm {
		DPrintf(dLeader, "S%d Append Entries (Term Failure): Old Term %v, New Term %v, Other Raft %v", rf.me, rf.CurrentTerm, reply.Term, peerIndex)
		rf.isLeader = false
		rf.CurrentTerm = reply.Term
		rf.VotedFor = -1
		rf.persist()
		return
	} 
	
	if args.Term != rf.CurrentTerm {
		return
	} 
	
	if reply.Success == true {
		DPrintf(dLeader, "S%d Append Entries (Success): Last Log index %v, Other raft %v", rf.me, lastLogIndex, peerIndex)
		rf.nextIndexPerPeer[peerIndex] = lastLogIndex + 1
		rf.matchIndexPerPeer[peerIndex] = args.PrevLogIndex + len(args.Entries)
		rf.commitLogsOnce()
		rf.applyLogsOnce()
	} else {
		DPrintf(dLeader, "S%d Append Entries (Failure): Last Log index %v, Other raft %v", rf.me, lastLogIndex, peerIndex)
		// followers log was too short
		if reply.XTerm == 0 || reply.XIndex == 0 {
			rf.nextIndexPerPeer[peerIndex] = reply.XLength
			return
		}

		// leader has XTerm
		for i:=rf.logLength(); i > rf.baseLogIndex; i-- {
			if rf.Logs[i].Term == reply.XTerm {
				rf.nextIndexPerPeer[peerIndex]= i
				return
			}
		}

		// leader doesn't have XTerm
		rf.nextIndexPerPeer[peerIndex] = reply.XIndex
	}
}

func (rf *Raft) sendInstallSnapshot(peerIndex int, args *InstallSnapshotArgs) {
	reply := &InstallSnapshotReply{}
	peerReplied := rf.peers[peerIndex].Call("Raft.InstallSnapshot", args, reply)
	if peerReplied == false {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	if reply.Term > rf.CurrentTerm {
		DPrintf(dLeader, "S%d Install Snapshot (Term Failure): Old Term %v, New Term %v, Other Raft %v", rf.me, rf.CurrentTerm, reply.Term, peerIndex)
		rf.isLeader = false
		rf.CurrentTerm = reply.Term
		rf.VotedFor = -1
		rf.persist()
		return
	} 
	
	if reply.Success == true {
		rf.nextIndexPerPeer[peerIndex] = rf.logLength() + 1
	}
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
		return 0, 0, false
	} else {
		newLogIndex := rf.logLength() + 1
		log := Log{Command: command, Term: rf.CurrentTerm, Index: newLogIndex}
		rf.Logs[newLogIndex] = log
		rf.persist()
		return newLogIndex, rf.CurrentTerm, rf.isLeader
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
		rf.mu.Lock()
		rf.applyLogsOnce()
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) applyLogsOnce(){
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		applyMsg := ApplyMsg{CommandValid: true, Command: rf.Logs[i].Command, CommandIndex: i}
		rf.applyCh <- applyMsg
	}	
	rf.lastApplied = rf.commitIndex	
}

func (rf *Raft) startAppendEntries(){
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.isLeader == true {
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me {
					rf.startAppendEntriesPerPeer(i)
				} 
			}
		} 
		rf.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

func (rf *Raft) startAppendEntriesPerPeer(peerIndex int){
	lastLogIndex := rf.logLength()
	nextPeerIndex := rf.nextIndexPerPeer[peerIndex]
	if nextPeerIndex <= rf.baseLogIndex {
		installSnapshotArgs := &InstallSnapshotArgs{Term: rf.CurrentTerm, LastIncludedIndex: rf.baseLogIndex, LastIncludedTerm: rf.lastSnapshotTerm, Snapshot: rf.persister.ReadSnapshot()}
		go rf.sendInstallSnapshot(peerIndex, installSnapshotArgs)
		return
	}
	// DPrintf(dLog, "S%v Last Log Index %v next peer index %v, other raft %v", rf.me, lastLogIndex, nextPeerIndex, peerIndex)
	entries:= []Log{}
	if lastLogIndex >= nextPeerIndex {
		for i := nextPeerIndex; i <= lastLogIndex; i++ {
			entries = append(entries, rf.Logs[i])
		}
	}
	prevLogIndex:= nextPeerIndex - 1;
	if prevLogIndex == rf.bas //HERE and request vote too..?
	prevLogTerm := rf.Logs[prevLogIndex].Term
	appendEntriesArgs := &AppendEntriesArgs{Term: rf.CurrentTerm, LeaderId: rf.me, Entries: entries, PrevLogIndex: prevLogIndex, PrevLogTerm: prevLogTerm, LeaderCommitIndex: rf.commitIndex }
	
	go rf.sendAppendEntries(peerIndex, appendEntriesArgs, lastLogIndex)
}

func (rf *Raft) commitLogs(){
	for rf.killed() == false {
		rf.mu.Lock()
		rf.commitLogsOnce()
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) commitLogsOnce(){
	if rf.isLeader == true {
		majority := (len(rf.peers) / 2) + 1
		for N := rf.logLength(); N > rf.commitIndex; N-- {
			if rf.Logs[N].Term == rf.CurrentTerm {
				committedPeersCount := 1
				for i := 0; i < len(rf.matchIndexPerPeer); i++ {
					if i != rf.me && rf.matchIndexPerPeer[i] >= N {
						committedPeersCount++
					}
				}
				// DPrintf(dLog, "S%v Index %v Replicated Peers Count %v Majority %v", rf.me, i, committedPeersCount, majority )
				if committedPeersCount >= majority {
					rf.commitIndex = N
					return
				}
			}
		}
	}
}

func (rf *Raft) elections() {
	for rf.killed() == false {
		rand.Seed(raftSeed())
		electionTimeoutDuration := time.Duration(350 + (rand.Int63() % 100)) * time.Millisecond
		now := time.Now()
		rf.mu.Lock()
		if now.Sub(rf.lastReceivedCommunication) > electionTimeoutDuration && rf.isLeader == false {
			DPrintf(dTimer, "S%d Election (Timeout): PriorTerm %v, Timeout(ms) %v", rf.me, rf.CurrentTerm, electionTimeoutDuration)
			rf.CurrentTerm++
			rf.VotedFor = rf.me
			rf.votesGranted = 1
			rf.lastReceivedCommunication = time.Now()
			rf.persist()
			lastLogIndex := rf.logLength()
			requestVoteArgs := &RequestVoteArgs{Term: rf.CurrentTerm, CandidateId: rf.me, LastLogIndex: lastLogIndex, LastLogTerm: rf.Logs[lastLogIndex].Term}
			for i:=0; i < len(rf.peers); i++{
				if i != rf.me {
					go rf.sendRequestVote(i, requestVoteArgs)				
				}
			}
		}	
		rf.mu.Unlock()
		time.Sleep(electionTimeoutDuration)
	}
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
	rf.VotedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.Logs = make(map[int]Log)
	rf.nextIndexPerPeer = make([]int, len(rf.peers))
	rf.matchIndexPerPeer = make([]int, len(rf.peers))
	rf.applyCh = applyCh
	
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	
	for i:= 0; i < len(rf.peers); i++ {
		rf.nextIndexPerPeer[i] = rf.logLength() + 1
		rf.matchIndexPerPeer[i] = 0
	}

	go rf.elections()
	go rf.startAppendEntries()
	go rf.commitLogs()
	go rf.applyLogs()

	return rf
}

func raftSeed() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := crand.Int(crand.Reader, max)
	x := bigx.Int64()
	return x
}

func (rf *Raft) logLength() int {
	return rf.baseLogIndex + len(rf.Logs)
}

func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}