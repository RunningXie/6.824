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

import "sync"
import (
	"bytes"
	"encoding/gob"
	"fmt"
	"labrpc"
	"math/rand"
	"time"
)

// import "bytes"
// import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state         int
	voteFor       int
	log           []LogEntry
	currentTerm   int
	commitChan    chan bool
	heartBeatChan chan bool
	leaderChan    chan bool
	voteCount     int
	nextIndex     []int
	matchIndex    []int
	commitIndex   int
	lastApplied   int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	return rf.currentTerm, rf.state == STATE_LEADER
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
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogTerm  int
	LastLogIndex int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	VoteGranted bool
	Term        int
	VoteFor     int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		rf.state = STATE_FOLLOWER
		rf.voteFor = -1
		rf.currentTerm = args.Term
	}
	reply.Term = rf.currentTerm
	if args.LastLogTerm > rf.log[len(rf.log)-1].LogTerm || (args.LastLogTerm == rf.log[len(rf.log)-1].LogTerm && args.LastLogIndex >= rf.log[len(rf.log)-1].LogIndex) {
		if rf.voteFor == -1 || rf.voteFor == args.CandidateID {
			fmt.Printf("Vote %d -> %d granted\n", rf.me, args.CandidateID)
			rf.state = STATE_FOLLOWER
			reply.VoteFor = args.CandidateID
			reply.VoteGranted = true
		}
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		fmt.Printf("Reply: %v\n", reply)
		if rf.state != STATE_CANDIDATE || args.Term != rf.currentTerm {
			return ok
		}
		if reply.Term > rf.currentTerm { //candidate term>当前rf.term,投票被拒绝
			rf.state = STATE_FOLLOWER
			rf.voteFor = -1
			rf.persist()
			rf.currentTerm = reply.Term
		}
		if reply.VoteGranted {
			rf.voteCount++
			fmt.Printf("%d's voteCount is：%d, state is %v\n", rf.me, rf.voteCount, rf.state)
			if rf.state == STATE_CANDIDATE && rf.voteCount*2 > len(rf.peers) {
				rf.state = STATE_FOLLOWER
				rf.leaderChan <- true
			}
		}
	}
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
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
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
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
const (
	STATE_LEADER = iota
	STATE_CANDIDATE
	STATE_FOLLOWER
	HEARTBEAT_INTERVAL = 50 * time.Millisecond
)

type LogEntry struct {
	LogTerm    int
	LogContent interface{}
	LogIndex   int
}

func (rf *Raft) broadcastVoteRequest() {
	var args RequestVoteArgs
	rf.mu.Lock()
	args.Term = rf.currentTerm
	args.CandidateID = rf.me
	args.LastLogTerm = rf.log[len(rf.log)-1].LogTerm
	args.LastLogIndex = rf.getLastIndex()
	rf.mu.Unlock()
	for i := range rf.peers {
		if i != rf.me && rf.state == STATE_CANDIDATE {
			go func(i int) {
				var reply RequestVoteReply
				fmt.Printf("%d request vote from %d\n", rf.me, i)
				rf.sendRequestVote(i, &args, &reply)
			}(i)
		}
	}
}

type appendEntriesArgs struct {
	Term              int
	LeaderId          int
	LastLogIndex      int
	LastLogTerm       int
	Entries           []LogEntry
	LeaderCommitIndex int
}
type appendEntriesReply struct {
	Success      bool
	Term         int
	NextLogIndex int
}

func (rf *Raft) getLastIndex() int {
	return rf.log[len(rf.log)-1].LogIndex
}
func (rf *Raft) AppendEntries(args *appendEntriesArgs, reply *appendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	reply.Success = false
	lastIndex := rf.getLastIndex()
	if rf.currentTerm > args.Term {
		fmt.Printf("%d 's term (%d) larger than request term (%d)\n", rf.me, rf.currentTerm, args.Term)
		reply.Term = rf.currentTerm
		reply.NextLogIndex = rf.getLastIndex() + 1
		return
	}
	rf.heartBeatChan <- true
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = STATE_FOLLOWER
		rf.voteFor = -1
	}
	reply.Term = args.Term
	if args.LastLogIndex > rf.getLastIndex() {
		reply.NextLogIndex = lastIndex + 1
		return
	}
	term := rf.log[args.LastLogIndex].LogTerm
	if args.LastLogTerm != term {
		for i := args.LastLogIndex - 1; i >= 0; i-- {
			if rf.log[i].LogTerm != term {
				reply.NextLogIndex = i + 1
				break
			}
		}
		return
	}
	rf.log = rf.log[:args.LastLogIndex+1]
	rf.log = append(rf.log, args.Entries...)
	reply.Success = true
	reply.NextLogIndex = lastIndex + 1
	if args.LeaderCommitIndex > rf.commitIndex {
		if lastIndex < args.LeaderCommitIndex {
			rf.commitIndex = lastIndex
		} else {
			rf.commitIndex = args.LeaderCommitIndex
		}
		rf.commitChan <- true
	}
	return
}
func (rf *Raft) sendAppendEntries(severIndex int, args *appendEntriesArgs, reply *appendEntriesReply) bool {
	ok := rf.peers[severIndex].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		term := rf.currentTerm
		if rf.state == STATE_LEADER {
			return ok
		}
		if term != args.Term { //在appendEntries中(leader)args.Term>(candidate)term则term=args.Term,这里只会是(leader)args.Term<(candidate)term
			return ok
		}
		if reply.Term > rf.currentTerm { //(leader)args.Term<(candidate)term
			rf.currentTerm = reply.Term
			rf.state = STATE_FOLLOWER
			fmt.Printf("%d change into follower from leader\n", rf.me)
			rf.voteFor = -1
			rf.persist()
			return ok
		}
		if reply.Success {
			if len(args.Entries) > 0 {
				rf.nextIndex[severIndex] += len(args.Entries)
				rf.matchIndex[severIndex] = rf.nextIndex[severIndex] - 1
				fmt.Printf("%d 's next index is %d", rf.me, rf.nextIndex[severIndex])
			} else {
				rf.nextIndex[severIndex] = reply.NextLogIndex
			}
		}
	}
	return ok
}
func (rf *Raft) broadcastAppendEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	newCommitIndex := rf.commitIndex
	lastCommitIndex := rf.getLastIndex()
	for i := rf.commitIndex + 1; i <= lastCommitIndex; i++ {
		count := 1
		for j := range rf.peers {
			if j != rf.me && rf.matchIndex[j] >= i && rf.log[i].LogTerm == rf.currentTerm {
				count++
			}
		}
		if 2*count > len(rf.peers) {
			newCommitIndex = i
		}

	}
	if newCommitIndex != rf.commitIndex {
		rf.commitIndex = newCommitIndex
		rf.commitChan <- true
	}
	for i := range rf.peers {
		if i != rf.me && rf.state == STATE_LEADER {
			var args appendEntriesArgs
			args.LastLogIndex = rf.nextIndex[i] - 1
			args.LeaderId = rf.me
			args.Term = rf.currentTerm
			args.LastLogTerm = rf.log[args.LastLogIndex].LogTerm
			args.Entries = make([]LogEntry, len(rf.log[args.LastLogIndex+1:]))
			copy(args.Entries, rf.log[args.LastLogIndex+1:])
			args.LeaderCommitIndex = rf.commitIndex
			go func(severIndex int, args appendEntriesArgs) {
				var reply appendEntriesReply
				rf.sendAppendEntries(severIndex, &args, &reply)
			}(i, args)

		}
	}
}
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = STATE_FOLLOWER
	rf.voteFor = -1
	rf.log = append(rf.log, LogEntry{LogTerm: 0})
	rf.currentTerm = 0
	rf.commitChan = make(chan bool)
	rf.heartBeatChan = make(chan bool)
	rf.leaderChan = make(chan bool)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go func() {
		for {
			switch rf.state{
			case STATE_FOLLOWER:
				select {
				case <-rf.heartBeatChan:
				case <-time.After(time.Duration(rand.Int63()%330+550) * time.Millisecond):
					rf.state = STATE_CANDIDATE
				}
			case STATE_CANDIDATE:
				rf.mu.Lock()
				rf.currentTerm++
				rf.voteFor = rf.me
				rf.voteCount = 1
				rf.persist()
				rf.mu.Unlock()
				fmt.Printf("%d change into candidate int term %d\n", rf.me, rf.currentTerm)
				go rf.broadcastVoteRequest()
				select {
				case <-time.After(time.Duration(rand.Int63()%330+550) * time.Millisecond):
				case <-rf.heartBeatChan:
					fmt.Printf("Candidate %d receive heartBeatChan, state change into follower\n", rf.me)
					rf.state = STATE_FOLLOWER
				case <-rf.leaderChan:
					rf.mu.Lock()
					rf.state = STATE_LEADER
					fmt.Printf("%d change into leader\n", rf.me)
					rf.nextIndex = make([]int, len(rf.peers))
					rf.matchIndex = make([]int, len(rf.peers))
					for i := range rf.peers {
						rf.nextIndex[i] = rf.getLastIndex() + 1
						rf.matchIndex[i] = 0
					}
					rf.mu.Unlock()
				}
			case STATE_LEADER:
				rf.broadcastAppendEntries()
				time.Sleep(HEARTBEAT_INTERVAL)
			}
		}
	}()
	go func() {
		for {
			select {
			case <-rf.commitChan:
				rf.mu.Lock()
				for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
					applyCh <- ApplyMsg{Index: i, Command: rf.log[i].LogContent}
					rf.lastApplied = i
				}
				rf.mu.Unlock()
			}
		}
	}()
	return rf
}
