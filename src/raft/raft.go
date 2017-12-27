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
	applyChan     chan ApplyMsg
	grantVoteChan chan bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	return rf.currentTerm, rf.state == STATE_LEADER
}
func (rf *Raft) getLastTerm() int {
	return rf.log[len(rf.log)-1].LogTerm
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

func (rf *Raft) readSnapshort(data []byte) { //byte字符类型
	rf.readPersist(rf.persister.ReadRaftState())
	if len(data) == 0 {
		return
	}
	d := gob.NewDecoder(bytes.NewBuffer(data))
	var lastIncludeTerm int
	var lastIncludeIndex int
	d.Decode(&lastIncludeIndex)
	d.Decode(&lastIncludeTerm)
	rf.commitIndex = lastIncludeIndex
	rf.lastApplied = lastIncludeIndex
	rf.log = truncateLog(lastIncludeIndex, lastIncludeTerm, rf.log)
	go func() {
		rf.applyChan <- ApplyMsg{UseSnapshot: true, Snapshot: data}
	}()
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.voteFor)
	d.Decode(&rf.log)
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
		fmt.Printf("rf %d current term %d < args.term %d,candidate term:%d, update current term\n", rf.me, rf.currentTerm, args.Term, args.CandidateID)
		rf.state = STATE_FOLLOWER
		rf.voteFor = -1
		rf.currentTerm = args.Term
	}
	reply.Term = rf.currentTerm
	lastTerm := rf.getLastTerm()
	fmt.Printf("args.LastLogTerm:%d,rf.log[len(rf.log)-1].LogTerm:%d,args.LastLogIndex:%d,rf.log[len(rf.log)-1].LogIndex:%d\n", args.LastLogTerm, rf.log[len(rf.log)-1].LogTerm, args.LastLogIndex, rf.log[len(rf.log)-1].LogIndex)
	if args.LastLogTerm > lastTerm || (args.LastLogTerm == lastTerm && args.LastLogIndex >= rf.log[len(rf.log)-1].LogIndex) { //通过日志的对比防止刚刚重连的raft获得选票
		if rf.voteFor == -1 || rf.voteFor == args.CandidateID {
			fmt.Printf("rf %v currentTerm:%v, vote for: candidate%v args term:%v\n", rf.me, rf.currentTerm, args.CandidateID, args.Term)
			rf.grantVoteChan <- true
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
		if rf.state != STATE_CANDIDATE || args.Term != rf.currentTerm {
			return ok
		}
		if reply.Term > rf.currentTerm { //candidate term>当前rf.term,投票被拒绝
			rf.state = STATE_FOLLOWER
			rf.voteFor = -1
			rf.currentTerm = reply.Term
			rf.persist() //存储voteFor,currentTerm,log,所以要放在最后
		}
		if reply.VoteGranted {
			rf.voteCount++
			fmt.Printf("%d's voteCount is：%d\n", rf.me, rf.voteCount)
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
func (rf *Raft) Start(command interface{}) (int, int, bool) { //从客户端接收command
	index := -1

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isLeader := rf.state == STATE_LEADER
	if isLeader {
		index = rf.getLastIndex() + 1
		rf.log = append(rf.log, LogEntry{LogTerm: term, LogContent: command, LogIndex: index}) //%v输出时是按照定义结构体时的顺序输出
		fmt.Printf("rf.me: %d, Get new command(term,command,index): %v, rf.log changed\n", rf.me, LogEntry{LogTerm: term, LogContent: command, LogIndex: index})
		rf.persist()
	}
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
	args.LastLogTerm = rf.getLastTerm()
	args.LastLogIndex = rf.getLastIndex()
	rf.mu.Unlock()
	for i := range rf.peers {
		if i != rf.me && rf.state == STATE_CANDIDATE {
			go func(i int) {
				var reply RequestVoteReply
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
		reply.Term = rf.currentTerm
		reply.NextLogIndex = lastIndex + 1
		fmt.Printf("rf %v currentTerm: %v rejected append entries leader:%v, leaderTerm:%v\n", rf.me, rf.currentTerm, args.LeaderId, args.Term)
		return
	}
	rf.heartBeatChan <- true
	fmt.Printf("rf %d get heartbeat from leader %v\n", rf.me, args.LeaderId)
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = STATE_FOLLOWER
		rf.voteFor = -1
	}
	reply.Term = args.Term
	if args.LastLogIndex > lastIndex {
		reply.NextLogIndex = lastIndex + 1
		return
	}
	baseIndex := rf.log[0].LogIndex
	if baseIndex <= args.LastLogIndex {
		if baseIndex < args.LastLogIndex {
			term := rf.log[args.LastLogIndex-baseIndex].LogTerm
			if args.LastLogTerm != term {
				for i := args.LastLogIndex - 1; i >= baseIndex; i-- {
					if rf.log[i-baseIndex].LogTerm != term {
						reply.NextLogIndex = i + 1
						break
					}
				}
				return
			}
		}
		rf.log = rf.log[:args.LastLogIndex-baseIndex+1]
		rf.log = append(rf.log, args.Entries...) //args是个数组，所以要加上...
		reply.Success = true
		reply.NextLogIndex = rf.getLastIndex() + 1 //rf.log被修改了，要重新取lastIndex
	}
	fmt.Printf("args.LeaderCommitIndex:%d, rf.commitIndex:%d\n", args.LeaderCommitIndex, rf.commitIndex)
	if args.LeaderCommitIndex > rf.commitIndex {
		if rf.getLastIndex() < args.LeaderCommitIndex {
			rf.commitIndex = rf.getLastIndex()
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
		if rf.state != STATE_LEADER {
			fmt.Printf("%v send to %d failed, because rf's leader state changed\n", args.Entries, severIndex)
			return ok
		}
		if term != args.Term { //在appendEntries中(leader)args.Term>(candidate)term则term=args.Term,这里只会是(leader)args.Term<(candidate)term
			fmt.Printf("%v send to %d,fail in 2\n", args.Entries, severIndex)
			return ok
		}
		if reply.Term > rf.currentTerm { //(leader)args.Term<(candidate)term
			fmt.Printf("%v send to %d failed, because reply.term>rf.current.term, %d will change into follower from leader\n", args.Entries, severIndex, rf.me)
			rf.currentTerm = reply.Term
			rf.state = STATE_FOLLOWER
			rf.voteFor = -1
			rf.persist()
			return ok
		}
		if reply.Success {
			if len(args.Entries) > 0 {
				rf.nextIndex[severIndex] = args.Entries[len(args.Entries)-1].LogIndex + 1
				rf.matchIndex[severIndex] = rf.nextIndex[severIndex] - 1
			}
		} else {
			rf.nextIndex[severIndex] = reply.NextLogIndex
		}
	}
	return ok
}

type snapShortArgs struct {
	Term      int
	LeaderId  int
	LastIndex int
	LastTerm  int
	Data      []byte
}
type snapShortReply struct {
	Term int
}

func truncateLog(lastIndex, lastTerm int, log []LogEntry) []LogEntry {
	var newLogEntry []LogEntry
	newLogEntry = append(newLogEntry, LogEntry{LogTerm: lastTerm, LogIndex: lastIndex})
	for index := len(log) - 1; index >= 0; index-- {
		if lastIndex == log[index].LogIndex && lastTerm == log[index].LogTerm {
			newLogEntry = append(newLogEntry, log[index+1:]...) //使用log[index+1:]时要配合使用...
			break
		}
	}
	return newLogEntry
}
func (rf *Raft) installSnapShort(args snapShortArgs, reply *snapShortReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	rf.heartBeatChan <- true
	rf.state = STATE_FOLLOWER
	rf.persister.SaveRaftState(args.Data)
	rf.log = truncateLog(args.LastIndex, args.LastTerm, rf.log)
	rf.lastApplied = args.LastIndex
	rf.commitIndex = args.LastIndex
	rf.persist()
	rf.applyChan <- ApplyMsg{UseSnapshot: true, Snapshot: args.Data}
}
func (rf *Raft) sendInstallSnapShort(severIndex int, args snapShortArgs, reply *snapShortReply) bool {
	ok := rf.peers[severIndex].Call("Raft.installSnapShort", args, reply)
	if ok {
		if reply.Term > rf.currentTerm {
			rf.state = STATE_FOLLOWER
			rf.voteFor = -1
			rf.currentTerm = reply.Term
			return ok
		}
		rf.nextIndex[severIndex] = args.LastIndex + 1
		rf.matchIndex[severIndex] = args.LastIndex
	}
	return ok
}
func (rf *Raft) broadcastAppendEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	newCommitIndex := rf.commitIndex
	lastCommitIndex := rf.getLastIndex()
	baseIndex := rf.log[0].LogIndex
	for i := rf.commitIndex + 1; i <= lastCommitIndex; i++ { //每次caseAppednEntries先同步一下commitndex
		count := 1
		for j := range rf.peers {
			if j != rf.me && rf.matchIndex[j] >= i && rf.log[i-baseIndex].LogTerm == rf.currentTerm {
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
			if rf.nextIndex[i] > baseIndex {
				var args appendEntriesArgs
				args.LastLogIndex = rf.nextIndex[i] - 1
				args.LeaderId = rf.me
				args.Term = rf.currentTerm
				args.LastLogTerm = rf.log[args.LastLogIndex-baseIndex].LogTerm
				fmt.Printf("[broadcast append entries]baseIndex:%d PrevLogIndex:%d, severIndex: %d,current leader: %d\n", rf.log[0].LogIndex, args.LastLogIndex, i, rf.me)
				args.Entries = make([]LogEntry, len(rf.log[args.LastLogIndex+1-baseIndex:])) //会把leader的新日志存在里面
				copy(args.Entries, rf.log[args.LastLogIndex+1-baseIndex:])
				args.LeaderCommitIndex = rf.commitIndex
				go func(severIndex int, args appendEntriesArgs) {
					var reply appendEntriesReply
					rf.sendAppendEntries(severIndex, &args, &reply)
				}(i, args)

			} else {
				var args snapShortArgs
				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				args.LastIndex = rf.log[0].LogIndex
				args.LastTerm = rf.log[0].LogTerm
				args.Data = rf.persister.snapshot
				go func(args snapShortArgs, severIndex int) {
					reply := &snapShortReply{}
					rf.sendInstallSnapShort(severIndex, args, reply)
				}(args, i)
			}

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
	rf.commitChan = make(chan bool, 100) //设置一定的缓存空间，防止彼此阻塞
	rf.heartBeatChan = make(chan bool, 100)
	rf.leaderChan = make(chan bool, 100)
	rf.grantVoteChan = make(chan bool, 100)
	rf.applyChan = applyCh
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.readSnapshort(persister.ReadSnapshot())
	go func() {
		for {
			switch rf.state {
			case STATE_FOLLOWER:
				select {
				case <-rf.heartBeatChan:
				case <-rf.grantVoteChan: //每次投票后都会重新计时
				case <-time.After(time.Duration(rand.Int63()%333+550) * time.Millisecond): //最开始从follower切换到candidate状态的等待时间也是随机的
					rf.state = STATE_CANDIDATE
				}
			case STATE_CANDIDATE:
				rf.mu.Lock()
				rf.currentTerm++
				rf.voteFor = rf.me
				rf.voteCount = 1
				rf.persist()
				rf.mu.Unlock()
				go rf.broadcastVoteRequest()
				select {
				case <-time.After(time.Duration(rand.Int63()%333+550) * time.Millisecond):
				case <-rf.heartBeatChan: //心跳并不会影响纪元
					fmt.Printf("Candidate %d receive heartBeatChan, state change into follower\n", rf.me)
					rf.state = STATE_FOLLOWER
				case <-rf.leaderChan:
					rf.mu.Lock()
					rf.state = STATE_LEADER
					fmt.Printf("time:%v,%d change into leader\n", time.Now().Format("15:04:05"), rf.me)
					rf.nextIndex = make([]int, len(rf.peers))
					rf.matchIndex = make([]int, len(rf.peers))
					for i := range rf.peers {
						rf.nextIndex[i] = rf.getLastIndex() + 1 //nextIndex是每个peer下一条要存储的日志的索引
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
					applyCh <- ApplyMsg{Index: i, Command: rf.log[i-rf.log[0].LogIndex].LogContent} //剩下的属性是默认值,applyCh是在start1()中取出，然后更改日志的
					fmt.Printf("[commitChan] me: %d msg: %v\n", rf.me, ApplyMsg{Index: i, Command: rf.log[i].LogContent})
					rf.lastApplied = i
				}
				rf.mu.Unlock()
			}
		}
	}()
	return rf
}
