package raftkv

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Id        int64
	Key       string
	RequestId int
	Kind      string
	Value     string
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	result map[int]chan Op
	db     map[string]string
	ack    map[int64]int
}

func (kv *RaftKV) AppendEntryToLog(entry Op) bool {
	index, _, isleader := kv.rf.Start(entry)
	if !isleader {
		return false
	}
	kv.mu.Lock()
	ch, ok := kv.result[index]
	if !ok {
		ch = make(chan Op, 1)
		kv.result[index] = ch
	}
	kv.mu.Unlock()
	select {
	case op := <-ch: //leader commit后会把相应的log放在管道里
		return op == entry
	case <-time.After(1 * time.Second):
		fmt.Printf("[RaftKV] AppendEntryToLog timeout, %d is old leader\n",kv.me)
		return false
	}
}
func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	ok := kv.AppendEntryToLog(Op{Kind: "Get", Id: args.Id, RequestId: args.RequestId, Key: args.Key})
	if !ok {
		reply.WrongLeader = true
	} else {
		reply.WrongLeader = false
		reply.Err = OK
		kv.mu.Lock()
		reply.Value = kv.db[args.Key]
		kv.ack[args.Id] = args.RequestId
		kv.mu.Unlock()
		fmt.Printf("[RaftKV]Leader server:%d, get,Op(Id,Key,RequestId,Kind,Value):%v\n", kv.me, Op{Kind: "Get", Id: args.Id, RequestId: args.RequestId, Key: args.Key})
	}
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) { //put,append操作都是增加了一条rf的日志
	// Your code here.
	ok := kv.AppendEntryToLog(Op{Kind: args.Op, Id: args.Id, RequestId: args.RequestId, Key: args.Key, Value: args.Value})
	if ok {
		fmt.Printf("[RaftKV]server leader:%v %v ,key:%v,value:%v\n",kv.me,args.Op,args.Key,args.Value)
		reply.WrongLeader = false
		reply.Err = OK
	} else {
		reply.WrongLeader = true
	}
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.db = make(map[string]string)
	kv.result = make(map[int]chan Op)
	kv.ack = make(map[int64]int)
	kv.applyCh = make(chan raft.ApplyMsg, 100)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go func() {
		for {
			msg := <-kv.applyCh //当leader commit将会把entry放进管道
			if msg.UseSnapshot {
				var lastIncludeIndex int
				var lastIncludeTerm int
				d := gob.NewDecoder(bytes.NewBuffer(msg.Snapshot))
				kv.mu.Lock()
				d.Decode(&lastIncludeIndex)
				d.Decode(&lastIncludeTerm)
				kv.db = make(map[string]string) //数据都存在db里，kv是指这个map结构
				kv.ack = make(map[int64]int)
				fmt.Printf("kv:%v,db changed\n",kv.me)
				d.Decode(&kv.db)
				d.Decode(&kv.ack)
				kv.mu.Unlock()
			} else {
				op := msg.Command.(Op)
				kv.mu.Lock()
				checkDuplicate := false
				ack, ok := kv.ack[op.Id] //通过对每个client作为key,value作为该次请求请求号，维护一个map来保证对每个client的请求不会重复返回
				if ok && ack >= op.RequestId {
					checkDuplicate = true
				}
				if !checkDuplicate {
					switch op.Kind {
					case "Put":
						kv.db[op.Key] = op.Value
					case "Append":
						kv.db[op.Key] += op.Value
					}
					kv.ack[op.Id] = op.RequestId
				}
				ch, ok := kv.result[msg.Index] //防止阻塞先清空一下
				if ok {
					select {
					case <-kv.result[msg.Index]:
					default: //防止select阻塞

					}
					ch <- op
				} else {
					kv.result[msg.Index] = make(chan Op, 1)
				}
				if maxraftstate != -1 && kv.rf.GetPersistSize() > maxraftstate {
					w := new(bytes.Buffer)
					e := gob.NewEncoder(w)
					e.Encode(kv.db)
					e.Encode(kv.ack)
					data := w.Bytes()
					go kv.rf.StartSnapShot(data, msg.Index)
				}
				kv.mu.Unlock()
			}
		}
	}()
	return kv
}
