package raftkv

import "labrpc"
import "crypto/rand"
import (
	"fmt"
	"math/big"
	"sync"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	id        int64
	requestId int //相当于int32
	mu        sync.RWMutex
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.id = nrand()
	ck.requestId = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	var args GetArgs
	args.Id = ck.id
	args.Key = key
	ck.mu.Lock()
	args.RequestId = ck.requestId
	ck.requestId++
	ck.mu.Unlock()
	for {
		for _, sever := range ck.servers {
			var reply GetReply
			ok := sever.Call("RaftKV.Get", &args, &reply)
			if ok && reply.WrongLeader == false {
				fmt.Printf("[RaftKV]Get key:%v,value:%v\n", args.Key, reply.Value)
				return reply.Value
			}
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	var args PutAppendArgs
	args.Key = key
	args.Value = value
	args.Op = op
	args.Id = ck.id
	ck.mu.Lock()
	args.RequestId = ck.requestId
	ck.requestId++
	ck.mu.Unlock()
	for { //不成功提交不会停
		for _, server := range ck.servers {
			var reply PutAppendReply
			ok := server.Call("RaftKV.PutAppend", &args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
