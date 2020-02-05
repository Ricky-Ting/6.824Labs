package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"fmt"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func DPrintln(a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Println(a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Cid 		int64 	// Clerk's Id
	RequestId 	int 	// Request's Id
	OpType 		string
	Key 		string
	Value 		string
}

type KVServerState struct {
	Database 		   map[string]string
	LastRequestID 	   map[int64]int
	LastResponse 	   map[int64]string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	persister *raft.Persister
	database 	map[string]string
	applyCond 	*sync.Cond
	lastRequestID map[int64]int // Record latest request for different clerks
	lastResponse map[int64]string // Record latest response for different clerks
	waitRequest map[int]int  // map[index] = 0, 1 : whether a goroutine wait for index
	Request 	map[int]Op 	 // map[index] = Op
	shutdown 	bool

}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	//DPrintf("In server Receive %s \n", args.Key)
	kv.mu.Lock()
	
	if args.RequestId <= kv.lastRequestID[args.Cid] {
		reply.WrongLeader = false
		reply.Value = kv.lastResponse[args.Cid]
		DPrintln("In server Get complete clerk = ", args.Cid," RequestId = ", args.RequestId)
		kv.mu.Unlock()
		return 
	}

	cmd := Op{args.Cid, args.RequestId, "Get", args.Key, ""}
	index, term, isLeader := kv.rf.Start(cmd)
	if !isLeader {
		reply.WrongLeader = true
		kv.mu.Unlock()
		return
	}
	reply.WrongLeader = false
	kv.waitRequest[index] = 1
	DPrintf("In server %d Get %s index = %d \n", kv.me, args.Key, index)
	kv.mu.Unlock()


	kv.applyCond.L.Lock()
	kv.mu.Lock()
	for _, ok := kv.Request[index]; !ok; _, ok = kv.Request[index]  {
		kv.mu.Unlock()
		kv.applyCond.Wait()
		kv.mu.Lock()

		if kv.shutdown {
			delete(kv.waitRequest, index)
			kv.applyCond.Broadcast()
			reply.WrongLeader = true
			kv.mu.Unlock()
			kv.applyCond.L.Unlock()
			return 
		}

		curTerm, curIsLeader := kv.rf.GetState()
		if curTerm != term || !curIsLeader {
			delete(kv.waitRequest, index)
			//kv.waitRequest[index] = 0
			reply.WrongLeader = true
			kv.applyCond.Broadcast()
			DPrintf("In server Get %d not leader any more index %d failed", kv.me, index)
			kv.mu.Unlock()
			kv.applyCond.L.Unlock()
			return 
		}

	}
	if kv.Request[index] != cmd {
		delete(kv.waitRequest, index)
		//kv.waitRequest[index] = 0
		reply.WrongLeader = true
		DPrintf("In server Get index %d not origin", index)
		kv.applyCond.Broadcast()
		kv.mu.Unlock()
		kv.applyCond.L.Unlock()
		return
	}

	delete(kv.waitRequest, index)
	reply.Value = kv.lastResponse[args.Cid]
	DPrintln("In server Get complete ", index)
	kv.applyCond.Broadcast()
	kv.mu.Unlock()
	kv.applyCond.L.Unlock()

	return


}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	//DPrintln("In server receive ", args)
	kv.mu.Lock()
	if args.RequestId <= kv.lastRequestID[args.Cid] {
		reply.WrongLeader = false
		//reply.Value = lastResponse[args.Cid]
		DPrintln("In server PutAppend complete clerk = ", args.Cid," RequestId = ", args.RequestId)
		kv.mu.Unlock()
		return 
	}

	cmd := Op{args.Cid, args.RequestId, args.Op, args.Key, args.Value}
	index, term, isLeader := kv.rf.Start(cmd)
	if !isLeader {
		reply.WrongLeader = true
		kv.mu.Unlock()
		return
	}
	reply.WrongLeader = false
	kv.waitRequest[index] = 1
	kv.mu.Unlock()

	DPrintln("In server ", kv.me, " ", args, " index = ", index)

	kv.applyCond.L.Lock()
	kv.mu.Lock()
	for _, ok := kv.Request[index]; !ok; _, ok = kv.Request[index]  {
		//DPrintln("In server Put ", v, " ", ok)
		kv.mu.Unlock()
		kv.applyCond.Wait()
		kv.mu.Lock()

		if kv.shutdown {
			delete(kv.waitRequest, index)
			kv.applyCond.Broadcast()
			reply.WrongLeader = true
			kv.mu.Unlock()
			kv.applyCond.L.Unlock()
			return 
		}

		//DPrintln("In server PutAppend", kv.Request[index] )
		curTerm, curIsLeader := kv.rf.GetState()
		if curTerm != term || !curIsLeader {
			delete(kv.waitRequest, index)
			kv.applyCond.Broadcast()
			reply.WrongLeader = true
			DPrintf("In server PutAppend %d not leader any more index %d failed", kv.me, index)
			kv.mu.Unlock()
			kv.applyCond.L.Unlock()
			return 
		}

	}

	
	if kv.Request[index] != cmd {
		delete(kv.waitRequest, index)
		kv.applyCond.Broadcast()
		reply.WrongLeader = true
		DPrintf("In server PutAppend index %d not origin", index)
		kv.mu.Unlock()
		kv.applyCond.L.Unlock()
		return
	}
	delete(kv.waitRequest, index)

	DPrintln("In server PutAppend complete ", index)
	kv.applyCond.Broadcast()
	kv.mu.Unlock()
	kv.applyCond.L.Unlock()

	return
}


func (kv *KVServer) Apply() {
	for {
		msg, ok := <- kv.applyCh
		if !ok {
			return
		}
		DPrintln(" Server ", kv.me, " Apply receive ", msg)
		
		if !msg.CommandValid {
			kv.applyCond.L.Lock()
			kv.mu.Lock()
			
			sp, ok := msg.Command.(raft.Snapshot)
			if !ok {
				fmt.Println("In Apply: type error")
			}
			state, ok2 := sp.ApplicationState.(KVServerState)
			if !ok2 {
				fmt.Println("In Apply: type error")
			}

			kv.database = state.Database
			kv.lastRequestID = state.LastRequestID
			kv.lastResponse = state.LastResponse
			
			kv.mu.Unlock()
			kv.applyCond.L.Unlock()
			continue
		}
		

		kv.applyCond.L.Lock()
		kv.mu.Lock()

		index := msg.CommandIndex
		term := msg.CommandTerm
		op, ok := msg.Command.(Op)
		if !ok {
			fmt.Println("In Apply: type error")
		}
		kv.Request[index] = op 
		if op.RequestId > kv.lastRequestID[op.Cid] {
			kv.lastRequestID[op.Cid] = op.RequestId
			
			DPrintln(" Server ", kv.me, " Apply  ", msg)
			if op.OpType == "Append" {
				kv.database[op.Key] = kv.database[op.Key] + op.Value
			} else if op.OpType == "Put" {
				
				kv.database[op.Key] = op.Value
			} else {
				kv.lastResponse[op.Cid] = kv.database[op.Key]
			}
		}

		kv.applyCond.Broadcast()

		//DPrintln(kv.Request[index])

		for kv.waitRequest[index] == 1 {
			kv.mu.Unlock()
			kv.applyCond.Wait()
			kv.mu.Lock()
		}

		delete(kv.Request, index)

		if kv.maxraftstate != -1 {
			kv.saveSnapshot(index, term)
		}

		kv.mu.Unlock()
		kv.applyCond.L.Unlock()
	}
}



func (kv *KVServer) saveSnapshot(index, term int) {

	if kv.maxraftstate > kv.persister.RaftStateSize() {
		return
	}

	DPrintf("server %d, maxraftstate is %d, raftstatesize is %d \n", kv.me, kv.maxraftstate, kv.persister.RaftStateSize())
	sp := raft.Snapshot{index, term, KVServerState{kv.database, kv.lastRequestID, kv.lastResponse}}
	DPrintf("server %d call raft.Snapshot \n", kv.me)
	kv.rf.SaveSnapshot(sp)
	DPrintf("server %d call raft.Snapshot return \n", kv.me)
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	kv.shutdown = true
	DPrintf("server %d shutdown \n", kv.me)
	//close(kv.applyCh)
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(KVServerState{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	//kv.maxraftstate = 1
	kv.persister = persister

	// You may need initialization code here.
	kv.database = make(map[string]string)
	kv.applyCond = sync.NewCond(new(sync.Mutex))
	kv.lastRequestID = make(map[int64]int)
	kv.lastResponse = make(map[int64]string)
	kv.waitRequest = make(map[int]int)
	kv.Request = make(map[int]Op)
	

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.shutdown = false
	
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)


	// You may need initialization code here.
	

	go kv.Apply()

	go func() {
		for {
			time.Sleep(500 * time.Millisecond)
			kv.mu.Lock()
			if kv.shutdown {
				return
			}
			kv.mu.Unlock()
			kv.applyCond.Broadcast()
		}
	}()

	return kv
}
