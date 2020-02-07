package shardkv


import "shardmaster"
import "labrpc"
import "raft"
import "sync"
import "labgob"
import "fmt"
import "log"
import "time"

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
	OpType 		string  // Get, Put, Append or Config
	Key 		string
	Value 		string
	Cfg 		shardmaster.Config		
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	persister *raft.Persister
	database 		[shardmaster.NShards]map[string]string
	applyCond 		*sync.Cond
	lastRequestID 	[shardmaster.NShards]map[int64]int // Record latest request for different clerks
	lastResponse 	[shardmaster.NShards]map[int64]string // Record latest response for different clerks
	waitRequest 	map[int]int  // map[index] = 0, 1 : whether a goroutine wait for index
	Request 		map[int]Op 	 // map[index] = Op
	isWrongGroup 	map[int]bool // map[index] = isWrongGroup
	shutdown 		bool
	mck 			*shardmaster.Clerk
	cfg 			shardmaster.Config
}


func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	shard := key2shard(args.Key)
	if kv.cfg.Shards[shard] != kv.gid {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	DPrintf("Key %s map to %d \n", args.Key, shard)

	if args.RequestId <= kv.lastRequestID[shard][args.Cid] {
		reply.WrongLeader = false
		reply.Err = OK
		reply.Value = kv.lastResponse[shard][args.Cid]
		DPrintln("Gid = ", kv.gid, " In server Get complete clerk = ", args.Cid," RequestId = ", args.RequestId)
		kv.mu.Unlock()
		return 
	}

	cmd := Op{args.Cid, args.RequestId, "Get", args.Key, "", shardmaster.Config{}}
	index, term, isLeader := kv.rf.Start(cmd)
	if !isLeader {
		reply.WrongLeader = true
		kv.mu.Unlock()
		return
	}
	reply.WrongLeader = false
	kv.waitRequest[index] = 1
	DPrintln("Gid = ", kv.gid, " In server ", kv.me, " ", args, " index = ", index)
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
			DPrintf("Gid = %d In server Get %d not leader any more index %d failed", kv.gid, kv.me, index)
			kv.mu.Unlock()
			kv.applyCond.L.Unlock()
			return 
		}

	}
	if kv.Request[index].Cid != cmd.Cid || kv.Request[index].RequestId != cmd.RequestId {
		delete(kv.waitRequest, index)
		reply.WrongLeader = true
		DPrintf("Gid = %d In server Get index %d not origin", kv.gid, index)
		kv.applyCond.Broadcast()
		kv.mu.Unlock()
		kv.applyCond.L.Unlock()
		return
	}

	delete(kv.waitRequest, index)
	if !kv.isWrongGroup[index] {
		reply.Err = OK
		reply.Value = kv.lastResponse[shard][args.Cid]
		DPrintln("Gid = ", kv.gid, " In server Get complete index = ", index)
	} else {
		reply.Err = ErrWrongGroup
		delete(kv.isWrongGroup, index)
	}
	
	kv.applyCond.Broadcast()
	kv.mu.Unlock()
	kv.applyCond.L.Unlock()

	return
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	shard := key2shard(args.Key)
	if kv.cfg.Shards[shard] != kv.gid {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	DPrintf("Key %s map to %d \n", args.Key, shard)
	if args.RequestId <= kv.lastRequestID[shard][args.Cid] {
		reply.WrongLeader = false
		reply.Err = OK
		DPrintln("Gid = ", kv.gid, " In server PutAppend complete clerk = ", args.Cid," RequestId = ", args.RequestId)
		kv.mu.Unlock()
		return 
	}

	cmd := Op{args.Cid, args.RequestId, args.Op, args.Key, args.Value, shardmaster.Config{}}
	index, term, isLeader := kv.rf.Start(cmd)
	if !isLeader {
		reply.WrongLeader = true
		kv.mu.Unlock()
		return
	}
	reply.WrongLeader = false
	kv.waitRequest[index] = 1
	kv.mu.Unlock()

	DPrintln("Gid = ", kv.gid, " In server ", kv.me, " ", args, " index = ", index)

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
			DPrintf("Gid = %d In server PutAppend %d not leader any more index %d failed", kv.gid, kv.me, index)
			kv.mu.Unlock()
			kv.applyCond.L.Unlock()
			return 
		}

	}

	
	if kv.Request[index].Cid != cmd.Cid || kv.Request[index].RequestId != cmd.RequestId {
		delete(kv.waitRequest, index)
		kv.applyCond.Broadcast()
		reply.WrongLeader = true
		DPrintf("Gid = %d In server PutAppend index %d not origin", kv.gid, index)
		kv.mu.Unlock()
		kv.applyCond.L.Unlock()
		return
	}
	delete(kv.waitRequest, index)
	if !kv.isWrongGroup[index] {
		reply.Err = OK
		DPrintln("Gid = ", kv.gid, " In server PutAppend complete ", index)
	} else {
		reply.Err = ErrWrongGroup
		delete(kv.isWrongGroup, index)
	}
	kv.applyCond.Broadcast()
	kv.mu.Unlock()
	kv.applyCond.L.Unlock()

	return
}


func (kv *ShardKV) Apply() {
	for {
		msg, ok := <- kv.applyCh
		if !ok {
			return
		}
		DPrintln("Gid = ", kv.gid, " Server ", kv.me, " Apply receive ", msg)

		kv.applyCond.L.Lock()
		kv.mu.Lock()

		index := msg.CommandIndex
		//term := msg.CommandTerm
		op, ok := msg.Command.(Op)
		if !ok {
			fmt.Println("In Apply: type error")
		}

		if op.OpType == "Config" {
			if op.Cfg.Num > kv.cfg.Num {
				kv.cfg = op.Cfg
			}
			kv.mu.Unlock()
			kv.applyCond.L.Unlock()
			continue
		}

		kv.Request[index] = op 
		shard := key2shard(op.Key)
		if op.RequestId > kv.lastRequestID[shard][op.Cid] && kv.cfg.Shards[shard] == kv.gid {
			kv.lastRequestID[shard][op.Cid] = op.RequestId
			
			DPrintln("Gid = ", kv.gid, " Server ", kv.me, " Apply  ", msg)
			if op.OpType == "Append" {
				kv.database[shard][op.Key] = kv.database[shard][op.Key] + op.Value
				kv.lastResponse[shard][op.Cid] = OK
			} else if op.OpType == "Put" {
				kv.database[shard][op.Key] = op.Value
				kv.lastResponse[shard][op.Cid] = OK
			} else if op.OpType == "Get" {
				kv.lastResponse[shard][op.Cid] = kv.database[shard][op.Key]
			}
		}

		if kv.cfg.Shards[shard] != kv.gid {
			kv.isWrongGroup[index] = true
		}

		kv.applyCond.Broadcast()

		for kv.waitRequest[index] == 1 {
			kv.mu.Unlock()
			kv.applyCond.Wait()
			kv.mu.Lock()
		}

		delete(kv.Request, index)

		kv.mu.Unlock()
		kv.applyCond.L.Unlock()
	}
}


func (kv *ShardKV) CheckConfig() {
	for {
		time.Sleep(100 * time.Millisecond)
		kv.mu.Lock()
		if _, isLeader := kv.rf.GetState(); !isLeader {
			kv.mu.Unlock()
			continue
		}
		kv.mu.Unlock()
		config := kv.mck.Query(-1)
		kv.mu.Lock()
		if config.Num <= kv.cfg.Num {
			kv.mu.Unlock()
			continue
		}
		cmd := Op{0, 0, "Config", "", "", config}
		kv.rf.Start(cmd)
		kv.mu.Unlock()
	}
}



//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}


//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.
	kv.persister = persister
	kv.applyCond = sync.NewCond(new(sync.Mutex))
	kv.waitRequest = make(map[int]int)
	kv.Request = make(map[int]Op)
	kv.isWrongGroup = make(map[int]bool)
	kv.shutdown = false
	kv.cfg.Num = 0

	for i := 0; i < shardmaster.NShards; i++ {
		kv.database[i] = make(map[string]string)
		kv.lastRequestID[i] = make(map[int64]int)
		kv.lastResponse[i] = make(map[int64]string)
	}

	// Use something like this to talk to the shardmaster:
	kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.Apply()
	go kv.CheckConfig()	

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
