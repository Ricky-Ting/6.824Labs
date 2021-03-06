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
	OpType 		string  // Get, Put, Append, Config, ShardState
	Key 		string
	Value 		string
	Cfg 		shardmaster.Config	
	ShardState	TransferArgs
}

type ShardServerState struct {
	Database 		   	[shardmaster.NShards]map[string]string
	LastRequestID 	   	[shardmaster.NShards]map[int64]int
	LastResponse 	   	[shardmaster.NShards]map[int64]string
	Cfg 				shardmaster.Config
	IsReady 			[shardmaster.NShards]bool
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
	isReady 		[shardmaster.NShards]bool
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
	kv.mu.Unlock()

	kv.applyCond.L.Lock()
	kv.mu.Lock()
	for !kv.isReady[shard] {
		kv.mu.Unlock()
		kv.applyCond.Wait()
		kv.mu.Lock()
		if kv.cfg.Shards[shard] != kv.gid || kv.shutdown {
			reply.Err = ErrWrongGroup
			kv.mu.Unlock()
			kv.applyCond.L.Unlock()
			return
		}
		DPrintln("Gid = ", kv.gid, " In server sleep")
	}

	cmd := Op{args.Cid, args.RequestId, "Get", args.Key, "", shardmaster.Config{}, TransferArgs{}}
	index, term, isLeader := kv.rf.Start(cmd)
	if !isLeader {
		reply.WrongLeader = true
		kv.mu.Unlock()
		kv.applyCond.L.Unlock()
		return
	}
	reply.WrongLeader = false
	kv.waitRequest[index] = 1
	DPrintln("Gid = ", kv.gid, " In server ", kv.me, " ", args, " index = ", index)
	kv.mu.Unlock()
	kv.applyCond.L.Unlock()

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

		if kv.cfg.Shards[shard] != kv.gid {
			delete(kv.waitRequest, index)
			reply.Err = ErrWrongGroup
			kv.applyCond.Broadcast()
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
	DPrintln("Key is ", args.Key, " kv.cfg is ", kv.cfg)
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
	kv.mu.Unlock()

	kv.applyCond.L.Lock()
	kv.mu.Lock()
	for !kv.isReady[shard] {
		kv.mu.Unlock()
		kv.applyCond.Wait()
		kv.mu.Lock()
		if kv.cfg.Shards[shard] != kv.gid || kv.shutdown {
			reply.Err = ErrWrongGroup
			kv.mu.Unlock()
			kv.applyCond.L.Unlock()
			return
		}
		DPrintln("Gid = ", kv.gid, " In server sleep")
		
	}

	cmd := Op{args.Cid, args.RequestId, args.Op, args.Key, args.Value, shardmaster.Config{}, TransferArgs{}}
	index, term, isLeader := kv.rf.Start(cmd)
	if !isLeader {
		reply.WrongLeader = true
		kv.mu.Unlock()
		kv.applyCond.L.Unlock()
		return
	}
	reply.WrongLeader = false
	kv.waitRequest[index] = 1
	kv.mu.Unlock()
	kv.applyCond.L.Unlock()

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

		if kv.cfg.Shards[shard] != kv.gid {
			delete(kv.waitRequest, index)
			reply.Err = ErrWrongGroup
			kv.applyCond.Broadcast()
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

		if !msg.CommandValid {
			kv.applyCond.L.Lock()
			kv.mu.Lock()
			
			sp, ok := msg.Command.(raft.Snapshot)
			if !ok {
				fmt.Println("In Apply: type error")
			}
			state, ok2 := sp.ApplicationState.(ShardServerState)
			if !ok2 {
				fmt.Println("In Apply: type error")
			}

			kv.database = state.Database
			kv.lastRequestID = state.LastRequestID
			kv.lastResponse = state.LastResponse
			kv.cfg= state.Cfg
			kv.isReady = state.IsReady
			
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

		if op.OpType == "Config" {
			if op.Cfg.Num <= kv.cfg.Num {
				DPrintf("gid %d, server %d, outdate cfg\n", kv.gid, kv.me)
				if kv.maxraftstate != -1 {
					kv.saveSnapshot(index, term)
				}
				kv.mu.Unlock()
				kv.applyCond.L.Unlock()
				continue
			}

			for i := 0; i < shardmaster.NShards; i++ {
				if kv.cfg.Shards[i] == 0 && op.Cfg.Shards[i] == kv.gid {
					kv.isReady[i] = true
					DPrintf("In gid %d, server %d, shard %d is ready \n", kv.gid, kv.me, i)
					continue
				}
				if kv.cfg.Shards[i] != kv.gid && op.Cfg.Shards[i] == kv.gid {
					kv.isReady[i] = false
					//go kv.requsetTransfer(op.Cfg.Groups[kv.cfg.Shards[i]], i)
					continue
				}
				if kv.cfg.Shards[i] == kv.gid && op.Cfg.Shards[i] != kv.gid {
					kv.isReady[i] = false
					database := make(map[string]string)
					lastRequestID := make(map[int64]int)
					lastResponse := make(map[int64]string)
					for k, v := range kv.database[i] {
						database[k] = v
					}
					for k, v := range kv.lastRequestID[i] {
						lastRequestID[k] = v
					}
					for k, v := range kv.lastResponse[i] {
						lastResponse[k] = v
					}
					go kv.sendTransfer(op.Cfg.Groups[op.Cfg.Shards[i]], op.Cfg.Num, i, database, lastRequestID, lastResponse)
				}
			}
			kv.cfg = op.Cfg
			kv.applyCond.Broadcast()
			if kv.maxraftstate != -1 {
				kv.saveSnapshot(index, term)
			}
			kv.mu.Unlock()
			kv.applyCond.L.Unlock()
			DPrintln("Gid = ", kv.gid, " Server ", kv.me, " Apply receive ", msg, " Done!!!")
			continue
		}

		if op.OpType == "ShardState" {
			args := op.ShardState
			if args.Num != kv.cfg.Num || kv.isReady[args.Shard] {
				if kv.maxraftstate != -1 {
					kv.saveSnapshot(index, term)
				}
				kv.mu.Unlock()
				kv.applyCond.L.Unlock()
				continue
			}
			kv.database[args.Shard] = args.Database
			kv.lastRequestID[args.Shard] = args.LastRequestID
			kv.lastResponse[args.Shard] = args.LastResponse
			kv.isReady[args.Shard] = true
			kv.applyCond.Broadcast()
			if kv.maxraftstate != -1 {
				kv.saveSnapshot(index, term)
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

		if kv.maxraftstate != -1 {
			kv.saveSnapshot(index, term)
		}

		kv.mu.Unlock()
		kv.applyCond.L.Unlock()
	}
}


func (kv *ShardKV) saveSnapshot(index, term int) {

	if kv.maxraftstate > kv.persister.RaftStateSize() {
		return
	}

	DPrintf("server %d, maxraftstate is %d, raftstatesize is %d \n", kv.me, kv.maxraftstate, kv.persister.RaftStateSize())
	sp := raft.Snapshot{index, term, ShardServerState{kv.database, kv.lastRequestID, kv.lastResponse, kv.cfg, kv.isReady}}
	DPrintf("server %d call raft.Snapshot \n", kv.me)
	kv.rf.SaveSnapshot(sp)
	DPrintf("server %d call raft.Snapshot return \n", kv.me)
}


func (kv *ShardKV) CheckConfig() {
	for {
		time.Sleep(100 * time.Millisecond)
		kv.mu.Lock()
		if kv.shutdown {
			kv.mu.Unlock()
			return
		}
		if _, isLeader := kv.rf.GetState(); !isLeader {
			kv.mu.Unlock()
			continue
		}
		query := kv.cfg.Num + 1
		kv.mu.Unlock()
		config := kv.mck.Query(query)

		kv.applyCond.L.Lock()
		kv.mu.Lock()
		if config.Num <= kv.cfg.Num {
			kv.mu.Unlock()
			kv.applyCond.L.Unlock()
			continue
		}
		flag := true
		for flag {
			flag = false
			for i := 0; i < shardmaster.NShards; i++ {
				if kv.cfg.Shards[i] == kv.gid && !kv.isReady[i] {
					flag = true
				}
			}
			if flag {
				kv.mu.Unlock()
				kv.applyCond.Wait()
				kv.mu.Lock()
			}
		}
		cmd := Op{0, 0, "Config", "", "", config, TransferArgs{}}
		_, _, isLeader := kv.rf.Start(cmd)
		if isLeader {
			DPrintln("gid: ",kv.gid, "server: ", kv.me, "old config: ", kv.cfg, " new config: ", config)
		}
		kv.mu.Unlock()
		kv.applyCond.L.Unlock()
	}
}
/*
func (kv *ShardKV) Transfer(args *TransferArgs, reply *TransferReply) {
	kv.applyCond.L.Lock()
	defer kv.applyCond.L.Unlock()
	kv.mu.Lock()
	defer kv.mu.Unlock()
	
	shard := args.Shard
	DPrintf("gid %d server %d receive request shard %d \n", kv.gid, kv.me, shard)
	
	for kv.isReady[shard] {
		kv.mu.Unlock()
		kv.applyCond.Wait()
		kv.mu.Lock()
	}
	DPrintf("gid %d server %d send request shard %d \n", kv.gid, kv.me, shard)
	database := make(map[string]string)
	lastRequestID := make(map[int64]int)
	lastResponse := make(map[int64]string)
	for k, v := range kv.database[shard] {
		database[k] = v
	}
	for k, v := range kv.lastRequestID[shard] {
		lastRequestID[k] = v
	}
	for k, v := range kv.lastResponse[shard] {
		lastResponse[k] = v
	}

	reply.Num = kv.cfg.Num
	reply.Shard = shard
	reply.Database = database
	reply.LastRequestID = lastRequestID
	reply.LastResponse = lastResponse

	return
}

func (kv *ShardKV) requsetTransfer(servers []string, shard int) {
	args := TransferArgs{shard}
	for {
		for _, server := range servers {
			reply := TransferReply{}
			srv := kv.make_end(server)
			DPrintf("gid %d server %d request shard %d \n", kv.gid, kv.me, shard)
			ok := srv.Call("ShardKV.Transfer", &args, &reply)
			DPrintf("gid %d server %d receive shard %d \n", kv.gid, kv.me, shard)
			DPrintln("ok is ", ok)
			if ok {
				kv.mu.Lock()
				DPrintf("gid %d server %d receive shard %d \n", kv.gid, kv.me, shard)
				kv.database[shard] = reply.Database
				kv.lastRequestID[shard] = reply.LastRequestID
				kv.lastResponse[shard] = reply.LastResponse
				kv.isReady[shard] = true
				kv.applyCond.Broadcast()
				kv.mu.Unlock()
				return
			}
		}
	}
}

*/

func (kv *ShardKV) sendTransfer(servers []string, num int, shard int, database map[string]string, lastRequestID map[int64]int, lastResponse map[int64]string) {
	args := TransferArgs{num, shard, database, lastRequestID, lastResponse}
	for _, server := range servers {
		srv := kv.make_end(server)
		go func(srv *labrpc.ClientEnd, args TransferArgs, server string) {
			for {
				//DPrintln("send ", args, " to ", server)
				reply := TransferReply{}
				ok := srv.Call("ShardKV.Transfer", &args, &reply)
				if ok && reply.Success {
					return
				}
				time.Sleep(500 * time.Millisecond)
			}
		}(srv, args, server)
	}
}

func (kv *ShardKV) Transfer(args *TransferArgs, reply *TransferReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if args.Num < kv.cfg.Num {
		reply.Success = true
		return
	}
	if args.Num > kv.cfg.Num {
		reply.Success = false
		return
	}
	if kv.isReady[args.Shard] {
		reply.Success = true
		return
	}

	ShardState := TransferArgs{args.Num, args.Shard, args.Database, args.LastRequestID, args.LastResponse}

	cmd := Op{0, 0, "ShardState", "", "", shardmaster.Config{}, ShardState}
	_, _, isLeader := kv.rf.Start(cmd)
	if isLeader {
		reply.Success = false
	} else {
		reply.Success = false
	}
	/*
	kv.database[args.Shard] = args.Database
	kv.lastRequestID[args.Shard] = args.LastRequestID
	kv.lastResponse[args.Shard] = args.LastResponse
	kv.isReady[args.Shard] = true
	reply.Success = true
	*/
	//DPrintf("gid %d server %d receive shard %d \n", kv.gid, kv.me, args.Shard)
	
}


//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	
	kv.mu.Lock()
	kv.shutdown = true
	kv.mu.Unlock()
	
	// Your code here, if desired.\
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
	labgob.Register(ShardServerState{})
	DPrintln("Make ShardKV ", me)

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
