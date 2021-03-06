package shardmaster


import "raft"
import "labrpc"
import "sync"
import "labgob"
import "log"
import "fmt"
import "sort"
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


type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	applyCond 	*sync.Cond
	lastRequestID map[int64]int // Record latest request for different clerks
	lastResponse map[int64]Config // Record latest response for different clerks
	waitRequest map[int]int  // map[index] = 0, 1 : whether a goroutine wait for index
	Request 	map[int]Op 	 // map[index] = Op
	shutdown 	bool

	configs []Config // indexed by config num
}


type Op struct {
	// Your data here.
	Cid 		int64
	RequestId 	int
	OpType 		string 				// Join, Leave, Move, or Query
	Servers 	map[int][]string 	// args for Join
	GIDs 		[]int 				// args for Leave 
	GID 		int 				// args for Move
	Shard 		int 				// args for Move
	Num 		int 				// args for Query
}

type RevMap struct {
	GID 	int
	Shards 	int
}


func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	sm.mu.Lock()

	if args.RequestId <= sm.lastRequestID[args.Cid] {
		reply.WrongLeader = false
		DPrintln("In server Join complete clerk = ", args.Cid," RequestId = ", args.RequestId)
		sm.mu.Unlock()
		return 
	}

	cmd := Op{
			Cid: args.Cid,
			RequestId: args.RequestId,
			OpType: "Join", 
			Servers: args.Servers}
	index, term, isLeader := sm.rf.Start(cmd)
	if !isLeader {
		reply.WrongLeader = true
		sm.mu.Unlock()
		return
	}
	reply.WrongLeader = false
	sm.waitRequest[index] = 1
	DPrintf("In server %d Join index = %d \n", sm.me, index)
	sm.mu.Unlock()

	sm.applyCond.L.Lock()
	sm.mu.Lock()
	for _, ok := sm.Request[index]; !ok; _, ok = sm.Request[index]  {
		sm.mu.Unlock()
		sm.applyCond.Wait()
		sm.mu.Lock()

		if sm.shutdown {
			delete(sm.waitRequest, index)
			sm.applyCond.Broadcast()
			reply.WrongLeader = true
			sm.mu.Unlock()
			sm.applyCond.L.Unlock()
			return 
		}

		curTerm, curIsLeader := sm.rf.GetState()
		if curTerm != term || !curIsLeader {
			delete(sm.waitRequest, index)
			//kv.waitRequest[index] = 0
			reply.WrongLeader = true
			sm.applyCond.Broadcast()
			DPrintf("In server Join %d not leader any more index %d failed", sm.me, index)
			sm.mu.Unlock()
			sm.applyCond.L.Unlock()
			return 
		}

	}
	if sm.Request[index].Cid != cmd.Cid || sm.Request[index].RequestId != cmd.RequestId {
		delete(sm.waitRequest, index)
		reply.WrongLeader = true
		DPrintf("In server Join index %d not origin", index)
		sm.applyCond.Broadcast()
		sm.mu.Unlock()
		sm.applyCond.L.Unlock()
		return
	}

	delete(sm.waitRequest, index)
	DPrintln("In server Join complete ", index)
	sm.applyCond.Broadcast()
	sm.mu.Unlock()
	sm.applyCond.L.Unlock()

	return
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	sm.mu.Lock()

	if args.RequestId <= sm.lastRequestID[args.Cid] {
		reply.WrongLeader = false
		DPrintln("In server Leave complete clerk = ", args.Cid," RequestId = ", args.RequestId)
		sm.mu.Unlock()
		return 
	}

	cmd := Op{
			Cid: args.Cid,
			RequestId: args.RequestId,
			OpType: "Leave", 
			GIDs: args.GIDs}
	index, term, isLeader := sm.rf.Start(cmd)
	if !isLeader {
		reply.WrongLeader = true
		sm.mu.Unlock()
		return
	}
	reply.WrongLeader = false
	sm.waitRequest[index] = 1
	DPrintf("In server %d Leave index = %d \n", sm.me, index)
	sm.mu.Unlock()

	sm.applyCond.L.Lock()
	sm.mu.Lock()
	for _, ok := sm.Request[index]; !ok; _, ok = sm.Request[index]  {
		sm.mu.Unlock()
		sm.applyCond.Wait()
		sm.mu.Lock()

		if sm.shutdown {
			delete(sm.waitRequest, index)
			sm.applyCond.Broadcast()
			reply.WrongLeader = true
			sm.mu.Unlock()
			sm.applyCond.L.Unlock()
			return 
		}

		curTerm, curIsLeader := sm.rf.GetState()
		if curTerm != term || !curIsLeader {
			delete(sm.waitRequest, index)
			//kv.waitRequest[index] = 0
			reply.WrongLeader = true
			sm.applyCond.Broadcast()
			DPrintf("In server Leave %d not leader any more index %d failed", sm.me, index)
			sm.mu.Unlock()
			sm.applyCond.L.Unlock()
			return 
		}

	}
	if sm.Request[index].Cid != cmd.Cid || sm.Request[index].RequestId != cmd.RequestId {
		delete(sm.waitRequest, index)
		reply.WrongLeader = true
		DPrintf("In server Leave index %d not origin", index)
		sm.applyCond.Broadcast()
		sm.mu.Unlock()
		sm.applyCond.L.Unlock()
		return
	}

	delete(sm.waitRequest, index)
	DPrintln("In server Join complete ", index)
	sm.applyCond.Broadcast()
	sm.mu.Unlock()
	sm.applyCond.L.Unlock()

	return
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	sm.mu.Lock()

	if args.RequestId <= sm.lastRequestID[args.Cid] {
		reply.WrongLeader = false
		DPrintln("In server Move complete clerk = ", args.Cid," RequestId = ", args.RequestId)
		sm.mu.Unlock()
		return 
	}

	cmd := Op{
			Cid: args.Cid,
			RequestId: args.RequestId,
			OpType: "Move", 
			Shard: args.Shard,
			GID: args.GID}
	index, term, isLeader := sm.rf.Start(cmd)
	if !isLeader {
		reply.WrongLeader = true
		sm.mu.Unlock()
		return
	}
	reply.WrongLeader = false
	sm.waitRequest[index] = 1
	DPrintf("In server %d Move index = %d \n", sm.me, index)
	sm.mu.Unlock()

	sm.applyCond.L.Lock()
	sm.mu.Lock()
	for _, ok := sm.Request[index]; !ok; _, ok = sm.Request[index]  {
		sm.mu.Unlock()
		sm.applyCond.Wait()
		sm.mu.Lock()

		if sm.shutdown {
			delete(sm.waitRequest, index)
			sm.applyCond.Broadcast()
			reply.WrongLeader = true
			sm.mu.Unlock()
			sm.applyCond.L.Unlock()
			return 
		}

		curTerm, curIsLeader := sm.rf.GetState()
		if curTerm != term || !curIsLeader {
			delete(sm.waitRequest, index)
			//kv.waitRequest[index] = 0
			reply.WrongLeader = true
			sm.applyCond.Broadcast()
			DPrintf("In server Move %d not leader any more index %d failed", sm.me, index)
			sm.mu.Unlock()
			sm.applyCond.L.Unlock()
			return 
		}

	}
	if sm.Request[index].Cid != cmd.Cid || sm.Request[index].RequestId != cmd.RequestId {
		delete(sm.waitRequest, index)
		reply.WrongLeader = true
		DPrintf("In server Move index %d not origin", index)
		sm.applyCond.Broadcast()
		sm.mu.Unlock()
		sm.applyCond.L.Unlock()
		return
	}

	delete(sm.waitRequest, index)
	DPrintln("In server Move complete ", index)
	sm.applyCond.Broadcast()
	sm.mu.Unlock()
	sm.applyCond.L.Unlock()

	return
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	sm.mu.Lock()

	if args.RequestId <= sm.lastRequestID[args.Cid] {
		reply.WrongLeader = false
		reply.Config = sm.lastResponse[args.Cid]
		DPrintln("In server Query complete clerk = ", args.Cid," RequestId = ", args.RequestId)
		sm.mu.Unlock()
		return 
	}

	cmd := Op{
			Cid: args.Cid,
			RequestId: args.RequestId,
			OpType: "Query", 
			Num: args.Num}
	index, term, isLeader := sm.rf.Start(cmd)
	if !isLeader {
		reply.WrongLeader = true
		sm.mu.Unlock()
		return
	}
	reply.WrongLeader = false
	sm.waitRequest[index] = 1
	DPrintf("In server %d Query index = %d \n", sm.me, index)
	sm.mu.Unlock()

	sm.applyCond.L.Lock()
	sm.mu.Lock()
	for _, ok := sm.Request[index]; !ok; _, ok = sm.Request[index]  {
		sm.mu.Unlock()
		sm.applyCond.Wait()
		sm.mu.Lock()

		if sm.shutdown {
			delete(sm.waitRequest, index)
			sm.applyCond.Broadcast()
			reply.WrongLeader = true
			sm.mu.Unlock()
			sm.applyCond.L.Unlock()
			return 
		}

		curTerm, curIsLeader := sm.rf.GetState()
		if curTerm != term || !curIsLeader {
			delete(sm.waitRequest, index)
			//kv.waitRequest[index] = 0
			reply.WrongLeader = true
			sm.applyCond.Broadcast()
			DPrintf("In server Query %d not leader any more index %d failed", sm.me, index)
			sm.mu.Unlock()
			sm.applyCond.L.Unlock()
			return 
		}

	}
	if sm.Request[index].Cid != cmd.Cid || sm.Request[index].RequestId != cmd.RequestId {
		delete(sm.waitRequest, index)
		reply.WrongLeader = true
		DPrintf("In server Query index %d not origin", index)
		sm.applyCond.Broadcast()
		sm.mu.Unlock()
		sm.applyCond.L.Unlock()
		return
	}

	delete(sm.waitRequest, index)
	reply.Config = sm.lastResponse[args.Cid]
	DPrintln("In server Query complete ", index)
	sm.applyCond.Broadcast()
	sm.mu.Unlock()
	sm.applyCond.L.Unlock()

	return
}


func (sm *ShardMaster) Apply() {
	for {
		msg, ok := <- sm.applyCh
		if !ok {
			return
		}
		DPrintln(" Server ", sm.me, " Apply receive ", msg)
		
		sm.applyCond.L.Lock()
		sm.mu.Lock()

		index := msg.CommandIndex
		//term := msg.CommandTerm
		op, ok := msg.Command.(Op)
		if !ok {
			fmt.Println("In Apply: type error")
		}
		sm.Request[index] = op 
		if op.RequestId > sm.lastRequestID[op.Cid] {
			sm.lastRequestID[op.Cid] = op.RequestId

			oldCfg := sm.configs[len(sm.configs)-1]
			newCfg := Config{Num: len(sm.configs)}
			newCfg.Groups = map[int][]string{}
			groupCnt := 0
			var avg int
			groupNum := make(map[int]int)  // map[GID] = #shards on GID
			// Copy old Groups to new Groups
			for k, v := range oldCfg.Groups {
				newCfg.Groups[k] = v
				groupNum[k] = 0
				groupCnt++
			}

			DPrintln(" Server ", sm.me, " Apply  ", msg)
			if op.OpType == "Join" {
				for k, v := range op.Servers {
					newCfg.Groups[k] = v
					groupNum[k] = 0
					groupCnt++
				}
				if groupCnt != 0 {
					avg = len(oldCfg.Shards) / groupCnt
					x := len(oldCfg.Shards) - avg * groupCnt // num for avg+1
					//y := len(oldCfg.Shards) - x 			// num for avg
					
					for _, k := range oldCfg.Shards {
						if _, ok := newCfg.Groups[k]; ok {
							groupNum[k]++
						}
					}

					rev := make([]RevMap, 0, 0)
					for k, num := range groupNum {
						rev = append(rev, RevMap{k, num})
					}

					sort.Slice(rev, func(i, j int) bool {
						return rev[i].Shards > rev[j].Shards || (rev[i].Shards == rev[j].Shards && rev[i].GID < rev[j].GID)
					})

					for i, RM := range rev {
						if i < x {
							groupNum[RM.GID] = groupNum[RM.GID] - (avg + 1)
						} else {
							groupNum[RM.GID] = groupNum[RM.GID] - avg
						}
					}

					for i, k := range oldCfg.Shards {
						if _, ok := newCfg.Groups[k]; ok {
							if groupNum[k] <= 0 {
								newCfg.Shards[i] = k
								continue
							}
							groupNum[k]--
						}
						for j := groupCnt-1; j >= 0; j-- {
							if groupNum[rev[j].GID] < 0 {
								newCfg.Shards[i] = rev[j].GID
								groupNum[rev[j].GID]++
								break
							}
						}
					}
				}
				sm.configs = append(sm.configs, newCfg)
			} else if op.OpType == "Leave" {
				for _, k := range op.GIDs {
					delete(newCfg.Groups, k)
					delete(groupNum, k)
					groupCnt--
				}
				if groupCnt != 0 {
					avg = len(oldCfg.Shards) / groupCnt
					x := len(oldCfg.Shards) - avg * groupCnt // num for avg+1
					//y := len(oldCfg.Shards) - x 			// num for avg
					
					for _, k := range oldCfg.Shards {
						if _, ok := newCfg.Groups[k]; ok {
							groupNum[k]++
						}
					}

					rev := make([]RevMap, 0, 0)
					for k, num := range groupNum {
						rev = append(rev, RevMap{k, num})
					}

					sort.Slice(rev, func(i, j int) bool {
						return rev[i].Shards > rev[j].Shards || (rev[i].Shards == rev[j].Shards && rev[i].GID < rev[j].GID)
					})

					for i, RM := range rev {
						if i < x {
							groupNum[RM.GID] = groupNum[RM.GID] - (avg + 1)
						} else {
							groupNum[RM.GID] = groupNum[RM.GID] - avg
						}
					}

					for i, k := range oldCfg.Shards {
						if _, ok := newCfg.Groups[k]; ok {
							if groupNum[k] <= 0 {
								newCfg.Shards[i] = k
								continue
							}
							groupNum[k]--
						}
						for j := groupCnt-1; j >= 0; j-- {
							if groupNum[rev[j].GID] < 0 {
								newCfg.Shards[i] = rev[j].GID
								groupNum[rev[j].GID]++
								break
							}
						}
					}
				}
				sm.configs = append(sm.configs, newCfg)
				
			} else if op.OpType == "Move" {
				for i, k := range oldCfg.Shards {
					newCfg.Shards[i] = k
				}
				newCfg.Shards[op.Shard] = op.GID

				sm.configs = append(sm.configs, newCfg)

			} else if op.OpType == "Query" {
				if op.Num < len(sm.configs) && op.Num >= 0 {
					sm.lastResponse[op.Cid] = sm.configs[op.Num]
				} else {
					sm.lastResponse[op.Cid] = sm.configs[len(sm.configs) - 1] 
				}
			} else {
				fmt.Println("In Apply: OpType Errror")
			}
		}

		sm.applyCond.Broadcast()

		//DPrintln(kv.Request[index])

		for sm.waitRequest[index] == 1 {
			sm.mu.Unlock()
			sm.applyCond.Wait()
			sm.mu.Lock()
		}

		delete(sm.Request, index)

		sm.mu.Unlock()
		sm.applyCond.L.Unlock()
	}
}



//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
	sm.mu.Lock()
	sm.shutdown = true
	sm.mu.Unlock()
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.applyCond = sync.NewCond(new(sync.Mutex))
	sm.lastRequestID = make(map[int64]int)
	sm.lastResponse = make(map[int64]Config)
	sm.waitRequest = make(map[int]int)
	sm.Request = make(map[int]Op)
	sm.shutdown = false

	go sm.Apply()

	go func() {
		for {
			time.Sleep(500 * time.Millisecond)
			sm.mu.Lock()
			if sm.shutdown {
				return
			}
			sm.mu.Unlock()
			sm.applyCond.Broadcast()
		}
	}()

	return sm
}
