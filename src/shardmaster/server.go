package shardmaster


import "raft"
import "labrpc"
import "sync"
import "labgob"


type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	applyCond 	*sync.Cond
	lastRequestID map[int64]int // Record latest request for different clerks
	lastResponse map[int64]string // Record latest response for different clerks
	waitRequest map[int]int  // map[index] = 0, 1 : whether a goroutine wait for index
	Request 	map[int]Op 	 // map[index] = Op
	shutdown 	bool

	configs []Config // indexed by config num
}


type Op struct {
	// Your data here.
	Optype 		string 				// Join, Leave, Move, or Query
	Servers 	map[int][]string 	// args for Join
	GIDs 		[]int 				// args for Leave or Move
	Shard 		int 				// args for Move
	Num 		int 				// args for Query
}


func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
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
	sm.lastResponse = make(map[int64]string)
	sm.waitRequest = make(map[int]int)
	sm.Request = make(map[int]Op)

	return sm
}
