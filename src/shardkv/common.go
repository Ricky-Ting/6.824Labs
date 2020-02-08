package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK            = "OK"
	ErrNoKey      = "ErrNoKey"
	ErrWrongGroup = "ErrWrongGroup"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Cid   		int64
	RequestId 	int
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Cid   		int64
	RequestId 	int
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}

type TransferArgs struct {
	Num 			int // Cfg.Num
	Shard 			int // Shard
	Database 		map[string]string
	LastRequestID 	map[int64]int
	LastResponse 	map[int64]string
}

type TransferReply struct {
	Success 	bool
	Err 		Err
}

/*
type TransferArgs struct {
	Shard 		int
}

type TransferReply struct {
	Num 			int // Cfg.Num
	Shard 			int // Shard
	Database 		map[string]string
	LastRequestID 	map[int64]int
	LastResponse 	map[int64]string
}
*/
