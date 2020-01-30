package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"
import "sync"
//import "fmt"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	mu      sync.Mutex
	lastMaster 	int 	// latest master
	nServers  	int 	// number of servers
	cid			int64 	// Clerk's Id
	lastRequseId int 	// lastest Request ID
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
	ck.lastMaster = 0
	ck.nServers = len(servers)
	ck.cid = nrand()
	//fmt.Println("Clerk Id is ", ck.cid)
	ck.lastRequseId = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	ret := ""
	
	ck.mu.Lock()
	curLeader := ck.lastMaster
	ck.mu.Unlock()

	ck.mu.Lock()
	args := GetArgs{key, ck.cid, ck.lastRequseId + 1}
	ck.lastRequseId++
	ck.mu.Unlock()

	for {
		reply := GetReply{}
		ok := ck.servers[curLeader].Call("KVServer.Get", &args, &reply)
		if ok && !reply.WrongLeader {
			ck.mu.Lock()
			ret = reply.Value
			ck.lastMaster = curLeader
			ck.mu.Unlock()
			//fmt.Println("Succeed in client Get ", args, " ", reply)
			break
		}
		curLeader = (curLeader + 1) % ck.nServers
	}
	//fmt.Println("Succeed in client Gett ", args)

	return ret
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.mu.Lock()
	curLeader := ck.lastMaster
	ck.mu.Unlock()

	ck.mu.Lock()
	args := PutAppendArgs{key, value, op, ck.cid, ck.lastRequseId + 1}
	ck.lastRequseId++
	ck.mu.Unlock()

	for {
		reply := PutAppendReply{}
		ok := ck.servers[curLeader].Call("KVServer.PutAppend", &args, &reply)
		if ok && !reply.WrongLeader {
			ck.mu.Lock()
			ck.lastMaster = curLeader
			//fmt.Println("Succeed in client PutAppend ", args, " ", reply)
			ck.mu.Unlock()
			break
		}
		curLeader = (curLeader + 1) % ck.nServers
	}

	//fmt.Println("Succeed in client PutAppenddd ", args, " ")
	return
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
