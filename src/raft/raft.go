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
import "labrpc"
import "time"
import "math/rand"
import "fmt"

// import "bytes"
// import "labgob"

const (
	Follower  int = 0
	Candidate int = 1
	Leader    int = 2
)


const debugEnabled = false

func debug(format string, a ...interface{}) (n int, err error) {
	if debugEnabled {
		n, err = fmt.Printf(format, a...)
	}
	return
}
//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	applyCh   chan ApplyMsg
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent
	currentTerm  int 	
	votedFor     int 			// vote for which server in the latest term
	log          []LogEntry     // log entries

	

	// Volatile on all servers	
	state   int 				// Leader, Follower, or Candidate
	timeout time.Time 			// Used for cal timeout
	votes   int 				// votes get for the lastest term
	randGen *rand.Rand 			// random gen for election timeout
	commitIndex	 int  			
	lastApplied  int
	lastLogIndex int 
	lastLogTerm  int

	// Volatile state on master
	nextIndex 	[]int 
	matchIndex 	[]int

	Appendch 	[]chan bool
	thisTermFirst int

}

type LogEntry struct {
	Term    int
	Command interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = (rf.state == Leader)

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogTerm  int
	LastLogIndex int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	//debug("%d receive request vote from %d \n", rf.me, args.CandidateId)
	//fmt.Println(args)
	//debug("term %d me %d voteFor %d \n", rf.currentTerm, rf.me, rf.votedFor)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// If RPC request or response contains term T > currentTerm: 
	// set currentTerm = T, convert to follower
	if args.Term > rf.currentTerm {
		rf.votedFor = -1
		rf.currentTerm = args.Term
		rf.state = Follower
	}

	reply.Term = rf.currentTerm

	// 1. Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}

	// 2. Reply false if voteFor is not null and candidatedId
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		reply.VoteGranted = false
		return
	}

	// 3. Reply false if candidate’s log is not at least as up-date as receiver's log
	if rf.lastLogTerm > args.LastLogTerm || (rf.lastLogTerm == args.LastLogTerm && rf.lastLogIndex > args.LastLogIndex) {
		reply.VoteGranted = false
		return
	}

	debug("%d vote for %d \n", rf.me, args.CandidateId)
	debug("my lastLogTerm %d, lastLogIndex %d \n", rf.lastLogTerm, rf.lastLogTerm)

	// 4. Otherwise reply true
	reply.VoteGranted = true
	rf.votedFor = args.CandidateId

	// granting vote to candidate, then reset timer
	rf.timeout = time.Now().Add(time.Millisecond * time.Duration(500+20*(rf.randGen.Int()%16)))

	return
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.og
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
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := -1
	isLeader := (rf.state == Leader)

	// Not the leader, just return 
	if !isLeader {
		return index, term, isLeader
	}


	// new log entry 
	index = rf.lastLogIndex + 1
	term = rf.currentTerm

	// update lastLogIndex
	rf.lastLogIndex++
	rf.lastLogTerm = rf.currentTerm

	// write to local log entries
	if index >= len(rf.log) {
		rf.log = append(rf.log, LogEntry{term, command})
	} else {
		rf.log[index] = LogEntry{term, command}
	}

	// Send AppendEntries to all followers
	for server, _ := range rf.peers {
		if server == rf.me {
			continue
		}
		go func(server int) {
			rf.Appendch[server] <- true
		}(server)
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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	debug("Make %d \n", me)
	// Your initialization code here (2A, 2B, 2C).

	// set: State = Follower, currentTerm = 0, voteFor = -1
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1

	// initial the random generator for election timeout 
	// use current time and serverId to get the seed
	rf.randGen = rand.New(rand.NewSource(time.Now().UnixNano() * int64(me)))

	// Inital election timeout
	rf.timeout = time.Now().Add(time.Millisecond * time.Duration(500+20*(rf.randGen.Int()%16)))


	// Initially, we have log entry at index 0
	// All new log entries start from 1
	rf.log = append(rf.log, LogEntry{Term: 0})
	rf.lastLogIndex = 0
	rf.lastLogTerm = 0
	rf.lastApplied = 0
	rf.commitIndex = 0

	// initialize from state persisted before a crash
	// if it has persisted state saved, it will cover the initialization above
	rf.readPersist(persister.ReadRaftState())


	// Election Timeout :  300ms - 600ms
	go func(rf *Raft) {
		for {
			rf.mu.Lock()
			dura := time.Until(rf.timeout) // Get the duration from now to tiemout
			rf.mu.Unlock()

			time.Sleep(dura) // Sleep for dura time, Sleep without holding the lock

			rf.mu.Lock()
			if rf.timeout.Before(time.Now()) && rf.state != Leader { 
				// timeout and not the leader

				// Become candidate and vote for self
				rf.currentTerm++
				rf.state = Candidate
				rf.votes = 1
				rf.votedFor = rf.me

				for server, _ := range rf.peers {
					if server == rf.me {
						continue // Pass myself, since I have voted for myself
					}
					go func(rf *Raft, server int, term int, me int, lastLogTerm int, lastLogIndex int) {
						args := RequestVoteArgs{term, me, lastLogTerm, lastLogIndex}
						reply := RequestVoteReply{}
						ok := rf.sendRequestVote(server, &args, &reply)
						if ok && reply.VoteGranted {
							rf.mu.Lock()
							if rf.currentTerm == term && rf.state == Candidate {
								// state doesn't change since RequestVote
								rf.votes++

								if rf.votes >= len(rf.peers)/2+1 {
									// Only once
									debug("%d becomes Leader for term %d with lastterm %d lastindex %d\n", rf.me, term, rf.lastLogTerm, rf.lastLogIndex)
									rf.state = Leader
									rf.thisTermFirst = rf.lastLogIndex + 1
									// initialize nextIndex and matchIndex
									rf.Appendch = make([]chan bool, len(rf.peers))
									rf.nextIndex = []int{}
									rf.matchIndex = []int{}
									for i := 0; i < len(rf.peers); i++ {
										rf.nextIndex = append(rf.nextIndex, rf.lastLogIndex+1)
										rf.matchIndex = append(rf.matchIndex, 0)
										rf.Appendch[i] = make(chan bool)
									}

									// start heartbeating
									go rf.heartBeating(term)
								}
							}

							rf.mu.Unlock()
						} else if ok && !reply.VoteGranted {
							rf.mu.Lock()
							if reply.Term > rf.currentTerm {
								rf.currentTerm = reply.Term 
								rf.state = Follower
							}
							rf.mu.Unlock()
						}
					}(rf, server, rf.currentTerm, rf.me, rf.lastLogTerm, rf.lastLogIndex)
				}
			}
			rf.timeout = time.Now().Add(time.Millisecond * time.Duration(300+20*(rf.randGen.Int()%16)))
			rf.mu.Unlock()
		}
	}(rf)

	return rf
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()


	// If RPC request or response contains term T > currentTerm: 
	// set currentTerm = T, convert to follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
	}

	reply.Term = rf.currentTerm

	// 1. Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	// Receive AppendEntries from current leader, reset timer 
	rf.timeout = time.Now().Add(time.Millisecond * time.Duration(500+20*(rf.randGen.Int()%16)))

	// 2. Reply false if log doesn’t contain an entry at prevLogIndex 
	// whose term matches prevLogTerm
	if args.PrevLogIndex >= len(rf.log) || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		return
	}

	// 3. If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it
	i := 0
	for ; i < len(args.Entries); i++ {
		newIndex := args.PrevLogIndex + 1 + i
		if newIndex >= len(rf.log) || rf.log[newIndex].Term != args.Entries[i].Term {
			break
		}
	}
	rf.log = rf.log[:args.PrevLogIndex + 1 + i]

	// 4. Append any new entries not already in the log
	for ; i < len(args.Entries); i++ {
		rf.log = append(rf.log, args.Entries[i])
	}


	// 5. If leaderCommit > commitIndex, 
	// set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1 )
		go rf.Apply()
	}

	rf.lastLogIndex = len(rf.log) - 1
	rf.lastLogTerm = rf.log[rf.lastLogIndex].Term


	debug("%d receive \n", rf.me)
	//fmt.Println(args.Entries)
	debug("%d commitIndex %d\n", rf.me, rf.commitIndex)

	reply.Success = true
	return

}


func (rf *Raft) sendAppendEntries(term int, server int) {
	for {
		_, chanOK := <- rf.Appendch[server]
		if !chanOK {
			return
		}
		ok := false
		for !ok {
			rf.mu.Lock()
			if rf.state != Leader || rf.currentTerm != term {
				rf.mu.Unlock()
				return 
			}
			args := AppendEntriesArgs{
				Term: 			rf.currentTerm,
				LeaderId: 		rf.me,
				PrevLogIndex: 	rf.nextIndex[server] - 1,
				PrevLogTerm: 	rf.log[rf.nextIndex[server]-1].Term,
				Entries: 		rf.log[rf.nextIndex[server]:],
				LeaderCommit: 	rf.commitIndex }
			reply := AppendEntriesReply{}
			rf.mu.Unlock()

			debug("%d send AppendEntries to %d \n", rf.me, server)
			//fmt.Println(args)
			ok = rf.peers[server].Call("Raft.AppendEntries", &args, &reply)

			rf.mu.Lock()
			if rf.state != Leader || rf.currentTerm != term {
				rf.mu.Unlock()
				return 
			}
			if !ok {
				rf.mu.Unlock()
				continue
			}

			if reply.Success {
				rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
				rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)

			} else {
				
				if reply.Term > rf.currentTerm {
					rf.state = Follower
					rf.currentTerm = reply.Term
					rf.mu.Unlock()
					return
				}

				if rf.nextIndex[server] > 0 {
					rf.nextIndex[server]--
					ok = false
				}
			}
			rf.mu.Unlock()

		}
	}
}


func (rf *Raft) heartBeating(term int) {
	rf.mu.Lock()
	
	for server, _ := range rf.peers {
		if server == rf.me {
			continue
		}
		go func(term, server int) {
			rf.sendAppendEntries(term,server)
		}(term, server)
		go func(server int) {
			rf.Appendch[server] <- true
		}(server)
	}
	go func(term int) {
		rf.checkCommit(term)
	}(term)

	rf.mu.Unlock()

	for {
		time.Sleep(100 * time.Millisecond) 	// a heartbeating per 100ms
		for server, _ := range rf.peers {
			if server == rf.me {
				continue
			}
			go func(server int) {
				rf.Appendch[server] <- true
			}(server)
		}
	}
}


func (rf *Raft) checkCommit(term int) {
	cur := 0
	for {
		time.Sleep(50 * time.Millisecond)
		rf.mu.Lock()
		if rf.state != Leader || rf.currentTerm != term {
			rf.mu.Unlock()
			return
		}
		if rf.commitIndex < rf.thisTermFirst {
			cur = rf.thisTermFirst
		} else {
			cur = rf.commitIndex + 1
		}

		cnt := 0
		for server, _ := range rf.peers {
			if server == rf.me {
				cnt++
				continue
			}
			if rf.matchIndex[server] >= cur {
				cnt++
			}
		}
		if cnt >= len(rf.peers)/2 + 1 {
			rf.commitIndex = cur
			go rf.Apply()
		}
		debug("%d commitindex is %d \n", rf.me, rf.commitIndex)
		rf.mu.Unlock()
	}
}


func (rf *Raft) Apply() {
	for {
		rf.mu.Lock()
		if rf.lastApplied == rf.commitIndex {
			rf.mu.Unlock()
			return
		}
		apply := rf.lastApplied + 1
		rf.lastApplied++
		rf.mu.Unlock()
		debug("%d apply : \n", rf.me)
		//fmt.Println(ApplyMsg{true, rf.log[apply].Command, apply})
		rf.applyCh <- ApplyMsg{true, rf.log[apply].Command, apply}
		
	}
} 


func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}



