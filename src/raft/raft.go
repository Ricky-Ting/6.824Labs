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

	// 2A
	currentTerm  int
	votedFor     int
	log          []LogEntry
	lastLogIndex int
	lastLogTerm  int

	state   int
	timeout time.Time
	votes   int
	randGen *rand.Rand

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

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
	LastLogIndex int
	LastLogTerm  int
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

	debug("receive request vote from %d \n", args.CandidateId)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.votedFor = -1
		rf.currentTerm = args.Term
		rf.state = Follower
	}
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}

	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		reply.VoteGranted = false
		return
	}

	if rf.lastLogTerm > args.LastLogTerm || (rf.lastLogTerm == args.LastLogTerm && rf.lastLogIndex > args.LastLogIndex) {
		reply.VoteGranted = false
		return
	}

	debug(" vote for %d \n", args.CandidateId)

	reply.VoteGranted = true
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

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

	debug("Make %d \n", me)
	// Your initialization code here (2A, 2B, 2C).

	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1

	rf.randGen = rand.New(rand.NewSource(time.Now().UnixNano() * int64(me)))

	rf.timeout = time.Now().Add(time.Millisecond * time.Duration(500+20*(rf.randGen.Int()%16)))

	//rf.randGen.Seed(time.Now().UnixNano() * me)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// Election Timeout :  300ms - 600ms
	go func(rf *Raft) {
		for {
			rf.mu.Lock()
			dura := time.Until(rf.timeout)
			rf.mu.Unlock()

			time.Sleep(dura)

			rf.mu.Lock()
			if rf.timeout.Before(time.Now()) && rf.state != Leader {
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
								rf.votes++

								if rf.votes >= len(rf.peers)/2+1 {
									debug("%d becomes Leader for term %d\n", rf.me, term)
									rf.state = Leader
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

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	rf.currentTerm = args.Term
	rf.state = Follower
	reply.Term = rf.currentTerm
	rf.timeout = time.Now().Add(time.Millisecond * time.Duration(500+20*(rf.randGen.Int()%16)))
	reply.Success = true
	return

}

func (rf *Raft) heartBeating(term int) {
	for {
		time.Sleep(100 * time.Millisecond)
		rf.mu.Lock()
		if rf.state == Leader && rf.currentTerm == term {
			for server, _ := range rf.peers {
				if server == rf.me {
					continue
				}
				go func(rf *Raft, server int, term int, leaderId int, prevLogIndex int, prevLogTerm int, leaderCommit int) {
					args := AppendEntriesArgs{
						Term:         term,
						LeaderId:     leaderId,
						PrevLogIndex: prevLogIndex,
						PrevLogTerm:  prevLogTerm,
						LeaderCommit: leaderCommit}
					reply := AppendEntriesReply{}

					rf.mu.Lock()
					if rf.state != Leader {
						rf.mu.Unlock()
						return
					}
					rf.mu.Unlock()

					ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
					if ok && !reply.Success {
						rf.mu.Lock()
						if reply.Term > rf.currentTerm {
							rf.state = Follower
							rf.currentTerm = reply.Term
						}
						rf.mu.Unlock()
					}
				}(rf, server, rf.currentTerm, rf.me, 0, 0, 0)
			}
		} else {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

	}
}




