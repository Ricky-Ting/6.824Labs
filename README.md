# 6.824 Labs



## Description 

This is my completion of MIT 6.824 Distributed Systems's course project(see [here](http://nil.csail.mit.edu/6.824/2018/index.html)] for more details).

**Language**: Go

**Platform**: Linux, Mac OS X



## Outline

### [Lab 1](http://nil.csail.mit.edu/6.824/2018/labs/lab-1.html) 

In lab 1, I build a MapReduce library following the paper [MapReduce](http://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf). Also, I write some MapReduce applications, such as Wordcount, Inverted index generation. 

Codes are in `./src/mapreduce` and `./src/main`.



### [Lab 2](http://nil.csail.mit.edu/6.824/2018/labs/lab-raft.html)

In lab 2, I implement Raft, a replicated state machine protocol, following the paper [Raft](http://nil.csail.mit.edu/6.824/2018/papers/raft-extended.pdf). You can refer to a [visual illustration](http://thesecretlivesofdata.com/raft/) of Raft. 

Codes are in `./src/raft`. 



### [Lab 3](http://nil.csail.mit.edu/6.824/2018/labs/lab-kvraft.html)

In lab 3, I build a fault-tolerant key/value database service using my Raft library from lab 2. 

This service supports three operations: `Put(key, value)`, `Append(key, arg)`, and `Get(key)`.

As there exist concurrent requests from mutiple clients and database is replicated to many machines, my service have to provide string consistency to clients. Things are tricky on a condition where machines may fail, network may delay or break. My service have to ensure every client see the latest and consistent state.

Codes are in `./src/kvraft`



### [Lab 4](http://nil.csail.mit.edu/6.824/2018/labs/lab-shard.html)

In lab 4, I build a key/value database system that "shards", or partitions, the keys over a set of replica groups. 

In shardmaster, I have to implement four APIs: "Join", "Leave", "Move", "Query". These APIs are intended to allow an administrator to control the shardmaster: to add new replica groups, to eliminate replica groups, and to move shards between replica groups. Every call to a API(except Query) will generate a new configuration. "Query" is used to query master about some configuration. Also master are also fault-tolerant as it's on top of raft, a replica groups.

In shardkv, I have to do some details. Provide service to clients just as lab 2. But this differ from lab 2 in that service are paritioned and shards may move from one replica group to another group, which adds to lots of difficulty.

Codes are in `./src/shardmaster` and `./src/shardkv`.