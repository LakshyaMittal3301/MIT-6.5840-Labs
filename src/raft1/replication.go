package raft

import "time"

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []interface{}
	LeaderCommit []int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		Debug(dTerm, "S%d sees higher term in AE from S%d: %d > %d",
			rf.me, args.LeaderId, args.Term, rf.currentTerm)
		rf.becomeFollowerLocked(args.Term)
	}
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		Debug(dLog1, "S%d rejects AE from S%d (stale term %d < %d)",
			rf.me, args.LeaderId, args.Term, rf.currentTerm)
		reply.Success = false
		return
	}

	reply.Success = true
	rf.lastHeard = time.Now()
	Debug(dTimer, "S%d got heartbeat from S%d at T%d",
		rf.me, args.LeaderId, args.Term)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) startLogReplication(term int) {
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		go rf.startAppendEntries(server, term)
	}
}

func (rf *Raft) startAppendEntries(server int, term int) {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.role != Leader || rf.currentTerm != term {
			rf.mu.Unlock()
			return
		}

		args := AppendEntriesArgs{
			Term:     term,
			LeaderId: rf.me,
		}
		reply := AppendEntriesReply{}
		Debug(dLog1, "S%d -> S%d sending AE heartbeat T%d", rf.me, server, term)
		rf.mu.Unlock()

		ok := rf.sendAppendEntries(server, &args, &reply)

		rf.mu.Lock()
		if rf.role != Leader || rf.currentTerm != term {
			rf.mu.Unlock()
			return
		}
		if ok && reply.Term > rf.currentTerm {
			Debug(dTerm, "S%d sees higher term in AE reply from S%d: %d > %d",
				rf.me, server, reply.Term, rf.currentTerm)
			rf.becomeFollowerLocked(reply.Term)
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		time.Sleep(TimeToSleepBetweenAppendEntries)
	}
}
