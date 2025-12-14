package raft

import "time"

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term            int
	Success         bool
	ConflictingTerm int
	FirstIndex      int
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

	rf.lastHeard = time.Now()
	Debug(dTimer, "S%d got AE from S%d at T%d",
		rf.me, args.LeaderId, args.Term)

	lastLogIndex := len(rf.log) - 1
	if lastLogIndex < args.PrevLogIndex {
		reply.Success = false
		reply.FirstIndex = lastLogIndex
		reply.ConflictingTerm = rf.log[lastLogIndex].term
		return
	}

	if rf.log[args.PrevLogIndex].term != args.PrevLogTerm {
		conflictingTerm := rf.log[args.PrevLogIndex].term
		currIdx := args.PrevLogIndex
		for currIdx-1 >= 0 && rf.log[currIdx-1].term == conflictingTerm {
			currIdx -= 1
		}
		reply.Success = false
		reply.FirstIndex = currIdx
		reply.ConflictingTerm = conflictingTerm
		return
	}

	reply.Success = true
	i := 0
	idx := args.PrevLogIndex + i + 1
	for idx < len(rf.log) && i < len(args.Entries) && rf.log[idx].term == args.Entries[i].term {
		i++
		idx++
	}
	rf.log = rf.log[:idx]
	rf.log = append(rf.log, args.Entries[i:]...)
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
		for !rf.killed() {
			rf.mu.Lock()
			if rf.role != Leader || rf.currentTerm != term {
				rf.mu.Unlock()
				return
			}
			lastLogIndex := len(rf.log) - 1
			haveEntries := rf.nextIndex[server] <= lastLogIndex
			heartbeatDue := time.Since(rf.lastSent[server]) >= TimeToHeartBeat
			if haveEntries || heartbeatDue {
				rf.mu.Unlock()
				break
			}
			rf.mu.Unlock()
			time.Sleep(TimeToSleepBetweenChecks)
		}

		rf.mu.Lock()
		if rf.role != Leader || rf.currentTerm != term {
			rf.mu.Unlock()
			return
		}
		prevLogIndex := rf.nextIndex[server] - 1
		prevLogTerm := rf.log[prevLogIndex].term

		// Sending entire log suffic
		// TODO: Cap with K entries later
		src := rf.log[prevLogIndex+1:]
		entries := make([]LogEntry, len(src))
		copy(entries, src)

		args := AppendEntriesArgs{
			Term:         term,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: rf.commitIndex,
		}
		reply := AppendEntriesReply{}
		Debug(dLog1, "S%d -> S%d sending AE T%d", rf.me, server, term)

		rf.mu.Unlock()

		ok := rf.sendAppendEntries(server, &args, &reply)

		rf.mu.Lock()
		if rf.role != Leader || rf.currentTerm != term {
			rf.mu.Unlock()
			return
		}
		if !ok {
			rf.mu.Unlock()
			continue
		}

		rf.lastSent[server] = time.Now()
		if reply.Term > rf.currentTerm {
			Debug(dTerm, "S%d sees higher term in AE reply from S%d: %d > %d",
				rf.me, server, reply.Term, rf.currentTerm)
			rf.becomeFollowerLocked(reply.Term)
			rf.mu.Unlock()
			return
		}

		rf.mu.Unlock()
	}
}
