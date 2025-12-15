package raft

import (
	"sort"
	"time"
)

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

	if args.Term == rf.currentTerm && rf.role != Follower {
		rf.role = Follower
	}

	rf.lastHeard = time.Now()
	Debug(dTimer, "S%d got AE from S%d at T%d",
		rf.me, args.LeaderId, args.Term)

	lastLogIndex := len(rf.log) - 1
	if lastLogIndex < args.PrevLogIndex {
		reply.Success = false
		reply.FirstIndex = lastLogIndex + 1
		reply.ConflictingTerm = -1
		return
	}

	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		conflictingTerm := rf.log[args.PrevLogIndex].Term
		currIdx := args.PrevLogIndex
		for currIdx-1 >= 0 && rf.log[currIdx-1].Term == conflictingTerm {
			currIdx -= 1
		}
		reply.Success = false
		reply.FirstIndex = currIdx
		reply.ConflictingTerm = conflictingTerm
		return
	}

	idx := args.PrevLogIndex + 1

	for i := range args.Entries {
		if idx+i >= len(rf.log) {
			rf.log = append(rf.log, args.Entries[i:]...)
			break
		} else if rf.log[idx+i].Term != args.Entries[i].Term {
			rf.log = rf.log[:idx+i]
			rf.log = append(rf.log, args.Entries[i:]...)
			break
		}
	}

	newCommit := min(args.LeaderCommit, len(rf.log)-1)
	if newCommit > rf.commitIndex {
		rf.commitIndex = newCommit
		rf.applyCond.Signal()
	}

	reply.Success = true
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
		prevLogTerm := rf.log[prevLogIndex].Term

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

		rf.lastSent[server] = time.Now()
		rf.mu.Unlock()

		ok := rf.sendAppendEntries(server, &args, &reply)

		rf.mu.Lock()
		if rf.role != Leader || rf.currentTerm != term {
			rf.mu.Unlock()
			return
		}
		if !ok {
			rf.mu.Unlock()
			time.Sleep(TimeToSleepBetweenChecks)
			continue
		}

		if reply.Term > rf.currentTerm {
			Debug(dTerm, "S%d sees higher term in AE reply from S%d: %d > %d",
				rf.me, server, reply.Term, rf.currentTerm)
			rf.becomeFollowerLocked(reply.Term)
			rf.mu.Unlock()
			return
		}

		if !reply.Success {
			// backoff nextIndex
			rf.backoffNextIndex(server, reply.ConflictingTerm, reply.FirstIndex)
		} else {
			// update matchindex and commitindex
			rf.matchIndex[server] = prevLogIndex + len(entries)
			rf.nextIndex[server] = rf.matchIndex[server] + 1
			rf.updateCommitIndex()
		}

		rf.mu.Unlock()
	}
}

func (rf *Raft) backoffNextIndex(server, conflictingTerm, firstIndex int) {
	if conflictingTerm == -1 {
		if firstIndex < 1 {
			rf.nextIndex[server] = 1
		} else {
			rf.nextIndex[server] = firstIndex
		}
		return
	}

	// If we have entries with the conflicting term, skip all of them.
	lastIdxWithTerm := -1
	for i := len(rf.log) - 1; i >= 0; i-- {
		if rf.log[i].Term == conflictingTerm {
			lastIdxWithTerm = i
			break
		}
		if rf.log[i].Term < conflictingTerm {
			// Since we scan backwards, once we see a smaller term we
			// know we don't have the conflicting term.
			break
		}
	}

	if lastIdxWithTerm != -1 {
		rf.nextIndex[server] = lastIdxWithTerm + 1
	} else {
		rf.nextIndex[server] = firstIndex
	}
	if rf.nextIndex[server] < 1 {
		rf.nextIndex[server] = 1
	}
}

func (rf *Raft) updateCommitIndex() {
	n := len(rf.matchIndex)
	matchIdxs := make([]int, n)
	copy(matchIdxs, rf.matchIndex)

	sort.Slice(matchIdxs, func(i, j int) bool {
		return matchIdxs[i] < matchIdxs[j]
	})

	maxMajorityIdx := matchIdxs[n/2]
	for i := rf.commitIndex + 1; i <= maxMajorityIdx; i++ {
		if rf.log[i].Term == rf.currentTerm {
			rf.commitIndex = i
			rf.applyCond.Signal()
		}
	}
}
