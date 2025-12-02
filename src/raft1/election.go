package raft

import "time"

type RequestVoteArgs struct {
	Term        int
	CandidateId int
	// LastLogIndex int
	// LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		Debug(dTerm, "S%d sees higher term in RV from S%d: %d > %d",
			rf.me, args.CandidateId, args.Term, rf.currentTerm)
		rf.becomeFollowerLocked(args.Term)
	}
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		Debug(dVote, "S%d rejects RV from S%d (stale term %d < %d)",
			rf.me, args.CandidateId, args.Term, rf.currentTerm)
		reply.VoteGranted = false
		return
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.lastHeard = time.Now()
		Debug(dVote, "S%d grants RV to S%d at T%d",
			rf.me, args.CandidateId, args.Term)
	} else {
		reply.VoteGranted = false
		Debug(dVote, "S%d rejects RV from S%d (already voted for %d) at T%d",
			rf.me, args.CandidateId, rf.votedFor, args.Term)
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		time.Sleep(TimeToSleepBetweenChecks)
		rf.mu.Lock()
		shouldStartElection := time.Since(rf.lastHeard) > rf.electionTimeout
		var term int
		if shouldStartElection {
			term = rf.becomeCandidateLocked()
			Debug(dTimer, "S%d election timeout; starting election T%d", rf.me, term)
		}
		rf.mu.Unlock()

		if shouldStartElection {
			go rf.startElection(term)
		}
	}
}

func (rf *Raft) startElection(term int) {
	Debug(dVote, "S%d starting election T%d", rf.me, term)

	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		go rf.startRequestVotes(server, term)
	}
}

func (rf *Raft) startRequestVotes(server int, term int) {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.role != Candidate || rf.currentTerm != term {
			rf.mu.Unlock()
			return
		}

		args := RequestVoteArgs{
			Term:        term,
			CandidateId: rf.me,
		}
		reply := RequestVoteReply{}
		Debug(dVote, "S%d -> S%d sending RequestVote T%d", rf.me, server, term)

		rf.mu.Unlock()

		ok := rf.sendRequestVote(server, &args, &reply)
		if !ok {
			Debug(dDrop, "S%d -> S%d RequestVote lost/dropped T%d", rf.me, server, term)
			time.Sleep(TimeToRetryRequestVotes)
			continue
		}

		rf.mu.Lock()
		if rf.role != Candidate || rf.currentTerm != term {
			rf.mu.Unlock()
			return
		}

		if rf.currentTerm < reply.Term {
			Debug(dTerm, "S%d sees higher term in RV reply from S%d: %d > %d",
				rf.me, server, reply.Term, rf.currentTerm)
			rf.becomeFollowerLocked(reply.Term)
		} else if reply.VoteGranted {
			rf.votesReceived += 1
			Debug(dVote, "S%d got vote from S%d at T%d (votes=%d)",
				rf.me, server, term, rf.votesReceived)
			if rf.votesReceived > len(rf.peers)/2 {
				term := rf.becomeLeaderLocked()
				rf.mu.Unlock()
				go rf.startLogReplication(term)
				return
			}
		}
		rf.mu.Unlock()
		return
	}
}
