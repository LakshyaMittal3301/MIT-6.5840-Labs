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
		rf.becomeFollowerLocked(args.Term)
	}
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.lastHeard = time.Now()
	} else {
		reply.VoteGranted = false
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
		shouldStartElection := (rf.role != Leader) && (time.Since(rf.lastHeard) > rf.electionTimeout)
		var term int
		if shouldStartElection {
			term = rf.becomeCandidateLocked()
		}
		rf.mu.Unlock()

		if shouldStartElection {
			go rf.startElection(term)
		}
	}
}

func (rf *Raft) startElection(term int) {
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
		rf.mu.Unlock()

		ok := rf.sendRequestVote(server, &args, &reply)
		if !ok {
			time.Sleep(TimeToRetryRequestVotes)
			continue
		}

		rf.mu.Lock()
		if rf.role != Candidate || rf.currentTerm != term {
			rf.mu.Unlock()
			return
		}

		if rf.currentTerm < reply.Term {
			rf.becomeFollowerLocked(reply.Term)
		} else if reply.VoteGranted {
			rf.votesReceived += 1
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
