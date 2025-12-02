package raft

import "time"

func (rf *Raft) becomeCandidateLocked() int {
	rf.role = Candidate
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.votesReceived = 1
	rf.lastHeard = time.Now()
	rf.electionTimeout = getRandomElectionTimeout()
	return rf.currentTerm
}

func (rf *Raft) becomeFollowerLocked(term int) {
	rf.currentTerm = term
	rf.role = Follower
	rf.votedFor = -1
	rf.votesReceived = 0
}

func (rf *Raft) becomeLeaderLocked() int {
	rf.role = Leader
	rf.votesReceived = 0
	return rf.currentTerm
}
