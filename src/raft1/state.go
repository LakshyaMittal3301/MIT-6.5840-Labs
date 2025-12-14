package raft

import "time"

func (rf *Raft) becomeCandidateLocked() int {
	prevRole := rf.role
	prevTerm := rf.currentTerm

	rf.role = Candidate
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.votesReceived = 1
	rf.lastHeard = time.Now()
	rf.electionTimeout = getRandomElectionTimeout()

	Debug(dTerm, "S%d -> CANDIDATE T%d (from %v, T%d)", rf.me, rf.currentTerm, prevRole, prevTerm)

	return rf.currentTerm
}

func (rf *Raft) becomeFollowerLocked(term int) {
	prevRole := rf.role
	prevTerm := rf.currentTerm

	rf.currentTerm = term
	rf.role = Follower
	rf.votedFor = -1
	rf.votesReceived = 0

	Debug(dTerm, "S%d -> FOLLOWER T%d (from %v, T%d)", rf.me, rf.currentTerm, prevRole, prevTerm)
}

func (rf *Raft) becomeLeaderLocked() int {
	prevRole := rf.role

	rf.role = Leader
	rf.votesReceived = 0
	n := len(rf.peers)
	rf.matchIndex = make([]int, n)
	rf.nextIndex = make([]int, n)
	rf.lastSent = make([]time.Time, n)

	lastIndex := len(rf.log) - 1
	now := time.Now()
	for idx := range n {
		rf.nextIndex[idx] = lastIndex + 1
		rf.lastSent[idx] = now
	}
	Debug(dLeader, "S%d -> LEADER T%d (from %v)", rf.me, rf.currentTerm, prevRole)

	return rf.currentTerm
}
