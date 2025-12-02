package raft

import "log"

// Debugging
const DEBUG = false

func DPrintf(format string, a ...interface{}) {
	if DEBUG {
		log.Printf(format, a...)
	}
}
