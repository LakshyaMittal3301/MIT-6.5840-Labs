package raft

import (
	"log"
	"os"
	"strconv"
	"time"
)

type logTopic string

const (
	dClient  logTopic = "CLNT"
	dCommit  logTopic = "CMIT"
	dDrop    logTopic = "DROP"
	dError   logTopic = "ERRO"
	dInfo    logTopic = "INFO"
	dLeader  logTopic = "LEAD"
	dLog1    logTopic = "LOG1"
	dLog2    logTopic = "LOG2"
	dPersist logTopic = "PERS"
	dSnap    logTopic = "SNAP"
	dTerm    logTopic = "TERM"
	dTest    logTopic = "TEST"
	dTimer   logTopic = "TIMR"
	dTrace   logTopic = "TRCE"
	dVote    logTopic = "VOTE"
	dWarn    logTopic = "WARN"
)

var debugVerbosity int
var debugStart time.Time

func init() {
	debugStart = time.Now()
	debugVerbosity = getVerbosity()

	// Remove date/time from Go’s default logger; we print elapsed instead.
	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func getVerbosity() int {
	v := os.Getenv("VERBOSE")
	if v == "" {
		return 0
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		log.Fatalf("Invalid VERBOSE=%q", v)
	}
	return n
}

// Debug prints a log line if VERBOSE > 0.
// Format is: "TTTTTT TOPI message..." where TTTTTT is elapsed in 0.1ms.
// This matches the format Jose’s dslogs expects: time, space, 4-char topic, space, message.
func Debug(topic logTopic, format string, a ...interface{}) {
	if debugVerbosity == 0 {
		return
	}
	elapsed := time.Since(debugStart).Microseconds() / 100 // 0.1ms units

	// "%06d %s" gives: 6-digit time, space, then "TERM " etc.
	prefixFmt := "%06d %s" + format
	args := make([]interface{}, 0, len(a)+2)
	args = append(args, elapsed, string(topic)+" ")
	args = append(args, a...)

	log.Printf(prefixFmt, args...)
}
