package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type Args struct {
	Processing bool
	Success    bool
	Task       int
	Index      int
	File       string
}

type Reply struct {
	//0 map
	//1 reduce
	Task              int
	Index             int
	FromPath          string
	ToPath            string
	FromReducePath    []string
	ReduceWorkerIndex int
	NReduce           int
	Done              bool
	Exit              bool
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
