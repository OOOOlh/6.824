package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
)

type Master struct {
	// Your definitions here.
	mapFiles      []string
	mapP          int
	reduceFiles   []string
	reduceWorkers int
	finished      int
	done          bool
}

var suffix int
var mapPrefix string = "maped-"
var reducePrefix string = "mr-out-"

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Assign(args *Args, reply *Reply) error {
	if args != nil && !args.success {
		fmt.Println("task", args.task, args.file, "fail")
		return nil
	}

	if args != nil && args.success && args.task == 0 {
		m.reduceFiles = append(m.reduceFiles, args.file)
	} else if args != nil && args.success && args.task == 1 {
		m.finished++
	}
	args = &Args{}

	if m.finished != 0 && m.finished == workerNum {
		m.done = true
		reply.done = true
		return nil
	}

	if m.mapP < len(m.mapFiles) {
		reply.task = 0
		reply.fromPath = m.mapFiles[m.mapP]
		reply.toPath = mapPrefix + strconv.Itoa(suffix)
		m.mapP++
	} else if len(m.reduceFiles) == len(m.mapFiles) && m.reduceWorkers < workerNum {
		reply.task = 1
		reply.fromReducePath = m.reduceFiles
		reply.reduceWorkerIndex = m.reduceWorkers
		m.reduceWorkers++
	}

	// reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	// Your code here.
	return m.done
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.mapFiles = files

	m.server()
	return &m
}
