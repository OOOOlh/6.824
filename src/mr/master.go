package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"strconv"
	"sync"
	"time"
)

type Master struct {
	// Your definitions here.
	mapFiles      []string
	mapP          int
	reduceFiles   []string
	nReduce       int
	reduceWorkers int
	finished      int
	done          bool
	mapChan       []chan int
	reduceChan    []chan int
	failedMap     []int
	failedReduce  []int
	mu            sync.Mutex
}

var mapPrefix string = "maped-"
var reducePrefix string = "mr-out-"

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//

func (m *Master) Response(args *Args, reply *Reply) error {
	log.Println("receive worker response")
	log.Println("recieved args ", args)
	//fail
	if !args.Success {
		fmt.Println("task", args.Task, args.File, "fail")
		return nil
	}
	if m.finished != 0 && m.finished == m.nReduce && args.Processing {
		m.done = true
		reply.Done = true
		return nil
	}

	//success
	if args.Success && args.Task == 0 && args.Processing {
		m.reduceFiles = append(m.reduceFiles, args.File)
		//stop the goroutine
		m.mapChan[args.Index] <- 1
		log.Printf("stop the map gouroutine %d", args.Index)
		return nil
	} else if args.Success && args.Task == 1 && args.Processing {
		m.finished++
		m.reduceChan[args.Index] <- 1
		log.Printf("stop the reduce gouroutine %d", args.Index)
		return nil
	}

	return nil
}

func (m *Master) Assign(args *Args, reply *Reply) error {
	log.Println("receive request")

	if m.mapP < len(m.mapFiles) {
		reply.Task = 0
		reply.Index = m.mapP
		reply.FromPath = m.mapFiles[m.mapP]
		reply.ToPath = mapPrefix + strconv.Itoa(m.mapP)
		reply.NReduce = m.nReduce
		log.Printf("map task %d %v %v", reply.Index, reply.FromPath, reply.ToPath)
		//a timer task for every map
		go func(mapP int) {
			myTimer := time.NewTimer(time.Second * 10)

			select {
			case <-myTimer.C:
				m.mu.Lock()
				m.failedMap = append(m.failedMap, mapP)
				m.mu.Unlock()
				log.Printf("map %d fail", mapP)
				break
			case <-m.mapChan[mapP]:
				log.Printf("map %d success", mapP)
				break
			}
			myTimer.Stop()
		}(m.mapP)
		m.mapP++
	} else if len(m.failedMap) > 0 {
		reply.Task = 0
		reply.Index = m.mapP
		m.mu.Lock()
		failedIndex := m.failedMap[0]
		if len(m.failedMap) > 1 {
			m.failedMap = m.failedMap[1:]
		}
		m.mu.Unlock()
		reply.FromPath = m.mapFiles[failedIndex]
		reply.ToPath = mapPrefix + strconv.Itoa(m.mapP)
		reply.NReduce = m.nReduce
		log.Printf("retry failed map task %d %v %v", reply.Index, reply.FromPath, reply.ToPath)
		//a timer task for every map
		go func(mapP int) {
			myTimer := time.NewTimer(time.Second * 10)

			select {
			case <-myTimer.C:
				m.mu.Lock()
				m.failedMap = append(m.failedMap, mapP)
				m.mu.Unlock()
				break
			case <-m.mapChan[mapP]:
				log.Printf("map %d success", mapP)
				break
			}
			myTimer.Stop()
		}(failedIndex)
	} else if len(m.reduceFiles) == len(m.mapFiles) && m.reduceWorkers < m.nReduce {
		reply.Task = 1
		reply.FromReducePath = m.reduceFiles
		reply.ReduceWorkerIndex = m.reduceWorkers
		reply.ToPath = reducePrefix + strconv.Itoa(m.reduceWorkers)
		reply.NReduce = m.nReduce
		log.Printf("reduce task %v %d", reply.FromReducePath, reply.ReduceWorkerIndex)

		go func(reduceIndex int) {
			myTimer := time.NewTimer(time.Second * 10)

			select {
			case <-myTimer.C:
				m.mu.Lock()
				m.failedReduce = append(m.failedReduce, reduceIndex)
				m.mu.Unlock()
				break
			case <-m.reduceChan[reduceIndex]:
				log.Printf("reduce %d success", reduceIndex)
				break
			}
			myTimer.Stop()
		}(m.reduceWorkers)
		m.reduceWorkers++
	} else if len(m.failedReduce) > 0 {
		reply.Task = 1
		reply.FromReducePath = m.reduceFiles
		m.mu.Lock()
		reduceIndex := m.failedReduce[0]
		if len(m.failedReduce) > 0 {
			m.failedReduce = m.failedReduce[1:]
		}
		m.mu.Unlock()
		reply.ReduceWorkerIndex = reduceIndex
		reply.ToPath = reducePrefix + strconv.Itoa(reduceIndex)
		reply.NReduce = m.nReduce
		log.Printf("reduce task %v %d", reply.FromReducePath, reply.ReduceWorkerIndex)
		go func(reduceIndex int) {
			myTimer := time.NewTimer(time.Second * 10)
			select {
			case <-myTimer.C:
				m.mu.Lock()
				m.failedReduce = append(m.failedReduce, reduceIndex)
				m.mu.Unlock()
				break
			case <-m.reduceChan[reduceIndex]:
				log.Printf("reduce %d success", reduceIndex)
				break
			}

			myTimer.Stop()
		}(reduceIndex)
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":8888")
	// sockname := masterSock()
	// os.Remove(sockname)
	// l, e := net.Listen("unix", sockname)
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
	log.Printf("file nums: %d", len(files))
	m.mapChan = make([]chan int, len(files))
	for i := 0; i < len(files); i++ {
		m.mapChan[i] = make(chan int, 1)
	}
	m.reduceChan = make([]chan int, nReduce)
	for i := 0; i < nReduce; i++ {
		m.reduceChan[i] = make(chan int, 1)
	}
	m.nReduce = nReduce

	m.server()
	return &m
}
