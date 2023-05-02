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
	log.Println("Master-: receive worker response")
	// log.Println("Master: recieved args ", args)
	//fail
	if !args.Success {
		fmt.Println("Master-: task", args.Task, args.File, "fail")
		return nil
	}

	//success
	if args.Success && args.Task == 0 && args.Processing {
		m.mu.Lock()
		m.reduceFiles = append(m.reduceFiles, args.File)
		m.mu.Unlock()
		//stop the goroutine
		m.mapChan[args.Index] <- 1
		log.Printf("Master-: ready to stop the map gouroutine %d", args.Index)
		return nil
	} else if args.Success && args.Task == 1 && args.Processing {
		m.mu.Lock()
		m.finished++
		m.mu.Unlock()
		m.reduceChan[args.Index] <- 1
		log.Printf("Master-: ready to stop the reduce gouroutine %d", args.Index)
	}
	log.Println(m.finished, m.nReduce)
	if m.finished == m.nReduce && args.Processing {
		m.done = true
		reply.Done = true
		return nil
	}

	return nil
}

func (m *Master) Assign(args *Args, reply *Reply) error {
	log.Println("Master-: receive task request")

	if m.mapP < len(m.mapFiles) {
		m.mu.Lock()
		if m.mapP == len(m.mapFiles) {
			reply.Exit = true
			return nil
		}
		log.Println("-----------", m.mapP)
		workerMapP := m.mapP
		m.mapP++
		m.mu.Unlock()
		reply.Task = 0
		reply.Index = workerMapP
		reply.FromPath = m.mapFiles[workerMapP]
		reply.ToPath = mapPrefix + strconv.Itoa(workerMapP)
		reply.NReduce = m.nReduce
		log.Printf("Master-: map task %d %v %v", reply.Index, reply.FromPath, reply.ToPath)
		//a timer task for every map
		go func(mapP int) {
			myTimer := time.NewTimer(time.Second * 10)
			select {
			case <-myTimer.C:
				m.mu.Lock()
				m.failedMap = append(m.failedMap, mapP)
				m.mu.Unlock()
				log.Printf("Master-: map %d fail", mapP)
				break
			case <-m.mapChan[mapP]:
				log.Printf("Master-: map %d success", mapP)
				break
			}
			myTimer.Stop()
		}(workerMapP)

	} else if len(m.failedMap) > 0 {
		reply.Task = 0
		m.mu.Lock()
		if len(m.failedMap) == 0 {
			reply.Exit = true
			return nil
		}
		failedIndex := m.failedMap[0]
		if len(m.failedMap) > 1 {
			m.failedMap = m.failedMap[1:]
		}
		m.mu.Unlock()
		reply.Index = failedIndex
		reply.FromPath = m.mapFiles[failedIndex]
		reply.ToPath = mapPrefix + strconv.Itoa(m.mapP)
		reply.NReduce = m.nReduce
		log.Printf("Master-: retry failed map task %d %v %v", reply.Index, reply.FromPath, reply.ToPath)
		//a timer task for every map
		go func(mapP int) {
			myTimer := time.NewTimer(time.Second * 10)
			select {
			case <-myTimer.C:
				m.mu.Lock()
				m.failedMap = append(m.failedMap, mapP)
				m.mu.Unlock()
				log.Printf("Master-: reduce %d fail", mapP)
				break
			case <-m.mapChan[mapP]:
				log.Printf("Master-: map %d success", mapP)
				break
			}
			myTimer.Stop()
		}(failedIndex)
	} else if len(m.reduceFiles) == len(m.mapFiles) && m.reduceWorkers < m.nReduce {
		reply.Task = 1
		m.mu.Lock()
		if m.reduceWorkers == m.nReduce {
			reply.Exit = true
			return nil
		}
		log.Println("+++++++++++++++++++++++++++++", m.reduceWorkers)
		reply.ReduceWorkerIndex = m.reduceWorkers
		m.reduceWorkers++
		m.mu.Unlock()
		reply.FromReducePath = m.reduceFiles
		reply.ToPath = reducePrefix + strconv.Itoa(reply.ReduceWorkerIndex)
		reply.NReduce = m.nReduce
		log.Printf("Master-: reduce task %v %d", reply.FromReducePath, reply.ReduceWorkerIndex)

		go func(reduceIndex int) {
			myTimer := time.NewTimer(time.Second * 10)
			select {
			case <-myTimer.C:
				m.mu.Lock()
				m.failedReduce = append(m.failedReduce, reduceIndex)
				m.mu.Unlock()
				log.Printf("Master-: reduce %d fail", reduceIndex)
				break
			case <-m.reduceChan[reduceIndex]:
				log.Printf("Master-: reduce %d success", reduceIndex)
				break
			}
			myTimer.Stop()
		}(reply.ReduceWorkerIndex)

	} else if len(m.failedReduce) > 0 {
		reply.Task = 1
		reply.FromReducePath = m.reduceFiles
		m.mu.Lock()
		if len(m.failedReduce) > 0 {
			reply.Exit = true
			return nil
		}
		reduceIndex := m.failedReduce[0]
		if len(m.failedReduce) > 0 {
			m.failedReduce = m.failedReduce[1:]
		}
		m.mu.Unlock()
		reply.ReduceWorkerIndex = reduceIndex
		reply.ToPath = reducePrefix + strconv.Itoa(reduceIndex)
		reply.NReduce = m.nReduce
		log.Printf("Master-: reduce task %v %d", reply.FromReducePath, reply.ReduceWorkerIndex)
		go func(reduceIndex int) {
			myTimer := time.NewTimer(time.Second * 10)
			select {
			case <-myTimer.C:
				m.mu.Lock()
				m.failedReduce = append(m.failedReduce, reduceIndex)
				log.Printf("Master-: reduce %d fail", reduceIndex)
				m.mu.Unlock()
				break
			case <-m.reduceChan[reduceIndex]:
				log.Printf("Master-: reduce %d success", reduceIndex)
				break
			}

			myTimer.Stop()
		}(reduceIndex)
	} else {
		reply.Exit = true
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
	log.Printf("Master: file nums: %d", len(files))
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
