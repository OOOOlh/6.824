package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

var workerNum = 4

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32()&0x7fffffff) % workerNum
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// declare an argument structure.
	args := Args{}

	// fill in the argument(s).
	// args.X = 99

	// declare a reply structure.
	reply := Reply{}

	for {
		// send the RPC request, wait for the reply.
		done, err := CallServer(mapf, reducef, &args, &reply)
		if done {
			break
		}
		if err != nil {
			log.Fatalf("get fatal")
		}
	}
}

//
// the RPC argument and reply types are defined in rpc.go.
//
func CallServer(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string, args *Args, reply *Reply) (bool, error) {

	// send the RPC request, wait for the reply.
	call("Master.Assign", &args, &reply)

	//map operation has not been finished
	if reply == nil {
		time.Sleep(time.Second)
		return false, nil
	}

	if reply.done {
		return true, nil
	}

	task := reply.task
	filename := reply.fromPath
	if task == 0 {
		//map
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))
		sort.Sort(ByKey(kva))

		newFile, err := os.Create(reply.toPath)
		if err != nil {
			fmt.Println("file create fail")
			args.success = false
			return false, err
		}

		enc := json.NewEncoder(newFile)
		i := 0
		for i < len(kva) {
			j := i + 1
			for j < len(kva) && kva[j].Key == kva[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, kva[k].Value)
			}
			output := reducef(kva[i].Key, values)
			kva[i].Value = output
			err := enc.Encode(&kva[i])
			if err == nil {
				log.Fatalf("map kv error %v %v %v", filename, kva[i].Key, kva[i].Value)
			}

			i = j
		}
		newFile.Close()

		reply = &Reply{}
		args.success = true
		args.file = reply.toPath
		args.task = 0
		return false, err
	} else {
		// reduce
		workIndex := reply.reduceWorkerIndex
		intermediate := []KeyValue{}
		for _, filename := range reply.fromReducePath {
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()
			// function to detect word separators.
			ff := func(r rune) bool { return r == '\n' }

			// split contents into an array of words.
			words := strings.FieldsFunc(string(content), ff)
			for _, w := range words {
				kv := KeyValue{}
				s := strings.Split(w, " ")
				if ihash(s[0]) == workIndex {
					kv.Key = s[0]
					kv.Value = s[1]
					intermediate = append(intermediate, kv)
				}
			}
		}

		sort.Sort(ByKey(intermediate))

		newFile, err := os.Create(reply.toPath)
		if err != nil {
			fmt.Println("file create fail")
			args.success = false

			return false, err
		}
		i := 0
		for i < len(intermediate) {
			j := i + 1
			for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, intermediate[k].Value)
			}
			output := reducef(intermediate[i].Key, values)

			// this is the correct format for each line of Reduce output.
			fmt.Fprintf(newFile, "%v %v\n", intermediate[i].Key, output)
			i = j
		}
		newFile.Close()
		reply = &Reply{}
		args.success = true
		args.task = 1
		return false, err
	}
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
