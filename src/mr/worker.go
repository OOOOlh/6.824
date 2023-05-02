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

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// declare an argument structure.
var args = Args{}

// declare a reply structure.
var reply = Reply{}

//
// main/mrworker.go calls this function.
//
func Worker(client *rpc.Client, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	args.Success = true
	args.Task = -1
	for {
		// send the RPC request, wait for the reply.
		done, err := CallServer(client, mapf, reducef, &args, &reply)
		if done {
			break
		}
		if err != nil {
			log.Fatalf("get fatal")
			break
		}
		//if map task has not been finishen,sleep for one second
		if &reply == nil {
			time.Sleep(time.Second)
		}
	}
}

//
// the RPC argument and reply types are defined in rpc.go.
//
func CallServer(client *rpc.Client, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string, args *Args, reply *Reply) (bool, error) {

	log.Println("args ", args)
	// send the RPC request, wait for the reply.
	result := client.Call("Master.Assign", &args, &reply)
	if result != nil {
		log.Fatal(result)
	}
	//map operation has not been finished
	if reply == nil {
		time.Sleep(time.Second)
		fmt.Println("empty reply")
		return false, nil
	}
	log.Println("reply:", reply)

	if reply.Done {
		return true, nil
	}

	task := reply.Task
	filename := reply.FromPath
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

		err = os.Remove(reply.ToPath)
		if err != nil {
			log.Println(err)
		}

		newFile, err := os.Create(reply.ToPath)
		if err != nil {
			fmt.Println("file create fail")
			args.Success = false
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
			if err != nil {
				log.Fatalf("map kv error %v %v %v", filename, kva[i].Key, kva[i].Value)
			}

			i = j
		}
		newFile.Close()

		args = &Args{}
		args.Processing = true
		args.Index = reply.Index
		args.Success = true
		args.File = reply.ToPath
		args.Task = 0
		log.Printf("map success index: %d filename: %v topath: %v", reply.Index, filename, reply.ToPath)
		log.Println("map success args: ", args)
		reply = &Reply{}
		result := client.Call("Master.Response", &args, &reply)
		if result != nil {
			log.Fatal(result)
		}
		args = &Args{}
		return false, err
	} else {
		// reduce
		workIndex := reply.ReduceWorkerIndex
		intermediate := []KeyValue{}
		for _, filename := range reply.FromReducePath {
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			dec := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				if ihash(kv.Key)%reply.NReduce == workIndex {
					intermediate = append(intermediate, kv)
				}
			}
			file.Close()
		}

		sort.Sort(ByKey(intermediate))
		os.Remove(reply.ToPath)
		newFile, err := os.Create(reply.ToPath)
		if err != nil {
			fmt.Println("file create fail")
			args.Success = false
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
		args = &Args{}
		args.Processing = true
		args.Index = reply.ReduceWorkerIndex
		args.Success = true
		args.Task = 1
		log.Printf("reduce success index: %d topath: %v", reply.ReduceWorkerIndex, reply.ToPath)
		log.Println("reduce success args: ", args)
		reply = &Reply{}
		result := client.Call("Master.Response", &args, &reply)
		if result != nil {
			log.Fatal(result)
		}
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
