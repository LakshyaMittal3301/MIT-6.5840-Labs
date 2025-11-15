package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"time"
)

// 1. Poll the coordinator for tasks.

// Map task:
// 2. You'll get location of original filename
// 3. Pass filename and content to map function -> []KeyValue
// 4. For each pair, hash the key and write the key value pair to a file tmp-taskId-hash
// 5. Once all writing is done. Change filenames in an atomic operation.
// 6. Report Coordinator that you are done, along with intermediate file names.

// Reduce task:
// 2. You'll get the location of all intermediate files.
// 3. Load all files to a slice of key value pairs.
// 4. Sort the slice
// 5. Iterate to find all pairs for a single key, and call the reduce function.
// 6. Write to a temporary output file, and change its name once writing is done.
// 7. Report to coordinator that task is done.

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {

		reply, ok := pollGetTask()

		if !ok {
			log.Printf("worker: could not reach coordinator, exiting\n")
			return
		}
		if reply.Type == TaskTypeExit {
			log.Printf("worker: got exit task, exiting\n")
			return
		}

		err := handleTask(reply, mapf, reducef)
		if err != nil {
			log.Printf("worker: error occured while handling task: %v\n", err)
			// time.Sleep(time.Second * 2)
			return
		}
	}
}

func pollGetTask() (GetTaskReply, bool) {
	args := GetTaskArgs{}
	const idleWait = time.Second

	for {
		reply, ok := callGetTask(args)
		if !ok {
			return GetTaskReply{}, ok
		}
		if reply.Type == TaskTypeIdle {
			log.Printf("worker: Idle recieved, sleeping for: %ds\n", idleWait/time.Second)
			time.Sleep(idleWait)
		} else {
			return reply, ok
		}
	}
}

func handleTask(reply GetTaskReply, mapf func(string, string) []KeyValue, reducef func(string, []string) string) error {
	switch reply.Type {
	case TaskTypeMap:
		return handleMapTask(reply.Map, mapf)
	case TaskTypeReduce:
		return handleReduceTask(reply.Reduce, reducef)
	default:
		return fmt.Errorf("worker: unexpected task type recieved: %v", reply.Type)
	}
}

func handleMapTask(taskInfo *MapTaskInfo, mapf func(string, string) []KeyValue) error {
	if taskInfo == nil {
		return fmt.Errorf("worker: no map task information found")
	}
	// TODO: Implement Map
	return nil
}

func handleReduceTask(taskInfo *ReduceTaskInfo, reducef func(string, []string) string) error {
	if taskInfo == nil {
		return fmt.Errorf("worker: no reduce task information found")
	}
	// TODO: Implement Reduce
	return nil
}

func callGetTask(args GetTaskArgs) (GetTaskReply, bool) {
	reply := GetTaskReply{}
	ok := call("Coordinator.GetTask", &args, &reply)
	return reply, ok
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
