package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
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

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	CallIsDone()

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

func CallIsDone() {
	args := GetTaskArgs{}
	reply := GetTaskReply{}

	ok := call("Coordinator.IsDone", &args, &reply)
	if ok {
		fmt.Printf("Status / Task Type: %v\n", reply.Type)
	} else {
		fmt.Printf("Call Failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
