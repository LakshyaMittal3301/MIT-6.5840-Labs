package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

// Init Phase
// len(files) is the number of map tasks
// nReduce is numnber of reduce tasks
// Job has phases: Map, Reduce, Done
// Workers will constantly ask for tasks -> {Map, Reduce, Idle, Exit}

// Task -> Map
// 1. Assign worker a map task, along with a file, and the id (which could just be the file number (0 -> n-1))
// 2,

// Task -> Map
// 1. Worker will ask for a task
// 2. Assign a map task -> By sending file locations, task type, etc.
// 3. Wait for the worker to inform whether the task is done or not, along with the location of intermediate file.
// 4. If 10 seconds pass, assume worker is dead, and assign the task to someone else.
// 5. Once all Map tasks are done

// Task -> Reduce
// 1. Worker will ask for a task
// 2. Assign a map task -> By sending file locations, task type, etc.
// 3. Wait for the worker to inform whether the task is done or not.
// 4. If 10 seconds pass, assume worker is dead, and assign the task to someone else.
// 5. Once all reduce tasks are done, mark the complete job done. And can return true from Done().

type Phase string

const (
	PhaseMap Phase = "Map"
	PhaseReduce Phase = "Reduce"
	PhaseDone Phase = "Done"
)

type Coordinator struct {
	Files []string
	NMap int
	NReduce int
	CurrentPhase Phase
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) IsDone(args *GetTaskArgs, reply *GetTaskReply) error {
	reply.Type = TaskTypeIdle
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	return c.CurrentPhase == PhaseDone
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files, len(files), nReduce, PhaseMap,
	}

	c.server()
	return &c
}
