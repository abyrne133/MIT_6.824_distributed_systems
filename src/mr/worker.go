package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "time"

// Map functions return a slice of KeyValue.
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

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
		workerRequest := WorkerRequest{WorkerId: time.Now().Format(time.RFC850)}
		coordinatorResponse := CoordinatorResponse{}
			
		ok := call("Coordinator.HandleWorkerRequest", &workerRequest, &coordinatorResponse)
		if ok {
			fmt.Println("Coordinator Response", coordinatorResponse.ReduceTasks)
		} else {
			fmt.Printf("call failed!\n")
		}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
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
