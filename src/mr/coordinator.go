package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"


type Coordinator struct {
	fileStatus map[string]bool
	reduceTasks int
}

func (c *Coordinator) HandleWorkerRequest(workerRequest *WorkerRequest, coordinatorResponse *CoordinatorResponse) error {
	coordinatorResponse.ReduceTasks = c.reduceTasks
	return nil;
}

func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// check if the entire job has finished.
func (c *Coordinator) Done() bool {

	for _, fileStatus := range c.fileStatus{
		if(fileStatus == false){
			return false;
		}
	}

	return true;
}

// main/mrcoordinator.go calls this function.
func MakeCoordinator(files []string, reduceTasks int) *Coordinator {
	c := Coordinator{fileStatus: make(map[string]bool)}
	
	c.reduceTasks = reduceTasks

	for _, filename := range files{
		c.fileStatus[filename]=false
	}

	c.server()
	return &c
}
