package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "errors"
import "sync"


type Coordinator struct {
	fileTasks map[string]Task
	reduceTasks int
}

type Task struct {
	id int
	done bool
}

func (c *Coordinator) HandleWorkerRequest(workerRequest *WorkerRequest, coordinatorResponse *CoordinatorResponse) error {
	var mutex sync.Mutex
	mutex.Lock()
	defer mutex.Unlock()
	coordinatorResponse.ReduceTasks = c.reduceTasks
	for fileName, task := range c.fileTasks {
		if task.done == false {
			coordinatorResponse.FileNamesToProcess = []string{fileName}
			coordinatorResponse.Task = c.fileTasks[fileName].id
			return nil;
		}
	}
	return errors.New("No work remaining")
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

	for _, fileTask := range c.fileTasks{
		if(fileTask.done == false){
			return false;
		}
	}

	return true;
}

// main/mrcoordinator.go calls this function.
func MakeCoordinator(files []string, reduceTasks int) *Coordinator {
	c := Coordinator{fileTasks: make(map[string]Task), reduceTasks: reduceTasks}
	
	for index, filename := range files{
		c.fileTasks[filename]= Task{id: index, done: false}
	}

	c.server()
	return &c
}
