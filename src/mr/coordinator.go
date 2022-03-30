package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "errors"
import "sync"
import "time"
import "strconv"
import "strings"


type Coordinator struct {
	fileTasks map[string]Task
	reduceTasks int
}

type Task struct {
	id int
	expectedMapFunctionDoneFileName string
	progressing bool
	done bool
}

func (c *Coordinator) HandleWorkerRequest(workerRequest *WorkerRequest, coordinatorResponse *CoordinatorResponse) error {
	var mutex sync.Mutex
	mutex.Lock()
	defer mutex.Unlock()
	coordinatorResponse.ReduceTasks = c.reduceTasks
	potentialWorkRemaining := false
	for fileName, task := range c.fileTasks {
		if task.done == false && task.progressing == false {
			task.progressing = true
			c.fileTasks[fileName] = task
			coordinatorResponse.FileNamesToProcess = []string{fileName}
			coordinatorResponse.Task = c.fileTasks[fileName].id
			coordinatorResponse.ExpectedMapDoneFileName =c.fileTasks[fileName].expectedMapFunctionDoneFileName
			go monitorTask(c.fileTasks[fileName])
			return nil;
		} else if task.done == false && task.progressing == true {
			potentialWorkRemaining = true
		}
	}
	coordinatorResponse.PotentialWorkRemaining = potentialWorkRemaining
	return errors.New("No work remaining")
}

func monitorTask(task Task){
	var mutex sync.Mutex	
	for i:=0; i <10; i++ {
		time.Sleep(time.Second)
		file, err := os.Open(task.expectedMapFunctionDoneFileName)
		defer file.Close()
		if err != nil {
			log.Printf("cannot open %v, task may not be complete yet", task.expectedMapFunctionDoneFileName)
		} else {
			mutex.Lock()
			defer mutex.Unlock()
			task.done = true
			return	
		} 
	}
	mutex.Lock()
	defer mutex.Unlock()
	task.progressing = false
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
		if fileTask.done == false {
			return false;
		}
	}
	return true;
}

// main/mrcoordinator.go calls this function.
func MakeCoordinator(files []string, reduceTasks int) *Coordinator {
	c := Coordinator{fileTasks: make(map[string]Task), reduceTasks: reduceTasks}
	
	for index, filename := range files{
		var sb strings.Builder
		sb.WriteString("map-function-task-")
		sb.WriteString(strconv.Itoa(index))
		sb.WriteString("-done")
		fileName:= sb.String()
		c.fileTasks[filename]= Task{id: index, done: false, expectedMapFunctionDoneFileName: fileName}
	}

	c.server()
	return &c
}
