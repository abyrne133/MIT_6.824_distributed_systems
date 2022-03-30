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
	mapTasks map[string]Task
	reduceTasks map[string]Task
	mappingFinished bool
	reducingFinished bool
	reduceTasksCount int
}

type Task struct {
	id int
	expectedDoneFileName string
	progressing bool
	done bool
}

func (c *Coordinator) HandleWorkerRequest(workerRequest *WorkerRequest, coordinatorResponse *CoordinatorResponse) error {
	
	coordinatorResponse.ReduceTasks = c.reduceTasksCount
	mappingTasksStillInProgress := false
	if c.mappingFinished == false {
		for fileName, task := range c.mapTasks {
			if task.done == false && task.progressing == false {	
				coordinatorResponse.IsMapTask = true
				coordinatorResponse.FileNamesToProcess = []string{fileName}
				coordinatorResponse.Wait = false
				coordinatorResponse.TaskNumber = c.mapTasks[fileName].id
				coordinatorResponse.ExpectedDoneFileName =c.mapTasks[fileName].expectedDoneFileName
				go c.monitorTask(fileName, true)
				return nil
			} else if task.done == false && task.progressing == true {
				mappingTasksStillInProgress = true
			}
		}
	}
	
	if mappingTasksStillInProgress == true {
		coordinatorResponse.Wait = true
		return nil
	}
	
	log.Println("Mapping finished")
	c.mappingFinished = true

	reduceTasksStillInProgress := false
	if c.reducingFinished == false {
		for fileName, task := range c.reduceTasks {
			if task.done == false && task.progressing == false {
				coordinatorResponse.IsMapTask = false
				coordinatorResponse.TaskNumber = c.reduceTasks[fileName].id
				coordinatorResponse.Wait = false
				coordinatorResponse.ExpectedDoneFileName =c.reduceTasks[fileName].expectedDoneFileName
				go c.monitorTask(fileName, false)
				return nil;
			} else if task.done == false && task.progressing == true {
				reduceTasksStillInProgress = true
			}
		}
	}

	if reduceTasksStillInProgress == true {
		coordinatorResponse.Wait = true
		return nil
	}

	log.Println("Reducing finished")
	c.reducingFinished = true
	return errors.New("No work remaining")
}

func (c *Coordinator) monitorTask(fileName string, isMapTask bool){
	var task Task
	if isMapTask {
		task = c.mapTasks[fileName]
	} else {
		task = c.reduceTasks[fileName]
	} 
	
	task.progressing = true
	c.updateTask(fileName, task, isMapTask)

	for i:=0; i <10; i++ {
		time.Sleep(time.Second)
		file, err := os.Open(task.expectedDoneFileName)
		defer file.Close()
		if err == nil {
			task.done = true
			c.updateTask(fileName, task, isMapTask)
			return	
		} 
	}

	task.progressing = false
	c.updateTask(fileName, task, isMapTask)
}

func (c *Coordinator) updateTask(fileName string, task Task, isMapTask bool){
	var mutex sync.Mutex
	mutex.Lock()
	if isMapTask {
		c.mapTasks[fileName] = task
	} else {
		c.reduceTasks[fileName] = task
	}
	mutex.Unlock()
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
	return c.reducingFinished
}

// main/mrcoordinator.go calls this function.
func MakeCoordinator(files []string, reduceTasks int) *Coordinator {
	c := Coordinator{mapTasks: make(map[string]Task), reduceTasks: make(map[string]Task), reduceTasksCount: reduceTasks}
	
	for index, filename := range files{
		var sbMap strings.Builder
		sbMap.WriteString("map-function-task-")
		sbMap.WriteString(strconv.Itoa(index))
		sbMap.WriteString("-done")
		expectedDoneMapFileName:= sbMap.String()
		c.mapTasks[filename]= Task{id: index, done: false, expectedDoneFileName: expectedDoneMapFileName}
	}

	for i:=0; i	< reduceTasks; i++ {
		var sbReduce strings.Builder
		sbReduce.WriteString("mr-out-")
		sbReduce.WriteString(strconv.Itoa(i))
		expectedDoneReduceFileName:= sbReduce.String()
		c.reduceTasks[expectedDoneReduceFileName]= Task{id: i, done: false, expectedDoneFileName: expectedDoneReduceFileName}
	}

	c.server()
	return &c
}
