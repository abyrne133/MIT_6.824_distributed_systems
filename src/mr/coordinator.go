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
import "regexp"

type Coordinator struct {
	mu sync.RWMutex
	mapTasks map[string]Task
	reduceTasks map[string]Task
	mappingFinished bool
	reducingFinished bool
	reduceTasksCount int
	reduceFileNamesToProcess map[int][]string
}

type Task struct {
	id int
	expectedDoneFileName string
	progressing bool
	done bool
}

func(c *Coordinator) HandleWorkerDoneRequest(workerRequest *WorkerRequest, coordinatorResponse *CoordinatorResponse) error {
		
	for i:=0; i < c.reduceTasksCount; i++ {
		var sb strings.Builder
		sb.WriteString("^mr-")
		sb.WriteString(strconv.Itoa(workerRequest.TaskNumber))
		sb.WriteString("-")
		sb.WriteString(strconv.Itoa(i))
		newReduceFiles := filter(workerRequest.CompletedIntermediateFiles, func(filename string) bool {
			matched, err := regexp.MatchString(sb.String(), filename)
			if err != nil {
				log.Println("Error matching files for reduce task", strconv.Itoa(i))
				return false
			}
			return matched

		})
		if len(newReduceFiles)>0 {
			c.mu.Lock()
			c.reduceFileNamesToProcess[i] = append(c.reduceFileNamesToProcess[i], newReduceFiles...)
			c.mu.Unlock()
		}
	}

	c.markTaskAsDone(workerRequest.CompletedInputFile, true)
	return nil
}

func (c *Coordinator) HandleWorkerRequest(workerRequest *WorkerRequest, coordinatorResponse *CoordinatorResponse) error {
	c.mu.RLock()
	coordinatorResponse.ReduceTasks = c.reduceTasksCount
	c.mu.RUnlock()
	mappingTasksStillInProgress := false
	if c.isMappingFinished() == false {
		for fileName, task := range c.mapTasks {
			if task.done == false && task.progressing == false {	
				coordinatorResponse.IsMapTask = true
				coordinatorResponse.MapFileNameToProcess = fileName
				coordinatorResponse.Wait = false
				coordinatorResponse.TaskNumber = task.id
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
	} else {
		log.Println("Mapping finished")
		c.mu.Lock()
		c.mappingFinished = true
		c.mu.Unlock()
	}
	

	reduceTasksStillInProgress := false
	if c.Done() == false {
		for fileName, task := range c.reduceTasks {
			if task.done == false && task.progressing == false {
				coordinatorResponse.IsMapTask = false
				coordinatorResponse.Wait = false
				coordinatorResponse.TaskNumber = task.id
				coordinatorResponse.ReduceFilesToProcess = c.getReduceFilesToProcess(task.id)
				coordinatorResponse.ExpectedDoneFileName = task.expectedDoneFileName
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
	} else {
		log.Println("Reducing finished")
		c.mu.Lock()
		c.reducingFinished = true
		c.mu.Unlock()
		return errors.New("No work remaining")
	}
}

func (c *Coordinator) monitorTask(fileName string, isMapTask bool){
	c.markTaskAsProgressing(fileName, isMapTask)
	
	for i:=0; i <10; i++ {
		time.Sleep(1000 * time.Millisecond)
		if isMapTask == true {
			task := c.getMapTask(fileName)
			if task.done == true {
				return
			}
		} else {
			task := c.getReduceTask(fileName)
			file, err := os.Open(task.expectedDoneFileName)
			file.Close()
			if err == nil {
				c.markTaskAsDone(fileName, false)
				return	
			} 
		}
	}

	c.markTaskAsNotProgressing(fileName, isMapTask)
}

func (c *Coordinator) isMappingFinished() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.mappingFinished
}

func (c *Coordinator) getReduceFilesToProcess(taskId int) []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.reduceFileNamesToProcess[taskId]
}

func (c *Coordinator) markTaskAsDone(fileName string, isMapTask bool){
	if isMapTask {
		task := c.getMapTask(fileName)
		task.done = true
		c.setMapTask(fileName, task)
	} else {
		task := c.getReduceTask(fileName)
		task.done = true
		c.setReduceTask(fileName, task)
	}
}

func (c *Coordinator) markTaskAsProgressing(fileName string, isMapTask bool) Task{
	if isMapTask {
		task := c.getMapTask(fileName)
		task.progressing = true
		c.setMapTask(fileName, task)
		return task
	} else {
		task := c.getReduceTask(fileName)
		task.progressing = true
		c.setReduceTask(fileName, task)
		return task
	}
}

func (c *Coordinator) markTaskAsNotProgressing(fileName string, isMapTask bool) Task{
	if isMapTask {
		task := c.getMapTask(fileName)
		task.progressing = false
		c.setMapTask(fileName, task)
		return task
	} else {
		task := c.getReduceTask(fileName)
		task.progressing = false
		c.setReduceTask(fileName, task)
		return task
	}
}

func (c *Coordinator) getMapTask(fileName string) Task {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.mapTasks[fileName]
}

func (c *Coordinator) setMapTask(fileName string, task Task) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.mapTasks[fileName] = task
}

func (c *Coordinator) getReduceTask(fileName string) Task {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.reduceTasks[fileName]
}

func (c *Coordinator) setReduceTask(fileName string, task Task) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reduceTasks[fileName] = task
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
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.reducingFinished
}

// main/mrcoordinator.go calls this function.
func MakeCoordinator(files []string, reduceTasks int) *Coordinator {
	c := Coordinator{mapTasks: make(map[string]Task), reduceTasks: make(map[string]Task), reduceTasksCount: reduceTasks, reduceFileNamesToProcess: make(map[int][]string)}
	
	for index, filename := range files{
		c.mapTasks[filename]= Task{id: index, done: false}
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

func filter(arr []string, cond func(string) bool) []string {
	result := []string{}
	for i := range arr {
	  if cond(arr[i]) {
		result = append(result, arr[i])
	  }
	}
	return result
 }

