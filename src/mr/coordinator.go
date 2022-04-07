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
	mapTasks map[int]Task
	reduceTasks map[int]Task
	mappingFinished bool
	reducingFinished bool
	mapTasksCount int
	reduceTasksCount int
}

type Task struct {
	id int
	fileNamesToProcess []string
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
			task := c.getReduceTask(i)
			task.fileNamesToProcess = append(task.fileNamesToProcess, newReduceFiles...)
			c.setReduceTask(i, task)
		}
	}

	c.markTaskAsDone(workerRequest.TaskNumber, true)
	return nil
}

func (c *Coordinator) HandleWorkerRequest(workerRequest *WorkerRequest, coordinatorResponse *CoordinatorResponse) error {
	c.mu.RLock()
	coordinatorResponse.ReduceTasks = c.reduceTasksCount
	c.mu.RUnlock()
	if c.isMappingFinished() == false {
		isTaskAssigned, err := c.assignMapTask(coordinatorResponse)
		if err != nil {
			coordinatorResponse.Wait = true
			return nil
		}
		if isTaskAssigned == true {
			return nil
		} else {
			c.mu.RLock()
			isMappingFinished := c.mappingFinished
			c.mu.RUnlock()
			if isMappingFinished == false{
				log.Println("Mapping Finished")
				c.mu.Lock()
				c.mappingFinished = true
				c.mu.Unlock()
			}
		}		
	}

	if c.Done() == false {
		isTaskAssigned, err := c.assignReduceTask(coordinatorResponse)
		if err != nil {
			coordinatorResponse.Wait = true
			return nil
		}
		if isTaskAssigned == true {
			return nil
		} else {
			c.mu.RLock()
			isReducingFinished := c.reducingFinished
			c.mu.RUnlock()
			if isReducingFinished == false {
				log.Println("Reducing Finished")
				c.mu.Lock()
				c.reducingFinished = true
				c.mu.Unlock()
			}
		}		
	}

	return errors.New("No work remaining")
}

func (c *Coordinator) assignMapTask(coordinatorResponse *CoordinatorResponse) (bool, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	mappingTasksStillInProgress := false
	for i, task := range c.mapTasks {
		if task.done == false && task.progressing == false {	
			coordinatorResponse.IsMapTask = true
			coordinatorResponse.FilesToProcess = task.fileNamesToProcess
			coordinatorResponse.Wait = false
			coordinatorResponse.TaskNumber = task.id
			go c.monitorTask(i, true)
			return true, nil
		} else if task.done == false && task.progressing == true {
			mappingTasksStillInProgress = true
		}
	}
	if mappingTasksStillInProgress == true {
		return false, errors.New("Map tasks currently unavailable")
	} else {
		return false, nil
	}
}

func (c *Coordinator) assignReduceTask(coordinatorResponse *CoordinatorResponse) (bool, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	reduceTasksStillInProgress := false
	for i, task := range c.reduceTasks {
		if task.done == false && task.progressing == false {	
			coordinatorResponse.IsMapTask = false
			coordinatorResponse.FilesToProcess = task.fileNamesToProcess
			coordinatorResponse.Wait = false
			coordinatorResponse.TaskNumber = task.id
			coordinatorResponse.ExpectedDoneFileName = task.expectedDoneFileName
			go c.monitorTask(i, false)
			return true, nil
		} else if task.done == false && task.progressing == true {
			reduceTasksStillInProgress = true
		}
	}
	if reduceTasksStillInProgress == true {
		return false, errors.New("Map tasks currently unavailable")
	} else {
		return false, nil
	}
}

func (c *Coordinator) monitorTask(taskIndex int, isMapTask bool){
	c.markTaskAsProgressing(taskIndex, isMapTask)
	
	for i:=0; i <10; i++ {
		time.Sleep(1000 * time.Millisecond)
		if isMapTask == true {
			task := c.getMapTask(taskIndex)
			if task.done == true {
				return
			}
		} else {
			task := c.getReduceTask(taskIndex)
			file, err := os.Open(task.expectedDoneFileName)
			file.Close()
			if err == nil {
				c.markTaskAsDone(taskIndex, false)
				return	
			} 
		}
	}

	c.markTaskAsNotProgressing(taskIndex, isMapTask)
}

func (c *Coordinator) isMappingFinished() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.mappingFinished
}

func (c *Coordinator) markTaskAsDone(taskIndex int, isMapTask bool){
	if isMapTask {
		task := c.getMapTask(taskIndex)
		task.done = true
		c.setMapTask(taskIndex, task)
	} else {
		task := c.getReduceTask(taskIndex)
		task.done = true
		c.setReduceTask(taskIndex, task)
	}
}

func (c *Coordinator) markTaskAsProgressing(taskIndex int, isMapTask bool) Task{
	if isMapTask {
		task := c.getMapTask(taskIndex)
		task.progressing = true
		c.setMapTask(taskIndex, task)
		return task
	} else {
		task := c.getReduceTask(taskIndex)
		task.progressing = true
		c.setReduceTask(taskIndex, task)
		return task
	}
}

func (c *Coordinator) markTaskAsNotProgressing(taskIndex int, isMapTask bool) Task{
	if isMapTask {
		task := c.getMapTask(taskIndex)
		task.progressing = false
		c.setMapTask(taskIndex, task)
		return task
	} else {
		task := c.getReduceTask(taskIndex)
		task.progressing = false
		c.setReduceTask(taskIndex, task)
		return task
	}
}

func (c *Coordinator) getMapTask(taskIndex int) Task {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.mapTasks[taskIndex]
}

func (c *Coordinator) setMapTask(taskIndex int, task Task) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.mapTasks[taskIndex] = task
}

func (c *Coordinator) getReduceTask(taskIndex int) Task {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.reduceTasks[taskIndex]
}

func (c *Coordinator) setReduceTask(taskIndex int, task Task) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reduceTasks[taskIndex] = task
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
	c := Coordinator{mapTasks: make(map[int]Task), reduceTasks: make(map[int]Task), mapTasksCount: len(files), reduceTasksCount: reduceTasks}
	
	for i, filename := range files{
		c.mapTasks[i]= Task{id: i, progressing: false, done: false, fileNamesToProcess: []string{filename} }
	}

	for i:=0; i	< reduceTasks; i++ {
		var sbReduce strings.Builder
		sbReduce.WriteString("mr-out-")
		sbReduce.WriteString(strconv.Itoa(i))
		expectedDoneReduceFileName:= sbReduce.String()
		c.reduceTasks[i]= Task{id: i, progressing: false, done: false, expectedDoneFileName: expectedDoneReduceFileName}
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

