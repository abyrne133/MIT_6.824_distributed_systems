package mr

import "log"
import "net/rpc"
import "hash/fnv"
import "io/ioutil"
import "os"
import "time"
import "sort"
import "strings"
import "strconv"
import "encoding/json"
import "fmt"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
		work(mapf, reducef)
}

func work(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
		workerRequest := WorkerRequest{WorkerId: time.Now().Format(time.RFC850)}
		coordinatorResponse := CoordinatorResponse{}
			
		ok := call("Coordinator.HandleWorkerRequest", &workerRequest, &coordinatorResponse)
		
		if !ok {
			log.Println("Work Request failed, worker Id:", workerRequest.WorkerId)
			os.Exit(1);
		}

		if coordinatorResponse.Wait == true {
			log.Println("sleeping")
			time.Sleep(100 * time.Millisecond)
			work(mapf, reducef)
		}

		if coordinatorResponse.IsMapTask == true {
			mapWork(mapf, coordinatorResponse)
		} else {
			reduceWork(reducef, coordinatorResponse)
		}


		work(mapf, reducef)
}

func mapWork(mapf func(string, string) []KeyValue, coordinatorResponse CoordinatorResponse){
	
	filename := coordinatorResponse.MapFileNameToProcess
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	if err := file.Close(); err != nil {
		log.Fatalf("Could not close %v", filename)
	}
	mapData := mapf(filename, string(content))

	sort.Sort(ByKey(mapData))

	mapDataByReduceNumber := make(map[int][]KeyValue)
	for _, keyValueArray := range mapData {
		reduceTaskNumber := ihash(keyValueArray.Key) % coordinatorResponse.ReduceTasks
		mapDataByReduceNumber[reduceTaskNumber] = append(mapDataByReduceNumber[reduceTaskNumber], keyValueArray)
	}

	completedMapFiles := []string{}
	for reduceTaskNumber, keyValueArray := range mapDataByReduceNumber {
		var sb strings.Builder
		sb.WriteString("mr-")
		sb.WriteString(strconv.Itoa(coordinatorResponse.TaskNumber))
		sb.WriteString("-")
		sb.WriteString(strconv.Itoa(reduceTaskNumber))
		fileName := sb.String()
		file, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatal(err)
		}
		enc := json.NewEncoder(file)
		for _, keyValue := range keyValueArray {
			encodeErr:= enc.Encode(&keyValue)
			if encodeErr != nil {
				log.Fatal(err)
			}
		}
		if err := file.Close(); err != nil {
			log.Fatal(err)
		} else {
			completedMapFiles = append(completedMapFiles, fileName)
		}
	}

	workerDoneRequest := WorkerRequest{WorkerId: time.Now().Format(time.RFC850), TaskNumber: coordinatorResponse.TaskNumber, CompletedIntermediateFiles: completedMapFiles, CompletedInputFile: coordinatorResponse.MapFileNameToProcess}
	workerDoneCoordinatorResponse := CoordinatorResponse{}
	ok := call("Coordinator.HandleWorkerDoneRequest", &workerDoneRequest, &workerDoneCoordinatorResponse)
	if !ok {
		log.Println("WorkerDoneRequest failed, worker Id:", workerDoneRequest.WorkerId)
		os.Exit(1);
	}
	log.Println("Map Task Complete: ", coordinatorResponse.TaskNumber)
	
}

func reduceWork(reducef func(string, []string) string, coordinatorResponse CoordinatorResponse){
	kva := []KeyValue{}
	for _, fileName := range coordinatorResponse.ReduceFilesToProcess {
		file, err:= os.Open(fileName)	
		if err != nil {
			log.Fatalln("Could not open file", fileName)
		} else {
			dec := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
				  break
				}
				kva = append(kva, kv)
			  }
		}
		if err := file.Close(); err != nil {
			log.Fatalln("Could not close file", err)
		}	
	}

	sort.Sort(ByKey(kva))

	ofile, err := os.OpenFile(coordinatorResponse.ExpectedDoneFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalln("Could not open output file", coordinatorResponse.ExpectedDoneFileName)
		return
	}
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	ofile.Close()
	log.Println("Reduce Task Complete: ", coordinatorResponse.TaskNumber)
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

	log.Println(err)
	return false
}
