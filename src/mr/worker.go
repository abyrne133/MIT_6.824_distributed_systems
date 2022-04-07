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
		workerRequest := WorkerRequest{}
		coordinatorResponse := CoordinatorResponse{}
			
		ok := call("Coordinator.HandleWorkerRequest", &workerRequest, &coordinatorResponse)
		
		if !ok {
			os.Exit(1);
		}

		if coordinatorResponse.Wait == true {
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
	
	filename := coordinatorResponse.FilesToProcess[0]
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
		file, err := ioutil.TempFile("",fileName)
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
			os.Rename(file.Name(), fileName)
			completedMapFiles = append(completedMapFiles, fileName)
		}
	}

	workerDoneRequest := WorkerRequest{TaskNumber: coordinatorResponse.TaskNumber, CompletedIntermediateFiles: completedMapFiles, CompletedInputFile: coordinatorResponse.FilesToProcess[0]}
	workerDoneCoordinatorResponse := CoordinatorResponse{}
	ok := call("Coordinator.HandleWorkerDoneRequest", &workerDoneRequest, &workerDoneCoordinatorResponse)
	if !ok {
		os.Exit(1);
	}	
}

func reduceWork(reducef func(string, []string) string, coordinatorResponse CoordinatorResponse){
	kva := []KeyValue{}
	for _, fileName := range coordinatorResponse.FilesToProcess {
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

	ofile, err := ioutil.TempFile("", coordinatorResponse.ExpectedDoneFileName)
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
	os.Rename(ofile.Name(), coordinatorResponse.ExpectedDoneFileName)
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
