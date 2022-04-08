package mr

import "os"
import "strconv"

type WorkerRequest struct {
	TaskNumber int
	IsMapTask bool
	CompletedIntermediateFiles []string
	CompletedInputFile string
}

type CoordinatorResponse struct {
	TaskNumber int
	ReduceTasks int
	IsMapTask bool // false indicates reduce task
	FilesToProcess []string
	ExpectedDoneFileName string
	Wait bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
