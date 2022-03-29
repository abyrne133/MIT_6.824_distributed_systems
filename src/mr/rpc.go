package mr

import "os"
import "strconv"

type WorkerRequest struct {
	WorkerId string
}

type CoordinatorResponse struct {
	ReduceTasks int
	FileNamesToProcess []string
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
