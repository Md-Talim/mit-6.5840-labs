package mr

// RequestTaskArgs is an empty struct used for
// requesting a task from the coordinator.
type RequestTaskArgs struct{}

// RequestTaskReply contains the details of the task
// assigned to a worker, including the task type,
// file name, task number, and the number of reduce or map tasks.
type RequestTaskReply struct {
	TaskType   string
	File       string
	TaskNumber int
	NReduce    int
	NMap       int
}

// ReportTaskArgs contains the details of a completed task reported by a worker,
// including the task type, task number, and the file name associated with the task.
type ReportTaskArgs struct {
	TaskType   string
	TaskNumber int
	File       string
}

// ReportTaskReply is an empty struct used for acknowledging
// the completion of a task reported by a worker.
type ReportTaskReply struct{}
