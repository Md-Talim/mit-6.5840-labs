package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// taskState represents the state of a single map or reduce task.
type taskState struct {
	file        string
	isCompleted bool
	inProgress  bool
	startedAt   *time.Time
	taskNumber  int
}

// Coordinator manages the state of map and reduce tasks, assigns them to workers,
// and handles task timeouts and completion.
type Coordinator struct {
	mu          sync.Mutex
	mapTasks    []taskState
	reduceTasks []taskState
	nReduce     int
	nMap        int
}

// RequestTask is called by a worker to request a task from the coordinator.
// It checks for available map and reduce tasks, assigns them to the worker,
// and handles task timeouts. If all tasks are completed, it instructs the worker to exit.
func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.checkTaskTimeouts()

	if task, ok := c.getMapTask(); ok {
		reply.TaskType = "map"
		reply.File = task.file
		reply.TaskNumber = task.taskNumber
		reply.NReduce = c.nReduce
		log.Printf("Coordinator: assigned map task %d (file=%s)", task.taskNumber, task.file)
		return nil
	}
	if task, ok := c.getReduceTask(); ok {
		reply.TaskType = "reduce"
		reply.File = task.file
		reply.TaskNumber = task.taskNumber
		reply.NMap = c.nMap
		log.Printf("Coordinator: assigned reduce task %d (nMap=%d)", task.taskNumber, c.nMap)
		return nil
	}
	if c.done() {
		reply.TaskType = "exit"
		return nil
	}

	reply.TaskType = "wait"
	return nil
}

// RReportTask is called by a worker to report the completion of a task.
func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if args.TaskType == "map" {
		for i, task := range c.mapTasks {
			if task.taskNumber == args.TaskNumber {
				c.mapTasks[i].isCompleted = true
				c.mapTasks[i].inProgress = false
				log.Printf("Coordinator: map task %d completed", args.TaskNumber)
				break
			}
		}
	}
	if args.TaskType == "reduce" {
		for i, task := range c.reduceTasks {
			if task.taskNumber == args.TaskNumber {
				c.reduceTasks[i].isCompleted = true
				c.reduceTasks[i].inProgress = false
				log.Printf("Coordinator: reduce task %d completed", args.TaskNumber)
				break
			}
		}
	}

	return nil
}

// getMapTask returns the next available map task that is not completed and not in progress.
func (c *Coordinator) getMapTask() (taskState, bool) {
	for i := range c.mapTasks {
		if !c.mapTasks[i].isCompleted && !c.mapTasks[i].inProgress {
			// check if task timed out (>10s)
			if c.mapTasks[i].startedAt != nil && time.Since(*c.mapTasks[i].startedAt) > 10*time.Second {
				log.Printf("Coordinator: map task %d timed out, re-assigning", c.mapTasks[i].taskNumber)
			}
			now := time.Now()
			c.mapTasks[i].inProgress = true
			c.mapTasks[i].startedAt = &now
			return c.mapTasks[i], true
		}
	}
	return taskState{}, false
}

// getReduceTask returns the next available reduce task that is not completed and not in progress.
func (c *Coordinator) getReduceTask() (taskState, bool) {
	// all maps must be done before reduce can start
	for _, task := range c.mapTasks {
		if !task.isCompleted {
			return taskState{}, false
		}
	}

	for i := range c.reduceTasks {
		if !c.reduceTasks[i].isCompleted && !c.reduceTasks[i].inProgress {
			// check if task timed out (>10s)
			if c.reduceTasks[i].startedAt != nil && time.Since(*c.reduceTasks[i].startedAt) > 10*time.Second {
				log.Printf("Coordinator: reduce task %d timed out, re-assigning", c.reduceTasks[i].taskNumber)
			}
			now := time.Now()
			c.reduceTasks[i].inProgress = true
			c.reduceTasks[i].startedAt = &now
			return c.reduceTasks[i], true
		}
	}
	return taskState{}, false
}

// checkTaskTimeouts checks for tasks that have been in progress
// for more than 10 seconds and marks them as not in progress
// so they can be reassigned.
func (c *Coordinator) checkTaskTimeouts() {
	for i := range c.mapTasks {
		if c.mapTasks[i].inProgress && !c.mapTasks[i].isCompleted {
			if c.mapTasks[i].startedAt != nil && time.Since(*c.mapTasks[i].startedAt) > 10*time.Second {
				log.Printf("Coordinator: map task %d timed out, re-assigning", c.mapTasks[i].taskNumber)
				c.mapTasks[i].inProgress = false
				c.mapTasks[i].startedAt = nil
			}
		}
	}

	for i := range c.reduceTasks {
		if c.reduceTasks[i].inProgress && !c.reduceTasks[i].isCompleted {
			if c.reduceTasks[i].startedAt != nil && time.Since(*c.reduceTasks[i].startedAt) > 10*time.Second {
				log.Printf("Coordinator: reduce task %d timed out, re-assigning", c.reduceTasks[i].taskNumber)
				c.reduceTasks[i].inProgress = false
				c.reduceTasks[i].startedAt = nil
			}
		}
	}
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

func (c *Coordinator) done() bool {
	for _, task := range c.mapTasks {
		if !task.isCompleted {
			return false
		}
	}
	for _, task := range c.reduceTasks {
		if !task.isCompleted {
			return false
		}
	}
	return true
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.done()
	// log.Printf("Coordinator: all tasks completed, job is done")
	// return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	mapTasks := []taskState{}
	reduceTasks := make([]taskState, nReduce)
	for i, f := range files {
		mapTasks = append(mapTasks, taskState{f, false, false, nil, i})
	}
	for i := range nReduce {
		reduceTasks[i] = taskState{"", false, false, nil, i}
	}

	log.Printf("Coordinator: starting with %d map tasks, %d reduce tasks", len(files), nReduce)

	c := Coordinator{
		mapTasks:    mapTasks,
		reduceTasks: reduceTasks,
		nReduce:     nReduce,
		nMap:        len(files),
	}

	c.server(sockname)
	return &c
}
