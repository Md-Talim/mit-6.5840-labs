package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"slices"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// KeyValueCompare compares two KeyValue pairs based on their keys.
func KeyValueCompare(a KeyValue, b KeyValue) int {
	if a.Key < b.Key {
		return -1
	} else if a.Key > b.Key {
		return 1
	} else {
		return 0
	}
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var coordSockName string // socket for coordinator

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	coordSockName = sockname

	// continuously request tasks from the coordinator until told to exit
	for {
		args := RequestTaskArgs{}
		reply := RequestTaskReply{}

		ok := call("Coordinator.RequestTask", &args, &reply)
		if !ok {
			log.Printf("Worker %d: failed to contact coordinator, exiting", os.Getpid())
			return
		}

		switch reply.TaskType {
		case "map":
			log.Printf("Worker %d: got MAP task %d (file=%s)", os.Getpid(), reply.TaskNumber, reply.File)
			executeMap(reply, mapf)
		case "reduce":
			log.Printf("Worker %d: got REDUCE task %d (nMap=%d)", os.Getpid(), reply.TaskNumber, reply.NMap)
			executeReduce(reply, reducef)
		case "exit":
			log.Printf("Worker %d: told to exit, job is done", os.Getpid())
			return
		case "wait":
			log.Printf("Worker %d: no task available, sleeping 1s", os.Getpid())
			time.Sleep(time.Second)
		}
	}
}

// executeMap reads the input file, applies the map function, partitions the output
// into intermediate files, and reports task completion to the coordinator.
func executeMap(reply RequestTaskReply, mapf func(string, string) []KeyValue) {
	content, err := os.ReadFile(reply.File)
	if err != nil {
		log.Printf("Worker %d: failed to read file %s: %v", os.Getpid(), reply.File, err)
		return
	}
	log.Printf("Worker %d: read %d bytes from %s", os.Getpid(), len(content), reply.File)
	result := mapf(reply.File, string(content))
	log.Printf("Worker %d: mapf produced %d k/v pairs", os.Getpid(), len(result))

	files := make([]*os.File, reply.NReduce)
	encoders := make([]*json.Encoder, reply.NReduce)
	for i := range reply.NReduce {
		filename := fmt.Sprintf("mr-%d-%d", reply.TaskNumber, i)
		file, err := os.Create(filename)
		if err != nil {
			log.Printf("Worker %d: failed to create %s: %v", os.Getpid(), filename, err)
			return
		}
		files[i] = file
		encoders[i] = json.NewEncoder(file)
	}

	for _, kv := range result {
		reduceBucket := ihash(kv.Key) % reply.NReduce
		encoders[reduceBucket].Encode(&kv)
	}
	log.Printf("Worker %d: wrote %d intermediate files for map task %d", os.Getpid(), reply.NReduce, reply.TaskNumber)

	for _, file := range files {
		file.Close()
	}

	ok := call("Coordinator.ReportTask", &ReportTaskArgs{
		TaskType:   "map",
		File:       reply.File,
		TaskNumber: reply.TaskNumber,
	}, &ReportTaskReply{})
	if ok {
		log.Printf("Worker %d: reported map task %d as done", os.Getpid(), reply.TaskNumber)
	}
}

// executeReduce reads all intermediate files for the reduce task, applies the reduce function,
// writes the output to a result file, and reports task completion to the coordinator.
func executeReduce(reply RequestTaskReply, reducef func(string, []string) string) {
	// read all intermediate files
	intermediate := []KeyValue{}
	for i := 0; i < reply.NMap; i++ {
		filename := fmt.Sprintf("mr-%d-%d", i, reply.TaskNumber)
		file, err := os.Open(filename)
		if err != nil {
			log.Printf("Worker %d: failed to open %s: %v", os.Getpid(), filename, err)
			continue
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}
	log.Printf("Worker %d: reduce task %d read %d k/v pairs from intermediate files", os.Getpid(), reply.TaskNumber, len(intermediate))

	slices.SortFunc(intermediate, KeyValueCompare)

	data := make(map[string][]string)
	for _, kv := range intermediate {
		data[kv.Key] = append(data[kv.Key], kv.Value)
	}
	log.Printf("Worker %d: reduce task %d has %d unique keys", os.Getpid(), reply.TaskNumber, len(data))

	resultFile, err := os.OpenFile(fmt.Sprintf("mr-out-%d", reply.TaskNumber), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("Worker %d: failed to create mr-out-%d: %v", os.Getpid(), reply.TaskNumber, err)
		return
	}
	defer resultFile.Close()
	for key, values := range data {
		result := reducef(key, values)
		fmt.Fprintf(resultFile, "%s %s\n", key, result)
	}

	ok := call("Coordinator.ReportTask", &ReportTaskArgs{
		TaskType:   "reduce",
		File:       "mr-out-" + fmt.Sprint(reply.TaskNumber),
		TaskNumber: reply.TaskNumber,
	}, &ReportTaskReply{})
	if ok {
		log.Printf("Worker %d: reported reduce task %d as done", os.Getpid(), reply.TaskNumber)
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args any, reply any) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		return false
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}
