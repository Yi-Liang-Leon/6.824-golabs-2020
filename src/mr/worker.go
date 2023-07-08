package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	var tempfiles []*os.File

	for {
		task := Task{}
		if !call("Master.Request", 0, &task) {
			return
		}
		switch task.Mode {
		case 1: // map task
			// map
			content, _ := os.ReadFile(task.MapSource)
			kva := mapf(task.MapSource, string(content))

			// split intermediates
			intermediates := make([][]KeyValue, task.NReduce)
			for _, kv := range kva {
				hash := ihash(kv.Key) % task.NReduce
				intermediates[hash] = append(intermediates[hash], kv)
			}

			// write intermediates to temporary files
			tempfiles = make([]*os.File, task.NReduce)
			for i, intermediate := range intermediates {
				filename := fmt.Sprintf("mr-%d-%d", task.Number, i)
				tempfiles[i], _ = os.CreateTemp(".", filename+"_*")
				defer tempfiles[i].Close()
				enc := json.NewEncoder(tempfiles[i])
				for _, kv := range intermediate {
					enc.Encode(kv)
				}
			}

		case 2: // reduce task
			intermediate := make([]KeyValue, 0)

			// read intermediate from files
			for _, reduceSource := range task.ReduceSources {
				file, _ := os.Open(reduceSource)
				defer file.Close()
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv)
				}
			}

			// reduce
			sort.Sort(ByKey(intermediate))
			i := 0
			filename := fmt.Sprintf("mr-out-%d", task.Number)
			file, _ := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, os.FileMode(0666))
			defer file.Close()
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values)

				fmt.Fprintf(file, "%v %v\n", intermediate[i].Key, output)

				i = j
			}
		default:
			return

		}
		// rename temp file
		for _, tempfile := range tempfiles {
			os.Rename(tempfile.Name(), strings.Split(tempfile.Name(), "_")[0]) // remove suffix
		}
		call("Master.OK", task, nil)
		time.Sleep(time.Millisecond)
	}
}

// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	return err == nil
}
