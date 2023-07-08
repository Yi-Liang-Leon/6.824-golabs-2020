package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Master struct {
	files          []string
	mapAssigned    []bool
	reduceAssigned []bool
	nReduce        int
	isMapping      bool
	isFinished     bool
	l              sync.Mutex
}

func (m *Master) lock()   { m.l.Lock() }
func (m *Master) unlock() { m.l.Unlock() }

type Task struct {
	Mode          int //1: map, 2: reduce
	Number        int
	MapSource     string
	ReduceSources []string
	NReduce       int
}

// start a thread that listens for RPCs from worker.go
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {
	m.lock()
	defer m.unlock()
	if m.isFinished {
		time.Sleep(time.Second)
	}
	return m.isFinished
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	m.files = files
	m.mapAssigned = make([]bool, len(files))
	m.reduceAssigned = make([]bool, nReduce)
	m.nReduce = nReduce
	m.isMapping = true
	m.server()
	return &m
}

func (m *Master) Request(args int, task *Task) error {
	// mapping
	m.lock()
	defer m.unlock()
	if m.isMapping {
		mapping := false
		for i, assigned := range m.mapAssigned {
			if !assigned {
				task.MapSource = m.files[i]
				task.Mode = 1
				task.Number = i
				task.NReduce = m.nReduce
				mapping = true
				break
			}
		}
		m.isMapping = mapping
	}
	if !m.isMapping {
		reducing := false
		for i, assigned := range m.reduceAssigned {
			if !assigned {
				task.ReduceSources = m.intermediateFiles(i)
				task.Mode = 2
				task.Number = i
				task.NReduce = m.nReduce
				reducing = true
				break
			}
		}
		if !reducing || m.isFinished {
			task.Mode = 3
			m.isFinished = true
			return nil
		}
	}
	return nil
}

func (m *Master) OK(task *Task, reply *int) error {
	m.lock()
	defer m.unlock()
	switch task.Mode {
	case 1:
		m.mapAssigned[task.Number] = true
	case 2:
		m.reduceAssigned[task.Number] = true
	default:
		return nil
	}
	return nil
}

func (m *Master) intermediateFiles(num int) (ret []string) {
	for i := range m.files {
		name := fmt.Sprintf("mr-%d-%d", i, num)
		ret = append(ret, name)
	}
	return
}
