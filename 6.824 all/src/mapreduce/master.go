package mapreduce

import "container/list"
import (
	"fmt"
)

type WorkerInfo struct {
	address string
	// You can add definitions here.
}


// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}

	return l
}



func (mr *MapReduce) RunMaster() *list.List {
	// Your code here
	//my code begin

	mapDoneChannel := make(chan int, mr.nMap)
	reduceDoneChannel := make(chan int, mr.nReduce)

	schedule := func(doneChannel chan int, jobNumber int, numOfPharse int, operation JobType) {
		for {
			// get the idle  worker
			worker := <- mr.idleWorkerChannel

			// set the jobargs and reply
			jobArgs := &DoJobArgs{}
			jobReply := &DoJobReply{}
			jobArgs.NumOtherPhase = numOfPharse
			jobArgs.Operation = operation
			jobArgs.File = mr.file
			jobArgs.JobNumber = jobNumber

			// call worker.DoJob
			ok := call(worker, "Worker.DoJob", jobArgs, jobReply)
			if ok == true {
				mr.idleWorkerChannel <- worker
				doneChannel <- jobNumber
				return
			}
		}
	}
	for i := 0; i < mr.nMap; i++ {
		go schedule(mapDoneChannel, i, mr.nReduce, Map)
	}

	for i := 0; i < mr.nMap; i++ {
		<- mapDoneChannel
	}

	for i := 0; i < mr.nReduce; i++ {
		go schedule(reduceDoneChannel, i, mr.nMap, Reduce)
	}

	for i := 0; i < mr.nReduce; i++ {
		<- reduceDoneChannel
	}

	fmt.Println("Jobs are all done.")
	//my code end
	return mr.KillWorkers()
}
