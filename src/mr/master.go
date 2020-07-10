package mr

import "fmt"
import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "io/ioutil"
import "sync"
import "time"
import "sort"

var Files []string

type MRData struct {
	// mux sync.Mutex
	MapperInput KeyValue
	MapperOutput []KeyValue
	WorkerType string
	// ReducerOutput map[string] int
}

type ReducerData struct {
	Input []KeyValue
	Output []KeyValue
}

type Master struct {
	mux sync.Mutex
	WorkerEndPoint string
	scheduledMappers map[string] *WorkerConfig
	scheduledReduers map[string] *WorkerConfig

	mapperData map[string]string
	mapperOutput []KeyValue

	address string
	spawnWKChan chan bool
	doneChannel chan bool
	shutdown chan bool

	nMappers int
	nReducers int
}

// Your code here -- RPC handlers for the worker to call.


type Listener int

type Reply struct {
   Data map[string]string
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(msg string, reply *MRData) error {
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func rpcServer() {
	address, err := net.ResolveTCPAddr("tcp", "0.0.0.0:12345")

	if err != nil {
	 log.Fatal(err)
	}

	inbound, err := net.ListenTCP("tcp", address)

	if err != nil {
	 log.Fatal(err)
	}

	listener := new(Listener)
	rpc.Register(listener)
	rpc.Accept(inbound)
}

func startRPCServer(mr *Master) {
	fmt.Print("Starting Master Server...\n")
	address, err := net.ResolveTCPAddr("tcp", "0.0.0.0:12345")

	if err != nil {
		log.Fatal(err)
	}

	inbound, err := net.ListenTCP("tcp", address)

	if err != nil {
		log.Fatal(err)
	}

	rpc.Register(mr)
	rpc.Accept(inbound)	
	fmt.Printf("Now Accepting Connections...\n")
}

func (l *Listener) GetLine(line string, job *Job) error {
	mapperInput := make(map[string]string)
	
	for _, filename := range Files {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		mapperInput[filename] = string(content)
	}

	job.NMappers = 10

	return nil
}

func (mr *Master) EstConnection(msg string, job *Job) error {
	job.NMappers = mr.nMappers;
	job.NReducers = mr.nReducers;
	return nil
}

func (mr *Master) RegisterWorker(wk* WorkerConfig, mrData *MRData) error {
	mr.mux.Lock()
	mr.scheduledMappers[wk.Address] = wk
	wk.scheduled = true
	wk.completedJob = false
	fmt.Printf("Mapper @%s registered\n", wk.Address)
	mr.mux.Unlock()

	return nil
}

func (mr *Master) RegisterReducer(wk* WorkerConfig, mrData *MRData) error {
	mr.mux.Lock()
	mr.scheduledReduers[wk.Address] = wk
	wk.scheduled = true
	wk.completedJob = false
	mr.mux.Unlock()

	fmt.Printf("Reducer @%s registered\n", wk.Address)

	return nil
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.


	return ret
}

func (mr *Master) scheduleMappers(mapperDataArr []*MRData) []KeyValue {
	numOfWorkDone := 0

	for numOfWorkDone < mr.nMappers {
		time.Sleep(time.Second)
		numOfWorkDone = len(mr.scheduledMappers)
	}

	fmt.Print("\nStaring Mapper Phase...\n***********************")

	for k,v := range mr.mapperData {
		mapData := new(MRData)
		kva := KeyValue{k,v}
		mapData.MapperInput = kva
		mapData.WorkerType = Mapper
		mapperDataArr = append(mapperDataArr, mapData)
	}

	i := 0

	var done sync.WaitGroup
	for address,_ := range mr.scheduledMappers {
		// mrd := new(mrData)
		done.Add(1)
		go func(i int) {
			distributeMapJob(address,mapperDataArr[i])
			done.Done()
		}(i)
		i++
	}

	done.Wait()

	return collectAndSort(mapperDataArr)
}

func distributeMapJob(workerAddress string, mrData *MRData) {
	//_ := mr.spawnWKChan 
	fmt.Printf("\nAssigning Map Job to %s",workerAddress)
	client, err := rpc.Dial("tcp", workerAddress)
	if err != nil {
	  log.Fatal(err)
	}

	// var kva KeyValue
	err = client.Call("Job.MapJob", &mrData, &mrData)

	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("\n@%s Complted Map Job",workerAddress)
}

func (mr *Master)scheduleReducePhase(buckets []ReducerData) {
	i := 0
	fmt.Print("\nStaring Reducer Phase...\n***********************")
	var done sync.WaitGroup
	for address,_ := range mr.scheduledReduers {
		// mrd := new(mrData)
		done.Add(1)
		go func(i int) {
			distributeReducerJob(address,&buckets[i])
			done.Done()
		}(i)
		i++
	}

	done.Wait()
}

func distributeReducerJob(workerAddress string, reduceData *ReducerData) {
	fmt.Printf("\nAssigning Reduce Job to %s",workerAddress)
	client, err := rpc.Dial("tcp", workerAddress)
	if err != nil {
	  log.Fatal(err)
	}

	// var kva KeyValue
	err = client.Call("Job.ReduceJob", &reduceData, &reduceData)

	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("\n@%s Completed Reduce Job",workerAddress)
}

func initaliseMaster(master string, files[] string, nReduce int) *Master {
	m := new(Master)
	m.address = master
	m.scheduledMappers = make(map[string] *WorkerConfig)
	m.scheduledReduers = make(map[string] *WorkerConfig)
	m.nReducers = nReduce
	m.setData(files)
	return m
}

func (mr *Master) setData(files []string) {
	mr.mapperData = make(map[string]string)
	for _, filename := range files {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		mr.mapperData[filename] = string(content)
	}

	mr.nMappers = len(mr.mapperData)
}

func collectAndSort(mapperDataArr []*MRData) [] KeyValue{
	interm := []KeyValue{}
	for _, mrd := range mapperDataArr {
		interm = append(interm, mrd.MapperOutput...)
	}

	sort.Sort(ByKey(interm))

	return interm
}

func makeReduceBuckets(intermData [] KeyValue, nBuckets int) []ReducerData {
	buckets := []ReducerData{}
	size := len(intermData)/nBuckets
	index := 0
	var rd ReducerData
	for index < len(intermData) {
		limit := index+size
		for limit+1 < len(intermData) && intermData[limit].Key == intermData[limit+1].Key {
			limit++
		}
		if (limit > len(intermData)) {
			limit = len(intermData)
		}
		rd = ReducerData{Input: intermData[index:limit]}
		buckets = append(buckets,rd)
		index = limit
	}

	return buckets
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	add := masterSock()
	m := initaliseMaster(add, files, nReduce)
	// Files = files

	go startRPCServer(m)
	
	// returnChan := make(chan bool)
	var mapperDataArr []*MRData

	mapperOP := m.scheduleMappers(mapperDataArr)
	fmt.Print("\n\nCollection and Sorting Phase...\n***********************\n")
	reducerData := makeReduceBuckets(mapperOP, nReduce)
	m.scheduleReducePhase(reducerData)
	// for _,rd := range reducerData {
	// 	for _,V := range rd.Output{
	// 		fmt.Printf("\n%s %s",V.Key, V.Value)
	// 	}
	// }
	return m
}
