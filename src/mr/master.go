package mr

import "fmt"
import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "io/ioutil"
import "sync"


type WorkerInfo struct {
	address string
}

var Files []string

type MRData struct {
	MapperInput map[string]string
	IntermData map[string] int
	ReducerOutput map[string] int
	
}

type Master struct {
	// sync.Mutex
	mux sync.Mutex

	workerEndPoint string
	scheduledWorkers map[string]bool
	// workers [] chan KeyValue
	address string
	newWorkerBroadcast *sync.Cond
	spawnWKChan chan bool
	doneChannel chan bool
	shutdown chan bool
	input_files []string
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
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	fmt.Print("\nStarting Master Server...\n")
	address, err := net.ResolveTCPAddr("tcp", "0.0.0.0:12345")

	if err != nil {
		fmt.Print("**XX**")
		log.Fatal(err)
	}

	inbound, err := net.ListenTCP("tcp", address)

	if err != nil {
		fmt.Print("**XX**")
		log.Fatal(err)
	}

	rpc.Register(mr)
	rpc.Accept(inbound)	
	fmt.Printf("\nNow Accepting Connections...\n")
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
	fmt.Print("Received Spawing Channel\n")

	job.NMappers = 10;
	return nil
}

func (mr *Master) RegisterWorker(wk* WorkerConfig, mrData *MRData) error {
	mr.mux.Lock()
	mr.scheduledWorkers[wk.Address] = false
	wk.scheduled = true
	wk.completedJob = false
	fmt.Printf("\nRegistering new worker @: %s", wk.Address)
	mr.mux.Unlock()

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

func (mr *Master)distributeMapJob(workerAddress string, dataChunck *KeyValue) {
	//_ := mr.spawnWKChan 
	fmt.Print("Distributing Work to Workers\n")
	client, err := rpc.Dial("tcp", workerAddress)
	if err != nil {
	  log.Fatal(err)
	}

	msg := Mapper
	
	
	err = client.Call("WorkerConfig.SpawnNewWorker", msg, dataChunck)

	if err != nil {
		log.Fatal(err)
	}
}


func (mr *Master) scheduleMappers(returnChan chan bool) {
	fmt.Printf("\nDistributed Jobs...\n")

	fmt.Printf("Done Distributing...\n")
	returnChan <- true
}


func initaliseMaster(master string) *Master {
	m := new(Master)
	m.address = master
	// m.newWorkerBroadcast = sync.NewCond(m)
	m.doneChannel = make(chan bool)
	m.spawnWKChan = make(chan bool)
	// m.mapperChannel = make(chan KeyValue)
	return m
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	add := masterSock()
	m := initaliseMaster(add)
	m.input_files = files
	m.scheduledWorkers = make(map[string]bool)
	// m.mapperChannel = make(chan KeyValue)
	Files = files
	// rpcServer()
	

	go startRPCServer(m)
	
	returnChan := make(chan bool)
	go m.scheduleMappers(returnChan)

	<- returnChan
	return m
}
