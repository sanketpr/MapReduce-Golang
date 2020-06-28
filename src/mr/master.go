package mr

import "fmt"
import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "io/ioutil"


type WorkerInfo struct {
	address string
}

var Files []string

type MRData struct {
	mapperInput map[string]string
	// intermData map[string] int
	// reducerOutput map[string] int
}

type Master struct {
	listener net.Listener
	isAlive bool
	workers map[string] *WorkerInfo
	address string
	mapperChannel chan KeyValue
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

func (l *Listener) GetLine(line string, reply *Reply) error {
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

	*reply = Reply{mapperInput}

	return nil
}

func (l *Listener) GetData(line string, mrData *MRData) error {
	// mrData = new(MRData)
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

	*mrData = MRData{mapperInput}

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

func distributeMapJob(file_contents map[int]string) {
	fmt.Print("Distribution Center\n")
}


// func initaliseMaster() *Master {
// 	m := new(Master)
// 	m.address = ""
// 	m.isAlive = true

// 	return &m
// }

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.mapperChannel = make(chan KeyValue)
	Files = files
	rpcServer()
	return &m
}
