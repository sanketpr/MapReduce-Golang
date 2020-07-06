package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
// import "sort"
import "sync"
// import "os"
import "net"
import "math/rand"
import "strconv"

const(
	Mapper = "Mapper"
	Reducer = "Reducer"
)

type Ports struct{
	usedPorts map[int] bool
	mux sync.Mutex
}

var ports Ports
//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type WorkerConfig struct {
//	isAlive bool
	Address string
	WorkerType string
	DataChunck KeyValue
	scheduled bool
	completedJob bool
}

type Job struct {
	NMappers int
	NReducers int
	JobType string
	MapFunc func(string, string) []KeyValue
	RedFunc func(string, []string) string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }


//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


func rpcClient(c chan KeyValue) {
	client, err := rpc.Dial("tcp", "localhost:12345")
	if err != nil {
	  log.Fatal(err)
	}

	l := string("Test String")
	
	var mrData Reply
	err = client.Call("Listener.GetLine", l, &mrData)

	if err != nil {
		log.Fatal(err)
	}

	for k, v := range mrData.Data {
		c <- KeyValue{k,v}
	}

	close(c)
}

func mapOperation(mapf func(string, string) []KeyValue, c chan KeyValue) []KeyValue {
	kva := make([]KeyValue,9)
	var done sync.WaitGroup

	for keyVal := range c{
		done.Add(1)
		go func(keyValue KeyValue) {
			temp := mapf(keyVal.Key, keyVal.Value)
			kva = append(kva,temp...)
			done.Done()
		}(keyVal)
	}

	done.Wait()
	return kva
}

func reduceOperation(reducef func(string, []string) string, intermediate []KeyValue) []KeyValue {
	i := 0

	keys := []string{}
	vals := [][]string{}


	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		keys = append(keys,intermediate[i].Key)
		vals = append(vals, values)

		i = j
	}


	final_output := []KeyValue{}
	var done sync.WaitGroup

	for k := 0; k < len(keys); k++ {
		done.Add(1)
		go func(key string, values []string) {
			output := reducef(key, values)
			kv_output := KeyValue{key,output}
			final_output = append(final_output,kv_output)
			done.Done()
		}(keys[k], vals[k])
	}
	done.Wait()
	return final_output
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

		ports = Ports{ usedPorts: make(map[int]bool) }
		

		job := new(Job)
		job.MapFunc = mapf
		job.RedFunc = reducef
		job.JobType = Mapper


		spawnChannel := make(chan int)

		go StartRPCClient(spawnChannel, job)
		SpawnMappers(spawnChannel, job)
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func SpawnMappers(spawnChannel chan int ,job *Job) {
	var done sync.WaitGroup
	fmt.Printf("Waiting on mapper channel\n")
	for n := range spawnChannel {
		fmt.Printf("Receiving on mapper channel\n")
		for i := 0; i < n; i++ {
			done.Add(1)
			fmt.Printf("Spawning a new Mapper\n")
			go func() {
				CreateNewWorker(job)
				done.Done()
			}()
		}

		done.Wait()
	}
	return
}

func CreateNewWorker(job *Job) {
	wk := new(WorkerConfig)
	// wk.dataChan = make(chan KeyValue)
	wk.Address = generateAddress()
	wk.WorkerType = job.JobType

	StartWorkerRPCServer(wk,job)
	wk.registerWithMaster()
}

func (wk *WorkerConfig) registerWithMaster() {
	client, err := rpc.Dial("tcp", "localhost:12345")
	if err != nil {
	  log.Fatal(err)
	}

	fmt.Printf("Registering with master %s\n",wk.Address)
	err = client.Call("Master.RegisterWorker",&wk, nil)

	if err != nil {
		log.Fatal(err)
	}	
}

func generateAddress() string {
	min := 2000
	max := 4000
	ip := string("0.0.0.0:")

	var randn int
	for {
		randn = rand.Intn(max-min) + min
		ports.mux.Lock()
		if(!ports.usedPorts[randn]) {
			ports.usedPorts[randn] = true
			ports.mux.Unlock()
			break
		}
		ports.mux.Unlock()
	}

	port := strconv.Itoa(randn)
	address := ip+port
	return address
}

/*
RPC Implementation
---------------------------------------------------------------------------------------------------------------------
*/

func StartRPCClient(spawnChannel chan int, job *Job) {
	fmt.Print("\nInitating connection with master\n")
	client, err := rpc.Dial("tcp", "0.0.0.0:12345")
	if err != nil {
	  log.Fatal(err)
	}

	l := string("Test String")

	err = client.Call("Master.EstConnection", l, &job)

	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("\nPusblising on mapper spawn channel\n")
	spawnChannel <- job.NMappers
	close(spawnChannel)
}

func (wk *WorkerConfig) Somefunc(msg string, job *Job) error {
	return nil
} 

func StartWorkerRPCServer(wk *WorkerConfig, job *Job) {
	fmt.Print("Starting Worker Server...\n")
	address, err := net.ResolveTCPAddr("tcp", wk.Address)

	if err != nil {
		log.Fatal(err)
	}

	inbound, err := net.ListenTCP("tcp", address)

	if err != nil {
	 log.Fatal(err)
	}

	// rpc.Register(job)
	rpc.Register(wk)
	go func() {
		rpc.Accept(inbound)
	}()
}