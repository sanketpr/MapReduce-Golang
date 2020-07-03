package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
// import "sort"
import "sync"
// import "os"
import "net"

const(
	Mapper = "Mapper"
	Reducer = "Reducer"
)

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
}

type Job struct {
	NMappers int
	NReducers int
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
		// fmt.Printf("Worker Running...\n")
		// c := make(chan KeyValue)
		// go rpcClient(c)

		spawnChannel := make(chan int)

		go StartRPCClient(spawnChannel)
		SpawnWorker(spawnChannel, Mapper)


		// interm := mapOperation(mapf,c)

		// sort.Sort(ByKey(interm))
		// final_output := reduceOperation(reducef,interm)

		// sort.Sort(ByKey(final_output))

		// wk := new(WorkerConfig)
		// wk.address = "0.0.0.0:11444"
		// wk.workerType = Mapper
		// StartWorkerRPCServer(wk)
		
		// oname := "mr-out-0"
		// ofile, _ := os.Create(oname)


		// for _,kv := range final_output {
		// 	fmt.Printf("%s   -   %s\n",kv.Key,kv.Value)
		// 	fmt.Fprintf(ofile, "%v %v\n", kv.Key, kv.Value)
		// }

		// ofile.Close()
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

/*
RPC Implementation
---------------------------------------------------------------------------------------------------------------------
*/

func StartRPCClient(spawnChannel chan int) {
	fmt.Print("\nInitating connection with master\n")
	client, err := rpc.Dial("tcp", "0.0.0.0:12345")
	if err != nil {
	  log.Fatal(err)
	}

	l := string("Test String")
	job := new(Job)
	// job.nMappers = 0
	// job.nReducers = 0

	err = client.Call("Master.EstConnection", l, &job)

	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("\nPusblising on mapper spawn channel\n")
	spawnChannel <- job.NMappers
	close(spawnChannel)
}

func ListenToSpawnNewWorker(spawnChannel chan int) {
	fmt.Print("\nWaiting for a signal to create new worker...\n")
	// for c := range spawnChannel {
	// 	if(c) {
	// 		fmt.Print("\nHearback, master wants new worker\n")
	// 	} else {
	// 		fmt.Print("\nHearback, master DOES NOT want new worker\n")	
	// 	}
	// }
	fmt.Print("\nDone Waiting...\n")
}

func StartWorkerRPCServer(wk *WorkerConfig) {
	fmt.Print("\nStarting Worker Server...\n")
	address, err := net.ResolveTCPAddr("tcp", wk.Address)

	if err != nil {
	 log.Fatal(err)
	}

	inbound, err := net.ListenTCP("tcp", address)

	if err != nil {
	 log.Fatal(err)
	}

	rpc.Register(wk)
	rpc.Accept(inbound)	
}

func (wk *WorkerConfig) RegisterWithMaster() {
	client, err := rpc.Dial("tcp", "localhost:12345")
	if err != nil {
	  log.Fatal(err)
	}

	// msg := string("Message")
	fmt.Printf("\nRegistering with master %s\n",wk.Address)
	err = client.Call("Master.RegisterWorker",&wk, nil)

	if err != nil {
		log.Fatal(err)
	}	
}

func SpawnWorker(spawnChannel chan int ,jobType string) {
	var done sync.WaitGroup
	fmt.Printf("Waiting on mapper channel\n")
	for n := range spawnChannel {
		fmt.Printf("Receiving on mapper channel\n")
		for i := 0; i < n; i++ {
			done.Add(1)
			fmt.Printf("Spawning a new mapper..\n")
			go func() {
				CreateNewWorker(jobType)
				done.Done()
			}()
		}

		done.Wait()
	}
	return
}

func CreateNewWorker(workerType string) {
	wk := new(WorkerConfig)
	// wk.dataChan = make(chan KeyValue)
	wk.Address = generateAddress()
	wk.WorkerType = workerType

	// StartWorkerRPCServer(wk)
	wk.RegisterWithMaster()
}


func (wk *WorkerConfig) SpawnNewWorker(workerType string, dataChunck *KeyValue) error {
	new_wk := new(WorkerConfig)
	new_wk.DataChunck = KeyValue{dataChunck.Key,dataChunck.Value}

	new_wk.Address = generateAddress()
	new_wk.WorkerType = workerType

	// StartWorkerRPCServer(wk)
	// wk.RegisterWithMaster()

	fmt.Print("\nNew Worker Create and Registered \n")

	return nil
}
// func (wk *WorkerConfig) PerformJob(dataChunck *KeyValue) {
// 	fmt.Print("\nInitating the job...\n")
// 	switch wk.workerType {
// 	case Mapper:
// 		// performMapperJob()
// 		fmt.Println("Mapper Job Assigned")
// 	case Reducer:
// 		// performReducerJob()
// 		fmt.Println("Reducer Job Assigned")
// 	default:
// 		fmt.Print("\n*******\nUnidentified Job Type Assigned\n*******\n")
// 	}
// }


func generateAddress() string {
	name := string("some_address")
	return name
}