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
import "time"

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
	ready bool
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

func mapOP(mapf func(string, string) []KeyValue, keyVal KeyValue) []KeyValue {
	kva := make([]KeyValue,9)
	temp := mapf(keyVal.Key, keyVal.Value)
	kva = append(kva,temp...)
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
		somechan := make(chan bool)
		go StartRPCClient(spawnChannel, somechan, job)

		time.Sleep(10*time.Millisecond)
		go SpawnReducers(somechan, job)
		SpawnMappers(spawnChannel, job)
}

func SpawnMappers(spawnChannel chan int ,job *Job) {
	var done sync.WaitGroup
	for n := range spawnChannel {
		fmt.Printf("\n> Spawning Mappers\n")
		for i := 0; i < n; i++ {
			done.Add(1)
			go func() {
				CreateNewWorker(job, Mapper)
				done.Done()
			}()
		}

		done.Wait()
	}
	return
}

func SpawnReducers(somechan chan bool, job *Job) {
	var done sync.WaitGroup
	for _ = range somechan {
		for i := 0; i < job.NReducers; i++ {
			done.Add(1)
			go func() {
				CreateNewWorker(job, Reducer)
				done.Done()
			}()
		}

		done.Wait()
	}
}

func CreateNewWorker(job *Job, jobType string) {
	wk := new(WorkerConfig)
	wk.Address = generateAddress()
	wk.WorkerType = jobType

	go func() {
		/*
		TODO: Remove the time delay
		creatiStartWorkerRPCServerng a small delay so that the new mapper gets
		enought time to start the rpc server before regestering
		with master and accepting rpc requests
		*/
		// time.Sleep(time.Second)
		wk.registerWithMaster(jobType)
	}()

	StartWorkerRPCServer(wk,job)
}

func (wk *WorkerConfig) registerWithMaster(jobType string) {
	client, err := rpc.Dial("tcp", "localhost:12345")
	if err != nil {
	  log.Fatal(err)
	}

	fmt.Printf("Registering with master %s\n",wk.Address)
	
	if (jobType == Mapper) {
		err = client.Call("Master.RegisterWorker",&wk, nil)
	} else {
		err = client.Call("Master.RegisterReducer",&wk, nil)
	}

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
RPC Methods
--------------------------------------------------------------------------
*/

func (wk *WorkerConfig) Somefunc(msg string, job *Job) error {
	return nil
}

func (job *Job) MapJob(mrData* MRData, mrOutput* MRData) error {
	switch mrData.WorkerType{
	case Mapper:
		fmt.Printf("\nInitating Mapper Job...")
		map_out := mapOP(job.MapFunc,mrData.MapperInput)
		mrOutput.MapperOutput = map_out
		return nil
	case Reducer:
		// reduce_out := reduceOperation(job.RedFunc, mrData.reduceInput)
		fmt.Print("\nReduceJob\n")
		return nil
	default:
		fmt.Print("Undefined worker job type\n")
	}
	return nil
}

func (job *Job) ReduceJob(reduceData *ReducerData, reduceResult *ReducerData) error {
	fmt.Print("\nInitating Reduce Job...")
	reduceResult.Output = reduceOperation(job.RedFunc, reduceData.Input)
	return nil
}

/*
RPC Network Implementation
--------------------------------------------------------------------------
*/

func StartRPCClient(spawnChannel chan int, somechan chan bool,job *Job) {
	fmt.Print("\n> Initating connection with master")
	client, err := rpc.Dial("tcp", "0.0.0.0:12345")
	if err != nil {
	  log.Fatal(err)
	}

	l := string("Test String")

	err = client.Call("Master.EstConnection", l, &job)

	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("\n> Pusblising on mapper spawn channel")
	spawnChannel <- job.NMappers
	somechan <- true
	close(spawnChannel)
	close(somechan)
}

func StartWorkerRPCServer(wk *WorkerConfig, job *Job) {
	fmt.Printf("Starting Worker Server @%s...\n",wk.Address)
	address, err := net.ResolveTCPAddr("tcp", wk.Address)

	if err != nil {
		log.Fatal(err)
	}

	inbound, err := net.ListenTCP("tcp", address)

	if err != nil {
	 log.Fatal(err)
	}

	if(job != nil) {
		rpc.Register(job)
	}

	rpc.Register(wk)
	// go func(inbound *net.TCPListener, wk *WorkerConfig) {
		wk.ready = true
		rpc.Accept(inbound)

	// }(inbound, wk)
}