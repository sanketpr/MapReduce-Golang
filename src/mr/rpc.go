package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

// func (mr *Master) startRPCServer() {
// 	rpcs := rpc.NewServer()
// 	rpcs.Register(mr)
// 	os.Remove(mr.address) // only needed for "unix"
// 	l, e := net.Listen("unix", mr.address)
// 	if e != nil {
// 		log.Fatal("RegstrationServer", mr.address, " error: ", e)
// 	}
// 	mr.listener = l

// 	// now that we are listening on the master address, can fork off
// 	// accepting connections to another thread.
// 	go func() {
// 	loop:
// 		for {
// 			select {
// 			case <-mr.shutdown:
// 				break loop
// 			default:
// 			}
// 			conn, err := mr.listener.Accept()
// 			if err == nil {
// 				go func() {
// 					rpcs.ServeConn(conn)
// 					conn.Close()
// 				}()
// 			} else {
// 				fmt.Print("RegistrationServer: accept error", err)
// 				break
// 			}
// 		}
// 		fmt.Print("RegistrationServer: done\n")
// 	}()
// }
