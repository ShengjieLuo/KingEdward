/******************************************************************
 **                    15-640 Project1 CheckPoint3               **
 **                     Distributed Bitcoin Miner                **
 ******************************************************************
 **Intorduction:                                                 **
 **1. Build upon the LSP protocol( client end).                  **
 **2. Implementation of the distributed bitcoin miner server     **
 **3. The load-balance Policy:                                   **
 **   (1) The request would be divided into several small tasks  **
 **   (2) Each Task would be scheduled into one miner            **
 **   (3) If there's no miner, it would be stored in a buffer list*
 **   (4) Buffer tasks would be rescheduled if miner available   **
 ******************************************************************
 **Author:                                                       **
 ** Shengjie Luo shengjil@andrew.cmu.edu                         **
 ** Ke Chang     kec1@andrew.cmu.edu                             **
 ******************************************************************
 */

package main

import (
	"container/list"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/cmu440/bitcoin"
	"github.com/cmu440/lsp"
)

//Const Variables
const (
	AVAILABLE     = 1
	INUSE         = 0
	MINER         = 0
	REQUEST       = 1
	taskPerWorker = 2000
)

//Parameter Variables
var (
	epochLimit = flag.Int(
		"elim", lsp.DefaultEpochLimit, "epoch limit")
	epochMillis = flag.Int(
		"ems", lsp.DefaultEpochMillis, "epoch duration (ms)")
	windowSize = flag.Int(
		"wsize", lsp.DefaultWindowSize, "window size")
	maxBackoff = flag.Int(
		"maxbackoff", lsp.DefaultMaxBackOffInterval, "maximum interval epoch")
	LOGF *log.Logger
)

//Result: Struct to store result
type Result struct {
	Hash  uint64
	Nonce uint64
}

//Request: Struct to store request from clients
type Request struct {
	Id          int
	Data        string
	Lower       uint64
	Upper       uint64
	Conn        int
	totalUnits  int
	finishUnits int
	workers     map[int]Result
}

//Miner: Struct to store miner status
type Miner struct {
	Id     int
	Conn   int
	Status int
	Req    int
	Task   []byte
}

//Task: Struct to store task
type Task struct {
	Req int
	Msg []byte
}

//Server: Key Data Structure
//1. lspServer: The server in lsplayer
//2. requests: A golang map to store requests
//3. miners: A golang map to store miners
//4. availMiners: A linked list to store available miners
//5. bufferTasks: A linked list to store buffer tasks
//6. reqCount,minCount: Counter
type server struct {
	lspServer   lsp.Server
	requests    map[int]Request
	miners      map[int]Miner
	availMiners *list.List
	bufferTasks *list.List
	reqCount    int
	minCount    int
}

/*StartServer:
  1. Initialize the server structure
  2. Launch server to accept message
*/
func startServer(port int) (*server, error) {
	LOGF.Printf("[start] Start Server Begin!")
	params := &lsp.Params{
		EpochLimit:         *epochLimit,
		EpochMillis:        *epochMillis,
		WindowSize:         *windowSize,
		MaxBackOffInterval: *maxBackoff,
	}
	portFlag := flag.Int("port", port, "port number")
	srv, err := lsp.NewServer(*portFlag, params)
	if err != nil {
		return nil, errors.New("Cannot start server")
	}

	requests := make(map[int]Request)
	miners := make(map[int]Miner)
	availMiners := list.New()
	bufferTasks := list.New()
	reqCount := 0
	minCount := 0
	s := &server{srv, requests, miners,
		availMiners, bufferTasks, reqCount, minCount}
	LOGF.Printf("[start] Start Server Finish!")
	return s, nil
}

//InitLogger: Inital the log module
func initLogger() *log.Logger {
	const (
		name = "log.txt"
		flag = os.O_RDWR | os.O_CREATE
		perm = os.FileMode(0666)
	)
	file, err := os.OpenFile(name, flag, perm)
	if err != nil {
		return nil
	}
	defer file.Close()
	return log.New(file, "", log.Lshortfile|log.Lmicroseconds)
}

//readParameter: Read Parameter from command line
func readParameter() int {
	const numArgs = 2
	if len(os.Args) != numArgs {
		fmt.Printf("Usage: ./%s <port>", os.Args[0])
		return -1
	}

	port, err := strconv.Atoi(os.Args[1])
	if err != nil {
		fmt.Println("Port must be a number:", err)
		return -1
	}
	return port
}

//extractInfo: Unmarshal the payload and return message
func extractInfo(payload []byte) *bitcoin.Message {
	var msg = new(bitcoin.Message)
	err := json.Unmarshal(payload, msg)
	if err != nil {
		LOGF.Printf("Error: Message Payload cannot be unmarshalled!\n")
	}
	if msg.Data == "" && msg.Upper == 0 && msg.Hash == 0 {
		LOGF.Printf("Error: Unmarshalled Message is empty")
	}
	return msg
}

//addRequest: Add a new request into requests map
func addRequest(srv *server, m *bitcoin.Message, id int) *Request {
	//1. Initialize req.worker
	workers := make(map[int]Result)
	//2. Initialize req.totalunits
	taskCount := int(m.Upper - m.Lower + 1)
	workersCount := taskCount / taskPerWorker
	if taskCount-taskPerWorker*workersCount > 0 {
		workersCount = workersCount + 1
	}
	//3. Initialize other parts of req and put it into requests map
	req := Request{srv.reqCount, m.Data,
		m.Lower, m.Upper, id, workersCount, 0, workers}
	srv.requests[req.Conn] = req
	srv.reqCount = srv.reqCount + 1
	return &req
}

//addMiner: Add a new miner into miners map
func addMiner(srv *server, id int) {
	miner := Miner{srv.minCount, id, AVAILABLE, -1, nil}
	srv.miners[miner.Conn] = miner
	srv.minCount = srv.minCount + 1
	srv.availMiners.PushBack(miner.Conn)
}

//scheduleMiner: Schedule the request into miners
func scheduleMiner(srv *server, req *Request) {
	minersCount := srv.availMiners.Len()
	var i int
	for i = 0; i < req.totalUnits; i++ {
		//Calculate the task range first
		lo := int(req.Lower) + i*taskPerWorker
		hi := int(req.Lower) + (i+1)*taskPerWorker
		if i == req.totalUnits-1 {
			hi = int(req.Upper) + 1
		}
		//If there is available miner, then use it as worker
		if i < minersCount {
			m := bitcoin.NewRequest(req.Data, uint64(lo), uint64(hi))
			conn := srv.availMiners.Front()
			miner := srv.miners[conn.Value.(int)]
			miner.Req = req.Conn
			miner.Status = INUSE
			miner.Task, _ = json.Marshal(m)
			srv.miners[conn.Value.(int)] = miner
			srv.lspServer.Write(miner.Conn, miner.Task)
			srv.availMiners.Remove(conn)
		} else {
			//If there is no available miner, then put it into bufferTasks
			m := bitcoin.NewRequest(req.Data, uint64(lo), uint64(hi))
			msg, _ := json.Marshal(m)
			task := Task{req.Conn, msg}
			srv.bufferTasks.PushBack(task)
		}
	}
	return
}

//updateRequest: Update the request status if one task is completed
func updateRequest(srv *server, m *bitcoin.Message, conn int) {
	miner := srv.miners[conn]
	req, existFlag := srv.requests[miner.Req]
	tmp := miner.Req
	miner.Req = -1
	miner.Status = AVAILABLE
	miner.Task = nil
	srv.miners[conn] = miner
	srv.availMiners.PushBack(conn)
	if !existFlag {
		return
	}
	req.workers[req.finishUnits] = Result{m.Hash, m.Nonce}
	req.finishUnits = req.finishUnits + 1
	srv.requests[tmp] = req
	var resultHash uint64
	var resultNonce uint64
	if req.finishUnits == req.totalUnits {
		for _, v := range req.workers {
			if resultHash == 0 {
				resultHash = v.Hash
				resultNonce = v.Nonce
			} else if v.Hash < resultHash {
				resultHash = v.Hash
				resultNonce = v.Nonce
			}
		}
		_, ok := srv.requests[req.Conn]
		if !ok {
			return
		}
		m := bitcoin.NewResult(resultHash, resultNonce)
		msg, _ := json.Marshal(m)
		srv.lspServer.Write(req.Conn, msg)
		delete(srv.requests, req.Conn)
		return
	}
	return
}

//judgeLoss: Judge the connection is from miner or from request
func judgeLoss(srv *server, conn int) int {
	_, ok := srv.miners[conn]
	if ok {
		return MINER
	} else {
		return REQUEST
	}
}

//readRoutine: read message and deal with it
func readRoutine(srv *server) {
	for {
		if id, payload, err := srv.lspServer.Read(); err != nil {
			LOGF.Printf("[read] Client %d has died: %s\n", id, err)
			switch judgeLoss(srv, id) {
			case MINER:
				miner := srv.miners[id]
				delete(srv.miners, id)
				flag := true
				for iter := srv.availMiners.Front(); iter != nil; iter = iter.Next() {
					if iter.Value.(int) == id {
						srv.availMiners.Remove(iter)
						flag = false
						break
					}
				}
				//If the lost miner is a worker miner
				if flag {
					task := Task{miner.Req, miner.Task}
					srv.bufferTasks.PushBack(task)
				}
			case REQUEST:
				delete(srv.requests, id)
			}
		} else {
			m := extractInfo(payload)
			switch m.Type {

			case bitcoin.Request:
				req := addRequest(srv, m, id)
				scheduleMiner(srv, req)

			case bitcoin.Join:
				addMiner(srv, id)
				for srv.availMiners.Len() > 0 {
					if srv.bufferTasks.Len() != 0 {
						//1. Fetch the task from bufferTasks
						taskele := srv.bufferTasks.Front()
						srv.bufferTasks.Remove(taskele)
						task := taskele.Value.(Task)
						//2. Fetch the miner from availMiners
						conn := srv.availMiners.Front()
						miner := srv.miners[conn.Value.(int)]
						miner.Req = task.Req
						miner.Status = INUSE
						miner.Task = task.Msg
						srv.miners[conn.Value.(int)] = miner
						srv.lspServer.Write(miner.Conn, miner.Task)
						srv.availMiners.Remove(conn)
					} else {
						break
					}
				}

			case bitcoin.Result:
				LOGF.Printf("[%d] Server begin dealing RESULT message\n", id)
				updateRequest(srv, m, id)
				for srv.availMiners.Len() > 0 {
					if srv.bufferTasks.Len() != 0 {
						//1. Fetch the task from bufferTasks
						taskele := srv.bufferTasks.Front()
						srv.bufferTasks.Remove(taskele)
						task := taskele.Value.(Task)
						//2. Fetch the miner from availMiners
						conn := srv.availMiners.Front()
						miner := srv.miners[conn.Value.(int)]
						miner.Req = task.Req
						miner.Status = INUSE
						miner.Task = task.Msg
						srv.miners[conn.Value.(int)] = miner
						srv.lspServer.Write(miner.Conn, miner.Task)
						srv.availMiners.Remove(conn)
					} else {
						break
					}
				}
			default:
				LOGF.Printf("[%d] Message Type Could not be identified\n", id)
				continue
			}
		}
	}
}

func main() {
	port := readParameter()
	LOGF = initLogger()
	srv, err := startServer(port)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Println("Server listening on port", port)
	defer srv.lspServer.Close()
	readRoutine(srv)
}
