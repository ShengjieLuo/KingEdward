
/**********************************************************
 *             15-640 Distributed Computing               *
 *                    Project  2                          *
 *                 Tribbler Framework                     *
 **********************************************************
 * Introduction:                                          *
 * Tribbler is a distributed backend framework allowing   *
 * user to subscript the contents. Tribbler Server is the *
 * major part of the application server layer to extract  *
 * the business logic here.                               *
 **********************************************************
 * Author:                                                *
 * Shengjie Luo shengjil@andrew.cmu.edu                   *
 * Ke Chang     kec1@andrew.cmu.edu                       *
 **********************************************************/

package libstore

import (
	"errors"
	"fmt"
	"net/rpc"
	"time"
	"sort"

	"github.com/cmu440/tribbler/rpc/librpc"
	"github.com/cmu440/tribbler/rpc/storagerpc"
)

/********************************************
 * Data Structure Definition                *
 ********************************************/

/*Status=0: insert string
  Status=1: insert []string
*/
type cacheArgs struct {
	Status int
	Key    string
	ReplyA storagerpc.GetReply
	ReplyB storagerpc.GetListReply
}

/*Status=1: Not in Cache && Want Lease
  Status=2: Not in Cache && Donot want Lease
  Status=3: In Cache && return string
  Status=4: In Cache && return string list
*/
type cacheReply struct {
	Status int
	ValueA string
	ValueB []string
}

type hashClient struct {
	client *rpc.Client
	hash   uint32
}

/********************************************
 * Sort Helper Function                     *
 ********************************************/
type hashClientSlice []hashClient

func (c hashClientSlice) Len() int {
	return len(c)
}

func (c hashClientSlice) Swap(i, j int) {
	ele := hashClient{client:c[i].client,hash:c[i].hash}
	c[i].client = c[j].client
	c[i].hash   = c[j].hash
	c[j].client = ele.client
	c[j].hash   = ele.hash
}

func (c hashClientSlice) Less(i, j int) bool {
	return c[i].hash < c[j].hash
}


/********************************************
 * Major data structure for libstore        *
 ********************************************/
type libstore struct {
	Clock    int
	HostPort string
	Clients  hashClientSlice
	Lease    LeaseMode
	CacheA   map[string]string
	CacheB   map[string][]string
	//Get()/Getlist() asks Main Routine to find if they want lease
	QueryReqs chan string
	QueryReps chan cacheReply
	//Get() insert items into CacheA, GetList() insert items into CacheB
	Insert     chan cacheArgs
	InsertReps chan storagerpc.Status
	RemoveReqs chan string
	RemoveReps chan storagerpc.Status
	Ticker     chan int
}

/*********************************************
 * Routing Helper Function                   *
 * Find the workserver contains the key-value*
 *********************************************/
func getClient(ls *libstore, key string) *rpc.Client {
	value := StoreHash(key)
	if value < ls.Clients[0].hash||value > ls.Clients[len(ls.Clients)-1].hash {
		return ls.Clients[0].client
	}
	for _, cli := range ls.Clients {
		if cli.hash >= value {
			return cli.client
		}
	}
	return nil
}

/********************************************
 * Longtime -run routine:                   *
 * Time Routine: Keep to manage time        *
 * Main Routine: System Management          *
 ********************************************/
func timeRoutine(ls *libstore) {
	ticker := time.NewTicker(time.Second)
	for _ = range ticker.C {
		ls.Ticker <- 1
	}
}

func mainRoutine(ls *libstore) {
	queryList := make([][]string, storagerpc.QueryCacheSeconds)
	expireMap := make(map[string]int)
	queryUnit := make([]string, 0)
	for {
		select {
		case <-ls.Ticker:
			ls.Clock = ls.Clock + 1
			//0. Remove outdate queryunit
			queryList = queryList[1:]
			//1. Add new queryunit
			queryList = append(queryList, queryUnit)
			queryUnit = nil
			//2. Expire Items
			for key, value := range expireMap {
				if value == ls.Clock {
					delete(expireMap, key)
					_, ok1 := ls.CacheA[key]
					_, ok2 := ls.CacheB[key]
					if ok1 {
						delete(ls.CacheA, key)
					} else if ok2 {
						delete(ls.CacheB, key)
					}
				}
			}
		case req := <-ls.QueryReqs:
			//0. Find the key in CacheA
			valueA, ok := ls.CacheA[req]
			if ok {
				ls.QueryReps <- cacheReply{Status: 3, ValueA: valueA,
													  ValueB: nil}
				continue
			}
			//1. Find the key in CacheB
			valueB, ok := ls.CacheB[req]
			if ok {
				ls.QueryReps <- cacheReply{Status: 4, ValueA: "",
													  ValueB: valueB}
				continue
			}
			//2. Find the key required lease
			count := 0
			for _, queryu := range queryList {
				for _, query := range queryu {
					if query == req {
						count += 1
						if count >= storagerpc.QueryCacheThresh {
							break
						}
					}
				}
			}
			for _, query := range queryUnit {
				if query == req {
					count += 1
					if count >= storagerpc.QueryCacheThresh {
						break
					}
				}
			}
			queryUnit = append(queryUnit, req)
			if count >= storagerpc.QueryCacheThresh {
				ls.QueryReps <- cacheReply{Status: 1, ValueA: "", ValueB: nil}
			} else {
				ls.QueryReps <- cacheReply{Status: 2, ValueA: "", ValueB: nil}
			}
		case arg := <-ls.Insert:
			//0. Insert into cache
			if arg.Status == 0 {
				ls.CacheA[arg.Key] = arg.ReplyA.Value
			} else if arg.Status == 1 {
				ls.CacheB[arg.Key] = arg.ReplyB.Value
			}
			//1. Update ExpireMap
			if arg.Status == 0 {
				expireMap[arg.Key] = ls.Clock +
                                     arg.ReplyA.Lease.ValidSeconds +
                                     storagerpc.LeaseGuardSeconds - 1
			} else if arg.Status == 1 {
				expireMap[arg.Key] = ls.Clock +
                                     arg.ReplyB.Lease.ValidSeconds +
                                     storagerpc.LeaseGuardSeconds - 1
			}
			ls.InsertReps <- storagerpc.OK
		case rem := <-ls.RemoveReqs:
			//0. Remove from Cache
			_, ok1 := ls.CacheA[rem]
			_, ok2 := ls.CacheB[rem]
			if ok1 {
				delete(ls.CacheA, rem)
				ls.RemoveReps <- storagerpc.OK
			} else if ok2 {
				delete(ls.CacheB, rem)
				ls.RemoveReps <- storagerpc.OK
			} else {
				ls.RemoveReps <- storagerpc.KeyNotFound
			}
			//1. Update Expiremap
			delete(expireMap, rem)
		}
	}
}

/**********************************************
 *Interface Part                              *
 **********************************************/

//NewLibstore: Initialize the libstore
func NewLibstore(masterServerHostPort, myHostPort string, mode LeaseMode)
    (Libstore, error) {
	fmt.Printf("[libstore] Create new libstore\n")
	var cli *rpc.Client
	var err error
	cli, err = rpc.DialHTTP("tcp", masterServerHostPort)
	if err != nil {
		count:=0
		for {
			cli, err = rpc.DialHTTP("tcp", masterServerHostPort)
			if (err!=nil){
				time.Sleep(500 * time.Millisecond)
				count++
				if (count>10){
					return nil,err
				}
			} else {
				fmt.Printf("[libstore] Establish Connection-> Storage Master\n")
				break
			}
		}
	}
	fmt.Printf("[libstore] Establish Connection with Storage Server Master\n")
	//Get Routing Servers List
	clients := make(hashClientSlice, 0)
	retryCount := 0
	for {
		args := &storagerpc.GetArgs{}
		var reply storagerpc.GetServersReply
		errGetServer := cli.Call("StorageServer.GetServers", args, &reply)
		if errGetServer != nil {
			return nil, errGetServer
		}
		if reply.Status != storagerpc.OK {
			time.Sleep(1 * time.Second)
			if retryCount<20{
				retryCount += 1
				continue
			} else {
				return nil,errors.New("[fatal] Storage Server not ready 
                                       after 20 retries!\n")
			}
		}
		for _, node := range reply.Servers {
			client, errWorkServer := rpc.DialHTTP("tcp", node.HostPort)
			if errWorkServer != nil {
				return nil, errors.New("[fatal] Cannot connect work server!\n")
			}
			hashclient := hashClient{client, node.NodeID}
			clients = append(clients, hashclient)
		}
		sort.Sort(clients)
		fmt.Println(clients)
		break
	}
	fmt.Printf("[libstore] Get Work Server from storage server\n")

	cacheA := make(map[string]string)
	cacheB := make(map[string][]string)
	queryReqs := make(chan string)
	queryReps := make(chan cacheReply)
	insert := make(chan cacheArgs)
	insertReps := make(chan storagerpc.Status)
	ticker := make(chan int)
	removeReqs := make(chan string)
	removeReps := make(chan storagerpc.Status)
	libstore := libstore{Clients: clients, HostPort: myHostPort,
                         Lease: mode, CacheA: cacheA, CacheB: cacheB,
		                 QueryReqs: queryReqs, QueryReps: queryReps,
                         Insert: insert, InsertReps: insertReps,
		                 RemoveReqs: removeReqs, RemoveReps: removeReps,
                         Ticker: ticker, Clock: 0}
	ls := &libstore
	err = rpc.RegisterName("LeaseCallbacks", librpc.Wrap(ls))
	if err != nil {
		return nil, err
	}
	fmt.Printf("[libstore] Initialize data structure and register rpc\n")
	go timeRoutine(ls)
	go mainRoutine(ls)
	return ls, nil
}

/* Interface1: Get
   Get a key-value pair from backend storage server
*/
func (ls *libstore) Get(key string) (string, error) {
	//fmt.Printf("[libstore] Get(%s)\n", key)

	//Step0: Query Cache
	var leaseFlag bool
	ls.QueryReqs <- key
	rep := <-ls.QueryReps
	if rep.Status == 3 {
		return rep.ValueA, nil
	} else if rep.Status == 4 {
		return "", errors.New("Found incompatiable type in cache")
	} else if rep.Status == 1 {
		leaseFlag = true
	} else {
		leaseFlag = false
	}

	//Step1:Prepare data and configuration
	if ls.Lease == Always {
		leaseFlag = true
	} else if ls.Lease == Never {
		leaseFlag = false
	}
	args := &storagerpc.GetArgs{key, leaseFlag, ls.HostPort}

	//Step2: Send RPC request
	client := getClient(ls, key)
	var reply storagerpc.GetReply
	err := client.Call("StorageServer.Get", args, &reply)
	if err != nil {
		fmt.Println(err)
		return "", err
	}

	//Step3:Fetch reply
	if leaseFlag && reply.Lease.Granted {
		emptyReplyB := storagerpc.GetListReply{Status: 0, Value: nil,
           Lease: storagerpc.Lease{Granted: false, ValidSeconds: 0}}
		ls.Insert <- cacheArgs{Status: 0, Key: key, ReplyA: reply,
                                                    ReplyB: emptyReplyB}
		<-ls.InsertReps
	}
	if reply.Status == storagerpc.OK {
		return reply.Value, nil
	} else {
		return reply.Value, errors.New(string(reply.Status))
	}
}

/* Interface2: Put
   Put a key-value to from backend storage server
*/
func (ls *libstore) Put(key, value string) error {
	if len(value) < 30 {
		//fmt.Printf("[libstore] Put(%s->%s)\n", key, value)
	} else {
		//fmt.Printf("[libstore] Put(%s)\n",key)
	}
	args := &storagerpc.PutArgs{key, value}
	var reply storagerpc.PutReply
	client := getClient(ls, key)
	err := client.Call("StorageServer.Put", args, &reply)
	if err != nil {
			fmt.Printf("[Fatal] Put key-value crash:%d!",reply.Status)
		return err
	}
	if reply.Status == storagerpc.OK {
		//fmt.Printf("[libstore] Put ok\n")
		return nil
	} else {
		fmt.Printf("[Fatal] Put reply error status %d!\n", reply.Status)
		return errors.New(string(reply.Status))
	}
}

/* Interface3: Delete
   Delete a key-value to from backend storage server
*/
func (ls *libstore) Delete(key string) error {
	//fmt.Printf("[libstore] Delete(%s)\n",key)
	args := &storagerpc.DeleteArgs{key}
	var reply storagerpc.DeleteReply
	client := getClient(ls, key)
	err := client.Call("StorageServer.Delete", args, &reply)
	if err != nil {
		fmt.Println(err)
		return err
	}
	if reply.Status == storagerpc.OK {
		return nil
	} else {
		return errors.New(string(reply.Status))
	}
}

/* Interface4: GetList
   Get a key-valuelist to from backend storage server
*/
func (ls *libstore) GetList(key string) ([]string, error) {
	//fmt.Printf("[libstore] GetList(%s)\n",key)
	//Step0: Quer Cache
	var leaseFlag bool
	ls.QueryReqs <- key
	rep := <-ls.QueryReps
	if rep.Status == 4 {
		return rep.ValueB, nil
	} else if rep.Status == 3 {
		return nil, errors.New("Found incompatiable type in cache")
	} else if rep.Status == 1 {
		leaseFlag = true
	} else {
		leaseFlag = false
	}

	//Step1: Prepare data and configuration
	if ls.Lease == Always {
		leaseFlag = true
	} else if ls.Lease == Never {
		leaseFlag = false
	}
	args := &storagerpc.GetArgs{key, leaseFlag, ls.HostPort}

	//Step2:Send rpc request
	client := getClient(ls, key)
	var reply storagerpc.GetListReply
	err := client.Call("StorageServer.GetList", args, &reply)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	//Step2:Fetch reply
	if leaseFlag && reply.Lease.Granted {
		emptyReplyA := storagerpc.GetReply{Status: 0, Value: "",
            Lease: storagerpc.Lease{Granted: false, ValidSeconds: 0}}
		ls.Insert <- cacheArgs{Status: 1, Key: key, ReplyA: emptyReplyA,
                                                    ReplyB: reply}
		<-ls.InsertReps
	}
	if reply.Status == storagerpc.OK {
		return reply.Value, nil
	} else {
		return reply.Value, errors.New(string(reply.Status))
	}
}

/* Interface5: RemoveFromList
   Remove an item from key-valuelist in backend storage server
*/
func (ls *libstore) RemoveFromList(key, removeItem string) error {
	if len(removeItem) < 30 {
		//fmt.Printf("[libstore] RemoveFromList(%s->%s)\n",key,removeItem)
	}
	args := &storagerpc.PutArgs{key, removeItem}
	var reply storagerpc.PutReply
	client := getClient(ls, key)
	err := client.Call("StorageServer.RemoveFromList", args, &reply)
	if err != nil {
		fmt.Println(err)
		return err
	}
	if reply.Status == storagerpc.OK {
		return nil
	} else {
		fmt.Printf("[libstore] RemoveFromList Error Reply:%d\n",reply.Status)
		return errors.New(string(reply.Status))
	}
}

/* Interface6: AppendToList
   Append an item to key-valuelist in backend storage server
*/
func (ls *libstore) AppendToList(key, newItem string) error {
	if len(newItem) < 30 {
		//fmt.Printf("[libstore] AppendToList(%s->%s)\n",key,newItem)
	}
	args := &storagerpc.PutArgs{key, newItem}
	client := getClient(ls, key)
	var reply storagerpc.PutReply
	err := client.Call("StorageServer.AppendToList", args, &reply)
	if err != nil {
		fmt.Println(err)
		return err
	}
	if reply.Status == storagerpc.OK {
		return nil
	} else {
		fmt.Printf("[libstore] AppendToList Error Reply:%d\n",reply.Status)
		return errors.New(string(reply.Status))
	}
}

/* Interface7: Revoke Lease
   State that one lease in the cache is expired
*/
func (ls *libstore) RevokeLease(args *storagerpc.RevokeLeaseArgs, reply *storagerpc.RevokeLeaseReply) error {
	//fmt.Printf("[libstore] RevokeLease(%s)\n",args.Key)
	ls.RemoveReqs <- args.Key
	reply.Status = <-ls.RemoveReps
	return nil
}
