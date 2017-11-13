package libstore

import (
	"errors"
	"fmt"
	"net/rpc"
	"time"

	"github.com/cmu440/tribbler/rpc/storagerpc"
	//"github.com/cmu440/tribbler/rpc/librpc"
	//"github.com/cmu440/tribbler/storageserver"
)

/*Status=0: insert string
  Status=1: insert []string
*/
type cacheArgs struct {
    Status	int
	Key		string
	ReplyA	storagerpc.GetReply
	ReplyB  storagerpc.GetListReply
}

/*Status=1: Not in Cache && Want Lease
  Status=2: Not in Cache && Donot want Lease
  Status=3: In Cache && return string
  Status=4: In Cache && return string list
*/
type cacheReply struct {
	Status	int
	ValueA	string
	ValueB	[]string
}


type libstore struct {
	HostPort   string
	client     *rpc.Client
	Lease      LeaseMode
	CacheA     map[string]string
	CacheB     map[string][]string
	//Get()/Getlist() asks Main Routine to find if they want lease
	QueryReqs  chan string
	QueryReps  chan cacheReply
	//Get() insert items into CacheA, GetList() insert items into CacheB
	Insert     chan cacheArgs
	RemoveReqs chan string
	RemoveReps chan storagerpc.Status
	Ticker     chan int
}

func timeRoutine(ls *libstore) {
    i := 1
    ticker := time.NewTicker(time.Second)
    for _ = range ticker.C {
		ls.Ticker <- i
		i += 1
    }
}

func mainRoutine(ls *libstore) {
	queryList  := make([][]string,storagerpc.QueryCacheSeconds)
	expireMap  := make(map[string]int)
	timeTicker := 0
	queryUnit  := make([]string)
	for {
		select{
			case timeTicker:<-ls.Ticker:
				//0. Remove outdate queryunit
				queryList = append(queryList[:0], queryList[1:]...)
				//1. Add new queryunit
				queryList = append(queryList,queryUnit)
				queryUnit = queryUnit[:0]
				//2. Expire Items
				for key,value := range expireMap {
					if value==timeTicker {
						delete(expireMap,key)
						_,ok1 := ls.CacheA[key];
						_,ok2 := ls.CacheB[key];
						if ok1 {
							delete(ls.CacheA,key);
						} else if ok2 {
							delete(ls.CacheB,key);
						}
					}
				}
			case req:<-ls.QueryReqs:
				//0. Find the key in CacheA
				value,ok := ls.CacheA[req]
				if ok{
					ls.QueryReps <- cacheReply{Status:3,ValueA:value,ValueB:nil}
					continue
				}
				//1. Find the key in CacheB
				value,ok := ls.CacheB[req]
				if ok{
					ls.QueryReps <- cacheReply{Status:4,ValueA:nil,ValueB:value}
					continue
				}
				//2. Find the key required lease
				count := 0
				for _,queryUnit := range(queryList){
					for _,query := range(queryUnit) {
						if query==req {
							count += 1
							if count>=storage {
								break;
							}
						}
					}
				}
				queryUnit = append(queryUnit,req)
				if count>=storage {
					ls.QueryReps <- cacheReply{Status:1,ValueA:nil,ValueB:nil}
				} else {
					ls.QueryReps <- cacheReply{Status:2,ValueA:nil,ValueB:nil}
				}
			case arg:<-ls.Insert:
				//0. Insert into cache
				if arg.Status==0{
					ls.CacheA[arg.Key] = arg.ValueA
				} else if arg.Status==1 {
					ls.CacheB[arg.Key] = arg.ValueB
				}
				//1. Update ExpireMap
				expireMap[arg.Key] = timeTicker + arg.Lease.LeaseSeconds + arg.Lease.LeaseGuardSeconds
			case rem:<-ls.RemoveReqs:
				//0. Remove from Cache
				_,ok1 := ls.CacheA[rem];
				_,ok2 := ls.CacheB[rem];
				if ok1 {
					delete(ls.CacheA,rem);
					ls.RemoveReps<-storagerpc.OK
				} else if ok2 {
					delete(ls.CacheB,rem);
					ls.RemoveReps<-storagerpc.OK
				} else {
					ls.RemoveReps<-storagerpc.KeyNotFound
				}
				//1. Update Expiremap
				delete(expireMap,arg.Key)
		}
	}
}

func NewLibstore(masterServerHostPort, myHostPort string, mode LeaseMode) (Libstore, error) {
	cli, err := rpc.DialHTTP("tcp", masterServerHostPort)
	if err != nil {
		return nil, err
	}
	cacheA    := make(map[string]string)
	cacheB    := make(map[string][]string)
	queryReqs := make(chan string)
	queryReps := make(chan cacheReply)
	insert    := make(chan cahceArgs)
	ticker    := make(chan int)
	removeReqs:= make(chan string)
	removeReps:= make(chan storagerpc.Status)
	ls        := &libstore{client: cli, HostPort: myHostPort, Lease:mode, CacheA:cacheA, CacheB:cacheB,
		      QueryReqs: queryReqs, QueryReps: queryReps, InsertA: insertA, InsertB: insertB,
              RemoveReqs:removeReqs, RemoveReps:removeReps,Ticker:ticker}
	go timeRoutine(ls)
	go mainRoutine(ls)
	return ls,nil
}

func (ls *libstore) Get(key string) (string, error) {
	fmt.Printf("[libstore] Get(%s)\n",key)

    //Step0: Quer Cache
	var leaseFlag bool
	ls.QueryReqs <- key
	rep :<-ls.QuerReps
	if (rep.Status==3){
		return rep.ValueA,nil
	} else if (rep.Status==4){
		return nil,errors.New("Found incompatiable type in cache")
	} else if (rep.Status==1){
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
	var reply storagerpc.GetReply
	err := ls.client.Call("StorageServer.Get", args, &reply)
	if err != nil {
		fmt.Println(err)
		return "", err
	}

	//Step3:Fetch reply
	if leaseFlag && reply.Lease.Granted {
		insert <-cacheArgs{Status:0,Key:key,ReplyA:reply,ReplyB:nil}
	}
	if reply.Status == storagerpc.OK {
		return reply.Value, nil
	} else {
		return reply.Value, errors.New(string(reply.Status))
	}
}

func (ls *libstore) Put(key, value string) error {
	fmt.Printf("[libstore] Put(%s->%s)\n",key,value)
	args := &storagerpc.PutArgs{key, value}
	var reply storagerpc.PutReply
	err := ls.client.Call("StorageServer.Put", args, &reply)
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

func (ls *libstore) Delete(key string) error {
	//fmt.Printf("[libstore] Delete(%s)\n",key)
	args := &storagerpc.DeleteArgs{key}
	var reply storagerpc.DeleteReply
	err := ls.client.Call("StorageServer.Delete", args, &reply)
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

func (ls *libstore) GetList(key string) ([]string, error) {
	fmt.Printf("[libstore] GetList(%s)\n",key)
	//Step0: Quer Cache
	var leaseFlag bool
	var rep cacheReply
	ls.QueryReqs <- key
	rep <-ls.QuerReps
	if (rep.Status==4){
		return rep.ValueB,nil
	} else if (rep.Status==3){
		return nil,errors.New("Found incompatiable type in cache")
	} else if (rep.Status==1){
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
	var reply storagerpc.GetListReply
	err := ls.client.Call("StorageServer.GetList", args, &reply)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	//Step2:Fetch reply
	if leaseFlag && reply.Lease.Granted {
		insert <-cacheArgs{Status:1,Key:key,ReplyA:nil,ReplyB:reply}
	}
	if reply.Status == storagerpc.OK {
		return reply.Value, nil
	} else {
		return reply.Value, errors.New(string(reply.Status))
	}
}

func (ls *libstore) RemoveFromList(key, removeItem string) error {
	//fmt.Printf("[libstore] RemoveFromList(%s->%s)\n",key,removeItem)
	args := &storagerpc.PutArgs{key, removeItem}
	var reply storagerpc.PutReply
	err := ls.client.Call("StorageServer.RemoveFromList", args, &reply)
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

func (ls *libstore) AppendToList(key, newItem string) error {
	//fmt.Printf("[libstore] AppendToList(%s->%s)\n",key,newItem)
	args := &storagerpc.PutArgs{key, newItem}
	var reply storagerpc.PutReply
	err := ls.client.Call("StorageServer.AppendToList", args, &reply)
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

func (ls *libstore) RevokeLease(args *storagerpc.RevokeLeaseArgs, reply *storagerpc.RevokeLeaseReply) error {
	ls.RemoveReqs <- args.Key
	reply.Status  <- ls.RemoveReps
	return nil
}
