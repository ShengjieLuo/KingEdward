package storageserver

import (
	"fmt"
	"container/list"
	"github.com/cmu440/tribbler/rpc/storagerpc"
	"github.com/cmu440/tribbler/libstore"
	"net"
	"net/http"
	"net/rpc"
	"time"
	"sync"
	"math"
	"strings"
)

const leaseSeconds = storagerpc.LeaseSeconds + storagerpc.LeaseGuardSeconds

type leaseTracker struct {
	hostport  string
	grantedAt time.Time
}

type storageServer struct {
	numNodes int
	nodeID uint32
	joinedNode int
	initDone bool
	serverList []storagerpc.Node
	//initDoneRequest chan int
	//initDoneChanel chan int
	newNodesChanel chan storagerpc.Node
	newNodesResult chan storagerpc.RegisterReply
	postData map[string]string
	listData map[string]*list.List
	canLease map[string]bool
	leaseOwner map[string]*list.List
	cachedConn map[string]*rpc.Client
	postLock	*sync.Mutex
	listLock	*sync.Mutex
}

func (ss *storageServer)rightServer(key string) (res bool){
	userInfo := strings.Split(key, ":")[0]
	userHash := libstore.StoreHash(userInfo)
	var curCandidate uint32 = math.MaxUint32
	found := false
	for _, curServer := range ss.serverList{
		if curServer.NodeID < curCandidate && curServer.NodeID >= userHash{
			curCandidate = curServer.NodeID
			found = true
		}
	}
	if found{
		res = (curCandidate == ss.nodeID)
	}else{
		res = true
		for _, curServer := range ss.serverList{
			if curServer.NodeID < ss.nodeID{
				res = false
				break
			}
		}
	}
	return 
}


func (ss *storageServer)grantLease(key, hostport string)(lease storagerpc.Lease){
	_, ok := ss.cachedConn[hostport]
	if !ok{
		newConn, _ := rpc.DialHTTP("tcp", hostport)
		ss.cachedConn[hostport] = newConn
	}

	lease = storagerpc.Lease{}
	val, leaseok := ss.canLease[key]
	if (!leaseok || val){
		lease.Granted = true
		lease.ValidSeconds = storagerpc.LeaseSeconds
		_, grantok := ss.leaseOwner[key]
		if !grantok{
			ss.leaseOwner[key] = list.New()
		}
		ss.leaseOwner[key].PushBack(&leaseTracker{hostport, time.Now()})
	}else{
		lease.Granted = false
		lease.ValidSeconds = 0
	}
	return
}

func (ss *storageServer)revokeLease(key string){
	ss.canLease[key] = false

	owners, ok := ss.leaseOwner[key]
	if !ok{
		return
	}
	for element := owners.Front(); element != nil; element = element.Next(){
		eachOwner := element.Value.(*leaseTracker)
		timeRemaining := leaseSeconds - time.Since(eachOwner.grantedAt).Seconds()
		if timeRemaining >= 0{
			revokeArgs := storagerpc.RevokeLeaseArgs{key}
			revokeReply := storagerpc.RevokeLeaseReply{}
			curConn := ss.cachedConn[eachOwner.hostport]
			callRes := curConn.Go("LeaseCallbacks.RevokeLease", &revokeArgs, &revokeReply, nil)
			select{
			case <-callRes.Done:
				break
			case <-time.After(time.Duration(timeRemaining) * time.Second):	
				break
			}	
		}
		ss.leaseOwner[key].Remove(element)
	}
	return

}

// NewStorageServer creates and starts a new StorageServer. masterServerHostPort
// is the master storage server's host:port address. If empty, then this server
// is the master; otherwise, this server is a slave. numNodes is the total number of
// servers in the ring. port is the port number that this server should listen on.
// nodeID is a random, unsigned 32-bit ID identifying this server.
//
// This function should return only once all storage servers have joined the ring,
// and should return a non-nil error if the storage server could not be started.
func NewStorageServer(masterServerHostPort string, numNodes, port int, nodeID uint32) (StorageServer, error) {
	
	s := storageServer{numNodes, nodeID, 1, false,
		make([]storagerpc.Node, numNodes),
		make(chan storagerpc.Node),
		make(chan storagerpc.RegisterReply),
		make(map[string]string),
		make(map[string]*list.List),
		make(map[string]bool),
		make(map[string]*list.List),
		make(map[string]*rpc.Client),
		&sync.Mutex{},
		&sync.Mutex{},
	}
	
	fullAddress := fmt.Sprintf("localhost:%d",port)
	listener, _ := net.Listen("tcp", fullAddress)
	rpc.RegisterName("StorageServer", storagerpc.Wrap(&s))
	rpc.HandleHTTP()
	go http.Serve(listener, nil)
	selfNode := storagerpc.Node{fullAddress, nodeID}
	if masterServerHostPort == ""{
		s.serverList[0] = selfNode
		for !s.initDone{
			select{
			//case <- s.initDoneRequest:
			//	if s.initDone{
			//		s.initDoneChanel <- 1
			//	}else{
			//		s.initDoneChanel <- 0
			//	}
				
			case newNode := <- s.newNodesChanel:
				flag := false
				for i:= range s.serverList{
					if s.serverList[i] == newNode{
						flag = true
					}
				}
				if !flag{
					s.serverList[s.joinedNode] = newNode
					s.joinedNode += 1
				}
			
				reply := storagerpc.RegisterReply{storagerpc.OK, s.serverList}
				if s.joinedNode < numNodes{
					reply = storagerpc.RegisterReply{storagerpc.NotReady,nil}
				}else{
					s.initDone = true
				}
				s.newNodesResult <- reply
			}
		}
	}else{
		connWithMaster, _ := rpc.DialHTTP("tcp", masterServerHostPort)
		args, reply := storagerpc.RegisterArgs{selfNode}, storagerpc.RegisterReply{}
		for{
			connWithMaster.Call("StorageServer.RegisterServer", &args, &reply)
			if reply.Status  == storagerpc.OK{
				s.initDone = true
				s.joinedNode = numNodes
				s.serverList = reply.Servers
				break
			}
			time.Sleep(time.Second)
		}
	}
	return &s, nil
}


func (ss *storageServer) RegisterServer(args *storagerpc.RegisterArgs, reply *storagerpc.RegisterReply) error {
	reply.Status = storagerpc.OK
	reply.Servers = ss.serverList
	//ss.initDoneRequest <- 1
	//isDone := <- ss.initDoneChanel
	//if isDone != 1{
	if !ss.initDone{
		ss.newNodesChanel <- args.ServerInfo
		*reply = <- ss.newNodesResult 
	}
	return nil
}

func (ss *storageServer) GetServers(args *storagerpc.GetServersArgs, reply *storagerpc.GetServersReply) error {
	reply.Status = storagerpc.OK
	reply.Servers = ss.serverList
	//ss.initDoneRequest <- 1
	//isDone := <- ss.initDoneChanel
	//if isDone!=1 {
	if !ss.initDone{
		reply.Status = storagerpc.NotReady
		reply.Servers = nil
	}
	return nil
}

func (ss *storageServer) Get(args *storagerpc.GetArgs, reply *storagerpc.GetReply) error {
	if !ss.rightServer(args.Key){
		reply.Status = storagerpc.WrongServer
		return nil
	}
	ss.postLock.Lock()
	val, ok := ss.postData[args.Key]
	if !ok{
		reply.Status = storagerpc.KeyNotFound
	}else{
		if args.WantLease{
			reply.Lease = ss.grantLease(args.Key, args.HostPort)
		}
		reply.Status = storagerpc.OK
		reply.Value = val
	}
	ss.postLock.Unlock()
	return nil
}

func (ss *storageServer) GetList(args *storagerpc.GetArgs, reply *storagerpc.GetListReply) error {
	if !ss.rightServer(args.Key){
		reply.Status = storagerpc.WrongServer
		return nil
	}
	ss.listLock.Lock()
	val, ok := ss.listData[args.Key]
	if !ok{
		reply.Status = storagerpc.KeyNotFound
	}else{
		reply.Status = storagerpc.OK
		reply.Value = make([]string, val.Len())
		if args.WantLease{
			reply.Lease = ss.grantLease(args.Key, args.HostPort)
		}

		for idx, element := 0, val.Front(); element != nil; element, idx = element.Next(), idx+1{
			reply.Value[idx] = element.Value.(string)
		}

	}
	ss.listLock.Unlock()
	return nil
}

func (ss *storageServer) Put(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	if !ss.rightServer(args.Key){
		reply.Status = storagerpc.WrongServer
		return nil
	}
	ss.revokeLease(args.Key)

	ss.postLock.Lock()
	ss.postData[args.Key] = args.Value
	ss.canLease[args.Key] = true
	reply.Status = storagerpc.OK
	ss.postLock.Unlock()
	return nil
}

func (ss *storageServer) AppendToList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	if !ss.rightServer(args.Key){
		reply.Status = storagerpc.WrongServer
		return nil
	}
	ss.revokeLease(args.Key)
	ss.listLock.Lock()

	_, ok := ss.listData[args.Key]
	if !ok{
		ss.listData[args.Key] = list.New()
	}

	flag := false
	for element := ss.listData[args.Key].Front(); element != nil; element = element.Next(){
		if args.Value == element.Value.(string){
			reply.Status = storagerpc.ItemExists
			flag = true
		}
	}

	if !flag{
		reply.Status = storagerpc.OK
		ss.listData[args.Key].PushBack(args.Value)	
	}
	ss.canLease[args.Key] = true	
	ss.listLock.Unlock()
	return nil
}

func (ss *storageServer) RemoveFromList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	if !ss.rightServer(args.Key){
		reply.Status = storagerpc.WrongServer
		return nil
	}
	ss.revokeLease(args.Key)
	
	ss.listLock.Lock()

	val, ok := ss.listData[args.Key]
	if !ok{
		reply.Status = storagerpc.ItemNotFound
	}
	flag := false
	for element := val.Front(); element != nil; element = element.Next(){
		if args.Value == element.Value.(string){
			val.Remove(element)
			reply.Status = storagerpc.OK
			flag = true
		}
	}
	if !flag{
		reply.Status = storagerpc.ItemNotFound
	}
	ss.canLease[args.Key] = true
	ss.listLock.Unlock()
	return nil
}

func (ss *storageServer) Delete(args *storagerpc.DeleteArgs, reply *storagerpc.DeleteReply) error {
	if !ss.rightServer(args.Key){
		reply.Status = storagerpc.WrongServer
		return nil
	}

	ss.revokeLease(args.Key)

	ss.postLock.Lock()
	_, ok := ss.postData[args.Key] 
	if ok{
		delete(ss.postData, args.Key)
		reply.Status = storagerpc.OK
	}else{
		reply.Status = storagerpc.KeyNotFound
	}
	ss.postLock.Unlock()
	return nil
}