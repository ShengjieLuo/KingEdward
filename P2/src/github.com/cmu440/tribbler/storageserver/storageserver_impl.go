package storageserver

import (
	"container/list"
	"fmt"
	"github.com/cmu440/tribbler/libstore"
	"github.com/cmu440/tribbler/rpc/storagerpc"
	"math"
	"net"
	"net/http"
	"net/rpc"
	"strings"
	"sync"
	"time"
)

const leaseSeconds = storagerpc.LeaseSeconds + storagerpc.LeaseGuardSeconds

type leaseTracker struct {
	hostport  string
	grantedAt time.Time
}

/* Data Structure Definition: storageServer
   1. numNodes: number of nodes
   2. nodeID: node id number
   3. joinedNode: current jointed node number
   4. initDone: finished initialization or not
   5. serverList: all server list
   6. newNodesChanle: pass new node info
   7. newNodesResult: new node result
   8. postData: store each post
   9. listData: store user list
   10. canLease: whether a key can be leased
   11. leaseOwner: info of owner of each lease
   12. cachedConn: do rpc call
   13. postLock: post data lock
   14. listLock: list data lock
*/
type storageServer struct {
	numNodes   int
	nodeID     uint32
	joinedNode int
	initDone   bool
	serverList []storagerpc.Node
	//initDoneRequest chan int
	//initDoneChanel chan int
	newNodesChanel chan storagerpc.Node
	newNodesResult chan storagerpc.RegisterReply
	postData       map[string]string
	listData       map[string]*list.List
	canLease       map[string]bool
	leaseOwner     map[string]*list.List
	cachedConn     map[string]*rpc.Client
	postLock       *sync.Mutex
	listLock       *sync.Mutex
}
/* 
 *	Based on ring hash to determine if the key belongs to
 *  current node
 */
func (ss *storageServer) rightServer(key string) (res bool) {
	userInfo := strings.Split(key, ":")[0] // To be hashed
	userHash := libstore.StoreHash(userInfo) // Key hash Value
	var curCandidate uint32 = math.MaxUint32 // Candidate Node
	// Search for candidate node
	found := false
	for _, curServer := range ss.serverList {
		// Find one
		if curServer.NodeID < curCandidate && curServer.NodeID >= userHash {
			curCandidate = curServer.NodeID
			found = true
		}
	}
	// If there is no candidate, choose the smalles one
	if found {
		res = (curCandidate == ss.nodeID)
	} else {
		res = true
		for _, curServer := range ss.serverList {
			if curServer.NodeID < ss.nodeID {
				res = false
				break
			}
		}
	}
	return
}


/*
 * Grant leases to certain node
 */
func (ss *storageServer) grantLease(key, hostport string) (lease storagerpc.Lease) {
	// Check and establish the connection to do rcp call 
	_, ok := ss.cachedConn[hostport]
	if !ok {
		newConn, _ := rpc.DialHTTP("tcp", hostport)
		ss.cachedConn[hostport] = newConn
	}

	lease = storagerpc.Lease{}
	val, leaseok := ss.canLease[key] // Check if can be leased
	// Yes
	if !leaseok || val {
		lease.Granted = true
		lease.ValidSeconds = storagerpc.LeaseSeconds // Lease time
		_, grantok := ss.leaseOwner[key] // Record in lease owner
		if !grantok {
			ss.leaseOwner[key] = list.New()
		}
		ss.leaseOwner[key].PushBack(&leaseTracker{hostport, time.Now()})
	} else { // No leasing
		lease.Granted = false
		lease.ValidSeconds = 0
	}
	return
}

/*
 * Revoke lease from all lease owners
 */
func (ss *storageServer) revokeLease(key string) {
	ss.canLease[key] = false

	// If no lease owner
	owners, ok := ss.leaseOwner[key]
	if !ok {
		return
	}

	// Send revokelease rpc call to each owner if not expired
	for element := owners.Front(); element != nil; element = element.Next() {
		eachOwner := element.Value.(*leaseTracker)
		timeRemaining := leaseSeconds - time.Since(eachOwner.grantedAt).Seconds()
		// Not naturally expired
		if timeRemaining >= 0 {
			revokeArgs := storagerpc.RevokeLeaseArgs{key}
			revokeReply := storagerpc.RevokeLeaseReply{}
			curConn := ss.cachedConn[eachOwner.hostport]
			callRes := curConn.Go("LeaseCallbacks.RevokeLease", &revokeArgs, &revokeReply, nil)
			// This is to deal with the time out in rpc call
			select {
			case <-callRes.Done:
				break
			case <-time.After(time.Duration(timeRemaining) * time.Second):
				break
			}
		}
		// Delete the owner from the recording
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
    // Init a server's parameter
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

	fullAddress := fmt.Sprintf("localhost:%d", port)
	listener, _ := net.Listen("tcp", fullAddress)
	// Register and initialize rpc
	rpc.RegisterName("StorageServer", storagerpc.Wrap(&s))
	rpc.HandleHTTP()
	// Start listening the incoming call
	go http.Serve(listener, nil)
	selfNode := storagerpc.Node{fullAddress, nodeID}
	// Master Node
	if masterServerHostPort == "" {
		s.serverList[0] = selfNode
		// If only one node
		if numNodes == 1{
			s.initDone = true
		}
		// Wait for new nodes to join
		for !s.initDone {
			select {
			//case <- s.initDoneRequest:
			//	if s.initDone{
			//		s.initDoneChanel <- 1
			//	}else{
			//		s.initDoneChanel <- 0
			//	}
			// Receive new node info
			case newNode := <-s.newNodesChanel:
				// Check if already joined
				flag := false
				for i := range s.serverList {
					if s.serverList[i] == newNode {
						flag = true
					}
				}
				fmt.Printf("[storage] New Node:%s Flag:%d\n",newNode,flag)
				// New node
				if !flag {
					s.serverList[s.joinedNode] = newNode
					s.joinedNode += 1
				}
				// Write reply
				reply := storagerpc.RegisterReply{storagerpc.OK, s.serverList}
				// Not ready
				if s.joinedNode < numNodes {
					reply = storagerpc.RegisterReply{storagerpc.NotReady, nil}
				} else { // Join ready
					s.initDone = true
				}
				// Return value
				s.newNodesResult <- reply
			}
		}
	} else { // Client node
		fmt.Printf("[storage] Slave Routine %d\n",nodeID)
		// Estimate connection until dial succeeded
		connWithMaster, err := rpc.DialHTTP("tcp", masterServerHostPort)
		if (err!=nil){
			for {
				connWithMaster, err = rpc.DialHTTP("tcp", masterServerHostPort)
				if (err==nil){
					break
				} else {
					time.Sleep(time.Second)
				}
			}
			fmt.Printf("[Fatal] storage slave cannot connect storage server")
		}
		// Communicate with server
		args, reply := storagerpc.RegisterArgs{selfNode}, storagerpc.RegisterReply{}
		for {
			connWithMaster.Call("StorageServer.RegisterServer", &args, &reply)
			// Check reply status
			if reply.Status == storagerpc.OK {
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

/*
 * Add server to serverlist
 */
func (ss *storageServer) RegisterServer(args *storagerpc.RegisterArgs, reply *storagerpc.RegisterReply) error {
	reply.Status = storagerpc.OK
	reply.Servers = ss.serverList
	//ss.initDoneRequest <- 1
	//isDone := <- ss.initDoneChanel
	//if isDone != 1{
	fmt.Printf("[storageSerer] Storage slave connect to master:%s\n",args.ServerInfo)
	// If still wait for new node
	if !ss.initDone {
		ss.newNodesChanel <- args.ServerInfo
		*reply = <-ss.newNodesResult
	}
	return nil
}

/*
 * Get all server list
 */
func (ss *storageServer) GetServers(args *storagerpc.GetServersArgs, reply *storagerpc.GetServersReply) error {
	reply.Status = storagerpc.OK
	reply.Servers = ss.serverList
	//ss.initDoneRequest <- 1
	//isDone := <- ss.initDoneChanel
	//if isDone!=1 {
	fmt.Printf("[Storage] All Nodes:%d Joined Nodes:%d Status:%d\n", ss.numNodes, ss.joinedNode, ss.initDone)
	// If not ready
	if !ss.initDone {
		reply.Status = storagerpc.NotReady
		reply.Servers = nil
	}
	return nil
}

/*
 * Get post data
 */
func (ss *storageServer) Get(args *storagerpc.GetArgs, reply *storagerpc.GetReply) error {
	// Check routing
	if !ss.rightServer(args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}
	// Lock post data
	ss.postLock.Lock()
	val, ok := ss.postData[args.Key]
	// Key not found
	if !ok {
		reply.Status = storagerpc.KeyNotFound
	} else { // Find key, check lease info
		if args.WantLease {
			reply.Lease = ss.grantLease(args.Key, args.HostPort)
		}
		reply.Status = storagerpc.OK
		reply.Value = val
	}
	ss.postLock.Unlock()
	return nil
}

/*
 * Get user friend list
 */
func (ss *storageServer) GetList(args *storagerpc.GetArgs, reply *storagerpc.GetListReply) error {
	if !ss.rightServer(args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}
	// Lock list data
	ss.listLock.Lock()
	val, ok := ss.listData[args.Key]
	// Key not found
	if !ok {
		reply.Status = storagerpc.KeyNotFound
	} else {
		reply.Status = storagerpc.OK
		// Make reply value
		reply.Value = make([]string, val.Len())
		if args.WantLease { // Check lease
			reply.Lease = ss.grantLease(args.Key, args.HostPort)
		}
		// Add each element to value
		for idx, element := 0, val.Front(); element != nil; element, idx = element.Next(), idx+1 {
			reply.Value[idx] = element.Value.(string)
		}

	}
	ss.listLock.Unlock()
	return nil
}

/*
 * Post new data
 */
func (ss *storageServer) Put(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	//fmt.Printf("[storage] Put (%s->%s)\n",args.Key,args.Value)
	if !ss.rightServer(args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}
	// Revoke lease for this key
	ss.revokeLease(args.Key)
    // Lock post data
	ss.postLock.Lock()
	ss.postData[args.Key] = args.Value
	ss.canLease[args.Key] = true
	reply.Status = storagerpc.OK
	ss.postLock.Unlock()
	return nil
}


/*
 * Append new friend
 */
func (ss *storageServer) AppendToList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	if !ss.rightServer(args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}
	// Revoke lease
	ss.revokeLease(args.Key)

	// Add lock to list
	ss.listLock.Lock()
	// Check and new list
	_, ok := ss.listData[args.Key]
	if !ok {
		ss.listData[args.Key] = list.New()
	}

	flag := false
	for element := ss.listData[args.Key].Front(); element != nil; element = element.Next() {
		if args.Value == element.Value.(string) {
			// Friend already exist
			reply.Status = storagerpc.ItemExists
			flag = true
		}
	}
 	// If not exist
	if !flag {
		reply.Status = storagerpc.OK
		ss.listData[args.Key].PushBack(args.Value)
	}
	ss.canLease[args.Key] = true
	ss.listLock.Unlock()
	return nil
}

/*
 * Delete post data
 */ 
func (ss *storageServer) Delete(args *storagerpc.DeleteArgs, reply *storagerpc.DeleteReply) error {
	if !ss.rightServer(args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}
	// Revoke
	ss.revokeLease(args.Key)
	// Lock post data
	ss.postLock.Lock()
	_, ok := ss.postData[args.Key]
	// If found
	if ok {
		delete(ss.postData, args.Key)
		reply.Status = storagerpc.OK
	} else { // Not found
		reply.Status = storagerpc.KeyNotFound
	}
	ss.postLock.Unlock()
	return nil
}

/*
 * Remove a friend from list
 */
func (ss *storageServer) RemoveFromList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	if !ss.rightServer(args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}
	// Revoke
	ss.revokeLease(args.Key)
	// Lock 
	ss.listLock.Lock()

	// Check if empty list
	val, ok := ss.listData[args.Key]
	if !ok {
		reply.Status = storagerpc.ItemNotFound
	}
	// Find friend in list
	flag := false
	for element := val.Front(); element != nil; element = element.Next() {
		if args.Value == element.Value.(string) {
			val.Remove(element)
			reply.Status = storagerpc.OK
			flag = true
		}
	}
	// If not found in list
	if !flag {
		reply.Status = storagerpc.ItemNotFound
	}
	ss.canLease[args.Key] = true
	ss.listLock.Unlock()
	return nil
}