package libstore

import (
	"fmt"
	"net/rpc"
	"errors"

	"github.com/cmu440/tribbler/rpc/storagerpc"
	//"github.com/cmu440/tribbler/rpc/librpc"
	//"github.com/cmu440/tribbler/storageserver"
)

type libstore struct {
	HostPort string
	client *rpc.Client
}

// NewLibstore creates a new instance of a TribServer's libstore. masterServerHostPort
// is the master storage server's host:port. myHostPort is this Libstore's host:port
// (i.e. the callback address that the storage servers should use to send back
// notifications when leases are revoked).
//
// The mode argument is a debugging flag that determines how the Libstore should
// request/handle leases. If mode is Never, then the Libstore should never request
// leases from the storage server (i.e. the GetArgs.WantLease field should always
// be set to false). If mode is Always, then the Libstore should always request
// leases from the storage server (i.e. the GetArgs.WantLease field should always
// be set to true). If mode is Normal, then the Libstore should make its own
// decisions on whether or not a lease should be requested from the storage server,
// based on the requirements specified in the project PDF handout.  Note that the
// value of the mode flag may also determine whether or not the Libstore should
// register to receive RPCs from the storage servers.
//
// To register the Libstore to receive RPCs from the storage servers, the following
// line of code should suffice:
//
//     rpc.RegisterName("LeaseCallbacks", librpc.Wrap(libstore))
//
// Note that unlike in the NewTribServer and NewStorageServer functions, there is no
// need to create a brand new HTTP handler to serve the requests (the Libstore may
// simply reuse the TribServer's HTTP handler since the two run in the same process).
func NewLibstore(masterServerHostPort, myHostPort string, mode LeaseMode) (Libstore, error) {
	cli,err := rpc.DialHTTP("tcp",masterServerHostPort)
	if err!=nil{
		return nil,err
	}

	return &libstore{client: cli, HostPort:myHostPort}, nil
}

func (ls *libstore) Get(key string) (string, error) {
	//fmt.Printf("[libstore] Get(%s)\n",key)
	args := &storagerpc.GetArgs{key,false,ls.HostPort}
	var reply storagerpc.GetReply
	err := ls.client.Call("StorageServer.Get",args,&reply)
	if err!=nil{
		fmt.Println(err)
		return "",err
	}
	if reply.Status == storagerpc.OK {
		return reply.Value,nil
	} else {
		return reply.Value,errors.New(string(reply.Status))
	}
}

func (ls *libstore) Put(key, value string) error {
	//fmt.Printf("[libstore] Put(%s->%s)\n",key,value)
	args := &storagerpc.PutArgs{key,value}
	var reply storagerpc.PutReply
	err := ls.client.Call("StorageServer.Put",args,&reply)
	if err!=nil{
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
	err := ls.client.Call("StorageServer.Delete",args,&reply)
	if err!=nil {
		fmt.Println(err)
		return err
	}
	if reply.Status == storagerpc.OK{
		return nil
	} else {
		return errors.New(string(reply.Status))
	}
}

func (ls *libstore) GetList(key string) ([]string, error) {
	//fmt.Printf("[libstore] GetList(%s)\n",key)
	args := &storagerpc.GetArgs{key,false,ls.HostPort}
	var reply storagerpc.GetListReply
	err := ls.client.Call("StorageServer.GetList",args,&reply)
	if err!=nil {
		fmt.Println(err)
		return nil,err
	}
	if reply.Status == storagerpc.OK{
		return reply.Value,nil
	} else {
		return reply.Value,errors.New(string(reply.Status))
	}
}

func (ls *libstore) RemoveFromList(key, removeItem string) error {
	//fmt.Printf("[libstore] RemoveFromList(%s->%s)\n",key,removeItem)
	args := &storagerpc.PutArgs{key,removeItem}
	var reply storagerpc.PutReply
	err := ls.client.Call("StorageServer.RemoveFromList",args,&reply)
	if err!=nil {
		fmt.Println(err)
		return err
	}
	if reply.Status == storagerpc.OK{
		return nil
	} else {
		return errors.New(string(reply.Status))
	}
}

func (ls *libstore) AppendToList(key, newItem string) error {
	//fmt.Printf("[libstore] AppendToList(%s->%s)\n",key,newItem)
	args := &storagerpc.PutArgs{key,newItem}
	var reply storagerpc.PutReply
	err := ls.client.Call("StorageServer.AppendToList",args,&reply)
	if err!=nil {
		fmt.Println(err)
		return err
	}
	if reply.Status == storagerpc.OK{
		return nil
	} else {
		return errors.New(string(reply.Status))
	}
}

func (ls *libstore) RevokeLease(args *storagerpc.RevokeLeaseArgs, reply *storagerpc.RevokeLeaseReply) error {
	return errors.New("not implemented")
}
