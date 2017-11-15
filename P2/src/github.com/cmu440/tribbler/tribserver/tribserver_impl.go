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

package tribserver

import (
	"net"
	"net/rpc"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/cmu440/tribbler/libstore"
	"github.com/cmu440/tribbler/rpc/tribrpc"
	"github.com/cmu440/tribbler/rpc/storagerpc"
)

type tribServer struct {
	lib libstore.Libstore
}

/*
NewTribServer creates, starts and returns a new TribServer. masterServer
is the master storage server's host:port and port is this #port on which
the TribServer should listen. A non-nil error returned if the TribServer
could not be started.
*/
func NewTribServer(masterServerHostPort, myHostPort string) (TribServer, error) {

	//Step1:Initialize libstore
	fmt.Printf("[tribServer] Begin to Initialize server\n")
	lib, errlib := libstore.NewLibstore(masterServerHostPort,
                   myHostPort, libstore.Never)
	if errlib != nil {
		return nil, errlib
	}

	//Step2: Establish the http listener to listen tribclient
	tribServer := new(tribServer)
	err := rpc.RegisterName("TribServer", tribrpc.Wrap(tribServer))
	if err != nil {
		return nil, err
	}
	rpc.HandleHTTP()
	listener, err2 := net.Listen("tcp", myHostPort)
	if err2 != nil {
		return nil, err2
	}
	go http.Serve(listener, nil)

	//Step3: Initalize the key-value store
	tribServer.lib = lib
	tribServer.lib.Put("PC", "0")
	fmt.Printf("[tribServer] Server Initialization Complete\n")
	return tribServer, nil
}

/* Function1: CreateUser
   Create user by given args and output the result into reply structure
   Create a new key-value pair to maintain the newuser status
*/
func (ts *tribServer) CreateUser(
                      args *tribrpc.CreateUserArgs,
                      reply *tribrpc.CreateUserReply) error {
	userID := args.UserID
	fmt.Printf("[tribServer] Create user:%s\n",userID)
	_, err := ts.lib.GetList(userID + "-sub")
	if err == nil {
		fmt.Printf("[tribServer] user:%d existed\n",userID)
		reply.Status = tribrpc.Exists
	} else {
		key := userID + "-sub"
		err1 := ts.lib.AppendToList(key, "init")
		key = userID + "-trib"
		err2 := ts.lib.AppendToList(key, "init")
		if err1 != nil || err2 != nil {
			fmt.Printf("tmperror\n")
		}
		reply.Status = tribrpc.OK
	}
	return nil
}

/* Function2: AddSubscription
   Add Subscription by given args and output the result into reply structure
   Append a new itme into the user list to record the subscription
*/
func (ts *tribServer) AddSubscription(
                      args *tribrpc.SubscriptionArgs,
                      reply *tribrpc.SubscriptionReply) error {
	userID := args.UserID
	targetID := args.TargetUserID
	//fmt.Printf("[tribServer] AddSubscription:%s->%s\n",userID,targetID)
	userlist, erruser := ts.lib.GetList(userID + "-sub")
	if erruser != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	_, errtarget := ts.lib.GetList(targetID + "-sub")
	if errtarget != nil {
		reply.Status = tribrpc.NoSuchTargetUser
		return nil
	}
	for _, user := range userlist {
		if user == targetID {
			reply.Status = tribrpc.Exists
			return nil
		}
	}
	err := ts.lib.AppendToList(userID+"-sub", targetID)
	if err != nil && err.Error()!=string(storagerpc.ItemExists) {
		fmt.Printf("[tribServer] Addsubscription Error:%s\n",err.Error())
		return err
	}
	reply.Status = tribrpc.OK
	return nil
}

/* Function3: Remove Subscription 
   Remove Subscription by given args and output the result into reply structure
   Romve an existed item from the user list to record the changes
*/
func (ts *tribServer) RemoveSubscription(
                      args *tribrpc.SubscriptionArgs,
                      reply *tribrpc.SubscriptionReply) error {
	userID := args.UserID
	targetID := args.TargetUserID
	//fmt.Printf("[tribServer] RemoveSubscription:%s->%s\n",userID,targetID)
	userlist, erruser := ts.lib.GetList(userID + "-sub")
	if erruser != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	_, errtarget := ts.lib.GetList(targetID + "-sub")
	if errtarget != nil {
		reply.Status = tribrpc.NoSuchTargetUser
		return nil
	}
	flag := false
	for _, user := range userlist {
		if user == targetID {
			flag = true
		}
	}
	if !flag {
		reply.Status = tribrpc.NoSuchTargetUser
		return nil
	}
	err := ts.lib.RemoveFromList(userID+"-sub", targetID)
	if err != nil && err.Error()!=string(storagerpc.ItemNotFound) {
		fmt.Printf("[tribServer] Removescription Error:%s\n",err.Error())
		return errors.New("[Fatal] Remove From List failed:"+userID+"-sub")
	}
	reply.Status = tribrpc.OK
	return nil
}

/* Function4: GetFriends
   Get friends by given args and output the result into reply structure
   Search thorugh the subsciption list to find its friend
*/
func (ts *tribServer) GetFriends(
                      args *tribrpc.GetFriendsArgs,
                      reply *tribrpc.GetFriendsReply) error {
	userID := args.UserID
	//fmt.Printf("[tribServer] GetFriends:%s\n",userID)
	var friends []string
	userlist, erruser := ts.lib.GetList(userID + "-sub")
	if erruser != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	for _, target := range userlist {
		if target == userID {
			continue
		}
		targetlist, _ := ts.lib.GetList(target + "-sub")
		for _, tuser := range targetlist {
			if tuser == userID {
				friends = append(friends, target)
			}
		}
	}
	reply.UserIDs = friends
	reply.Status = tribrpc.OK
	return nil
}

/* Function5: Post Tribble
   Post tribble  by given args and output the result into reply structure
   Put a key-value into storage and update the user list
*/
func (ts *tribServer) PostTribble(
                      args *tribrpc.PostTribbleArgs,
                      reply *tribrpc.PostTribbleReply) error {
	userID := args.UserID
	content := args.Contents
	//Step1 : Whether it is a valid user
	_, err := ts.lib.GetList(userID + "-sub")
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	//Step2 : Update tribble items
	key := "trib-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	ts.lib.AppendToList(userID+"-trib", key)
	ts.lib.Put(key, content)
	reply.Status = tribrpc.OK
	reply.PostKey = key
	//fmt.Printf("[tribServer] PostTribble:%s->%s\n",userID,key)
	return nil
}

/* Function6: Delete Tribble
   Delete tribble  by given args and output the result into reply structure
   Delete a key-value from storage and update the user list
*/
func (ts *tribServer) DeleteTribble(
                      args *tribrpc.DeleteTribbleArgs,
                      reply *tribrpc.DeleteTribbleReply) error {
	userID := args.UserID
	key := args.PostKey
	//fmt.Printf("[tribServer] Delete Tribble:%s->%s\n",userID,key)
	//Step1 : Whether it is a valid user
	triblist, err := ts.lib.GetList(userID + "-trib")
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	//Step2 : Whether it is a valid post
	flag := false
	for _, trib := range triblist {
		if trib == key {
			flag = true
			break
		}
	}
	if !flag {
		reply.Status = tribrpc.NoSuchPost
		return nil
	}

	//Step3: Delete Post
	ts.lib.Delete(key)
	ts.lib.RemoveFromList(userID+"-trib", key)
	reply.Status = tribrpc.OK
	return nil
}

/* Function7: Get Tribble
   Get tribble  by given args and output the result into reply structure
   Get a key-value pair from storage according to the records in user list
*/

func (ts *tribServer) GetTribbles(
                      args *tribrpc.GetTribblesArgs,
                      reply *tribrpc.GetTribblesReply) error {
	userID := args.UserID
	//fmt.Printf("[tribServer] GetTribble:%s\n",userID)
	//Step1 : Whether it is a valid user
	triblist, err := ts.lib.GetList(userID + "-trib")
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	//Step2 : Get recent 100 tribbles and reverse order
	var resultlist []string
	for i := len(triblist) - 1; i > 0; i-- {
		resultlist = append(resultlist, triblist[i])
	}

	//Step3 : Return tribble result
	var tribbles []tribrpc.Tribble
	length := 0
	for i := 0; i < len(resultlist); i++ {
		if length == 100 {
			break
		}
		contents, err := ts.lib.Get(resultlist[i])
		if err != nil {
			continue
		}
		nano, _ := strconv.ParseInt(resultlist[i][5:], 10, 64)
		posted := time.Unix(0, nano)
		tribble := tribrpc.Tribble{userID, posted, contents}
		tribbles = append(tribbles, tribble)
		//fmt.Printf("Time:%s Content:%s\n",posted.UnixNano,contents)
		length++
	}
	reply.Status = tribrpc.OK
	reply.Tribbles = tribbles
	return nil
}

/*********************************
 *  Sort helper function         *
 *********************************/
type tribSlice [][2]string

func (c tribSlice) Len() int {
	return len(c)
}
func (c tribSlice) Swap(i, j int) {
	var ele [2]string
	ele[0] = c[i][0]
	ele[1] = c[i][1]
	c[i][0] = c[j][0]
	c[i][1] = c[j][1]
	c[j][0] = ele[0]
	c[j][1] = ele[1]
}

func (c tribSlice) Less(i, j int) bool {
	return c[i][0] > c[j][0]
}

/* Function8: Get Tribble by subscription
   Get tribble  by given args and output the result into reply structure
   Search thorugh the user-list and get tribble from subscription user
*/
func (ts *tribServer) GetTribblesBySubscription(
                      args *tribrpc.GetTribblesArgs,
                      reply *tribrpc.GetTribblesReply) error {
	userID := args.UserID
	//fmt.Printf("[tribServer] GetTribble:%s\n",userID)
	//Step1 : Whether it is a valid user
	userlist, err := ts.lib.GetList(userID + "-sub")
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	//Step2 : Select all tribs from targets
	var triblist tribSlice
	for _, user := range userlist {
		if user == userID {
			continue
		}
		tribunits, _ := ts.lib.GetList(user + "-trib")
		for _, trib := range tribunits {
			if trib == string("init") {
				continue
			}
			var ele [2]string
			ele[0] = trib
			ele[1] = user
			triblist = append(triblist, ele)
		}
	}

	//Step3 : Sort the tribble list
	sort.Sort(triblist)
	var tribbles []tribrpc.Tribble
	length := 0
	for i := 0; i < len(triblist); i++ {
		trib := triblist[i][0]
		if length == 100 {
			break
		}
		contents, err := ts.lib.Get(trib)
		if err != nil {
			//fmt.Println("Key not Found")
			continue
		}
		nano, _ := strconv.ParseInt(trib[5:], 10, 64)
		posted := time.Unix(0, nano)
		tribble := tribrpc.Tribble{triblist[i][1], posted, contents}
		tribbles = append(tribbles, tribble)
		//fmt.Printf("Add tribble:%s,%s\n",tribble.UserID,posted.String())
		length++
	}
	reply.Status = tribrpc.OK
	reply.Tribbles = tribbles
	return nil
}
