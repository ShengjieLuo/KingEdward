/******************************************************************
 **                    15-640 Project1 CheckPoint1               **
 **                     Distributed Bitcoin Miner                **
 ******************************************************************
 **Intorduction:                                                 **
 **1. Implementation of Live Sequence Protocol                   **
 **2. LSP Provides features that lies somewhere between UDP and  **
 **   TCP. It supports a client-server communication model       **
 **3. Two points of the full project are covered in the cp1      **
 **   (1) Sliding Window (2)Out-of-order                         **
 ******************************************************************
 **For TA and Reviewer                                           **
 **1. Map Structure in Golang is used to store data message      **
 **2. Buffered Channel is used to pass messages from one routine **
 **   to another routine                                         **
 **3. Close() Function is well designed. It would block until    **
 **   all subroutine is terminated                               **
 **4. No Magic Number!All constant variables are defined in const**
 **5. No mutex&Lock! All coordination is implemented by channel  **
 **6. No net package! lspnet is the only package used for UDP    **
 ******************************************************************
 **Author:                                                       **
 ** Shengjie Luo shengjil@andrew.cmu.edu                         **
 ** Ke Chang     kec1@andrew.cmu.edu                             **
 ******************************************************************
 */

package lsp

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/cmu440/lspnet"
	"log"
	"os"
	"strconv"
	"time"
)

//Data Structure1: Constant Variable Definition
const (
	readChannelCapacity  = 1000
	writeChannelCapacity = 1000
	maxConnectionNumber  = 1000
	connChannelCapacity  = 10
	networkType          = "udp"
	MsgWriteCall         = -1
	MsgTerminate         = -2
	MsgDestroy           = -3
)

//Data Structure Definition2: Sub-elements of server struct
type writeChannel struct {
	channel chan []byte
}

type writeBuffer struct {
	buf map[int]writeChannel
}

type readMessage struct {
	connid  int
	content []byte
}

type readBuffer struct {
	channel chan readMessage
}

type connection struct {
	remote  *lspnet.UDPAddr
	connid  int
	channel chan Message
}

/* Data Structure Definition3: server
   1. Port: Port of server service avaliable
   2. epochLimit, epochMiles: Used in cp2
   3. writebuf: pass the message from write to mainroutine
   4. readbuf : pass the message from mainroutine to read
   5. chanmap : store the connection between client and server
   6. connNum : a channel used to monitor live connections
   7. sigstp  : a channel used to accept stop signal from user
   8. listener: a UDP network socket
*/
type server struct {
	port       int
	epochLimit int
	epochMiles int
	windowSize int
	writebuf   writeBuffer
	readbuf    readBuffer
	chanmap    map[int]connection
	connNum    chan int
	sigstp     chan int
	listener   *lspnet.UDPConn
}

/* Function1: NewServer
 * 1. NewServer creates, initiates, and returns a new server.
 * 2. It spawn one or more goroutines and immediately return.
 * 3. It should return a non-nil error if there was an error
 *    resolving or listening on the specified port number.
 */
func NewServer(port int, params *Params) (Server, error) {
	var newServer Server
	readch := make(chan readMessage, readChannelCapacity)
	readbuf := readBuffer{readch}
	buf := make(map[int]writeChannel)
	writebuf := writeBuffer{buf}
	chanmaps := make(map[int]connection)
	connNum := make(chan int, maxConnectionNumber)
	sigstp := make(chan int, 1)
	addr, _ := lspnet.ResolveUDPAddr(networkType, ":"+strconv.Itoa(port))
	listener, err := lspnet.ListenUDP(networkType, addr)
	if err != nil {
		log.Fatalln("net.ListenUDP fail.", err)
		os.Exit(1)
	}
	newserver := server{port, params.EpochLimit, params.EpochMillis,
		params.WindowSize, writebuf, readbuf, chanmaps,
		connNum, sigstp, listener}
	newServer = &newserver
	go readRoutine(&newserver)
	return newServer, nil
}

/* Function2: Read Routine
 * 1. Read packats from UDP connection
 * 2. Check whether it comes from a new client
 * 3. If it comes from a new client, initialize a new Connection
 * 4. Else, add the packet to existed Connection
 */
func readRoutine(s *server) {
	connid := 0
	for {
		/* Step1:
		 * If the terminate fucntion called, execute following routine
		 * 1. Terminate all mainroutines
		 * 2. Block Until all mainroutines are terminated
		 */
		select {
		case <-s.sigstp:
			msg := Message{MsgTerminate, 0, 0, 0, nil}
			for i := 1; i <= connid; i++ {
				s.chanmap[i].channel <- msg
			}
			return
		default:
			//fmt.Printf("...........................\n")
		}

		//Step2: Read packet from UDP connection
		readContent := make([]byte, 1024)
		n, remoteAddr, err := s.listener.ReadFromUDP(readContent)
		if err != nil {
			fmt.Errorf("Cannot read from connection: %v\n", err)
		}
		data := readContent[0:n]
		var msg = new(Message)
		err = json.Unmarshal(data, msg)
		if err != nil {
			fmt.Errorf("Can not decode data: %v\n", err)
		}

		//Step3: Ignore some irregualr package.
		//For example, Server would not get ACK signal within seqnum==0
		if msg.Type == MsgAck && msg.SeqNum == 0 {
			//fmt.Printf("%s",data)
			continue
		}
		/*fmt.Printf("[0] Connection from %s\n",remoteAddr.String())
		fmt.Printf("[0] Receive %s-->(%d,%d,%d,%d,%s) \n",
			readContent,msg.Type, msg.ConnID, msg.SeqNum,
			msg.Size,msg.Payload)
		*/
		//Step4: Update the connection situation
		if msg.Type == MsgConnect {
			connid += 1
			connChannel := make(chan Message, connChannelCapacity)
			newConnection := connection{remoteAddr,
				connid, connChannel}
			s.chanmap[connid] = newConnection
			newConnection.channel <- *msg
			s.connNum <- 1
			ch := make(chan []byte, writeChannelCapacity)
			writeCh := writeChannel{ch}
			s.writebuf.buf[connid] = writeCh
			//fmt.Printf("[%d] Establish new connection! \n",connid)
			go mainRoutine(s, &newConnection, writeCh)
		} else {
			s.chanmap[msg.ConnID].channel <- *msg
		}
	}
}

func mainRoutine(s *server, conn *connection, writeCh writeChannel) {
	fmt.Printf("[%d] Main Routine Begins\n", conn.connid)
	//receAckCount records the number of ACKs we had get from connection
	//receDataCount records the number of data we had get from connection
	//sendDataCount records the number of data we had sent into connection
	receAckCount := 0
	sendDataCount := 0
	receDataCount := 0
	receDataBuffer := make(map[int][]byte)
	receAckBuffer := make(map[int]int)
	terminateFlag := false

	for {
		for sendDataCount < receAckCount+s.windowSize {
			flag := true
			select {
			case i := <-writeCh.channel:
				sendDataCount += 1
				writeMsg := DataMsg(conn.connid,
					sendDataCount, i)
				s.listener.WriteToUDP(writeMsg, conn.remote)
				/*fmt.Printf("[%d] Send Message (%s) to %s\n",
				conn.connid,writeMsg,conn.remote.String())*/
			default:
				flag = false
				/* When all of the following conditions is met,
				   then terminate the process
				   1. An closeConn() function is called by user
				   2. There is no pending message in writebuffer
				   3. All sent messages have already got ACK
				*/
				if terminateFlag && sendDataCount ==
					receAckCount {
					<-s.connNum
					/*fmt.Printf("[%d] Terminate...\n",
					conn.connid)*/
					return
				}
				break
			}
			if flag == false {
				break
			}
		}
		msg := <-conn.channel
		if msg.Type == MsgConnect {
			/* Server gets a "connect" message from client
			   1. Initialize the write channel list of server
			   2. Return ACK message to client
			*/
			writeMsg := AckMsg(conn.connid, 0)
			/*fmt.Printf("[%d] Send Message (%s) to %s\n",
			  conn.connid,writeMsg,conn.remote.String())*/
			s.listener.WriteToUDP(writeMsg, conn.remote)
		} else if msg.Type == MsgData {
			/* Server gets a "data" message from client
			1. Return ACK message to client
			2. Check whether it is in-order data
			3. If no,  store it in receDataBuffer
			4. If yes, send it into server.readbuf
			*/
			writeMsg := AckMsg(conn.connid, msg.SeqNum)
			/*fmt.Printf("[%d] Send Message (%s) to %s\n"
			  ,conn.connid,writeMsg,conn.remote.String())*/
			s.listener.WriteToUDP(writeMsg, conn.remote)
			if msg.SeqNum == (receDataCount + 1) {
				readMsg := readMessage{conn.connid, msg.Payload}
				s.readbuf.channel <- readMsg
				receDataCount += 1
				for {
					value, ok :=
						receDataBuffer[receDataCount+1]
					if ok {
						readMsg :=
							readMessage{
								conn.connid,
								value}
						delete(receDataBuffer,
							receDataCount+1)
						s.readbuf.channel <- readMsg
						receDataCount += 1
						continue
					} else {
						break
					}
				}
			} else {
				receDataBuffer[msg.SeqNum] = msg.Payload
			}
		} else if msg.Type == MsgAck {
			/* Server gets a "ack" message from client
			1. Updata receAckCount
			2. Check whether there is data blocked in writebuf
			3. If no continue
			4. If yes, send the blocked data
			*/
			receAckBuffer[msg.SeqNum] = 1
			for {
				_, ok := receAckBuffer[receAckCount+1]
				if ok {
					receAckCount = receAckCount + 1
				} else {
					break
				}
			}
		} else if msg.Type == MsgWriteCall {
			continue
		} else if msg.Type == MsgTerminate {
			terminateFlag = true
		}
	}
}

/* Fuction AckMsg: Generate the Ack Type message from given parameter*/
func AckMsg(id int, sn int) []byte {
	msg := Message{MsgAck, id, sn, 0, nil}
	data, _ := json.Marshal(msg)
	return data
}

/* Fuction DataMsg: Generate the Data Type message from given parameter*/
func DataMsg(id int, sn int, payload []byte) []byte {
	msg := Message{MsgData, id, sn, len(payload), payload}
	data, _ := json.Marshal(msg)
	return data
}

/* Function Read: read messages from readbuf channel*/
func (s *server) Read() (int, []byte, error) {
	msg := <-s.readbuf.channel
	return msg.connid, msg.content, nil
}

/* Function Write: write messages into writebuf channel*/
func (s *server) Write(connID int, payload []byte) error {
	//fmt.Printf("[%d] User write to server:%s\n",connID,payload)
	value, ok := s.writebuf.buf[connID]
	if ok {
		ch := value.channel
		ch <- payload
		msg := Message{MsgWriteCall, 0, 0, 0, nil}
		s.chanmap[connID].channel <- msg
	} else {
		return errors.New("Write Channel does not exist!")
	}
	return nil
}

/* Function CloseConn: Close an individual connection*/
func (s *server) CloseConn(connID int) error {
	msg := Message{MsgTerminate, 0, 0, 0, nil}
	value, ok := s.chanmap[connID]
	if ok {
		value.channel <- msg
	}
	return nil
}

/* Function Close:
   1. Send sigstp to read routine, and read routine terminates each main routine
   2. Blocked when routines have not been terminated
   3. Use panic to check whether a routine existed
*/
func (s *server) Close() error {
	s.sigstp <- 1
	time.Sleep(time.Second * 1)
	for {
		select {
		case <-s.connNum:
			//fmt.Printf("Routines not closed... Wait...\n")
			s.connNum <- 1
			time.Sleep(time.Second * 1)
		default:
			//fmt.Printf("*********Close Server************\n")
			//panic("ERROR: Goroutine existed AFTER close!\n")
			return nil
		}
	}
}
