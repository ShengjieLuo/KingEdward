/******************************************************************
 **                    15-640 Project1 CheckPoint2               **
 **                     Distributed Bitcoin Miner                **
 ******************************************************************
 **Intorduction:                                                 **
 **1. Implementation of Live Sequence Protocol                   **
 **2. LSP Provides features that lies somewhere between UDP and  **
 **   TCP. It supports a client-server communication model       **
 **3. Four points of the full project are covered in the cp2     **
 **   (1) Sliding Window                                         **
 **   (2) Out-of-order                                           **
 **   (3) Epoch Mechnism is designed for timeout message         **
 **   (4) Close is optimized for epoch mechnism to use           **
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
	"math"
	"os"
	"strconv"
	"time"
)

//Data Structure1: Constant Variable Definition
const (
	readChannelCapacity  = 10000
	writeChannelCapacity = 10000
	maxConnectionNumber  = 10000
	connChannelCapacity  = 2000
	networkType          = "udp"
	MsgWriteCall         = -1
	MsgTerminate         = -2
	MsgDestroy           = -3
	MsgTicker            = -4
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

type epoch struct {
	sendEpoch int
	backoff   int
	content   []byte
}

/* Data Structure Definition3: client
   1. remote: the UDP address
   2. connid: id of connection
   3. channel: Channel used as the beridge between readRoutine and mainRoutine
   4. sigstp: signal stop channel (used in close)
   5. sigter: signal terminate channel (used in close)
   6. epochMap: store epochs
   7. currentEpoch: record current epoch
   8. lastMsg, lastData: used in Epoch Mechnism
*/
type connection struct {
	remote       *lspnet.UDPAddr
	connid       int
	channel      chan Message
	sigstp       chan int
	sigter       chan int
	epochMap     map[int]epoch
	currentEpoch int
	lastMsg      int
	lastData     int
}

/* Data Structure Definition4: server
   1. Port: Port of server service avaliable
   2. epochLimit, epochMiles, windowssize: Used in cp2
   3. writebuf: pass the message from write to mainroutine
   4. readbuf : pass the message from mainroutine to read
   5. chanmap : store the connection between client and server
   6. connNum : a channel used to monitor live connections
   7. sigstp  : a channel used to accept stop signal from user
   8. sigter  : a channel used to accept terminate signal
   9. sigclo  : a channel used to accept close signal
   8. listener: a UDP network socket
*/
type server struct {
	port               int
	epochLimit         int
	epochMiles         int
	windowSize         int
	maxBackOffInterval int
	writebuf           writeBuffer
	readbuf            readBuffer
	chanmap            map[int]connection
	connNum            chan int
	sigstp             chan int
	sigter             chan int
	sigclo             chan int
	listener           *lspnet.UDPConn
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
	sigter := make(chan int, 100)
	sigclo := make(chan int, 100)
	addr, _ := lspnet.ResolveUDPAddr(networkType, ":"+strconv.Itoa(port))
	listener, err := lspnet.ListenUDP(networkType, addr)
	if err != nil {
		log.Fatalln("net.ListenUDP fail.", err)
		os.Exit(1)
	}
	newserver := server{port, params.EpochLimit, params.EpochMillis,
		params.WindowSize, params.MaxBackOffInterval, writebuf, readbuf,
		chanmaps, connNum, sigstp, sigter, sigclo, listener}
	newServer = &newserver
	go readRoutine(&newserver)
	return newServer, nil
}

/* Function2: TerminateRoutine
 * 1. The specific routine to terminate server only
 * 2. Begin when Close() function is called
 * 3. End when all routines are terminated
 */
func terminateRoutine(s *server) {
	waitFlag := false
	for waitFlag == false {
		select {
		case <-s.sigstp:
			msg := Message{MsgTerminate, 0, 0, 0, nil}
			for i := 1; i <= len(s.chanmap); i++ {
				s.chanmap[i].channel <- msg
			}
			waitFlag = true
			break
		default:
		}
	}
	for {
		select {
		case <-s.connNum:
			s.connNum <- 1
			time.Sleep(time.Millisecond * 10)
		default:
			s.sigclo <- 1
			return
		}
	}
	return
}

/* Function3: Read Routine
 * 1. Read packats from UDP connection
 * 2. Check whether it comes from a new client
 * 3. If it comes from a new client, initialize a new Connection
 * 4. Else, add the packet to existed Connection
 */
func readRoutine(s *server) {
	connid := 0
	for {
		//Step1: Read packet from UDP connection
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
		if msg.Type == MsgData {
			if msg.Size < len(msg.Payload) {
				msg.Payload = msg.Payload[0:msg.Size]
			} else if msg.Size > len(msg.Payload) {
				continue
			}
		}

		//Step2: Update the connection situation
		if msg.Type == MsgConnect {
			flag := true
			for _, v := range s.chanmap {
				if v.remote.String() == remoteAddr.String() {
					flag = false
					break
				}
			}
			if flag == false {
				continue
			}
			connid += 1
			connChannel := make(chan Message, connChannelCapacity)
			connSigstp := make(chan int, 100)
			connSigter := make(chan int, 100)
			connEpochMap := make(map[int]epoch)
			newConnection := connection{remoteAddr, connid, connChannel,
				connSigstp, connSigter, connEpochMap, 0, -1, -1}
			s.chanmap[connid] = newConnection
			newConnection.channel <- *msg
			s.connNum <- 1 //Count Connection Main Routine
			s.connNum <- 1 //Count Connection Time Routine
			ch := make(chan []byte, writeChannelCapacity)
			writeCh := writeChannel{ch}
			s.writebuf.buf[connid] = writeCh
			go timeRoutine(s, &newConnection)
			go mainRoutine(s, &newConnection, writeCh)
		} else {
			s.chanmap[msg.ConnID].channel <- *msg
		}
	}
}

/* Function4: TimeRoutine
 * Time routine is used to trigger the epoch event
 */
func timeRoutine(s *server, conn *connection) {
	ticker := time.NewTicker(time.Millisecond * time.Duration(s.epochMiles))
	for _ = range ticker.C {
		msg := Message{MsgTicker, 0, 0, 0, nil}
		conn.channel <- msg
		select {
		case <-conn.sigstp:
			<-s.connNum
			return
		default:
		}
	}
}

/* Function5: MainRoutine
 * MainRoutine is the most critical part in this program. It keeps the status
 * of each alive routine and accepts the read/write request from users.
 * Also the epoch protocol and window protocal are implemented in it.
 */
func mainRoutine(s *server, conn *connection, writeCh writeChannel) {

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
				writeMsg := DataMsg(conn.connid, sendDataCount, i)
				conn.epochMap[sendDataCount] =
					epoch{conn.currentEpoch, 0, writeMsg}
				s.listener.WriteToUDP(writeMsg, conn.remote)
			default:
				flag = false
				if terminateFlag && sendDataCount == receAckCount {
					<-s.connNum
					conn.sigstp <- 1
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
			s.listener.WriteToUDP(writeMsg, conn.remote)
		} else if msg.Type == MsgData {
			writeMsg := AckMsg(conn.connid, msg.SeqNum)
			s.listener.WriteToUDP(writeMsg, conn.remote)
			if msg.SeqNum == (receDataCount + 1) {
				readMsg := readMessage{conn.connid, msg.Payload}
				s.readbuf.channel <- readMsg
				receDataCount += 1
				for {
					value, ok :=
						receDataBuffer[receDataCount+1]
					if ok {
						readMsg := readMessage{conn.connid, value}
						delete(receDataBuffer, receDataCount+1)
						s.readbuf.channel <- readMsg
						receDataCount += 1
						continue
					} else {
						break
					}
				}
			} else if msg.SeqNum > receDataCount+1 {
				receDataBuffer[msg.SeqNum] = msg.Payload
			}
			conn.lastMsg = conn.currentEpoch
			conn.lastData = conn.currentEpoch
		} else if msg.Type == MsgAck {
			if msg.SeqNum >= receAckCount+1 {
				receAckBuffer[msg.SeqNum] = 1
				for {
					_, ok := receAckBuffer[receAckCount+1]
					if ok {
						receAckCount = receAckCount + 1
					} else {
						break
					}
				}
				delete(conn.epochMap, msg.SeqNum)
			}
			conn.lastMsg = conn.currentEpoch
		} else if msg.Type == MsgWriteCall {
			continue
		} else if msg.Type == MsgTerminate {
			terminateFlag = true
		} else if msg.Type == MsgTicker {
			if conn.currentEpoch >= conn.lastMsg+s.epochLimit {
				<-s.connNum
				conn.sigstp <- 1 //Terminate Time Routine
				s.readbuf.channel <- readMessage{conn.connid, nil}
				return
			}
			if conn.currentEpoch != conn.lastData {
				msg := AckMsg(conn.connid, 0)
				s.listener.WriteToUDP(msg, conn.remote)
			}
			for sn, epoch := range conn.epochMap {
				if conn.currentEpoch == epoch.sendEpoch+epoch.backoff {
					s.listener.WriteToUDP(epoch.content, conn.remote)
					newBackoff := updateBackoff(epoch.backoff,
						s.maxBackOffInterval)
					epoch.backoff = newBackoff
					delete(conn.epochMap, sn)
					conn.epochMap[sn] = epoch
				}
			}
			conn.currentEpoch = conn.currentEpoch + 1
		}
	}
}

/* Function UpdateBackoff:
   An tool function to update the backoff interval
*/
func updateBackoff(val int, limit int) int {
	i := -1.0
	back := -1
	for {
		if int(i) == -1 {
			back = 0
		} else {
			back = int(math.Pow(2, i))
		}
		i = i + 1
		if val >= back && val < back*2+1 {
			if back > limit {
				return val + limit + 1
			} else {
				return val + back + 1
			}
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
	for {
		msg := <-s.readbuf.channel
		if msg.content != nil {
			return msg.connid, msg.content, nil
		} else {
			return msg.connid, nil, errors.New("Connection Closed")
		}
	}
}

/* Function Write: write messages into writebuf channel*/
func (s *server) Write(connID int, payload []byte) error {
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
   1. Send sigstp to read routine, and read routine terminates each mainRoutine
   2. Blocked when routines have not been terminated
   3. Use panic to check whether a routine existed
*/
func (s *server) Close() error {
	//fmt.Printf("~~~~ Begin server.Close() ~~~~\n")
	s.sigstp <- 1
	go terminateRoutine(s)
	time.Sleep(time.Millisecond * 10)
	for {
		select {
		case <-s.sigclo:
			return nil
			//panic("ERROR: Goroutine existed AFTER close!\n")
		default:
			time.Sleep(time.Millisecond * 10)
		}
	}
}
