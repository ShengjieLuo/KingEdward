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
	"time"
)

//Data Structure1: Constant Variable Definition
const (
	maxPendingData = 10000
	maxDataChanel  = 10000
)

//Data Structure Definition2: Sub-elements of server struct
type dataSendBuffer struct {
	data [][]byte
}

type clientSeqAck struct {
	ackBuf map[int]int
}

type dataChanel struct {
	chanel chan []byte
}

type dataReceived struct {
	buf map[int][]byte
}

type epochInfo struct {
	sentEpoch      int
	currentBackOff int
}

/* Data Structure Definition3: server
   1. params: parameters
   2. conn: UDP connection
   3. connID: connection id
   4. connResult: pass result of connection
   5. clientClose: Close() chanel
   6. closeEachComp: close each running routine
   7. closeFinished: indicate whether close is done
   8. closeRead: close read routine
   9. beclose: indicate client has been closed and its type
   10. lastMsgEpoch: last epoch# with data arrived
   11. currentEpoch: current epoch #
   12. epochFire: timer
   13. writeDataChanel: data chanel from Write() to main routine
   14. writeDataBuffer: pending data sent by Write()
   15. writeSeqNum: start from 1
   16. writeWindowBase: sliding window base, start from 1
   17. writeACKReceived: received ACK
   18. readSeqNum: received data sequence
   19. readMessageChanel: new incoming message chanel
   20. readDataChanel: chanel from main routine to Read()
   21. readDataReceived: received out of order server data
*/
type client struct {
	params        *Params
	conn          *lspnet.UDPConn
	addr          *lspnet.UDPAddr
	connID        int
	connResult    chan int
	clientClose   chan int
	closeEachComp chan int
	closeFinished chan int
	closeRead     chan int
	beClosed      int
	lastMsgEpoch  int
	currentEpoch  int
	receivedLastEpoch int
	epochFirer    *time.Ticker

	writeDataChanel  dataChanel
	writeDataBuffer  dataSendBuffer
	writeSeqNum      int
	writeWindowBase  int
	writeACKReceived map[int]int
	writeOnFly       map[int]epochInfo

	readSeqNum        int
	readMessageChanel dataChanel
	readDataChanel    dataChanel
	readDataReceived  dataReceived
}

/*
 * NewClient(): Create a new client()
 */
func NewClient(hostport string, params *Params) (*client, error) {
	// Initialize each attribute of client
	c := client{params,
		&lspnet.UDPConn{}, &lspnet.UDPAddr{}, -1,
		make(chan int), make(chan int), make(chan int),
		make(chan int), make(chan int),
		0, 0, 0, 0
		time.NewTicker(time.Duration(params.EpochMillis) * time.Millisecond),
		dataChanel{make(chan []byte, maxDataChanel)},
		dataSendBuffer{make([]([]byte), 0, maxPendingData)},
		1, 1, make(map[int]int), make(map[int]epochInfo),
		-1,
		dataChanel{make(chan []byte, maxDataChanel)},
		dataChanel{make(chan []byte, maxDataChanel)},
		dataReceived{make(map[int][]byte)}}
	// Get UPD address
	c.addr, _ = lspnet.ResolveUDPAddr("udp", hostport)
	// Dial the server
	c.conn, _ = lspnet.DialUDP("udp", nil, c.addr)
	// Create read and main routine
	go c.readRoutine()
	go c.mainRoutine()
	// Send connect message
	c.sendMsg(MsgConnect, 0, nil)
	// Wait for connect result
	if result := <-c.connResult; result == 0 {
		//Failed
		return &c, errors.New("Can't conect to server, time out!")
	}
	// Succeed
	return &c, nil
}

/*
 * sendMsg(): send a message of type:MsgType, sequence number: seqnum
 *				and payload: payload
 */
func (c *client) sendMsg(tp MsgType, seqNum int, payload []byte) {
	var data *Message
	// onnect message
	if tp == MsgConnect {
		data = NewConnect()
	} else {
		// Data message
		if tp == MsgData {
			data = NewData(c.connID, seqNum, len(payload), payload)
		} else {
			// Ack message
			data = NewAck(c.connID, seqNum)
		}
	}
	// Send generated message
	msg, _ := json.Marshal(*data)
	c.conn.Write(msg)
}

/*
 * myUnmarshal(): decode byte array into struct
 */
func (c *client) myUnmarshal(data []byte) Message {
	var msg = new(Message)
	err := json.Unmarshal(data, msg)
	if err != nil {
		fmt.Errorf("[Error]Cannot unmarshal data:%v!\n", err)
	}
	return *msg
}

/*
 * sendClientData(): clear all pending data within the window
 */
func (c *client) sendClientData() {
	// If there are data in the window that not sent
	for c.writeSeqNum <= c.writeWindowBase+c.params.WindowSize-1 &&
		c.writeSeqNum <= len(c.writeDataBuffer.data) {
		payload := c.writeDataBuffer.data[c.writeSeqNum-1]
		c.sendMsg(MsgData, c.writeSeqNum, payload)
		// Record the on fly data
		c.writeOnFly[c.writeSeqNum] = epochInfo{c.currentEpoch, 0}
		// Update sequence number
		c.writeSeqNum += 1
	}
}

/*
 * closeEachComponent(): close each running routine(read routine)
 */
func (c *client) closeEachComponent() {
	numOfCom := 1
	//if c.beClosed == 1{
	//	numOfCom = 2
	//}
	for i := 0; i < numOfCom; i++ {
		c.closeEachComp <- 1
	}
	// Stop the timer
	c.epochFirer.Stop()
	return
}

/*
 * returnToClose(): Unblock the Close() call
 */

func (c *client) returnToClose() {
	c.closeFinished <- 1
}

/*
 * mainRoutine(): process epoch signal and corresponding operation
 *				  deal with close operation
 *				  send new data to server
 *				  process new message from server
 */
func (c *client) mainRoutine() {
	for {
		select {
		// epoch event
		case <-c.epochFirer.C:
			c.currentEpoch += 1
			// Still not connected to server
			if c.connID == -1 {
				// Connection time out
				if c.currentEpoch >= 5 {
					c.beClosed = 2
					c.connResult <- 0
					c.closeEachComponent()
					return
				}
				// Send another connect message
				c.sendMsg(MsgConnect, 0, nil)
			} else {
				// Slient epoch exceed limit, close
				if c.currentEpoch-c.lastMsgEpoch >= c.params.EpochLimit {
					c.beClosed = 3
					c.closeRead <- 1
					c.closeEachComponent()
					return
				}
				// No data from server ever, send ACK(0)
				if c.readSeqNum == -1 || c.receivedLastEpoch == 0{
					c.sendMsg(MsgAck, 0, nil)
				}
				c.receivedLastEpoch = 0 
				// Check each on fly message and re-send if needed
				for seqNum, epInfo := range c.writeOnFly {
					// Reach limit, resend
					if epInfo.sentEpoch+epInfo.currentBackOff+1 == c.currentEpoch {
						payload := c.writeDataBuffer.data[seqNum-1]
						c.sendMsg(MsgData, seqNum, payload)
						// Calculate new backoff
						newBackoff := epInfo.currentBackOff * 2
						if epInfo.currentBackOff == 0 {
							newBackoff = 1
						}
						if newBackoff > c.params.MaxBackOffInterval {
							newBackoff = c.params.MaxBackOffInterval
						}
						c.writeOnFly[seqNum] = epochInfo{c.currentEpoch, newBackoff}
					}
				}
			}
		// close event
		case <-c.clientClose:
			//fmt.Printf("[ClientClose] Set c.beClosed Value\n")
			c.beClosed = 1
			// All data has ack, done
			if c.writeWindowBase > len(c.writeDataBuffer.data) {
				c.returnToClose()
				return
			}
		// Write() is called
		case newData := <-c.writeDataChanel.chanel:
			// Put the data in buffer
			c.writeDataBuffer.data = append(c.writeDataBuffer.data, newData)
			// Check if data need to be sent
			c.sendClientData()
		// New message from server
		case newMessage := <-c.readMessageChanel.chanel:
			// Update epoch
			c.lastMsgEpoch = c.currentEpoch
			response := c.myUnmarshal(newMessage)
			// Data message
			if response.Type == 1 {
				// Check length
				if response.Size < len(response.Payload) {
					response.Payload = response.Payload[0:response.Size]
				} else if response.Size > len(response.Payload) {
					continue
				}
				c.receivedLastEpoch = 1
				// Send ACK
				c.sendMsg(MsgAck, response.SeqNum, nil)
				// If it's first message from server
				if c.readSeqNum == -1 {
					// First message arrived in order
					if response.SeqNum == 1 {
						c.readSeqNum = response.SeqNum + 1
						c.readDataChanel.chanel <- response.Payload
					} else {
						// First message is out of order, buffer it first
						c.readSeqNum = 1
						c.readDataReceived.buf[response.SeqNum] = response.Payload
					}
				} else {
					// Discard message already received
					if response.SeqNum < c.readSeqNum {
						continue
					}
					// Message is next message we want
					if response.SeqNum == c.readSeqNum {
						// Send to Read()
						c.readDataChanel.chanel <- response.Payload
						c.readSeqNum = c.readSeqNum + 1
						// Check the out-of-order data buffer
						data, ok := c.readDataReceived.buf[c.readSeqNum]
						for ok {
							c.readSeqNum = c.readSeqNum + 1
							c.readDataChanel.chanel <- data
							data, ok = c.readDataReceived.buf[c.readSeqNum]
						}
					} else {
						// Out-of-order message, buffer it
						c.readDataReceived.buf[response.SeqNum] = response.Payload
					}
				}
			} else {
				// ACK message
				if response.Type == 2 {
					// If sequence number is 0
					if response.SeqNum == 0 {
						// If connection has not been established
						if c.connID == -1 {
							c.connID = response.ConnID
							c.connResult <- 1
						}
						// Else ignore
						continue
					}
					// Mark the data has been received
					c.writeACKReceived[response.SeqNum] = 1
					// Delete from on fly data set
					delete(c.writeOnFly, response.SeqNum)
					// If it's ACK of window base, move the window forward
					if response.SeqNum == c.writeWindowBase {
						// Check the received out-of-order ACK
						_, ok := c.writeACKReceived[c.writeWindowBase]
						for ok {
							c.writeWindowBase += 1
							_, ok = c.writeACKReceived[c.writeWindowBase]
						}
						// Check if data in the window can be sent
						c.sendClientData()
					}
					// Close() has been called and all data has been received,return
					if c.beClosed == 1 &&
						c.writeWindowBase > len(c.writeDataBuffer.data) {
						c.closeEachComponent()
						defer c.returnToClose()
						return
					}
				}
			}
		}
	}
}

/*
 * readRoutine(): read new message from the connection
 */
func (c *client) readRoutine() {
	for {
		select {
		// Signal to close read routine
		case <-c.closeEachComp:
			return
		// Read message from connection
		default:
			ack := make([]byte, 1500)
			n, _, _ := c.conn.ReadFromUDP(ack)
			ack = ack[0:n]
			c.readMessageChanel.chanel <- ack
		}
	}
}

/*
 * ConnID(): return connection ID
 */
func (c *client) ConnID() int {
	return c.connID
}

/*
 * Read(): Read() data sent by server to client
 */
func (c *client) Read() ([]byte, error) {
	select {
	// Return data
	case data := <-c.readDataChanel.chanel:
		return data, nil
	// If client is closed during Read(), return error
	case <-c.closeRead:
		return nil, errors.New("Server closed!")
	}
}

/*
 * Write(): Write data to server
 */
func (c *client) Write(payload []byte) error {
	c.writeDataChanel.chanel <- payload
	return nil
}

/*
 * Close(): Close all routines after all data has been received and sent
 */
func (c *client) Close() error {
	c.clientClose <- 1
	<-c.closeFinished
	return nil
}
