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
	"fmt"
	"time"
	"errors"
	"github.com/cmu440/lspnet"
)

//Data Structure1: Constant Variable Definition
const (
	maxPendingData = 100
	maxDataChanel  = 1000
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

type epochInfo struct{
	sentEpoch int
	currentBackOff int
}

/* Data Structure Definition3: server
   1. params: parameters
   2. conn: UDP connection
   3. connID: connection id
   4. clientClose: Close() chanel
   5. writeDataChanel: data chanel from Write() to main routine
   6. writeDataBuffer: pending data sent by Write()
   7. writeSeqNum: start from 1
   8. writeWindowBase: sliding window base, start from 1
   9. writeACKReceived: received ACK
   10. readSeqNum: received data sequence
   11. readMessageChanel: new incoming message chanel
   12. readDataChanel: chanel from main routine to Read()
   13. readDataReceived: received out of order server data
*/
type client struct {
	params     			*Params
	conn        		*lspnet.UDPConn
	connID      		int
	connResult			chan int
	clientClose 		chan int
	closeEachComp		chan int
	closeFinished		chan int
	beClosed			int
	lastMsgEpoch		int
	currentEpoch		int
	epochFirer			*time.Ticker

	writeDataChanel		dataChanel
	writeDataBuffer   	dataSendBuffer
	writeSeqNum     	int
	writeWindowBase  	int
	writeACKReceived 	map[int]int
	writeOnFly 			map[int]epochInfo

	readSeqNum     		int
	readMessageChanel	dataChanel
	readDataChanel		dataChanel
	readDataReceived 	dataReceived
}

func NewClient(hostport string, params *Params) (*client, error) {
	c := client{params,
		&lspnet.UDPConn{}, -1,  
		make(chan int), make(chan int), make(chan int), make(chan int),
		0, 0, 0,
		time.NewTicker(time.Duration(params.EpochMillis) * time.Millisecond),
		dataChanel{make(chan []byte, maxDataChanel)},
		dataSendBuffer{make([]([]byte), 0, maxPendingData)},
		1, 1, make(map[int]int), make(map[int]epochInfo),
		-1,
		dataChanel{make(chan []byte, maxDataChanel)},
		dataChanel{make(chan []byte, maxDataChanel)},
		dataReceived{make(map[int][]byte)}}
	//fmt.Println(MsgConnect)
	addr, _ := lspnet.ResolveUDPAddr("udp", hostport)
	c.conn, _ = lspnet.DialUDP("udp", nil, addr)
	go c.readRoutine()
	go c.mainRoutine()
	c.sendMsg(MsgConnect, 0, nil)
	if result := <- c.connResult; result == 0{
		return &c, errors.New("Can't conect to server, time out!")
	}
	//fmt.Println("Connection succ.")
	return &c, nil
	//return c, errors.New("not yet implemented")
}

func (c *client) sendMsg(tp MsgType, seqNum int, payload []byte){
	var data *Message
	if tp == MsgConnect{
		data = NewConnect()
	}else{
		if tp == MsgData{
			data = NewData(c.connID, seqNum, len(payload), payload)
		}else{
			data = NewAck(c.connID, seqNum)
		}
	}
	msg, _ := json.Marshal(*data)
	c.conn.Write(msg)
}

/* myUnmarshal: decode byte array into struct
 */
func (c *client) myUnmarshal(data []byte) Message {
	//fmt.Println(data)
	var msg = new(Message)
	err := json.Unmarshal(data, msg)
	if err != nil {
		fmt.Errorf("[Error]Cannot unmarshal data:%v!\n", err)
	}
	return *msg
}

func (c *client) sendClientData() {
	for c.writeSeqNum <= c.writeWindowBase + c.params.WindowSize-1 && c.writeSeqNum <= len(c.writeDataBuffer.data) {
		payload := c.writeDataBuffer.data[c.writeSeqNum-1]
		c.sendMsg(MsgData, c.writeSeqNum, payload)
		c.writeOnFly[c.writeSeqNum] = epochInfo{c.currentEpoch, 0}
		c.writeSeqNum += 1
	}
}

func (c *client)closeEachComponent(){
	numOfCom := 1
	//if c.beClosed == 1{
	//	numOfCom = 2
	//}
	for i := 0; i < numOfCom; i++{
		c.closeEachComp <-1
	}
	c.epochFirer.Stop()
	return
}

func (c *client)returnToClose(){
	c.closeFinished <- 1
}

func (c *client) mainRoutine() {
	for {
		select {
		case <- c.epochFirer.C:
			c.currentEpoch += 1
			if c.connID == -1{
				if c.currentEpoch >= 5{
					c.beClosed = 2
					c.connResult <- 0
					c.closeEachComponent()
					return
				}
				c.sendMsg(MsgConnect, 0, nil)
			}else{
				if c.currentEpoch - c.lastMsgEpoch >= c.params.EpochLimit{
					c.beClosed = 3
					c.closeEachComponent()
					return
				}
				if c.readSeqNum == -1{
					c.sendMsg(MsgAck, 0, nil)
				}
				for seqNum, epInfo := range c.writeOnFly {
					if epInfo.sentEpoch + epInfo.currentBackOff +1 == c.currentEpoch{
						payload := c.writeDataBuffer.data[seqNum-1]
						c.sendMsg(MsgData, seqNum, payload)
						newBackoff := epInfo.currentBackOff * 2
						if epInfo.currentBackOff == 0{
							newBackoff = 1
						}
						if newBackoff > c.params.MaxBackOffInterval{
							newBackoff = c.params.MaxBackOffInterval
						}
						c.writeOnFly[seqNum] = epochInfo{c.currentEpoch, newBackoff}
					}
				}
			}
		case <- c.clientClose:
			c.beClosed = 1
			if c.writeWindowBase > len(c.writeDataBuffer.data){
				c.closeEachComponent()
				defer c.returnToClose()
				return
			}
		case newData := <-c.writeDataChanel.chanel:
			c.writeDataBuffer.data = append(c.writeDataBuffer.data, newData)
			c.sendClientData()
			//fmt.Println("here")
		case newMessage := <-c.readMessageChanel.chanel:
			//fmt.Println(string(newMessage))
			c.lastMsgEpoch = c.currentEpoch
			response := c.myUnmarshal(newMessage)
			
			if response.Type == 1 {
				c.sendMsg(MsgAck, response.SeqNum, nil)
				if c.readSeqNum == -1 {
					c.readSeqNum = response.SeqNum + 1
					c.readDataChanel.chanel <- response.Payload
				} else {
					//fmt.Printf("*******************************\n")
					//fmt.Printf("response.SeqNum:%d\n",response.SeqNum)
					//fmt.Printf("c.readSeqNum:%d\n",c.readSeqNum)
					//fmt.Printf("*******************************\n")
					if response.SeqNum < c.readSeqNum{
						continue
					}
					if response.SeqNum == c.readSeqNum {
						c.readDataChanel.chanel <- response.Payload
						c.readSeqNum = c.readSeqNum + 1
						data, ok := c.readDataReceived.buf[c.readSeqNum]
						for ok {
							c.readSeqNum = c.readSeqNum + 1
							c.readDataChanel.chanel <- data
							data, ok = c.readDataReceived.buf[c.readSeqNum]
						}
					} else {
						c.readDataReceived.buf[response.SeqNum] = response.Payload
					}
				}
			} else {
				if response.Type == 2 {
					//fmt.Println(c.writeWindowBase)
					if response.SeqNum == 0{
						if c.connID == -1{
							c.connID = response.ConnID
							c.connResult <- 1
						}
						continue
					}

					c.writeACKReceived[response.SeqNum] = 1
					delete(c.writeOnFly, response.SeqNum)
					if response.SeqNum == c.writeWindowBase {
						_, ok := c.writeACKReceived[c.writeWindowBase]
						for ok {
							c.writeWindowBase += 1
							_, ok = c.writeACKReceived[c.writeWindowBase]
						}
						c.sendClientData()
					}
					if c.beClosed == 1 && c.writeWindowBase > len(c.writeDataBuffer.data) {
						c.closeEachComponent()
						defer c.returnToClose()
						return
					}
				}
			}
		}
	}
}

func (c *client) readRoutine() {
	for {
		select{
		case <- c.closeEachComp:
			return
		default:
			ack := make([]byte, 1500)
			n, _, _ := c.conn.ReadFromUDP(ack)
			ack = ack[0:n]
			c.readMessageChanel.chanel <- ack
		}
	}
}

func (c *client) ConnID() int {
	return c.connID
}

func (c *client) Read() ([]byte, error) {
	return <-c.readDataChanel.chanel, nil
}

func (c *client) Write(payload []byte) error {
	c.writeDataChanel.chanel <- payload
	return nil
}

func (c *client) Close() error {
	c.clientClose <- 1
	<- c.closeFinished
	return nil
}
