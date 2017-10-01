package lsp

import (
	"fmt"
	//"errors"
	//"container/list"
	"encoding/json"
	"github.com/cmu440/lspnet"
	//"strings"
	//"strconv"
)

type client struct {
	params         *Params
	messagePending []([]byte)
	clientSeqNum   int
	windowLow      int

	clientSeqArrived map[int]int
	serverStartSend  bool
	serverSeqNum     int
	clientClose      chan int
	conn             *lspnet.UDPConn
	connID           int
	newClientData    chan []byte
	newMessage       chan []byte
	serverData       chan []byte
	serverDataBuffer map[int][]byte
}

func NewClient(hostport string, params *Params) (*client, error) {
	c := client{params, make([]([]byte), 0, 100), 0, 0, make(map[int]int), false, 0, make(chan int), &lspnet.UDPConn{}, 0, make(chan []byte, 500), make(chan []byte, 500), make(chan []byte, 500), make(map[int][]byte)}
	//fmt.Println(MsgConnect)
	addr, _ := lspnet.ResolveUDPAddr("udp", hostport)
	c.conn, _ = lspnet.DialUDP("udp", nil, addr)
	connMsg := Message{0, 0, 0, 0, nil}

	data, _ := json.Marshal(connMsg)
	c.conn.Write(data)
	ack := make([]byte, 1500)
	n, _, _ := c.conn.ReadFromUDP(ack)
	shortAck := ack[0:n]
	c.connID = c.myUnmarshal(shortAck).ConnID
	//fmt.Println(c.connID)
	go c.readRoutine()
	go c.mainRoutine()
	return &c, nil
	//return c, errors.New("not yet implemented")
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
	msg.SeqNum = msg.SeqNum - 1
	return *msg
}

func (c *client) sendClientData() {
	for c.clientSeqNum <= c.windowLow+c.params.WindowSize && c.clientSeqNum < len(c.messagePending) {
		payload := c.messagePending[c.clientSeqNum]
		data := NewData(c.connID, c.clientSeqNum+1, len(payload), payload)
		msg, _ := json.Marshal(*data)
		//fmt.Println(string(msg))
		c.conn.Write(msg)
		c.clientSeqNum += 1
	}
}

func (c *client) mainRoutine() {
	for {
		select {
		case newData := <-c.newClientData:
			c.messagePending = append(c.messagePending, newData)
			c.sendClientData()
			//fmt.Println("here")
		case newMessage := <-c.newMessage:
			//fmt.Println(string(newMessage))
			response := c.myUnmarshal(newMessage)
			if response.Type == 1 {
				ack := NewAck(c.connID, response.SeqNum+1)
				//fmt.Println("here")
				//fmt.Println(ack)
				msg, _ := json.Marshal(*ack)
				c.conn.Write(msg)
				if c.serverStartSend == false {
					c.serverStartSend = true
					c.serverSeqNum = response.SeqNum + 1
					c.serverData <- response.Payload
				} else {
					//fmt.Printf("*******************************\n")
					//fmt.Printf("response.SeqNum:%d\n",response.SeqNum)
					//fmt.Printf("c.serverSeqNum:%d\n",c.serverSeqNum)
					//fmt.Printf("*******************************\n")
					if response.SeqNum == c.serverSeqNum {
						c.serverData <- response.Payload
						c.serverSeqNum = c.serverSeqNum + 1
						data, ok := c.serverDataBuffer[c.serverSeqNum+1]
						for ok {
							c.serverSeqNum = c.serverSeqNum + 1
							c.serverData <- data
							data, ok = c.serverDataBuffer[c.serverSeqNum]
						}
					} else {
						c.serverDataBuffer[response.SeqNum] = response.Payload
					}
				}
			} else {
				if response.Type == 2 {
					//fmt.Println(c.windowLow)
					c.clientSeqArrived[response.SeqNum] = 1
					if response.SeqNum == c.windowLow {
						_, ok := c.clientSeqArrived[c.windowLow]
						for ok {
							c.windowLow += 1
							_, ok = c.clientSeqArrived[c.windowLow]
						}
						c.sendClientData()
					}
				}
			}
		}
	}
}

func (c *client) readRoutine() {
	for {
		ack := make([]byte, 1500)
		n, _, _ := c.conn.ReadFromUDP(ack)
		ack = ack[0:n]
		//fmt.Printf("------------------------------\n")
		//fmt.Println(ack)
		//fmt.Println(string(ack))
		//fmt.Printf("------------------------------\n")
		c.newMessage <- ack
	}
}

func (c *client) ConnID() int {
	return c.connID
}

func (c *client) Read() ([]byte, error) {
	return <-c.serverData, nil
}

func (c *client) Write(payload []byte) error {
	c.newClientData <- payload
	return nil
}

func (c *client) Close() error {
	return nil
}
