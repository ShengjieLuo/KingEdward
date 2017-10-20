#!/bin/bash

go test -race -run TestBasic1 #pass
go test -race -run TestBasic2 #pass
go test -race -run TestBasic3 #pass
go test -race -run TestBasic4 #pass
go test -race -run TestBasic5 #pass
go test -race -run TestBasic6 #pass
go test -race -run TestBasic7 #pass
go test -race -run TestBasic8 #pass
go test -race -run TestBasic9 #pass
go test -race -run TestSendReceive1 #pass
go test -race -run TestSendReceive2 #pass
go test -race -run TestSendReceive3 #pass
go test -race -run TestRobust1 #pass
go test -race -run TestRobust2 #pass
go test -race -run TestRobust3 #pass
go test -race -run TestRobust4 #pass
go test -race -run TestRobust5 #pass
go test -race -run TestRobust6 #pass
go test -race -run TestExpBackOff1 #pass
go test -race -run TestExpBackOff2 #pass
go test -race -run TestWindow1 #pass
go test -race -run TestWindow2 #pass
go test -race -run TestWindow3 #pass
go test -race -run TestWindow4 #pass
go test -race -run TestWindow5 #pass
go test -race -run TestWindow6 #pass
go test -race -run TestOutOfOrderMsg1 #pass
go test -race -run TestOutOfOrderMsg2 #pass
go test -race -run TestOutOfOrderMsg3 #pass
go test -race -run TestServerSlowStart1 #Timeout: Scheduled Time 2.50s Used Time 3.50s
go test -race -run TestServerSlowStart2 #Timeout: Scheduled Time 2.50s Used Time 4.21s
go test -race -run TestServerClose1 #pass 
go test -race -run TestServerClose2 #pass
go test -race -run TestServerCloseConns1 #pass
go test -race -run TestServerCloseConns2 #pass
go test -race -run TestClientClose1 #pass 
go test -race -run TestClientClose2 #pass
go test -race -run TestServerFastClose1
go test -race -run TestServerFastClose2
go test -race -run TestServerFastClose3
go test -race -run TestServerToClient1
go test -race -run TestServerToClient2
go test -race -run TestServerToClient3
go test -race -run TestClientToServer1
go test -race -run TestClientToServer2
go test -race -run TestClientToServer3
go test -race -run TestRoundTrip1
go test -race -run TestRoundTrip2
go test -race -run TestRoundTrip3
go test -race -run TestVariableLengthMsgServer
go test -race -run TestVariableLengthMsgClient
