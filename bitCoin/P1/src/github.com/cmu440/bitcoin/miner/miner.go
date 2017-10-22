package main

import (
	"fmt"
	"os"
	"encoding/json"
	"github.com/cmu440/bitcoin"
	"github.com/cmu440/lsp"
)

// Attempt to connect miner as a client to the server.
func joinWithServer(hostport string) (lsp.Client, error) {
	// TODO: implement this!
	//miner, err := lsp.NewClient(hostport, lsp.NewParams())
	return lsp.NewClient(hostport, lsp.NewParams())
}

func parseMessage(msg []byte, msgType int)(* bitcoin.Message){
	result := &bitcoin.Message{0, "", 0, 0, 0, 0}
	switch msgType{
	case 0:
		var s string
		fmt.Sscanf(string(msg), "[%s %s %d %d]", &s, &result.Data, &result.Lower, &result.Upper)
	case 1:
		var s string
		fmt.Sscanf(string(msg), "[%s %d %d]", &s, &result.Hash, &result.Nonce)
	}
	return result
}

func findMinimalHash(hsh string, Lower uint64, Upper uint64)(resHsh uint64, nonce uint64){
	resHsh = bitcoin.Hash(hsh, Lower)
	nonce = Lower
	for i := Lower + 1; i <= Upper; i++{
		t := bitcoin.Hash(hsh, i)
		if t < resHsh{
			resHsh = t
			nonce = i
		}
	}
	return
}

func main() {
	const numArgs = 2
	if len(os.Args) != numArgs {
		fmt.Printf("Usage: ./%s <hostport>", os.Args[0])
		return
	}

	hostport := os.Args[1]
	miner, err := joinWithServer(hostport)
	if err != nil {
		fmt.Println("Failed to join with server:", err)
		return
	}

	defer miner.Close()

	// TODO: implement this!
	for{
		newMsg, err := miner.Read()
		if err != nil{
			break
		}
		newRequest := parseMessage(newMsg, 0)
		if err != nil{
			fmt.Println("Error Unmarshaling!")
			return
		}
		resHsh, nonce := findMinimalHash(newRequest.Data, newRequest.Lower, newRequest.Upper)
		byteRequest, _ := json.Marshal(bitcoin.NewResult(resHsh, nonce))
		miner.Write(byteRequest)
	}
	return
}
