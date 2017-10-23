package main

import (
	"encoding/json"
	"fmt"
	"github.com/cmu440/bitcoin"
	"github.com/cmu440/lsp"
	"os"
)

// Attempt to connect miner as a client to the server.
func joinWithServer(hostport string) (lsp.Client, error) {
	// TODO: implement this!
	miner, err := lsp.NewClient(hostport, lsp.NewParams())
	byteRequest, _ := json.Marshal(bitcoin.NewJoin())
	miner.Write(byteRequest)
	return miner, err
}

func findMinimalHash(hsh string, Lower uint64, Upper uint64) (resHsh uint64, nonce uint64) {
	resHsh = bitcoin.Hash(hsh, Lower)
	nonce = Lower
	for i := Lower + 1; i <= Upper; i++ {
		t := bitcoin.Hash(hsh, i)
		if t < resHsh {
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

	for {
		newMsg, err := miner.Read()
		if err != nil {
			return
		}
		var newRequest = new(bitcoin.Message)
		json.Unmarshal(newMsg, newRequest)
		resHsh, nonce := findMinimalHash(newRequest.Data, newRequest.Lower, newRequest.Upper)
		byteRequest, _ := json.Marshal(bitcoin.NewResult(resHsh, nonce))
		miner.Write(byteRequest)
	}
	miner.Close()
	return
}
