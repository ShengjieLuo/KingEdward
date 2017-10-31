/******************************************************************
 **                    15-640 Project1 CheckPoint3               **
 **                     Distributed Bitcoin Miner                **
 ******************************************************************
 **Intorduction:                                                 **
 **1. Build upon the LSP protocol( client end).                  **
 **2. Implementation the miner which receive jobs from server    **
 **   and send back the calculated result.					     **
 ******************************************************************
 **Author:                                                       **
 ** Shengjie Luo shengjil@andrew.cmu.edu                         **
 ** Ke Chang     kec1@andrew.cmu.edu                             **
 ******************************************************************
 */
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
	// New miner
	miner, err := lsp.NewClient(hostport, lsp.NewParams())
	// Send join request
	byteRequest, _ := json.Marshal(bitcoin.NewJoin())
	// Send request
	miner.Write(byteRequest)
	return miner, err
}

// Do the work, find minimal hash value
func findMinimalHash(hsh string, Lower uint64, Upper uint64) (resHsh uint64, nonce uint64) {
	resHsh = bitcoin.Hash(hsh, Lower)
	nonce = Lower
	// Search from Lower to Upper
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
	miner, err := joinWithServer(hostport) // Join
	if err != nil {
		fmt.Println("Failed to join with server:", err)
		return
	}

	// Wait for new job
	for {
		// Read new job
		newMsg, err := miner.Read()
		// Closed connection
		if err != nil {
			return
		}
		// Unmarshal request
		var newRequest = new(bitcoin.Message)
		json.Unmarshal(newMsg, newRequest)
		// Calculate result
		resHsh, nonce := findMinimalHash(newRequest.Data, newRequest.Lower, newRequest.Upper)
		// Marshal and send result
		byteRequest, _ := json.Marshal(bitcoin.NewResult(resHsh, nonce))
		miner.Write(byteRequest)
	}
	miner.Close()
	return
}
