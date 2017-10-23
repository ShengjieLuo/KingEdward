/******************************************************************
 **                    15-640 Project1 CheckPoint3               **
 **                     Distributed Bitcoin Miner                **
 ******************************************************************
 **Intorduction:                                                 **
 **1. Build upon the LSP protocol( client end).                  **
 **2. Implementation the client which send jobs to server and    **
 **   output result. 						 **
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
	"strconv"
)

func main() {
	const numArgs = 4
	if len(os.Args) != numArgs {
		fmt.Printf("Usage: ./%s <hostport> <message> <maxNonce>", os.Args[0])
		return
	}
	hostport := os.Args[1]
	message := os.Args[2]
	maxNonce, err := strconv.ParseUint(os.Args[3], 10, 64)
	//fmt.Printf("[Client] Message:%s Nonce:%d \n",message,maxNonce)
	if err != nil {
		fmt.Printf("%s is not a number.\n", os.Args[3])
		return
	}

	client, err := lsp.NewClient(hostport, lsp.NewParams())
	if err != nil {
		fmt.Println("Failed to connect to server:", err)
		return
	}

	// New request message
	requestMessge := bitcoin.NewRequest(message, 0, maxNonce)
	byteRequest, _ := json.Marshal(requestMessge) // Marshal msg
	client.Write(byteRequest)                     // Send msg
	byteResult, err := client.Read()              // Wait for response
	// Server closed
	if err != nil {
		printDisconnected()
	} else {
		// Unmarshal result
		var result = new(bitcoin.Message)
		json.Unmarshal(byteResult, result)
		// Output
		printResult(result.Hash, result.Nonce)
		// CLose client
		client.Close()
	}
	return
}

// printResult prints the final result to stdout.
func printResult(hash, nonce uint64) {
	fmt.Println("Result", hash, nonce)
}

// printDisconnected prints a disconnected message to stdout.
func printDisconnected() {
	fmt.Println("Disconnected")
}
