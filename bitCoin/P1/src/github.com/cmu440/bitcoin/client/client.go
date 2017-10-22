package main

import (
	"fmt"
	"os"
	"encoding/json"
	"strconv"
	"github.com/cmu440/bitcoin"
	"github.com/cmu440/lsp"
)

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

func main() {
	const numArgs = 4
	if len(os.Args) != numArgs {
		fmt.Printf("Usage: ./%s <hostport> <message> <maxNonce>", os.Args[0])
		return
	}
	hostport := os.Args[1]
	message := os.Args[2]
	maxNonce, err := strconv.ParseUint(os.Args[3], 10, 64)
	if err != nil {
		fmt.Printf("%s is not a number.\n", os.Args[3])
		return
	}

	client, err := lsp.NewClient(hostport, lsp.NewParams())
	if err != nil {
		fmt.Println("Failed to connect to server:", err)
		return
	}

	defer client.Close()

	//_ = message    // Keep compiler happy. Please remove!
	//_ = maxNonce   // Keep compiler happy. Please remove!
	// TODO: implement this!
	requestMessge := bitcoin.NewRequest(message, 0, maxNonce)
	byteRequest, _ := json.Marshal(requestMessge)
	client.Write(byteRequest)
	result, err := client.Read()
	if err != nil{
		printDisconnected()
		return
	}
	newResult := parseMessage(result, 1)
	printResult(newResult.Hash, newResult.Nonce)
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
