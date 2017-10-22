package main

import (
	"fmt"
	"os"
	"encoding/json"
	"strconv"
	"github.com/cmu440/bitcoin"
	"github.com/cmu440/lsp"
	"log"
	"time"
)


var LOGF *log.Logger

func initLogger()(*log.Logger){
        const (
                flag = os.O_RDWR | os.O_CREATE
                perm = os.FileMode(0666)
        )
	now  := time.Now()
        name := "logClient_"+strconv.FormatInt(now.Unix(),10)+".txt"
        file, err := os.OpenFile(name, flag, perm)
        if err != nil {
                return nil
        }
        defer file.Close()
        return log.New(file, "", log.Lshortfile|log.Lmicroseconds)
}

func main() {
	LOGF = initLogger()
	LOGF.Printf("[Client] Begin Execution\n")
	fmt.Printf("[Client] Begin Execution\n")
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

	requestMessge := bitcoin.NewRequest(message, 0, maxNonce)
	byteRequest, _ := json.Marshal(requestMessge)
	client.Write(byteRequest)
	byteResult, err := client.Read()
	if err != nil{
		LOGF.Printf("[Read] Logf Read Error and Exit now!\n")
		printDisconnected()
		return
	}
	var result = new(bitcoin.Message)
	LOGF.Printf("[Result] "+ result.String()+"\n")
	json.Unmarshal(byteResult, result)
	printResult(result.Hash, result.Nonce)
	//newResult := parseMessage(result, 1)
	//printResult(newResult.Hash, newResult.Nonce)
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
