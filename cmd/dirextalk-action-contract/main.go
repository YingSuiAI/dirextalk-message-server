package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/YingSuiAI/dirextalk-message-server/p2p/serviceapi"
)

func main() {
	data, err := json.MarshalIndent(serviceapi.ActionContract(), "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "marshal action contract: %v\n", err)
		os.Exit(1)
	}
	os.Stdout.Write(data)
	os.Stdout.Write([]byte("\n"))
}
