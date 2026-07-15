package main

import (
	"log"
	"os"

	"github.com/aws/aws-lambda-go/lambda"

	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/api"
	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/lambdaadapter"
)

func main() {
	resolver, err := api.NewStaticKeyResolver(
		os.Getenv("DIREXTALK_CONNECTION_ID"),
		os.Getenv("DIREXTALK_NODE_KEY_ID"),
		os.Getenv("DIREXTALK_NODE_PUBLIC_KEY_SPKI_B64"),
	)
	if err != nil {
		// The values are deliberately not included in the log line. Lambda still
		// starts and the public endpoint fails closed rather than admitting an
		// unconfigured command.
		log.Printf("connection stack broker static registration is invalid")
	}
	broker := api.Broker{Resolver: resolver}
	lambda.Start(lambdaadapter.New(broker).Handle)
}
