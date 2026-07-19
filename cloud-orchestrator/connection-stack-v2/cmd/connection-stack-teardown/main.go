// connection-stack-teardown is an owner-operated Go cleanup controller for a
// disposable Connection Stack. It intentionally has no AWS credential, ARN,
// resource-name, or arbitrary-operation flags.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"

	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/stackteardown"
)

func main() {
	if err := run(context.Background(), os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "connection-stack-teardown: operation failed")
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return stackteardown.ErrInvalidRequest
	}
	switch args[0] {
	case "plan":
		request, err := parseRequest(args[1:], stderr)
		if err != nil {
			return err
		}
		configuration, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(request.Region))
		if err != nil {
			return stackteardown.ErrProviderUnavailable
		}
		plan, err := stackteardown.NewAWSService(configuration).BuildPlan(ctx, request)
		if err != nil {
			return err
		}
		return json.NewEncoder(stdout).Encode(plan)
	case "execute", "readback":
		plan, err := parsePlanFile(args[1:], stderr)
		if err != nil {
			return err
		}
		configuration, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(plan.Region))
		if err != nil {
			return stackteardown.ErrProviderUnavailable
		}
		service := stackteardown.NewAWSService(configuration)
		var report stackteardown.Report
		if args[0] == "execute" {
			report, err = service.Execute(ctx, plan)
		} else {
			report, err = service.ReadBack(ctx, plan)
		}
		if err != nil {
			return err
		}
		return json.NewEncoder(stdout).Encode(report)
	default:
		return stackteardown.ErrInvalidRequest
	}
}

func parseRequest(args []string, stderr io.Writer) (stackteardown.Request, error) {
	flags := flag.NewFlagSet("connection-stack-teardown plan", flag.ContinueOnError)
	flags.SetOutput(stderr)
	connectionID := flags.String("connection-id", "", "Connection ID from the approved Role Plan")
	region := flags.String("region", "", "AWS Region from the approved Role Plan")
	if flags.Parse(args) != nil || flags.NArg() != 0 {
		return stackteardown.Request{}, stackteardown.ErrInvalidRequest
	}
	request := stackteardown.Request{ConnectionID: *connectionID, Region: *region}
	if request.Validate() != nil {
		return stackteardown.Request{}, stackteardown.ErrInvalidRequest
	}
	return request, nil
}

func parsePlanFile(args []string, stderr io.Writer) (stackteardown.Plan, error) {
	flags := flag.NewFlagSet("connection-stack-teardown execute", flag.ContinueOnError)
	flags.SetOutput(stderr)
	path := flags.String("plan", "", "strict teardown plan JSON emitted by this command")
	if flags.Parse(args) != nil || flags.NArg() != 0 || *path == "" {
		return stackteardown.Plan{}, stackteardown.ErrInvalidRequest
	}
	raw, err := readRegular(*path, 128<<10)
	if err != nil {
		return stackteardown.Plan{}, err
	}
	plan, err := stackteardown.ParsePlan(raw)
	clear(raw)
	if err != nil {
		return stackteardown.Plan{}, err
	}
	return plan, nil
}

func readRegular(path string, maximum int64) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > maximum {
		return nil, stackteardown.ErrInvalidRequest
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, stackteardown.ErrInvalidRequest
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil || !os.SameFile(info, opened) || opened.Size() != info.Size() {
		return nil, stackteardown.ErrInvalidRequest
	}
	raw, err := io.ReadAll(io.LimitReader(file, maximum+1))
	if err != nil || int64(len(raw)) != opened.Size() {
		return nil, stackteardown.ErrInvalidRequest
	}
	return raw, nil
}
