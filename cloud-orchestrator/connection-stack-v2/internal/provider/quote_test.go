package provider

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/aws-sdk-go-v2/service/pricing"

	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/api"
	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/contract"
)

func TestOnDemandQuoteProviderReturnsBoundCapacityAndIndependentPriceCeilings(t *testing.T) {
	provider, err := NewOnDemandQuoteProvider(fakeEC2QuoteAPI{}, fakePricingAPI{})
	if err != nil {
		t.Fatalf("NewOnDemandQuoteProvider(): %v", err)
	}
	command, request := quoteProviderCommand(t)
	now := time.Date(2026, 7, 15, 1, 2, 4, 0, time.UTC)
	quote, err := provider.Quote(t.Context(), command, request, now)
	if err != nil {
		t.Fatalf("Quote(): %v", err)
	}
	if len(quote.Candidates) != 1 {
		t.Fatalf("candidates = %#v", quote.Candidates)
	}
	candidate := quote.Candidates[0]
	if candidate.Architecture != "amd64" || candidate.VCPU != 2 || candidate.MemoryMiB != 8192 || candidate.GPUCount != 0 || candidate.GPUMemoryMiB != 0 {
		t.Fatalf("capacity = %#v", candidate)
	}
	if candidate.HourlyMinor != 9 || candidate.ThirtyDayMinor != 5991 || candidate.StartupUpperMinor != 0 {
		t.Fatalf("cost estimate = %#v", candidate)
	}
	if len(candidate.AvailabilityZones) != 2 || candidate.AvailabilityZones[0] != "ap-northeast-1a" || candidate.AvailabilityZones[1] != "ap-northeast-1c" {
		t.Fatalf("availability zones = %#v", candidate.AvailabilityZones)
	}
	if err := contract.ValidateQuoteResult(command, request, contract.QuoteResult{
		Status: "quote_issued",
		Receipt: contract.QuoteReceipt{
			Schema: contract.ReceiptSchema, Disposition: "committed", ConnectionID: command.ConnectionID,
			ExpectedGeneration: command.ExpectedGeneration, NodeCounter: command.NodeCounter, CommandID: command.CommandID,
			RequestSHA256: quote.RequestSHA256, Action: contract.ActionQuoteRequest, Quote: &quote,
		},
		Quote: quote,
	}); err != nil {
		t.Fatalf("quote does not satisfy public contract: %v", err)
	}
}

func TestOnDemandQuoteProviderRejectsAmbiguousPrice(t *testing.T) {
	provider, err := NewOnDemandQuoteProvider(fakeEC2QuoteAPI{}, fakePricingAPI{duplicate: true})
	if err != nil {
		t.Fatal(err)
	}
	command, request := quoteProviderCommand(t)
	_, err = provider.Quote(t.Context(), command, request, time.Date(2026, 7, 15, 1, 2, 4, 0, time.UTC))
	var providerErr *api.Error
	if !errors.As(err, &providerErr) || providerErr.Code != "quote_price_ambiguous" || providerErr.StatusCode != 502 {
		t.Fatalf("Quote() error = %v", err)
	}
}

type fakeEC2QuoteAPI struct{}

func (fakeEC2QuoteAPI) DescribeInstanceTypeOfferings(_ context.Context, _ *ec2.DescribeInstanceTypeOfferingsInput, _ ...func(*ec2.Options)) (*ec2.DescribeInstanceTypeOfferingsOutput, error) {
	return &ec2.DescribeInstanceTypeOfferingsOutput{InstanceTypeOfferings: []ec2types.InstanceTypeOffering{
		{InstanceType: ec2types.InstanceType("t3.large"), Location: aws.String("ap-northeast-1c")},
		{InstanceType: ec2types.InstanceType("t3.large"), Location: aws.String("ap-northeast-1a")},
	}}, nil
}

func (fakeEC2QuoteAPI) DescribeInstanceTypes(_ context.Context, _ *ec2.DescribeInstanceTypesInput, _ ...func(*ec2.Options)) (*ec2.DescribeInstanceTypesOutput, error) {
	return &ec2.DescribeInstanceTypesOutput{InstanceTypes: []ec2types.InstanceTypeInfo{{
		InstanceType:  ec2types.InstanceType("t3.large"),
		ProcessorInfo: &ec2types.ProcessorInfo{SupportedArchitectures: []ec2types.ArchitectureType{ec2types.ArchitectureTypeX8664}},
		VCpuInfo:      &ec2types.VCpuInfo{DefaultVCpus: aws.Int32(2)},
		MemoryInfo:    &ec2types.MemoryInfo{SizeInMiB: aws.Int64(8192)},
	}}}, nil
}

type fakePricingAPI struct{ duplicate bool }

func (f fakePricingAPI) GetProducts(_ context.Context, _ *pricing.GetProductsInput, _ ...func(*pricing.Options)) (*pricing.GetProductsOutput, error) {
	document := `{"product":{"attributes":{"instanceType":"t3.large","regionCode":"ap-northeast-1"}},"terms":{"OnDemand":{"term":{"priceDimensions":{"dimension":{"unit":"Hrs","beginRange":"0","endRange":"Inf","pricePerUnit":{"USD":"0.0832"}}}}}}}`
	priceList := []string{document}
	if f.duplicate {
		priceList = append(priceList, document)
	}
	return &pricing.GetProductsOutput{PriceList: priceList}, nil
}

func quoteProviderCommand(t *testing.T) (contract.Command, contract.QuoteRequest) {
	t.Helper()
	request := contract.QuoteRequest{
		QuoteRequestID: "quote-request-0001",
		PlanDigest:     "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Region:         "ap-northeast-1",
		Candidates: []contract.QuoteCandidate{{
			CandidateID: "candidate-0001", Tier: "recommended", InstanceType: "t3.large",
			PurchaseOption: "on_demand", EstimatedDiskGiB: 40,
		}},
	}
	payload, err := json.Marshal(request)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	sum := sha256.Sum256(payload)
	command := contract.Command{
		Schema: contract.CommandSchema, ConnectionID: "connection-0001", CommandID: "command-0001", NodeKeyID: "node-key-01",
		IssuedAt: "2026-07-15T01:02:03.000Z", ExpiresAt: "2026-07-15T01:07:03.000Z", ExpectedGeneration: 1,
		NodeCounter: 7, Action: contract.ActionQuoteRequest, PayloadB64: base64.StdEncoding.EncodeToString(payload),
		PayloadSHA256: hex.EncodeToString(sum[:]), SignatureB64: base64.StdEncoding.EncodeToString(make([]byte, ed25519.SignatureSize)),
	}
	decoded, err := command.QuoteRequest()
	if err != nil {
		t.Fatalf("QuoteRequest(): %v", err)
	}
	return command, decoded
}
