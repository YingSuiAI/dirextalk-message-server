package provider

import (
	"context"
	"encoding/json"
	"math/big"
	"sort"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/aws-sdk-go-v2/service/pricing"
	pricingtypes "github.com/aws/aws-sdk-go-v2/service/pricing/types"

	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/api"
	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/contract"
)

const hoursPerThirtyDays = int64(24 * 30)

type EC2QuoteAPI interface {
	DescribeInstanceTypeOfferings(ctx context.Context, params *ec2.DescribeInstanceTypeOfferingsInput, optFns ...func(*ec2.Options)) (*ec2.DescribeInstanceTypeOfferingsOutput, error)
	DescribeInstanceTypes(ctx context.Context, params *ec2.DescribeInstanceTypesInput, optFns ...func(*ec2.Options)) (*ec2.DescribeInstanceTypesOutput, error)
}

type PricingAPI interface {
	GetProducts(ctx context.Context, params *pricing.GetProductsInput, optFns ...func(*pricing.Options)) (*pricing.GetProductsOutput, error)
}

type OnDemandQuoteProvider struct {
	ec2     EC2QuoteAPI
	pricing PricingAPI
}

func NewOnDemandQuoteProvider(ec2Client EC2QuoteAPI, pricingClient PricingAPI) (*OnDemandQuoteProvider, error) {
	if ec2Client == nil || pricingClient == nil {
		return nil, api.NewError("quote_provider_unavailable", 503)
	}
	return &OnDemandQuoteProvider{ec2: ec2Client, pricing: pricingClient}, nil
}

func (p *OnDemandQuoteProvider) Quote(ctx context.Context, command contract.Command, request contract.QuoteRequest, now time.Time) (contract.Quote, error) {
	if p == nil || p.ec2 == nil || p.pricing == nil || len(request.Candidates) < 1 || len(request.Candidates) > 3 {
		return contract.Quote{}, api.NewError("quote_provider_unavailable", 503)
	}
	instanceTypes := make([]ec2types.InstanceType, 0, len(request.Candidates))
	for _, candidate := range request.Candidates {
		instanceTypes = append(instanceTypes, ec2types.InstanceType(candidate.InstanceType))
	}
	offerings, err := p.ec2.DescribeInstanceTypeOfferings(ctx, &ec2.DescribeInstanceTypeOfferingsInput{
		LocationType: ec2types.LocationTypeAvailabilityZone,
		Filters:      []ec2types.Filter{{Name: aws.String("instance-type"), Values: candidateInstanceTypeStrings(request.Candidates)}},
		MaxResults:   aws.Int32(100),
	})
	if err != nil || offerings.NextToken != nil {
		return contract.Quote{}, api.NewError("quote_provider_unavailable", 503)
	}
	zones, err := zonesByInstance(offerings.InstanceTypeOfferings, request)
	if err != nil {
		return contract.Quote{}, err
	}
	described, err := p.ec2.DescribeInstanceTypes(ctx, &ec2.DescribeInstanceTypesInput{InstanceTypes: instanceTypes, MaxResults: aws.Int32(100)})
	if err != nil || described.NextToken != nil {
		return contract.Quote{}, api.NewError("quote_provider_unavailable", 503)
	}
	capacities, err := capacitiesByInstance(described.InstanceTypes, request)
	if err != nil {
		return contract.Quote{}, err
	}
	quoted := make([]contract.QuotedCandidate, 0, len(request.Candidates))
	for _, candidate := range request.Candidates {
		hourlyMinor, thirtyDayMinor, priceErr := p.priceMinors(ctx, request.Region, candidate.InstanceType)
		if priceErr != nil {
			return contract.Quote{}, priceErr
		}
		capacity := capacities[candidate.InstanceType]
		quoted = append(quoted, contract.QuotedCandidate{
			CandidateID: candidate.CandidateID, Tier: candidate.Tier, InstanceType: candidate.InstanceType,
			PurchaseOption: candidate.PurchaseOption, EstimatedDiskGiB: candidate.EstimatedDiskGiB,
			Architecture: capacity.architecture, VCPU: capacity.vcpu, MemoryMiB: capacity.memoryMiB,
			GPUCount: capacity.gpuCount, GPUMemoryMiB: capacity.gpuMemoryMiB,
			HourlyMinor: hourlyMinor, ThirtyDayMinor: thirtyDayMinor, StartupUpperMinor: 0,
			AvailabilityZones: append([]string(nil), zones[candidate.InstanceType]...),
		})
	}
	requestSHA, err := command.RequestSHA256()
	if err != nil {
		return contract.Quote{}, api.NewError("quote_provider_unavailable", 503)
	}
	quotedAt := now.UTC().Truncate(time.Millisecond)
	return contract.Quote{
		Schema: contract.QuoteSchema, QuoteID: "quote-" + requestSHA[:32], ConnectionID: command.ConnectionID,
		CommandID: command.CommandID, RequestSHA256: requestSHA, QuoteRequestID: request.QuoteRequestID,
		PlanDigest: request.PlanDigest, Region: request.Region, Currency: "USD",
		QuotedAt: contract.CanonicalInstant(quotedAt), ValidUntil: contract.CanonicalInstant(quotedAt.Add(contract.QuoteValidity)),
		Candidates: quoted, IncludedItems: []string{"ec2_linux_ondemand"},
		UnincludedItems: []string{"cloudwatch_logs", "data_transfer", "ebs_gp3", "public_ipv4", "snapshots", "taxes"},
	}, nil
}

func (p *OnDemandQuoteProvider) priceMinors(ctx context.Context, region, instanceType string) (int64, int64, error) {
	filters := []pricingtypes.Filter{
		pricingFilter("regionCode", region),
		pricingFilter("instanceType", instanceType),
		pricingFilter("operatingSystem", "Linux"),
		pricingFilter("tenancy", "Shared"),
		pricingFilter("preInstalledSw", "NA"),
		pricingFilter("licenseModel", "No License required"),
		pricingFilter("capacitystatus", "Used"),
	}
	output, err := p.pricing.GetProducts(ctx, &pricing.GetProductsInput{
		ServiceCode: aws.String("AmazonEC2"), FormatVersion: aws.String("aws_v1"), MaxResults: aws.Int32(100), Filters: filters,
	})
	if err != nil {
		return 0, 0, api.NewError("quote_provider_unavailable", 503)
	}
	if output.NextToken != nil || len(output.PriceList) == 0 {
		return 0, 0, api.NewError("quote_price_ambiguous", 502)
	}
	prices := make([]string, 0, 1)
	for _, raw := range output.PriceList {
		var document priceDocument
		if json.Unmarshal([]byte(raw), &document) != nil || document.Product.Attributes.InstanceType != instanceType || document.Product.Attributes.RegionCode != region {
			return 0, 0, api.NewError("quote_price_ambiguous", 502)
		}
		for _, term := range document.Terms.OnDemand {
			for _, dimension := range term.PriceDimensions {
				if dimension.Unit == "Hrs" && dimension.BeginRange == "0" && dimension.EndRange == "Inf" {
					if value := dimension.PricePerUnit["USD"]; value != "" {
						prices = append(prices, value)
					}
				}
			}
		}
	}
	if len(prices) != 1 {
		return 0, 0, api.NewError("quote_price_ambiguous", 502)
	}
	hourlyMinor, hourlyOK := decimalCeiling(prices[0], 100)
	thirtyDayMinor, monthlyOK := decimalCeiling(prices[0], 100*hoursPerThirtyDays)
	if !hourlyOK || !monthlyOK {
		return 0, 0, api.NewError("quote_price_ambiguous", 502)
	}
	return hourlyMinor, thirtyDayMinor, nil
}

type capacity struct {
	architecture string
	vcpu         int64
	memoryMiB    int64
	gpuCount     int64
	gpuMemoryMiB int64
}

func capacitiesByInstance(values []ec2types.InstanceTypeInfo, request contract.QuoteRequest) (map[string]capacity, error) {
	result := make(map[string]capacity, len(values))
	for _, info := range values {
		name := string(info.InstanceType)
		if name == "" || info.VCpuInfo == nil || info.VCpuInfo.DefaultVCpus == nil || info.MemoryInfo == nil || info.MemoryInfo.SizeInMiB == nil || info.ProcessorInfo == nil {
			return nil, api.NewError("quote_capacity_invalid", 502)
		}
		architecture := ""
		for _, value := range info.ProcessorInfo.SupportedArchitectures {
			switch value {
			case ec2types.ArchitectureTypeX8664:
				if architecture != "" && architecture != "amd64" {
					return nil, api.NewError("quote_capacity_invalid", 502)
				}
				architecture = "amd64"
			case ec2types.ArchitectureTypeArm64:
				if architecture != "" && architecture != "arm64" {
					return nil, api.NewError("quote_capacity_invalid", 502)
				}
				architecture = "arm64"
			}
		}
		if architecture == "" || *info.VCpuInfo.DefaultVCpus < 1 || *info.MemoryInfo.SizeInMiB < 1 {
			return nil, api.NewError("quote_capacity_invalid", 502)
		}
		entry := capacity{architecture: architecture, vcpu: int64(*info.VCpuInfo.DefaultVCpus), memoryMiB: *info.MemoryInfo.SizeInMiB}
		if info.GpuInfo != nil {
			for _, gpu := range info.GpuInfo.Gpus {
				if gpu.Count == nil || *gpu.Count < 1 || gpu.MemoryInfo == nil || gpu.MemoryInfo.SizeInMiB == nil || *gpu.MemoryInfo.SizeInMiB < 1 {
					return nil, api.NewError("quote_capacity_invalid", 502)
				}
				entry.gpuCount += int64(*gpu.Count)
				entry.gpuMemoryMiB += int64(*gpu.Count) * int64(*gpu.MemoryInfo.SizeInMiB)
			}
		}
		if _, duplicate := result[name]; duplicate {
			return nil, api.NewError("quote_capacity_invalid", 502)
		}
		result[name] = entry
	}
	for _, candidate := range request.Candidates {
		if _, ok := result[candidate.InstanceType]; !ok {
			return nil, api.NewError("quote_capacity_invalid", 502)
		}
	}
	return result, nil
}

func zonesByInstance(values []ec2types.InstanceTypeOffering, request contract.QuoteRequest) (map[string][]string, error) {
	allowed := make(map[string]struct{}, len(request.Candidates))
	for _, candidate := range request.Candidates {
		allowed[candidate.InstanceType] = struct{}{}
	}
	sets := make(map[string]map[string]struct{}, len(allowed))
	for _, offering := range values {
		name := string(offering.InstanceType)
		zone := aws.ToString(offering.Location)
		if _, ok := allowed[name]; !ok || !contract.ValidAvailabilityZone(request.Region, zone) {
			continue
		}
		if sets[name] == nil {
			sets[name] = map[string]struct{}{}
		}
		sets[name][zone] = struct{}{}
	}
	result := make(map[string][]string, len(allowed))
	for name := range allowed {
		if len(sets[name]) == 0 {
			return nil, api.NewError("quote_instance_unavailable", 409)
		}
		for zone := range sets[name] {
			result[name] = append(result[name], zone)
		}
		sort.Strings(result[name])
	}
	return result, nil
}

type priceDocument struct {
	Product struct {
		Attributes struct {
			InstanceType string `json:"instanceType"`
			RegionCode   string `json:"regionCode"`
		} `json:"attributes"`
	} `json:"product"`
	Terms struct {
		OnDemand map[string]struct {
			PriceDimensions map[string]struct {
				Unit         string            `json:"unit"`
				BeginRange   string            `json:"beginRange"`
				EndRange     string            `json:"endRange"`
				PricePerUnit map[string]string `json:"pricePerUnit"`
			} `json:"priceDimensions"`
		} `json:"OnDemand"`
	} `json:"terms"`
}

func pricingFilter(field, value string) pricingtypes.Filter {
	return pricingtypes.Filter{Field: aws.String(field), Type: pricingtypes.FilterTypeTermMatch, Value: aws.String(value)}
}

func candidateInstanceTypeStrings(candidates []contract.QuoteCandidate) []string {
	result := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		result = append(result, candidate.InstanceType)
	}
	return result
}

func decimalCeiling(value string, multiplier int64) (int64, bool) {
	rat, ok := new(big.Rat).SetString(value)
	if !ok || rat.Sign() < 0 || multiplier < 0 {
		return 0, false
	}
	rat.Mul(rat, new(big.Rat).SetInt64(multiplier))
	quotient, remainder := new(big.Int), new(big.Int)
	quotient.QuoRem(rat.Num(), rat.Denom(), remainder)
	if remainder.Sign() > 0 {
		quotient.Add(quotient, big.NewInt(1))
	}
	if !quotient.IsInt64() || quotient.Int64() > 9007199254740991 {
		return 0, false
	}
	return quotient.Int64(), true
}
