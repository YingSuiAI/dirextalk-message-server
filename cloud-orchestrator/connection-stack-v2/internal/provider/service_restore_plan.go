package provider

import (
	"context"
	"encoding/json"
	"math/big"
	"sort"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/aws-sdk-go-v2/service/pricing"
	pricingtypes "github.com/aws/aws-sdk-go-v2/service/pricing/types"

	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/api"
	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/contract"
)

type ServiceRestorePlanEC2API interface {
	DescribeInstances(context.Context, *ec2.DescribeInstancesInput, ...func(*ec2.Options)) (*ec2.DescribeInstancesOutput, error)
	DescribeVolumes(context.Context, *ec2.DescribeVolumesInput, ...func(*ec2.Options)) (*ec2.DescribeVolumesOutput, error)
	DescribeSnapshots(context.Context, *ec2.DescribeSnapshotsInput, ...func(*ec2.Options)) (*ec2.DescribeSnapshotsOutput, error)
	DescribeImages(context.Context, *ec2.DescribeImagesInput, ...func(*ec2.Options)) (*ec2.DescribeImagesOutput, error)
}

type ServiceRestorePlanProvider struct {
	ec2     ServiceRestorePlanEC2API
	pricing PricingAPI
}

func NewServiceRestorePlanProvider(ec2Client ServiceRestorePlanEC2API, pricingClient PricingAPI) (*ServiceRestorePlanProvider, error) {
	if ec2Client == nil || pricingClient == nil {
		return nil, api.NewError("service_restore_plan_provider_unavailable", 503)
	}
	return &ServiceRestorePlanProvider{ec2: ec2Client, pricing: pricingClient}, nil
}

func (p *ServiceRestorePlanProvider) Plan(ctx context.Context, command contract.Command, request contract.ServiceRestorePlanRequest, now time.Time) (contract.ServiceRestorePlan, error) {
	if p == nil || p.ec2 == nil || p.pricing == nil {
		return contract.ServiceRestorePlan{}, api.NewError("service_restore_plan_provider_unavailable", 503)
	}
	instanceOutput, err := p.ec2.DescribeInstances(ctx, &ec2.DescribeInstancesInput{InstanceIds: []string{request.InstanceID}})
	if err != nil || instanceOutput.NextToken != nil || len(instanceOutput.Reservations) != 1 || len(instanceOutput.Reservations[0].Instances) != 1 {
		return contract.ServiceRestorePlan{}, api.NewError("service_restore_readback_invalid", 502)
	}
	instance := instanceOutput.Reservations[0].Instances[0]
	if aws.ToString(instance.InstanceId) != request.InstanceID || instance.Placement == nil || !contract.ValidAvailabilityZone(request.Region, aws.ToString(instance.Placement.AvailabilityZone)) || instance.State == nil || instance.State.Name == types.InstanceStateNameTerminated || len(instance.BlockDeviceMappings) != len(request.SnapshotRefs) {
		return contract.ServiceRestorePlan{}, api.NewError("service_restore_readback_invalid", 502)
	}
	refByVolume := make(map[string]contract.ServiceRestoreSnapshotRef, len(request.SnapshotRefs))
	volumeIDs, snapshotIDs := make([]string, 0, len(request.SnapshotRefs)), make([]string, 0, len(request.SnapshotRefs))
	for _, ref := range request.SnapshotRefs {
		refByVolume[ref.OriginalVolumeID] = ref
		volumeIDs, snapshotIDs = append(volumeIDs, ref.OriginalVolumeID), append(snapshotIDs, ref.SnapshotID)
	}
	deviceByVolume := make(map[string]struct {
		name                string
		deleteOnTermination bool
	}, len(instance.BlockDeviceMappings))
	for _, mapping := range instance.BlockDeviceMappings {
		if mapping.Ebs == nil || refByVolume[aws.ToString(mapping.Ebs.VolumeId)].OriginalVolumeID == "" || aws.ToString(mapping.DeviceName) == "" {
			return contract.ServiceRestorePlan{}, api.NewError("service_restore_readback_invalid", 502)
		}
		deviceByVolume[aws.ToString(mapping.Ebs.VolumeId)] = struct {
			name                string
			deleteOnTermination bool
		}{aws.ToString(mapping.DeviceName), aws.ToBool(mapping.Ebs.DeleteOnTermination)}
	}
	volumesOutput, err := p.ec2.DescribeVolumes(ctx, &ec2.DescribeVolumesInput{VolumeIds: volumeIDs})
	if err != nil || volumesOutput.NextToken != nil || len(volumesOutput.Volumes) != len(volumeIDs) {
		return contract.ServiceRestorePlan{}, api.NewError("service_restore_readback_invalid", 502)
	}
	snapshotsOutput, err := p.ec2.DescribeSnapshots(ctx, &ec2.DescribeSnapshotsInput{SnapshotIds: snapshotIDs})
	if err != nil || snapshotsOutput.NextToken != nil || len(snapshotsOutput.Snapshots) != len(snapshotIDs) {
		return contract.ServiceRestorePlan{}, api.NewError("service_restore_readback_invalid", 502)
	}
	imagesOutput, err := p.ec2.DescribeImages(ctx, &ec2.DescribeImagesInput{ImageIds: []string{request.ImageID}})
	if err != nil || imagesOutput.NextToken != nil || len(imagesOutput.Images) != 1 || imagesOutput.Images[0].State != types.ImageStateAvailable {
		return contract.ServiceRestorePlan{}, api.NewError("service_restore_readback_invalid", 502)
	}
	imageDevices := map[string]string{}
	for _, mapping := range imagesOutput.Images[0].BlockDeviceMappings {
		if mapping.Ebs != nil {
			imageDevices[aws.ToString(mapping.Ebs.SnapshotId)] = aws.ToString(mapping.DeviceName)
		}
	}
	snapshotByID := make(map[string]types.Snapshot, len(snapshotsOutput.Snapshots))
	for _, snapshot := range snapshotsOutput.Snapshots {
		snapshotByID[aws.ToString(snapshot.SnapshotId)] = snapshot
	}
	plan := contract.ServiceRestorePlan{Schema: contract.ServiceRestorePlanSchema, RestorePlanID: request.RestorePlanID, ConnectionID: command.ConnectionID, CommandID: command.CommandID, ServiceID: request.ServiceID, DeploymentID: request.DeploymentID, BackupID: request.BackupID, InstanceID: request.InstanceID, Region: request.Region, AvailabilityZone: aws.ToString(instance.Placement.AvailabilityZone), RestoreMode: "in_place", DowntimeRequired: true, OriginalVolumeRetention: "manual", FailurePolicy: "reattach_original", Currency: "USD", Unincluded: []string{"data_transfer", "existing_instance_and_volumes", "provisioned_iops", "provisioned_throughput", "taxes"}}
	plan.RequestSHA256, _ = command.RequestSHA256()
	quotedAt := now.UTC().Truncate(time.Millisecond)
	plan.QuotedAt, plan.ValidUntil = contract.CanonicalInstant(quotedAt), contract.CanonicalInstant(quotedAt.Add(contract.ServiceRestorePlanValidity))
	plan.QuoteID = "quote-restore-" + plan.RequestSHA256[:24]
	monthly := new(big.Rat)
	for _, volume := range volumesOutput.Volumes {
		volumeID := aws.ToString(volume.VolumeId)
		ref, ok := refByVolume[volumeID]
		device, deviceOK := deviceByVolume[volumeID]
		snapshot, snapshotOK := snapshotByID[ref.SnapshotID]
		if !ok || !deviceOK || !snapshotOK || aws.ToString(volume.AvailabilityZone) != plan.AvailabilityZone || !aws.ToBool(volume.Encrypted) || volume.State != types.VolumeStateInUse || len(volume.Attachments) != 1 || aws.ToString(volume.Attachments[0].InstanceId) != request.InstanceID || aws.ToString(volume.Attachments[0].Device) != device.name || snapshot.State != types.SnapshotStateCompleted || !aws.ToBool(snapshot.Encrypted) || aws.ToString(snapshot.VolumeId) != volumeID || imageDevices[ref.SnapshotID] != device.name || volume.Size == nil || *volume.Size <= 0 {
			return contract.ServiceRestorePlan{}, api.NewError("service_restore_readback_invalid", 502)
		}
		volumeType := string(volume.VolumeType)
		price, priceErr := p.storagePrice(ctx, request.Region, volumeType)
		if priceErr != nil {
			return contract.ServiceRestorePlan{}, priceErr
		}
		monthly.Add(monthly, new(big.Rat).Mul(price, big.NewRat(int64(*volume.Size), 1)))
		plan.VolumeSwaps = append(plan.VolumeSwaps, contract.ServiceRestoreVolumeSwap{OriginalVolumeID: volumeID, SnapshotID: ref.SnapshotID, DeviceName: device.name, VolumeType: volumeType, SizeGiB: int64(*volume.Size), IOPS: int64(aws.ToInt32(volume.Iops)), ThroughputMiB: int64(aws.ToInt32(volume.Throughput)), Encrypted: true, DeleteOnTermination: device.deleteOnTermination})
	}
	sort.Slice(plan.VolumeSwaps, func(i, j int) bool {
		return plan.VolumeSwaps[i].OriginalVolumeID < plan.VolumeSwaps[j].OriginalVolumeID
	})
	plan.EstimatedThirtyDayMinor, err = ceilPositiveRat(new(big.Rat).Mul(monthly, big.NewRat(100, 1)))
	if err != nil {
		return contract.ServiceRestorePlan{}, err
	}
	plan.EstimatedHourlyMinor, err = ceilPositiveRat(new(big.Rat).Quo(new(big.Rat).Mul(monthly, big.NewRat(100, 1)), big.NewRat(hoursPerThirtyDays, 1)))
	if err != nil {
		return contract.ServiceRestorePlan{}, err
	}
	return plan, nil
}

func (p *ServiceRestorePlanProvider) storagePrice(ctx context.Context, region, volumeType string) (*big.Rat, error) {
	output, err := p.pricing.GetProducts(ctx, &pricing.GetProductsInput{ServiceCode: aws.String("AmazonEC2"), FormatVersion: aws.String("aws_v1"), MaxResults: aws.Int32(100), Filters: []pricingtypes.Filter{pricingFilter("regionCode", region), pricingFilter("volumeApiName", volumeType), pricingFilter("productFamily", "Storage")}})
	if err != nil || output.NextToken != nil || len(output.PriceList) == 0 {
		return nil, api.NewError("service_restore_price_ambiguous", 502)
	}
	prices := []string{}
	for _, raw := range output.PriceList {
		var document priceDocument
		if json.Unmarshal([]byte(raw), &document) != nil || document.Product.Attributes.RegionCode != region || document.Product.Attributes.VolumeAPIName != volumeType || document.Product.ProductFamily != "Storage" {
			return nil, api.NewError("service_restore_price_ambiguous", 502)
		}
		for _, term := range document.Terms.OnDemand {
			for _, dimension := range term.PriceDimensions {
				if dimension.Unit == "GB-Mo" && dimension.BeginRange == "0" && dimension.EndRange == "Inf" && dimension.PricePerUnit["USD"] != "" {
					prices = append(prices, dimension.PricePerUnit["USD"])
				}
			}
		}
	}
	if len(prices) != 1 {
		return nil, api.NewError("service_restore_price_ambiguous", 502)
	}
	price, ok := new(big.Rat).SetString(prices[0])
	if !ok || price.Sign() <= 0 {
		return nil, api.NewError("service_restore_price_ambiguous", 502)
	}
	return price, nil
}

func ceilPositiveRat(value *big.Rat) (int64, error) {
	quotient, remainder := new(big.Int), new(big.Int)
	quotient.QuoRem(value.Num(), value.Denom(), remainder)
	if remainder.Sign() > 0 {
		quotient.Add(quotient, big.NewInt(1))
	}
	if value.Sign() <= 0 || !quotient.IsInt64() || quotient.Int64() > 9007199254740991 {
		return 0, api.NewError("service_restore_price_ambiguous", 502)
	}
	return quotient.Int64(), nil
}
