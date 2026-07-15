package provider

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"sort"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"

	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/api"
	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/contract"
)

const restorePhaseFallback = "fallback"

type EC2ServiceRestoreAPI interface {
	CreateVolume(context.Context, *ec2.CreateVolumeInput, ...func(*ec2.Options)) (*ec2.CreateVolumeOutput, error)
	DescribeVolumes(context.Context, *ec2.DescribeVolumesInput, ...func(*ec2.Options)) (*ec2.DescribeVolumesOutput, error)
	DescribeInstances(context.Context, *ec2.DescribeInstancesInput, ...func(*ec2.Options)) (*ec2.DescribeInstancesOutput, error)
	StopInstances(context.Context, *ec2.StopInstancesInput, ...func(*ec2.Options)) (*ec2.StopInstancesOutput, error)
	StartInstances(context.Context, *ec2.StartInstancesInput, ...func(*ec2.Options)) (*ec2.StartInstancesOutput, error)
	DetachVolume(context.Context, *ec2.DetachVolumeInput, ...func(*ec2.Options)) (*ec2.DetachVolumeOutput, error)
	AttachVolume(context.Context, *ec2.AttachVolumeInput, ...func(*ec2.Options)) (*ec2.AttachVolumeOutput, error)
	ModifyInstanceAttribute(context.Context, *ec2.ModifyInstanceAttributeInput, ...func(*ec2.Options)) (*ec2.ModifyInstanceAttributeOutput, error)
	CreateTags(context.Context, *ec2.CreateTagsInput, ...func(*ec2.Options)) (*ec2.CreateTagsOutput, error)
}

type EC2ServiceRestoreProvider struct{ client EC2ServiceRestoreAPI }

func NewEC2ServiceRestoreProvider(client EC2ServiceRestoreAPI) (*EC2ServiceRestoreProvider, error) {
	if client == nil {
		return nil, api.NewError("service_restore_provider_unavailable", 503)
	}
	return &EC2ServiceRestoreProvider{client: client}, nil
}

// EnsureRestore converges an approval-bound in-place restore. Every retry
// reconstructs state from AWS. Replacement volumes are fenced by deterministic
// ClientTokens and tags; a persisted fallback tag makes a failed swap converge
// only toward reattaching the retained originals.
func (p *EC2ServiceRestoreProvider) EnsureRestore(ctx context.Context, spec api.ServiceRestoreSpec) (contract.ServiceRestoreAWSEvidence, bool, error) {
	if !validServiceRestoreSpec(spec) {
		return contract.ServiceRestoreAWSEvidence{}, false, api.NewError("service_restore_spec_invalid", 500)
	}
	replacements, complete, err := p.ensureReplacementVolumes(ctx, spec)
	if err != nil || !complete {
		return contract.ServiceRestoreAWSEvidence{}, false, err
	}
	instance, err := p.describeRestoreInstance(ctx, spec)
	if err != nil {
		return contract.ServiceRestoreAWSEvidence{}, false, err
	}
	if restoreFallbackRequested(replacements) {
		return p.ensureOriginalsReattached(ctx, spec, instance, replacements)
	}
	if restoreMappingsMatch(instance, spec, replacements, false) && restoreInstanceState(instance) == "running" {
		return restoreEvidence(spec, instance, replacements, "restored", false), true, nil
	}
	if restoreInstanceState(instance) != "stopped" {
		if restoreInstanceState(instance) == "running" {
			if _, err = p.client.StopInstances(ctx, &ec2.StopInstancesInput{InstanceIds: []string{spec.InstanceID}}); err != nil {
				if awsTransitionPending(err, "IncorrectInstanceState") {
					return contract.ServiceRestoreAWSEvidence{}, false, nil
				}
				return contract.ServiceRestoreAWSEvidence{}, false, api.NewError("service_restore_provider_unavailable", 503)
			}
		}
		return contract.ServiceRestoreAWSEvidence{}, false, nil
	}
	if restoreHasOriginalAttachment(instance, spec) {
		for _, swap := range spec.VolumeSwaps {
			if restoreMappingVolume(instance, swap.DeviceName) != swap.OriginalVolumeID {
				continue
			}
			if _, err = p.client.DetachVolume(ctx, &ec2.DetachVolumeInput{Device: aws.String(swap.DeviceName), InstanceId: aws.String(spec.InstanceID), VolumeId: aws.String(swap.OriginalVolumeID)}); err != nil {
				if awsTransitionPending(err, "IncorrectState", "InvalidAttachment.NotFound") {
					return contract.ServiceRestoreAWSEvidence{}, false, nil
				}
				return p.beginRestoreFallback(ctx, spec, replacements)
			}
		}
		return contract.ServiceRestoreAWSEvidence{}, false, nil
	}
	for index, replacement := range replacements {
		swap := spec.VolumeSwaps[index]
		if restoreMappingVolume(instance, swap.DeviceName) == replacement.id {
			continue
		}
		if _, err = p.client.AttachVolume(ctx, &ec2.AttachVolumeInput{Device: aws.String(swap.DeviceName), InstanceId: aws.String(spec.InstanceID), VolumeId: aws.String(replacement.id)}); err != nil {
			if awsTransitionPending(err, "IncorrectState", "VolumeInUse") {
				return contract.ServiceRestoreAWSEvidence{}, false, nil
			}
			return p.beginRestoreFallback(ctx, spec, replacements)
		}
		return contract.ServiceRestoreAWSEvidence{}, false, nil
	}
	for index, replacement := range replacements {
		swap := spec.VolumeSwaps[index]
		if _, err = p.client.ModifyInstanceAttribute(ctx, &ec2.ModifyInstanceAttributeInput{
			InstanceId: aws.String(spec.InstanceID),
			BlockDeviceMappings: []types.InstanceBlockDeviceMappingSpecification{{
				DeviceName: aws.String(swap.DeviceName),
				Ebs: &types.EbsInstanceBlockDeviceSpecification{
					DeleteOnTermination: aws.Bool(swap.DeleteOnTermination),
					VolumeId:            aws.String(replacement.id),
				},
			}},
		}); err != nil {
			if awsTransitionPending(err, "IncorrectInstanceState") {
				return contract.ServiceRestoreAWSEvidence{}, false, nil
			}
			return p.beginRestoreFallback(ctx, spec, replacements)
		}
	}
	if _, err = p.client.StartInstances(ctx, &ec2.StartInstancesInput{InstanceIds: []string{spec.InstanceID}}); err != nil {
		if awsTransitionPending(err, "IncorrectInstanceState") {
			return contract.ServiceRestoreAWSEvidence{}, false, nil
		}
		return p.beginRestoreFallback(ctx, spec, replacements)
	}
	return contract.ServiceRestoreAWSEvidence{}, false, nil
}

type restoreVolume struct {
	id                  string
	snapshotID          string
	originalVolumeID    string
	deviceName          string
	state               string
	encrypted           bool
	availabilityZone    string
	volumeType          string
	sizeGiB             int64
	iops                int64
	throughputMiB       int64
	deleteOnTermination bool
	phase               string
	attachments         []types.VolumeAttachment
}

func (p *EC2ServiceRestoreProvider) ensureReplacementVolumes(ctx context.Context, spec api.ServiceRestoreSpec) ([]restoreVolume, bool, error) {
	replacements, err := p.describeRestoreVolumes(ctx, spec)
	if err != nil {
		return nil, false, err
	}
	byOriginal := make(map[string]restoreVolume, len(replacements))
	for _, volume := range replacements {
		if _, duplicate := byOriginal[volume.originalVolumeID]; duplicate {
			return nil, false, api.NewError("provider_readback_invalid", 502)
		}
		byOriginal[volume.originalVolumeID] = volume
	}
	if len(replacements) > len(spec.VolumeSwaps) {
		return nil, false, api.NewError("provider_readback_invalid", 502)
	}
	for _, swap := range spec.VolumeSwaps {
		if _, found := byOriginal[swap.OriginalVolumeID]; found {
			continue
		}
		input := &ec2.CreateVolumeInput{
			AvailabilityZone: aws.String(spec.AvailabilityZone),
			ClientToken:      aws.String(restoreVolumeClientToken(spec, swap)),
			Encrypted:        aws.Bool(true),
			Iops:             aws.Int32(int32(swap.IOPS)),
			Size:             aws.Int32(int32(swap.SizeGiB)),
			SnapshotId:       aws.String(swap.SnapshotID),
			Throughput:       aws.Int32(int32(swap.ThroughputMiB)),
			VolumeType:       types.VolumeType(swap.VolumeType),
			TagSpecifications: []types.TagSpecification{{
				ResourceType: types.ResourceTypeVolume,
				Tags:         serviceRestoreTags(spec, swap),
			}},
		}
		if _, err = p.client.CreateVolume(ctx, input); err != nil {
			return nil, false, api.NewError("service_restore_provider_unavailable", 503)
		}
		return nil, false, nil
	}
	ordered := make([]restoreVolume, 0, len(spec.VolumeSwaps))
	available := true
	for _, swap := range spec.VolumeSwaps {
		volume := byOriginal[swap.OriginalVolumeID]
		if volume.snapshotID != swap.SnapshotID || volume.deviceName != swap.DeviceName || !volume.encrypted || volume.availabilityZone != spec.AvailabilityZone || volume.volumeType != swap.VolumeType || volume.sizeGiB != swap.SizeGiB || volume.iops != swap.IOPS || volume.throughputMiB != swap.ThroughputMiB {
			return nil, false, api.NewError("provider_readback_invalid", 502)
		}
		if volume.state == "error" {
			return nil, false, api.NewError("service_restore_failed", 502)
		}
		if volume.state != "available" && volume.state != "in-use" {
			available = false
		}
		volume.deleteOnTermination = swap.DeleteOnTermination
		ordered = append(ordered, volume)
	}
	return ordered, available, nil
}

func (p *EC2ServiceRestoreProvider) describeRestoreVolumes(ctx context.Context, spec api.ServiceRestoreSpec) ([]restoreVolume, error) {
	output, err := p.client.DescribeVolumes(ctx, &ec2.DescribeVolumesInput{Filters: []types.Filter{
		{Name: aws.String("tag:DirextalkConnectionId"), Values: []string{spec.ConnectionID}},
		{Name: aws.String("tag:DirextalkRestoreId"), Values: []string{spec.RestoreID}},
	}})
	if err != nil {
		return nil, api.NewError("service_restore_provider_unavailable", 503)
	}
	result := make([]restoreVolume, 0, len(output.Volumes))
	for _, volume := range output.Volumes {
		tags := restoreTagValues(volume.Tags)
		id := aws.ToString(volume.VolumeId)
		if id == "" || tags["DirextalkServiceId"] != spec.ServiceID || tags["DirextalkDeploymentId"] != spec.DeploymentID || tags["DirextalkBackupId"] != spec.BackupID {
			return nil, api.NewError("provider_readback_invalid", 502)
		}
		if tags["dirextalk:managed"] != "true" || tags["dirextalk:connection-id"] != spec.ConnectionID || tags["dirextalk:deployment-id"] != spec.DeploymentID || tags["DirextalkRetention"] != "manual" {
			return nil, api.NewError("provider_readback_invalid", 502)
		}
		result = append(result, restoreVolume{id: id, snapshotID: aws.ToString(volume.SnapshotId), originalVolumeID: tags["DirextalkOriginalVolumeId"], deviceName: tags["DirextalkDeviceName"], state: string(volume.State), encrypted: aws.ToBool(volume.Encrypted), availabilityZone: aws.ToString(volume.AvailabilityZone), volumeType: string(volume.VolumeType), sizeGiB: int64(aws.ToInt32(volume.Size)), iops: int64(aws.ToInt32(volume.Iops)), throughputMiB: int64(aws.ToInt32(volume.Throughput)), phase: tags["DirextalkRestorePhase"], attachments: volume.Attachments})
	}
	return result, nil
}

func (p *EC2ServiceRestoreProvider) describeRestoreInstance(ctx context.Context, spec api.ServiceRestoreSpec) (types.Instance, error) {
	output, err := p.client.DescribeInstances(ctx, &ec2.DescribeInstancesInput{InstanceIds: []string{spec.InstanceID}})
	if err != nil || len(output.Reservations) != 1 || len(output.Reservations[0].Instances) != 1 {
		return types.Instance{}, api.NewError("service_restore_provider_unavailable", 503)
	}
	instance := output.Reservations[0].Instances[0]
	if aws.ToString(instance.InstanceId) != spec.InstanceID || instance.State == nil || instance.Placement == nil || aws.ToString(instance.Placement.AvailabilityZone) != spec.AvailabilityZone {
		return types.Instance{}, api.NewError("provider_readback_invalid", 502)
	}
	tags := restoreTagValues(instance.Tags)
	if tags["dirextalk:managed"] != "true" || tags["dirextalk:connection-id"] != spec.ConnectionID || tags["dirextalk:deployment-id"] != spec.DeploymentID {
		return types.Instance{}, api.NewError("provider_readback_invalid", 502)
	}
	return instance, nil
}

func (p *EC2ServiceRestoreProvider) beginRestoreFallback(ctx context.Context, spec api.ServiceRestoreSpec, replacements []restoreVolume) (contract.ServiceRestoreAWSEvidence, bool, error) {
	ids := make([]string, 0, len(replacements))
	for _, replacement := range replacements {
		ids = append(ids, replacement.id)
	}
	_, tagErr := p.client.CreateTags(ctx, &ec2.CreateTagsInput{Resources: ids, Tags: []types.Tag{{Key: aws.String("DirextalkRestorePhase"), Value: aws.String(restorePhaseFallback)}}})
	instance, readErr := p.describeRestoreInstance(ctx, spec)
	if tagErr != nil || readErr != nil {
		p.bestEffortEmergencyFallback(ctx, spec, instance, replacements)
		return restoreEvidence(spec, instance, replacements, "restore_blocked", false), true, nil
	}
	for index := range replacements {
		replacements[index].phase = restorePhaseFallback
	}
	return p.ensureOriginalsReattached(ctx, spec, instance, replacements)
}

func (p *EC2ServiceRestoreProvider) ensureOriginalsReattached(ctx context.Context, spec api.ServiceRestoreSpec, instance types.Instance, replacements []restoreVolume) (contract.ServiceRestoreAWSEvidence, bool, error) {
	state := restoreInstanceState(instance)
	if state == "running" && restoreMappingsMatch(instance, spec, replacements, true) {
		return restoreEvidence(spec, instance, replacements, "original_restored", true), true, nil
	}
	if state != "stopped" {
		if state == "running" {
			if _, err := p.client.StopInstances(ctx, &ec2.StopInstancesInput{InstanceIds: []string{spec.InstanceID}}); err != nil {
				if awsTransitionPending(err, "IncorrectInstanceState") {
					return contract.ServiceRestoreAWSEvidence{}, false, nil
				}
				return restoreEvidence(spec, instance, replacements, "restore_blocked", false), true, nil
			}
		}
		return contract.ServiceRestoreAWSEvidence{}, false, nil
	}
	for index, replacement := range replacements {
		swap := spec.VolumeSwaps[index]
		if restoreMappingVolume(instance, swap.DeviceName) != replacement.id {
			continue
		}
		if _, err := p.client.DetachVolume(ctx, &ec2.DetachVolumeInput{Device: aws.String(swap.DeviceName), InstanceId: aws.String(spec.InstanceID), VolumeId: aws.String(replacement.id)}); err != nil {
			if awsTransitionPending(err, "IncorrectState", "InvalidAttachment.NotFound") {
				return contract.ServiceRestoreAWSEvidence{}, false, nil
			}
			return restoreEvidence(spec, instance, replacements, "restore_blocked", false), true, nil
		}
		return contract.ServiceRestoreAWSEvidence{}, false, nil
	}
	for _, swap := range spec.VolumeSwaps {
		if restoreMappingVolume(instance, swap.DeviceName) == swap.OriginalVolumeID {
			continue
		}
		if _, err := p.client.AttachVolume(ctx, &ec2.AttachVolumeInput{Device: aws.String(swap.DeviceName), InstanceId: aws.String(spec.InstanceID), VolumeId: aws.String(swap.OriginalVolumeID)}); err != nil {
			if awsTransitionPending(err, "IncorrectState", "VolumeInUse") {
				return contract.ServiceRestoreAWSEvidence{}, false, nil
			}
			return restoreEvidence(spec, instance, replacements, "restore_blocked", false), true, nil
		}
		return contract.ServiceRestoreAWSEvidence{}, false, nil
	}
	for _, swap := range spec.VolumeSwaps {
		if _, err := p.client.ModifyInstanceAttribute(ctx, &ec2.ModifyInstanceAttributeInput{InstanceId: aws.String(spec.InstanceID), BlockDeviceMappings: []types.InstanceBlockDeviceMappingSpecification{{DeviceName: aws.String(swap.DeviceName), Ebs: &types.EbsInstanceBlockDeviceSpecification{DeleteOnTermination: aws.Bool(swap.DeleteOnTermination), VolumeId: aws.String(swap.OriginalVolumeID)}}}}); err != nil {
			if awsTransitionPending(err, "IncorrectInstanceState") {
				return contract.ServiceRestoreAWSEvidence{}, false, nil
			}
			return restoreEvidence(spec, instance, replacements, "restore_blocked", false), true, nil
		}
	}
	if _, err := p.client.StartInstances(ctx, &ec2.StartInstancesInput{InstanceIds: []string{spec.InstanceID}}); err != nil {
		if awsTransitionPending(err, "IncorrectInstanceState") {
			return contract.ServiceRestoreAWSEvidence{}, false, nil
		}
		return restoreEvidence(spec, instance, replacements, "restore_blocked", false), true, nil
	}
	return contract.ServiceRestoreAWSEvidence{}, false, nil
}

func (p *EC2ServiceRestoreProvider) bestEffortEmergencyFallback(ctx context.Context, spec api.ServiceRestoreSpec, instance types.Instance, replacements []restoreVolume) {
	if restoreInstanceState(instance) == "running" {
		_, _ = p.client.StopInstances(ctx, &ec2.StopInstancesInput{InstanceIds: []string{spec.InstanceID}})
		return
	}
	if restoreInstanceState(instance) != "stopped" {
		return
	}
	for index, replacement := range replacements {
		swap := spec.VolumeSwaps[index]
		if restoreMappingVolume(instance, swap.DeviceName) == replacement.id {
			_, _ = p.client.DetachVolume(ctx, &ec2.DetachVolumeInput{Device: aws.String(swap.DeviceName), InstanceId: aws.String(spec.InstanceID), VolumeId: aws.String(replacement.id)})
		}
	}
	for _, swap := range spec.VolumeSwaps {
		if restoreMappingVolume(instance, swap.DeviceName) == "" {
			_, _ = p.client.AttachVolume(ctx, &ec2.AttachVolumeInput{Device: aws.String(swap.DeviceName), InstanceId: aws.String(spec.InstanceID), VolumeId: aws.String(swap.OriginalVolumeID)})
		}
	}
}

func restoreEvidence(spec api.ServiceRestoreSpec, instance types.Instance, replacements []restoreVolume, outcome string, fallbackVerified bool) contract.ServiceRestoreAWSEvidence {
	evidence := contract.ServiceRestoreAWSEvidence{RestoreID: spec.RestoreID, ServiceID: spec.ServiceID, DeploymentID: spec.DeploymentID, BackupID: spec.BackupID, InstanceID: spec.InstanceID, Region: spec.Region, AvailabilityZone: spec.AvailabilityZone, Outcome: outcome, InstanceState: restoreInstanceState(instance), FallbackVerified: fallbackVerified}
	for index, replacement := range replacements {
		state := "unknown"
		if restoreMappingVolume(instance, spec.VolumeSwaps[index].DeviceName) == replacement.id {
			state = "attached_current"
		} else if replacement.id != "" {
			state = "retained_detached"
		}
		evidence.Replacements = append(evidence.Replacements, contract.ServiceRestoreReplacementVolume{OriginalVolumeID: spec.VolumeSwaps[index].OriginalVolumeID, ReplacementVolumeID: replacement.id, SnapshotID: spec.VolumeSwaps[index].SnapshotID, DeviceName: spec.VolumeSwaps[index].DeviceName, State: state, Encrypted: replacement.encrypted, DeleteOnTermination: spec.VolumeSwaps[index].DeleteOnTermination})
	}
	return evidence
}

func restoreFallbackRequested(volumes []restoreVolume) bool {
	for _, volume := range volumes {
		if volume.phase == restorePhaseFallback {
			return true
		}
	}
	return false
}

func restoreInstanceState(instance types.Instance) string {
	if instance.State == nil {
		return "unknown"
	}
	return string(instance.State.Name)
}

func restoreMappingVolume(instance types.Instance, deviceName string) string {
	for _, mapping := range instance.BlockDeviceMappings {
		if aws.ToString(mapping.DeviceName) == deviceName && mapping.Ebs != nil {
			return aws.ToString(mapping.Ebs.VolumeId)
		}
	}
	return ""
}

func restoreMappingDeleteOnTermination(instance types.Instance, deviceName string) bool {
	for _, mapping := range instance.BlockDeviceMappings {
		if aws.ToString(mapping.DeviceName) == deviceName && mapping.Ebs != nil {
			return aws.ToBool(mapping.Ebs.DeleteOnTermination)
		}
	}
	return false
}

func restoreHasOriginalAttachment(instance types.Instance, spec api.ServiceRestoreSpec) bool {
	for _, swap := range spec.VolumeSwaps {
		if restoreMappingVolume(instance, swap.DeviceName) == swap.OriginalVolumeID {
			return true
		}
	}
	return false
}

func restoreMappingsMatch(instance types.Instance, spec api.ServiceRestoreSpec, replacements []restoreVolume, originals bool) bool {
	for index, swap := range spec.VolumeSwaps {
		expected := replacements[index].id
		if originals {
			expected = swap.OriginalVolumeID
		}
		if restoreMappingVolume(instance, swap.DeviceName) != expected || restoreMappingDeleteOnTermination(instance, swap.DeviceName) != swap.DeleteOnTermination {
			return false
		}
	}
	return true
}

func validServiceRestoreSpec(spec api.ServiceRestoreSpec) bool {
	if !contract.ValidConnectionID(spec.ConnectionID) || !contract.ValidID(spec.RestoreID) || !contract.ValidID(spec.ServiceID) || !contract.ValidID(spec.DeploymentID) || !contract.ValidID(spec.BackupID) || spec.InstanceID == "" || spec.Region == "" || !contract.ValidAvailabilityZone(spec.Region, spec.AvailabilityZone) || len(spec.VolumeSwaps) == 0 {
		return false
	}
	for _, swap := range spec.VolumeSwaps {
		if swap.SizeGiB < 1 || swap.SizeGiB > 16384 || swap.IOPS < 0 || swap.IOPS > 256000 || swap.ThroughputMiB < 0 || swap.ThroughputMiB > 4000 {
			return false
		}
	}
	return true
}

func restoreVolumeClientToken(spec api.ServiceRestoreSpec, swap contract.ServiceRestoreVolumeSwap) string {
	sum := sha256.Sum256([]byte(spec.ConnectionID + "\x00" + spec.RestoreID + "\x00" + swap.OriginalVolumeID + "\x00" + swap.SnapshotID))
	return hex.EncodeToString(sum[:])
}

func serviceRestoreTags(spec api.ServiceRestoreSpec, swap contract.ServiceRestoreVolumeSwap) []types.Tag {
	tags := []types.Tag{
		{Key: aws.String("dirextalk:managed"), Value: aws.String("true")},
		{Key: aws.String("dirextalk:connection-id"), Value: aws.String(spec.ConnectionID)},
		{Key: aws.String("dirextalk:deployment-id"), Value: aws.String(spec.DeploymentID)},
		{Key: aws.String("DirextalkConnectionId"), Value: aws.String(spec.ConnectionID)},
		{Key: aws.String("DirextalkRestoreId"), Value: aws.String(spec.RestoreID)},
		{Key: aws.String("DirextalkServiceId"), Value: aws.String(spec.ServiceID)},
		{Key: aws.String("DirextalkDeploymentId"), Value: aws.String(spec.DeploymentID)},
		{Key: aws.String("DirextalkBackupId"), Value: aws.String(spec.BackupID)},
		{Key: aws.String("DirextalkOriginalVolumeId"), Value: aws.String(swap.OriginalVolumeID)},
		{Key: aws.String("DirextalkDeviceName"), Value: aws.String(swap.DeviceName)},
		{Key: aws.String("DirextalkRetention"), Value: aws.String("manual")},
	}
	sort.Slice(tags, func(i, j int) bool { return aws.ToString(tags[i].Key) < aws.ToString(tags[j].Key) })
	return tags
}

func restoreTagValues(tags []types.Tag) map[string]string {
	values := make(map[string]string, len(tags))
	for _, tag := range tags {
		values[aws.ToString(tag.Key)] = aws.ToString(tag.Value)
	}
	return values
}
