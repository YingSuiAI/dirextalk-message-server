package stackteardown

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"
)

// State describes independently observed cleanup progress. In particular,
// pending_key_deletion is not reported as verified_destroyed: AWS retains a
// scheduled KMS key for its mandatory waiting period.
type State string

const (
	StatePlanned            State = "planned"
	StateDestroying         State = "destroying"
	StateVerifiedDestroyed  State = "verified_destroyed"
	StatePendingKeyDeletion State = "pending_key_deletion"
	StateBlocked            State = "blocked"
)

type ItemReport struct {
	LogicalID  string       `json:"logical_id"`
	Kind       ResourceKind `json:"kind"`
	Identifier string       `json:"identifier"`
	State      State        `json:"state"`
}

type Report struct {
	PlanDigest string       `json:"plan_digest"`
	State      State        `json:"state"`
	Items      []ItemReport `json:"items"`
}

type StackObservation struct {
	ID     string
	Name   string
	Status string
}

type StackResourceObservation struct {
	LogicalID  string
	Type       string
	Identifier string
}

type TableObservation struct {
	Status                    string
	DeletionProtectionEnabled bool
}

type TaggedObservation struct {
	ID   string
	Tags map[string]string
}

type KeyObservation struct{ State string }

// teardownProvider is intentionally closed. The only resource identity a
// caller supplies is the Connection ID in Request; every other ID is read
// from CloudFormation or a connection-scoped EC2 tag query.
type teardownProvider interface {
	FindStack(context.Context, string) (StackObservation, bool, error)
	StackResources(context.Context, string) ([]StackResourceObservation, error)
	DeleteStack(context.Context, string) error

	Table(context.Context, string) (TableObservation, bool, error)
	DisableTableDeletionProtection(context.Context, string) error
	DeleteTable(context.Context, string) error

	BucketExists(context.Context, string) (bool, error)
	EmptyBucket(context.Context, string) error
	DeleteBucket(context.Context, string) error

	ManagedImages(context.Context, string) ([]TaggedObservation, error)
	ManagedSnapshots(context.Context, string) ([]TaggedObservation, error)
	Image(context.Context, string) (TaggedObservation, bool, error)
	DeregisterImage(context.Context, string) error
	Snapshot(context.Context, string) (TaggedObservation, bool, error)
	DeleteSnapshot(context.Context, string) error

	Key(context.Context, string) (KeyObservation, bool, error)
	AliasExists(context.Context, string, string) (bool, error)
	DeleteAlias(context.Context, string) error
	ScheduleKeyDeletion(context.Context, string, int32) error
}

type Service struct{ provider teardownProvider }

func newService(provider teardownProvider) *Service { return &Service{provider: provider} }

// BuildPlan discovers the exact resources retained by this reviewed template.
// It fails closed if the stack has a new retained DynamoDB/S3/KMS resource the
// binary does not understand, rather than silently leaving it billable.
func (service *Service) BuildPlan(ctx context.Context, request Request) (Plan, error) {
	if service == nil || service.provider == nil || request.Validate() != nil {
		return Plan{}, ErrInvalidRequest
	}
	stackName := request.stackName()
	stack, found, err := service.provider.FindStack(ctx, stackName)
	if err != nil {
		return Plan{}, err
	}
	if !found {
		return Plan{}, ErrStackNotFound
	}
	if stack.Name != stackName || stack.ID == "" || strings.HasPrefix(stack.Status, "DELETE_") {
		return Plan{}, ErrPlanStale
	}
	resources, err := service.provider.StackResources(ctx, stack.ID)
	if err != nil {
		return Plan{}, err
	}
	retained, err := closedRetainedResources(request, stackName, resources)
	if err != nil {
		return Plan{}, err
	}
	images, err := service.provider.ManagedImages(ctx, request.ConnectionID)
	if err != nil {
		return Plan{}, err
	}
	imageIDs, err := managedIDs(images, request.ConnectionID, amiIDPattern)
	if err != nil {
		return Plan{}, err
	}
	snapshots, err := service.provider.ManagedSnapshots(ctx, request.ConnectionID)
	if err != nil {
		return Plan{}, err
	}
	snapshotIDs, err := managedIDs(snapshots, request.ConnectionID, snapshotIDPattern)
	if err != nil {
		return Plan{}, err
	}
	plan := Plan{
		Schema:                PlanSchema,
		ConnectionID:          request.ConnectionID,
		Region:                request.Region,
		StackName:             stackName,
		StackID:               stack.ID,
		RetainedResources:     sortedResourceRefs(retained),
		ManagedImageIDs:       sortedStrings(imageIDs),
		ManagedSnapshotIDs:    sortedStrings(snapshotIDs),
		KMSDeletionWindowDays: defaultKeyDeletionDays,
	}
	if plan.Validate() != nil {
		return Plan{}, ErrPlanStale
	}
	return plan, nil
}

// Execute requests CloudFormation deletion without RetainResources overrides,
// then advances each retained item only after independent read-back. Repeated
// invocations are expected while DynamoDB applies deletion-protection changes
// or KMS waits through its deletion window.
func (service *Service) Execute(ctx context.Context, plan Plan) (Report, error) {
	if service == nil || service.provider == nil || plan.Validate() != nil {
		return Report{}, ErrInvalidRequest
	}
	if err := service.verifyPlanProvenance(ctx, plan); err != nil {
		return Report{}, err
	}
	stack, found, err := service.provider.FindStack(ctx, plan.StackName)
	if err != nil {
		return Report{}, err
	}
	if found {
		if stack.ID != plan.StackID || stack.Name != plan.StackName {
			return Report{}, ErrPlanStale
		}
		if !strings.HasPrefix(stack.Status, "DELETE_") {
			if err := service.provider.DeleteStack(ctx, plan.StackID); err != nil {
				return Report{}, err
			}
		}
		_, found, err = service.provider.FindStack(ctx, plan.StackName)
		if err != nil {
			return Report{}, err
		}
		if found {
			return service.ReadBack(ctx, plan)
		}
	}
	if err := service.applyRetainedCleanup(ctx, plan); err != nil {
		return Report{}, err
	}
	return service.ReadBack(ctx, plan)
}

// ReadBack does not mutate. It returns verified_destroyed only once the stack,
// retained data resources, AMIs, and snapshots are absent. KMS keys remain
// explicitly pending_key_deletion until AWS removes them.
func (service *Service) ReadBack(ctx context.Context, plan Plan) (Report, error) {
	if service == nil || service.provider == nil || plan.Validate() != nil {
		return Report{}, ErrInvalidRequest
	}
	items := make([]ItemReport, 0, len(plan.RetainedResources)+1+len(plan.ManagedImageIDs)+len(plan.ManagedSnapshotIDs))
	stack, found, err := service.provider.FindStack(ctx, plan.StackName)
	if err != nil {
		return Report{}, err
	}
	stackState := StateVerifiedDestroyed
	if found {
		stackState = StateDestroying
		if stack.ID != plan.StackID || stack.Name != plan.StackName {
			stackState = StateBlocked
		}
	}
	items = append(items, ItemReport{LogicalID: "ConnectionStack", Kind: "cloudformation_stack", Identifier: plan.StackName, State: stackState})

	serviceKeyID := resourceIdentifier(plan, "ServiceSecretSealingKey")
	for _, resource := range plan.RetainedResources {
		state, err := service.readResource(ctx, resource, serviceKeyID)
		if err != nil {
			return Report{}, err
		}
		items = append(items, ItemReport{LogicalID: resource.LogicalID, Kind: resource.Kind, Identifier: resource.Identifier, State: state})
	}
	for _, imageID := range plan.ManagedImageIDs {
		_, present, err := service.provider.Image(ctx, imageID)
		if err != nil {
			return Report{}, err
		}
		state := StateVerifiedDestroyed
		if present {
			state = StateDestroying
		}
		items = append(items, ItemReport{LogicalID: "ManagedBackupAMI", Kind: "ec2_ami", Identifier: imageID, State: state})
	}
	for _, snapshotID := range plan.ManagedSnapshotIDs {
		_, present, err := service.provider.Snapshot(ctx, snapshotID)
		if err != nil {
			return Report{}, err
		}
		state := StateVerifiedDestroyed
		if present {
			state = StateDestroying
		}
		items = append(items, ItemReport{LogicalID: "ManagedBackupSnapshot", Kind: "ec2_snapshot", Identifier: snapshotID, State: state})
	}
	return Report{PlanDigest: planDigest(plan), State: reportState(items), Items: items}, nil
}

func (service *Service) verifyPlanProvenance(ctx context.Context, plan Plan) error {
	stack, stackPresent, err := service.provider.FindStack(ctx, plan.StackName)
	if err != nil {
		return err
	}
	if stackPresent && (stack.ID != plan.StackID || stack.Name != plan.StackName) {
		return ErrPlanStale
	}
	resources, err := service.provider.StackResources(ctx, plan.StackID)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrPlanProvenanceUnavailable, err)
	}
	observed, err := closedRetainedResources(plan.request(), plan.StackName, resources)
	if err != nil {
		return err
	}
	if !sameResourceRefs(sortedResourceRefs(observed), plan.RetainedResources) {
		return ErrPlanStale
	}
	images, err := service.provider.ManagedImages(ctx, plan.ConnectionID)
	if err != nil {
		return err
	}
	imageIDs, err := managedIDs(images, plan.ConnectionID, amiIDPattern)
	if err != nil || !(sameStrings(imageIDs, plan.ManagedImageIDs) || (!stackPresent && subsetStrings(imageIDs, plan.ManagedImageIDs))) {
		return ErrPlanStale
	}
	snapshots, err := service.provider.ManagedSnapshots(ctx, plan.ConnectionID)
	if err != nil {
		return err
	}
	snapshotIDs, err := managedIDs(snapshots, plan.ConnectionID, snapshotIDPattern)
	if err != nil || !(sameStrings(snapshotIDs, plan.ManagedSnapshotIDs) || (!stackPresent && subsetStrings(snapshotIDs, plan.ManagedSnapshotIDs))) {
		return ErrPlanStale
	}
	return nil
}

func (service *Service) applyRetainedCleanup(ctx context.Context, plan Plan) error {
	serviceKeyID := resourceIdentifier(plan, "ServiceSecretSealingKey")
	for _, resource := range plan.RetainedResources {
		if resource.Kind != ResourceKMSAlias {
			continue
		}
		present, err := service.provider.AliasExists(ctx, resource.Identifier, serviceKeyID)
		if err != nil {
			return err
		}
		if present {
			if err := service.provider.DeleteAlias(ctx, resource.Identifier); err != nil {
				return err
			}
		}
	}
	for _, resource := range plan.RetainedResources {
		switch resource.Kind {
		case ResourceDynamoDBTable:
			if err := service.ensureTableDeleted(ctx, resource.Identifier); err != nil {
				return err
			}
		case ResourceS3Bucket:
			if err := service.ensureBucketDeleted(ctx, resource.Identifier); err != nil {
				return err
			}
		}
	}
	for _, imageID := range plan.ManagedImageIDs {
		image, present, err := service.provider.Image(ctx, imageID)
		if err != nil {
			return err
		}
		if present {
			if !managedTagsMatch(image, plan.ConnectionID) {
				return ErrPlanStale
			}
			if err := service.provider.DeregisterImage(ctx, imageID); err != nil {
				return err
			}
		}
	}
	for _, snapshotID := range plan.ManagedSnapshotIDs {
		snapshot, present, err := service.provider.Snapshot(ctx, snapshotID)
		if err != nil {
			return err
		}
		if present {
			if !managedTagsMatch(snapshot, plan.ConnectionID) {
				return ErrPlanStale
			}
			if err := service.provider.DeleteSnapshot(ctx, snapshotID); err != nil {
				return err
			}
		}
	}
	for _, resource := range plan.RetainedResources {
		if resource.Kind != ResourceKMSKey {
			continue
		}
		key, present, err := service.provider.Key(ctx, resource.Identifier)
		if err != nil {
			return err
		}
		if present && key.State != "PendingDeletion" {
			if err := service.provider.ScheduleKeyDeletion(ctx, resource.Identifier, plan.KMSDeletionWindowDays); err != nil {
				return err
			}
		}
	}
	return nil
}

func (service *Service) ensureTableDeleted(ctx context.Context, identifier string) error {
	table, present, err := service.provider.Table(ctx, identifier)
	if err != nil || !present {
		return err
	}
	if table.DeletionProtectionEnabled {
		return service.provider.DisableTableDeletionProtection(ctx, identifier)
	}
	if table.Status != "ACTIVE" {
		return nil
	}
	return service.provider.DeleteTable(ctx, identifier)
}

func (service *Service) ensureBucketDeleted(ctx context.Context, identifier string) error {
	present, err := service.provider.BucketExists(ctx, identifier)
	if err != nil || !present {
		return err
	}
	if err := service.provider.EmptyBucket(ctx, identifier); err != nil {
		return err
	}
	return service.provider.DeleteBucket(ctx, identifier)
}

func (service *Service) readResource(ctx context.Context, resource ResourceRef, serviceKeyID string) (State, error) {
	switch resource.Kind {
	case ResourceDynamoDBTable:
		_, present, err := service.provider.Table(ctx, resource.Identifier)
		if err != nil {
			return StateBlocked, err
		}
		if present {
			return StateDestroying, nil
		}
		return StateVerifiedDestroyed, nil
	case ResourceS3Bucket:
		present, err := service.provider.BucketExists(ctx, resource.Identifier)
		if err != nil {
			return StateBlocked, err
		}
		if present {
			return StateDestroying, nil
		}
		return StateVerifiedDestroyed, nil
	case ResourceKMSAlias:
		present, err := service.provider.AliasExists(ctx, resource.Identifier, serviceKeyID)
		if err != nil {
			return StateBlocked, err
		}
		if present {
			return StateDestroying, nil
		}
		return StateVerifiedDestroyed, nil
	case ResourceKMSKey:
		key, present, err := service.provider.Key(ctx, resource.Identifier)
		if err != nil {
			return StateBlocked, err
		}
		if !present {
			return StateVerifiedDestroyed, nil
		}
		if key.State == "PendingDeletion" {
			return StatePendingKeyDeletion, nil
		}
		return StateDestroying, nil
	default:
		return StateBlocked, ErrInvalidRequest
	}
}

func closedRetainedResources(request Request, stackName string, resources []StackResourceObservation) ([]ResourceRef, error) {
	known := make(map[string]ResourceRef, len(resources))
	for _, resource := range resources {
		kind, tracked := retainedTemplateResources[resource.LogicalID]
		if !tracked {
			if retainedResourceType(resource.Type) {
				return nil, ErrUntrackedRetainedResource
			}
			continue
		}
		if _, duplicate := known[resource.LogicalID]; duplicate || resource.Identifier == "" || !resourceTypeMatches(kind, resource.Type) {
			return nil, ErrPlanStale
		}
		known[resource.LogicalID] = ResourceRef{LogicalID: resource.LogicalID, Kind: kind, Identifier: resource.Identifier}
	}
	for logicalID := range requiredDynamoTables {
		if _, found := known[logicalID]; !found {
			return nil, ErrPlanStale
		}
	}
	plan := Plan{Schema: PlanSchema, ConnectionID: request.ConnectionID, Region: request.Region, StackName: stackName, StackID: stackIDForValidation(request, stackName), RetainedResources: sortedResourceRefs(mapResourceRefs(known)), KMSDeletionWindowDays: defaultKeyDeletionDays}
	// Resource identifier validation is independent from the actual Stack ARN;
	// use a shape-valid stand-in here and validate the real ARN in Plan.Validate.
	if !retainedIdentifiersValid(plan, plan.RetainedResources) || !optionalResourceGroupsValid(keysOf(known)) {
		return nil, ErrPlanStale
	}
	return mapResourceRefs(known), nil
}

func retainedIdentifiersValid(plan Plan, resources []ResourceRef) bool {
	for _, resource := range resources {
		if resource.Kind == ResourceKMSKey {
			// KMS key ID validation does not depend on StackID; an ARN is checked
			// against Region in validResourceIdentifier once Plan is assembled.
			if !kmsKeyIDPattern.MatchString(resource.Identifier) && !kmsKeyARNPattern.MatchString(resource.Identifier) {
				return false
			}
			continue
		}
		if !validResourceIdentifier(plan, resource) {
			return false
		}
	}
	return true
}

func stackIDForValidation(request Request, stackName string) string {
	// This only gives closedRetainedResources a syntactically valid carrier for
	// local identifier checks. BuildPlan replaces it with AWS's exact stack ID
	// before Plan.Validate is called.
	return "arn:aws:cloudformation:" + request.Region + ":000000000000:stack/" + stackName + "/00000000-0000-0000-0000-000000000000"
}

func mapResourceRefs(values map[string]ResourceRef) []ResourceRef {
	result := make([]ResourceRef, 0, len(values))
	for _, value := range values {
		result = append(result, value)
	}
	return result
}

func keysOf(values map[string]ResourceRef) map[string]struct{} {
	result := make(map[string]struct{}, len(values))
	for key := range values {
		result[key] = struct{}{}
	}
	return result
}

func retainedResourceType(value string) bool {
	switch value {
	case "AWS::DynamoDB::Table", "AWS::S3::Bucket", "AWS::KMS::Key", "AWS::KMS::Alias":
		return true
	default:
		return false
	}
}

func resourceTypeMatches(kind ResourceKind, value string) bool {
	switch kind {
	case ResourceDynamoDBTable:
		return value == "AWS::DynamoDB::Table"
	case ResourceS3Bucket:
		return value == "AWS::S3::Bucket"
	case ResourceKMSKey:
		return value == "AWS::KMS::Key"
	case ResourceKMSAlias:
		return value == "AWS::KMS::Alias"
	default:
		return false
	}
}

func managedIDs(values []TaggedObservation, connectionID string, pattern *regexp.Regexp) ([]string, error) {
	result := make([]string, 0, len(values))
	for _, value := range values {
		if !pattern.MatchString(value.ID) || !managedTagsMatch(value, connectionID) {
			return nil, ErrPlanStale
		}
		result = append(result, value.ID)
	}
	sort.Strings(result)
	if !sortedUnique(result) {
		return nil, ErrPlanStale
	}
	return result, nil
}

func managedTagsMatch(value TaggedObservation, connectionID string) bool {
	return value.Tags["DirextalkConnectionId"] == connectionID && value.Tags["DirextalkRetention"] == "manual" && value.Tags["DirextalkBackupId"] != "" && value.Tags["DirextalkServiceId"] != "" && value.Tags["DirextalkDeploymentId"] != ""
}

func resourceIdentifier(plan Plan, logicalID string) string {
	for _, resource := range plan.RetainedResources {
		if resource.LogicalID == logicalID {
			return resource.Identifier
		}
	}
	return ""
}

func sameResourceRefs(left, right []ResourceRef) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func sameStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func subsetStrings(values, set []string) bool {
	index := 0
	for _, value := range values {
		for index < len(set) && set[index] < value {
			index++
		}
		if index == len(set) || set[index] != value {
			return false
		}
	}
	return true
}

func reportState(items []ItemReport) State {
	hasPendingKey := false
	for _, item := range items {
		switch item.State {
		case StateBlocked:
			return StateBlocked
		case StateDestroying, StatePlanned:
			return StateDestroying
		case StatePendingKeyDeletion:
			hasPendingKey = true
		case StateVerifiedDestroyed:
		default:
			return StateBlocked
		}
	}
	if hasPendingKey {
		return StatePendingKeyDeletion
	}
	return StateVerifiedDestroyed
}
