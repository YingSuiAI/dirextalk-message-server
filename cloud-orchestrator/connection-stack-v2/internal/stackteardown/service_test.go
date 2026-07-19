package stackteardown

import (
	"context"
	"errors"
	"sort"
	"testing"

	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/connectionbootstrap"
)

func TestRequestStackNameCompatibilityVector(t *testing.T) {
	request := Request{ConnectionID: "connection-cleanup-0001", Region: "us-east-1"}
	if got, want := request.stackName(), "dirextalk-connection-2c008a87faf7ead59ffbcc76"; got != want {
		t.Fatalf("stackName()=%q want=%q", got, want)
	}
	if got, want := request.stackName(), connectionbootstrap.DeterministicStackName(request.ConnectionID); got != want {
		t.Fatalf("stackName()=%q bootstrap contract=%q", got, want)
	}
}

func TestTeardownPlanExecuteAndReadBackKeepsClosedCleanupBoundary(t *testing.T) {
	t.Parallel()
	request := Request{ConnectionID: "connection-cleanup-0001", Region: "us-east-1"}
	fake := newTeardownFake(request)
	service := newService(fake)

	plan, err := service.BuildPlan(context.Background(), request)
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}
	if plan.Validate() != nil || len(plan.RetainedResources) != len(retainedTemplateResources) || len(plan.ManagedImageIDs) != 1 || len(plan.ManagedSnapshotIDs) != 1 {
		t.Fatalf("unexpected plan: %#v", plan)
	}

	first, err := service.Execute(context.Background(), plan)
	if err != nil {
		t.Fatalf("first Execute: %v", err)
	}
	if first.State != StateDestroying || fake.deleteStackCalls != 1 || fake.deleteTableCalls != 0 {
		t.Fatalf("first cleanup must delete Stack, wait for DynamoDB protection, and remain pending: report=%#v fake=%#v", first, fake)
	}
	if len(fake.disabledTables) != len(requiredDynamoTables)+2 || fake.aliases[serviceAlias(plan)] || fake.buckets[resourceIdentifier(plan, "DynamicArtifactBucket")] || len(fake.images) != 0 || len(fake.snapshots) != 0 {
		t.Fatalf("first cleanup did not follow bounded resource lifecycle: %#v", fake)
	}
	for _, keyID := range []string{resourceIdentifier(plan, "ServiceSecretSealingKey"), resourceIdentifier(plan, "DynamicArtifactKey")} {
		if fake.keys[keyID].State != "PendingDeletion" {
			t.Fatalf("key %s is not pending deletion: %#v", keyID, fake.keys[keyID])
		}
	}

	second, err := service.Execute(context.Background(), plan)
	if err != nil {
		t.Fatalf("second Execute: %v", err)
	}
	if second.State != StatePendingKeyDeletion || len(fake.tables) != 0 || fake.deleteTableCalls != len(requiredDynamoTables)+2 {
		t.Fatalf("second cleanup must have only KMS pending deletion left: report=%#v fake=%#v", second, fake)
	}

	delete(fake.keys, resourceIdentifier(plan, "ServiceSecretSealingKey"))
	delete(fake.keys, resourceIdentifier(plan, "DynamicArtifactKey"))
	readBack, err := service.ReadBack(context.Background(), plan)
	if err != nil || readBack.State != StateVerifiedDestroyed {
		t.Fatalf("terminal readback report=%#v err=%v", readBack, err)
	}
}

func TestTeardownExecuteRejectsTamperedPlanBeforeAnyMutation(t *testing.T) {
	t.Parallel()
	request := Request{ConnectionID: "connection-cleanup-0002", Region: "us-east-1"}
	fake := newTeardownFake(request)
	service := newService(fake)
	plan, err := service.BuildPlan(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	plan.ManagedImageIDs = append(plan.ManagedImageIDs, "ami-0abcdef0123456789")
	sort.Strings(plan.ManagedImageIDs)
	if plan.Validate() != nil {
		t.Fatal("fixture plan should remain syntactically valid before provider provenance checks")
	}
	if _, err := service.Execute(context.Background(), plan); !errors.Is(err, ErrPlanStale) {
		t.Fatalf("tampered Execute error=%v, want stale plan", err)
	}
	if fake.deleteStackCalls != 0 || fake.deleteTableCalls != 0 || len(fake.disabledTables) != 0 {
		t.Fatalf("tampered plan caused a mutation: %#v", fake)
	}
}

func TestTeardownPlanRejectsUnknownRetainedTemplateResource(t *testing.T) {
	t.Parallel()
	request := Request{ConnectionID: "connection-cleanup-0003", Region: "us-east-1"}
	fake := newTeardownFake(request)
	fake.resources = append(fake.resources, StackResourceObservation{LogicalID: "UnexpectedRetainedTable", Type: "AWS::DynamoDB::Table", Identifier: fake.stack.Name + "-UnexpectedRetainedTable-1234"})
	if _, err := newService(fake).BuildPlan(context.Background(), request); !errors.Is(err, ErrUntrackedRetainedResource) {
		t.Fatalf("BuildPlan error=%v, want untracked retained resource", err)
	}
}

func TestParsePlanRejectsDuplicateJSONKeys(t *testing.T) {
	t.Parallel()
	if _, err := ParsePlan([]byte(`{"schema":"` + PlanSchema + `","schema":"` + PlanSchema + `"}`)); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("duplicate keys error=%v", err)
	}
}

type teardownFake struct {
	stack            StackObservation
	stackPresent     bool
	resources        []StackResourceObservation
	tables           map[string]TableObservation
	buckets          map[string]bool
	images           map[string]TaggedObservation
	snapshots        map[string]TaggedObservation
	keys             map[string]KeyObservation
	aliases          map[string]bool
	deleteStackCalls int
	deleteTableCalls int
	disabledTables   map[string]bool
}

func newTeardownFake(request Request) *teardownFake {
	stackName := request.stackName()
	stack := StackObservation{ID: "arn:aws:cloudformation:" + request.Region + ":123456789012:stack/" + stackName + "/01234567-89ab-cdef-0123-456789abcdef", Name: stackName, Status: "CREATE_COMPLETE"}
	fake := &teardownFake{
		stack:          stack,
		stackPresent:   true,
		tables:         map[string]TableObservation{},
		buckets:        map[string]bool{},
		images:         map[string]TaggedObservation{},
		snapshots:      map[string]TaggedObservation{},
		keys:           map[string]KeyObservation{},
		aliases:        map[string]bool{},
		disabledTables: map[string]bool{},
	}
	for logicalID, kind := range retainedTemplateResources {
		identifier := fake.resourceIdentifier(logicalID, kind)
		fake.resources = append(fake.resources, StackResourceObservation{LogicalID: logicalID, Type: resourceType(kind), Identifier: identifier})
		switch kind {
		case ResourceDynamoDBTable:
			fake.tables[identifier] = TableObservation{Status: "ACTIVE", DeletionProtectionEnabled: true}
		case ResourceS3Bucket:
			fake.buckets[identifier] = true
		case ResourceKMSKey:
			fake.keys[identifier] = KeyObservation{State: "Enabled"}
		case ResourceKMSAlias:
			fake.aliases[identifier] = true
		}
	}
	tags := managedTags(request.ConnectionID)
	fake.images["ami-0123456789abcdef0"] = TaggedObservation{ID: "ami-0123456789abcdef0", Tags: tags}
	fake.snapshots["snap-0123456789abcdef0"] = TaggedObservation{ID: "snap-0123456789abcdef0", Tags: tags}
	return fake
}

func (fake *teardownFake) resourceIdentifier(logicalID string, kind ResourceKind) string {
	switch kind {
	case ResourceDynamoDBTable:
		return fake.stack.Name + "-" + logicalID + "-0123456789abcdef"
	case ResourceS3Bucket:
		return "dtx-artifact-0123456789abcdef0123456789abcdef0123456789"
	case ResourceKMSKey:
		if logicalID == "DynamicArtifactKey" {
			return "12345678-1234-1234-1234-1234567890ab"
		}
		return "abcdef12-1234-1234-1234-1234567890ab"
	case ResourceKMSAlias:
		return "alias/dirextalk/" + fake.stack.Name + "/service-secret-sessions"
	default:
		panic("unknown resource kind")
	}
}

func resourceType(kind ResourceKind) string {
	switch kind {
	case ResourceDynamoDBTable:
		return "AWS::DynamoDB::Table"
	case ResourceS3Bucket:
		return "AWS::S3::Bucket"
	case ResourceKMSKey:
		return "AWS::KMS::Key"
	case ResourceKMSAlias:
		return "AWS::KMS::Alias"
	default:
		panic("unknown resource kind")
	}
}

func managedTags(connectionID string) map[string]string {
	return map[string]string{
		"DirextalkConnectionId": connectionID,
		"DirextalkRetention":    "manual",
		"DirextalkBackupId":     "backup-cleanup-0001",
		"DirextalkServiceId":    "service-cleanup-0001",
		"DirextalkDeploymentId": "deployment-cleanup-0001",
	}
}

func serviceAlias(plan Plan) string { return resourceIdentifier(plan, "ServiceSecretSealingKeyAlias") }

func (fake *teardownFake) FindStack(_ context.Context, name string) (StackObservation, bool, error) {
	if !fake.stackPresent || name != fake.stack.Name {
		return StackObservation{}, false, nil
	}
	return fake.stack, true, nil
}
func (fake *teardownFake) StackResources(_ context.Context, stackID string) ([]StackResourceObservation, error) {
	if stackID != fake.stack.ID {
		return nil, ErrProviderUnavailable
	}
	return append([]StackResourceObservation(nil), fake.resources...), nil
}
func (fake *teardownFake) DeleteStack(_ context.Context, stackID string) error {
	if stackID != fake.stack.ID {
		return ErrProviderUnavailable
	}
	fake.deleteStackCalls++
	fake.stackPresent = false
	return nil
}
func (fake *teardownFake) Table(_ context.Context, identifier string) (TableObservation, bool, error) {
	value, found := fake.tables[identifier]
	return value, found, nil
}
func (fake *teardownFake) DisableTableDeletionProtection(_ context.Context, identifier string) error {
	value, found := fake.tables[identifier]
	if !found {
		return ErrProviderUnavailable
	}
	value.DeletionProtectionEnabled = false
	fake.tables[identifier] = value
	fake.disabledTables[identifier] = true
	return nil
}
func (fake *teardownFake) DeleteTable(_ context.Context, identifier string) error {
	if _, found := fake.tables[identifier]; !found {
		return nil
	}
	fake.deleteTableCalls++
	delete(fake.tables, identifier)
	return nil
}
func (fake *teardownFake) BucketExists(_ context.Context, identifier string) (bool, error) {
	return fake.buckets[identifier], nil
}
func (fake *teardownFake) EmptyBucket(context.Context, string) error { return nil }
func (fake *teardownFake) DeleteBucket(_ context.Context, identifier string) error {
	delete(fake.buckets, identifier)
	return nil
}
func (fake *teardownFake) ManagedImages(_ context.Context, connectionID string) ([]TaggedObservation, error) {
	return taggedValues(fake.images, connectionID), nil
}
func (fake *teardownFake) ManagedSnapshots(_ context.Context, connectionID string) ([]TaggedObservation, error) {
	return taggedValues(fake.snapshots, connectionID), nil
}
func taggedValues(values map[string]TaggedObservation, connectionID string) []TaggedObservation {
	result := []TaggedObservation{}
	for _, value := range values {
		if value.Tags["DirextalkConnectionId"] == connectionID {
			result = append(result, value)
		}
	}
	return result
}
func (fake *teardownFake) Image(_ context.Context, identifier string) (TaggedObservation, bool, error) {
	value, found := fake.images[identifier]
	return value, found, nil
}
func (fake *teardownFake) DeregisterImage(_ context.Context, identifier string) error {
	delete(fake.images, identifier)
	return nil
}
func (fake *teardownFake) Snapshot(_ context.Context, identifier string) (TaggedObservation, bool, error) {
	value, found := fake.snapshots[identifier]
	return value, found, nil
}
func (fake *teardownFake) DeleteSnapshot(_ context.Context, identifier string) error {
	delete(fake.snapshots, identifier)
	return nil
}
func (fake *teardownFake) Key(_ context.Context, identifier string) (KeyObservation, bool, error) {
	value, found := fake.keys[identifier]
	return value, found, nil
}
func (fake *teardownFake) AliasExists(_ context.Context, identifier, _ string) (bool, error) {
	return fake.aliases[identifier], nil
}
func (fake *teardownFake) DeleteAlias(_ context.Context, identifier string) error {
	delete(fake.aliases, identifier)
	return nil
}
func (fake *teardownFake) ScheduleKeyDeletion(_ context.Context, identifier string, days int32) error {
	if days != defaultKeyDeletionDays {
		return ErrInvalidRequest
	}
	value, found := fake.keys[identifier]
	if !found {
		return nil
	}
	value.State = "PendingDeletion"
	fake.keys[identifier] = value
	return nil
}
