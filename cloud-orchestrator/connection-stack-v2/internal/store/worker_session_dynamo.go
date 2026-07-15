package store

import (
	"context"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/contract"
)

const workerSessionActiveRetention = 24 * time.Hour

var (
	workerNamedDigestPattern   = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	workerAMIObjectPattern     = regexp.MustCompile(`^ami-[0-9a-f]{8,17}$`)
	workerInstancePattern      = regexp.MustCompile(`^i-[0-9a-f]{8,17}$`)
	workerVPCPattern           = regexp.MustCompile(`^vpc-[0-9a-f]{8,17}$`)
	workerSubnetPattern        = regexp.MustCompile(`^subnet-[0-9a-f]{8,17}$`)
	workerSecurityGroupPattern = regexp.MustCompile(`^sg-[0-9a-f]{8,17}$`)
	workerTypePattern          = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{1,63}$`)
	workerAZPattern            = regexp.MustCompile(`^(?:af|ap|ca|cn|eu|il|me|mx|sa|us)(?:-gov)?-[a-z]+-[0-9][a-z]$`)
)

func (s *DynamoRepository) LookupWorkerSession(ctx context.Context, bootstrapSessionID string) (WorkerSession, bool, error) {
	if !contract.ValidID(bootstrapSessionID) {
		return WorkerSession{}, false, NewError("worker_session_invalid")
	}
	output, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{TableName: &s.workerSessionsTable, ConsistentRead: boolPtr(true), Key: map[string]dynamodbtypes.AttributeValue{"bootstrap_session_id": &dynamodbtypes.AttributeValueMemberS{Value: bootstrapSessionID}}})
	if err != nil {
		return WorkerSession{}, false, NewError("worker_session_unavailable")
	}
	if len(output.Item) == 0 {
		return WorkerSession{}, false, nil
	}
	session, err := workerSessionFromItem(output.Item)
	if err != nil || session.BootstrapSessionID != bootstrapSessionID {
		return WorkerSession{}, false, NewError("worker_session_store_invalid")
	}
	return session, true, nil
}

func (s *DynamoRepository) ActivateWorkerSession(ctx context.Context, claim WorkerSessionClaim) (WorkerSession, error) {
	if !validWorkerSession(claim.Session) || (claim.Session.State != "bound" && claim.Session.State != "active") || !validSHA256(claim.TokenSHA256) {
		return WorkerSession{}, NewError("worker_session_invalid")
	}
	now, err := time.Parse("2006-01-02T15:04:05.000Z", claim.Now)
	if err != nil || now.UTC().Format("2006-01-02T15:04:05.000Z") != claim.Now {
		return WorkerSession{}, NewError("worker_session_invalid")
	}
	lease, err := time.Parse("2006-01-02T15:04:05.000Z", claim.LeaseExpiresAt)
	if err != nil || !lease.After(now) || lease.After(now.Add(10*time.Minute)) {
		return WorkerSession{}, NewError("worker_session_invalid")
	}
	values := map[string]dynamodbtypes.AttributeValue{
		":active": &dynamodbtypes.AttributeValueMemberS{Value: "active"}, ":bound": &dynamodbtypes.AttributeValueMemberS{Value: "bound"},
		":connection_id": &dynamodbtypes.AttributeValueMemberS{Value: claim.Session.ConnectionID}, ":deployment_id": &dynamodbtypes.AttributeValueMemberS{Value: claim.Session.DeploymentID},
		":request_sha256": &dynamodbtypes.AttributeValueMemberS{Value: claim.Session.RequestSHA256}, ":instance_id": &dynamodbtypes.AttributeValueMemberS{Value: claim.Session.ExpectedInstanceID},
		":worker_image_digest": &dynamodbtypes.AttributeValueMemberS{Value: claim.Session.WorkerImageDigest}, ":artifact_manifest_digest": &dynamodbtypes.AttributeValueMemberS{Value: claim.Session.ArtifactManifestDigest},
		":bootstrap_endpoint": &dynamodbtypes.AttributeValueMemberS{Value: claim.Session.BootstrapEndpoint}, ":expected_ami_id": &dynamodbtypes.AttributeValueMemberS{Value: claim.Session.ExpectedAMIID},
		":expected_instance_type": &dynamodbtypes.AttributeValueMemberS{Value: claim.Session.ExpectedInstanceType}, ":expected_architecture": &dynamodbtypes.AttributeValueMemberS{Value: claim.Session.ExpectedArchitecture},
		":expected_vpc_id": &dynamodbtypes.AttributeValueMemberS{Value: claim.Session.ExpectedVPCID}, ":expected_subnet_id": &dynamodbtypes.AttributeValueMemberS{Value: claim.Session.ExpectedSubnetID},
		":expected_availability_zone": &dynamodbtypes.AttributeValueMemberS{Value: claim.Session.ExpectedAvailabilityZone}, ":expected_security_group_id": &dynamodbtypes.AttributeValueMemberS{Value: claim.Session.ExpectedSecurityGroupID},
		":expires_at":  &dynamodbtypes.AttributeValueMemberS{Value: claim.Session.ExpiresAt},
		":lease_epoch": &dynamodbtypes.AttributeValueMemberN{Value: strconv.FormatInt(claim.Session.LeaseEpoch, 10)},
		":one":         &dynamodbtypes.AttributeValueMemberN{Value: "1"}, ":zero": &dynamodbtypes.AttributeValueMemberN{Value: "0"},
		":lease_expires_at": &dynamodbtypes.AttributeValueMemberS{Value: claim.LeaseExpiresAt}, ":token_sha256": &dynamodbtypes.AttributeValueMemberS{Value: claim.TokenSHA256},
		":ttl": &dynamodbtypes.AttributeValueMemberN{Value: strconv.FormatInt(lease.Add(workerSessionActiveRetention).Unix(), 10)},
	}
	condition := "connection_id = :connection_id AND deployment_id = :deployment_id AND request_sha256 = :request_sha256 AND expected_instance_id = :instance_id AND " +
		"worker_image_digest = :worker_image_digest AND artifact_manifest_digest = :artifact_manifest_digest AND bootstrap_endpoint = :bootstrap_endpoint AND " +
		"expected_ami_id = :expected_ami_id AND expected_instance_type = :expected_instance_type AND expected_architecture = :expected_architecture AND " +
		"expected_vpc_id = :expected_vpc_id AND expected_subnet_id = :expected_subnet_id AND expected_availability_zone = :expected_availability_zone AND " +
		"expected_security_group_id = :expected_security_group_id AND expires_at = :expires_at AND lease_epoch = :lease_epoch AND (#state = :bound OR #state = :active)"
	input := &dynamodb.TransactWriteItemsInput{TransactItems: []dynamodbtypes.TransactWriteItem{{Update: &dynamodbtypes.Update{TableName: &s.workerSessionsTable, Key: map[string]dynamodbtypes.AttributeValue{"bootstrap_session_id": &dynamodbtypes.AttributeValueMemberS{Value: claim.Session.BootstrapSessionID}}, ConditionExpression: stringPtr(condition), UpdateExpression: stringPtr("SET #state = :active, lease_epoch = lease_epoch + :one, lease_expires_at = :lease_expires_at, token_sha256 = :token_sha256, last_sequence = :zero, ttl_epoch_seconds = :ttl REMOVE last_event_at, last_event_sha256"), ExpressionAttributeNames: map[string]string{"#state": "state"}, ExpressionAttributeValues: values}}}}
	_, writeErr := s.client.TransactWriteItems(ctx, input)
	stored, found, readErr := s.LookupWorkerSession(ctx, claim.Session.BootstrapSessionID)
	if readErr != nil {
		return WorkerSession{}, readErr
	}
	if found && stored.ConnectionID == claim.Session.ConnectionID && stored.DeploymentID == claim.Session.DeploymentID && stored.RequestSHA256 == claim.Session.RequestSHA256 && stored.ExpectedInstanceID == claim.Session.ExpectedInstanceID && stored.State == "active" && stored.TokenSHA256 == claim.TokenSHA256 && stored.LeaseExpiresAt == claim.LeaseExpiresAt {
		return stored, nil
	}
	if writeErr != nil {
		return WorkerSession{}, NewError("worker_session_conflict")
	}
	return WorkerSession{}, NewError("worker_session_store_invalid")
}

func (s *DynamoRepository) RecordWorkerSessionEvent(ctx context.Context, event WorkerSessionEvent) (WorkerSession, bool, error) {
	if s == nil || s.client == nil || !contract.ValidConnectionID(event.ConnectionID) || !contract.ValidID(event.DeploymentID) || !contract.ValidID(event.BootstrapSessionID) || !workerInstancePattern.MatchString(event.ExpectedInstanceID) || event.LeaseEpoch < 1 || event.Sequence < 1 || !validSHA256(event.TokenSHA256) || !validSHA256(event.EventSHA256) || !canonicalWorkerEventInstant(event.OccurredAt) || !canonicalWorkerEventInstant(event.Now) {
		return WorkerSession{}, false, NewError("worker_event_invalid")
	}
	previous := event.Sequence - 1
	values := map[string]dynamodbtypes.AttributeValue{
		":active":            &dynamodbtypes.AttributeValueMemberS{Value: "active"},
		":connection_id":     &dynamodbtypes.AttributeValueMemberS{Value: event.ConnectionID},
		":deployment_id":     &dynamodbtypes.AttributeValueMemberS{Value: event.DeploymentID},
		":instance_id":       &dynamodbtypes.AttributeValueMemberS{Value: event.ExpectedInstanceID},
		":lease_epoch":       &dynamodbtypes.AttributeValueMemberN{Value: strconv.FormatInt(event.LeaseEpoch, 10)},
		":token_sha256":      &dynamodbtypes.AttributeValueMemberS{Value: event.TokenSHA256},
		":now":               &dynamodbtypes.AttributeValueMemberS{Value: event.Now},
		":previous_sequence": &dynamodbtypes.AttributeValueMemberN{Value: strconv.FormatInt(previous, 10)},
		":sequence":          &dynamodbtypes.AttributeValueMemberN{Value: strconv.FormatInt(event.Sequence, 10)},
		":occurred_at":       &dynamodbtypes.AttributeValueMemberS{Value: event.OccurredAt},
		":last_event_sha256": &dynamodbtypes.AttributeValueMemberS{Value: event.EventSHA256},
	}
	condition := "#state = :active AND connection_id = :connection_id AND deployment_id = :deployment_id AND expected_instance_id = :instance_id AND lease_epoch = :lease_epoch AND token_sha256 = :token_sha256 AND lease_expires_at > :now AND last_sequence = :previous_sequence"
	update := &dynamodbtypes.Update{TableName: &s.workerSessionsTable, Key: map[string]dynamodbtypes.AttributeValue{"bootstrap_session_id": &dynamodbtypes.AttributeValueMemberS{Value: event.BootstrapSessionID}}, ConditionExpression: stringPtr(condition), UpdateExpression: stringPtr("SET last_sequence = :sequence, last_event_at = :occurred_at, last_event_sha256 = :last_event_sha256"), ExpressionAttributeNames: map[string]string{"#state": "state"}, ExpressionAttributeValues: values}
	_, writeErr := s.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: []dynamodbtypes.TransactWriteItem{{Update: update}}})
	stored, found, readErr := s.LookupWorkerSession(ctx, event.BootstrapSessionID)
	if readErr != nil {
		return WorkerSession{}, false, readErr
	}
	if found && workerSessionEventBinding(stored, event) && stored.LastSequence == event.Sequence && stored.LastEventSHA256 == event.EventSHA256 && stored.LastEventAt == event.OccurredAt {
		return stored, writeErr != nil, nil
	}
	if writeErr != nil {
		return WorkerSession{}, false, NewError("worker_event_conflict")
	}
	return WorkerSession{}, false, NewError("worker_session_store_invalid")
}

func workerSessionEventBinding(session WorkerSession, event WorkerSessionEvent) bool {
	return session.State == "active" && session.ConnectionID == event.ConnectionID && session.DeploymentID == event.DeploymentID && session.BootstrapSessionID == event.BootstrapSessionID && session.ExpectedInstanceID == event.ExpectedInstanceID && session.LeaseEpoch == event.LeaseEpoch && session.TokenSHA256 == event.TokenSHA256 && session.LeaseExpiresAt > event.Now
}

func canonicalWorkerEventInstant(value string) bool {
	parsed, err := time.Parse("2006-01-02T15:04:05.000Z", value)
	return err == nil && parsed.UTC().Format("2006-01-02T15:04:05.000Z") == value
}

func workerSessionItem(session WorkerSession) map[string]dynamodbtypes.AttributeValue {
	expires, _ := time.Parse("2006-01-02T15:04:05.000Z", session.ExpiresAt)
	return map[string]dynamodbtypes.AttributeValue{
		"bootstrap_session_id": &dynamodbtypes.AttributeValueMemberS{Value: session.BootstrapSessionID}, "connection_id": &dynamodbtypes.AttributeValueMemberS{Value: session.ConnectionID},
		"deployment_id": &dynamodbtypes.AttributeValueMemberS{Value: session.DeploymentID}, "request_sha256": &dynamodbtypes.AttributeValueMemberS{Value: session.RequestSHA256},
		"worker_image_digest": &dynamodbtypes.AttributeValueMemberS{Value: session.WorkerImageDigest}, "artifact_manifest_digest": &dynamodbtypes.AttributeValueMemberS{Value: session.ArtifactManifestDigest},
		"bootstrap_endpoint": &dynamodbtypes.AttributeValueMemberS{Value: session.BootstrapEndpoint}, "expected_ami_id": &dynamodbtypes.AttributeValueMemberS{Value: session.ExpectedAMIID},
		"expected_instance_type": &dynamodbtypes.AttributeValueMemberS{Value: session.ExpectedInstanceType}, "expected_architecture": &dynamodbtypes.AttributeValueMemberS{Value: session.ExpectedArchitecture},
		"expected_vpc_id": &dynamodbtypes.AttributeValueMemberS{Value: session.ExpectedVPCID}, "expected_subnet_id": &dynamodbtypes.AttributeValueMemberS{Value: session.ExpectedSubnetID},
		"expected_availability_zone": &dynamodbtypes.AttributeValueMemberS{Value: session.ExpectedAvailabilityZone}, "expected_security_group_id": &dynamodbtypes.AttributeValueMemberS{Value: session.ExpectedSecurityGroupID},
		"state": &dynamodbtypes.AttributeValueMemberS{Value: session.State}, "expires_at": &dynamodbtypes.AttributeValueMemberS{Value: session.ExpiresAt},
		"lease_epoch": &dynamodbtypes.AttributeValueMemberN{Value: "0"}, "last_sequence": &dynamodbtypes.AttributeValueMemberN{Value: "0"},
		"ttl_epoch_seconds": &dynamodbtypes.AttributeValueMemberN{Value: strconv.FormatInt(expires.Unix(), 10)},
	}
}

func workerSessionFromItem(item map[string]dynamodbtypes.AttributeValue) (WorkerSession, error) {
	allowed := map[string]struct{}{
		"bootstrap_session_id": {}, "connection_id": {}, "deployment_id": {}, "request_sha256": {},
		"worker_image_digest": {}, "artifact_manifest_digest": {}, "bootstrap_endpoint": {}, "expected_ami_id": {},
		"expected_instance_type": {}, "expected_architecture": {}, "expected_vpc_id": {}, "expected_subnet_id": {},
		"expected_availability_zone": {}, "expected_security_group_id": {}, "expected_instance_id": {}, "state": {},
		"expires_at": {}, "lease_epoch": {}, "lease_expires_at": {}, "token_sha256": {}, "last_sequence": {},
		"last_event_at": {}, "last_event_sha256": {}, "ttl_epoch_seconds": {},
	}
	for name := range item {
		if _, ok := allowed[name]; !ok {
			return WorkerSession{}, NewError("worker_session_store_invalid")
		}
	}
	var result WorkerSession
	var err error
	for name, target := range map[string]*string{
		"bootstrap_session_id": &result.BootstrapSessionID, "connection_id": &result.ConnectionID, "deployment_id": &result.DeploymentID,
		"request_sha256": &result.RequestSHA256, "worker_image_digest": &result.WorkerImageDigest, "artifact_manifest_digest": &result.ArtifactManifestDigest,
		"bootstrap_endpoint": &result.BootstrapEndpoint, "expected_ami_id": &result.ExpectedAMIID, "expected_instance_type": &result.ExpectedInstanceType,
		"expected_architecture": &result.ExpectedArchitecture, "expected_vpc_id": &result.ExpectedVPCID, "expected_subnet_id": &result.ExpectedSubnetID,
		"expected_availability_zone": &result.ExpectedAvailabilityZone, "expected_security_group_id": &result.ExpectedSecurityGroupID, "state": &result.State, "expires_at": &result.ExpiresAt,
	} {
		*target, err = stringAttribute(item, name)
		if err != nil {
			return WorkerSession{}, err
		}
	}
	result.LeaseEpoch, err = numberAttribute(item, "lease_epoch", true)
	if err != nil {
		return WorkerSession{}, err
	}
	result.LastSequence, err = numberAttribute(item, "last_sequence", true)
	if err != nil {
		return WorkerSession{}, err
	}
	optional := map[string]*string{"expected_instance_id": &result.ExpectedInstanceID, "lease_expires_at": &result.LeaseExpiresAt, "token_sha256": &result.TokenSHA256, "last_event_at": &result.LastEventAt, "last_event_sha256": &result.LastEventSHA256}
	for name, target := range optional {
		if value, ok := item[name].(*dynamodbtypes.AttributeValueMemberS); ok {
			*target = value.Value
		}
	}
	if !validWorkerSession(result) {
		return WorkerSession{}, NewError("worker_session_store_invalid")
	}
	return result, nil
}

func validWorkerSession(session WorkerSession) bool {
	if !contract.ValidID(session.BootstrapSessionID) || !contract.ValidConnectionID(session.ConnectionID) || !contract.ValidID(session.DeploymentID) || !validSHA256(session.RequestSHA256) || !workerNamedDigestPattern.MatchString(session.WorkerImageDigest) || !workerNamedDigestPattern.MatchString(session.ArtifactManifestDigest) || !contract.ValidWorkerBootstrapEndpoint(session.BootstrapEndpoint) || !workerAMIObjectPattern.MatchString(session.ExpectedAMIID) || !workerTypePattern.MatchString(session.ExpectedInstanceType) || (session.ExpectedArchitecture != "x86_64" && session.ExpectedArchitecture != "arm64") || !workerVPCPattern.MatchString(session.ExpectedVPCID) || !workerSubnetPattern.MatchString(session.ExpectedSubnetID) || !workerAZPattern.MatchString(session.ExpectedAvailabilityZone) || !contract.ValidAvailabilityZone(strings.TrimSuffix(session.ExpectedAvailabilityZone, session.ExpectedAvailabilityZone[len(session.ExpectedAvailabilityZone)-1:]), session.ExpectedAvailabilityZone) || !workerSecurityGroupPattern.MatchString(session.ExpectedSecurityGroupID) {
		return false
	}
	expires, err := time.Parse("2006-01-02T15:04:05.000Z", session.ExpiresAt)
	if err != nil || expires.UTC().Format("2006-01-02T15:04:05.000Z") != session.ExpiresAt || session.LeaseEpoch < 0 || session.LastSequence < 0 {
		return false
	}
	switch session.State {
	case "issued":
		return session.ExpectedInstanceID == "" && session.LeaseEpoch == 0 && session.LeaseExpiresAt == "" && session.TokenSHA256 == "" && session.LastSequence == 0 && session.LastEventAt == "" && session.LastEventSHA256 == ""
	case "bound":
		return workerInstancePattern.MatchString(session.ExpectedInstanceID) && session.LeaseEpoch == 0 && session.LeaseExpiresAt == "" && session.TokenSHA256 == "" && session.LastSequence == 0 && session.LastEventAt == "" && session.LastEventSHA256 == ""
	case "active":
		if !workerInstancePattern.MatchString(session.ExpectedInstanceID) || session.LeaseEpoch < 1 || !validSHA256(session.TokenSHA256) {
			return false
		}
		lease, err := time.Parse("2006-01-02T15:04:05.000Z", session.LeaseExpiresAt)
		if err != nil || lease.UTC().Format("2006-01-02T15:04:05.000Z") != session.LeaseExpiresAt {
			return false
		}
		if session.LastSequence == 0 {
			return session.LastEventAt == "" && session.LastEventSHA256 == ""
		}
		eventAt, eventErr := time.Parse("2006-01-02T15:04:05.000Z", session.LastEventAt)
		return eventErr == nil && eventAt.UTC().Format("2006-01-02T15:04:05.000Z") == session.LastEventAt && validSHA256(session.LastEventSHA256)
	default:
		return false
	}
}
