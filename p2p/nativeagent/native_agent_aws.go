package nativeagent

import (
	"context"
	cryptorand "crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"
)

const (
	awsApprovalTTL       = 10 * time.Minute
	awsManagedTagKey     = "dirextalk-managed"
	awsManagedTagValue   = "true"
	awsAgentTagKey       = "dirextalk-agent"
	awsAgentTagValue     = "native"
	awsApprovalTagKey    = "dirextalk-approval"
	defaultAWSImageAlias = "amazon-linux-2023"
)

var (
	awsInstanceIDPattern   = regexp.MustCompile(`^i-[0-9a-fA-F]{8,32}$`)
	awsInstanceTypePattern = regexp.MustCompile(`^[a-z][a-z0-9-]*\.[a-z0-9-]+$`)
	awsResourceIDPattern   = regexp.MustCompile(`^[A-Za-z0-9._:/+=,@-]{1,255}$`)
)

// AWSClientCredentials are request-scoped. Implementations must not persist,
// log, or expose them in tool results.
type AWSClientCredentials struct {
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
	Region          string
}

type AWSIdentity struct {
	AccountID string
	ARN       string
	UserID    string
}

type AWSInstance struct {
	InstanceID       string
	InstanceType     string
	ImageID          string
	State            string
	AvailabilityZone string
	PublicIPAddress  string
	PrivateIPAddress string
	LaunchTime       string
	Name             string
	Managed          bool
}

type AWSCreateInstanceInput struct {
	Region           string
	ImageID          string
	InstanceType     string
	SubnetID         string
	SecurityGroupIDs []string
	KeyName          string
	VolumeSizeGB     int32
	Purpose          string
}

// AWSClient is intentionally narrow so approval behavior can be tested without
// network calls and the AWS SDK stays behind one adapter.
type AWSClient interface {
	Identity(context.Context) (AWSIdentity, error)
	ListInstances(context.Context) ([]AWSInstance, error)
	ResolveImage(context.Context, string) (string, error)
	DescribeInstance(context.Context, string) (AWSInstance, error)
	CreateInstance(context.Context, AWSCreateInstanceInput, string) (AWSInstance, error)
	TerminateInstance(context.Context, string) (AWSInstance, error)
}

type AWSClientFactory func(context.Context, AWSClientCredentials, *http.Client) (AWSClient, error)

type awsApprovalPlan struct {
	Operation string                  `json:"operation"`
	Region    string                  `json:"region"`
	Create    *AWSCreateInstanceInput `json:"create,omitempty"`
	Terminate *awsTerminatePlan       `json:"terminate,omitempty"`
}

type awsTerminatePlan struct {
	InstanceID   string `json:"instance_id"`
	InstanceType string `json:"instance_type,omitempty"`
	Name         string `json:"name,omitempty"`
}

type pendingAWSApproval struct {
	ID             string
	ConversationID string
	AccountID      string
	AccountARN     string
	Digest         string
	Plan           awsApprovalPlan
	CreatedAt      time.Time
	ExpiresAt      time.Time
	Executing      bool
}

type awsApprovalStore struct {
	mu      sync.Mutex
	pending map[string]pendingAWSApproval
	now     func() time.Time
}

func newAWSApprovalStore() *awsApprovalStore {
	return &awsApprovalStore{
		pending: make(map[string]pendingAWSApproval),
		now:     time.Now,
	}
}

func (s *awsApprovalStore) create(conversationID string, identity AWSIdentity, plan awsApprovalPlan) (pendingAWSApproval, error) {
	digest, err := awsApprovalPlanDigest(plan)
	if err != nil {
		return pendingAWSApproval{}, err
	}
	approvalID, err := newAWSApprovalID()
	if err != nil {
		return pendingAWSApproval{}, err
	}
	now := s.now().UTC()
	approval := pendingAWSApproval{
		ID:             approvalID,
		ConversationID: sanitizeNativeID(conversationID),
		AccountID:      strings.TrimSpace(identity.AccountID),
		AccountARN:     strings.TrimSpace(identity.ARN),
		Digest:         digest,
		Plan:           plan,
		CreatedAt:      now,
		ExpiresAt:      now.Add(awsApprovalTTL),
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.purgeExpiredLocked(now)
	s.pending[approval.ID] = approval
	return approval, nil
}

func newAWSApprovalID() (string, error) {
	var bytes [16]byte
	if _, err := cryptorand.Read(bytes[:]); err != nil {
		return "", fmt.Errorf("create AWS approval identity: %w", err)
	}
	return "aws_" + hex.EncodeToString(bytes[:]), nil
}

func (s *awsApprovalStore) begin(id, conversationID string) (pendingAWSApproval, error) {
	id = strings.TrimSpace(id)
	conversationID = sanitizeNativeID(conversationID)
	now := s.now().UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.purgeExpiredLocked(now)
	approval, ok := s.pending[id]
	if !ok {
		return pendingAWSApproval{}, fmt.Errorf("AWS approval is missing or expired")
	}
	if approval.ConversationID != conversationID {
		return pendingAWSApproval{}, fmt.Errorf("AWS approval does not belong to this conversation")
	}
	if approval.Executing {
		return pendingAWSApproval{}, fmt.Errorf("AWS approval is already executing")
	}
	approval.Executing = true
	s.pending[id] = approval
	return approval, nil
}

func (s *awsApprovalStore) release(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	approval, ok := s.pending[id]
	if !ok {
		return
	}
	approval.Executing = false
	s.pending[id] = approval
}

func (s *awsApprovalStore) complete(id string) {
	s.mu.Lock()
	delete(s.pending, strings.TrimSpace(id))
	s.mu.Unlock()
}

func (s *awsApprovalStore) cancel(id, conversationID string) error {
	id = strings.TrimSpace(id)
	conversationID = sanitizeNativeID(conversationID)
	now := s.now().UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.purgeExpiredLocked(now)
	approval, ok := s.pending[id]
	if !ok {
		return fmt.Errorf("AWS approval is missing or expired")
	}
	if approval.ConversationID != conversationID {
		return fmt.Errorf("AWS approval does not belong to this conversation")
	}
	if approval.Executing {
		return fmt.Errorf("AWS approval is already executing")
	}
	delete(s.pending, id)
	return nil
}

func (s *awsApprovalStore) purgeExpiredLocked(now time.Time) {
	for id, approval := range s.pending {
		if !approval.ExpiresAt.After(now) {
			delete(s.pending, id)
		}
	}
}

func (r *Runtime) requestScopedAWSTools(params map[string]any) []Tool {
	credentials := toolCredentialsFromParams(params).AWS
	if credentials.validate() != nil {
		return nil
	}
	conversationID := nativeAgentConversationKey(params)
	return []Tool{
		{
			Name:        "aws_account_identity",
			Description: "Read the AWS account identity associated with the user's request-scoped credentials.",
			Parameters:  objectSchema(map[string]any{}),
			Handler: func(ctx context.Context, _ map[string]any) (any, error) {
				client, err := r.newAWSClient(ctx, credentials)
				if err != nil {
					return nil, err
				}
				identity, err := client.Identity(ctx)
				if err != nil {
					return nil, fmt.Errorf("read AWS identity: %w", err)
				}
				return awsIdentityResponse(credentials.Region, identity), nil
			},
		},
		{
			Name:        "aws_ec2_instances_list",
			Description: "List EC2 instances in the configured AWS region. This is read-only.",
			Parameters:  objectSchema(map[string]any{}),
			Handler: func(ctx context.Context, _ map[string]any) (any, error) {
				client, err := r.newAWSClient(ctx, credentials)
				if err != nil {
					return nil, err
				}
				instances, err := client.ListInstances(ctx)
				if err != nil {
					return nil, fmt.Errorf("list EC2 instances: %w", err)
				}
				return map[string]any{
					"region":    credentials.Region,
					"instances": awsInstanceResponses(instances),
				}, nil
			},
		},
		{
			Name:        "aws_ec2_instance_create",
			Description: "Prepare creation of one managed EC2 instance. This never creates immediately: it returns an exact confirmation request that only the user can approve in the app.",
			Parameters: map[string]any{
				"type":     "object",
				"required": []any{"instance_type"},
				"properties": map[string]any{
					"instance_type":      stringSchema(),
					"image_id":           stringSchema(),
					"image_alias":        stringSchema(),
					"subnet_id":          stringSchema(),
					"security_group_ids": map[string]any{"type": "array", "items": stringSchema(), "maxItems": 5},
					"key_name":           stringSchema(),
					"volume_size_gb":     map[string]any{"type": "integer", "minimum": 8, "maximum": 2048},
					"purpose":            stringSchema(),
				},
			},
			Write: true,
			Handler: func(ctx context.Context, args map[string]any) (any, error) {
				return r.prepareAWSCreateApproval(ctx, credentials, conversationID, args)
			},
		},
		{
			Name:        "aws_ec2_instance_terminate",
			Description: "Prepare termination of a Dirextalk-managed EC2 instance. This never terminates immediately: it returns a confirmation request that only the user can approve in the app.",
			Parameters: map[string]any{
				"type":     "object",
				"required": []any{"instance_id"},
				"properties": map[string]any{
					"instance_id": stringSchema(),
				},
			},
			Write: true,
			Handler: func(ctx context.Context, args map[string]any) (any, error) {
				return r.prepareAWSTerminateApproval(ctx, credentials, conversationID, args)
			},
		},
	}
}

func (r *Runtime) testAWSCredentials(ctx context.Context, params map[string]any) (map[string]any, error) {
	credentials := toolCredentialsFromParams(params).AWS
	if err := credentials.validate(); err != nil {
		return nil, err
	}
	client, err := r.newAWSClient(ctx, credentials)
	if err != nil {
		return nil, err
	}
	identity, err := client.Identity(ctx)
	if err != nil {
		return nil, fmt.Errorf("validate AWS credentials: %w", err)
	}
	return map[string]any{
		"ok": true,
		"identity": map[string]any{
			"account_id": strings.TrimSpace(identity.AccountID),
			"arn":        strings.TrimSpace(identity.ARN),
		},
	}, nil
}

func (r *Runtime) prepareAWSCreateApproval(
	ctx context.Context,
	credentials awsCredentials,
	conversationID string,
	args map[string]any,
) (map[string]any, error) {
	instanceType := strings.ToLower(strings.TrimSpace(trimString(args["instance_type"])))
	if !awsInstanceTypePattern.MatchString(instanceType) {
		return nil, fmt.Errorf("a valid EC2 instance_type is required")
	}
	imageID := strings.TrimSpace(trimString(args["image_id"]))
	imageAlias := strings.TrimSpace(trimString(args["image_alias"]))
	if imageID == "" && imageAlias == "" {
		imageAlias = defaultAWSImageAlias
	}
	if imageID != "" && !strings.HasPrefix(imageID, "ami-") {
		return nil, fmt.Errorf("AWS image_id must begin with ami-")
	}
	subnetID := strings.TrimSpace(trimString(args["subnet_id"]))
	if subnetID != "" && !strings.HasPrefix(subnetID, "subnet-") {
		return nil, fmt.Errorf("AWS subnet_id must begin with subnet-")
	}
	securityGroupIDs := stringSliceParam(args["security_group_ids"])
	for _, id := range securityGroupIDs {
		if !strings.HasPrefix(id, "sg-") || !awsResourceIDPattern.MatchString(id) {
			return nil, fmt.Errorf("AWS security_group_ids contains an invalid value")
		}
	}
	keyName := strings.TrimSpace(trimString(args["key_name"]))
	if keyName != "" && !awsResourceIDPattern.MatchString(keyName) {
		return nil, fmt.Errorf("AWS key_name is invalid")
	}
	volumeSize := int64Param(args["volume_size_gb"])
	if volumeSize != 0 && (volumeSize < 8 || volumeSize > 2048) {
		return nil, fmt.Errorf("AWS volume_size_gb must be between 8 and 2048")
	}
	purpose := previewText(trimString(args["purpose"]), 160)
	client, err := r.newAWSClient(ctx, credentials)
	if err != nil {
		return nil, err
	}
	identity, err := client.Identity(ctx)
	if err != nil {
		return nil, fmt.Errorf("read AWS identity before approval: %w", err)
	}
	if imageID == "" {
		imageID, err = client.ResolveImage(ctx, imageAlias)
		if err != nil {
			return nil, fmt.Errorf("resolve AWS image: %w", err)
		}
	}
	plan := awsApprovalPlan{
		Operation: "ec2.create",
		Region:    credentials.Region,
		Create: &AWSCreateInstanceInput{
			Region:           credentials.Region,
			ImageID:          imageID,
			InstanceType:     instanceType,
			SubnetID:         subnetID,
			SecurityGroupIDs: append([]string(nil), securityGroupIDs...),
			KeyName:          keyName,
			VolumeSizeGB:     int32(volumeSize),
			Purpose:          purpose,
		},
	}
	approval, err := r.awsApprovals.create(conversationID, identity, plan)
	if err != nil {
		return nil, err
	}
	return awsApprovalRequiredResponse(approval), nil
}

func (r *Runtime) prepareAWSTerminateApproval(
	ctx context.Context,
	credentials awsCredentials,
	conversationID string,
	args map[string]any,
) (map[string]any, error) {
	instanceID := strings.TrimSpace(trimString(args["instance_id"]))
	if !awsInstanceIDPattern.MatchString(instanceID) {
		return nil, fmt.Errorf("a valid EC2 instance_id is required")
	}
	client, err := r.newAWSClient(ctx, credentials)
	if err != nil {
		return nil, err
	}
	identity, err := client.Identity(ctx)
	if err != nil {
		return nil, fmt.Errorf("read AWS identity before approval: %w", err)
	}
	instance, err := client.DescribeInstance(ctx, instanceID)
	if err != nil {
		return nil, fmt.Errorf("describe EC2 instance before approval: %w", err)
	}
	if !instance.Managed {
		return nil, fmt.Errorf("refusing to terminate an EC2 instance not managed by Dirextalk")
	}
	plan := awsApprovalPlan{
		Operation: "ec2.terminate",
		Region:    credentials.Region,
		Terminate: &awsTerminatePlan{
			InstanceID:   instance.InstanceID,
			InstanceType: instance.InstanceType,
			Name:         instance.Name,
		},
	}
	approval, err := r.awsApprovals.create(conversationID, identity, plan)
	if err != nil {
		return nil, err
	}
	return awsApprovalRequiredResponse(approval), nil
}

func (r *Runtime) executeAWSApproval(ctx context.Context, params map[string]any) (map[string]any, error) {
	approvalID := strings.TrimSpace(trimString(params["approval_id"]))
	conversationID := nativeAgentConversationKey(params)
	credentials := toolCredentialsFromParams(params).AWS
	if err := credentials.validate(); err != nil {
		return nil, err
	}
	approval, err := r.awsApprovals.begin(approvalID, conversationID)
	if err != nil {
		return nil, err
	}
	completed := false
	defer func() {
		if !completed {
			r.awsApprovals.release(approval.ID)
		}
	}()
	if credentials.Region != approval.Plan.Region {
		return nil, fmt.Errorf("AWS approval region does not match the configured region")
	}
	digest, err := awsApprovalPlanDigest(approval.Plan)
	if err != nil || digest != approval.Digest {
		return nil, fmt.Errorf("AWS approval plan integrity check failed")
	}
	client, err := r.newAWSClient(ctx, credentials)
	if err != nil {
		return nil, err
	}
	identity, err := client.Identity(ctx)
	if err != nil {
		return nil, fmt.Errorf("validate AWS identity for approval: %w", err)
	}
	if approval.AccountID == "" || strings.TrimSpace(identity.AccountID) != approval.AccountID {
		return nil, fmt.Errorf("AWS approval belongs to a different AWS account")
	}
	var instance AWSInstance
	switch approval.Plan.Operation {
	case "ec2.create":
		if approval.Plan.Create == nil {
			return nil, fmt.Errorf("AWS create approval plan is invalid")
		}
		instance, err = client.CreateInstance(ctx, *approval.Plan.Create, approval.ID)
	case "ec2.terminate":
		if approval.Plan.Terminate == nil {
			return nil, fmt.Errorf("AWS terminate approval plan is invalid")
		}
		current, describeErr := client.DescribeInstance(ctx, approval.Plan.Terminate.InstanceID)
		if describeErr != nil {
			return nil, fmt.Errorf("verify EC2 instance before termination: %w", describeErr)
		}
		if !current.Managed {
			return nil, fmt.Errorf("refusing to terminate an EC2 instance not managed by Dirextalk")
		}
		instance, err = client.TerminateInstance(ctx, approval.Plan.Terminate.InstanceID)
	default:
		return nil, fmt.Errorf("AWS approval operation is not supported")
	}
	if err != nil {
		return nil, fmt.Errorf("execute AWS approval: %w", err)
	}
	r.awsApprovals.complete(approval.ID)
	completed = true
	return map[string]any{
		"ok":          true,
		"approval_id": approval.ID,
		"operation":   approval.Plan.Operation,
		"region":      approval.Plan.Region,
		"instance":    awsInstanceResponse(instance),
	}, nil
}

func awsApprovalPlanDigest(plan awsApprovalPlan) (string, error) {
	data, err := json.Marshal(plan)
	if err != nil {
		return "", fmt.Errorf("encode AWS approval plan: %w", err)
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

func (r *Runtime) cancelAWSApproval(params map[string]any) (map[string]any, error) {
	approvalID := strings.TrimSpace(trimString(params["approval_id"]))
	conversationID := nativeAgentConversationKey(params)
	if err := r.awsApprovals.cancel(approvalID, conversationID); err != nil {
		return nil, err
	}
	return map[string]any{
		"ok":          true,
		"approval_id": approvalID,
		"status":      "cancelled",
	}, nil
}

func (r *Runtime) newAWSClient(ctx context.Context, credentials awsCredentials) (AWSClient, error) {
	if err := credentials.validate(); err != nil {
		return nil, err
	}
	factory := r.awsClientFactory
	if factory == nil {
		factory = newAWSClient
	}
	publicCredentials := AWSClientCredentials{
		AccessKeyID:     credentials.AccessKeyID,
		SecretAccessKey: credentials.SecretAccessKey,
		SessionToken:    credentials.SessionToken,
		Region:          credentials.Region,
	}
	client, err := factory(ctx, publicCredentials, r.client)
	if err != nil {
		return nil, redactAWSError(err, publicCredentials)
	}
	return &redactingAWSClient{
		inner:       client,
		credentials: publicCredentials,
	}, nil
}

type redactingAWSClient struct {
	inner       AWSClient
	credentials AWSClientCredentials
}

func (c *redactingAWSClient) Identity(ctx context.Context) (AWSIdentity, error) {
	result, err := c.inner.Identity(ctx)
	return result, redactAWSError(err, c.credentials)
}

func (c *redactingAWSClient) ListInstances(ctx context.Context) ([]AWSInstance, error) {
	result, err := c.inner.ListInstances(ctx)
	return result, redactAWSError(err, c.credentials)
}

func (c *redactingAWSClient) ResolveImage(ctx context.Context, alias string) (string, error) {
	result, err := c.inner.ResolveImage(ctx, alias)
	return result, redactAWSError(err, c.credentials)
}

func (c *redactingAWSClient) DescribeInstance(ctx context.Context, instanceID string) (AWSInstance, error) {
	result, err := c.inner.DescribeInstance(ctx, instanceID)
	return result, redactAWSError(err, c.credentials)
}

func (c *redactingAWSClient) CreateInstance(ctx context.Context, input AWSCreateInstanceInput, token string) (AWSInstance, error) {
	result, err := c.inner.CreateInstance(ctx, input, token)
	return result, redactAWSError(err, c.credentials)
}

func (c *redactingAWSClient) TerminateInstance(ctx context.Context, instanceID string) (AWSInstance, error) {
	result, err := c.inner.TerminateInstance(ctx, instanceID)
	return result, redactAWSError(err, c.credentials)
}

func redactAWSError(err error, credentials AWSClientCredentials) error {
	if err == nil {
		return nil
	}
	message := err.Error()
	for _, secret := range []string{
		credentials.AccessKeyID,
		credentials.SecretAccessKey,
		credentials.SessionToken,
	} {
		if secret != "" {
			message = strings.ReplaceAll(message, secret, "<redacted>")
		}
	}
	return fmt.Errorf("%s", message)
}

func awsApprovalRequiredResponse(approval pendingAWSApproval) map[string]any {
	operationLabel := "Create EC2 instance"
	summary := ""
	plan := map[string]any{
		"region": approval.Plan.Region,
	}
	if approval.Plan.Create != nil {
		create := approval.Plan.Create
		plan["instance_type"] = create.InstanceType
		plan["image_id"] = create.ImageID
		if create.SubnetID != "" {
			plan["subnet_id"] = create.SubnetID
		}
		if len(create.SecurityGroupIDs) > 0 {
			plan["security_group_ids"] = append([]string(nil), create.SecurityGroupIDs...)
		}
		if create.KeyName != "" {
			plan["key_name"] = create.KeyName
		}
		if create.VolumeSizeGB > 0 {
			plan["volume_size_gb"] = create.VolumeSizeGB
		}
		if create.Purpose != "" {
			plan["purpose"] = create.Purpose
		}
		summary = fmt.Sprintf("Create one %s EC2 instance in %s", create.InstanceType, approval.Plan.Region)
	} else if approval.Plan.Terminate != nil {
		operationLabel = "Terminate EC2 instance"
		terminate := approval.Plan.Terminate
		plan["instance_id"] = terminate.InstanceID
		if terminate.InstanceType != "" {
			plan["instance_type"] = terminate.InstanceType
		}
		if terminate.Name != "" {
			plan["name"] = terminate.Name
		}
		summary = fmt.Sprintf("Terminate %s in %s", terminate.InstanceID, approval.Plan.Region)
	}
	return map[string]any{
		"status": "confirmation_required",
		"approval": map[string]any{
			"id":          approval.ID,
			"kind":        "aws",
			"operation":   approval.Plan.Operation,
			"title":       operationLabel,
			"summary":     summary,
			"account_id":  approval.AccountID,
			"account_arn": approval.AccountARN,
			"expires_at":  approval.ExpiresAt.Format(time.RFC3339),
			"plan":        plan,
		},
	}
}

func awsIdentityResponse(region string, identity AWSIdentity) map[string]any {
	return map[string]any{
		"account_id": strings.TrimSpace(identity.AccountID),
		"arn":        strings.TrimSpace(identity.ARN),
		"user_id":    strings.TrimSpace(identity.UserID),
		"region":     strings.TrimSpace(region),
	}
}

func awsInstanceResponses(instances []AWSInstance) []map[string]any {
	out := make([]map[string]any, 0, len(instances))
	for _, instance := range instances {
		out = append(out, awsInstanceResponse(instance))
	}
	return out
}

func awsInstanceResponse(instance AWSInstance) map[string]any {
	return map[string]any{
		"instance_id":        strings.TrimSpace(instance.InstanceID),
		"instance_type":      strings.TrimSpace(instance.InstanceType),
		"image_id":           strings.TrimSpace(instance.ImageID),
		"state":              strings.TrimSpace(instance.State),
		"availability_zone":  strings.TrimSpace(instance.AvailabilityZone),
		"public_ip_address":  strings.TrimSpace(instance.PublicIPAddress),
		"private_ip_address": strings.TrimSpace(instance.PrivateIPAddress),
		"launch_time":        strings.TrimSpace(instance.LaunchTime),
		"name":               strings.TrimSpace(instance.Name),
		"managed":            instance.Managed,
	}
}
