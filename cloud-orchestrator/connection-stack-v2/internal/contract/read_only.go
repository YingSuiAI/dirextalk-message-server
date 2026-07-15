package contract

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"time"
)

const (
	ReceiptSchema      = "dirextalk.aws.command-receipt/v2"
	RegistrationSchema = "dirextalk.aws.connection-registration/v1"
	QuoteSchema        = "dirextalk.aws.quote/v1"
	QuoteValidity      = 15 * time.Minute
)

var (
	namedSHA256Pattern      = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	regionPattern           = regexp.MustCompile(`^(af|ap|ca|cn|eu|il|me|mx|sa|us)(-gov)?-[a-z]+-[0-9]$`)
	availabilityZonePattern = regexp.MustCompile(`^(af|ap|ca|cn|eu|il|me|mx|sa|us)(-gov)?-[a-z]+-[0-9][a-z]$`)
	instanceTypePattern     = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{1,63}$`)
	accountIDPattern        = regexp.MustCompile(`^[0-9]{12}$`)
	amiIDPattern            = regexp.MustCompile(`^ami-[0-9a-f]{8,17}$`)
	vpcIDPattern            = regexp.MustCompile(`^vpc-[0-9a-f]{8,17}$`)
	subnetIDPattern         = regexp.MustCompile(`^subnet-[0-9a-f]{8,17}$`)
	stackARNPattern         = regexp.MustCompile(`^arn:(?:aws|aws-cn|aws-us-gov):cloudformation:((?:af|ap|ca|cn|eu|il|me|mx|sa|us)(?:-gov)?-[a-z]+-[0-9]):([0-9]{12}):stack/[A-Za-z][-A-Za-z0-9]{0,127}/[A-Za-z0-9-]{8,128}$`)
	apiGatewayHostPattern   = regexp.MustCompile(`^[a-z0-9]{10}\.execute-api\.((?:af|ap|ca|cn|eu|il|me|mx|sa|us)(?:-gov)?-[a-z]+-[0-9])\.amazonaws\.com(?:\.cn)?$`)

	quoteIncludedItems   = []string{"ec2_linux_ondemand"}
	quoteUnincludedItems = []string{"cloudwatch_logs", "data_transfer", "ebs_gp3", "public_ipv4", "snapshots", "taxes"}
)

type RegistrationRequest struct {
	BootstrapID     string `json:"bootstrap_id"`
	RequestedRegion string `json:"requested_region"`
	StackARN        string `json:"stack_arn"`
}

type QuoteCandidate struct {
	CandidateID      string `json:"candidate_id"`
	Tier             string `json:"tier"`
	InstanceType     string `json:"instance_type"`
	PurchaseOption   string `json:"purchase_option"`
	EstimatedDiskGiB int64  `json:"estimated_disk_gib"`
}

type QuoteRequest struct {
	QuoteRequestID string           `json:"quote_request_id"`
	PlanDigest     string           `json:"plan_digest"`
	Region         string           `json:"region"`
	Candidates     []QuoteCandidate `json:"candidates"`
}

type WorkerArtifactReference struct {
	Kind  string `json:"kind"`
	AMIID string `json:"ami_id"`
}

type WorkerNetworkReference struct {
	VPCID            string `json:"vpc_id"`
	SubnetID         string `json:"subnet_id"`
	AvailabilityZone string `json:"availability_zone"`
}

type Registration struct {
	Schema                       string                  `json:"schema"`
	BootstrapID                  string                  `json:"bootstrap_id"`
	ConnectionID                 string                  `json:"connection_id"`
	AccountID                    string                  `json:"account_id"`
	Region                       string                  `json:"region"`
	BrokerCommandURL             string                  `json:"broker_command_url"`
	NodeKeyID                    string                  `json:"node_key_id"`
	ConnectionGeneration         int64                   `json:"connection_generation"`
	WorkerArtifact               WorkerArtifactReference `json:"worker_artifact"`
	WorkerNetwork                WorkerNetworkReference  `json:"worker_network"`
	WorkerResourceManifestDigest string                  `json:"worker_resource_manifest_digest"`
	StackARN                     string                  `json:"stack_arn"`
	CommandID                    string                  `json:"command_id"`
	RequestSHA256                string                  `json:"request_sha256"`
}

type QuotedCandidate struct {
	CandidateID       string   `json:"candidate_id"`
	Tier              string   `json:"tier"`
	InstanceType      string   `json:"instance_type"`
	PurchaseOption    string   `json:"purchase_option"`
	EstimatedDiskGiB  int64    `json:"estimated_disk_gib"`
	Architecture      string   `json:"architecture"`
	VCPU              int64    `json:"vcpu"`
	MemoryMiB         int64    `json:"memory_mib"`
	GPUCount          int64    `json:"gpu_count"`
	GPUMemoryMiB      int64    `json:"gpu_memory_mib"`
	HourlyMinor       int64    `json:"hourly_minor"`
	ThirtyDayMinor    int64    `json:"thirty_day_minor"`
	StartupUpperMinor int64    `json:"startup_upper_minor"`
	AvailabilityZones []string `json:"availability_zones"`
}

type Quote struct {
	Schema          string            `json:"schema"`
	QuoteID         string            `json:"quote_id"`
	ConnectionID    string            `json:"connection_id"`
	CommandID       string            `json:"command_id"`
	RequestSHA256   string            `json:"request_sha256"`
	QuoteRequestID  string            `json:"quote_request_id"`
	PlanDigest      string            `json:"plan_digest"`
	Region          string            `json:"region"`
	Currency        string            `json:"currency"`
	QuotedAt        string            `json:"quoted_at"`
	ValidUntil      string            `json:"valid_until"`
	Candidates      []QuotedCandidate `json:"candidates"`
	IncludedItems   []string          `json:"included_items"`
	UnincludedItems []string          `json:"unincluded_items"`
}

type QuoteReceipt struct {
	Schema             string `json:"schema"`
	Disposition        string `json:"disposition"`
	ConnectionID       string `json:"connection_id"`
	ExpectedGeneration int64  `json:"expected_generation"`
	NodeCounter        int64  `json:"node_counter"`
	CommandID          string `json:"command_id"`
	RequestSHA256      string `json:"request_sha256"`
	Action             string `json:"action"`
	Quote              *Quote `json:"quote"`
}

type RegistrationReceipt struct {
	Schema             string `json:"schema"`
	Disposition        string `json:"disposition"`
	ConnectionID       string `json:"connection_id"`
	ExpectedGeneration int64  `json:"expected_generation"`
	NodeCounter        int64  `json:"node_counter"`
	CommandID          string `json:"command_id"`
	RequestSHA256      string `json:"request_sha256"`
	Action             string `json:"action"`
}

type QuoteResult struct {
	Status  string       `json:"status"`
	Receipt QuoteReceipt `json:"receipt"`
	Quote   Quote        `json:"quote"`
}

type RegistrationResult struct {
	Status       string              `json:"status"`
	Receipt      RegistrationReceipt `json:"receipt"`
	Registration Registration        `json:"registration"`
}

func (c Command) RegistrationRequest() (RegistrationRequest, error) {
	if c.Action != ActionRegistrationVerify {
		return RegistrationRequest{}, errCode("invalid_registration_request")
	}
	payload, err := c.actionPayload()
	if err != nil {
		return RegistrationRequest{}, err
	}
	if fields, err := exactJSONObject(payload); err != nil || !exactFields(fields, []string{"bootstrap_id", "requested_region", "stack_arn"}) {
		return RegistrationRequest{}, errCode("invalid_registration_request")
	}
	var request RegistrationRequest
	if err := decodeSingle(payload, &request); err != nil || !ValidID(request.BootstrapID) || !regionPattern.MatchString(request.RequestedRegion) {
		return RegistrationRequest{}, errCode("invalid_registration_request")
	}
	stackRegion, _, ok := ParseStackARN(request.StackARN)
	if !ok || stackRegion != request.RequestedRegion {
		return RegistrationRequest{}, errCode("invalid_registration_request")
	}
	canonical, _ := json.Marshal(request)
	if !bytes.Equal(canonical, payload) {
		return RegistrationRequest{}, errCode("noncanonical_payload")
	}
	return request, nil
}

func (c Command) QuoteRequest() (QuoteRequest, error) {
	if c.Action != ActionQuoteRequest {
		return QuoteRequest{}, errCode("invalid_quote_request")
	}
	payload, err := c.actionPayload()
	if err != nil {
		return QuoteRequest{}, err
	}
	fields, err := exactJSONObject(payload)
	if err != nil || !exactFields(fields, []string{"quote_request_id", "plan_digest", "region", "candidates"}) {
		return QuoteRequest{}, errCode("invalid_quote_request")
	}
	var rawCandidates []json.RawMessage
	if err := decodeSingle(fields["candidates"], &rawCandidates); err != nil {
		return QuoteRequest{}, errCode("invalid_quote_request")
	}
	for _, rawCandidate := range rawCandidates {
		candidateFields, candidateErr := exactJSONObject(rawCandidate)
		if candidateErr != nil || !exactFields(candidateFields, []string{"candidate_id", "tier", "instance_type", "purchase_option", "estimated_disk_gib"}) {
			return QuoteRequest{}, errCode("invalid_quote_request")
		}
	}
	var request QuoteRequest
	if err := decodeSingle(payload, &request); err != nil || validateQuoteRequest(request) != nil {
		return QuoteRequest{}, errCode("invalid_quote_request")
	}
	canonical, _ := json.Marshal(request)
	if !bytes.Equal(canonical, payload) {
		return QuoteRequest{}, errCode("noncanonical_payload")
	}
	return request, nil
}

func (c Command) actionPayload() ([]byte, error) {
	if err := c.ValidateStructure(); err != nil {
		return nil, err
	}
	payload, err := base64.StdEncoding.DecodeString(c.PayloadB64)
	if err != nil {
		return nil, errCode("invalid_payload")
	}
	return payload, nil
}

func validateQuoteRequest(request QuoteRequest) error {
	if !ValidID(request.QuoteRequestID) || !namedSHA256Pattern.MatchString(request.PlanDigest) || !regionPattern.MatchString(request.Region) || len(request.Candidates) < 1 || len(request.Candidates) > 3 {
		return errCode("invalid_quote_request")
	}
	ids, tiers := map[string]struct{}{}, map[string]struct{}{}
	for _, candidate := range request.Candidates {
		if !ValidID(candidate.CandidateID) || !validTier(candidate.Tier) || !instanceTypePattern.MatchString(candidate.InstanceType) || candidate.PurchaseOption != "on_demand" || candidate.EstimatedDiskGiB < 8 || candidate.EstimatedDiskGiB > 16384 {
			return errCode("invalid_quote_request")
		}
		if _, ok := ids[candidate.CandidateID]; ok {
			return errCode("invalid_quote_request")
		}
		if _, ok := tiers[candidate.Tier]; ok {
			return errCode("invalid_quote_request")
		}
		ids[candidate.CandidateID], tiers[candidate.Tier] = struct{}{}, struct{}{}
	}
	return nil
}

func ValidateRegistrationResult(command Command, request RegistrationRequest, result RegistrationResult) error {
	requestSHA, err := command.RequestSHA256()
	if err != nil {
		return err
	}
	if !validStatusDisposition(result.Status, result.Receipt.Disposition, "connection_registered") || result.Receipt.Schema != ReceiptSchema || result.Receipt.ConnectionID != command.ConnectionID || result.Receipt.ExpectedGeneration != command.ExpectedGeneration || result.Receipt.NodeCounter != command.NodeCounter || result.Receipt.CommandID != command.CommandID || result.Receipt.RequestSHA256 != requestSHA || result.Receipt.Action != ActionRegistrationVerify {
		return errCode("invalid_registration_receipt")
	}
	r := result.Registration
	stackRegion, stackAccount, stackOK := ParseStackARN(r.StackARN)
	endpointRegion, endpointOK := BrokerEndpointRegion(r.BrokerCommandURL)
	if r.Schema != RegistrationSchema || r.BootstrapID != request.BootstrapID || r.ConnectionID != command.ConnectionID || !accountIDPattern.MatchString(r.AccountID) || r.Region != request.RequestedRegion || r.NodeKeyID != command.NodeKeyID || r.ConnectionGeneration != command.ExpectedGeneration || r.StackARN != request.StackARN || r.CommandID != command.CommandID || r.RequestSHA256 != requestSHA || !stackOK || stackRegion != r.Region || stackAccount != r.AccountID || !endpointOK || endpointRegion != r.Region || !ValidWorkerBindings(r.Region, r.WorkerArtifact, r.WorkerNetwork, r.WorkerResourceManifestDigest) {
		return errCode("invalid_registration")
	}
	return nil
}

func ValidateQuoteResult(command Command, request QuoteRequest, result QuoteResult) error {
	requestSHA, err := command.RequestSHA256()
	if err != nil {
		return err
	}
	if !validStatusDisposition(result.Status, result.Receipt.Disposition, "quote_issued") || result.Receipt.Schema != ReceiptSchema || result.Receipt.ConnectionID != command.ConnectionID || result.Receipt.ExpectedGeneration != command.ExpectedGeneration || result.Receipt.NodeCounter != command.NodeCounter || result.Receipt.CommandID != command.CommandID || result.Receipt.RequestSHA256 != requestSHA || result.Receipt.Action != ActionQuoteRequest || result.Receipt.Quote == nil {
		return errCode("invalid_quote_receipt")
	}
	if err := validateQuote(command, request, result.Quote); err != nil {
		return err
	}
	left, _ := json.Marshal(result.Quote)
	right, _ := json.Marshal(result.Receipt.Quote)
	if !bytes.Equal(left, right) {
		return errCode("quote_receipt_mismatch")
	}
	return nil
}

func validateQuote(command Command, request QuoteRequest, quote Quote) error {
	requestSHA, _ := command.RequestSHA256()
	if quote.Schema != QuoteSchema || quote.QuoteID != "quote-"+requestSHA[:32] || quote.ConnectionID != command.ConnectionID || quote.CommandID != command.CommandID || quote.RequestSHA256 != requestSHA || quote.QuoteRequestID != request.QuoteRequestID || quote.PlanDigest != request.PlanDigest || quote.Region != request.Region || quote.Currency != "USD" || len(quote.Candidates) != len(request.Candidates) || !sameStrings(quote.IncludedItems, quoteIncludedItems) || !sameStrings(quote.UnincludedItems, quoteUnincludedItems) {
		return errCode("invalid_quote")
	}
	quotedAt, err := parseReadOnlyInstant(quote.QuotedAt)
	if err != nil {
		return errCode("invalid_quote")
	}
	validUntil, err := parseReadOnlyInstant(quote.ValidUntil)
	issuedAt, _ := parseReadOnlyInstant(command.IssuedAt)
	expiresAt, _ := parseReadOnlyInstant(command.ExpiresAt)
	if err != nil || validUntil.Sub(quotedAt) != QuoteValidity || quotedAt.Before(issuedAt) || quotedAt.After(expiresAt) {
		return errCode("invalid_quote")
	}
	for i, candidate := range quote.Candidates {
		requested := request.Candidates[i]
		if candidate.CandidateID != requested.CandidateID || candidate.Tier != requested.Tier || candidate.InstanceType != requested.InstanceType || candidate.PurchaseOption != requested.PurchaseOption || candidate.EstimatedDiskGiB != requested.EstimatedDiskGiB || (candidate.Architecture != "amd64" && candidate.Architecture != "arm64") || candidate.VCPU < 1 || candidate.VCPU > 65535 || candidate.MemoryMiB < 1 || candidate.MemoryMiB > 4294967295 || candidate.GPUCount < 0 || candidate.GPUCount > 65535 || candidate.GPUMemoryMiB < 0 || candidate.GPUMemoryMiB > 4294967295 || (candidate.GPUCount == 0) != (candidate.GPUMemoryMiB == 0) || candidate.HourlyMinor < 0 || candidate.ThirtyDayMinor < 0 || candidate.StartupUpperMinor < 0 || !canonicalZones(candidate.AvailabilityZones, request.Region) {
			return errCode("invalid_quote")
		}
	}
	return nil
}

func validStatusDisposition(status, disposition, committedStatus string) bool {
	return (status == committedStatus && disposition == "committed") || (status == "idempotent" && disposition == "idempotent")
}

func validTier(value string) bool {
	return value == "economy" || value == "recommended" || value == "performance"
}

func canonicalZones(values []string, region string) bool {
	if len(values) == 0 || !sort.StringsAreSorted(values) {
		return false
	}
	for i, value := range values {
		if !ValidAvailabilityZone(region, value) || (i > 0 && values[i-1] == value) {
			return false
		}
	}
	return true
}

func ValidAvailabilityZone(region, zone string) bool {
	return regionPattern.MatchString(region) && availabilityZonePattern.MatchString(zone) && len(zone) == len(region)+1 && strings.HasPrefix(zone, region)
}

func ValidWorkerBindings(region string, artifact WorkerArtifactReference, network WorkerNetworkReference, manifestDigest string) bool {
	return artifact.Kind == "fixed_ami" && amiIDPattern.MatchString(artifact.AMIID) && vpcIDPattern.MatchString(network.VPCID) && subnetIDPattern.MatchString(network.SubnetID) && ValidAvailabilityZone(region, network.AvailabilityZone) && namedSHA256Pattern.MatchString(manifestDigest)
}

func sameStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func parseReadOnlyInstant(value string) (time.Time, error) {
	if !canonicalInstantPattern.MatchString(value) {
		return time.Time{}, errCode("invalid_instant")
	}
	return time.Parse(canonicalInstantLayout, value)
}

func ParseStackARN(value string) (region, accountID string, ok bool) {
	matches := stackARNPattern.FindStringSubmatch(value)
	if len(matches) != 3 {
		return "", "", false
	}
	return matches[1], matches[2], true
}

func BrokerEndpointRegion(value string) (string, bool) {
	parsed, err := url.ParseRequestURI(value)
	if err != nil || parsed.Scheme != "https" || parsed.User != nil || parsed.Port() != "" || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.RawPath != "" || parsed.Path != "/prod/v2/commands" {
		return "", false
	}
	matches := apiGatewayHostPattern.FindStringSubmatch(parsed.Hostname())
	if len(matches) != 2 {
		return "", false
	}
	return matches[1], true
}

func ValidID(value string) bool { return idPattern.MatchString(value) }

func MarshalCommittedRegistrationResult(command Command, registration Registration) ([]byte, error) {
	request, err := command.RegistrationRequest()
	if err != nil {
		return nil, err
	}
	requestSHA, _ := command.RequestSHA256()
	result := RegistrationResult{Status: "connection_registered", Receipt: RegistrationReceipt{
		Schema: ReceiptSchema, Disposition: "committed", ConnectionID: command.ConnectionID, ExpectedGeneration: command.ExpectedGeneration,
		NodeCounter: command.NodeCounter, CommandID: command.CommandID, RequestSHA256: requestSHA, Action: ActionRegistrationVerify,
	}, Registration: registration}
	if err := ValidateRegistrationResult(command, request, result); err != nil {
		return nil, err
	}
	return json.Marshal(result)
}

func MarshalCommittedQuoteResult(command Command, quote Quote) ([]byte, error) {
	request, err := command.QuoteRequest()
	if err != nil {
		return nil, err
	}
	requestSHA, _ := command.RequestSHA256()
	result := QuoteResult{Status: "quote_issued", Receipt: QuoteReceipt{
		Schema: ReceiptSchema, Disposition: "committed", ConnectionID: command.ConnectionID, ExpectedGeneration: command.ExpectedGeneration,
		NodeCounter: command.NodeCounter, CommandID: command.CommandID, RequestSHA256: requestSHA, Action: ActionQuoteRequest, Quote: &quote,
	}, Quote: quote}
	if err := ValidateQuoteResult(command, request, result); err != nil {
		return nil, err
	}
	return json.Marshal(result)
}

func IdempotentResult(command Command, raw []byte) ([]byte, error) {
	switch command.Action {
	case ActionRegistrationVerify:
		var result RegistrationResult
		if err := decodeRegistrationResult(raw, &result); err != nil || result.Status != "connection_registered" || result.Receipt.Disposition != "committed" {
			return nil, errCode("receipt_store_invalid")
		}
		request, err := command.RegistrationRequest()
		if err != nil || ValidateRegistrationResult(command, request, result) != nil {
			return nil, errCode("receipt_store_invalid")
		}
		result.Status, result.Receipt.Disposition = "idempotent", "idempotent"
		return json.Marshal(result)
	case ActionQuoteRequest:
		var result QuoteResult
		if err := decodeQuoteResult(raw, &result); err != nil || result.Status != "quote_issued" || result.Receipt.Disposition != "committed" {
			return nil, errCode("receipt_store_invalid")
		}
		request, err := command.QuoteRequest()
		if err != nil || ValidateQuoteResult(command, request, result) != nil {
			return nil, errCode("receipt_store_invalid")
		}
		result.Status, result.Receipt.Disposition = "idempotent", "idempotent"
		return json.Marshal(result)
	case ActionServiceRestorePlan:
		var result ServiceRestorePlanResult
		if err := decodeServiceRestorePlanResult(raw, &result); err != nil || result.Status != "restore_plan_ready" || result.Receipt.Disposition != "committed" || ValidateServiceRestorePlanResult(command, result) != nil {
			return nil, errCode("receipt_store_invalid")
		}
		result.Status, result.Receipt.Disposition = "idempotent", "idempotent"
		return json.Marshal(result)
	default:
		return nil, errCode("operation_not_enabled")
	}
}

func ValidateCommittedResult(command Command, raw []byte) error {
	switch command.Action {
	case ActionRegistrationVerify:
		var result RegistrationResult
		if err := decodeRegistrationResult(raw, &result); err != nil || result.Status != "connection_registered" || result.Receipt.Disposition != "committed" {
			return errCode("receipt_store_invalid")
		}
		request, err := command.RegistrationRequest()
		if err != nil || ValidateRegistrationResult(command, request, result) != nil {
			return errCode("receipt_store_invalid")
		}
		return nil
	case ActionQuoteRequest:
		var result QuoteResult
		if err := decodeQuoteResult(raw, &result); err != nil || result.Status != "quote_issued" || result.Receipt.Disposition != "committed" {
			return errCode("receipt_store_invalid")
		}
		request, err := command.QuoteRequest()
		if err != nil || ValidateQuoteResult(command, request, result) != nil {
			return errCode("receipt_store_invalid")
		}
		return nil
	case ActionServiceRestorePlan:
		var result ServiceRestorePlanResult
		if err := decodeServiceRestorePlanResult(raw, &result); err != nil || result.Status != "restore_plan_ready" || result.Receipt.Disposition != "committed" || ValidateServiceRestorePlanResult(command, result) != nil {
			return errCode("receipt_store_invalid")
		}
		return nil
	default:
		return errCode("operation_not_enabled")
	}
}

func decodeRegistrationResult(raw []byte, result *RegistrationResult) error {
	object, err := exactJSONObject(raw)
	if err != nil || !exactFields(object, []string{"status", "receipt", "registration"}) {
		return errCode("receipt_store_invalid")
	}
	receipt, err := exactJSONObject(object["receipt"])
	if err != nil || !exactFields(receipt, []string{"schema", "disposition", "connection_id", "expected_generation", "node_counter", "command_id", "request_sha256", "action"}) {
		return errCode("receipt_store_invalid")
	}
	registration, err := exactJSONObject(object["registration"])
	if err != nil || !exactFields(registration, []string{"schema", "bootstrap_id", "connection_id", "account_id", "region", "broker_command_url", "node_key_id", "connection_generation", "worker_artifact", "worker_network", "worker_resource_manifest_digest", "stack_arn", "command_id", "request_sha256"}) {
		return errCode("receipt_store_invalid")
	}
	workerArtifact, err := exactJSONObject(registration["worker_artifact"])
	if err != nil || !exactFields(workerArtifact, []string{"kind", "ami_id"}) {
		return errCode("receipt_store_invalid")
	}
	workerNetwork, err := exactJSONObject(registration["worker_network"])
	if err != nil || !exactFields(workerNetwork, []string{"vpc_id", "subnet_id", "availability_zone"}) {
		return errCode("receipt_store_invalid")
	}
	if err := decodeSingle(raw, result); err != nil {
		return errCode("receipt_store_invalid")
	}
	return nil
}

func decodeQuoteResult(raw []byte, result *QuoteResult) error {
	object, err := exactJSONObject(raw)
	if err != nil || !exactFields(object, []string{"status", "receipt", "quote"}) {
		return errCode("receipt_store_invalid")
	}
	receipt, err := exactJSONObject(object["receipt"])
	if err != nil || !exactFields(receipt, []string{"schema", "disposition", "connection_id", "expected_generation", "node_counter", "command_id", "request_sha256", "action", "quote"}) {
		return errCode("receipt_store_invalid")
	}
	if err := validateQuoteShape(receipt["quote"]); err != nil {
		return err
	}
	if err := validateQuoteShape(object["quote"]); err != nil {
		return err
	}
	if err := decodeSingle(raw, result); err != nil {
		return errCode("receipt_store_invalid")
	}
	return nil
}

func validateQuoteShape(raw []byte) error {
	quote, err := exactJSONObject(raw)
	if err != nil || !exactFields(quote, []string{"schema", "quote_id", "connection_id", "command_id", "request_sha256", "quote_request_id", "plan_digest", "region", "currency", "quoted_at", "valid_until", "candidates", "included_items", "unincluded_items"}) {
		return errCode("receipt_store_invalid")
	}
	var candidates []json.RawMessage
	if err := decodeSingle(quote["candidates"], &candidates); err != nil {
		return errCode("receipt_store_invalid")
	}
	for _, rawCandidate := range candidates {
		candidate, candidateErr := exactJSONObject(rawCandidate)
		if candidateErr != nil || !exactFields(candidate, []string{"candidate_id", "tier", "instance_type", "purchase_option", "estimated_disk_gib", "architecture", "vcpu", "memory_mib", "gpu_count", "gpu_memory_mib", "hourly_minor", "thirty_day_minor", "startup_upper_minor", "availability_zones"}) {
			return errCode("receipt_store_invalid")
		}
	}
	return nil
}

// ParseStoredQuote decodes only the exact de-secretsed quote shape persisted
// by the Stack. Callers must still bind its identity and candidate to the
// deployment command and ApprovalV1 before mutation.
func ParseStoredQuote(raw []byte) (Quote, error) {
	if err := validateQuoteShape(raw); err != nil {
		return Quote{}, errCode("issued_quote_invalid")
	}
	var quote Quote
	if err := decodeSingle(raw, &quote); err != nil {
		return Quote{}, errCode("issued_quote_invalid")
	}
	return quote, nil
}

// ApprovalDigest reproduces QuoteV1.Digest from the Orchestrator contract.
// The Broker quote carries additional transport bindings, so this projection
// deliberately hashes only the immutable approval price estimate.
func (q Quote) ApprovalDigest() (string, error) {
	raw, err := json.Marshal(q)
	if err != nil || validateQuoteShape(raw) != nil {
		return "", errCode("issued_quote_invalid")
	}
	quotedAt, err := time.Parse(canonicalInstantLayout, q.QuotedAt)
	if err != nil {
		return "", errCode("issued_quote_invalid")
	}
	validUntil, err := time.Parse(canonicalInstantLayout, q.ValidUntil)
	if err != nil {
		return "", errCode("issued_quote_invalid")
	}
	document := approvalQuoteDocument{
		SchemaVersion: approvalSchemaVersion, QuoteID: q.QuoteID, CloudConnectionID: q.ConnectionID,
		Region: q.Region, Currency: q.Currency, QuotedAt: quotedAt.UTC(), ValidUntil: validUntil.UTC(),
		IncludedItems: canonicalQuoteSet(q.IncludedItems), UnincludedItems: canonicalQuoteSet(q.UnincludedItems),
	}
	document.Candidates = make([]approvalQuoteCandidate, len(q.Candidates))
	for i, candidate := range q.Candidates {
		document.Candidates[i] = approvalQuoteCandidate{
			CandidateID: candidate.CandidateID, Tier: candidate.Tier, InstanceType: candidate.InstanceType,
			PurchaseOption: candidate.PurchaseOption, Architecture: candidate.Architecture, VCPU: candidate.VCPU,
			MemoryMiB: candidate.MemoryMiB, GPUCount: candidate.GPUCount, GPUMemoryMiB: candidate.GPUMemoryMiB,
			HourlyMinor: candidate.HourlyMinor, ThirtyDayMinor: candidate.ThirtyDayMinor,
			StartupUpperMinor: candidate.StartupUpperMinor, EstimatedDiskGiB: candidate.EstimatedDiskGiB,
			AvailabilityZones: canonicalQuoteSet(candidate.AvailabilityZones),
		}
	}
	sort.Slice(document.Candidates, func(i, j int) bool {
		if document.Candidates[i].Tier == document.Candidates[j].Tier {
			return document.Candidates[i].CandidateID < document.Candidates[j].CandidateID
		}
		return document.Candidates[i].Tier < document.Candidates[j].Tier
	})
	canonical, err := deterministicCBOR(document)
	if err != nil {
		return "", errCode("issued_quote_invalid")
	}
	digest := sha256.Sum256(canonical)
	return "sha256:" + hex.EncodeToString(digest[:]), nil
}

type approvalQuoteDocument struct {
	SchemaVersion     string                   `json:"schema_version"`
	QuoteID           string                   `json:"quote_id"`
	CloudConnectionID string                   `json:"cloud_connection_id"`
	Region            string                   `json:"region"`
	Currency          string                   `json:"currency"`
	QuotedAt          time.Time                `json:"quoted_at"`
	ValidUntil        time.Time                `json:"valid_until"`
	Candidates        []approvalQuoteCandidate `json:"candidates"`
	IncludedItems     []string                 `json:"included_items,omitempty"`
	UnincludedItems   []string                 `json:"unincluded_items,omitempty"`
}

type approvalQuoteCandidate struct {
	CandidateID       string   `json:"candidate_id"`
	Tier              string   `json:"tier"`
	InstanceType      string   `json:"instance_type"`
	PurchaseOption    string   `json:"purchase_option"`
	Architecture      string   `json:"architecture"`
	VCPU              int64    `json:"vcpu"`
	MemoryMiB         int64    `json:"memory_mib"`
	GPUCount          int64    `json:"gpu_count"`
	GPUMemoryMiB      int64    `json:"gpu_memory_mib"`
	HourlyMinor       int64    `json:"hourly_minor"`
	ThirtyDayMinor    int64    `json:"thirty_day_minor"`
	StartupUpperMinor int64    `json:"startup_upper_minor"`
	EstimatedDiskGiB  int64    `json:"estimated_disk_gib"`
	AvailabilityZones []string `json:"availability_zones,omitempty"`
}

func canonicalQuoteSet(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	result := append([]string(nil), values...)
	sort.Strings(result)
	return result
}
