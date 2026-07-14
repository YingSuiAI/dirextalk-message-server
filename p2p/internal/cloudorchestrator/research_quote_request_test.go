package cloudorchestrator_test

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
)

func TestResearchDraftV1IsRestrictedToNonPriceQuoteCandidates(t *testing.T) {
	draft := validResearchDraft()
	if err := draft.Validate(); err != nil {
		t.Fatalf("ResearchDraftV1.Validate() error = %v", err)
	}

	encoded, err := json.Marshal(draft)
	if err != nil {
		t.Fatalf("json.Marshal(ResearchDraftV1) error = %v", err)
	}
	var document map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &document); err != nil {
		t.Fatalf("json.Unmarshal(ResearchDraftV1) error = %v", err)
	}
	if len(document) != 3 {
		t.Fatalf("research draft fields = %v, want only schema_version, region, candidates", document)
	}
	for _, forbidden := range []string{"hourly_minor", "thirty_day_minor", "startup_upper_minor", "plan_hash", "approval"} {
		if _, found := document[forbidden]; found {
			t.Fatalf("research draft leaked forbidden %q field: %s", forbidden, encoded)
		}
	}
	for _, field := range []string{"HourlyMinor", "ThirtyDayMinor", "StartupUpperMinor", "PlanHash", "Approval"} {
		if _, found := reflect.TypeOf(cloudorchestrator.QuoteRequestCandidateV1{}).FieldByName(field); found {
			t.Fatalf("quote request candidate unexpectedly exposes %q", field)
		}
		if _, found := reflect.TypeOf(cloudorchestrator.ResearchDraftV1{}).FieldByName(field); found {
			t.Fatalf("research draft unexpectedly exposes %q", field)
		}
	}

	reordered := draft
	reordered.Candidates = append([]cloudorchestrator.QuoteRequestCandidateV1(nil), draft.Candidates...)
	reordered.Candidates[0], reordered.Candidates[1] = reordered.Candidates[1], reordered.Candidates[0]
	first, err := draft.CanonicalResearchDraftCBOR()
	if err != nil {
		t.Fatalf("CanonicalResearchDraftCBOR() error = %v", err)
	}
	second, err := reordered.CanonicalResearchDraftCBOR()
	if err != nil {
		t.Fatalf("reordered CanonicalResearchDraftCBOR() error = %v", err)
	}
	if !bytes.Equal(first, second) {
		t.Fatal("research draft candidate order changed deterministic CBOR")
	}
	firstDigest, err := draft.Digest()
	if err != nil {
		t.Fatalf("ResearchDraftV1.Digest() error = %v", err)
	}
	secondDigest, err := reordered.Digest()
	if err != nil {
		t.Fatalf("reordered ResearchDraftV1.Digest() error = %v", err)
	}
	if firstDigest != secondDigest {
		t.Fatalf("research draft candidate order changed digest: %q != %q", firstDigest, secondDigest)
	}
}

func TestQuoteRequestV1RejectsSpotBeforeBrokerDispatch(t *testing.T) {
	request := validQuoteRequest(t)
	request.Candidates[0].PurchaseOption = cloudorchestrator.PurchaseSpot
	if err := request.Validate(); err == nil || !strings.Contains(err.Error(), "on_demand") {
		t.Fatalf("QuoteRequestV1.Validate() error = %v, want on-demand-only rejection", err)
	}
}

func TestResearchDraftAndQuoteRequestRejectInvalidAWSRegionBeforeBrokerDispatch(t *testing.T) {
	draft := validResearchDraft()
	draft.Region = "not-an-aws-region"
	if err := draft.Validate(); err == nil || !strings.Contains(err.Error(), "AWS region") {
		t.Fatalf("ResearchDraftV1.Validate() error = %v, want AWS region rejection", err)
	}
	request := validQuoteRequest(t)
	request.Region = draft.Region
	if err := request.Validate(); err == nil || !strings.Contains(err.Error(), "AWS region") {
		t.Fatalf("QuoteRequestV1.Validate() error = %v, want AWS region rejection", err)
	}
}

func TestQuoteRequestV1DigestBindsPreQuoteInputsNotPlanHash(t *testing.T) {
	now := time.Date(2026, time.July, 14, 10, 0, 0, 0, time.UTC)
	plan := validPlan(t, now)
	request := validQuoteRequestFromPlan(plan)

	baseline, err := request.Digest()
	if err != nil {
		t.Fatalf("QuoteRequestV1.Digest() error = %v", err)
	}
	reordered := request
	reordered.Candidates = append([]cloudorchestrator.QuoteRequestCandidateV1(nil), request.Candidates...)
	reordered.Candidates[0], reordered.Candidates[1] = reordered.Candidates[1], reordered.Candidates[0]
	reorderedDigest, err := reordered.Digest()
	if err != nil {
		t.Fatalf("reordered QuoteRequestV1.Digest() error = %v", err)
	}
	if baseline != reorderedDigest {
		t.Fatalf("candidate order changed quote request digest: %q != %q", baseline, reorderedDigest)
	}

	for _, test := range []struct {
		name   string
		mutate func(*cloudorchestrator.QuoteRequestV1)
	}{
		{name: "candidate", mutate: func(value *cloudorchestrator.QuoteRequestV1) {
			value.Candidates = append([]cloudorchestrator.QuoteRequestCandidateV1(nil), value.Candidates...)
			value.Candidates[0].InstanceType = "m7i.2xlarge"
		}},
		{name: "recipe", mutate: func(value *cloudorchestrator.QuoteRequestV1) {
			value.RecipeDigest = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
		}},
		{name: "connection", mutate: func(value *cloudorchestrator.QuoteRequestV1) {
			value.CloudConnectionID = "connection-2"
		}},
		{name: "revision", mutate: func(value *cloudorchestrator.QuoteRequestV1) {
			value.PlanRevision++
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			changed := request
			test.mutate(&changed)
			digest, err := changed.Digest()
			if err != nil {
				t.Fatalf("changed QuoteRequestV1.Digest() error = %v", err)
			}
			if digest == baseline {
				t.Fatalf("%s did not change quote request digest", test.name)
			}
		})
	}

	firstPlanHash, err := plan.Hash()
	if err != nil {
		t.Fatalf("PlanV1.Hash() error = %v", err)
	}
	plan.ResourceScope.DiskGiB++
	secondPlanHash, err := plan.Hash()
	if err != nil {
		t.Fatalf("changed PlanV1.Hash() error = %v", err)
	}
	if firstPlanHash == secondPlanHash {
		t.Fatal("plan fixture change did not change PlanV1.Hash()")
	}
	unchangedRequestDigest, err := request.Digest()
	if err != nil {
		t.Fatalf("QuoteRequestV1.Digest() after plan change error = %v", err)
	}
	if unchangedRequestDigest != baseline {
		t.Fatalf("quote request digest unexpectedly depended on PlanV1.Hash(): %q != %q", unchangedRequestDigest, baseline)
	}
}

func TestQuoteV1IncludedItemsAreValidatedAndCanonicalized(t *testing.T) {
	now := time.Date(2026, time.July, 14, 10, 0, 0, 0, time.UTC)
	quote := cloudorchestrator.QuoteV1{
		SchemaVersion:     cloudorchestrator.SchemaVersionV1,
		QuoteID:           "quote-1",
		CloudConnectionID: "connection-1",
		Region:            "us-east-1",
		Currency:          "USD",
		QuotedAt:          now,
		ValidUntil:        now.Add(15 * time.Minute),
		Candidates: []cloudorchestrator.QuoteCandidateV1{{
			CandidateID:       "recommended",
			Tier:              cloudorchestrator.QuoteTierRecommended,
			InstanceType:      "m7i.xlarge",
			PurchaseOption:    cloudorchestrator.PurchaseOnDemand,
			HourlyMinor:       2016,
			ThirtyDayMinor:    1451520,
			StartupUpperMinor: 0,
			EstimatedDiskGiB:  80,
		}},
		IncludedItems:   []string{"ec2_linux_ondemand", "instance_price"},
		UnincludedItems: []string{"data_transfer", "taxes"},
	}
	first, err := quote.Digest()
	if err != nil {
		t.Fatalf("QuoteV1.Digest() error = %v", err)
	}
	reordered := quote
	reordered.IncludedItems = []string{"instance_price", "ec2_linux_ondemand"}
	second, err := reordered.Digest()
	if err != nil {
		t.Fatalf("reordered QuoteV1.Digest() error = %v", err)
	}
	if first != second {
		t.Fatalf("included item order changed quote digest: %q != %q", first, second)
	}
	quote.UnincludedItems = append(quote.UnincludedItems, "ec2_linux_ondemand")
	if err := quote.Validate(); err == nil || !strings.Contains(err.Error(), "must not overlap") {
		t.Fatalf("QuoteV1.Validate() error = %v, want overlapping coverage rejection", err)
	}
}

func validResearchDraft() cloudorchestrator.ResearchDraftV1 {
	return cloudorchestrator.ResearchDraftV1{
		SchemaVersion: cloudorchestrator.SchemaVersionV1,
		Region:        "us-east-1",
		Candidates: []cloudorchestrator.QuoteRequestCandidateV1{
			{CandidateID: "economy-candidate", Tier: cloudorchestrator.QuoteTierEconomy, InstanceType: "m7i.large", PurchaseOption: cloudorchestrator.PurchaseOnDemand, EstimatedDiskGiB: 80},
			{CandidateID: "recommended-candidate", Tier: cloudorchestrator.QuoteTierRecommended, InstanceType: "m7i.xlarge", PurchaseOption: cloudorchestrator.PurchaseOnDemand, EstimatedDiskGiB: 80},
		},
	}
}

func validQuoteRequest(t *testing.T) cloudorchestrator.QuoteRequestV1 {
	t.Helper()
	return validQuoteRequestFromPlan(validPlan(t, time.Date(2026, time.July, 14, 10, 0, 0, 0, time.UTC)))
}

func validQuoteRequestFromPlan(plan cloudorchestrator.PlanV1) cloudorchestrator.QuoteRequestV1 {
	draft := validResearchDraft()
	return cloudorchestrator.QuoteRequestV1{
		SchemaVersion:     cloudorchestrator.SchemaVersionV1,
		QuoteRequestID:    "quote-request-1",
		PlanID:            plan.PlanID,
		PlanRevision:      plan.Revision,
		CloudConnectionID: plan.CloudConnectionID,
		RecipeDigest:      plan.Recipe.Digest,
		Region:            draft.Region,
		Candidates:        append([]cloudorchestrator.QuoteRequestCandidateV1(nil), draft.Candidates...),
	}
}
