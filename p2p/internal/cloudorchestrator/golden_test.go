package cloudorchestrator_test

import (
	"encoding/base64"
	"math"
	"testing"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
	"github.com/fxamacker/cbor/v2"
)

func TestV1DeterministicCBORPlanAndApprovalGoldenVectors(t *testing.T) {
	now := time.Date(2026, time.July, 14, 10, 0, 0, 0, time.UTC)
	plan := validPlan(t, now)
	canonicalPlan, err := plan.CanonicalPlanCBOR()
	if err != nil {
		t.Fatalf("CanonicalPlanCBOR() error = %v", err)
	}
	planHash, err := plan.Hash()
	if err != nil {
		t.Fatalf("Hash() error = %v", err)
	}
	approval, err := cloudorchestrator.NewApprovalV1(plan, "approval-1", "challenge-1", "owner-device-1", now.Add(10*time.Minute))
	if err != nil {
		t.Fatalf("NewApprovalV1() error = %v", err)
	}
	payload, err := approval.SigningPayload()
	if err != nil {
		t.Fatalf("SigningPayload() error = %v", err)
	}

	const wantPlanCBORBase64 = "q2VxdW90ZaRmZGlnZXN0eEdzaGEyNTY6ZGZkNzYxYWU1Mjc4NTE3ZmFkZWJkMzkwMjM5MWQzOTgxZWI3NDdiMjczNmYzMzA1MTdlNDUzNTIwNmIxZTcyNGhxdW90ZV9pZGdxdW90ZS0xa3ZhbGlkX3VudGlsdDIwMjYtMDctMTRUMTA6MTU6MDBabGNhbmRpZGF0ZV9pZGtyZWNvbW1lbmRlZGZyZWNpcGWjZmRpZ2VzdHhHc2hhMjU2OjQ2NTljODMzOTFhYTY3ZmZmMjY0ZTBiODQwYzM4M2ZlYjMyN2RkNzVkMDY2Zjg0NDU1ODg0MDJkZDcxYTk2MDZobWF0dXJpdHlsZXhwZXJpbWVudGFsaXJlY2lwZV9pZHdyZWNpcGUta25vd2xlZGdlLW5vZGUtMWdwbGFuX2lkZnBsYW4tMWhyZXZpc2lvbgdsc2VjcmV0X3Njb3BlgqNncHVycG9zZW1zb3VyY2UtYWNjZXNzaGRlbGl2ZXJ5ZGZpbGVqc2VjcmV0X3JlZnVzZWNyZXRfcmVmOmdpdGh1Yi1hcHCjZ3B1cnBvc2VsbW9kZWwtYWNjZXNzaGRlbGl2ZXJ5ZGZpbGVqc2VjcmV0X3JlZnZzZWNyZXRfcmVmOm1vZGVsLXRva2VubW5ldHdvcmtfc2NvcGWka2VudHJ5X3BvaW50ZG5vbmVsdGxzX3JlcXVpcmVk9G5wdWJsaWNfaW5ncmVzc/R3YXV0aGVudGljYXRpb25fcmVxdWlyZWT0bmhhc2hfYWxnb3JpdGhteBlkZXRlcm1pbmlzdGljLWNib3Itc2hhMjU2bnJlc291cmNlX3Njb3BlqGR2Y3B1BGZyZWdpb25pdXMtZWFzdC0xaGRpc2tfZ2liGFBqbWVtb3J5X21pYhlAAGxhcmNoaXRlY3R1cmVlYW1kNjRtaW5zdGFuY2VfdHlwZWptN2kueGxhcmdlb3B1cmNoYXNlX29wdGlvbmlvbl9kZW1hbmRyYXZhaWxhYmlsaXR5X3pvbmVzgmp1cy1lYXN0LTFhanVzLWVhc3QtMWJuc2NoZW1hX3ZlcnNpb251Y2xvdWQtb3JjaGVzdHJhdG9yL3YxcWludGVncmF0aW9uX3Njb3BlgqJka2luZGNtY3BkbmFtZWNtY3CiZGtpbmRjd2ViZG5hbWVmd2ViLXVpc2Nsb3VkX2Nvbm5lY3Rpb25faWRsY29ubmVjdGlvbi0x"
	const wantPlanHash = "sha256:cf121c7475ef21fb0b78a091934942a5ef420aa2bea960eeefcb43d5424426a2"
	const wantApprovalPayloadBase64 = "s2dwbGFuX2lkZnBsYW4tMWhxdW90ZV9pZGdxdW90ZS0xaXBsYW5faGFzaHhHc2hhMjU2OmNmMTIxYzc0NzVlZjIxZmIwYjc4YTA5MTkzNDk0MmE1ZWY0MjBhYTJiZWE5NjBlZWVmY2I0M2Q1NDI0NDI2YTJqZXhwaXJlc19hdHQyMDI2LTA3LTE0VDEwOjEwOjAwWmthcHByb3ZhbF9pZGphcHByb3ZhbC0xbGNoYWxsZW5nZV9pZGtjaGFsbGVuZ2UtMWxxdW90ZV9kaWdlc3R4R3NoYTI1NjpkZmQ3NjFhZTUyNzg1MTdmYWRlYmQzOTAyMzkxZDM5ODFlYjc0N2IyNzM2ZjMzMDUxN2U0NTM1MjA2YjFlNzI0bHNlY3JldF9zY29wZYKjZ3B1cnBvc2Vtc291cmNlLWFjY2Vzc2hkZWxpdmVyeWRmaWxlanNlY3JldF9yZWZ1c2VjcmV0X3JlZjpnaXRodWItYXBwo2dwdXJwb3NlbG1vZGVsLWFjY2Vzc2hkZWxpdmVyeWRmaWxlanNlY3JldF9yZWZ2c2VjcmV0X3JlZjptb2RlbC10b2tlbm1uZXR3b3JrX3Njb3BlpGtlbnRyeV9wb2ludGRub25lbHRsc19yZXF1aXJlZPRucHVibGljX2luZ3Jlc3P0d2F1dGhlbnRpY2F0aW9uX3JlcXVpcmVk9G1wbGFuX3JldmlzaW9uB21yZWNpcGVfZGlnZXN0eEdzaGEyNTY6NDY1OWM4MzM5MWFhNjdmZmYyNjRlMGI4NDBjMzgzZmViMzI3ZGQ3NWQwNjZmODQ0NTU4ODQwMmRkNzFhOTYwNm1zaWduZXJfa2V5X2lkbm93bmVyLWRldmljZS0xbmhhc2hfYWxnb3JpdGhteBlkZXRlcm1pbmlzdGljLWNib3Itc2hhMjU2bnJlc291cmNlX3Njb3BlqGR2Y3B1BGZyZWdpb25pdXMtZWFzdC0xaGRpc2tfZ2liGFBqbWVtb3J5X21pYhlAAGxhcmNoaXRlY3R1cmVlYW1kNjRtaW5zdGFuY2VfdHlwZWptN2kueGxhcmdlb3B1cmNoYXNlX29wdGlvbmlvbl9kZW1hbmRyYXZhaWxhYmlsaXR5X3pvbmVzgmp1cy1lYXN0LTFhanVzLWVhc3QtMWJuc2NoZW1hX3ZlcnNpb251Y2xvdWQtb3JjaGVzdHJhdG9yL3Yxb3BheWxvYWRfdmVyc2lvbngbYXBwcm92YWwtc2lnbmluZy1wYXlsb2FkL3YxcWludGVncmF0aW9uX3Njb3BlgqJka2luZGNtY3BkbmFtZWNtY3CiZGtpbmRjd2ViZG5hbWVmd2ViLXVpcXF1b3RlX3ZhbGlkX3VudGlsdDIwMjYtMDctMTRUMTA6MTU6MDBac2Nsb3VkX2Nvbm5lY3Rpb25faWRsY29ubmVjdGlvbi0x"
	if base64.RawStdEncoding.EncodeToString(canonicalPlan) != wantPlanCBORBase64 || planHash != wantPlanHash || base64.RawStdEncoding.EncodeToString(payload) != wantApprovalPayloadBase64 {
		t.Fatalf("update V1 deterministic-CBOR golden vectors:\nplan cbor base64: %s\nplan hash: %s\napproval payload cbor base64: %s", base64.RawStdEncoding.EncodeToString(canonicalPlan), planHash, base64.RawStdEncoding.EncodeToString(payload))
	}
}

func TestPlanV1CBORUsesJSONFieldNamesAndPreservesUnsignedIntegers(t *testing.T) {
	now := time.Date(2026, time.July, 14, 10, 0, 0, 0, time.UTC)
	plan := validPlan(t, now)
	plan.Revision = math.MaxUint64
	encoded, err := plan.CanonicalPlanCBOR()
	if err != nil {
		t.Fatalf("CanonicalPlanCBOR() error = %v", err)
	}
	var decoded map[string]any
	if err := cbor.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("cbor.Unmarshal() error = %v", err)
	}
	if _, found := decoded["cloud_connection_id"]; !found {
		t.Fatalf("decoded CBOR has no JSON-tagged cloud_connection_id key: %#v", decoded)
	}
	if _, found := decoded["CloudConnectionID"]; found {
		t.Fatalf("decoded CBOR leaked Go field name: %#v", decoded)
	}
	revision, ok := decoded["revision"].(uint64)
	if !ok || revision != math.MaxUint64 {
		t.Fatalf("revision = %#v (%T), want uint64(%d)", decoded["revision"], decoded["revision"], uint64(math.MaxUint64))
	}
}

func TestV1DeterministicCBORQuoteRequestGoldenVector(t *testing.T) {
	request := validQuoteRequest(t)
	canonical, err := request.CanonicalQuoteRequestCBOR()
	if err != nil {
		t.Fatalf("CanonicalQuoteRequestCBOR() error = %v", err)
	}
	digest, err := request.Digest()
	if err != nil {
		t.Fatalf("QuoteRequestV1.Digest() error = %v", err)
	}

	const wantQuoteRequestCBORBase64 = "qGZyZWdpb25pdXMtZWFzdC0xZ3BsYW5faWRmcGxhbi0xamNhbmRpZGF0ZXOCpWR0aWVyZ2Vjb25vbXlsY2FuZGlkYXRlX2lkcWVjb25vbXktY2FuZGlkYXRlbWluc3RhbmNlX3R5cGVpbTdpLmxhcmdlb3B1cmNoYXNlX29wdGlvbmlvbl9kZW1hbmRyZXN0aW1hdGVkX2Rpc2tfZ2liGFClZHRpZXJrcmVjb21tZW5kZWRsY2FuZGlkYXRlX2lkdXJlY29tbWVuZGVkLWNhbmRpZGF0ZW1pbnN0YW5jZV90eXBlam03aS54bGFyZ2VvcHVyY2hhc2Vfb3B0aW9uaW9uX2RlbWFuZHJlc3RpbWF0ZWRfZGlza19naWIYUG1wbGFuX3JldmlzaW9uB21yZWNpcGVfZGlnZXN0eEdzaGEyNTY6NDY1OWM4MzM5MWFhNjdmZmYyNjRlMGI4NDBjMzgzZmViMzI3ZGQ3NWQwNjZmODQ0NTU4ODQwMmRkNzFhOTYwNm5zY2hlbWFfdmVyc2lvbnVjbG91ZC1vcmNoZXN0cmF0b3IvdjFwcXVvdGVfcmVxdWVzdF9pZG9xdW90ZS1yZXF1ZXN0LTFzY2xvdWRfY29ubmVjdGlvbl9pZGxjb25uZWN0aW9uLTE"
	const wantQuoteRequestDigest = "sha256:1721e769985d75a110dc33341743312bc3bbeaa07d11b157fc5c6eb709ebb857"
	if base64.RawStdEncoding.EncodeToString(canonical) != wantQuoteRequestCBORBase64 || digest != wantQuoteRequestDigest {
		t.Fatalf("update V1 deterministic-CBOR quote request golden vector:\nquote request cbor base64: %s\nquote request digest: %s", base64.RawStdEncoding.EncodeToString(canonical), digest)
	}
}
