package connectionbootstrap

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"
)

func TestConnectionTemplateReferenceMatchesProductCoreClosedUnionJSONShape(t *testing.T) {
	tests := []string{
		`{"schema":"dirextalk.connection-template-reference/v1","mode":"s3_binding","binding":{"schema":"dirextalk.immutable-artifact-binding/v1","kind":"connection_stack_template","version":"v1.1.0-cloud-mvp.20260716.1","bucket":"dirextalk-artifacts","key":"releases/connection-stack/v1.1.0-cloud-mvp.20260716.1/connection-stack-v1.1.0-cloud-mvp.20260716.1-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.yaml","version_id":"version-00000001","sha256":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","size_bytes":512,"content_type":"application/x-yaml","kms_key_id":"arn:aws:kms:us-east-1:123456789012:key/01234567-89ab-cdef-0123-456789abcdef"}}`,
		`{"schema":"dirextalk.connection-template-reference/v1","mode":"publish_intent","publish_intent":{"kind":"connection_stack_template","version":"v1.1.0-cloud-mvp.20260716.1","sha256":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","size_bytes":512,"content_type":"application/x-yaml"}}`,
	}
	for _, raw := range tests {
		t.Run("union", func(t *testing.T) {
			reference, err := ParseConnectionTemplateReference([]byte(raw))
			if err != nil {
				t.Fatal(err)
			}
			roundTrip, err := json.Marshal(reference)
			if err != nil {
				t.Fatal(err)
			}
			var want, got map[string]any
			if json.Unmarshal([]byte(raw), &want) != nil || json.Unmarshal(roundTrip, &got) != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("round-trip changed ProductCore union shape: %s", roundTrip)
			}
		})
	}
}

func TestConnectionTemplateReferenceRejectsRawURLAndInvalidUnionBranches(t *testing.T) {
	for _, raw := range []string{
		`{"schema":"dirextalk.connection-template-reference/v1","mode":"s3_binding","binding":{"schema":"dirextalk.immutable-artifact-binding/v1","kind":"connection_stack_template","version":"v1.1.0-cloud-mvp.20260716.1","bucket":"dirextalk-artifacts","key":"releases/connection-stack/v1.1.0-cloud-mvp.20260716.1/connection-stack-v1.1.0-cloud-mvp.20260716.1-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.yaml","version_id":"version-00000001","sha256":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","size_bytes":512,"content_type":"application/x-yaml","kms_key_id":"arn:aws:kms:us-east-1:123456789012:key/01234567-89ab-cdef-0123-456789abcdef"},"template_url":"https://mutable.example.invalid/template.yaml"}`,
		`{"schema":"dirextalk.connection-template-reference/v1","mode":"s3_binding","binding":{"schema":"dirextalk.immutable-artifact-binding/v1","kind":"connection_stack_template","version":"v1.1.0-cloud-mvp.20260716.1","bucket":"dirextalk-artifacts","key":"releases/connection-stack/v1.1.0-cloud-mvp.20260716.1/connection-stack-v1.1.0-cloud-mvp.20260716.1-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.yaml","version_id":"version-00000001","sha256":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","size_bytes":512,"content_type":"application/x-yaml","kms_key_id":"arn:aws:kms:us-east-1:123456789012:key/01234567-89ab-cdef-0123-456789abcdef"},"publish_intent":{"kind":"connection_stack_template","version":"v1.1.0-cloud-mvp.20260716.1","sha256":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","size_bytes":512,"content_type":"application/x-yaml"}}`,
	} {
		if _, err := ParseConnectionTemplateReference([]byte(raw)); !errors.Is(err, ErrInvalid) {
			t.Fatalf("ParseConnectionTemplateReference() error=%v", err)
		}
	}
}
