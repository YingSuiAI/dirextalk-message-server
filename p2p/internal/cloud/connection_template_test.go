package cloud

import (
	"strings"
	"testing"
	"time"
)

func TestConnectionTemplateReferenceClosesBothExecutionBranches(t *testing.T) {
	binding := testConnectionTemplateS3Binding()
	intent := testConnectionTemplatePublishIntent()
	if err := binding.ValidateForRootCredentialBootstrap(false); err != nil {
		t.Fatalf("valid ordinary binding: %v", err)
	}
	if err := binding.ValidateForRootCredentialBootstrap(true); err == nil {
		t.Fatal("ordinary role path accepted a S3 binding as root publish intent")
	}
	if err := intent.ValidateForRootCredentialBootstrap(true); err != nil {
		t.Fatalf("valid root publish intent: %v", err)
	}
	if err := intent.ValidateForRootCredentialBootstrap(false); err == nil {
		t.Fatal("ordinary role path accepted a root publish intent")
	}

	for name, mutate := range map[string]func(*ConnectionTemplateReference){
		"mixed_union": func(reference *ConnectionTemplateReference) {
			reference.PublishIntent = testConnectionTemplatePublishIntent().PublishIntent
		},
		"missing_version_id": func(reference *ConnectionTemplateReference) { reference.Binding.VersionID = "" },
		"mutable_version_id": func(reference *ConnectionTemplateReference) { reference.Binding.VersionID = "null" },
		"release_alias":      func(reference *ConnectionTemplateReference) { reference.Binding.Version = "v1.1.0-latest" },
		"formal_release":     func(reference *ConnectionTemplateReference) { reference.Binding.Version = "v1.0.3" },
		"wrong_key":          func(reference *ConnectionTemplateReference) { reference.Binding.Key += ".mutable" },
		"wrong_content_type": func(reference *ConnectionTemplateReference) { reference.Binding.ContentType = "application/json" },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := binding.Clone()
			mutate(&candidate)
			if err := candidate.Validate(); err == nil {
				t.Fatalf("invalid connection template %q was accepted: %#v", name, candidate)
			}
		})
	}
	for name, mutate := range map[string]func(*ConnectionTemplateReference){
		"release_alias":  func(reference *ConnectionTemplateReference) { reference.PublishIntent.Version = "v1.1.0-latest" },
		"formal_release": func(reference *ConnectionTemplateReference) { reference.PublishIntent.Version = "v1.0.3" },
		"wrong_kind":     func(reference *ConnectionTemplateReference) { reference.PublishIntent.Kind = "worker_archive" },
		"wrong_size":     func(reference *ConnectionTemplateReference) { reference.PublishIntent.SizeBytes = 0 },
	} {
		t.Run("intent_"+name, func(t *testing.T) {
			candidate := intent.Clone()
			mutate(&candidate)
			if err := candidate.Validate(); err == nil {
				t.Fatalf("invalid publish intent %q was accepted: %#v", name, candidate)
			}
		})
	}
}

func TestConnectionTemplateURLRequiresCanonicalVersionPinnedS3URL(t *testing.T) {
	reference := testConnectionTemplateS3Binding()
	valid, err := reference.CloudFormationURL("us-east-1")
	if err != nil || !validTemplateURL(valid) {
		t.Fatalf("valid pinned template URL=%q err=%v", valid, err)
	}
	if err := validateTemplateURLForBinding(valid, "us-east-1", *reference.Binding); err != nil {
		t.Fatalf("valid binding URL was rejected: %v", err)
	}
	for name, raw := range map[string]string{
		"no_version":        strings.Split(valid, "?")[0],
		"extra_query":       valid + "&trace=1",
		"duplicate_version": valid + "&versionId=second",
		"fragment":          valid + "#fragment",
		"user":              strings.Replace(valid, "https://", "https://user@", 1),
		"escaped_path":      strings.Replace(valid, "/releases/", "/releases%2F", 1),
		"path_escape":       strings.Replace(valid, "/releases/", "/releases/../", 1),
		"non_s3":            strings.Replace(valid, "s3.us-east-1.amazonaws.com", "artifacts.example.invalid", 1),
	} {
		t.Run(name, func(t *testing.T) {
			if validTemplateURL(raw) {
				t.Fatalf("mutable or noncanonical template URL was accepted: %q", raw)
			}
		})
	}
}

func TestConnectionStackConfigRejectsRawTemplateURLAndBindsTemplateDigest(t *testing.T) {
	config := ConnectionStackConfig{
		TemplateDigest:          "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		ConnectionTemplate:      testConnectionTemplateS3Binding(),
		SourceTreeDigest:        "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		NodeKeyID:               "node-key-0001",
		NodePublicKeySPKIBase64: testCredentialBootstrapSPKI(t),
		RolePlanTTL:             15 * time.Minute,
	}
	if err := ValidateConnectionStackConfig(config); err != nil {
		t.Fatalf("valid typed connection stack config: %v", err)
	}
	url, err := config.ConnectionTemplate.CloudFormationURL("us-east-1")
	if err != nil {
		t.Fatal(err)
	}
	config.TemplateURL = url
	if err := ValidateConnectionStackConfig(config); err == nil {
		t.Fatal("legacy raw template URL was accepted")
	}
	config.TemplateURL = ""
	config.TemplateDigest = "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	if err := ValidateConnectionStackConfig(config); err == nil {
		t.Fatal("mismatched template digest was accepted")
	}
}

func TestConnectionBootstrapRequestDigestCoversRootPublishIntent(t *testing.T) {
	intent := testConnectionTemplatePublishIntent()
	first := connectionBootstrapRequestDigest("aws", "us-east-1", "device-key-0001", "public-key", true, intent)
	changed := intent.Clone()
	changed.PublishIntent.Version = "v1.1.0-cloud-mvp.20260716.2"
	second := connectionBootstrapRequestDigest("aws", "us-east-1", "device-key-0001", "public-key", true, changed)
	if first == second {
		t.Fatalf("root publish intent change did not affect request digest: %s", first)
	}
}

func TestConnectionTemplateReferencePersistenceEncodingIsCanonical(t *testing.T) {
	reference := testConnectionTemplatePublishIntent()
	raw, err := EncodeConnectionTemplateReference(reference)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := ParseConnectionTemplateReference(raw)
	if err != nil || decoded.IdentityDigest() != reference.IdentityDigest() {
		t.Fatalf("reference persistence round trip=%#v err=%v", decoded, err)
	}
	formatted := "\n" + raw + "\n"
	if decoded, err := ParseConnectionTemplateReference(formatted); err != nil || decoded.IdentityDigest() != reference.IdentityDigest() {
		t.Fatalf("strict configuration parser rejected harmless JSON whitespace: %#v err=%v", decoded, err)
	}
	if _, err := ParsePersistedConnectionTemplateReference(formatted); err == nil {
		t.Fatal("persisted reference accepted a noncanonical encoding")
	}
	if _, err := ParseConnectionTemplateReference(strings.TrimSuffix(raw, "}") + `,"unexpected":"field"}`); err == nil {
		t.Fatal("persistent reference accepted an unknown field")
	}
	if _, err := ParseConnectionTemplateReference(strings.Replace(raw, `"mode":"publish_intent"`, `"mode":"publish_intent","mode":"s3_binding"`, 1)); err == nil {
		t.Fatal("persistent reference accepted a duplicate JSON field")
	}
	if _, err := ParseConnectionTemplateReference(strings.Replace(raw, `"publish_intent":`, `"binding":null,"publish_intent":`, 1)); err == nil {
		t.Fatal("publish intent accepted an extraneous null binding branch")
	}
}

func testConnectionTemplateS3Binding() ConnectionTemplateReference {
	return ConnectionTemplateReference{
		Schema: connectionTemplateReferenceSchema,
		Mode:   connectionTemplateModeS3Binding,
		Binding: &ConnectionTemplateBinding{
			Schema: immutableArtifactBindingSchema, Kind: connectionTemplateArtifactKind, Version: "v1.1.0-cloud-mvp.20260716.1",
			Bucket: "dirextalk-artifacts", Key: connectionTemplateObjectKey("v1.1.0-cloud-mvp.20260716.1", "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
			VersionID: "version-00000001", SHA256: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", SizeBytes: 512,
			ContentType: connectionTemplateContentType, KMSKeyID: "alias/dirextalk-artifacts",
		},
	}
}

func testConnectionTemplatePublishIntent() ConnectionTemplateReference {
	return ConnectionTemplateReference{
		Schema: connectionTemplateReferenceSchema,
		Mode:   connectionTemplateModePublishIntent,
		PublishIntent: &ConnectionTemplatePublishIntent{
			Kind: connectionTemplateArtifactKind, Version: "v1.1.0-cloud-mvp.20260716.1",
			SHA256: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", SizeBytes: 512, ContentType: connectionTemplateContentType,
		},
	}
}
