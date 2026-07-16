// Package rootbootstrap resolves the one reviewed, non-secret release used
// when a user deliberately bootstraps a Connection Stack with a root key.
// It accepts neither client-provided AWS parameters nor mutable artifact refs.
package rootbootstrap

import (
	"bytes"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/artifactpublish"
	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/connectionfoundation"
)

const ReleaseConfigSchema = "dirextalk.root-bootstrap-release/v1"

var (
	ErrInvalidReleaseConfig = errors.New("root bootstrap release configuration is invalid")

	releaseRegionPattern       = regexp.MustCompile(`^(af|ap|ca|cn|eu|il|me|mx|sa|us)(-gov)?-[a-z]+-[0-9]$`)
	releaseAZPattern           = regexp.MustCompile(`^(af|ap|ca|cn|eu|il|me|mx|sa|us)(-gov)?-[a-z]+-[0-9][a-z]$`)
	amiPattern                 = regexp.MustCompile(`^ami-[0-9a-f]{8,17}$`)
	namedDigestPattern         = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	maxReleaseConfigBytes      = 64 << 10
	maxIIDRSAPublicKeyPEMBytes = 2048
)

// ReleaseConfig is loaded only from the server-owned, independently released
// configuration file. It contains fixed release facts and local paths, never
// user input, cloud resource identifiers, AWS URLs, or credentials.
type ReleaseConfig struct {
	SchemaVersion      string        `json:"schema_version"`
	Region             string        `json:"region"`
	ConnectionTemplate LocalArtifact `json:"connection_template"`
	BrokerZIP          LocalArtifact `json:"broker_zip"`
	Worker             WorkerRelease `json:"worker"`
}

// LocalArtifact is an immutable release file. Its Kind is intentionally not
// configurable: the enclosing ReleaseConfig field determines it.
type LocalArtifact struct {
	Path      string `json:"path"`
	Version   string `json:"version"`
	SHA256    string `json:"sha256"`
	SizeBytes int64  `json:"size_bytes"`
}

// WorkerRelease contains the already-built Worker image facts. Root
// bootstrap only uploads the fixed Worker archive and binds these values; it
// never builds an AMI or executes a dynamic release script.
type WorkerRelease struct {
	Archive                      LocalArtifact                       `json:"archive"`
	AMIID                        string                              `json:"ami_id"`
	ImageManifestSHA256          string                              `json:"image_manifest_sha256"`
	WorkerResourceManifestDigest string                              `json:"worker_resource_manifest_digest"`
	AvailabilityZone             string                              `json:"availability_zone"`
	IIDVerifier                  connectionfoundation.IIDVerifier `json:"iid_verifier"`
}

// ParseReleaseConfig accepts exactly the closed release-config JSON shape.
// Duplicate keys are rejected recursively because encoding/json otherwise
// lets the last duplicate silently replace an immutable release fact.
func ParseReleaseConfig(raw []byte) (ReleaseConfig, error) {
	if len(raw) == 0 || len(raw) > maxReleaseConfigBytes || rejectDuplicateKeys(raw) != nil {
		return ReleaseConfig{}, ErrInvalidReleaseConfig
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var config ReleaseConfig
	if err := decoder.Decode(&config); err != nil {
		return ReleaseConfig{}, ErrInvalidReleaseConfig
	}
	if err := decoder.Decode(new(struct{})); !errors.Is(err, io.EOF) {
		return ReleaseConfig{}, ErrInvalidReleaseConfig
	}
	if err := config.Validate(); err != nil {
		return ReleaseConfig{}, err
	}
	return config, nil
}

// Validate checks release facts without opening their local files. File type,
// size, and digest are checked again by FileSource and Resolver immediately
// before any AWS mutation.
func (config ReleaseConfig) Validate() error {
	if config.SchemaVersion != ReleaseConfigSchema || !releaseRegionPattern.MatchString(config.Region) ||
		!releaseAZPattern.MatchString(config.Worker.AvailabilityZone) || !strings.HasPrefix(config.Worker.AvailabilityZone, config.Region) ||
		!amiPattern.MatchString(config.Worker.AMIID) || !namedDigestPattern.MatchString(config.Worker.ImageManifestSHA256) ||
		!namedDigestPattern.MatchString(config.Worker.WorkerResourceManifestDigest) || !validIIDVerifier(config.Worker.IIDVerifier) {
		return ErrInvalidReleaseConfig
	}
	template, broker, worker, err := config.descriptors()
	if err != nil || template.Version != broker.Version || broker.Version != worker.Version {
		return ErrInvalidReleaseConfig
	}
	return nil
}

// ArtifactDescriptors returns the three closed immutable descriptors after
// validating the complete release configuration.
func (config ReleaseConfig) ArtifactDescriptors() (artifactpublish.ArtifactDescriptor, artifactpublish.ArtifactDescriptor, artifactpublish.ArtifactDescriptor, error) {
	if err := config.Validate(); err != nil {
		return artifactpublish.ArtifactDescriptor{}, artifactpublish.ArtifactDescriptor{}, artifactpublish.ArtifactDescriptor{}, err
	}
	return config.descriptors()
}

func (config ReleaseConfig) descriptors() (artifactpublish.ArtifactDescriptor, artifactpublish.ArtifactDescriptor, artifactpublish.ArtifactDescriptor, error) {
	template, err := config.ConnectionTemplate.descriptor(artifactpublish.KindConnectionTemplate)
	if err != nil {
		return artifactpublish.ArtifactDescriptor{}, artifactpublish.ArtifactDescriptor{}, artifactpublish.ArtifactDescriptor{}, ErrInvalidReleaseConfig
	}
	broker, err := config.BrokerZIP.descriptor(artifactpublish.KindBrokerZIP)
	if err != nil {
		return artifactpublish.ArtifactDescriptor{}, artifactpublish.ArtifactDescriptor{}, artifactpublish.ArtifactDescriptor{}, ErrInvalidReleaseConfig
	}
	worker, err := config.Worker.Archive.descriptor(artifactpublish.KindWorkerArchive)
	if err != nil {
		return artifactpublish.ArtifactDescriptor{}, artifactpublish.ArtifactDescriptor{}, artifactpublish.ArtifactDescriptor{}, ErrInvalidReleaseConfig
	}
	return template, broker, worker, nil
}

func (artifact LocalArtifact) descriptor(kind artifactpublish.ArtifactKind) (artifactpublish.ArtifactDescriptor, error) {
	descriptor := artifactpublish.ArtifactDescriptor{Kind: kind, Version: artifact.Version, SHA256: artifact.SHA256, SizeBytes: artifact.SizeBytes}
	if !validLocalPath(artifact.Path) || descriptor.Validate() != nil {
		return artifactpublish.ArtifactDescriptor{}, ErrInvalidReleaseConfig
	}
	return descriptor, nil
}

func validLocalPath(path string) bool {
	return path != "" && strings.TrimSpace(path) == path && !strings.ContainsRune(path, '\x00') &&
		!strings.Contains(path, "://") && filepath.IsAbs(path) && filepath.Clean(path) == path &&
		!strings.HasPrefix(path, `\\`)
}

func validIIDVerifier(verifier connectionfoundation.IIDVerifier) bool {
	if verifier.Algorithm != connectionfoundation.EC2IIDRSASHA256Verifier || len(verifier.RSAPublicKeyPEM) == 0 || len(verifier.RSAPublicKeyPEM) > maxIIDRSAPublicKeyPEMBytes {
		return false
	}
	block, rest := pem.Decode([]byte(verifier.RSAPublicKeyPEM))
	if block == nil || strings.TrimSpace(string(rest)) != "" {
		return false
	}
	var publicKey *rsa.PublicKey
	switch block.Type {
	case "PUBLIC KEY":
		parsed, err := x509.ParsePKIXPublicKey(block.Bytes)
		if err != nil {
			return false
		}
		var ok bool
		publicKey, ok = parsed.(*rsa.PublicKey)
		if !ok {
			return false
		}
	case "RSA PUBLIC KEY":
		parsed, err := x509.ParsePKCS1PublicKey(block.Bytes)
		if err != nil {
			return false
		}
		publicKey = parsed
	default:
		return false
	}
	return publicKey != nil && publicKey.N.BitLen() >= 2048 && publicKey.E >= 3 && publicKey.E%2 != 0
}

func rejectDuplicateKeys(raw []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	if err := scanJSON(decoder); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return ErrInvalidReleaseConfig
	}
	return nil
}

func scanJSON(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return ErrInvalidReleaseConfig
	}
	switch delim {
	case '{':
		seen := map[string]struct{}{}
		for decoder.More() {
			keyToken, err := decoder.Token()
			key, ok := keyToken.(string)
			if err != nil || !ok {
				return ErrInvalidReleaseConfig
			}
			if _, exists := seen[key]; exists {
				return ErrInvalidReleaseConfig
			}
			seen[key] = struct{}{}
			if err := scanJSON(decoder); err != nil {
				return err
			}
		}
		end, err := decoder.Token()
		if err != nil || end != json.Delim('}') {
			return ErrInvalidReleaseConfig
		}
	case '[':
		for decoder.More() {
			if err := scanJSON(decoder); err != nil {
				return err
			}
		}
		end, err := decoder.Token()
		if err != nil || end != json.Delim(']') {
			return ErrInvalidReleaseConfig
		}
	default:
		return ErrInvalidReleaseConfig
	}
	return nil
}
