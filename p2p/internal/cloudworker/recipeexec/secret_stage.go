package recipeexec

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

type TmpfsVerifier func(string) error

// FileSecretStager writes only catalog-selected basenames beneath one verified
// tmpfs root. Values never enter arguments, logs, checkpoints, or events.
type FileSecretStager struct {
	root   string
	verify TmpfsVerifier
}

func NewFileSecretStager(root string, verify TmpfsVerifier) (*FileSecretStager, error) {
	if root == "" || !filepath.IsAbs(root) || verify == nil {
		return nil, ErrSecretStage
	}
	clean := filepath.Clean(root)
	if err := verify(clean); err != nil {
		return nil, ErrSecretStage
	}
	return &FileSecretStager{root: clean, verify: verify}, nil
}

func (stager *FileSecretStager) Stage(ctx context.Context, deploymentID, executionID string, secrets []MaterializedSecret) (SecretDelivery, func(), error) {
	if stager == nil || !validBindingIdentifier(deploymentID) || !validBindingIdentifier(executionID) || len(secrets) == 0 {
		return SecretDelivery{}, nil, ErrSecretStage
	}
	if ctx == nil {
		ctx = context.Background()
	}
	directory := filepath.Join(stager.root, deploymentID+"-"+executionID)
	cleanup := func() { _ = os.RemoveAll(directory) }
	cleanup()
	if err := os.Mkdir(directory, 0o700); err != nil {
		return SecretDelivery{}, nil, ErrSecretStage
	}
	delivery := SecretDelivery{StagingDirectory: directory, Files: make(map[string]string)}
	environment := make([]byte, 0, 256)
	defer clear(environment)
	seenSlots, seenBasenames, seenEnvironmentKeys := map[string]struct{}{}, map[string]struct{}{}, map[string]struct{}{}
	for _, secret := range secrets {
		if err := ctx.Err(); err != nil {
			cleanup()
			return SecretDelivery{}, nil, err
		}
		if !validSecretDestination(secret.Target) {
			cleanup()
			return SecretDelivery{}, nil, ErrSecretStage
		}
		if _, exists := seenSlots[secret.Target.SlotID]; exists {
			cleanup()
			return SecretDelivery{}, nil, ErrSecretStage
		}
		seenSlots[secret.Target.SlotID] = struct{}{}
		switch {
		case secret.Target.FileName != "" && secret.Target.EnvironmentKey == "":
			basename := strings.ToLower(secret.Target.FileName)
			if basename == "environment" {
				cleanup()
				return SecretDelivery{}, nil, ErrSecretStage
			}
			if _, exists := seenBasenames[basename]; exists {
				cleanup()
				return SecretDelivery{}, nil, ErrSecretStage
			}
			seenBasenames[basename] = struct{}{}
			path := filepath.Join(directory, secret.Target.FileName)
			if err := atomicSecretWrite(path, secret.Value); err != nil {
				cleanup()
				return SecretDelivery{}, nil, ErrSecretStage
			}
			delivery.Files[secret.Target.SlotID] = path
		case secret.Target.FileName == "" && environmentKeyPattern.MatchString(secret.Target.EnvironmentKey):
			if _, exists := seenEnvironmentKeys[secret.Target.EnvironmentKey]; exists {
				cleanup()
				return SecretDelivery{}, nil, ErrSecretStage
			}
			seenEnvironmentKeys[secret.Target.EnvironmentKey] = struct{}{}
			encoded, err := encodeEnvironmentValue(secret.Value)
			if err != nil {
				cleanup()
				return SecretDelivery{}, nil, ErrSecretStage
			}
			environment = append(environment, secret.Target.EnvironmentKey...)
			environment = append(environment, '=')
			environment = append(environment, encoded...)
			environment = append(environment, '\n')
			clear(encoded)
		default:
			cleanup()
			return SecretDelivery{}, nil, ErrSecretStage
		}
	}
	if len(environment) > 0 {
		path := filepath.Join(directory, "environment")
		if err := atomicSecretWrite(path, environment); err != nil {
			cleanup()
			return SecretDelivery{}, nil, ErrSecretStage
		}
		delivery.EnvironmentFile = path
	}
	if err := validateStagedSecretDelivery(delivery, stager.verify); err != nil {
		cleanup()
		return SecretDelivery{}, nil, ErrSecretStage
	}
	return delivery, cleanup, nil
}

// ValidateStagedSecretDelivery verifies the complete filesystem boundary a
// Driver is allowed to consume. Empty delivery is valid for secretless work.
func ValidateStagedSecretDelivery(delivery SecretDelivery) error {
	return validateStagedSecretDelivery(delivery, verifyStagedSecretTmpfsRoot)
}

func validateStagedSecretDelivery(delivery SecretDelivery, verify TmpfsVerifier) error {
	if delivery.StagingDirectory == "" && delivery.EnvironmentFile == "" && len(delivery.Files) == 0 {
		return nil
	}
	directory := delivery.StagingDirectory
	if directory == "" || !filepath.IsAbs(directory) || filepath.Clean(directory) != directory || verify == nil || verify(directory) != nil {
		return ErrSecretStage
	}
	directoryInfo, err := os.Lstat(directory)
	if err != nil || directoryInfo.Mode()&os.ModeSymlink != 0 || !directoryInfo.IsDir() {
		return ErrSecretStage
	}
	if delivery.EnvironmentFile == "" && len(delivery.Files) == 0 {
		return ErrSecretStage
	}
	basenames := make(map[string]struct{}, len(delivery.Files)+1)
	validateFile := func(path string) error {
		if path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path || filepath.Dir(path) != directory {
			return ErrSecretStage
		}
		basename := filepath.Base(path)
		if basename == "." || basename == string(filepath.Separator) || strings.ContainsAny(basename, `/\`) {
			return ErrSecretStage
		}
		key := strings.ToLower(basename)
		if _, exists := basenames[key]; exists {
			return ErrSecretStage
		}
		info, err := os.Lstat(path)
		if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || runtime.GOOS != "windows" && info.Mode().Perm() != 0o600 {
			return ErrSecretStage
		}
		basenames[key] = struct{}{}
		return nil
	}
	if delivery.EnvironmentFile != "" {
		if err := validateFile(delivery.EnvironmentFile); err != nil {
			return err
		}
	}
	seenSlots := make(map[string]struct{}, len(delivery.Files))
	for slot, path := range delivery.Files {
		if !validBindingIdentifier(slot) {
			return ErrSecretStage
		}
		if _, exists := seenSlots[slot]; exists {
			return ErrSecretStage
		}
		seenSlots[slot] = struct{}{}
		if err := validateFile(path); err != nil {
			return err
		}
	}
	return nil
}

var verifyStagedSecretTmpfsRoot = VerifyTmpfsRoot

func atomicSecretWrite(path string, value []byte) error {
	temporary, err := os.CreateTemp(filepath.Dir(path), ".secret-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0o600); err != nil {
		temporary.Close()
		return err
	}
	if _, err := temporary.Write(value); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	return os.Rename(temporaryPath, path)
}

func encodeEnvironmentValue(value []byte) ([]byte, error) {
	if len(value) == 0 || bytesContainAny(value, 0, '\r', '\n') {
		return nil, errors.New("environment secret is invalid")
	}
	encoded := make([]byte, 0, len(value)+2)
	encoded = append(encoded, '"')
	for _, character := range value {
		if character == '\\' || character == '"' {
			encoded = append(encoded, '\\')
		}
		encoded = append(encoded, character)
	}
	encoded = append(encoded, '"')
	return encoded, nil
}

func bytesContainAny(value []byte, characters ...byte) bool {
	for _, candidate := range value {
		for _, character := range characters {
			if candidate == character {
				return true
			}
		}
	}
	return false
}
