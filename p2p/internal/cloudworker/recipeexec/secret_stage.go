package recipeexec

import (
	"context"
	"errors"
	"os"
	"path/filepath"
)

type TmpfsVerifier func(string) error

// FileSecretStager writes only catalog-selected basenames beneath one verified
// tmpfs root. Values never enter arguments, logs, checkpoints, or events.
type FileSecretStager struct {
	root string
}

func NewFileSecretStager(root string, verify TmpfsVerifier) (*FileSecretStager, error) {
	if root == "" || !filepath.IsAbs(root) || verify == nil {
		return nil, ErrSecretStage
	}
	clean := filepath.Clean(root)
	if err := verify(clean); err != nil {
		return nil, ErrSecretStage
	}
	return &FileSecretStager{root: clean}, nil
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
	delivery := SecretDelivery{Files: make(map[string]string)}
	environment := make([]byte, 0, 256)
	defer clear(environment)
	for _, secret := range secrets {
		if err := ctx.Err(); err != nil {
			cleanup()
			return SecretDelivery{}, nil, err
		}
		if !validSecretDestination(secret.Target) {
			cleanup()
			return SecretDelivery{}, nil, ErrSecretStage
		}
		switch {
		case secret.Target.FileName != "" && secret.Target.EnvironmentKey == "":
			path := filepath.Join(directory, secret.Target.FileName)
			if err := atomicSecretWrite(path, secret.Value); err != nil {
				cleanup()
				return SecretDelivery{}, nil, ErrSecretStage
			}
			delivery.Files[secret.Target.SlotID] = path
		case secret.Target.FileName == "" && environmentKeyPattern.MatchString(secret.Target.EnvironmentKey):
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
	return delivery, cleanup, nil
}

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
