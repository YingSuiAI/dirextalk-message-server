package recipeexec

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestValidateStagedSecretDeliveryRequiresDirectRegular0600Files(t *testing.T) {
	originalVerifier := verifyStagedSecretTmpfsRoot
	verifyStagedSecretTmpfsRoot = func(string) error { return nil }
	t.Cleanup(func() { verifyStagedSecretTmpfsRoot = originalVerifier })

	directory := filepath.Join(t.TempDir(), "staged")
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	filePath := filepath.Join(directory, "model-token")
	environmentPath := filepath.Join(directory, "environment")
	for _, path := range []string{filePath, environmentPath} {
		if err := os.WriteFile(path, []byte("canary"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(path, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	valid := SecretDelivery{StagingDirectory: directory, EnvironmentFile: environmentPath, Files: map[string]string{"model-token": filePath}}
	if err := ValidateStagedSecretDelivery(valid); err != nil {
		t.Fatal(err)
	}

	outside := filepath.Join(t.TempDir(), "outside")
	if err := os.WriteFile(outside, []byte("canary"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(outside, 0o600); err != nil {
		t.Fatal(err)
	}
	wrongMode := filepath.Join(directory, "wrong-mode")
	if err := os.WriteFile(wrongMode, []byte("canary"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(wrongMode, 0o644); err != nil {
		t.Fatal(err)
	}
	nestedDirectory := filepath.Join(directory, "nested")
	if err := os.Mkdir(nestedDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	nested := filepath.Join(nestedDirectory, "token")
	if err := os.WriteFile(nested, []byte("canary"), 0o600); err != nil {
		t.Fatal(err)
	}
	invalid := map[string]SecretDelivery{
		"missing staging directory": {EnvironmentFile: environmentPath},
		"outside":                   {StagingDirectory: directory, Files: map[string]string{"model-token": outside}},
		"nested":                    {StagingDirectory: directory, Files: map[string]string{"model-token": nested}},
		"duplicate basename":        {StagingDirectory: directory, EnvironmentFile: filePath, Files: map[string]string{"model-token": filePath}},
		"invalid slot":              {StagingDirectory: directory, Files: map[string]string{"../slot": filePath}},
		"incomplete":                {StagingDirectory: directory},
	}
	if runtime.GOOS != "windows" {
		invalid["wrong mode"] = SecretDelivery{StagingDirectory: directory, Files: map[string]string{"model-token": wrongMode}}
	}
	for name, delivery := range invalid {
		t.Run(name, func(t *testing.T) {
			if err := ValidateStagedSecretDelivery(delivery); !errors.Is(err, ErrSecretStage) {
				t.Fatalf("validation error=%v", err)
			}
		})
	}
}

func TestValidateStagedSecretDeliveryRejectsSymlink(t *testing.T) {
	originalVerifier := verifyStagedSecretTmpfsRoot
	verifyStagedSecretTmpfsRoot = func(string) error { return nil }
	t.Cleanup(func() { verifyStagedSecretTmpfsRoot = originalVerifier })
	directory := filepath.Join(t.TempDir(), "staged")
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(directory, "target")
	if err := os.WriteFile(target, []byte("canary"), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(directory, "link")
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	if err := ValidateStagedSecretDelivery(SecretDelivery{StagingDirectory: directory, Files: map[string]string{"model-token": link}}); !errors.Is(err, ErrSecretStage) {
		t.Fatalf("symlink validation error=%v", err)
	}
}

func TestFileSecretStagerRejectsDuplicateSlotsAndBasenames(t *testing.T) {
	root := t.TempDir()
	stager, err := NewFileSecretStager(root, func(string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	for name, secrets := range map[string][]MaterializedSecret{
		"slot": {
			{Target: SecretTarget{SlotID: "model-token", FileName: "first"}, Value: []byte("one")},
			{Target: SecretTarget{SlotID: "model-token", FileName: "second"}, Value: []byte("two")},
		},
		"basename": {
			{Target: SecretTarget{SlotID: "model-token-a", FileName: "token"}, Value: []byte("one")},
			{Target: SecretTarget{SlotID: "model-token-b", FileName: "token"}, Value: []byte("two")},
		},
		"reserved environment basename": {
			{Target: SecretTarget{SlotID: "model-token", FileName: "environment"}, Value: []byte("one")},
		},
	} {
		t.Run(name, func(t *testing.T) {
			if _, cleanup, err := stager.Stage(t.Context(), "deployment-1", "execution-1", secrets); !errors.Is(err, ErrSecretStage) || cleanup != nil {
				t.Fatalf("duplicate staging result cleanup=%v err=%v", cleanup != nil, err)
			}
		})
	}
}
