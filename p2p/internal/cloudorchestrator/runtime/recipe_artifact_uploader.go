package runtime

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

const maxRecipeArtifactUploadResponse = 4096

type StrictRecipeArtifactUploader struct {
	client *http.Client
	now    func() time.Time
}

func NewStrictRecipeArtifactUploader(timeout time.Duration) (*StrictRecipeArtifactUploader, error) {
	if timeout <= 0 || timeout > 5*time.Minute {
		return nil, errors.New("recipe artifact upload timeout is invalid")
	}
	transport := &http.Transport{
		Proxy:                 nil,
		DisableCompression:    true,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: timeout,
		TLSClientConfig:       &tls.Config{MinVersion: tls.VersionTLS12},
	}
	return &StrictRecipeArtifactUploader{
		client: &http.Client{
			Transport: transport, Timeout: timeout,
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
		now: time.Now,
	}, nil
}

func (uploader *StrictRecipeArtifactUploader) Upload(ctx context.Context, archive TrustedRecipeArtifactArchive, grant RecipeArtifactUploadGrant) (string, error) {
	if uploader == nil || uploader.client == nil || uploader.now == nil || ctx == nil || archive.Validate() != nil || validateRecipeArtifactUploadGrant(grant, archive, uploader.now().UTC()) != nil {
		return "", errors.New("recipe artifact upload is invalid")
	}
	file, before, err := openVerifiedTrustedRecipeArchive(archive)
	if err != nil {
		return "", err
	}
	defer file.Close()
	uploadHash := sha256.New()
	request, err := http.NewRequestWithContext(ctx, http.MethodPut, grant.URL, io.TeeReader(file, uploadHash))
	if err != nil {
		return "", errors.New("recipe artifact upload request is invalid")
	}
	request.ContentLength = archive.SizeBytes
	// Suppress net/http's default User-Agent so the wire request contains only
	// the three signed Stack headers plus Content-Length (and HTTP's Host field).
	request.Header["User-Agent"] = nil
	for key, value := range grant.Headers {
		request.Header.Set(key, value)
	}
	response, err := uploader.client.Do(request)
	if err != nil {
		return "", errors.New("recipe artifact upload failed")
	}
	defer response.Body.Close()
	raw, readErr := io.ReadAll(io.LimitReader(response.Body, maxRecipeArtifactUploadResponse+1))
	if readErr != nil || len(raw) > maxRecipeArtifactUploadResponse || response.StatusCode < 200 || response.StatusCode >= 300 {
		return "", errors.New("recipe artifact upload was not accepted")
	}
	versionID := strings.TrimSpace(response.Header.Get("x-amz-version-id"))
	if !validRecipeArtifactVersionID(versionID) {
		return "", errors.New("recipe artifact upload version is invalid")
	}
	after, err := file.Stat()
	if err != nil || !os.SameFile(before, after) || !rootOwnedFile(after) || after.Size() != archive.SizeBytes || hex.EncodeToString(uploadHash.Sum(nil)) != archive.ArchiveSHA256 {
		return "", errors.New("recipe artifact archive changed during upload")
	}
	return versionID, nil
}

func validateRecipeArtifactUploadGrant(grant RecipeArtifactUploadGrant, archive TrustedRecipeArtifactArchive, now time.Time) error {
	if grant.Method != http.MethodPut || len(grant.Headers) != 3 || grant.Headers["Content-Type"] != RecipeArtifactTarMediaType ||
		grant.Headers["x-amz-server-side-encryption"] != "aws:kms" || !grant.ExpiresAt.After(now) {
		return errors.New("recipe artifact upload grant is invalid")
	}
	checksum, err := base64.StdEncoding.DecodeString(grant.Headers["x-amz-checksum-sha256"])
	archiveHash, hashErr := hex.DecodeString(archive.ArchiveSHA256)
	if err != nil || hashErr != nil || len(checksum) != sha256.Size || base64.StdEncoding.EncodeToString(checksum) != grant.Headers["x-amz-checksum-sha256"] || !bytes.Equal(checksum, archiveHash) {
		return errors.New("recipe artifact upload checksum is invalid")
	}
	parsed, err := url.Parse(grant.URL)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil || parsed.Fragment != "" || parsed.RawQuery == "" ||
		!strings.Contains(strings.ToLower(parsed.RawQuery), "x-amz-signature=") {
		return errors.New("recipe artifact upload URL is invalid")
	}
	return nil
}

func openVerifiedTrustedRecipeArchive(archive TrustedRecipeArtifactArchive) (*os.File, os.FileInfo, error) {
	path := strings.TrimSpace(archive.Path)
	before, err := os.Lstat(path)
	if err != nil || !before.Mode().IsRegular() || before.Mode()&os.ModeSymlink != 0 || !rootOwnedFile(before) || before.Mode().Perm()&0o022 != 0 || before.Size() != archive.SizeBytes {
		return nil, nil, ErrTrustedArtifactCatalogInvalid
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, nil, ErrTrustedArtifactCatalogInvalid
	}
	after, err := file.Stat()
	if err != nil || !os.SameFile(before, after) || !rootOwnedFile(after) {
		file.Close()
		return nil, nil, ErrTrustedArtifactCatalogInvalid
	}
	hash := sha256.New()
	written, err := io.Copy(hash, io.LimitReader(file, archive.SizeBytes+1))
	if err != nil || written != archive.SizeBytes || hex.EncodeToString(hash.Sum(nil)) != archive.ArchiveSHA256 {
		file.Close()
		return nil, nil, ErrTrustedArtifactCatalogInvalid
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		file.Close()
		return nil, nil, ErrTrustedArtifactCatalogInvalid
	}
	return file, before, nil
}
