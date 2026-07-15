package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudworker/artifactbuilder"
)

const maxCLIInputBytes = 1 << 20

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "cloud-worker-artifact: build failed")
		os.Exit(1)
	}
}

func run(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("cloud-worker-artifact", flag.ContinueOnError)
	flags.SetOutput(stderr)
	recipePath := flags.String("recipe", "", "strict RecipeV1 JSON file")
	specPath := flags.String("build-spec", "", "strict worker artifact BuildSpecV1 JSON file")
	workerPath := flags.String("worker-binary", "", "current cloud-worker binary")
	outputPath := flags.String("output", "", "new deterministic tar output")
	if flags.Parse(args) != nil || flags.NArg() != 0 || *recipePath == "" || *specPath == "" || *workerPath == "" || *outputPath == "" {
		return artifactbuilder.ErrInvalidBuildInput
	}
	recipe, err := readBoundedRegular(*recipePath)
	if err != nil {
		return err
	}
	spec, err := readBoundedRegular(*specPath)
	if err != nil {
		return err
	}
	output, err := os.OpenFile(*outputPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	keep := false
	defer func() {
		_ = output.Close()
		if !keep {
			_ = os.Remove(*outputPath)
		}
	}()
	result, err := artifactbuilder.BuildArchive(recipe, spec, *workerPath, output)
	if err != nil {
		return err
	}
	if err := output.Sync(); err != nil {
		return err
	}
	keep = true
	return json.NewEncoder(stdout).Encode(struct {
		CatalogDigest string `json:"catalog_digest"`
		ArchiveSHA256 string `json:"archive_sha256"`
		ArchiveBytes  uint64 `json:"archive_bytes"`
	}{result.CatalogDigest, result.ArchiveSHA256, result.ArchiveBytes})
}

func readBoundedRegular(path string) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > maxCLIInputBytes {
		return nil, artifactbuilder.ErrInvalidBuildInput
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil || !os.SameFile(info, opened) || !opened.Mode().IsRegular() || opened.Size() != info.Size() {
		return nil, artifactbuilder.ErrInvalidBuildInput
	}
	raw, err := io.ReadAll(io.LimitReader(file, maxCLIInputBytes+1))
	if err != nil || int64(len(raw)) != opened.Size() {
		return nil, artifactbuilder.ErrInvalidBuildInput
	}
	return raw, nil
}
