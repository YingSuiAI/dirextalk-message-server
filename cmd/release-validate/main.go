package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/YingSuiAI/dirextalk-message-server/internal/releasecontrol"
)

func main() {
	manifestPath := flag.String("manifest", "", "path to release-manifest.json")
	manifestChecksumPath := flag.String("manifest-checksum", "", "path to release-manifest.json.sha256")
	indexPath := flag.String("index", "", "path to release-index.json")
	indexChecksumPath := flag.String("index-checksum", "", "path to release-index.json.sha256")
	flag.Parse()
	if *manifestPath == "" || *manifestChecksumPath == "" || *indexPath == "" || *indexChecksumPath == "" {
		fatalf("manifest, index, and both checksum paths are required")
	}
	manifestData, err := os.ReadFile(*manifestPath)
	if err != nil {
		fatalf("read manifest: %v", err)
	}
	manifestChecksum, err := os.ReadFile(*manifestChecksumPath)
	if err != nil {
		fatalf("read manifest checksum: %v", err)
	}
	indexData, err := os.ReadFile(*indexPath)
	if err != nil {
		fatalf("read index: %v", err)
	}
	indexChecksum, err := os.ReadFile(*indexChecksumPath)
	if err != nil {
		fatalf("read index checksum: %v", err)
	}
	index, err := releasecontrol.ValidateReleaseArtifacts(manifestData, manifestChecksum, indexData, indexChecksum)
	if err != nil {
		fatalf("validate release assets: %v", err)
	}
	latest := index.Releases[len(index.Releases)-1]
	fmt.Printf("validated %s %s\n", latest.Manifest.Version, latest.ManifestDigest)
}

func fatalf(format string, values ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", values...)
	os.Exit(1)
}
