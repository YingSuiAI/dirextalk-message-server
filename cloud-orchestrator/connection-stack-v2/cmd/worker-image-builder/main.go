package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/workerimage"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func main() {
	if err := run(context.Background(), os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "worker-image-builder: operation failed")
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return workerimage.ErrInvalidConfig
	}
	switch args[0] {
	case "build":
		return runBuild(ctx, args[1:], stdout, stderr)
	case "verify", "destroy":
		return runManifestAction(ctx, args[0], args[1:], stdout, stderr)
	default:
		return workerimage.ErrInvalidConfig
	}
}

func runBuild(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	options, err := parseBuildOptions(args, stderr)
	if err != nil {
		return err
	}
	artifact, err := workerimage.ValidateArchive(options.archive)
	if err != nil {
		return err
	}
	provider, builder, err := newAWSBuilder(ctx, options.config.Region)
	if err != nil {
		return err
	}
	_ = provider
	manifest, err := builder.Build(ctx, options.config, artifact)
	if err != nil {
		return err
	}
	if err := writeNewJSON(options.output, manifest); err != nil {
		return err
	}
	return json.NewEncoder(stdout).Encode(struct {
		ImageID              string `json:"image_id"`
		TrustedCatalogDigest string `json:"trusted_catalog_digest"`
		RecipeArtifactMode   string `json:"recipe_artifact_mode"`
	}{manifest.ImageID, manifest.TrustedCatalogDigest, manifest.RecipeArtifactMode})
}

type buildOptions struct {
	archive string
	output  string
	config  workerimage.BuildConfig
}

func parseBuildOptions(args []string, stderr io.Writer) (buildOptions, error) {
	flags := flag.NewFlagSet("build", flag.ContinueOnError)
	flags.SetOutput(stderr)
	archive := flags.String("artifact", "", "Stage S deterministic tar")
	region := flags.String("region", "", "AWS Region")
	baseAMI := flags.String("base-ami", "", "immutable base AMI id")
	subnet := flags.String("subnet", "", "private builder subnet id")
	securityGroup := flags.String("security-group", "", "zero-ingress security group id")
	bucket := flags.String("bucket", "", "versioned S3 bucket")
	key := flags.String("key", "", "temporary versioned S3 object key")
	version := flags.String("version", "", "immutable prerelease SemVer")
	instanceType := flags.String("instance-type", "m7i.large", "fixed builder instance type")
	ociSource := flags.String("oci-source", "", "supported registry/repository@sha256 digest")
	dynamicArtifacts := flags.Bool("dynamic-artifacts", false, "build a runtime-only AMI that resolves approved Recipe artifacts at task time")
	output := flags.String("output", "", "new image manifest JSON path")
	timeout := flags.Duration("timeout", 45*time.Minute, "build deadline")
	if flags.Parse(args) != nil || flags.NArg() != 0 || *archive == "" || *region == "" || *baseAMI == "" || *subnet == "" || *securityGroup == "" || *bucket == "" || *key == "" || *version == "" || *ociSource == "" || *output == "" {
		return buildOptions{}, workerimage.ErrInvalidConfig
	}
	return buildOptions{
		archive: *archive,
		output:  *output,
		config: workerimage.BuildConfig{
			Region:                 *region,
			BaseAMIID:              *baseAMI,
			SubnetID:               *subnet,
			SecurityGroupID:        *securityGroup,
			Bucket:                 *bucket,
			ObjectKey:              *key,
			ArtifactVersion:        *version,
			InstanceType:           *instanceType,
			OCISource:              *ociSource,
			DynamicRecipeArtifacts: *dynamicArtifacts,
			Timeout:                *timeout,
		},
	}, nil
}

func runManifestAction(ctx context.Context, action string, args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet(action, flag.ContinueOnError)
	flags.SetOutput(stderr)
	manifestPath := flags.String("manifest", "", "strict image manifest JSON")
	region := flags.String("region", "", "AWS Region")
	if flags.Parse(args) != nil || flags.NArg() != 0 || *manifestPath == "" || *region == "" {
		return workerimage.ErrInvalidConfig
	}
	raw, err := readRegular(*manifestPath, 1<<20)
	if err != nil {
		return err
	}
	manifest, err := workerimage.ParseImageManifest(raw)
	if err != nil || manifest.Region != *region {
		return workerimage.ErrInvalidConfig
	}
	_, builder, err := newAWSBuilder(ctx, *region)
	if err != nil {
		return err
	}
	if action == "verify" {
		err = builder.Verify(ctx, manifest)
	} else {
		err = builder.Destroy(ctx, manifest)
	}
	if err != nil {
		return err
	}
	return json.NewEncoder(stdout).Encode(struct {
		Status  string `json:"status"`
		ImageID string `json:"image_id"`
	}{"ok", manifest.ImageID})
}

func newAWSBuilder(ctx context.Context, region string) (*workerimage.AWSProvider, *workerimage.Builder, error) {
	configuration, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil {
		return nil, nil, err
	}
	provider, err := workerimage.NewAWSProvider(ec2.NewFromConfig(configuration), s3.NewFromConfig(configuration))
	if err != nil {
		return nil, nil, err
	}
	builder, err := workerimage.NewBuilder(provider, nil)
	return provider, builder, err
}
func writeNewJSON(path string, value any) (err error) {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	keep := false
	defer func() {
		_ = file.Close()
		if !keep {
			_ = os.Remove(path)
		}
	}()
	encoder := json.NewEncoder(file)
	encoder.SetEscapeHTML(false)
	if err = encoder.Encode(value); err != nil {
		return err
	}
	if err = file.Sync(); err != nil {
		return err
	}
	keep = true
	return nil
}
func readRegular(path string, maximum int64) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > maximum {
		return nil, workerimage.ErrInvalidConfig
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil || !os.SameFile(info, opened) || opened.Size() != info.Size() {
		return nil, workerimage.ErrInvalidConfig
	}
	raw, err := io.ReadAll(io.LimitReader(file, maximum+1))
	if err != nil || int64(len(raw)) != opened.Size() {
		return nil, workerimage.ErrInvalidConfig
	}
	return raw, nil
}
