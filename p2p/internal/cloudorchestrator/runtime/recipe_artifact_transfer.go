package runtime

import (
	"context"
	"errors"
	"time"
)

type RecipeArtifactTransferManager struct {
	store     RecipeArtifactTransferStore
	transport RecipeArtifactTransferTransport
	uploader  RecipeArtifactUploader
	archive   TrustedRecipeArtifactArchive
	now       func() time.Time
}

const recipeArtifactDurableWriteTimeout = 5 * time.Second

func NewRecipeArtifactTransferManager(store RecipeArtifactTransferStore, transport RecipeArtifactTransferTransport, uploader RecipeArtifactUploader, archive TrustedRecipeArtifactArchive, now func() time.Time) (*RecipeArtifactTransferManager, error) {
	if store == nil || transport == nil || uploader == nil || archive.Validate() != nil {
		return nil, errors.New("recipe artifact transfer manager configuration is invalid")
	}
	if now == nil {
		now = time.Now
	}
	return &RecipeArtifactTransferManager{store: store, transport: transport, uploader: uploader, archive: archive, now: now}, nil
}

func (manager *RecipeArtifactTransferManager) Ensure(ctx context.Context, claim RecipeInstallClaim) error {
	if manager == nil || ctx == nil || manager.store == nil || manager.transport == nil || manager.uploader == nil || manager.archive.Validate() != nil ||
		ValidateRecipeInstallClaim(claim) != nil || claim.Phase != RecipeInstallPhaseIssue ||
		claim.Manifest.RecipeDigest != manager.archive.RecipeDigest || claim.Manifest.ArtifactDigest != manager.archive.ArtifactDigest ||
		claim.Manifest.WorkerResourceManifestDigest != manager.archive.WorkerResourceManifestDigest {
		return errors.New("recipe artifact transfer binding is invalid")
	}
	binding := RecipeArtifactTransferBinding{
		DeploymentID: claim.DeploymentID, TaskID: claim.TaskID, ExecutionID: claim.ExecutionID,
		RecipeDigest: claim.Manifest.RecipeDigest, ArtifactDigest: claim.Manifest.ArtifactDigest, ManifestDigest: claim.ManifestDigest,
		ArchiveSHA256: manager.archive.ArchiveSHA256, SizeBytes: manager.archive.SizeBytes, MediaType: RecipeArtifactTarMediaType,
	}
	if binding.Validate() != nil {
		return errors.New("recipe artifact transfer binding is invalid")
	}
	for step := 0; step < 2; step++ {
		transfer, err := manager.store.LoadOrCreateRecipeArtifactTransfer(ctx, claim, binding)
		if err != nil {
			return err
		}
		if transfer.Binding != binding || transfer.ConnectionID != claim.ConnectionID {
			return errors.New("recipe artifact transfer store returned a conflicting binding")
		}
		if transfer.Phase == RecipeArtifactTransferPhaseVerified {
			return nil
		}
		if !recipeArtifactCommandBindsClaim(claim, transfer, binding) {
			return errors.New("recipe artifact transfer store returned a conflicting command")
		}
		signed := SignedRecipeArtifactTransferCommand{
			EnvelopeJSON: transfer.Command.SignedEnvelope, PayloadJSON: transfer.Command.PayloadJSON,
			PayloadSHA256: transfer.Command.PayloadSHA256, RequestSHA256: transfer.Command.RequestSHA256,
			IssuedAt: transfer.Command.IssuedAt, ExpiresAt: transfer.Command.ExpiresAt,
		}
		if signed.EnvelopeJSON == "" {
			switch transfer.Phase {
			case RecipeArtifactTransferPhasePrepare:
				request := RecipeArtifactPutPrepareRequest{Schema: RecipeArtifactPutPrepareSchema, RecipeArtifactTransferBinding: binding}
				signed, err = manager.transport.BuildRecipeArtifactPrepareCommand(transfer.Command, request, manager.now().UTC())
			case RecipeArtifactTransferPhaseComplete:
				request := RecipeArtifactPutCompleteRequest{Schema: RecipeArtifactPutCompleteSchema, RecipeArtifactTransferBinding: binding, VersionID: transfer.VersionID}
				signed, err = manager.transport.BuildRecipeArtifactCompleteCommand(transfer.Command, request, manager.now().UTC())
			default:
				err = errors.New("recipe artifact transfer phase is invalid")
			}
			if err != nil || validateRecipeArtifactTransferCommand(transfer.Command, signed) != nil {
				return errors.New("recipe artifact transfer command is invalid")
			}
			if err = manager.store.PersistRecipeArtifactTransferCommand(ctx, claim, transfer, signed); err != nil {
				return err
			}
		}
		if validateRecipeArtifactTransferCommand(transfer.Command, signed) != nil {
			return errors.New("persisted recipe artifact transfer command is invalid")
		}
		switch transfer.Phase {
		case RecipeArtifactTransferPhasePrepare:
			request := RecipeArtifactPutPrepareRequest{Schema: RecipeArtifactPutPrepareSchema, RecipeArtifactTransferBinding: binding}
			grant, err := manager.transport.RequestRecipeArtifactPrepare(ctx, claim.BrokerEndpoint, transfer.Command, signed, request)
			if err != nil {
				return err
			}
			versionID, err := manager.uploader.Upload(ctx, manager.archive, grant)
			if err != nil {
				return err
			}
			if !validRecipeArtifactVersionID(versionID) {
				return errors.New("recipe artifact upload version is invalid")
			}
			// The immutable S3 version is durable before a complete command can
			// be allocated or sent. The presigned URL and headers are discarded.
			writeCtx, writeCancel := recipeArtifactDurableWriteContext(ctx)
			err = manager.store.RecordRecipeArtifactVersion(writeCtx, claim, transfer, versionID)
			writeCancel()
			if err != nil {
				return err
			}
		case RecipeArtifactTransferPhaseComplete:
			request := RecipeArtifactPutCompleteRequest{Schema: RecipeArtifactPutCompleteSchema, RecipeArtifactTransferBinding: binding, VersionID: transfer.VersionID}
			if err := manager.transport.RequestRecipeArtifactComplete(ctx, claim.BrokerEndpoint, transfer.Command, signed, request); err != nil {
				return err
			}
			writeCtx, writeCancel := recipeArtifactDurableWriteContext(ctx)
			err := manager.store.CommitRecipeArtifactTransfer(writeCtx, claim, transfer)
			writeCancel()
			if err != nil {
				return err
			}
			return nil
		default:
			return errors.New("recipe artifact transfer phase is invalid")
		}
	}
	return errors.New("recipe artifact transfer did not reach verified state")
}

func recipeArtifactDurableWriteContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), recipeArtifactDurableWriteTimeout)
}

func recipeArtifactCommandBindsClaim(claim RecipeInstallClaim, transfer RecipeArtifactTransfer, binding RecipeArtifactTransferBinding) bool {
	command := transfer.Command
	if command.ExecutionID != claim.ExecutionID || command.DeploymentID != claim.DeploymentID || command.TaskID != claim.TaskID ||
		command.ConnectionID != claim.ConnectionID || command.NodeKeyID != claim.NodeKeyID || command.ExpectedGeneration != claim.ExpectedGeneration ||
		command.Phase != transfer.Phase || command.Action != RecipeArtifactPutAction {
		return false
	}
	var digest string
	var err error
	switch transfer.Phase {
	case RecipeArtifactTransferPhasePrepare:
		if transfer.VersionID != "" {
			return false
		}
		digest, err = (RecipeArtifactPutPrepareRequest{Schema: RecipeArtifactPutPrepareSchema, RecipeArtifactTransferBinding: binding}).Digest()
	case RecipeArtifactTransferPhaseComplete:
		if !validRecipeArtifactVersionID(transfer.VersionID) {
			return false
		}
		digest, err = (RecipeArtifactPutCompleteRequest{Schema: RecipeArtifactPutCompleteSchema, RecipeArtifactTransferBinding: binding, VersionID: transfer.VersionID}).Digest()
	default:
		return false
	}
	return err == nil && command.RequestDigest == digest
}
