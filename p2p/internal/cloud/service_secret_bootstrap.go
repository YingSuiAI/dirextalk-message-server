package cloud

import (
	"context"
	"errors"
	"net/url"
	"regexp"
	"strings"

	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
)

var (
	ErrServiceSecretBootstrapInvalid  = errors.New("service secret bootstrap plan is invalid")
	ErrServiceSecretBootstrapConflict = errors.New("service secret bootstrap plan conflicts with current state")
)

type PrepareServiceSecretBootstrapRequest struct {
	OwnerMXID        string
	DeploymentID     string
	SlotID           string
	ExpectedRevision int64
	IdempotencyHash  string
	RequestDigest    string
	SessionID        string
	ApprovalID       string
	ChallengeID      string
	CreatedAt        int64
	ExpiresAt        int64
}

type ServiceSecretBootstrapConfirmation struct {
	Approval cloudcontracts.ServiceSecretApprovalV1 `json:"approval"`
}

type PrepareServiceSecretBootstrapResult struct {
	Confirmation ServiceSecretBootstrapConfirmation `json:"confirmation"`
	StackBaseURL string                             `json:"stack_base_url"`
	Created      bool                               `json:"-"`
}

type ServiceSecretBootstrapStore interface {
	PrepareCloudServiceSecretBootstrap(context.Context, PrepareServiceSecretBootstrapRequest) (PrepareServiceSecretBootstrapResult, error)
}

var stackStageSegmentPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

func StackBaseURLFromBrokerCommandURL(raw string) (string, error) {
	parsed, err := url.ParseRequestURI(raw)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.Hostname() == "" || parsed.Port() != "" || parsed.User != nil || parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != "" || parsed.RawPath != "" {
		return "", ErrServiceSecretBootstrapInvalid
	}
	segments := strings.Split(strings.TrimPrefix(parsed.Path, "/"), "/")
	if len(segments) == 2 && segments[0] == "v2" && segments[1] == "commands" {
		parsed.Path = ""
	} else if len(segments) == 3 && stackStageSegmentPattern.MatchString(segments[0]) && segments[1] == "v2" && segments[2] == "commands" {
		parsed.Path = "/" + segments[0]
	} else {
		return "", ErrServiceSecretBootstrapInvalid
	}
	return parsed.String(), nil
}
