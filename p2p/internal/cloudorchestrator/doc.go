// Package cloudorchestrator defines versioned, provider-neutral control-plane
// contracts for the separately deployed Cloud Orchestrator.
//
// It is intentionally independent from ProductCore action and persistence
// code. It contains no provider client, credential transport, shell execution,
// or worker control capability. Only opaque secret_ref values may appear in a
// plan; secret values are rejected by validation.
package cloudorchestrator
