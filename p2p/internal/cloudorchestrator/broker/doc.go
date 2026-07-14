// Package broker implements the narrow, signed Connection Stack V2 client used
// by the Cloud Orchestrator. It deliberately exposes only read-only
// quote.request and fixed connection.registration.verify commands, and has no
// AWS SDK dependency.
package broker
