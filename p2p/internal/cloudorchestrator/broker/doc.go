// Package broker implements the narrow, signed Connection Stack V2 client used
// by the Cloud Orchestrator. It deliberately exposes only read-only
// quote.request, fixed connection.registration.verify, and the private,
// approval-bound deployment.create command; it has no AWS SDK dependency.
package broker
