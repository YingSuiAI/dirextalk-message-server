package projector

import (
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type consumerMetrics struct {
	mu                    sync.Mutex
	received              uint64
	processed             uint64
	discarded             uint64
	failed                uint64
	consecutiveFailures   uint64
	lastSuccessUnix       int64
	lastFailureUnix       int64
	lastMessageAgeSeconds float64
}

type consumerMetricsSnapshot struct {
	Received              uint64
	Processed             uint64
	Discarded             uint64
	Failed                uint64
	ConsecutiveFailures   uint64
	LastSuccessUnix       int64
	LastFailureUnix       int64
	LastMessageAgeSeconds float64
}

var defaultConsumerMetrics = newConsumerMetrics()

var consumerEvents = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "dirextalk_message_server",
		Subsystem: "p2p_projector",
		Name:      "consumer_events_total",
		Help:      "Total P2P projector roomserver output consumer events by result.",
	},
	[]string{"result"},
)

func init() {
	promauto.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "dirextalk_message_server",
		Subsystem: "p2p_projector",
		Name:      "consumer_consecutive_failures",
		Help:      "Current consecutive P2P projector consumer failures.",
	}, func() float64 {
		return float64(defaultConsumerMetrics.snapshot().ConsecutiveFailures)
	})
	promauto.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "dirextalk_message_server",
		Subsystem: "p2p_projector",
		Name:      "consumer_last_success_unixtime",
		Help:      "Unix time of the last successful P2P projector consumer message.",
	}, func() float64 {
		return float64(defaultConsumerMetrics.snapshot().LastSuccessUnix)
	})
	promauto.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "dirextalk_message_server",
		Subsystem: "p2p_projector",
		Name:      "consumer_last_failure_unixtime",
		Help:      "Unix time of the last failed P2P projector consumer message.",
	}, func() float64 {
		return float64(defaultConsumerMetrics.snapshot().LastFailureUnix)
	})
	promauto.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "dirextalk_message_server",
		Subsystem: "p2p_projector",
		Name:      "consumer_last_message_age_seconds",
		Help:      "Age in seconds of the last P2P projector roomserver output message when metadata is available.",
	}, func() float64 {
		return defaultConsumerMetrics.snapshot().LastMessageAgeSeconds
	})
}

func newConsumerMetrics() *consumerMetrics {
	return &consumerMetrics{}
}

func (m *consumerMetrics) recordReceived(msg *nats.Msg) {
	if m == nil {
		return
	}
	now := time.Now()
	var ageSeconds float64
	if msg != nil {
		if metadata, err := msg.Metadata(); err == nil {
			ageSeconds = now.Sub(metadata.Timestamp).Seconds()
			if ageSeconds < 0 {
				ageSeconds = 0
			}
		}
	}
	m.mu.Lock()
	m.received++
	if ageSeconds > 0 {
		m.lastMessageAgeSeconds = ageSeconds
	}
	m.mu.Unlock()
	consumerEvents.WithLabelValues("received").Inc()
}

func (m *consumerMetrics) recordProcessed() {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.processed++
	m.consecutiveFailures = 0
	m.lastSuccessUnix = time.Now().Unix()
	m.mu.Unlock()
	consumerEvents.WithLabelValues("processed").Inc()
}

func (m *consumerMetrics) recordDiscarded() {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.discarded++
	m.mu.Unlock()
	consumerEvents.WithLabelValues("discarded").Inc()
}

func (m *consumerMetrics) recordFailed() {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.failed++
	m.consecutiveFailures++
	m.lastFailureUnix = time.Now().Unix()
	m.mu.Unlock()
	consumerEvents.WithLabelValues("failed").Inc()
}

func (m *consumerMetrics) snapshot() consumerMetricsSnapshot {
	if m == nil {
		return consumerMetricsSnapshot{}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return consumerMetricsSnapshot{
		Received:              m.received,
		Processed:             m.processed,
		Discarded:             m.discarded,
		Failed:                m.failed,
		ConsecutiveFailures:   m.consecutiveFailures,
		LastSuccessUnix:       m.lastSuccessUnix,
		LastFailureUnix:       m.lastFailureUnix,
		LastMessageAgeSeconds: m.lastMessageAgeSeconds,
	}
}
