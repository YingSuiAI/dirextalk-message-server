package storage

import "github.com/sirupsen/logrus"

type closeErrorer interface {
	Close() error
}

func closeResource(resource closeErrorer) {
	if err := resource.Close(); err != nil {
		logrus.WithError(err).Warn("failed to close resource")
	}
}
