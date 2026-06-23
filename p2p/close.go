package p2p

type closeErrorer interface {
	Close() error
}

func closeResource(resource closeErrorer) {
	if resource != nil {
		_ = resource.Close()
	}
}
