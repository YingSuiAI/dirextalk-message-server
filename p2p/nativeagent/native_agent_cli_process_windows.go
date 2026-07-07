//go:build windows

package nativeagent

import (
	"context"
	"os/exec"
)

func prepareRuntimeCommand(*exec.Cmd) {}

func killRuntimeCommandOnCancel(context.Context, *exec.Cmd, <-chan struct{}) {}
