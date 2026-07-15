//go:build linux

package runtime

import (
	"os"
	"syscall"
)

func rootOwnedFile(info os.FileInfo) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && stat.Uid == 0
}
