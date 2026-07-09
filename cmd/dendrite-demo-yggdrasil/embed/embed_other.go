//go:build dendrite_p2p_demo && !elementweb
// +build dendrite_p2p_demo,!elementweb

package embed

import "github.com/gorilla/mux"

func Embed(_ *mux.Router, _ int, _ string) {

}
