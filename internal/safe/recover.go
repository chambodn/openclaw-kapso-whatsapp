// Package safe provides shared goroutine-safety helpers.
package safe

import (
	"log"
	"runtime/debug"
)

// Recover logs and swallows a panic so a single bad message, frame, or handler
// cannot crash the daemon via a long-lived goroutine. Defer it at the top of a
// goroutine; for a dispatch loop, defer it around each iteration (in a closure)
// so one bad event is logged and skipped rather than killing the whole loop.
func Recover(label string) {
	if r := recover(); r != nil {
		log.Printf("recovered panic in %s: %v\n%s", label, r, debug.Stack())
	}
}
