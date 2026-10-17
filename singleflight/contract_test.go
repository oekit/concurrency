package singleflight

import (
	"testing"

	"github.com/oekit/pkg/singleflighttest"
)

// TestContract 跑共享语义契约（与 pkg/internal/singleflight 同一套）。
// 任一侧语义漂移时双方 CI 同时红。见 oekit/pkg#4。
func TestContract(t *testing.T) {
	singleflighttest.Run[int](t, func() singleflighttest.Flight[int] {
		return New[int]()
	})
}
