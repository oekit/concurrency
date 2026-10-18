package httpx_test

import (
	"net/http"
	"testing"

	"github.com/oekit/concurrency/retry/httpx"
)

// TestRetryableWithHeaders 403 头感知判定（#22——GitHub 次级限流/主限流耗尽
// 走 403，裸状态码判定会误判为永久失败）。
func TestRetryableWithHeaders(t *testing.T) {
	// 与 Retryable 同口径的部分：429/5xx 重试，其余 4xx 不重试
	if !httpx.RetryableWithHeaders(429, nil) {
		t.Fatal("429 must be retryable")
	}
	if !httpx.RetryableWithHeaders(503, nil) {
		t.Fatal("5xx must be retryable")
	}
	if httpx.RetryableWithHeaders(404, nil) {
		t.Fatal("404 must not be retryable")
	}

	// 403 无头（nil/空）→ 权限拒绝，重试永远失败，不可重试
	if httpx.RetryableWithHeaders(403, nil) {
		t.Fatal("bare 403 (nil header) must not be retryable")
	}
	if httpx.RetryableWithHeaders(403, http.Header{}) {
		t.Fatal("bare 403 (empty header) must not be retryable")
	}

	// 403 + Retry-After → 次级限流/滥用检测，可重试
	h := http.Header{}
	h.Set("Retry-After", "60")
	if !httpx.RetryableWithHeaders(403, h) {
		t.Fatal("403 + Retry-After must be retryable (secondary rate limit)")
	}

	// 403 + X-RateLimit-Remaining: 0 → 主限流耗尽，等 reset 窗口，可重试
	h2 := http.Header{}
	h2.Set("X-RateLimit-Remaining", "0")
	if !httpx.RetryableWithHeaders(403, h2) {
		t.Fatal("403 + Remaining:0 must be retryable (primary limit exhausted)")
	}

	// Remaining 非 0 且无 Retry-After → 权限类 403，不可重试
	h3 := http.Header{}
	h3.Set("X-RateLimit-Remaining", "12")
	if httpx.RetryableWithHeaders(403, h3) {
		t.Fatal("403 + Remaining:12 without Retry-After must not be retryable")
	}

	// 头感知只扩 403：其他 4xx 带头也不可重试
	h4 := http.Header{}
	h4.Set("Retry-After", "1")
	if httpx.RetryableWithHeaders(404, h4) {
		t.Fatal("404 with Retry-After must not be retryable")
	}

	// http.Header 经 MIME 规范键查找，大小写不敏感
	h5 := http.Header{}
	h5.Set("rEtRy-aFtEr", "5")
	if !httpx.RetryableWithHeaders(403, h5) {
		t.Fatal("403 header lookup must be MIME-canonical case-insensitive")
	}
}

// mapHeader 是自研客户端 header 类型的替身：实现 Get 即满足 HeaderSource。
type mapHeader map[string]string

func (m mapHeader) Get(key string) string { return m[key] }

// TestRetryableWithHeaders_CustomSource 非标准 header 类型接入（包保持零依赖）。
func TestRetryableWithHeaders_CustomSource(t *testing.T) {
	if !httpx.RetryableWithHeaders(403, mapHeader{"X-RateLimit-Remaining": "0"}) {
		t.Fatal("custom header source with Remaining:0 must be retryable")
	}
	if httpx.RetryableWithHeaders(403, mapHeader{}) {
		t.Fatal("custom header source without signals must not be retryable")
	}
}
