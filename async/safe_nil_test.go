package async_test

import (
	"bytes"
	"context"
	"log"
	"os"
	"strings"
	"testing"

	"github.com/oekit/concurrency/async"
)

// captureStdLog 把标准库 log 的输出重定向到 buf（async 缺省 handler 走标准库
// log，验证「恢复默认」语义需要临时截获其输出），并注册还原。
func captureStdLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	log.SetOutput(&buf)
	log.SetFlags(0)
	t.Cleanup(func() {
		log.SetOutput(os.Stderr)
		log.SetFlags(log.LstdFlags)
	})
	return &buf
}

// TestSetPanicHandler_NilRestoresDefault SetPanicHandler(nil) 恢复默认 handler
// （标准库 log），而非静默关闭所有 panic 汇报（#165，对齐 gopool 同名 API 的
// nil 语义——两个同名 API 对 nil 不得一恢复一失明）。
func TestSetPanicHandler_NilRestoresDefault(t *testing.T) {
	customCalled := make(chan struct{}, 8)
	async.SetPanicHandler(func(ctx context.Context, r any, stack []byte) {
		customCalled <- struct{}{}
	})
	t.Cleanup(func() { async.SetPanicHandler(nil) })

	buf := captureStdLog(t)
	async.SetPanicHandler(nil)

	// RunSafe 同步执行：返回即代表默认 handler 已完成汇报
	async.RunSafe(func() { panic("nil-restore-boom") })

	out := buf.String()
	if !strings.Contains(out, "nil-restore-boom") || !strings.Contains(out, "[async] panic recovered") {
		t.Fatalf("默认 handler 未接管（输出：%q）", out)
	}
	select {
	case <-customCalled:
		t.Fatal("自定义 handler 不应再被调用")
	default:
	}
}
