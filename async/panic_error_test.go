package async_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/oekit/concurrency/async"
)

func TestPanicError_ErrorText(t *testing.T) {
	pe := &async.PanicError{Value: "boom"}
	if got := pe.Error(); got != "panic: boom" {
		t.Fatalf("Error() = %q, want %q", got, "panic: boom")
	}
	labeled := &async.PanicError{Value: "boom", Task: "janitor"}
	if got := labeled.Error(); got != "task=janitor: panic: boom" {
		t.Fatalf("Error() labeled = %q", got)
	}
}

func TestRunSafeErr_TypedPanicError(t *testing.T) {
	err := async.RunSafeErr(func() error {
		panic("boom-payload")
	})
	var pe *async.PanicError
	if !errors.As(err, &pe) {
		t.Fatalf("err = %v, want *PanicError", err)
	}
	if pe.Value != "boom-payload" {
		t.Fatalf("Value = %v", pe.Value)
	}
	if len(pe.Stack) == 0 || !strings.Contains(string(pe.Stack), "goroutine") {
		t.Fatal("Stack missing")
	}
	// Error() 文案与旧实现一致（现有字符串匹配零破坏）
	if !strings.HasPrefix(err.Error(), "panic: boom-payload") {
		t.Fatalf("Error() = %q", err.Error())
	}
}

func TestRunSafeRet_TypedPanicError(t *testing.T) {
	_, err := async.RunSafeRet(func() (int, error) {
		panic(42)
	})
	var pe *async.PanicError
	if !errors.As(err, &pe) || pe.Value != 42 {
		t.Fatalf("err = %v, want *PanicError(42)", err)
	}
}

func TestRunSafeRetLabeled_TypedPanicError(t *testing.T) {
	_, err := async.RunSafeRetLabeled("worker-1", func() (string, error) {
		panic("oom")
	})
	var pe *async.PanicError
	if !errors.As(err, &pe) || pe.Task != "worker-1" || pe.Value != "oom" {
		t.Fatalf("err = %v, want *PanicError(task=worker-1)", err)
	}
	if got := pe.Error(); got != "task=worker-1: panic: oom" {
		t.Fatalf("Error() = %q", got)
	}
}

func TestPanicError_WrapsPreservesAs(t *testing.T) {
	// 消费方 fmt.Errorf %w 包装后仍可 errors.As 还原
	base := &async.PanicError{Value: "root"}
	wrapped := fmt.Errorf("worker: queue panic: %w", base)
	var pe *async.PanicError
	if !errors.As(wrapped, &pe) || pe.Value != "root" {
		t.Fatalf("As after wrap broken: %v", wrapped)
	}
}

// TestPanicError_Unwrap #267 回归：error 型 panic 的错误身份全链不丢——
// errors.Is / errors.As 穿透 PanicError 直达业务侧 panic 出去的原始错误
// （require 类/断言失败的常见形态：panic 携带 error）。
func TestPanicError_Unwrap(t *testing.T) {
	sentinel := errors.New("assert failed")

	err := async.RunSafeErr(func() error {
		panic(sentinel)
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("errors.Is 应穿透 PanicError，得到 %v", err)
	}
	var pe *async.PanicError
	if !errors.As(err, &pe) || pe.Value != sentinel {
		t.Fatalf("errors.As(*PanicError) 失败: %v", err)
	}

	// 直接构造的载体同样穿透（导出面自洽，不依赖收编路径）
	if !errors.Is(&async.PanicError{Value: sentinel}, sentinel) {
		t.Fatal("字面量构造的 PanicError 应透传 Unwrap")
	}

	// 非 error 的 panic 值：Unwrap 返回 nil，不伪造错误身份
	_, err2 := async.RunSafeRet(func() (int, error) {
		panic("plain string panic")
	})
	if unwrapped := errors.Unwrap(err2); unwrapped != nil {
		t.Fatalf("非 error panic 值不应有 Unwrap，得到 %v", unwrapped)
	}
	if !errors.As(err2, &pe) || pe.Value != "plain string panic" {
		t.Fatalf("errors.As(*PanicError) 失败: %v", err2)
	}
}
