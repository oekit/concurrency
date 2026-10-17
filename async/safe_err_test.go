package async

import (
	"errors"
	"testing"
)

func TestRunSafeErr_Normal(t *testing.T) {
	want := errors.New("biz")
	if err := RunSafeErr(func() error { return want }); !errors.Is(err, want) {
		t.Fatalf("err=%v", err)
	}
	if err := RunSafeErr(func() error { return nil }); err != nil {
		t.Fatalf("err=%v", err)
	}
}

func TestRunSafeErr_PanicBecomesError(t *testing.T) {
	if err := RunSafeErr(func() error { panic("boom") }); err == nil {
		t.Fatal("panic must become error")
	}
}

func TestRunSafeRet_DistinguishesZeroAndPanic(t *testing.T) {
	v, err := RunSafeRet(func() (int, error) { return 0, nil })
	if v != 0 || err != nil {
		t.Fatalf("v=%v err=%v", v, err)
	}
	v, err = RunSafeRet(func() (int, error) { panic("x") })
	if v != 0 || err == nil {
		t.Fatalf("panic: v=%v err=%v want err!=nil", v, err)
	}
}
