package httpx_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/oekit/concurrency/retry/httpx"
)

// TestParseRetryAfter 表驱动：delay-seconds 与 HTTP-date（RFC 7231 双形态）
// 的忠实解析（#198）——负秒数拒绝、过去时刻钳 0、坏值 (0,false)。
func TestParseRetryAfter(t *testing.T) {
	now := time.Date(2026, 11, 8, 8, 0, 0, 0, time.UTC)

	tests := []struct {
		name   string
		value  string
		want   time.Duration
		wantOK bool
		atol   time.Duration // date 形态按差值比对时容忍的偏差（0 = 精确比对）
	}{
		// —— 形态一：delay-seconds ——
		{"zero seconds means retry now", "0", 0, true, 0},
		{"plain seconds", "120", 120 * time.Second, true, 0},
		{"explicit plus sign", "+45", 45 * time.Second, true, 0},
		{"leading zeros", "007", 7 * time.Second, true, 0},
		{"negative seconds rejected", "-10", 0, false, 0},
		{"fractional seconds rejected", "1.5", 0, false, 0},
		{"unit suffix rejected", "120s", 0, false, 0},
		{"overflow seconds rejected", "99999999999999999999999", 0, false, 0},

		// —— 秒→时长乘法溢出（#213/#215）——
		// 上界恰不溢出：9223372036 s = MaxInt64/int64(time.Second)
		{"max representable seconds accepted", "9223372036", 9223372036 * time.Second, true, 0},
		// 首个溢出秒数：裸乘回绕成负时长且 ok=true（修复前），契约钉死为 (0, false)
		{"first overflowing second rejected", "9223372037", 0, false, 0},
		// 回绕成**小正值**的形态（≈0.29s）：只钉负值会漏掉这一格
		{"wrap to small positive rejected", "18446744074", 0, false, 0},
		// 10 位秒数（Atoi 在 64 位平台成功、折算必溢出）
		{"ten-digit overflow rejected", "9999999999", 0, false, 0},

		// —— 形态二：HTTP-date（IMF-fixdate，必须接受）——
		{
			"future date gives delta",
			now.Add(2 * time.Minute).UTC().Format(http.TimeFormat),
			2 * time.Minute, true, time.Second,
		},
		{
			"past date clamps to zero",
			now.Add(-1 * time.Hour).UTC().Format(http.TimeFormat),
			0, true, 0,
		},
		{
			"exact now clamps to zero",
			now.UTC().Format(http.TimeFormat),
			0, true, 0,
		},

		// —— 形态二：废弃 RFC 850 / asctime（应兼容）——
		{
			"RFC 850 date",
			now.Add(30 * time.Second).Format(time.RFC850),
			30 * time.Second, true, time.Second,
		},
		{
			"asctime date",
			now.Add(90 * time.Second).Format(time.ANSIC),
			90 * time.Second, true, time.Second,
		},

		// —— 坏值 / 空值 ——
		{"empty string rejected", "", 0, false, 0},
		{"whitespace-only rejected", "   ", 0, false, 0},
		{"garbage rejected", "soon", 0, false, 0},
		{"malformed date rejected", "Wed, 32 Oct 2026 07:28:00 GMT", 0, false, 0},
		{"bare word GMT rejected", "GMT", 0, false, 0},

		// —— OWS 修剪（#26 口径：自造来源可能带空白）——
		{"padded seconds trimmed", "  60  ", 60 * time.Second, true, 0},
		{
			"padded date trimmed",
			"  " + now.Add(time.Minute).UTC().Format(http.TimeFormat) + " ",
			time.Minute, true, time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := httpx.ParseRetryAfter(tt.value, now)
			if ok != tt.wantOK {
				t.Fatalf("ParseRetryAfter(%q) ok = %v, want %v", tt.value, ok, tt.wantOK)
			}
			if !tt.wantOK {
				if got != 0 {
					t.Fatalf("ParseRetryAfter(%q) = %v with ok=false, want 0", tt.value, got)
				}
				return
			}
			if tt.atol == 0 {
				if got != tt.want {
					t.Fatalf("ParseRetryAfter(%q) = %v, want %v", tt.value, got, tt.want)
				}
				return
			}
			if got < tt.want-tt.atol || got > tt.want+tt.atol {
				t.Fatalf("ParseRetryAfter(%q) = %v, want %v (±%v)", tt.value, got, tt.want, tt.atol)
			}
		})
	}
}

// TestParseRetryAfterNonGMTDate 带 HTTP-date 形态中 now 与日期不同时区的求差。
func TestParseRetryAfterNonGMTDate(t *testing.T) {
	// 头值带 CST 偏移的 RFC 850 形态：解析后与 now（UTC）求差
	now := time.Date(2026, 11, 8, 8, 0, 0, 0, time.UTC)
	// "Sun, 08 Nov 26 08:01:00 CST"——CST 在 Go 里按未知缩进处理，差值不确定，
	// 这里只验证「能解析（ok=true）」；精确差值属时区数据库行为，不做断言
	_, ok := httpx.ParseRetryAfter("Sunday, 08-Nov-26 08:01:00 CST", now)
	if !ok {
		t.Fatal("RFC 850 with zone abbrev must parse")
	}
}

// TestParseRetryAfterZeroVsMissing 布尔口径的语义面：0 = 立即重试，
// false = 没建议走计算退避——两条分支调用方要能区分。
func TestParseRetryAfterZeroVsMissing(t *testing.T) {
	now := time.Now()
	d, ok := httpx.ParseRetryAfter("0", now)
	if !ok || d != 0 {
		t.Fatalf(`"0" = (%v, %v), want (0, true)`, d, ok)
	}
	d, ok = httpx.ParseRetryAfter("nonsense", now)
	if ok || d != 0 {
		t.Fatalf(`"nonsense" = (%v, %v), want (0, false)`, d, ok)
	}
}

// TestParseRetryAfterRoundsTripWithHint 解析结果接 NewRetryAfter 注入错误链的
// 完整链路（issue 的「判可重试 → 解析 → 构造建议」三步）。
func TestParseRetryAfterRoundsTripWithHint(t *testing.T) {
	d, ok := httpx.ParseRetryAfter("30", time.Now())
	if !ok {
		t.Fatal("parse must succeed")
	}
	err := httpx.NewRetryAfter(d)
	got, ok := httpx.RetryAfterHint()(err)
	if !ok || got != 30*time.Second {
		t.Fatalf("hint = (%v, %v), want (30s, true)", got, ok)
	}
}
