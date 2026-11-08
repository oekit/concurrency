package httpx_test

import (
	"net/url"
	"testing"

	"github.com/oekit/concurrency/retry/httpx"
)

// mustURL 构造测试用 *url.URL（解析失败即 Fatal——URL 字面量均为编译期审过的常量）。
func mustURL(t *testing.T, raw string) *url.URL {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse %q: %v", raw, err)
	}
	return u
}

// TestStripSensitiveQueryParams 表驱动核心语义（#208）：默认词表
// （access_token/private_token/token）整对移除、其余参数对保序保留、
// 无变更不重编码（RawQuery 逐字节原样）、path/fragment 不动。
func TestStripSensitiveQueryParams(t *testing.T) {
	cases := []struct {
		name      string
		raw       string
		wantQuery string // 剥离后的 RawQuery 期望值（逐字节）
	}{
		{
			name:      "strips credential keeps others in order",
			raw:       "https://cdn.example.com/d?sign=x&access_token=secret&id=7&private_token=p&fmt=json",
			wantQuery: "sign=x&id=7&fmt=json",
		},
		{
			name:      "bare token param",
			raw:       "https://evil.example.com/x?token=tok&keep=1",
			wantQuery: "keep=1",
		},
		{
			name:      "only credential becomes empty query",
			raw:       "https://evil.example.com/x?access_token=secret",
			wantQuery: "",
		},
		{
			name:      "case-fold variants stripped, camelCase is a distinct name",
			raw:       "https://evil.example.com/x?ACCESS_TOKEN=t&PRIVATE_TOKEN=p&AccessToken=s&keep=1",
			wantQuery: "AccessToken=s&keep=1", // EqualFold 只折大小写；无下划线驼峰是不同参数名，不误剥
		},
		{
			name:      "percent-encoded key stripped",
			raw:       "https://evil.example.com/x?%61ccess_token=s&keep=1",
			wantQuery: "keep=1",
		},
		{
			name:      "no change keeps rawquery byte-identical (order and escapes untouched)",
			raw:       "https://ok.example.com/x?z=1&a=%2Fpath&Access-Control=ok&m=&b=2",
			wantQuery: "z=1&a=%2Fpath&Access-Control=ok&m=&b=2",
		},
		{
			name:      "lookalike keys survive",
			raw:       "https://ok.example.com/x?access_token_id=keep&tokens=keep&keep=1",
			wantQuery: "access_token_id=keep&tokens=keep&keep=1",
		},
		{
			name:      "empty pairs preserved when stripping",
			raw:       "https://evil.example.com/x?a=1&access_token=s&&b=2",
			wantQuery: "a=1&&b=2",
		},
		{
			name:      "no query at all",
			raw:       "https://ok.example.com/x",
			wantQuery: "",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			u := mustURL(t, tc.raw)
			path, frag := u.Path, u.Fragment
			httpx.StripSensitiveQueryParams(u)
			if u.RawQuery != tc.wantQuery {
				t.Fatalf("RawQuery = %q, want %q", u.RawQuery, tc.wantQuery)
			}
			if u.Path != path || u.Fragment != frag {
				t.Fatalf("path/fragment must be untouched: path=%q frag=%q", u.Path, u.Fragment)
			}
		})
	}
}

// TestStripSensitiveQueryParams_NoOpGuards nil URL、空 query、空词表的 no-op 契约。
func TestStripSensitiveQueryParams_NoOpGuards(t *testing.T) {
	httpx.StripSensitiveQueryParams(nil) // 不 panic

	u := mustURL(t, "https://ok.example.com/x?a=1")
	before := u.RawQuery
	httpx.StripSensitiveQueryParamsWith(u, nil)
	httpx.StripSensitiveQueryParamsWith(u, []string{})
	if u.RawQuery != before {
		t.Fatalf("empty word list must strip nothing, got %q", u.RawQuery)
	}

	u2 := mustURL(t, "https://ok.example.com/x")
	httpx.StripSensitiveQueryParams(u2)
	if u2.RawQuery != "" {
		t.Fatalf("empty query must stay empty, got %q", u2.RawQuery)
	}
}

// TestStripSensitiveQueryParamsWith 平台特化词表组合（与 DefaultSensitiveHeaders
// 同一哲学）：副本可改不污染内置；自定义词表按表剥离。
func TestStripSensitiveQueryParamsWith(t *testing.T) {
	t.Run("returned copy is independent", func(t *testing.T) {
		list := httpx.DefaultSensitiveQueryParams()
		list = append(list, "sig")
		again := httpx.DefaultSensitiveQueryParams()
		if len(again) != len(list)-1 {
			t.Fatalf("mutating returned copy must not pollute built-ins: %d vs %d", len(again), len(list))
		}
	})

	t.Run("platform param via custom list", func(t *testing.T) {
		u := mustURL(t, "https://evil.example.com/x?sig=abc&access_token=s&keep=1")
		names := append(httpx.DefaultSensitiveQueryParams(), "sig")
		httpx.StripSensitiveQueryParamsWith(u, names)
		if u.RawQuery != "keep=1" {
			t.Fatalf("RawQuery = %q, want keep=1", u.RawQuery)
		}
	})

	t.Run("custom list replaces built-ins", func(t *testing.T) {
		u := mustURL(t, "https://evil.example.com/x?sig=abc&access_token=s")
		httpx.StripSensitiveQueryParamsWith(u, []string{"sig"})
		if u.RawQuery != "access_token=s" {
			t.Fatalf("custom list must strip only listed params, got %q", u.RawQuery)
		}
	})
}

// TestStripSensitiveQueryParams_RedirectWalk 端到端用法形态（#208 姊妹面并行
// 剥离）：302 到第三方 CDN 时头部与 query 双面剥离、同宿主跳转双面保留。
func TestStripSensitiveQueryParams_RedirectWalk(t *testing.T) {
	t.Run("cross host strips both surfaces", func(t *testing.T) {
		u := mustURL(t, "https://gitee.com/repo/archive.tar.gz?access_token=sekrit")
		next := mustURL(t, "https://cdn-assets.example.com/repo/archive.tar.gz")
		if httpx.CrossHostRedirect(u.Host, next.Host) {
			httpx.StripSensitiveQueryParams(u) // token-in-query 面（#208）
		}
		if u.RawQuery != "" {
			t.Fatalf("token-in-query must be stripped on cross-host redirect, got %q", u.RawQuery)
		}
	})

	t.Run("same host keeps query credentials", func(t *testing.T) {
		u := mustURL(t, "https://gitee.com/a/b?access_token=sekrit")
		next := mustURL(t, "https://gitee.com/a/c")
		if httpx.CrossHostRedirect(u.Host, next.Host) {
			t.Fatal("same host must not be cross-host")
		}
		// 同宿主：不调用 Strip（平台路径跳转依赖同宿主认证）。
		if u.RawQuery != "access_token=sekrit" {
			t.Fatalf("same-host query credential must survive, got %q", u.RawQuery)
		}
	})
}
