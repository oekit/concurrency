package httpx_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/oekit/concurrency/retry/httpx"
)

// fakeEditor 是自研客户端 header 类型的最小实现（Get/Del 规范 MIME 键），
// 验证 HeaderEditor 不绑死 http.Header。
type fakeEditor struct {
	m map[string][]string
}

func newFakeEditor() *fakeEditor { return &fakeEditor{m: map[string][]string{}} }

func (f *fakeEditor) Set(key, value string) { f.m[http.CanonicalHeaderKey(key)] = []string{value} }

func (f *fakeEditor) Get(key string) string {
	v := f.m[http.CanonicalHeaderKey(key)]
	if len(v) == 0 {
		return ""
	}
	return v[0]
}

func (f *fakeEditor) Del(key string) { delete(f.m, http.CanonicalHeaderKey(key)) }

func (f *fakeEditor) count(key string) int { return len(f.m[http.CanonicalHeaderKey(key)]) }

// credentialsEditor 预置全套内置词表凭据头。
func credentialsEditor() *fakeEditor {
	f := newFakeEditor()
	for _, name := range httpx.DefaultSensitiveHeaders() {
		f.Set(name, "credential-"+name)
	}
	f.Set("Accept", "application/json") // 非凭据头
	f.Set("X-Request-Id", "req-42")     // 非凭据头
	f.Set("If-Match", "\"etag-1\"")     // 非凭据头
	return f
}

// TestCrossHostRedirect 表驱动：host[:port] 判定、大小写不敏感、scheme 不参与。
func TestCrossHostRedirect(t *testing.T) {
	tests := []struct {
		name string
		from string
		to   string
		want bool
	}{
		{"same host", "api.github.com", "api.github.com", false},
		{"same host case-insensitive", "Api.GitHub.com", "api.github.com", false},
		{"cross host", "api.github.com", "evil.example.com", true},
		{"port change is cross host", "example.com:443", "example.com:8443", true},
		{"default port vs explicit is cross host (conservative)", "example.com", "example.com:443", true},
		{"same host with port", "example.com:8443", "example.com:8443", false},
		{"subdomain is cross host", "example.com", "cdn.example.com", true},

		// 空 toHost 按不同宿主处理（保守档，#214 显式钉住）：相对 Location
		// 未解析的典型产物——「解析不出来」不等于「另一个信任域」，但判定
		// 不确定时剥离是安全方向；调用方必须先 ResolveReference（见
		// TestStripSensitiveHeaders_RelativeLocation）。
		{"empty toHost is cross host (conservative, unresolved relative Location)", "api.github.com", "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := httpx.CrossHostRedirect(tt.from, tt.to); got != tt.want {
				t.Fatalf("CrossHostRedirect(%q, %q) = %v, want %v", tt.from, tt.to, got, tt.want)
			}
		})
	}
}

// TestStripSensitiveHeadersCore 核心语义：跨宿主剥凭据族、非凭据头保留、
// 同宿主一个不删、nil 与自研 Editor 均可接入。
func TestStripSensitiveHeadersCore(t *testing.T) {
	t.Run("cross host strips credentials keeps others", func(t *testing.T) {
		h := credentialsEditor()
		httpx.StripSensitiveHeaders(h, "api.github.com", "evil.example.com")
		for _, name := range httpx.DefaultSensitiveHeaders() {
			if h.count(name) != 0 {
				t.Errorf("cross-host redirect must strip %s", name)
			}
		}
		for _, keep := range []string{"Accept", "X-Request-Id", "If-Match"} {
			if h.count(keep) != 1 {
				t.Errorf("non-credential header %s must survive", keep)
			}
		}
	})

	t.Run("same host keeps everything", func(t *testing.T) {
		h := credentialsEditor()
		httpx.StripSensitiveHeaders(h, "api.github.com", "api.github.com")
		for _, name := range httpx.DefaultSensitiveHeaders() {
			if h.count(name) != 1 {
				t.Errorf("same-host redirect must keep %s (platform path redirect relies on it)", name)
			}
		}
	})

	t.Run("nil editor no-op", func(t *testing.T) {
		httpx.StripSensitiveHeaders(nil, "a.com", "b.com") // 不 panic
	})

	t.Run("http.Header via http.HeaderEditor interface", func(t *testing.T) {
		h := http.Header{}
		h.Set("Authorization", "Bearer tok")
		h.Set("Cookie", "sid=1")
		h.Set("Accept", "application/json")
		var editor httpx.HeaderEditor = h // http.Header 天然满足 HeaderEditor
		httpx.StripSensitiveHeaders(editor, "api.github.com", "redirected.example.com")
		if h.Get("Authorization") != "" || h.Get("Cookie") != "" {
			t.Fatal("http.Header credentials must be stripped")
		}
		if h.Get("Accept") != "application/json" {
			t.Fatal("http.Header non-credential must survive")
		}
	})
}

// TestStripSensitiveHeadersWordList 词表导出与组合：副本可改不污染内置、
// 空表显式放弃、平台特化 Token 头经 With 变体接入。
func TestStripSensitiveHeadersWordList(t *testing.T) {
	t.Run("returned copy is independent", func(t *testing.T) {
		list := httpx.DefaultSensitiveHeaders()
		list = append(list, "X-Custom-Thing")
		again := httpx.DefaultSensitiveHeaders()
		if len(again) != len(list)-1 {
			t.Fatalf("mutating returned copy must not pollute built-ins: %d vs %d", len(again), len(list))
		}
	})

	t.Run("empty custom list strips nothing", func(t *testing.T) {
		h := credentialsEditor()
		httpx.StripSensitiveHeadersWith(h, "a.com", "b.com", nil)
		httpx.StripSensitiveHeadersWith(h, "a.com", "b.com", []string{})
		for _, name := range httpx.DefaultSensitiveHeaders() {
			if h.count(name) != 1 {
				t.Fatalf("empty list must strip nothing, but %s was stripped", name)
			}
		}
	})

	t.Run("platform token via custom list", func(t *testing.T) {
		h := credentialsEditor()
		h.Set("X-TaskToken", "task-tok") // go-git-platform 形态的内部 Token 头
		names := append(httpx.DefaultSensitiveHeaders(), "X-TaskToken")
		httpx.StripSensitiveHeadersWith(h, "api.github.com", "other.example.com", names)
		if h.count("X-TaskToken") != 0 {
			t.Fatal("platform token in custom list must be stripped")
		}
		for _, name := range httpx.DefaultSensitiveHeaders() {
			if h.count(name) != 0 {
				t.Fatalf("built-in %s must also be stripped in cross-host custom run", name)
			}
		}
		if h.count("Accept") != 1 {
			t.Fatal("non-credential must survive")
		}
	})

	t.Run("custom list on same host strips nothing", func(t *testing.T) {
		h := credentialsEditor()
		httpx.StripSensitiveHeadersWith(h, "api.github.com", "api.github.com", []string{"Authorization"})
		if h.count("Authorization") != 1 {
			t.Fatal("same-host must keep credentials regardless of list")
		}
	})
}

// TestStripSensitiveHeaders_RelativeLocation 相对 Location 的判定口径（#214）：
// 先 ResolveReference 成绝对 URL 再判定——同主机相对跳转（平台规范化跳转
// 301 /a → /a/ 的最常见形态）不剥凭据、req.URL 保持绝对；负对照：绝对跨宿主
// 仍剥（防止「为修相对跳转把泄漏面打开」）。
func TestStripSensitiveHeaders_RelativeLocation(t *testing.T) {
	base, err := url.Parse("https://git.example.com/api/v4/repos")
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name        string
		location    string
		wantCross   bool
		wantNextURL string
	}{
		{name: "same-host path redirect keeps credentials", location: "/api/v4/repos/", wantCross: false, wantNextURL: "https://git.example.com/api/v4/repos/"},
		{name: "relative path keeps credentials", location: "login", wantCross: false, wantNextURL: "https://git.example.com/api/v4/login"},
		{name: "dotdot relative keeps credentials", location: "../x", wantCross: false, wantNextURL: "https://git.example.com/api/x"},
		{name: "absolute same host keeps credentials", location: "https://git.example.com/a", wantCross: false, wantNextURL: "https://git.example.com/a"},
		{name: "absolute cross host strips credentials", location: "https://evil.example.net/a", wantCross: true, wantNextURL: "https://evil.example.net/a"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 解析错误不丢（doc 示例口径）；变量名避让外层 err（govet shadow）
			ref, perr := url.Parse(tt.location)
			if perr != nil {
				t.Fatal(perr)
			}
			next := base.ResolveReference(ref)
			if got := httpx.CrossHostRedirect(base.Host, next.Host); got != tt.wantCross {
				t.Fatalf("CrossHostRedirect(%q, %q) = %v, want %v", base.Host, next.Host, got, tt.wantCross)
			}

			h := credentialsEditor()
			httpx.StripSensitiveHeaders(h, base.Host, next.Host)
			authKept := h.count("Authorization") == 1
			if authKept == tt.wantCross {
				t.Fatalf("cross=%v 时 Authorization kept=%v，与判定不符", tt.wantCross, authKept)
			}
			if !next.IsAbs() {
				t.Fatalf("ResolveReference 后 req.URL 应保持绝对 URL，got %q", next.String())
			}
			if next.String() != tt.wantNextURL {
				t.Fatalf("next = %q, want %q", next.String(), tt.wantNextURL)
			}
		})
	}

	// 反例钉死（doc 警告的现状语义）：跳过 ResolveReference 直接拿相对 URL
	// 判定——Host 为空串被按跨宿主处理，同主机凭据被误剥。
	rel, err := url.Parse("/api/v4/repos/")
	if err != nil {
		t.Fatal(err)
	}
	if rel.Host != "" {
		t.Fatalf("前置失效：相对 Location 解析出的 Host 应为空串，got %q", rel.Host)
	}
	if !httpx.CrossHostRedirect(base.Host, rel.Host) {
		t.Fatal("未解析的相对 URL 按跨宿主处理（保守档）——该 pitfall 已在 CrossHostRedirect doc 点名")
	}
}

// TestStripSensitiveHeadersMultiHop 手动多跳跟随的完整路径：逐跳判定，
// 中途跳回同宿主时凭据已丢（单向剥离——已剥离的不复活），后续跳转不再带出。
func TestStripSensitiveHeadersMultiHop(t *testing.T) {
	h := credentialsEditor()

	// 第一跳：api.github.com → evil.example.com（跨宿主，剥离）
	httpx.StripSensitiveHeaders(h, "api.github.com", "evil.example.com")
	if h.count("Authorization") != 0 {
		t.Fatal("hop 1 must strip Authorization")
	}

	// 第二跳：evil.example.com → evil.example.com/cdn（同宿主，保留剩余头）
	httpx.StripSensitiveHeaders(h, "evil.example.com", "evil.example.com")
	if h.count("Accept") != 1 {
		t.Fatal("same-host hop must keep non-credential headers")
	}
	for _, name := range httpx.DefaultSensitiveHeaders() {
		if h.count(name) != 0 {
			t.Fatalf("stripped credential %s must not reappear", name)
		}
	}
}
