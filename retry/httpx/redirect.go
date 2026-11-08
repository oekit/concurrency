package httpx

import "strings"

// HeaderEditor 在 HeaderSource 读取面之上加删除能力：跨宿主重定向敏感头
// 剥离（#200）需要「读键判定 + 删键」两个动作。net/http 的 http.Header
// 天然满足（Get/Del 都做规范 MIME 键查找）；自研客户端的 header 类型实现
// 同名两方法即可接入，包保持零依赖（不 import net/http）。
type HeaderEditor interface {
	HeaderSource
	// Del 删除键对应的全部值；无该键为 no-op。键为规范 MIME 形式
	// （与 Get 同一约定）。
	Del(key string)
}

// 默认凭据头词表（剥离面，#200）：RFC 7235 凭据族 + Cookie 会话凭据 +
// 常见平台 Token 头形态。词表经 DefaultSensitiveHeaders 导出（返回副本，
// 无包级可变全局——平台特化 Token 头由调用方追加后传
// StripSensitiveHeadersWith 组合，与 logredact「规则表公开可组合」同一哲学）。
var defaultSensitiveHeaders = [...]string{
	"Authorization",       // RFC 7235 凭据（Basic/Bearer/Token/Digest 全形态）
	"Proxy-Authorization", // RFC 7235 代理凭据
	"Cookie",              // RFC 6265 会话凭据（跨宿主必属另一个信任域）
	"Private-Token",       // Gitee/GitLab 私有令牌头
	"X-Api-Key",           // API Key 形态（各家平台通用变体）
	"X-Auth-Token",        // OpenStack 系平台 Token 头形态
}

// DefaultSensitiveHeaders 返回内置凭据头词表的副本——调用方可追加平台特化
// Token 头（如 X-TaskToken 等内部形态）后经 StripSensitiveHeadersWith 使用。
// 键为规范 MIME 形式（http.Header 的 Get/Del 大小写不敏感）。
func DefaultSensitiveHeaders() []string {
	names := make([]string, len(defaultSensitiveHeaders))
	copy(names, defaultSensitiveHeaders[:])
	return names
}

// CrossHostRedirect 判定一次重定向是否跨宿主（#200）：fromHost/toHost 为
// host[:port] 字符串（net/url 的 URL.Host 形态），scheme 不参与判定——
// http→https 同宿主升级不剥凭据，宿主相等才保留。
//
// 比较大小写不敏感（DNS 名不区分大小写），端口参与比较。缺省端口的等价
// 形态（example.com 与 example.com:443）按**不同宿主**处理——判定不确定时
// 剥离是安全方向：误剥的代价是重新认证，误留的代价是凭据发往另一个信任域。
func CrossHostRedirect(fromHost, toHost string) bool {
	return !strings.EqualFold(fromHost, toHost)
}

// StripSensitiveHeaders 在跨宿主重定向时剥离凭据头（#200）——手动跟随
// 重定向的调用方在**逐跳 RoundTrip 之前**调用（std http.Client 自动跟随
// 会剥离跨宿主 Authorization，手动跟随恰好绕开这层保护；剥离发生在「跟随」
// 而不是「响应处理」——晚了就已经在跳转请求里带出去了，go-github
// checkRedirection 同场景）。同宿主跳转原样返回（git 平台的 URL 常带路径
// 跳转，同宿主剥了反而破坏认证），凭据头一个不删。
//
// h 为 nil 时 no-op。剥离词表为内置缺省（见 DefaultSensitiveHeaders）；
// 平台特化 Token 头用 StripSensitiveHeadersWith 传自定义词表。
//
// 用法（手动跟随 + 每跳判定）：
//
//	for redirects := 0; ; redirects++ {
//	    resp, err := client.Do(req)
//	    if err != nil { return err }
//	    if resp.StatusCode/100 != 3 || redirects >= maxRedirects { return resp }
//	    loc := resp.Header.Get("Location")
//	    next, _ := url.Parse(loc)
//	    httpx.StripSensitiveHeaders(&req.Header, req.URL.Host, next.Host)
//	    req.URL = next
//	}
func StripSensitiveHeaders(h HeaderEditor, fromHost, toHost string) {
	stripSensitiveHeaders(h, fromHost, toHost, defaultSensitiveHeaders[:])
}

// StripSensitiveHeadersWith 同 StripSensitiveHeaders，剥离词表由调用方给
// （DefaultSensitiveHeaders() 返回副本后追加平台特化 Token 头，或整表自定）。
// names 为空 ⇒ 什么都不剥（显式给空表 = 显式放弃剥离，不静默回落内置表）。
func StripSensitiveHeadersWith(h HeaderEditor, fromHost, toHost string, names []string) {
	stripSensitiveHeaders(h, fromHost, toHost, names)
}

func stripSensitiveHeaders(h HeaderEditor, fromHost, toHost string, names []string) {
	if h == nil || len(names) == 0 {
		return
	}
	if !CrossHostRedirect(fromHost, toHost) {
		return // 同宿主：凭据保留（平台路径跳转依赖同宿主认证）
	}
	for _, name := range names {
		h.Del(name)
	}
}
