package httpx

import (
	"net/url"
	"strings"
)

// URL query 凭据参数剥离（#208）——StripSensitiveHeaders（#200）的姊妹面：
// 部分平台 SDK 把令牌放在 URL query 而非请求头（go-gitea/go-gitee 的
// tokenTransport 每一跳 q.Set("access_token", token)，包在 http.Client 的
// Transport 外层）⇒ 平台 302 到 CDN/对象存储（Gitee 的 archive/附件端点
// 实测如此）时，令牌跟着 query 一起发给第三方主机。头部剥除帮不上忙
// （令牌根本不在头部）；Go 的自动跟随也不剥 query（stdlib 只处理头部）；
// 手动跟随（#200 同场景）同样只顾头部。下游 go-git-platform 的
// transport/client.go 在头部剥离之外单列凭据 query 参数组并行剥离，
// 本文件把该固化形态收编为原语。
//
// 用法：与 StripSensitiveHeaders 并行调用（两条独立泄漏面，缺一即漏），
// 逐跳 RoundTrip 之前、跨宿主判定（CrossHostRedirect）之后：
//
//	for redirects := 0; ; redirects++ {
//	    resp, err := client.Do(req)
//	    if err != nil { return err }
//	    loc := resp.Header.Get("Location")
//	    next, _ := url.Parse(loc)
//	    if httpx.CrossHostRedirect(req.URL.Host, next.Host) {
//	        httpx.StripSensitiveHeaders(&req.Header, req.URL.Host, next.Host)
//	        httpx.StripSensitiveQueryParams(req.URL) // token-in-query 面
//	    }
//	    req.URL = next
//	}

// defaultSensitiveQueryParams 是内置凭据 query 参数词表（#208）：Gitee/GitLab
// 系平台 SDK 的 token-in-query 形态（下游 go-git-platform 的
// sensitiveQueryParams 固化组）。各家 query 形态比头部收敛，但同样值得
// 导出+扩展（经 DefaultSensitiveQueryParams / StripSensitiveQueryParamsWith）。
var defaultSensitiveQueryParams = [...]string{
	"access_token",  // OAuth2 / Gitee tokenTransport 的 query 形态
	"private_token", // GitLab 私有令牌 query 形态
	"token",         // 各家通用裸 token query 形态
}

// DefaultSensitiveQueryParams 返回内置凭据 query 参数词表的副本——调用方可
// 追加平台特化参数后经 StripSensitiveQueryParamsWith 使用（返回副本、无包级
// 可变全局，与 DefaultSensitiveHeaders 同一哲学）。
func DefaultSensitiveQueryParams() []string {
	names := make([]string, len(defaultSensitiveQueryParams))
	copy(names, defaultSensitiveQueryParams[:])
	return names
}

// StripSensitiveQueryParams 从 URL query 中剥离凭据参数（token-in-query
// 形态，#208）——与 StripSensitiveHeaders 的姊妹面。纯函数原语：只改传入的
// *url.URL 的 RawQuery，是否该剥（跨宿主判定）由调用方决定（与头部剥离
// 并行调用，两条独立泄漏面缺一即漏；同宿主跳转请勿调用——平台路径跳转
// 的 query 凭据依赖同宿主认证，剥了反而破坏认证）。
//
// 匹配口径：键与词表大小写不敏感比对（AccessToken/ACCESS_TOKEN 同属凭据，
// 保守方向多剥），键先尝试百分号解码（"%61ccess_token=x" 服务端解码后
// 同键）；带 '=0'、裸键（无 '='）与空段（"a=1&&b=2" 的空段）一律按参数对
// 处理。无变更不重编码：没剥掉任何参数时 RawQuery 逐字节原样保留（参数序
// 与转义形态不动）；发生删除时其余参数按原字节形态保序重接——不经
// url.Values.Encode（Encode 会重编全部键值并按字典序重排参数，扰动原 URL
// 形态）。u 为 nil、RawQuery 为空或词表为空时 no-op。
func StripSensitiveQueryParams(u *url.URL) {
	stripSensitiveQueryParams(u, defaultSensitiveQueryParams[:])
}

// StripSensitiveQueryParamsWith 同 StripSensitiveQueryParams，剥离词表由调用方给
// （DefaultSensitiveQueryParams() 返回副本后追加平台特化参数，或整表自定）。
// names 为空 ⇒ 什么都不剥（显式给空表 = 显式放弃剥离，不静默回落内置表）。
func StripSensitiveQueryParamsWith(u *url.URL, names []string) {
	stripSensitiveQueryParams(u, names)
}

func stripSensitiveQueryParams(u *url.URL, names []string) {
	if u == nil || len(names) == 0 || u.RawQuery == "" {
		return
	}
	stripped, changed := stripRawQuery(u.RawQuery, names)
	if changed {
		u.RawQuery = stripped
	}
}

// stripRawQuery 在 raw query 文本上做参数级删除：按 '&' 切对、命中词表的
// 参数对整对移除，其余参数对的原始字节形态与顺序原样保留。返回重接结果与
// 是否发生删除（false 时原样返回，不重编码）。
func stripRawQuery(rawQuery string, names []string) (string, bool) {
	pairs := strings.Split(rawQuery, "&")
	kept := make([]string, 0, len(pairs))
	changed := false
	for _, pair := range pairs {
		if queryKeySensitive(pair, names) {
			changed = true
			continue
		}
		kept = append(kept, pair)
	}
	if !changed {
		return rawQuery, false
	}
	return strings.Join(kept, "&"), true
}

// queryKeySensitive 判定一对 query 参数（"key=value" 或裸 "key"）的键是否
// 命中词表：键取 '=' 之前的文本，先尝试百分号解码（解码失败按原文），再与
// 词表大小写不敏感比对。
func queryKeySensitive(pair string, names []string) bool {
	key := pair
	if i := strings.IndexByte(pair, '='); i >= 0 {
		key = pair[:i]
	}
	if key == "" {
		return false // 空段（"a=1&&b=2" 的空槽）不命中任何词
	}
	if dec, err := url.QueryUnescape(key); err == nil {
		key = dec
	}
	for _, name := range names {
		if strings.EqualFold(key, name) {
			return true
		}
	}
	return false
}
