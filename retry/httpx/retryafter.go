package httpx

import (
	"strconv"
	"strings"
	"time"
)

// Retry-After 头的三种合法时刻格式（RFC 7231 §7.1.1.1/§7.1.1.2）：头值只能是
// delay-seconds 或 HTTP-date 二选一；HTTP-date 收到方必须接受 IMF-fixdate，
// 并应兼容废弃的 RFC 850 与 asctime 形态——与 net/http.ParseTime 的三格式
// 尝试次序一致。格式串本地定义、不 import net/http（包零依赖红线）。
const (
	retryAfterFormatIMF    = "Mon, 02 Jan 2006 15:04:05 GMT"  // IMF-fixdate（= RFC 1123 + GMT）
	retryAfterFormatRFC850 = "Monday, 02-Jan-06 15:04:05 MST" // 废弃 RFC 850
	retryAfterFormatANSIC  = "Mon Jan _2 15:04:05 2006"       // 废弃 asctime (ANSI C)
)

// ParseRetryAfter 解析 Retry-After 头值（#198）：delay-seconds（"120"）与
// HTTP-date（"Wed, 21 Oct 2026 07:28:00 GMT"）双形态都接受。
//
//   - delay-seconds：非负整数 ⇒ 对应时长；**负秒数不可解析**（返回 (0, false)
//     而非钳 0——负值属于坏值，「没带建议走指数退避」比「立即重试」更安全）；
//     超出 int64/int 范围的秒数解析失败同样返回 (0, false)；
//   - HTTP-date：已过去的时刻钳到 0（立即重试，不取负）；未来时刻返回与
//     now 的差值；
//   - 其余（空串、非数字非日期的坏值）返回 (0, false)。
//
// 返回 bool 而非 error：「没带/带坏」与「带了 0」对重试语义不同——0 = 立即
// 重试，false = 没有建议走计算退避（retry.Config.DelayHint 的 ok=false 路径），
// 布尔口径让调用方分支清晰。值先做 OWS 修剪（与 RetryableWithHeaders 的 #26
// 口径一致：net/textproto 解析路径已剥空白，自造 header 等来源可能带前导/
// 尾随空白）。
//
// 钳制（MaxDelay 上限等）调用方既有——本原语只负责「头值→时长」的忠实解析，
// 不替调用方做退避策略。
//
// now 供 date 形态求差，常规调用传 time.Now()。判可重试 → 解析 → 构造建议
// 的完整链路：
//
//	if httpx.RetryableWithHeaders(resp.StatusCode, resp.Header) {
//	    if d, ok := httpx.ParseRetryAfter(resp.Header.Get("Retry-After"), time.Now()); ok {
//	        cfg.DelayHint = func(error) (time.Duration, bool) { return d, true }
//	    }
//	}
func ParseRetryAfter(v string, now time.Time) (time.Duration, bool) {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0, false
	}
	// 形态一：delay-seconds。Atoi 接受符号前缀（"+120" 合法），拒绝小数与
	// 后缀单位——溢出报错后落入 date 形态也不可达，最终 (0, false)。
	if secs, err := strconv.Atoi(v); err == nil {
		if secs < 0 {
			return 0, false
		}
		return time.Duration(secs) * time.Second, true
	}
	// 形态二：HTTP-date。三格式依 IMF-fixdate → RFC 850 → asctime 次序尝试
	// （与 net/http.ParseTime 同序同语义，仅去掉 net/http 依赖）。
	for _, layout := range [...]string{retryAfterFormatIMF, retryAfterFormatRFC850, retryAfterFormatANSIC} {
		if t, err := time.Parse(layout, v); err == nil {
			d := t.Sub(now)
			if d < 0 {
				d = 0 // 过去的时刻立即重试（钳 0，不取负）
			}
			return d, true
		}
	}
	return 0, false
}
