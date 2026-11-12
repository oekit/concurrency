//go:build race

package singleflight

// staleJoinLimitPct 为 TestNarrowWindow_NoStaleJoin 的陈旧 join 率阈值（race
// 构建档）。race 探测器拖慢 leader「fn 产出 → 摘键」收尾窗口，正确实现
// （delete-first，与 pkg 参照一致）同机实测 ~10%；close-first 缺陷形态
// ~57-82%。30% 对两侧均保持 ≥2× 裕量（标定表见 stalewindow_test.go）。
const staleJoinLimitPct = 30.0
