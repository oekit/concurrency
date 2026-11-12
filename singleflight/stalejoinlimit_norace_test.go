//go:build !race

package singleflight

// staleJoinLimitPct 为 TestNarrowWindow_NoStaleJoin 的陈旧 join 率阈值（非
// race 构建档）。正确实现（delete-first，与 pkg 参照一致）同机实测 ~0.5%；
// close-first 缺陷形态 ~25%。5% 对两侧均保持 ~10× 裕量（标定表见
// stalewindow_test.go）。
const staleJoinLimitPct = 5.0
