// Package internal/ticker 承载 batch 与 timewheel 共用的 Ticker 周期信号源
// 抽象（#264）：Ticker/TickerFactory 五件套此前在两包逐字重复（timewheel
// 注释自认「与 batch.Ticker 同形；各自独立定义避免子包耦合」）。收敛为仓内
// internal 单源——internal 路径保住「不进导出面」的初衷，两包经类型别名
// 转发，既有导入面零破坏。
package ticker

import "time"

// Ticker 周期信号源抽象。注入手动实现即可确定性地驱动时序测试
// （batch 攒批 flush / timewheel 转动共用）。
type Ticker interface {
	C() <-chan time.Time
	Stop()
}

// TickerFactory 按给定间隔构造 Ticker。
type TickerFactory func(d time.Duration) Ticker

// New 标准库 time.Ticker 适配（两包缺省工厂共用的唯一实现）。
func New(d time.Duration) Ticker {
	return adapter{time.NewTicker(d)}
}

type adapter struct{ *time.Ticker }

func (a adapter) C() <-chan time.Time { return a.Ticker.C }
