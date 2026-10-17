// Package budget 提供预算/配额记账原语（#16）——超限不丢、可取回、按龄清理。
//
// 与仓内丢弃式流控面（ratelimit/shedding/breaker 保护资源）互补：本包面向
// 「外部输出放进预算框」的记账语义——模型输出预算（超限截断提示 + 完整原文
// 按需拉取）、日志/审计留存配额、批量抓取字节配额。
//
// 最小面（并发安全）：
//
//	l := budget.NewLedger(map[budget.Dimension]int64{"bytes": 100},
//	    budget.WithClock(clk.Now)) // 可注入时钟，确定性测试
//	ref, err := l.Consume("bytes", 250, fullOutput)
//	if errors.Is(err, budget.ErrOverBudget) {
//	    archived := l.Get(ref) // 完整原文按需取回
//	}
//	l.Sweep(time.Hour) // 按龄清理，防存档无界增长
//
// 语义要点：超限消耗不计入（内容未通过预算框）；未声明维度不限量；
// Get 只读不删，清理由 Sweep 负责。
package budget
