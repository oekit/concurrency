// Package breaker 提供 Google SRE 概率式自适应熔断之上的显式三态状态机
// （closed/open/half-open；概率算法移植 go-zero core/breaker 的 googleBreaker）。
//
// 三态语义：
//
//   - closed：请求全量放行，滚动窗口（缺省 40 桶 × 250ms）持续统计；
//     错误率超阈值（SRE 公式丢弃概率 > 0，即接受预算耗尽）或计数型触发
//     （WithConsecutiveFailures 连续失败 n 次，缺省关闭，与速率型并联）切入 open；
//   - open：流量收敛到探测级——每隔 forcePass（缺省 1s）强制放行一次探活
//     （恢复观察哨 + 样本通道），其余一律拒绝并记丢弃样本；自进入/重开起冷却期满后，
//     首个放行请求即半开探测（携带新代 generation）——冷却期经 WithCooldown 独立
//     配置，缺省沿用 probeTimeout；v0.13.0 起不再按 SRE 公式概率丢弃/愈合放行
//     （#25：公式退役为 closed→open 触发器，熔断期内不放流量——旧版 dropRatio<=0
//     短路全量放行会使计数型触发形同虚设）；
//   - half-open：挂起探测等待回报——成功关闸（closed + 清空窗口，旧失败样本不得
//     触发立即重开）；失败重开并重置计时；超 probeTimeout 未回报视为失联回收，
//     放行新探测；陈旧代（gen < 当前代）回报整体作废，不计窗口、不迁移。
//
// 代绑定（GenBreaker）：代仅在探测放行时自增，普通放行携带当前代——陈旧探测的
// 迟到回报可被精确识别丢弃（一次调用的结果不会被算进另一次调用的账）；
// 开放期普通失败只记窗口样本、不触碰冷却计时（冷却不被延长）。
// 现有 Allow/Do 系列行为保持不变：等价于每次放行携带当前代，Do 家族经内部
// Allow 透传代绑定，命中半开的 Do 调用即探测。
//
// ErrCircuitOpen 为全库熔断打开的规范哨兵（httpx/nns-endpoint 经别名链共用同一
// sentinel）；拒绝时返回裸哨兵，判定一律 errors.Is。
//
// 与上游 go-zero 的差异点：
//   - API 全量 ctx-aware（Allow(ctx)/Do(ctx,…)），ctx 已取消直接返回 ctx.Err()；
//   - Promise 回填命名为 Pass/Fail（上游为 Accept/Reject(reason)）；
//   - 显式三态状态机 + 代绑定/探测时限（上游无状态机，隐式由窗口统计推导）；
//   - 冷却期（WithCooldown）与探测失联时限（WithProbeTimeout）可独立配置——业务冷却
//     与调用在途上界是两个决策，缺省沿用同一值保持 v0.9.0 行为；
//   - 可选计数型触发 WithConsecutiveFailures（连续失败 n 次即熔断，缺省关闭）与速率型
//     并联——低 QPS 下速率型受 protection 保护阈值制约可能永不触发，计数型补足
//     「最近有没有连挂」信号；
//   - 时间源可注入 WithBreakerClock（缺省 time.Now），窗口推进、强制探活与
//     探测调度共用，测试注入假时钟即可完全确定地驱动状态机；
//   - 可观测性零强制注入：拒绝走可选钩子 WithBreakerRejectHandler（缺省 no-op），
//     由上层显式接 logx/metrics；State()/State 枚举（closed/open/half-open）供
//     监控与日志打点（v0.13.0 起，放行判定仍以 Allow/Do 返回结果为准）；
//   - 与上游 go-zero 的关键语义分叉（v0.13.0 起）：上游 open 态按 SRE 公式概率
//     丢弃、窗口滚空即全量放行；本仓三态化后熔断期收敛到探测级（forcePass +
//     半开探测），恢复只由探测成功驱动——「状态」与「流量」不再失配；
//   - 统计容器复用本仓 window.RollingWindow（自定义桶 bucket 实现
//     BucketInterface[int64]），不镜像上游 bucket 布局。
//
// 缺省值与「用缺省」的写法（#28）：缺省值已导出为 DefaultWindow / DefaultBuckets /
// DefaultForcePass / DefaultProbeTimeout，下游应**引用**而非复制魔法数字（上游调了缺省，
// 拷贝不会跟着动，而行为差异只在熔断阈值上体现，排查成本很高）。
// 冷却期与计数型触发的缺省**没有**对应导出常量——二者的缺省是「未配置」
// （cooldown 回落 DefaultProbeTimeout、计数型关闭），值为 0，导出一个 0 值常量是陷阱。
// **「用缺省」的正确写法是不传该选项**，不存在传 0 走缺省的形式：
//
//	breaker.NewBreaker("x")                                  // 冷却期 = DefaultProbeTimeout
//	breaker.NewBreaker("x", breaker.WithCooldown(5*time.Minute))
//	breaker.NewBreaker("x", breaker.WithCooldown(0))         // panic：d <= 0 一律拒绝
//	breaker.NewBreaker("x", breaker.WithConsecutiveFailures(0)) // 合法：显式关闭计数型触发
//
// 从旧 agentkit/breaker 迁移的语义反转（#28 实测）：旧库 `NewBreakers(trip, cooldown)`
// 形参传 0 表示"用缺省"，本包的选项式 API 里 **cooldown 传 0 是 panic**，
// 计数型传 0 则是"关闭触发"——两者在新旧 API 中含义相反，直译会编译通过、行为却变。
// 本包无 NewBreakers(trip, cooldown) 形态；请按上表改写为选项。
//
// 用法:
//
//	b := breaker.GetBreaker("user-rpc")
//	err := b.Do(context.Background(), func() error { return callUserRPC() })
//	if errors.Is(err, breaker.ErrCircuitOpen) {
//	    // 熔断打开，快速失败/走降级
//	}
//
// ⚠️ GetBreaker 是进程级单例，opts 仅在首次创建该名字时生效：同名再传不同 opts 会被
// 静默丢弃（#29）。需要不同参数请把参数编进名字做命名空间隔离，或用 NewBreaker 显式自建；
// 想让静默失效可被发现，注册 WithBreakerConflictHandler（同名异参时回调一次）。
//
// 需要精确代绑定的库形态消费方：
//
//	var gb breaker.GenBreaker = breaker.GetBreaker("llm-primary")
//	p, gen, err := gb.AllowGen(ctx)   // gen 为本次放行的代
//	...
//	gb.FailureGen(ctx, gen)           // 陈旧代回报被自动丢弃
//	gb.AbandonGen(gen)                // 探针失联时显式放弃
package breaker
