// Package poll 提供「带单调游标的长轮询批量拉取」循环原语：拉一批 → 逐项处理 →
// 游标前进 → 再拉，失败按 retry 的退避公式续拉，直到 ctx 取消。
//
// 收编自 IM / 消息平台收消息循环反复被复制的那一份形状（长轮询 + 游标 + 批量 +
// 失败续拉），把四个会各自漂移的语义一次钉死：
//
//  1. 游标只前进——非空批次必须带回不同游标，否则 ErrCursorStuck 当场返回
//  2. 处理完才推进——本批处理过才推进游标，语义恒为 at-least-once（不会「游标跑了、
//     消息没处理」）
//  3. 失败退避可指数可抖动——复用 retry.Config（含 DelayHint 服务端建议），
//     不再是手写固定几秒；不可续拉的错误带错误返回、不空转
//  4. 单项 panic 不杀循环——每项经 async.RunSafeCtx 就地收编成 *async.PanicError，
//     同时汇报全局 PanicHandler；循环存活
//  5. 毒丸有出口——死信档（HandleRetries>0 + DeadLetter，#231）：可续拉的失败
//     就地重试至多 N 次后交钩子出档、批次继续，毒丸不再绑架整条游标；缺省不开档，
//     首项失败即停批回退（Retryable 门判死的错误开档也带错返回，#226 语义不变）
//
// # 与相邻原语的分工
//
//   - retry.Do/DoValue：**有界**的「动作重试」——N 次之内把一件事做成，做不成返回错误。
//   - retry.Poll：**有界**的「条件轮询」——到 deadline 为止等条件成立。
//   - loop.Run：**定时**驱动的周期任务（心跳/清理/对账）。
//   - poll.RunCursor：**数据**驱动的常驻循环——只要 ctx 还活着就一直拉，游标是它
//     唯一的进度凭据，且这个凭据由库负责只进不退。
//
// 需要「N 次之内把一批数据处理完」用 retry；需要「一直收消息」用本包。
//
// # 用法
//
//	cfg := poll.Config{
//	    ErrorBackoff: retry.Config{
//	        Delay:       200 * time.Millisecond,
//	        Exponential: true,
//	        MaxDelay:    5 * time.Second,
//	        JitterRatio: 0.2, // 多实例同时撞故障时打散重试
//	    },
//	    IdleDelay: 0, // 服务端 hold 的自适应长轮询；不 hold 的接口必须给正值
//	    OnAdvance: store.Save,
//	}
//	final, err := poll.RunCursor(ctx, cfg, cursor, fetchBatch, handleOne)
//
// 游标持久化由调用方注入（OnAdvance）——各消费方的存储不同，库只负责**时机**
// （先落盘成功、再在库内算作推进）。进程退出时用 RunCursor 的返回值做最后一次
// 兜底持久化。
package poll
