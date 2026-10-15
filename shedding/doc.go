// Package shedding 提供 CPU 自适应降载（移植 go-zero core/load 的
// adaptiveshedder，算法忠实上游）。
//
// 与熔断（breaker，保护"下游调用"）互补，降载保护"本服务自身"：当进程 CPU
// 过载且在途并发超过服务容量估计时主动拒绝新请求，返回 ErrServiceOverloaded
// （可 errors.Is 判定）。核心机制：
//   - 服务容量用 Little's Law 在线估计：maxFlight = maxPass × minRt × 桶频，
//     其中 maxPass 为滑动窗口内单桶最大通过数（吞吐上界）、minRt 为窗口内
//     最小平均响应时间（毫秒）——两者都由调用方经 Promise.Pass 回填驱动；
//   - 只有 (CPU 过载 || 冷却期内) 且 实际在途并发（flying 与其滞后均值
//     avgFlying 都）超过 maxFlight×过载因子 时才丢弃：过载因子 =
//     (1000-cpu)/(1000-threshold)，夹在 [0.1, 1]——CPU 越满放行越少，
//     但无论多满都至少保留 10% 容量；
//   - avgFlying 为 EMA（beta=0.9，仅在请求完成即 delta<0 时更新）：并发骤降
//     时均值滞后偏高、少放行，骤升时滞后偏低、多放行，尽量喂饱服务；
//   - 冷却期 1s：最近发生过丢弃时，即使 CPU 已回落仍继续丢弃至冷却期满，
//     防止过载边界抖动。
//
// CPU 采样（缺省源）为纯标准库实现：/proc/stat 差分 + cgroup v1/v2 配额感知
// （限 CPU 容器下按配额折算）、250ms 周期、beta=0.95 指数平滑、毫秒‰ 单位
// （0~1000）。采样 goroutine 惰性启动（首个未注入 CPU 源的 Allow 才拉起）、
// 每周期 recover 兜底；非 linux 平台 polyfill 恒返回错误 → 降载自动禁用、
// 全放行（fail-open）；CPU 源读取错误同样视为 0（全放行）。
//
// 算法与上游 go-zero 保持一致；差异点：
//   - API ctx-aware（Allow(ctx)），ctx 已取消直接返回 ctx.Err()，不消耗统计；
//   - 时间源可注入 WithShedderClock（缺省 time.Now），冷却期与 RT 统计共用；
//   - CPU 源可注入包级 SetCPUUsageFunc（仿 async.SetPanicHandler，测试/自定义
//     平台采样）；注入后缺省采样管线不再被调用；
//   - 可观测性零强制注入：丢弃事件走可选钩子 WithShedderRejectHandler
//     （携带上游同格式诊断消息，缺省 no-op），由上层显式接 logx/metrics；
//   - 统计容器复用本仓 window.RollingWindow（上游为 collection.RollingWindow）；
//   - 不提供上游的包级 Disable（需要禁用时直接用 NewNopShedder）。
//
// 用法:
//
//	shedder := shedding.NewAdaptiveShedder()
//	p, err := shedder.Allow(ctx)
//	if errors.Is(err, shedding.ErrServiceOverloaded) {
//	    // 过载：快速失败/走降级响应
//	    return err
//	}
//	defer func() {
//	    if err != nil {
//	        p.Fail()
//	    } else {
//	        p.Pass()
//	    }
//	}()
package shedding
