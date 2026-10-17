// Package loop 提供周期任务循环原语：ctx 感知 + 单轮 panic 收编 + 忙时丢 tick + 排水。
//
// 收编自消费方反复手搓的同一模式（过期清理 / 心跳 / 快照落盘 / 配置轮询等），
// 把四个易漂移语义一次钉死：
//
//  1. 忙时丢 tick——fn 同步执行、不重叠，执行期间的 tick 直接丢弃
//  2. 单轮 panic 不打死循环——按 async.RunSafe 收编（走全局 PanicHandler），循环存活
//  3. 停机排水——ctx 取消拦「下一轮」，在飞轮次用独立 ctx 跑完；Wait() 等排水结束
//  4. 首跑延迟可被停机打断——FirstDelay 走 select 而非 time.Sleep
//
// # 用法
//
//	ctx, cancel := context.WithCancel(context.Background())
//	l := loop.Run(ctx, loop.Config{Interval: time.Hour, Name: "expire-sweep"}, func(ctx context.Context) {
//	    sweep(ctx)
//	})
//	// ... 服务运行 ...
//	cancel()      // 拦下一轮；在飞轮次跑完
//	l.Wait()      // 排水（超时预算由调用方包一层控制）
//
// # 语义契约
//
//   - fn 立即（或 FirstDelay 后）首跑，之后每 Interval 一轮
//   - fn 内 panic 按 RunSafe 收编，循环存活；Config.Name 非空时汇报带 task 标签
//   - 忙时丢 tick（fn 同步执行，不重叠）
//   - ctx 取消拦「下一轮」，在飞轮次跑完；Wait() 返回即排水完成
//   - Interval <= 0 时按 0 处理（忙轮询），调用方应给合理间隔
package loop
