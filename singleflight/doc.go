// Package singleflight 提供并发请求合并（防击穿）工具。
//
// 同一个 key 的并发调用只执行一次 fn，其他调用共享结果。
// 适用于缓存穿透防护、token 刷新、配置加载等场景。
//
// # 核心特性
//
//   - 基于 sync.Map + close(chan struct{}) 实现，零外部依赖
//   - 泛型支持，类型安全
//   - race detector 友好
//   - panic 安全：fn panic 不会导致等待者死锁
//
// # 快速接入
//
//	sf := singleflight.New[string]()
//	val, err := sf.Do(ctx, "token:wechat_app", func(ctx context.Context) (string, error) {
//	    return fetchAccessToken(ctx)
//	})
//
// # 原理
//
//	调用 Do(key, fn) 时：
//	 1. sync.Map.LoadOrStore 保证只有一个 goroutine 成为 caller
//	 2. caller 执行 fn，完成后 close(ready) 广播通知
//	 3. 其他 goroutine (waiter) 通过 <-ready 等待，读取共享结果
//	 4. close 提供 happens-before 保证，字段读取无 data race
package singleflight
