// Package window 提供滑动窗口统计容器（移植 go-zero collection.RollingWindow）。
//
// RollingWindow 把时间轴切成 size 个 interval 宽的桶：Add 写入当前桶，
// 跨桶时惰性重置过期桶（无后台 goroutine）；Reduce 遍历有效桶执行聚合，
// 可选 IgnoreCurrentBucket 跳过尚未写满的当前桶。桶类型可定制
// （实现 BucketInterface 即可），默认桶 Bucket 提供 Sum/Count 聚合。
//
// 算法、惰性重置与时间对齐语义与上游 go-zero 保持一致；差异点：
//   - 时间源可注入：WithWindowClock（缺省 time.Now），测试注入假时钟即可
//     完全确定地驱动窗口推进；
//   - 选项类型按本仓硬规则命名 WindowOption（上游为 RollingWindowOption）。
//
// 用法:
//
//	r := window.NewRollingWindow[int64](func() *window.Bucket[int64] {
//	    return new(window.Bucket[int64])
//	}, 10, time.Second) // 10 个桶，每桶 1s
//	r.Add(1)
//	var sum int64
//	r.Reduce(func(b *window.Bucket[int64]) { sum += b.Sum })
package window
