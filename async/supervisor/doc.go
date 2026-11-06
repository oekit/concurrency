// Package supervisor 提供「按键常驻循环集合」与动态目标集的持续对齐原语：
// Reconcile 对齐、Stop/Wait 停机排水，组内单条循环的监督语义复用 async.Supervise。
//
// 收编自通道/连接/租户 worker 池类服务反复手写的那份形状——`map[K]entry{cancel,
// spec, done}` + mutex，自己实现 spec diff、done 非阻塞探活、意外死亡重启。锁内的
// cancel/delete 顺序、快照比对都是并发正确性敏感点，每抄一份就多一处漂移。
//
// # 在矩阵里的位置
//
//	async.Supervise    单条循环活得好不好（panic 收编 + 指数退避重启 + 失败上限）
//	loop.Run           固定周期任务（心跳 / 清理 / 对账）
//	supervisor.Group   一组循环的**集合**与外部目标集持续对齐（起/停/规格变更/探活）
//
// 三者职责不重叠：Group 只管集合对齐，组内单条循环的健壮性交给 Supervise。
//
// # 最小用法
//
//	ctx, cancel := context.WithCancel(context.Background())
//	defer cancel()
//
//	g := supervisor.NewGroup(ctx,
//	    func(b Bot) string { return fmt.Sprintf("%d:%s", b.ID, b.Token) }, // 身份 + 规格变更判据
//	    func(ctx context.Context, b Bot) error { return b.Run(ctx) },      // 常驻循环
//	    supervisor.WithGroupTask("bot"),
//	)
//
//	// 定期把目标集拉下来对齐（对齐节奏交给 loop，不在本包再写一份定时器语义）
//	loop.Run(ctx, loop.Config{Interval: 30 * time.Second}, func(ctx context.Context) {
//	    bots, err := loadBots(ctx)
//	    if err != nil {
//	        return // 拉不到就保持现状：别因为一次读库失败把全部循环停掉
//	    }
//	    g.Reconcile(bots)
//	})
//
//	// 停机
//	cancel()   // 或 g.Stop()
//	g.Wait()   // 排水
//
// # 观测
//
// Len（受管目标数）/ Running（循环仍在跑的目标数）/ Draining（已移除但未退出的
// 循环数）三个非阻塞探针足够区分「集合对但没在服务」与「停机卡住」两类故障。
package supervisor
