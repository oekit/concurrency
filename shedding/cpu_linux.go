//go:build linux

package shedding

import (
	"errors"
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	procStatFile        = "/proc/stat"
	procSelfCgroupFile  = "/proc/self/cgroup"
	cgroupDir           = "/sys/fs/cgroup"
	cgroupV2CPUMaxFile  = cgroupDir + "/cpu.max"
	cgroupV2CPUStatFile = cgroupDir + "/cpu.stat"
	cgroupV2CPUsFile    = cgroupDir + "/cpuset.cpus.effective"

	// cpuTicks 为 USER_HZ（/proc/stat 时间片每秒 tick 数，Linux 恒为 100）。
	cpuTicks = 100
	// procStatFields 为 "cpu" 汇总行最少字段数：关键字 + user nice system idle
	// iowait irq softirq steal 共 9 列（与上游不同：上游漏计 steal，见
	// parseProcStatTotalNS 注释）。
	procStatFields = 9
	// nsPerTick 为单个 CPU 时间片的纳秒数。
	nsPerTick = int64(time.Second) / cpuTicks
)

// errCPUUnavailable 表示当前环境无法采样 CPU（/proc 或 cgroup 不可用）；
// 缺省管线按 0 处理（fail-open，全放行）。
var errCPUUnavailable = errors.New("shedding: cpu usage unavailable")

// cgroupLayout 为初始化时探测到的 cgroup 拓扑：v2 统一层级或 v1 各 cpu 系
// 控制器目录。
type cgroupLayout struct {
	v2     bool
	v1dirs map[string]string
}

// CPU 差分采样状态：仅 defaultRawCPUUsage（缺省采样 goroutine）与初始化触碰，
// 用互斥锁保护以容忍测试/多消费者并发调用。
var (
	cpuSampleMu  sync.Mutex
	cpuInitOnce  sync.Once
	noCgroup     bool         // 初始化失败（/proc 或 cgroup 不可用）→ 永久禁用采样
	activeLayout cgroupLayout // 初始化时探测到的 cgroup 拓扑

	preSystemNS uint64  // 上次系统总 CPU 时间（ns，/proc/stat 差分基线）
	preTotalNS  uint64  // 上次 cgroup CPU 时间（ns，差分基线）
	limitCpus   float64 // 生效 CPU 限额（核数与 cgroup 配额取小）
	coreCount   uint64  // 可用核数（cpuset）
)

// defaultRawCPUUsage 返回一次差分采样的容器 CPU 使用率（毫秒‰，0~1000）：
//
//	usage = ΔcgroupCPU × cores × 1000 / (ΔsystemCPU × limit)
//
// 首次调用做基线初始化（返回 0 无意义），并探测 cgroup v1/v2 与配额；
// /proc 或 cgroup 不可用（如 WSL、无 cgroup 挂载）时返回错误——调用方
// refreshCPUUsage 按 0 处理，降载自动失效（fail-open，对齐上游 noCgroup 语义）。
// 与上游差异：cgroup 版本判定用 /proc/self/cgroup 的 "0::" 条目（等价于上游
// 的 statfs 超级魔数检查，但不依赖 golang.org/x/sys），且探测结果缓存一次。
func defaultRawCPUUsage() (int64, error) {
	cpuSampleMu.Lock()
	defer cpuSampleMu.Unlock()

	cpuInitOnce.Do(func() {
		if err := initCPUSampling(); err != nil {
			noCgroup = true
		}
	})
	if noCgroup {
		return 0, errCPUUnavailable
	}

	total, err := activeLayout.cpuUsageNS()
	if err != nil {
		return 0, err
	}
	system, err := systemCPUUsageNS()
	if err != nil {
		return 0, err
	}

	var usage int64
	cpuDelta := total - preTotalNS
	systemDelta := system - preSystemNS
	if cpuDelta > 0 && systemDelta > 0 {
		usage = int64(float64(cpuDelta) * float64(coreCount) * cpuMax /
			(float64(systemDelta) * limitCpus))
		if usage > cpuMax {
			usage = cpuMax
		}
	}
	preSystemNS = system
	preTotalNS = total

	return usage, nil
}

// initCPUSampling 建立差分基线：探测可用核数与 cgroup 配额（limit 取两者较小），
// 记录首轮系统/cgroup CPU 时间。任一步失败即整体禁用（对齐上游 initialize）。
func initCPUSampling() error {
	layout, err := detectCgroup()
	if err != nil {
		return err
	}
	cpus, err := layout.effectiveCpus()
	if err != nil {
		return err
	}
	activeLayout = layout
	coreCount = uint64(cpus)
	limitCpus = float64(cpus)
	if quota, quotaErr := layout.cpuQuota(); quotaErr == nil && quota > 0 && quota < limitCpus {
		limitCpus = quota
	}

	preSystemNS, err = systemCPUUsageNS()
	if err != nil {
		return err
	}
	preTotalNS, err = layout.cpuUsageNS()
	return err
}

// detectCgroup 读取 /proc/self/cgroup 并判定 cgroup 拓扑。
func detectCgroup() (cgroupLayout, error) {
	data, err := os.ReadFile(procSelfCgroupFile)
	if err != nil {
		return cgroupLayout{}, err
	}
	return detectCgroupFrom(string(data))
}

// detectCgroupFrom 解析 /proc/self/cgroup 内容判定 cgroup 版本：存在
// "0::<path>" 条目即 v2 统一层级；同时出现 v1 cpu 系控制器条目则为混合模式——
// 按 v1 处理（与上游 statfs 判定 /sys/fs/cgroup 根文件系统类型的行为一致）。
// v1 各控制器的实际子路径被有意忽略（上游同款）：容器内 /sys/fs/cgroup 即本
// cgroup namespace 的根。
func detectCgroupFrom(data string) (cgroupLayout, error) {
	layout := cgroupLayout{v1dirs: make(map[string]string)}
	sawV2 := false
	for _, line := range strings.Split(data, "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		cols := strings.SplitN(line, ":", 3)
		if len(cols) != 3 {
			return cgroupLayout{}, fmt.Errorf("shedding: invalid cgroup line: %s", line)
		}
		if cols[0] == "0" && cols[1] == "" {
			sawV2 = true
			continue
		}
		// only read cpu staff
		if !strings.HasPrefix(cols[1], "cpu") {
			continue
		}
		// comma-separated list of controllers for cgroup version 1
		for _, val := range strings.Split(cols[1], ",") {
			layout.v1dirs[val] = path.Join(cgroupDir, val)
		}
	}
	if len(layout.v1dirs) > 0 {
		return layout, nil
	}
	if sawV2 {
		layout.v2 = true
		return layout, nil
	}
	return cgroupLayout{}, errors.New("shedding: no cpu cgroup found in " + procSelfCgroupFile)
}

// cpuQuota 返回 cgroup CPU 配额（核数）；无限制返回 -1，读取失败返回错误。
func (cg cgroupLayout) cpuQuota() (float64, error) {
	if cg.v2 {
		data, err := os.ReadFile(cgroupV2CPUMaxFile)
		if err != nil {
			return 0, err
		}
		return parseCPUMax(string(data))
	}

	quotaUs, err := readInt64(path.Join(cg.v1dirs["cpu"], "cpu.cfs_quota_us"))
	if err != nil {
		return 0, err
	}
	if quotaUs == -1 {
		return -1, nil
	}
	periodUs, err := readUint64(path.Join(cg.v1dirs["cpu"], "cpu.cfs_period_us"))
	if err != nil {
		return 0, err
	}
	return float64(quotaUs) / float64(periodUs), nil
}

// cpuUsageNS 返回 cgroup 累计 CPU 消耗（纳秒）：v2 读 cpu.stat 的 usage_usec，
// v1 读 cpuacct.usage（本身即纳秒）。
func (cg cgroupLayout) cpuUsageNS() (uint64, error) {
	if cg.v2 {
		data, err := os.ReadFile(cgroupV2CPUStatFile)
		if err != nil {
			return 0, err
		}
		usec, err := parseCPUStatUsageUSec(string(data))
		if err != nil {
			return 0, err
		}
		return usec * uint64(time.Microsecond), nil
	}
	return readUint64(path.Join(cg.v1dirs["cpuacct"], "cpuacct.usage"))
}

// effectiveCpus 返回 cpuset 允许的 CPU 数：v2 读 cpuset.cpus.effective，
// v1 读 cpuset.cpus。
func (cg cgroupLayout) effectiveCpus() (int, error) {
	file := cgroupV2CPUsFile
	if !cg.v2 {
		file = path.Join(cg.v1dirs["cpuset"], "cpuset.cpus")
	}
	data, err := os.ReadFile(file)
	if err != nil {
		return 0, err
	}
	return parseCPURangeCount(string(data))
}

// systemCPUUsageNS 读取 /proc/stat "cpu" 汇总行并换算为纳秒。
func systemCPUUsageNS() (uint64, error) {
	data, err := os.ReadFile(procStatFile)
	if err != nil {
		return 0, err
	}
	return parseProcStatTotalNS(data)
}

// parseProcStatTotalNS 从 /proc/stat 内容解析首字段恰为 "cpu" 的汇总行
// （不含 cpu0/cpu1 等逐核行），累加 8 列时间片并换算为纳秒。
// 与上游差异（有意）：上游累加 fields[1:8] 漏掉第 8 列 steal——虚拟化场景
// steal 被盗的时间不计入系统分母，会高估使用率；本实现累加全部 8 列。
func parseProcStatTotalNS(data []byte) (uint64, error) {
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 || fields[0] != "cpu" {
			continue
		}
		if len(fields) < procStatFields {
			return 0, errors.New("shedding: bad format of cpu stats")
		}
		var totalTicks uint64
		for _, f := range fields[1:procStatFields] {
			v, err := strconv.ParseUint(f, 10, 64)
			if err != nil {
				return 0, fmt.Errorf("shedding: bad cpu stat field %q: %w", f, err)
			}
			totalTicks += v
		}
		return totalTicks * uint64(nsPerTick), nil
	}
	return 0, errors.New("shedding: cpu stats line not found")
}

// parseCPUMax 解析 cgroup v2 cpu.max 内容（"quota period"）为核数配额；
// "max"（无限制）返回 -1。
func parseCPUMax(data string) (float64, error) {
	fields := strings.Fields(data)
	if len(fields) != 2 {
		return 0, fmt.Errorf("shedding: bad %s file: %s", cgroupV2CPUMaxFile, data)
	}
	if fields[0] == "max" {
		return -1, nil
	}
	quotaUs, err := strconv.ParseInt(fields[0], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("shedding: bad cpu.max quota %q: %w", fields[0], err)
	}
	periodUs, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("shedding: bad cpu.max period %q: %w", fields[1], err)
	}
	return float64(quotaUs) / float64(periodUs), nil
}

// parseCPUStatUsageUSec 解析 cgroup v2 cpu.stat 内容中的 usage_usec 行
// （微秒）。
func parseCPUStatUsageUSec(data string) (uint64, error) {
	for _, line := range strings.Split(data, "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 && fields[0] == "usage_usec" {
			return parseUint(fields[1])
		}
	}
	return 0, errors.New("shedding: usage_usec not found in " + cgroupV2CPUStatFile)
}

// parseCPURangeCount 解析 cpuset CPU 列表（如 "0-3,8,10-11"）并返回去重后的
// CPU 个数。
func parseCPURangeCount(data string) (int, error) {
	val := strings.TrimSpace(data)
	if val == "" {
		return 0, errors.New("shedding: empty cpuset")
	}
	seen := make(map[uint64]struct{})
	for _, part := range strings.Split(val, ",") {
		if strings.Contains(part, "-") {
			bounds := strings.SplitN(part, "-", 2)
			lo, err := parseUint(bounds[0])
			if err != nil {
				return 0, fmt.Errorf("shedding: bad cpuset list format: %s", val)
			}
			hi, err := parseUint(bounds[1])
			if err != nil {
				return 0, fmt.Errorf("shedding: bad cpuset list format: %s", val)
			}
			if hi < lo {
				return 0, fmt.Errorf("shedding: bad cpuset list format: %s", val)
			}
			for i := lo; i <= hi; i++ {
				seen[i] = struct{}{}
			}
			continue
		}
		v, err := parseUint(part)
		if err != nil {
			return 0, err
		}
		seen[v] = struct{}{}
	}
	return len(seen), nil
}

// readInt64 读单个整数值文件（v1 配额可为负数 -1 表示无限制）。
func readInt64(file string) (int64, error) {
	data, err := os.ReadFile(file)
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
}

// readUint64 读单个非负整数值文件（v1 cpuacct.usage 等）。
func readUint64(file string) (uint64, error) {
	data, err := os.ReadFile(file)
	if err != nil {
		return 0, err
	}
	return parseUint(strings.TrimSpace(string(data)))
}

// parseUint 解析非负整数：溢出（ErrRange）与负数按上游语义回退 0（不作为
// 错误——cgroup 计数器极端大值时降级而非禁用采样）。
func parseUint(s string) (uint64, error) {
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		if errors.Is(err, strconv.ErrRange) {
			return 0, nil
		}
		return 0, fmt.Errorf("shedding: bad int format: %s", s)
	}
	if v < 0 {
		return 0, nil
	}
	return uint64(v), nil
}
