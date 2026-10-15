//go:build linux

package shedding

import (
	"math"
	"os"
	"testing"
)

// TestParseProcStatTotalNS 解析 /proc/stat "cpu" 汇总行（8 列时间片求和，
// 跳过 cpu0 等逐核行），换算纳秒；坏格式报错。
func TestParseProcStatTotalNS(t *testing.T) {
	data := []byte("cpu  10 20 30 40 50 60 70 80 100 200\ncpu0 1 2 3 4 5 6 7 8 9 10\nintr 123\n")
	// (10+20+30+40+50+60+70+80) ticks × 10ms = 360 × 1e7 ns
	got, err := parseProcStatTotalNS(data)
	if err != nil {
		t.Fatalf("parse err = %v", err)
	}
	if want := uint64(360) * uint64(nsPerTick); got != want {
		t.Fatalf("total = %d, want %d", got, want)
	}

	cases := []struct {
		name string
		data string
	}{
		{"no cpu line", "cpu0 1 2 3 4 5 6 7 8\nintr 1\n"},
		{"too few fields", "cpu 1 2 3\nintr 1\n"},
		{"non numeric field", "cpu 1 2 x 4 5 6 7 8\n"},
		{"empty", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := parseProcStatTotalNS([]byte(tc.data)); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

// TestParseCPUMax 解析 cgroup v2 cpu.max（"max 100000" → 无限制 -1；
// "quota period" → 核数配额）。
func TestParseCPUMax(t *testing.T) {
	cases := []struct {
		data string
		want float64
	}{
		{"max 100000\n", -1},
		{"200000 100000\n", 2},
		{"50000 100000\n", 0.5},
		{"150000 100000\n", 1.5},
	}
	for _, tc := range cases {
		got, err := parseCPUMax(tc.data)
		if err != nil {
			t.Fatalf("parse %q err = %v", tc.data, err)
		}
		if math.Abs(got-tc.want) > 1e-9 {
			t.Fatalf("parse %q = %v, want %v", tc.data, got, tc.want)
		}
	}

	for _, bad := range []string{"200000\n", "a b\n", "100000 x\n", ""} {
		if _, err := parseCPUMax(bad); err == nil {
			t.Fatalf("parse %q expected error", bad)
		}
	}
}

// TestParseCPUStatUsageUSec 解析 cgroup v2 cpu.stat 的 usage_usec 行。
func TestParseCPUStatUsageUSec(t *testing.T) {
	got, err := parseCPUStatUsageUSec("nr_periods 5\nnr_throttled 2\nusage_usec 123456\nthrottled_usec 9\n")
	if err != nil {
		t.Fatalf("parse err = %v", err)
	}
	if got != 123456 {
		t.Fatalf("usage_usec = %d, want 123456", got)
	}
	if _, err := parseCPUStatUsageUSec("nr_periods 5\n"); err == nil {
		t.Fatal("missing usage_usec expected error")
	}
}

// TestParseCPURangeCount 解析 cpuset CPU 列表（区间/枚举/混合，去重计数）。
func TestParseCPURangeCount(t *testing.T) {
	cases := []struct {
		data string
		want int
	}{
		{"0-3\n", 4},
		{"0,2,4\n", 3},
		{"0-1,3,5-6\n", 5},
		{"0-1,1\n", 2}, // 去重
		{"  0-3  \n", 4},
	}
	for _, tc := range cases {
		got, err := parseCPURangeCount(tc.data)
		if err != nil {
			t.Fatalf("parse %q err = %v", tc.data, err)
		}
		if got != tc.want {
			t.Fatalf("parse %q = %d, want %d", tc.data, got, tc.want)
		}
	}

	for _, bad := range []string{"", "3-1\n", "x\n", "1,\n"} {
		if _, err := parseCPURangeCount(bad); err == nil {
			t.Fatalf("parse %q expected error", bad)
		}
	}
}

// TestParseUint 上游同款语义：溢出与负数回退 0（不视为错误），坏格式报错。
func TestParseUint(t *testing.T) {
	if v, err := parseUint("5"); err != nil || v != 5 {
		t.Fatalf("parseUint(5) = (%d, %v)", v, err)
	}
	if v, err := parseUint("-1"); err != nil || v != 0 {
		t.Fatalf("parseUint(-1) = (%d, %v), want (0, nil)", v, err)
	}
	if v, err := parseUint("99999999999999999999999"); err != nil || v != 0 {
		t.Fatalf("overflow = (%d, %v), want (0, nil)", v, err)
	}
	if _, err := parseUint("abc"); err == nil {
		t.Fatal("parseUint(abc) expected error")
	}
}

// TestDetectCgroup 按合成 /proc/self/cgroup 内容判定版本：
// 纯 v2 / 纯 v1 / 混合（按 v1 处理，与上游 statfs 判定一致）/ 坏行。
func TestDetectCgroup(t *testing.T) {
	cg, err := detectCgroupFrom("0::/system.slice/docker.service\n")
	if err != nil || !cg.v2 || len(cg.v1dirs) != 0 {
		t.Fatalf("v2 detect = (%+v, %v), want v2 only", cg, err)
	}

	cg, err = detectCgroupFrom("12:pids:/x\n3:cpuacct,cpu:/system.slice/foo\n2:cpuset:/foo\n")
	if err != nil || cg.v2 {
		t.Fatalf("v1 detect = (%+v, %v), want v1", cg, err)
	}
	for _, dir := range []string{"cpu", "cpuacct", "cpuset"} {
		if cg.v1dirs[dir] == "" {
			t.Fatalf("v1 dirs missing %s: %+v", dir, cg.v1dirs)
		}
	}
	if _, ok := cg.v1dirs["pids"]; ok {
		t.Fatalf("non-cpu controller must be skipped: %+v", cg.v1dirs)
	}

	// 混合模式：v1 cpu 控制器在场 → 按 v1（对齐上游）
	cg, err = detectCgroupFrom("2:cpu:/hybrid\n0::/hybrid\n")
	if err != nil || cg.v2 {
		t.Fatalf("hybrid detect = (%+v, %v), want v1 wins", cg, err)
	}

	if _, err := detectCgroupFrom("no-colon-line\n"); err == nil {
		t.Fatal("bad line expected error")
	}
	if _, err := detectCgroupFrom("12:pids:/x\n"); err == nil {
		t.Fatal("no cpu controller expected error")
	}
}

// TestDefaultRawCPUUsage_Smoke 真实环境冒烟：基线建立后第二次采样值域合法。
// 采样不可用的环境（无 cgroup 挂载等）允许返回错误——那是 fail-open 路径。
func TestDefaultRawCPUUsage_Smoke(t *testing.T) {
	if _, err := os.Stat(procStatFile); err != nil {
		t.Skipf("%s unavailable: %v", procStatFile, err)
	}
	first, err := defaultRawCPUUsage()
	if err != nil {
		t.Skipf("cpu sampling unavailable in this environment: %v", err)
	}
	_ = first // 首次调用建立基线，值无意义
	second, err := defaultRawCPUUsage()
	if err != nil {
		t.Fatalf("second sample err = %v", err)
	}
	if second < 0 || second > cpuMax {
		t.Fatalf("usage = %d, out of [0, %d]", second, cpuMax)
	}
}
