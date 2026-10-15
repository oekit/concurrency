//go:build !linux

package shedding

import "errors"

// errCPUUnsupported 表示当前平台无 /proc 与 cgroup CPU 统计，不支持缺省采样。
var errCPUUnsupported = errors.New("shedding: cpu usage sampling is only supported on linux")

// defaultRawCPUUsage 为非 linux 平台的 polyfill：恒返回错误。缺省采样管线把
// 错误按 0 处理（EMA 恒 0），降载自动禁用、全放行（fail-open）；业务可用
// SetCPUUsageFunc 注入自定义采样源（如容器 runtime API）重新启用降载。
func defaultRawCPUUsage() (int64, error) {
	return 0, errCPUUnsupported
}
