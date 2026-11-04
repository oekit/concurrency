package retry

import (
	"errors"
	"fmt"
)

// DetailError 是「错误 + 失败伴生数据」的容器（#138）。
//
// DoValue 的契约是「失败恒返零值」——防止调用方误用未成功的产物。但诊断/分类/
// 审计场景确实需要与失败伴生的中间结果（HTTP 状态码、响应体摘要、耗时），硬迁
// DoValueLast 会打破零值契约。正解是让 fn 把伴生数据**打包进 error**：error 本来
// 就是失败信息的唯一合法出口，DoValue 返回 lastErr 时伴生数据随之送达，零值契约
// 一字不动。
//
//	typical（octopus sendWithRetry 形态）：
//	res, err := retry.DoValue(ctx, cfg, func(attempt int) (*SendResult, error) {
//	    res, err := send(ctx)
//	    if err != nil {
//	        return nil, retry.WithDetail(err, res) // res 带 statusCode，失败也要
//	    }
//	    return res, nil
//	})
//	if err != nil {
//	    if res, ok := retry.Detail[*SendResult](err); ok {
//	        log.SendRecord(res.StatusCode, res.Duration) // 失败也记分类与审计
//	    }
//	}
//
// 约定：
//   - Detail 只是诊断伴生数据，**不是成功产物**——消费方不得把它当 fn 成功的
//     返回值使用（DoValue 返回的 T 仍是零值，这个契约没变）；
//   - 每次重试失败都会重新装配，DoValue 返回的 lastErr 携带的是**最后一次尝试**
//     的伴生数据；
//   - Error()/Unwrap() 透传 Err——errors.Is/As 链与既有错误分类零破坏。
type DetailError struct {
	// Err 是被包装的原始错误（nil 时 Error() 退化兜底文案，构造请走 WithDetail）。
	Err error
	// Detail 是失败伴生的诊断数据（如带 statusCode 的响应结构）。
	Detail any
}

func (e *DetailError) Error() string {
	if e.Err == nil {
		return fmt.Sprintf("retry: detail error (detail=%v)", e.Detail)
	}
	return e.Err.Error()
}

// Unwrap 透传原始错误：errors.Is/As 沿链继续走，既有错误分类不受包装影响。
func (e *DetailError) Unwrap() error {
	return e.Err
}

// WithDetail 给 err 挂上失败伴生数据；err 为 nil 时原样返回 nil（fn 的
// 「成功短路」路径可以无判空地统一走本函数）。
func WithDetail(err error, detail any) error {
	if err == nil {
		return nil
	}
	return &DetailError{Err: err, Detail: detail}
}

// Detail 从错误链中提取最后一次尝试的伴生数据。
// ok=false 的情形：链上没有 DetailError、或 Detail 的实际类型与 T 不符
// （不同 attempt 装配不同类型时，按类型断言失败处理，不 panic）。
func Detail[T any](err error) (T, bool) {
	var zero T
	var de *DetailError
	if !errors.As(err, &de) {
		return zero, false
	}
	if d, ok := de.Detail.(T); ok {
		return d, true
	}
	return zero, false
}
