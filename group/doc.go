// Package group 提供并发 Actor 生命周期管理和 WaitGroup 封装。
//
//   - Group: Actor 模式，第一个返回的函数会中断所有其他 Actor
//   - WaitGroupWrapper: context 感知的 WaitGroup，支持 panic 恢复和错误传播
package group
