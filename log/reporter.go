package log

// Reporter 用于报告读日志时发生的错误.
type Reporter interface {
	Corruption(err error)
}
