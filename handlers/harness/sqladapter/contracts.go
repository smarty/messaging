package sqladapter

type Logger interface {
	Printf(format string, args ...any)
}
