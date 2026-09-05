// Package console 提供控制台输出的 ANSI 颜色工具：
// INFO 绿色、ALERT/ERROR 红色、WARNING 黄色（整行/整块着色）；
// TRACE 与容器日志透传（--log-console）不着色，保持原始内容。
package console

const (
	reset  = "\033[0m"
	red    = "\033[31m"
	green  = "\033[32m"
	yellow = "\033[33m"
)

// Green 绿色（INFO 级别整行）
func Green(s string) string { return green + s + reset }

// Red 红色（ALERT / ERROR 级别整行/整块）
func Red(s string) string { return red + s + reset }

// Yellow 黄色（WARNING 级别整行/整块）
func Yellow(s string) string { return yellow + s + reset }
