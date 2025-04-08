package main

import (
	"fmt"
	"os"
	"time"

	"github.com/fatih/color"
)

// Logger provides colored logging functionality
type Logger struct {
	infoColor    *color.Color
	successColor *color.Color
	warnColor    *color.Color
	errorColor   *color.Color
	debugColor   *color.Color
	memoryColor  *color.Color
}

// NewLogger creates a new Logger instance
func NewLogger() *Logger {
	return &Logger{
		infoColor:    color.New(color.FgCyan),
		successColor: color.New(color.FgGreen),
		warnColor:    color.New(color.FgYellow),
		errorColor:   color.New(color.FgRed),
		debugColor:   color.New(color.FgWhite),
		memoryColor:  color.New(color.FgMagenta),
	}
}

// formatMessage formats a log message with timestamp
func (l *Logger) formatMessage(message string) string {
	return fmt.Sprintf("[%s] %s", time.Now().Format("2006-01-02 15:04:05"), message)
}

// Info logs an info message in cyan
func (l *Logger) Info(format string, args ...interface{}) {
	message := fmt.Sprintf(format, args...)
	l.infoColor.Fprintln(os.Stdout, l.formatMessage(message))
}

// Success logs a success message in green
func (l *Logger) Success(format string, args ...interface{}) {
	message := fmt.Sprintf(format, args...)
	l.successColor.Fprintln(os.Stdout, l.formatMessage(message))
}

// Warn logs a warning message in yellow
func (l *Logger) Warn(format string, args ...interface{}) {
	message := fmt.Sprintf(format, args...)
	l.warnColor.Fprintln(os.Stdout, l.formatMessage(message))
}

// Error logs an error message in red
func (l *Logger) Error(format string, args ...interface{}) {
	message := fmt.Sprintf(format, args...)
	l.errorColor.Fprintln(os.Stderr, l.formatMessage(message))
}

// Debug logs a debug message in white
func (l *Logger) Debug(format string, args ...interface{}) {
	message := fmt.Sprintf(format, args...)
	l.debugColor.Fprintln(os.Stdout, l.formatMessage(message))
}

// Memory logs memory usage information in magenta
func (l *Logger) Memory(format string, args ...interface{}) {
	message := fmt.Sprintf(format, args...)
	l.memoryColor.Fprintln(os.Stdout, l.formatMessage(fmt.Sprintf("MEMORY: %s", message)))
}
