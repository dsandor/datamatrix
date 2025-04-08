package main

import (
	"fmt"
	"runtime"
)

// MemoryStats holds memory usage statistics
type MemoryStats struct {
	Alloc      uint64 // bytes allocated and not yet freed
	TotalAlloc uint64 // bytes allocated (even if freed)
	Sys        uint64 // bytes obtained from system
	NumGC      uint32 // number of completed GC cycles
}

// GetMemoryStats returns current memory usage statistics
func GetMemoryStats() MemoryStats {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	
	return MemoryStats{
		Alloc:      m.Alloc,
		TotalAlloc: m.TotalAlloc,
		Sys:        m.Sys,
		NumGC:      m.NumGC,
	}
}

// FormatBytes formats bytes to a human-readable string (KB, MB, GB)
func FormatBytes(bytes uint64) string {
	const (
		KB = 1024
		MB = 1024 * KB
		GB = 1024 * MB
	)

	switch {
	case bytes >= GB:
		return fmt.Sprintf("%.2f GB", float64(bytes)/float64(GB))
	case bytes >= MB:
		return fmt.Sprintf("%.2f MB", float64(bytes)/float64(MB))
	case bytes >= KB:
		return fmt.Sprintf("%.2f KB", float64(bytes)/float64(KB))
	default:
		return fmt.Sprintf("%d bytes", bytes)
	}
}

// GetMemoryUsageSummary returns a formatted summary of memory usage
func GetMemoryUsageSummary() string {
	stats := GetMemoryStats()
	
	return fmt.Sprintf(
		"Alloc: %s, TotalAlloc: %s, Sys: %s, GC Cycles: %d",
		FormatBytes(stats.Alloc),
		FormatBytes(stats.TotalAlloc),
		FormatBytes(stats.Sys),
		stats.NumGC,
	)
}
