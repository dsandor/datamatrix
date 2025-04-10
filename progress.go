package main

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

// ProgressTracker manages progress tracking and status indicators
type ProgressTracker struct {
	sync.RWMutex
	logger        *Logger
	status        string
	progressBar   string
	percentage    int
	total         int
	current       int
	lastUpdate    time.Time
	isIdle        bool
	idleStartTime time.Time
	idleTimer     *time.Timer
}

// NewProgressTracker creates a new progress tracker
func NewProgressTracker(logger *Logger) *ProgressTracker {
	pt := &ProgressTracker{
		logger:     logger,
		status:     "Initializing",
		percentage: 0,
		isIdle:     false,
		lastUpdate: time.Now(),
	}
	
	// Start the idle timer
	pt.resetIdleTimer()
	
	return pt
}

// resetIdleTimer resets the idle timer
func (pt *ProgressTracker) resetIdleTimer() {
	// Cancel existing timer if any
	pt.Lock()
	if pt.idleTimer != nil {
		pt.idleTimer.Stop()
	}
	pt.Unlock()
	
	// Set a new timer for 30 seconds
	// Increased from 5 to 30 seconds to prevent premature idle state during processing
	pt.idleTimer = time.AfterFunc(30*time.Second, func() {
		// Use a separate function to avoid deadlock
		pt.setIdleSafe(true)
	})
}

// setIdleSafe sets the idle status safely from a timer goroutine
func (pt *ProgressTracker) setIdleSafe(idle bool) {
	pt.Lock()
	defer pt.Unlock()
	
	if idle && !pt.isIdle {
		// Transitioning to idle
		pt.isIdle = true
		pt.idleStartTime = time.Now()
		pt.status = "Idle"
		pt.logger.Info("System is now idle")
	}
}

// setIdle sets the idle status
func (pt *ProgressTracker) setIdle(idle bool) {
	pt.Lock()
	defer pt.Unlock()
	
	if idle && !pt.isIdle {
		// Transitioning to idle
		pt.isIdle = true
		pt.idleStartTime = time.Now()
		pt.status = "Idle"
		pt.logger.Info("System is now idle")
	} else if !idle && pt.isIdle {
		// Transitioning from idle
		pt.isIdle = false
		pt.logger.Info("System is no longer idle (was idle for %s)", time.Since(pt.idleStartTime).Round(time.Second))
	}
}

// StartProgress starts tracking progress for a new operation
func (pt *ProgressTracker) StartProgress(operation string, total int) {
	pt.Lock()
	
	// Update idle status directly without calling setIdle
	if pt.isIdle {
		pt.isIdle = false
		pt.logger.Info("System is no longer idle (was idle for %s)", time.Since(pt.idleStartTime).Round(time.Second))
	}
	
	pt.status = operation
	pt.total = total
	pt.current = 0
	pt.percentage = 0
	pt.lastUpdate = time.Now()
	
	if total > 0 {
		pt.updateProgressBar()
		pt.logger.Info("%s: Starting... (0/%d) 0%%", operation, total)
	} else {
		pt.logger.Info("%s: Starting...", operation)
	}
	pt.Unlock()
	
	// Reset the idle timer after releasing the lock
	pt.resetIdleTimer()
}

// UpdateProgress updates the current progress
func (pt *ProgressTracker) UpdateProgress(current int, status string) {
	pt.Lock()
	
	// Update idle status directly without calling setIdle
	if pt.isIdle {
		pt.isIdle = false
		pt.logger.Info("System is no longer idle (was idle for %s)", time.Since(pt.idleStartTime).Round(time.Second))
	}
	
	pt.current = current
	if status != "" {
		pt.status = status
	}
	
	// Calculate percentage
	if pt.total > 0 {
		pt.percentage = (current * 100) / pt.total
	}
	
	// Only update the display if it's been at least 500ms since the last update
	// or if we've reached 100%
	if time.Since(pt.lastUpdate) >= 500*time.Millisecond || pt.percentage >= 100 {
		pt.updateProgressBar()
		
		if pt.total > 0 {
			pt.logger.Info("%s: Progress (%d/%d) %d%%", pt.status, current, pt.total, pt.percentage)
		} else {
			pt.logger.Info("%s: Progress (%d items processed)", pt.status, current)
		}
		
		pt.lastUpdate = time.Now()
	}
	pt.Unlock()
	
	// Reset the idle timer after releasing the lock
	pt.resetIdleTimer()
}

// IncrementProgress increments the progress by 1
func (pt *ProgressTracker) IncrementProgress(status string) {
	pt.UpdateProgress(pt.current+1, status)
}

// CompleteProgress marks the progress as complete
func (pt *ProgressTracker) CompleteProgress(completionMessage ...string) {
	pt.Lock()
	
	// Update idle status directly without calling setIdle
	if pt.isIdle {
		pt.isIdle = false
		pt.logger.Info("System is no longer idle (was idle for %s)", time.Since(pt.idleStartTime).Round(time.Second))
	}
	
	// Use the provided completion message if available
	if len(completionMessage) > 0 && completionMessage[0] != "" {
		pt.status = completionMessage[0]
	}
	
	if pt.total > 0 {
		pt.logger.Success("%s: Complete (%d/%d) 100%%", pt.status, pt.total, pt.total)
	} else {
		pt.logger.Success("%s: Complete (%d items processed)", pt.status, pt.current)
	}
	pt.Unlock()
	
	// Reset the idle timer after releasing the lock
	pt.resetIdleTimer()
}

// SetStatus sets the current status without updating progress
func (pt *ProgressTracker) SetStatus(status string) {
	pt.Lock()
	
	// Update idle status directly without calling setIdle
	if pt.isIdle {
		pt.isIdle = false
		pt.logger.Info("System is no longer idle (was idle for %s)", time.Since(pt.idleStartTime).Round(time.Second))
	}
	
	pt.status = status
	pt.logger.Info("Status: %s", status)
	pt.Unlock()
	
	// Reset the idle timer after releasing the lock
	pt.resetIdleTimer()
}

// updateProgressBar updates the progress bar string
func (pt *ProgressTracker) updateProgressBar() {
	width := 30 // Width of the progress bar
	
	if pt.total <= 0 {
		pt.progressBar = "[" + strings.Repeat("=", width) + "]"
		return
	}
	
	// Calculate the number of "=" characters
	numEquals := (pt.current * width) / pt.total
	if numEquals > width {
		numEquals = width
	}
	
	// Calculate the number of " " characters
	numSpaces := width - numEquals
	
	// Create the progress bar
	pt.progressBar = "[" + strings.Repeat("=", numEquals) + strings.Repeat(" ", numSpaces) + "]"
}

// GetProgressString returns the current progress as a string
func (pt *ProgressTracker) GetProgressString() string {
	pt.RLock()
	defer pt.RUnlock()
	
	if pt.isIdle {
		idleTime := time.Since(pt.idleStartTime).Round(time.Second)
		return fmt.Sprintf("Status: %s (for %s)", pt.status, idleTime)
	}
	
	if pt.total > 0 {
		return fmt.Sprintf("%s %s (%d/%d) %d%%", pt.progressBar, pt.status, pt.current, pt.total, pt.percentage)
	}
	
	return fmt.Sprintf("Status: %s (%d items processed)", pt.status, pt.current)
}
