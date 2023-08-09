package util

import (
	"time"
)

var (
	// shamelessly copied from svix
	// https://docs.svix.com/retries#the-schedule
	backoff = map[int64]time.Duration{
		0: 0,
		1: 05 * time.Second,
		2: 30 * time.Second,
		3: 02 * time.Hour,
		4: 05 * time.Hour,
		5: 10 * time.Hour,
		6: 10 * time.Hour,
	}
)

func Assert(cond bool, msg string) {
	if !cond {
		panic(msg)
	}
}

func Backoff(attempt int64) (time.Duration, bool) {
	if duration, ok := backoff[attempt]; ok {
		return duration, true
	}

	return 0, false
}
