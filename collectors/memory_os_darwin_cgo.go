//go:build darwin && cgo

package collectors

/*
#include <mach/mach.h>
#include <stdint.h>

static int statekit_current_rss(uint64_t *rss) {
	mach_task_basic_info_data_t info;
	mach_msg_type_number_t count = MACH_TASK_BASIC_INFO_COUNT;
	kern_return_t kr = task_info(mach_task_self(), MACH_TASK_BASIC_INFO, (task_info_t)&info, &count);
	if (kr != KERN_SUCCESS) {
		return 0;
	}
	*rss = (uint64_t)info.resident_size;
	return 1;
}
*/
import "C"

import "syscall"

func currentRSSBytes() (uint64, bool) {
	var rss C.uint64_t
	if C.statekit_current_rss(&rss) == 0 {
		return 0, false
	}
	return uint64(rss), true
}

func peakRSSBytes() (uint64, bool) {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		return 0, false
	}
	if usage.Maxrss <= 0 {
		return 0, false
	}
	return uint64(usage.Maxrss), true
}
