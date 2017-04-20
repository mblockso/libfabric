#ifndef _FI_OPA1X_TIMER_H_
#define _FI_OPA1X_TIMER_H_

#include <time.h>

#ifndef TIMER_TYPE
#define TIMER_TYPE	(CLOCK_MONOTONIC_RAW)
#endif

#define FI_OPA1X_TIMER_CYCLE

#ifdef FI_OPA1X_TIMER_CYCLE
#include <string.h>
#endif

//#define TIMER(a) { struct timeval tp; gettimeofday(&tp, NULL); a=(double)tp.tv_sec+(1.e-6)*tp.tv_usec;}
//	double t0, t1;
//	TIMER(t0);
//
//	TIMER(t1);
//	double avg = (t1 -t0) / (double)ITERATIONS;
//	avg /= 2;


union fi_opa1x_timer_state {

#if defined(__x86_64__) || defined(__i386__)
#ifdef FI_OPA1X_TIMER_CYCLE
	struct {
		uint64_t accumulate;
		uint32_t picos_per_cycle;
		uint32_t unused;
	} __attribute__((__packed__)) cycle_timer;
#endif
#endif
	uint64_t something[2];
};


union fi_opa1x_timer_stamp {
	struct timespec	generic;
	struct {
		uint64_t cycles;
#if defined(__x86_64__) || defined(__i386__)
		uint32_t picos_per_cycle;
#endif
	} cycle_timer;
};


static inline void
fi_opa1x_timer_init (union fi_opa1x_timer_state * state) {

#if defined(__x86_64__) || defined(__i386__)
#ifdef FI_OPA1X_TIMER_CYCLE
	state->cycle_timer.accumulate = 0;
	state->cycle_timer.picos_per_cycle = 0;

	FILE *fp = fopen("/proc/cpuinfo", "r");
	char input[255];
	char *p = NULL;

	assert(fp);

	while (!feof(fp) && fgets(input, 255, fp)) {
		if (strstr(input, "cpu MHz")) {
			p = strchr(input, ':');
			double MHz = 0.0;
			if (p)
				MHz = atof(p + 1);
			state->cycle_timer.picos_per_cycle =
			    (uint32_t) (1000000. / MHz);
			break;
		}
	}
	fclose(fp);

	assert(p);
#endif
#endif

	return;
}


static inline uint64_t
fi_opa1x_timer_get_cycles () {
	uint64_t cycles;

#if defined(__x86_64__) || defined(__i386__)
	uint32_t a, d;
	asm volatile ("rdtsc" : "=a" (a), "=d"(d));
	cycles = ((uint64_t) a) | (((uint64_t) d) << 32);
#else
#error "Cycle timer not defined for this platform"
#endif

	return cycles;
}

static inline void
fi_opa1x_timer_now (union fi_opa1x_timer_stamp *now, union fi_opa1x_timer_state * state) {

#ifdef FI_OPA1X_TIMER_CYCLE
	now->cycle_timer.cycles = fi_opa1x_timer_get_cycles();
#else
	clock_gettime(TIMER_TYPE, (struct timespec *)now);
#endif
	return;
}

static inline uint64_t
fi_opa1x_timer_cycles_to_nanoseconds (const uint64_t cycles, union fi_opa1x_timer_state * state) {

	uint64_t nanoseconds = 0;

#if defined(__x86_64__) || defined(__i386__)
	nanoseconds = (state->cycle_timer.picos_per_cycle * cycles) / 1000ull;
#else
#error "Cycle timer not defined for this platform"
#endif

	return nanoseconds;
}



static inline double
fi_opa1x_timer_elapsed_usec (union fi_opa1x_timer_stamp *since, union fi_opa1x_timer_state * state) {

	double elapsed = 0.0;

#ifdef FI_OPA1X_TIMER_CYCLE
	const uint64_t now = fi_opa1x_timer_get_cycles();
	const uint64_t ns = fi_opa1x_timer_cycles_to_nanoseconds(now - since->cycle_timer.cycles, state);
	elapsed = (double)ns / 1000.0;

#else
	struct timespec now;
	clock_gettime(TIMER_TYPE, &now);

	uint64_t ns = 0;
	if ((now.tv_nsec - since->generic.tv_nsec) < 0) {

		ns += ((uint64_t)now.tv_nsec + 1000000000) - (uint64_t)since->generic.tv_nsec;
		--now.tv_sec;

	} else {

		ns += (uint64_t)now.tv_nsec - (uint64_t)since->generic.tv_nsec;
	}

	ns += ((uint64_t)now.tv_sec - (uint64_t)since->generic.tv_sec)
			* 1000*1000;	/* convert sec to usec */

	elapsed = (double)ns / 1000.0;

#error "Cycle timer not defined for this platform"
#endif

	return elapsed;
}

static inline double
fi_opa1x_timer_elapsed_ns (union fi_opa1x_timer_stamp *since, union fi_opa1x_timer_state * state) {

	double elapsed = 0.0;

#ifdef FI_OPA1X_TIMER_CYCLE
	const uint64_t now = fi_opa1x_timer_get_cycles();
	const uint64_t ns = fi_opa1x_timer_cycles_to_nanoseconds(now - since->cycle_timer.cycles, state);
	elapsed = (double)ns;

#else
	struct timespec now;
	clock_gettime(TIMER_TYPE, &now);

	uint64_t ns = 0;
	if ((now.tv_nsec - since->generic.tv_nsec) < 0) {

		ns += ((uint64_t)now.tv_nsec + 1000000000) - (uint64_t)since->generic.tv_nsec;
		--now.tv_sec;

	} else {

		ns += (uint64_t)now.tv_nsec - (uint64_t)since->generic.tv_nsec;
	}

	ns += ((uint64_t)now.tv_sec - (uint64_t)since->generic.tv_sec)
			* 1000*1000;	/* convert sec to usec */

	elapsed = (double)ns;

#error "Cycle timer not defined for this platform"
#endif

	return elapsed;
}


static inline void
fi_opa1x_timer_state_accumulate (union fi_opa1x_timer_stamp *since, union fi_opa1x_timer_state * state) {

#ifdef FI_OPA1X_TIMER_CYCLE

	const uint64_t now = fi_opa1x_timer_get_cycles();
	state->cycle_timer.accumulate += now - since->cycle_timer.cycles;

#else
#error fallback not implemented
#endif
	return;
}

static inline uint64_t
fi_opa1x_timer_state_total_nanoseconds (union fi_opa1x_timer_state * state) {

#ifdef FI_OPA1X_TIMER_CYCLE

	return fi_opa1x_timer_cycles_to_nanoseconds(state->cycle_timer.accumulate, state);

#else
#error fallback not implemented
#endif


}



#endif /* __FI_OPA1X_TIMER_H__ */
