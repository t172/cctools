/*
Copyright (C) 2015- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <math.h>

#include "stats.h"
#include "xxmalloc.h"

// Initial size of values buffer in struct stats (doubles as needed)
#define STATS_VALUES_INITSIZE (4096/sizeof(double))

// Doubles the values buffer size
static void stats_enlarge(struct stats *s) {
	s->values_alloc *= 2;
	s->values = xxrealloc(s->values, s->values_alloc*sizeof(*s->values));
}

// Comparator function for sorting
static int stats_values_cmp(const void *a, const void *b) {
	const double *A = a, *B = b;
	return (*A > *B) - (*A < *B);
}

// Sorts the values in the stats
static void stats_sort(struct stats *s) {
	qsort(s->values, s->count, sizeof(*s->values), stats_values_cmp);
	s->dirty = 0;
}

// Resets stats and initializes a values allocation
void stats_init(struct stats *s) {
	stats_reset(s);
	s->values_alloc = STATS_VALUES_INITSIZE;
	s->values = xxmalloc(s->values_alloc*sizeof(*s->values));
}

// Resets stats to zero
void stats_reset(struct stats *s) {
	s->sum = 0;
	s->sum_squares = 0;
	s->count = 0;
	s->dirty = 0;
}

void stats_process(struct stats *s, double value) {
	s->sum += value;
	s->sum_squares += value*value;
	if ( s->count == (long)s->values_alloc )
		stats_enlarge(s);
	s->values[s->count++] = value;
	s->dirty = 1;
}

double stats_mean(struct stats *s) {
	return s->sum / s->count;
}

double stats_stddev(struct stats *s) {
	double mean = stats_mean(s);
	return sqrt(s->sum_squares / s->count - mean*mean);
}

static double stats_middle_between(struct stats *s, int start, int end) {
	if ( s->dirty ) {
		stats_sort(s);
	}
	int delta = end - start;
	if ( delta == 0 ) {
		// No values, no median
		return NAN;
	}
	if ( delta % 2 == 1 ) {
		// Use the middle value if there is an odd number of values
		return s->values[start + delta/2];
	} else {
		// Average the two middle values if there is an even number of values
		return (s->values[start + delta/2 - 1] + s->values[start + delta/2])/2.0;
	}
}

double stats_median(struct stats *s) {
	return stats_middle_between(s, 0, s->count);
}

double stats_Q1(struct stats *s) {
	if ( s->count == 1 ) {
		return s->values[0];
	}
	return stats_middle_between(s, 0, s->count/2);
}

double stats_Q3(struct stats *s) {
	if ( s->count == 1 ) {
		return s->values[0];
	}
	if ( s->count % 2 == 0 ) {
		return stats_middle_between(s, s->count/2, s->count);
	} else {
		return stats_middle_between(s, s->count/2 + 1, s->count);
	}
}

