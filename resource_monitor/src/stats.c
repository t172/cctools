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

/* static int stats_values_cmp(const void *a, const void *b) { */
/* } */

/* static void stats_sort(struct stats *s) { */
/* } */

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

void stats_include(struct stats *s, double value) {
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

/* double stats_median(struct stats *s) { */
/* } */

