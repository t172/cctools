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
#include "histogram.h"

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

// Sorts the values in the stats.  The dirty flag indicates that
// something has been written to the values since the last time they
// were sorted.  This prevents unnecessary sorting when the dirty flag
// is not set.
static void stats_sort(struct stats *s) {
	if ( s->dirty ) {
		qsort(s->values, s->count, sizeof(*s->values), stats_values_cmp);
		s->dirty = 0;
	}
}

void stats_init(struct stats *s) {
	stats_reset(s);
	s->values_alloc = STATS_VALUES_INITSIZE;
	s->values = xxmalloc(s->values_alloc*sizeof(*s->values));
}

void stats_reset(struct stats *s) {
	s->sum = 0;
	s->sum_squares = 0;
	s->count = 0;
	s->dirty = 0;
}

void stats_insert(struct stats *s, double value) {
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

double stats_minimum(struct stats *s) {
	if ( s->count == 0 )
		return NAN;
	stats_sort(s);
	return s->values[0];
}

double stats_maximum(struct stats *s) {
	if ( s->count == 0 )
		return NAN;
	stats_sort(s);
	return s->values[s->count - 1];
}

static double stats_middle_between(struct stats *s, int start, int end) {
	stats_sort(s);

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

double stats_whisker_low(struct stats *s) {
	if ( s->count == 0 )
		return NAN;

	const double q1 = stats_Q1(s);
	const double threshold = q1 - 1.5*(stats_Q3(s) - q1);

	double lowest;
	for ( int i=0; i < s->count; ++i) {
		lowest = s->values[i];
		if ( lowest >= threshold )
			break;
	}
	return lowest;
}

double stats_whisker_high(struct stats *s) {
	if ( s->count == 0 )
		return NAN;

	const double q3 = stats_Q3(s);
	const double threshold = q3 + 1.5*(q3 - stats_Q1(s));

	double highest;
	for ( int i=s->count-1; i >= 0; --i) {
		highest = s->values[i];
		if ( highest <= threshold )
			break;
	}
	return highest;
}

struct histogram *stats_build_histogram(struct stats *s, enum outlier_handling h) {
	if ( s->count == 0 )
		return NULL;

	// Determine full range of values to use
	double low, high;
	if ( h == STATS_DISCARD_OUTLIERS ) {
		low = stats_whisker_low(s);
		high = stats_whisker_high(s);
	} else { // keep outliers
		low = stats_minimum(s);
		high = stats_maximum(s);
	}

	// Zero range means all values are the same
	if ( high == low ) {
		// Make a (hopefully) negligible but non-zero range
		high += high/1e6;
	}

	// Traditionally we want sqrt(n) buckets for n values
	double bucket_size = fabs((high - low)/((int)sqrt(s->count)));
	struct histogram *hist = histogram_create(bucket_size);

	// Throw values into histogram
	if ( h == STATS_DISCARD_OUTLIERS ) {
		double value;
		for ( int i=0; i < s->count; ++i ) {
			value = s->values[i];
			if ( value >= low && value <= high )
				histogram_insert(hist, value);
		}
	} else { // keep outliers
		for ( int i=0; i < s->count; ++i ) {
			histogram_insert(hist, s->values[i]);
		}
	}
	return hist;
}

//EOF
