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

void stats2_init(struct stats2 *s) {
	stats2_reset(s);
}

void stats_free(struct stats *s) {
	if ( s ) free(s->values);
}

void stats2_free(struct stats2 *s) {}

void stats_reset(struct stats *s) {
	s->sum = 0;
	s->sum_squares = 0;
	s->count = 0;
	s->dirty = 0;
}

void stats2_reset(struct stats2 *s) {
	memset(s, 0, sizeof(*s));
	s->min_x = s->min_y = INFINITY;
	s->max_x = s->max_y = -INFINITY;
}

void stats_insert(struct stats *s, double value) {
	if ( isnan(value) || isinf(value) )
		return;
	s->sum += value;
	s->sum_squares += value*value;
	if ( s->count == (long)s->values_alloc )
		stats_enlarge(s);
	s->values[s->count++] = value;
	s->dirty = 1;
}

void stats2_insert(struct stats2 *s, double x, double y) {
	if ( isnan(x) || isnan(y) || isinf(x) || isinf(y) )
		return;
	s->sum_x += x;
	s->sum_y += y;
	s->sum_xy += x*y;
	s->sum_squares_x += x*x;
	s->sum_squares_y += y*y;
	if ( s->min_x > x ) s->min_x = x;
	if ( s->min_y > y ) s->min_y = y;
	if ( s->max_x < x ) s->max_x = x;
	if ( s->max_y < y ) s->max_y = y;
	s->count++;
}

double stats_mean(struct stats *s) {
	return s->sum / s->count;
}

double stats2_mean_x(struct stats2 *s) {
	return s->sum_x / s->count;
}

double stats2_mean_y(struct stats2 *s) {
	return s->sum_y / s->count;
}

double stats_variance(struct stats *s) {
	const double mean = stats_mean(s);
	return s->sum_squares/s->count - mean*mean;
}

double stats_stddev(struct stats *s) {
	return sqrt(stats_variance(s));
}

double stats2_stddev_x(struct stats2 *s) {
	const double mean_x = stats2_mean_x(s);
	return sqrt(s->sum_squares_x / s->count - mean_x*mean_x);
}

double stats2_stddev_y(struct stats2 *s) {
	const double mean_y = stats2_mean_y(s);
	return sqrt(s->sum_squares_y / s->count - mean_y*mean_y);
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

struct histogram *stats_build_histogram(struct stats *s, double bucket_size, enum outlier_handling h) {
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

	// Throw values into histogram
	struct histogram *hist = histogram_create(bucket_size);
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

double stats_ideal_bucket_size(struct stats *s) {
	double max = fabs(stats_maximum(s));
	double min = fabs(stats_minimum(s));
	if ( max == min )
		max += max/1e6;
	return (max - min)/((int)sqrt(s->count));
}

void stats_merge(struct stats *cumulative, struct stats *another) {
	for ( int i=0; i<another->count; ++i ) {
		stats_insert(cumulative, another->values[i]);
	}
}

int stats2_linear_regression(struct stats2 *s, double *slope_dst, double *yint_dst) {
	if ( s->count < 2 || slope_dst == NULL || yint_dst == NULL )
		return 0;

	const double slope = stats2_linear_correlation(s)*(stats2_stddev_y(s)/stats2_stddev_x(s));
	if ( isnan(slope) || isinf(slope) )
		return 0;
	*slope_dst = slope;
	*yint_dst = s->sum_y/s->count - slope*s->sum_x/s->count;
	return 1;
}

// cov(x,y) = <(x - <x>)(y - <y>)> = <xy> - <x><y>
// Note: We may be overlooking numerical stability issues.
double stats2_covariance(struct stats2 *s) {
	return s->sum_xy/s->count - stats2_mean_x(s)*stats2_mean_y(s);
}

double stats2_linear_correlation(struct stats2 *s) {
	return stats2_covariance(s)/(stats2_stddev_x(s)*stats2_stddev_y(s));
}

//EOF
