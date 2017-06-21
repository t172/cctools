/*
Copyright (C) 2015- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef __STATS_H
#define __STATS_H

#include "histogram.h"

struct stats {
	double sum;
	double sum_squares;
	long count;
	double *values;
	size_t values_alloc;
	int dirty;
};

// Two-dimensional stats, for regression, correlation, etc.
struct stats2 {
	double sum_x, sum_y;
	double sum_xy;
	double sum_squares_x, sum_squares_y;
	double min_x, min_y;
	double max_x, max_y;
	long count;
};

enum outlier_handling {
	STATS_KEEP_OUTLIERS, STATS_DISCARD_OUTLIERS
};

// Resets stats and initializes a values allocation
void stats_init(struct stats *s);
void stats2_init(struct stats2 *s);

// Frees resources used by a stats object
void stats_free(struct stats *s);
void stats2_free(struct stats2 *s);

// Frees resources used by a stats object
void stats_free(struct stats *s);

// Resets stats to zero
void stats_reset(struct stats *s);
void stats2_reset(struct stats2 *s);

// Processes a value (adds it to the calculations)
void stats_insert(struct stats *s, double value);
void stats2_insert(struct stats2 *s, double x, double y);

// Calculates the mean of the processed values
double stats_mean(struct stats *s);
double stats2_mean_x(struct stats2 *s);
double stats2_mean_y(struct stats2 *s);

double stats_variance(struct stats *s);

// Calculates the standard deviation of the processed values
double stats_stddev(struct stats *s);
double stats2_stddev_x(struct stats2 *s);
double stats2_stddev_y(struct stats2 *s);

// Returns the lowest of the processed values
double stats_minimum(struct stats *s);

// Returns the highest of the processed values
double stats_maximum(struct stats *s);

// Calculates the median (Q2) of the processed values
double stats_median(struct stats *s);

// Calculates Q1 (lower quartile) of the processed values
double stats_Q1(struct stats *s);

// Calculates Q3 (upper quartile) of the processed values
double stats_Q3(struct stats *s);

// Calculates the low whisker position, the lowest value within 1.5
// times the inter-quartile range (IQR) of Q1.
double stats_whisker_low(struct stats *s);

// Calculates the high whisker position, the highest value within 1.5
// times the inter-quartile range (IQR) of Q3.
double stats_whisker_high(struct stats *s);

// Calculates a histogram with the given values and returns a pointer
// to it, which must be deleted with histogram_delete().  Outliers
// (<whisker_low and >whisker_high) will be discarded if
// discard_outliers is non-zero.
struct histogram *stats_build_histogram(struct stats *s, double bucket_size, enum outlier_handling h);

// Traditionally, we want sqrt(n) buckets for n samples.  This
// calculates the bucket size so that there are sqrt(n) buckets for
// the range of the given values.
double stats_ideal_bucket_size(struct stats *s);

// Merges another stats object into cumulative
void stats_merge(struct stats *cumulative, struct stats *another);

// Fits the line y = a*x + b to the data, returning non-zero if the
// slope a was written to slope_dst and y-intercept b was written to
// yint_dst.  A return of zero indicates an error (such as <2 points).
int stats2_linear_regression(struct stats2 *s, double *slope_dst, double *yint_dst);

// Covariance of x- and y-values.
double stats2_covariance(struct stats2 *s);

// Linear correlation
double stats2_linear_correlation(struct stats2 *s);

#endif
//EOF
