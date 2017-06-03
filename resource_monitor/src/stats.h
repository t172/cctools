/*
Copyright (C) 2015- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef __STATS_H
#define __STATS_H

struct stats {
	double sum;
	double sum_squares;
	long count;
	double *values;
	size_t values_alloc;
	int dirty;
};

enum outlier_handling {
	STATS_KEEP_OUTLIERS, STATS_DISCARD_OUTLIERS
};

// Resets stats and initializes a values allocation
void stats_init(struct stats *s);

// Resets stats to zero
void stats_reset(struct stats *s);

// Processes a value (adds it to the calculations)
void stats_insert(struct stats *s, double value);

// Calculates the mean of the processed values
double stats_mean(struct stats *s);

// Calculates the standard deviation of the processed values
double stats_stddev(struct stats *s);

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
struct histogram *stats_build_histogram(struct stats *s, enum outlier_handling h);

#endif
//EOF
