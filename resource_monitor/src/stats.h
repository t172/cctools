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

// Resets stats and initializes a values allocation
void stats_init(struct stats *s);

// Resets stats to zero
void stats_reset(struct stats *s);

// Processes a value (adds it to the calculations)
void stats_process(struct stats *s, double value);

// Calculates the mean of the processed values
double stats_mean(struct stats *s);

// Calculates the standard deviation of the processed values
double stats_stddev(struct stats *s);

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

#endif
//EOF
