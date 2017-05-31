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
void stats_include(struct stats *s, double value);

// Calculates the mean of the values processed so far
double stats_mean(struct stats *s);

// Calculates the standard deviation of the values processed so far
double stats_stddev(struct stats *s);

/* double stats_median(struct stats *s); */

#endif
//EOF
