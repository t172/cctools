/*
Copyright (C) 2017- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef __MORDOR_H
#define __MORDOR_H

#include <stdio.h>

#include "hash_table.h"
#include "histogram.h"
#include "stats.h"

// I think they look like mountains. Kevin says they look like Mordor.
// They are histograms inspired by famous pulsar plots from 1970.
struct mordor {
	// Hash table mapping a (string) key to a mordor_mountain
	struct hash_table *table;

	// Cumulative stats of all values.
	struct stats cumulative_stats;

	// Cumulative histogram of all values
	struct histogram *cumulative_hist;

	// The bucket size of a histogram must be determined before building
	// the histogram.
	double bucket_size;

	// Histograms have to be regenerated after values have been
	// inserted.  The dirty flag indicates that this needs to be done
	// (otherwise we can spare the computation).
	int dirty;
};

// Initializes the plot
void mordor_init(struct mordor *m);

// Frees resources
void mordor_free(struct mordor *m);

// Inserts a key-value pair into the plot
void mordor_insert(struct mordor *m, char *key, double value);

// Writes the data file to be used by mordor_write_gnuplot().
void mordor_write_datafile(struct mordor *m, FILE *out);

// Writes a gnuplot script to plot the histogram.  The data_name is
// the filename of the data written by mordor_write_data().
void mordor_write_gnuplot(struct mordor *m, FILE *out, const char *data_name);

#endif
//EOF
