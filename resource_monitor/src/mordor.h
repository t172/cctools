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

// Plotting styles
enum mordor_style {
	MORDOR_STYLE_CLASSIC, MORDOR_STYLE_CLEAN
};

// How to sort the mountains
enum mordor_sortby {
	MORDOR_SORTBY_NONE, MORDOR_SORTBY_MEAN, MORDOR_SORTBY_KEY
};

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

	// Plotting style (affects data file and gnuplot script output)
	enum mordor_style style;

	// Sorting style (sorts the data file, which affects how it appears
	// on the plot)
	enum mordor_sortby sortby;

	// (Optional) Strings for plotting
	char *title;
	char *xlabel;
	char *ylabel;
};

// Creates a Mordor plot.  The returned pointer must be freed with
// mordor_delete().
struct mordor *mordor_create(void);

// Frees resources
void mordor_delete(struct mordor *m);

// Inserts a key-value pair into the plot
void mordor_insert(struct mordor *m, char *key, double value);

// Writes the formatted data and gnuplot script.  The script will
// generate a file named pngfile.  The datafile is the name of the
// data file relative to the gnuplot script.
void mordor_plot(struct mordor *m, const char *pngfile, FILE *data, FILE *gnuplot, const char *datafile);

#endif
//EOF
