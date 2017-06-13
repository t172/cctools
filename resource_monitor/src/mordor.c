/*
Copyright (C) 2017- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <stdlib.h>
#include <stdio.h>

#include "mordor.h"
#include "list.h"
#include "debug.h"
#include "stringtools.h"
#include "xxmalloc.h"

#define OUTPUT_FIELD_SEPARATOR " "
#define OUTPUT_RECORD_SEPARATOR "\n"

// A single division/fraction.  This is the value of the key/value
// pairs of struct mordor's hash table.
struct mordor_mountain {
	struct stats stat;
	struct histogram *hist;
	int dirty;
};

static void mordor_build_histograms(struct mordor *m) {
	if ( !m->dirty )
		return;

	// Build cumulative histogram
	m->bucket_size = stats_ideal_bucket_size(&m->cumulative_stats);
	if ( m->cumulative_hist != NULL )
		histogram_delete(m->cumulative_hist);
	m->cumulative_hist = stats_build_histogram(&m->cumulative_stats, m->bucket_size, STATS_KEEP_OUTLIERS);

	// Build individual mountains, but only if needed
	struct mordor_mountain *mtn;
	hash_table_firstkey(m->table);
	for (char *key; hash_table_nextkey(m->table, &key, (void **)&mtn); ) {
		if ( !mtn->dirty && mtn->hist != NULL && histogram_bucket_size(mtn->hist) == m->bucket_size )
			continue;
		mtn->hist = stats_build_histogram(&mtn->stat, m->bucket_size, STATS_KEEP_OUTLIERS);
		mtn->dirty = 0;
	}
	m->dirty = 0;
}

void mordor_init(struct mordor *m) {
	m->table = hash_table_create(0, 0);
	stats_init(&m->cumulative_stats);

	// Calculate histogram when needed
	m->cumulative_hist = NULL;
	m->bucket_size = 0;
	m->dirty = 1;
}

void mordor_free(struct mordor *m) {
	// Free mordor_mountains (values of hash table)
	struct mordor_mountain *mtn;
	hash_table_firstkey(m->table);
	for ( char *key; hash_table_nextkey(m->table, &key, (void **)&mtn); ) {
		if ( mtn == NULL )
			continue;
		stats_free(&mtn->stat);
		if ( mtn->hist != NULL )
			histogram_delete(mtn->hist);
		free(mtn);
	}
	hash_table_delete(m->table);

	// Free cumulative
	stats_free(&m->cumulative_stats);
	if ( m->cumulative_hist != NULL ) {
		histogram_delete(m->cumulative_hist);
	}
}

void mordor_insert(struct mordor *m, char *key, double value) {
	// Find the right mountain
	struct mordor_mountain *mtn;
	if ( (mtn = hash_table_lookup(m->table, key)) == NULL ) {
		mtn = xxmalloc(sizeof(*mtn));
		stats_init(&mtn->stat);
		mtn->hist = NULL;
		m->dirty = 1;
		hash_table_insert(m->table, key, mtn);
	}

	// Insert value into the mountain
	stats_insert(&mtn->stat, value);
	mtn->dirty = 1;

	// Insert value into cumulative (double storing for now...)
	stats_insert(&m->cumulative_stats, value);
	m->dirty = 1;
}

struct keyvalue_pair {
	char *key;
	struct mordor_mountain *value;
};

static int qsort_by_mean(const void *a, const void *b) {
	const double A = stats_mean(&((struct keyvalue_pair *)a)->value->stat);
	const double B = stats_mean(&((struct keyvalue_pair *)b)->value->stat);
	return (A > B) - (A < B);
}

void mordor_write_datafile(struct mordor *m, FILE *out) {
	mordor_build_histograms(m);

	// Sorting the mountains by the mean of the distribution often gives
	// some visual continuity to the result.
	int num_mountains = hash_table_size(m->table);
	struct keyvalue_pair *pairs = xxmalloc(num_mountains*sizeof(*pairs));
	char *key;
	struct mordor_mountain *value;
	hash_table_firstkey(m->table); 
	for ( int i=0; hash_table_nextkey(m->table, &key, (void **)&value); ++i ) {
		pairs[i].key = key;
		pairs[i].value = value;
	}
	qsort(pairs, num_mountains, sizeof(*pairs), qsort_by_mean);

	// First value is the bucket size, followed by text headers
	fprintf(out, "%g" OUTPUT_FIELD_SEPARATOR "(all)", m->bucket_size);
	for ( int column=0; column < num_mountains; ++column )
		fprintf(out, OUTPUT_FIELD_SEPARATOR "%s", pairs[column].key);
	fprintf(out, OUTPUT_RECORD_SEPARATOR);

	// A common bucket size is used for all histograms, and the
	// cumulative histogram must necessarily have a bucket wherever an
	// individual mountain's histogram does.  The first column is the
	// start of the bucket's range, each subsequent column is the
	// frequency for each mountain.
	double *cumulative_buckets = histogram_buckets(m->cumulative_hist);

	// Padding each side with a line of zeros helps the plotter
	fprintf(out, "%g" OUTPUT_FIELD_SEPARATOR "0", cumulative_buckets[0] - m->bucket_size);
	for ( int column=0; column < num_mountains; ++column )
		fprintf(out, OUTPUT_FIELD_SEPARATOR "0");
	fprintf(out, OUTPUT_RECORD_SEPARATOR);

	// Iterate through every bucket
	int num_buckets = histogram_size(m->cumulative_hist);
	for ( int row=0; row < num_buckets; ++row ) {
		const double bucket_start = cumulative_buckets[row];
		fprintf(out, "%g" OUTPUT_FIELD_SEPARATOR "%d", bucket_start, histogram_count(m->cumulative_hist, bucket_start));
		for ( int column=0; column < num_mountains; ++column )
			fprintf(out, OUTPUT_FIELD_SEPARATOR "%d", histogram_count(pairs[column].value->hist, bucket_start));
		fprintf(out, OUTPUT_RECORD_SEPARATOR);
	}

	// A last line of zeros
	fprintf(out, "%g" OUTPUT_FIELD_SEPARATOR "0", cumulative_buckets[num_buckets-1] + m->bucket_size);
	for ( int column=0; column < num_mountains; ++column )
		fprintf(out, OUTPUT_FIELD_SEPARATOR "0");
	fprintf(out, OUTPUT_RECORD_SEPARATOR);

	// Clean up
	free(cumulative_buckets);
	free(pairs);
}

void mordor_write_gnuplot(struct mordor *m, FILE *out, const char *data_name) {
}

//EOF
