/*
Copyright (C) 2017- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "mordor.h"
#include "list.h"
#include "debug.h"
#include "stringtools.h"
#include "xxmalloc.h"

#define OUTPUT_FIELD_SEPARATOR " "
#define OUTPUT_RECORD_SEPARATOR "\n"

#define DEFAULT_MORDOR_STYLE MORDOR_STYLE_CLEAN
#define DEFAULT_MORDOR_SORTBY MORDOR_SORTBY_MEAN

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

struct mordor *mordor_create(void) {
	struct mordor *m = xxmalloc(sizeof(*m));
	stats_init(&m->cumulative_stats);
	m->table = hash_table_create(0, 0);
	m->style = DEFAULT_MORDOR_STYLE;
	m->sortby = DEFAULT_MORDOR_SORTBY;
	m->title = NULL;
	m->xlabel = NULL;
	m->ylabel = NULL;

	// Calculate histogram when needed
	m->cumulative_hist = NULL;
	m->bucket_size = 0;
	m->dirty = 1;
	return m;
}

void mordor_delete(struct mordor *m) {
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

	// Free cumulatives
	stats_free(&m->cumulative_stats);
	if ( m->cumulative_hist != NULL ) {
		histogram_delete(m->cumulative_hist);
	}
	free(m);
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

static int qsort_by_key(const void *a, const void *b) {
	return strcmp(((struct keyvalue_pair *)a)->key, ((struct keyvalue_pair *)b)->key);
}

static struct keyvalue_pair *create_sorted_keys(struct mordor *m) {
	int num_mountains = hash_table_size(m->table);
	struct keyvalue_pair *pairs = xxmalloc(num_mountains*sizeof(*pairs));

	// Populate array
	char *key;
	struct mordor_mountain *value;
	hash_table_firstkey(m->table); 
	for ( int i=0; hash_table_nextkey(m->table, &key, (void **)&value); ++i ) {
		pairs[i].key = key;
		pairs[i].value = value;
	}

	// Sort it
	switch ( m->sortby ) {
	case MORDOR_SORTBY_MEAN:
		qsort(pairs, num_mountains, sizeof(*pairs), qsort_by_mean);
		break;
	case MORDOR_SORTBY_KEY:
		qsort(pairs, num_mountains, sizeof(*pairs), qsort_by_key);
		break;
	case MORDOR_SORTBY_NONE: 
	default:
		break;
	}
	return pairs;
}

static void mordor_datafile_classic(struct mordor *m, struct keyvalue_pair *sorted, FILE *out) {
	int num_mountains = hash_table_size(m->table);

	// First value is the bucket size, followed by text headers
	fprintf(out, "%g" OUTPUT_FIELD_SEPARATOR "(all)", m->bucket_size);
	for ( int column=0; column < num_mountains; ++column )
		fprintf(out, OUTPUT_FIELD_SEPARATOR "%s", sorted[column].key);
	fprintf(out, OUTPUT_RECORD_SEPARATOR);

	// A common bucket size is used for all histograms, and the
	// cumulative histogram necessarily has a bucket wherever an
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
			fprintf(out, OUTPUT_FIELD_SEPARATOR "%d", histogram_count(sorted[column].value->hist, bucket_start));
		fprintf(out, OUTPUT_RECORD_SEPARATOR);
	}

	// A last line of zeros to help the plotter
	fprintf(out, "%g" OUTPUT_FIELD_SEPARATOR "0", cumulative_buckets[num_buckets-1] + m->bucket_size);
	for ( int column=0; column < num_mountains; ++column )
		fprintf(out, OUTPUT_FIELD_SEPARATOR "0");
	fprintf(out, OUTPUT_RECORD_SEPARATOR);

	// Clean up
	free(cumulative_buckets);
}

static void mordor_datafile_clean(struct mordor *m, struct keyvalue_pair *sorted, FILE *out) {
	int num_mountains = hash_table_size(m->table);
	int num_buckets = histogram_size(m->cumulative_hist);
	double *cumulative_buckets = histogram_buckets(m->cumulative_hist);

	// First value is the bucket size, followed by text headers
	fprintf(out, "%g" OUTPUT_FIELD_SEPARATOR "(all)", m->bucket_size);
	for ( int column=0; column < num_mountains; ++column )
		fprintf(out, OUTPUT_FIELD_SEPARATOR "%s", sorted[column].key);
	fprintf(out, OUTPUT_RECORD_SEPARATOR);

	// Arrays of first and last buckets' start for each mountain
	double *start = xxmalloc(num_mountains*sizeof(*start));
	double *finish = xxmalloc(num_mountains*sizeof(*finish));
	for ( int i=0; i < num_mountains; ++i ) {
		const int size = histogram_size(sorted[i].value->hist);
		if ( size == 0 ) {
			start[i] = 0;
			finish[i] = 0;
		} else {
			double *buckets = histogram_buckets(sorted[i].value->hist);
			start[i] = buckets[0];
			finish[i] = buckets[size - 1];
			free(buckets);
		}
	}

	// Each mountain switches on, then switches off
	enum {
		STATE_NOT_STARTED=0, STATE_STARTED, STATE_FINISHED
	} *state = xxcalloc(num_mountains, sizeof(*state));

	// Padding beginning and end with a line of zeros helps the plotter
	fprintf(out, "%f" OUTPUT_FIELD_SEPARATOR "0", cumulative_buckets[0] - m->bucket_size);
	for ( int mtn=0; mtn < num_mountains; ++mtn ) {
		// If first bucket starts the mountain, write a zero
		if ( cumulative_buckets[0] + 0.5*m->bucket_size >= start[mtn] ) {
			state[mtn] = STATE_STARTED;
			fprintf(out, OUTPUT_FIELD_SEPARATOR "0");
		} else {
			fprintf(out, OUTPUT_FIELD_SEPARATOR "NAN");
		}
	}
	fprintf(out, OUTPUT_RECORD_SEPARATOR);

	for ( int bucket = 0; bucket < num_buckets; ++bucket ) {
		double pos = cumulative_buckets[bucket];
	write_row:
		fprintf(out, "%f" OUTPUT_FIELD_SEPARATOR "%d", pos, histogram_count(m->cumulative_hist, pos));
		for ( int mtn=0; mtn < num_mountains; ++mtn ) {
			switch ( state[mtn] ) {
			case STATE_FINISHED:
				if ( pos - 1.5*m->bucket_size < finish[mtn] ) {
					fprintf(out, OUTPUT_FIELD_SEPARATOR "0");
				} else {
					fprintf(out, OUTPUT_FIELD_SEPARATOR "NAN");
				}
				continue;
			case STATE_NOT_STARTED:
				if ( pos + 1.5*m->bucket_size < start[mtn] ) {
					fprintf(out, OUTPUT_FIELD_SEPARATOR "NAN");
					continue;
				}
				state[mtn] = STATE_STARTED;
				// (fall through)
			case STATE_STARTED:
			default:
				if ( pos > finish[mtn] )
					state[mtn] = STATE_FINISHED;
				break;
			}
			fprintf(out, OUTPUT_FIELD_SEPARATOR "%d", histogram_count(sorted[mtn].value->hist, pos));
		}
		fprintf(out, OUTPUT_RECORD_SEPARATOR);

		// Histogram only records non-zero buckets, sometimes we need to insert rows to write zeros
		if ( bucket < num_buckets - 1 && pos + 1.5*m->bucket_size < cumulative_buckets[bucket + 1] ) {
			pos += m->bucket_size;
			goto write_row;
		}
	}

	// A last line of zeros to help the plotter
	fprintf(out, "%f" OUTPUT_FIELD_SEPARATOR "0", cumulative_buckets[num_buckets - 1] + m->bucket_size);
	for ( int mtn=0; mtn < num_mountains; ++mtn ) {
		if ( state[mtn] == STATE_STARTED ) {
			fprintf(out, OUTPUT_FIELD_SEPARATOR "0");
		} else {
			fprintf(out, OUTPUT_FIELD_SEPARATOR "NAN");
		}
	}
	fprintf(out, OUTPUT_RECORD_SEPARATOR);

	// Clean up
	free(state);
	free(finish);
	free(start);
	free(cumulative_buckets);
}

static void mordor_plotscript_classic(struct mordor *m, struct keyvalue_pair *sorted, FILE *out, const char *data_name, const char *pngfile) {
	fprintf(out,
	        "set terminal pngcairo enhanced size 1280,2048\n"
	        "set key off\n"
	        "set style fill transparent solid 0.9 border lc rgb 'black'\n"
	        "set lmargin at screen 0.18\n"
	        "set xtics font ',20'\n"
	        "set style line 1 lc rgb 'black'\n"
	        "set style line 2 lc rgb 'grey90'\n"
	        "set output '%s'\n"
	        "set multiplot layout 2,1", pngfile);
	if ( m->title == NULL ) {
		fprintf(out, "\n");
	} else {
		fprintf(out, " title '%s' font ',22'\n", m->title);
	}

	// Upper plot: Cumulative histogram
	fprintf(out,
	        "set grid xtics\n"
	        "set size 1,0.3\n"
	        "set origin 0,0.7\n"
	        "set bmargin 0\n"
	        "set tmargin 2\n"
	        "unset xlabel\n"
	        "set format x ''\n"
	        "unset ytics\n"
	        "set yrange [0:]\n");
	if ( m->ylabel != NULL ) {
		fprintf(out, "set ylabel '%s' font ',20'\n", m->ylabel);
	}
	fprintf(out, "plot '%s' using 1:2 with filledcurves ls 1 notitle\n\n", data_name);

	// Lower plot: Individual mountains
	fprintf(out,
	        "unset grid\n"
	        "set size 1,0.7\n"
	        "set origin 0,0\n"
	        "set bmargin 3.5\n"
	        "set tmargin 0\n"
	        "set format x '%%g'\n"
	        "unset ylabel\n"
	        "set format y ''\n");
	if ( m->xlabel != NULL ) {
		fprintf(out, "set xlabel '%s' font ',20'\n", m->xlabel);
	}

	const double yscale = 1.0;  // scale factor for height of mountains
	const double vspread = 1.5;  // separation between mountains
	const int num_mountains = hash_table_size(m->table);

	// Custom ytics labels from keys
	int yskip = 1;
	fprintf(out, "set ytics add (");
	const char *sep = "";
	for ( int i=0; i < num_mountains; i += yskip ) {
		fprintf(out, "%s \"%s\" %g", sep, sorted[i].key, -vspread*i);
		sep = ",";
	}
	fprintf(out, " ) font ',12'\n");
	fprintf(out, "set yrange [%g:]\n", -vspread*num_mountains);
	fprintf(out, "plot for [i=1:%d] '%s' using 1:(%g*column(i+3) - %g*i) with filledcurves ls 2 title columnhead(i+3)\n\n",
					num_mountains, data_name, yscale, vspread);
	fprintf(out, "unset multiplot\n");
}

static void mordor_plotscript_clean(struct mordor *m, struct keyvalue_pair *sorted, FILE *out, const char *data_name, const char *pngfile) {
	fprintf(out,
	        "set terminal pngcairo enhanced size 1280,2071\n"
	        "set key off\n"
	        "set border 1 lw 3\n"  // 1=bottom
	        //"set lmargin at screen 0.18\n"
	        "set lmargin at screen 0.01\n"
	        "set rmargin at screen 0.99\n"
	        "set style line 1 lc rgb 'black' lw 5\n"
	        "set style line 2 lc rgb 'white'\n"
	        "set style line 3 lc rgb 'gray50' lw 1 lt 0\n"
	        "set output '%s'\n", pngfile);

	// Multiplot with optional title string
	fprintf(out, "set multiplot layout 2,1");
	/* if ( m->title == NULL ) { */
		fprintf(out, "\n");
	/* } else { */
	/* 	fprintf(out, " title '%s' font ',24'\n", m->title); */
	/* } */

	// Set universal xrange
	double *buckets = histogram_buckets(m->cumulative_hist);
	int num_buckets = histogram_size(m->cumulative_hist);
	if ( num_buckets > 0 ) {
		double min = buckets[0] - m->bucket_size;
		double max = buckets[num_buckets - 1] + m->bucket_size;
		if ( min == max ) {
			min -= 1.0;
			max += 1.0;
		}
		fprintf(out, "set xrange [%g:%g]\n", min, max);
	}
	free(buckets);

	// Upper plot: Cumulative histogram
	fprintf(out,
	        "set size 1,0.3317\n"
	        "set origin 0,0.6683\n"  // golden ratio, adjusted for space at bottom
	        "set bmargin 0\n"
	        "set grid xtics ls 3\n"
	        "unset xlabel\n"
	        "set format x ''\n"
	        "set xtics scale 0\n"
	        "set style fill solid border lc rgb 'black'\n"
	        /* "set xtics out nomirror font ',20'\n" */
	        "unset ytics\n"
	        "set yrange [0:*]\n");
	if ( m->title != NULL ) {
		fprintf(out, "set tmargin 2\n");
	} else {
		fprintf(out, "set tmargin 0.5\n");
	}
	if ( m->ylabel != NULL )
		fprintf(out, "set ylabel '%s' font ',20'\n", m->ylabel);
	fprintf(out, "plot '%s' using 1:2 with filledcurves above x2 ls 2 lw 6 notitle\n\n", data_name);

	// Lower plot: Individual mountains
	fprintf(out,
	        "set size 1,0.6683\n"
	        "set origin 0,0\n"
	        "set bmargin 6\n"
	        "set tmargin 0\n"
	        //"unset grid\n"
	        "set grid xtics ls 3\n"
	        "set border 1 lw 6\n"
	        "set style fill transparent solid 0.8 border lc rgb 'black'\n"
	        "set xtics out scale default nomirror font 'Verdana,24'\n"
	        "set format x '%%g'\n"
	        "unset ylabel\n"
	        "set format y ''\n");
	if ( m->title != NULL ) {
		fprintf(out, "set xlabel '%s' font 'Verdana,32' offset 0,-1\n", m->title);
	}

	const double yscale = 1.0;  // scale factor for height of mountains
	const double vspread = 1.5;  // separation between mountains
	const int num_mountains = hash_table_size(m->table);

	/* // Custom ytics labels from keys */
	/* int yskip = 1; */
	/* fprintf(out, "set ytics add ("); */
	/* const char *sep = ""; */
	/* for ( int i=0; i < num_mountains; i += yskip ) { */
	/* 	fprintf(out, "%s \"%s\" %g", sep, sorted[i].key, -vspread*i); */
	/* 	sep = ","; */
	/* } */
	/* fprintf(out, " ) font ',12'\n"); */
	fprintf(out, "set yrange [%g:*]\n", -vspread*num_mountains);
	fprintf(out, "plot for [i=1:%d] '%s' using 1:(%g*column(i+3) - %g*i) with filledcurves closed ls 2 title columnhead(i+3)\n\n",
					num_mountains, data_name, yscale, vspread);
	fprintf(out, "unset multiplot\n");
}

void mordor_plot(struct mordor *m, const char *pngfile, FILE *data, FILE *gnuplot, const char *datafile) {
	// Refresh histograms if needed
	mordor_build_histograms(m);

	// Sorting the mountains by the mean of the distribution often gives
	// some visual continuity to the result.
	struct keyvalue_pair *sorted = create_sorted_keys(m);

	switch ( m->style ) {
	case MORDOR_STYLE_CLASSIC:
		mordor_datafile_classic(m, sorted, data);
		mordor_plotscript_classic(m, sorted, gnuplot, datafile, pngfile);
		break;
	case MORDOR_STYLE_CLEAN:
	default:
		mordor_datafile_clean(m, sorted, data);
		mordor_plotscript_clean(m, sorted, gnuplot, datafile, pngfile);
		break;
	}
	free(sorted);
}

//EOF
