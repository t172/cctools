/*
Copyright (C) 2017- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <math.h>

#include <sqlite3.h>

#include "debug.h"
#include "stringtools.h"
#include "xxmalloc.h"
#include "create_dir.h"
#include "jx.h"
#include "jx_parse.h"
#include "jx_pretty_print.h"
#include "list.h"
#include "hash_table.h"
#include "histogram.h"
#include "stats.h"

// What a category is called in the JSON summary data
#define FIELD_CATEGORY "category"

// What the task ID is called in the JSON summary data
#define FIELD_TASK_ID "task_id"

// How to separate fields in output (text) data files
#define OUTPUT_FIELD_SEPARATOR " "

// How to separate records in output (text) data files
#define OUTPUT_RECORD_SEPARATOR "\n"

// How to start a comment in output (text) data files
#define OUTPUT_COMMENT "#"

// A numeric placeholder for (text) data files (not-a-number is
// ignored by gnuplot and not plotted)
#define OUTPUT_PLACEHOLDER "NAN"

// Name used for cumulative stats (used in filename)
#define ALLSTATS_NAME "(all)"

#define VSUNITS_NAME "vs_units"

#define SUBDIR_DATA "data"
#define SUBDIR_PLOT ""

// Name of gnuplot binary
#define GNUPLOT_BINARY "gnuplot"

// Filenames for gnuplot scripts (one in each category's directory)
#define GNUPLOT_BOXPLOT_FILENAME "boxplot.gp"
#define GNUPLOT_HISTOGRAM_FILENAME "histogram.gp"

// Soft maxmimum on number of x-axis labels to put on a plot (if there
// are more than twice this amount, they will be culled)
#define GNUPLOT_SOFTMAX_XLABELS 40

#define CMDLINE_OPTS  "D:J:L:s:t:"
#define OPT_JSON      'J'
#define OPT_LIST      'L'
#define OPT_SPLIT     's'
#define OPT_THRESHOLD 't'
#define OPT_DBFILE    'D'

#define DEFAULT_SPLIT_FIELD "host"

#define TO_STR_EVAL(x) #x
#define TO_STR(x) TO_STR_EVAL(x)

static struct {
	// The input file name
	char *infile;

	// How to read the input file
	enum { INFILE_UNDEF, INFILE_LIST, INFILE_JSON	} infile_type;

	// The output directory (where to write stuff)
	char *output_dir;

	// A Lobster database file to read for more information (optional)
	char *db_file;

	// The field on which to split into groups (like an SQL "GROUP BY")
	char *split_field;

	// A group with less than this many summaries will be dropped
	int threshold;

	// The data fields to include in output
	int num_fields;
	char **fields;
} cmdline = {
	.infile = NULL,
	.infile_type = INFILE_UNDEF,
	.output_dir = ".",
	.db_file = NULL,
	.split_field = DEFAULT_SPLIT_FIELD,
	.threshold = 1,
	.num_fields = 0,
	.fields = NULL
};

struct record {
	char *filename;
	struct jx *json;
	int work_units_total;
	int work_units_processed;
};

static int string_compare(const void *a, const void *b) {
	return strcmp(*(char *const *)a, *(char *const *)b);
}

// Creates a new record pointing to the given JSON, which must be
// freed with record_delete().
struct record *record_create(struct jx *j) {
	struct record *r = xxcalloc(sizeof(*r), 1);
	r->json = j;
	return r;
}

void record_delete(struct record *r) {
	free(r->filename);
	jx_delete(r->json);
	free(r);
}

// Make a string pretty for formal presentation.  Returns a new string
// that must be freed.
static char *presentation_string(const char *s) {
	char *str = xxstrdup(s);
	char *pos;

	// Change underscores "_" to spaces " "
	for ( pos = str; *pos != '\0'; ++pos )
		if ( *pos == '_' )
			*pos = ' ';

	// Capitalize "cpu" to "CPU"
	for ( pos = str; (pos = strstr(pos, "cpu")); strncpy(pos, "CPU", 3) );

	// Capitalize the first letter of words
	for ( pos = str; *pos != '\0'; ) {
		if ( *pos >= 'a' && *pos <= 'z' )
			*pos -= 'a' - 'A';
		while ( *pos != '\0' ) {
			if ( *pos == ' ' || *pos == '\t' ) {
				pos++;
				break;
			} else {
				pos++;
			}
		}
	}
	return str;
}

void show_usage(char *cmd) {
	fprintf(stderr, "Usage:\n  %s [opts] (-%c <jsonfile> | -%c <listfile>) <outdir>\n", cmd, OPT_JSON, OPT_LIST);
	fprintf(stderr, "\nRequired: (one of the following)\n");
	fprintf(stderr, "  -%c <jsonfile>   read file with JSON-encoded summaries\n", OPT_JSON);
	fprintf(stderr, "  -%c <listfile>   read file with list of summary pathnames\n", OPT_LIST);
	fprintf(stderr, "\nOptions:\n");
	fprintf(stderr, "  -%c <dbfile>     use Lobster database <dbfile> for more information\n", OPT_DBFILE);
	fprintf(stderr, "  -%c <field>      split on <field> (default = \"%s\")\n", OPT_SPLIT, DEFAULT_SPLIT_FIELD);
	fprintf(stderr, "  -%c <threshold>  ignore groups with less than <threshold> matches\n", OPT_THRESHOLD);
}

void process_cmdline(int argc, char *argv[]) {
	int used_opts = 0;
	int ch;
	while( (ch = getopt(argc, argv, CMDLINE_OPTS)) != -1 ) {
		used_opts = 1;
		switch ( ch )		{
		case OPT_LIST:
			cmdline.infile_type = INFILE_LIST;
			cmdline.infile = optarg;
			break;
		case OPT_JSON:
			cmdline.infile_type = INFILE_JSON;
			cmdline.infile = optarg;
			break;
		case OPT_SPLIT:
			cmdline.split_field = optarg;
			break;
		case OPT_THRESHOLD:
			cmdline.threshold = atoi(optarg);
			break;
		case OPT_DBFILE:
			cmdline.db_file = optarg;
			break;
		default:
			show_usage(argv[0]);
			exit(EXIT_FAILURE);
			break;
		}
	}
	
	// Output directory must be given
	cmdline.output_dir = argv[optind];
	if ( argc - optind < 1 ) {
		if ( used_opts )
			fprintf(stderr, "No output directory specified.\n");
		show_usage(argv[0]);
		exit(EXIT_FAILURE);
	}

	// Input file must be given
	if ( cmdline.infile_type == INFILE_UNDEF || cmdline.infile == NULL || cmdline.infile[0] == '\0' ) {
		fprintf(stderr, "No input file given (use -%c or -%c).\n", OPT_LIST, OPT_JSON);
		show_usage(argv[0]);
		exit(EXIT_FAILURE);
	}

	// Split field must be given
	if ( cmdline.split_field == NULL || *cmdline.split_field == '\0' ) {
		fprintf(stderr, "No split field specified.\n");
		show_usage(argv[0]);
		exit(EXIT_FAILURE);
	}

	// Default output fields
	if ( cmdline.fields == NULL ) {
		cmdline.num_fields = 4;
		cmdline.fields = xxmalloc(cmdline.num_fields*sizeof(*cmdline.fields));
		cmdline.fields[0] = "wall_time";
		cmdline.fields[1] = "cpu_time";
		cmdline.fields[2] = "memory";
		cmdline.fields[3] = "disk";
	}
}

// Reads a file with a list of file names containing summaries
void read_listfile(const char *listfile, struct list *dst) {
	FILE *listfile_f;
	char *filename = NULL;
	size_t filename_alloc = 0;
	ssize_t len;
	int summaries_read = 0;
	int skipped_summaries = 0;

	if ( (listfile_f = fopen(listfile, "r")) == NULL )
		fatal("Cannot open list file \"%s\" for reading.", listfile);
	while ( (len = getline(&filename, &filename_alloc, listfile_f)) != -1 ) {
		if ( len > 0 && filename[len-1] == '\n' )
			filename[len-1] = '\0';

		struct jx *json = jx_parse_file(filename);
		if ( json == NULL ) {
			// from jx_parse.h on jx_parse_file(): "If the parse fails or no
			// JSON value is present, null is returned."
			skipped_summaries++;
			continue;
		}

		struct record *item = record_create(json);
		item->filename = xxstrdup(filename);

		list_push_tail(dst, item);
		summaries_read++;
	}
	free(filename);  // getline()'s buffer
	fclose(listfile_f);

	if ( skipped_summaries > 0 )
		warn(D_RMON, "Skipped %d summaries because file was not parsed or no JSON found.", skipped_summaries);
	printf("Successfully read %d summary files.\n", summaries_read);
}

// Reads a file containing summaries as JSON objects
void read_jsonfile(const char *jsonfile, struct list *dst) {
	FILE *jsonfile_f;
	if ( (jsonfile_f = fopen(jsonfile, "r")) == NULL )
		fatal("Cannot open summaries JSON file \"%s\" for reading.", jsonfile);

	printf("Reading JSON objects from \"%s\"\n", jsonfile);

	int parse_errors = 0;
	long count = 0;
	struct jx *json;
	for (;;) {
		if ( (json = jx_parse_stream(jsonfile_f)) == NULL ) {
			if ( feof(jsonfile_f) ) {
				break;
			} else {
				warn(D_RMON, "JSON Parser error at file position %ld bytes", ftell(jsonfile_f));
				parse_errors++;
				continue;
			}
		}
		struct record *new_record = record_create(json);
		list_push_tail(dst, new_record);
		count++;
	}
	if ( parse_errors != 0 )
		warn(D_RMON, "Found %d errors parsing \"%s\".", parse_errors, jsonfile);
	//printf("Stopped reading at position %ld\n", ftell(jsonfile_f));
	fclose(jsonfile_f);
	printf("Read %ld JSON objects.\n", count);
}

// Given a list of summaries, group (hash) them by the value of some JSON field
struct hash_table *group_by_field(struct list *list, const char *field) {
	struct hash_table *grouped = hash_table_create(0, 0);
	struct record *item;
	struct list *bucket;
	int num_groups = 0;
	int dropped_summaries = 0;

	list_first_item(list);
	while ( (item = list_next_item(list)) != 0 ) {
		struct jx *value = jx_lookup(item->json, field);
		if ( value == NULL || !jx_istype(value, JX_STRING) ) {
			dropped_summaries++;
			continue;
		}
		if ( (bucket = hash_table_lookup(grouped, value->u.string_value)) == 0 ) {
			bucket = list_create();
			hash_table_insert(grouped, value->u.string_value, bucket);
			num_groups++;
		}
		list_push_tail(bucket, item);
	}
	if ( dropped_summaries > 0 )
		warn(D_RMON, "Dropped %d summaries when grouping by field \"%s\".\n", dropped_summaries, field);
	printf("Split into %d groups by field \"%s\".\n", num_groups, field);
	return grouped;
}

void filter_by_threshold(struct hash_table *grouping, int threshold) {
	char *field;
	struct list *list;

	int filtered_groups = 0;
	hash_table_firstkey(grouping);
	while ( hash_table_nextkey(grouping, &field, (void **)&list) ) {
		if ( list_size(list) < threshold ) {
			list_delete(list);
			hash_table_remove(grouping, field);
			filtered_groups++;
		}
	}
	if ( filtered_groups > 0 )
		printf("Filtered out %d groups with fewer than %d matches.\n", filtered_groups, threshold);
}

// Name of directory in which to place stuff for a specific category.
// Returns a string that must be freed.
static char *category_directory(const char *category, const char *subdir) {
	if ( subdir == NULL || *subdir == '\0' ) {
		return string_format("%s/%s", cmdline.output_dir, category);
	} else {
		return string_format("%s/%s/%s", cmdline.output_dir, category, subdir);
	}
}

// Retrieves the value of the given field from a struct record's JSON,
// writing the value as a double to dst.  On error, returns zero and
// nothing is written to dst.
static int get_json_value(struct record *item, const char *field, struct hash_table *units_of_measure, double *dst) {
	static int warned_inconsistent_units = 0;
	struct jx *jx_value = jx_lookup(item->json, field);
	if ( jx_value == NULL )
		return 0;

	struct jx *jx_unit;
	if ( jx_value->type == JX_ARRAY ) {
		// Keep track of the unit of measure
		if ( units_of_measure != NULL ) {
			jx_unit = jx_array_index(jx_value, 1);
			if ( jx_unit != NULL && jx_unit->type == JX_STRING ) {
				char *previous_unit = hash_table_lookup(units_of_measure, field);
				if ( previous_unit == NULL ) {
					hash_table_insert(units_of_measure, field, xxstrdup(jx_unit->u.string_value));
				} else {
					if ( strcmp(previous_unit, jx_unit->u.string_value) != 0 && !warned_inconsistent_units ) {
						warn(D_RMON, "Encountered inconsistent units for \"%s\": \"%s\" and \"%s\".",
								 field, previous_unit, jx_unit->u.string_value);
						warned_inconsistent_units = 1;
					}
				}
			}
		}
		
		// First is the value
		jx_value = jx_array_index(jx_value, 0);
		if ( jx_value == NULL )
			return 0;
	}
	if ( jx_value->type == JX_DOUBLE ) {
		*dst = jx_value->u.double_value;
		return 1;
	} else if ( jx_value->type == JX_INTEGER ) {
		*dst = jx_value->u.integer_value;
		return 1;
	}
	return 0; // failure
}

// Opens an output file with the given name in a subdirectory created
// for the given category.
static FILE *open_category_file(const char *category, const char *subdir, const char *filename) {
	char *outdir = category_directory(category, subdir);
	if ( !create_dir(outdir, 0755) )
		fatal("Cannot create output directory \"%s\"", outdir);

	char *pathname = string_format("%s/%s", outdir, filename);
	FILE *f;
	if ( (f = fopen(pathname, "w")) == NULL )
		fatal("Cannot open file \"%s\" for writing", pathname);
	free(outdir);
	free(pathname);
	return f;
}

static void write_histograms_files(struct hash_table *stats_table[2], struct hash_table *bucket_sizes[2], const char *split_key, const char *category) {
	char *hist_filename[2] = { string_format("%s.hist", split_key), string_format("%s_perunit.hist", split_key) };
	for ( int i=0; i<2; ++i ) {
		FILE *out = open_category_file(category, SUBDIR_DATA, hist_filename[i]);
		free(hist_filename[i]);
		fprintf(out, OUTPUT_COMMENT);

		struct histogram **histograms = xxmalloc(cmdline.num_fields*sizeof(*histograms));
		double **buckets = xxmalloc(cmdline.num_fields*sizeof(*buckets));
		int max_histsize = 0;

		// Build histograms
		char *outfield;
		const char *sep = "";
		for ( int f=0; f < cmdline.num_fields; ++f ) {
			outfield = cmdline.fields[f];
			struct stats *stat = hash_table_lookup(stats_table[i], outfield);
			struct histogram *hist = stats_build_histogram(stat, *(double *)hash_table_lookup(bucket_sizes[i], outfield), STATS_KEEP_OUTLIERS);
			histograms[f] = hist;
			if ( hist != NULL ) {
				buckets[f] = histogram_buckets(hist);
				const int hist_size = histogram_size(hist);
				if ( max_histsize < hist_size )
					max_histsize = hist_size;
			}

			// Print header with column names
			fprintf(out, "%1$s%2$s_start" OUTPUT_FIELD_SEPARATOR "%2$s_freq", sep, outfield);
			sep = OUTPUT_FIELD_SEPARATOR;
		}
		fprintf(out, OUTPUT_RECORD_SEPARATOR);

		// Write them to file
		for ( int bucket=0; bucket < max_histsize; ++bucket ) {
			sep = "";
			for ( int f=0; f < cmdline.num_fields; ++f ) {
				struct histogram *hist = histograms[f];
				if ( hist == NULL || bucket >= histogram_size(hist) ) {
					// Insert placeholders
					fprintf(out, "%s" OUTPUT_PLACEHOLDER OUTPUT_FIELD_SEPARATOR OUTPUT_PLACEHOLDER, sep);
				} else {
					const double start = buckets[f][bucket];
					fprintf(out, "%s%g" OUTPUT_FIELD_SEPARATOR "%d", sep, start, histogram_count(hist, start));
				}
				sep = OUTPUT_FIELD_SEPARATOR;
			}
			fprintf(out, OUTPUT_RECORD_SEPARATOR);
		}

		// Clean up
		fclose(out);
		for ( int f=0; f < cmdline.num_fields; ++f ) {
			if ( histograms[f] != NULL )
				histogram_delete(histograms[f]);
		}
		free(histograms);
	}
}

// Plots <output_field> vs. <work_units_processed>, regardless of
// split_key (e.g. host).
void write_vs_units_plots(struct hash_table *grouping, const char *category, struct hash_table *units_of_measure) {
	// Find ways to give up
	if ( hash_table_size(grouping) == 0 )
		return;
	if ( category == NULL || category[0] == '\0' ) {
		warn(D_RMON, "No category given or empty string.");
		return;
	}

	// Write data file header
	FILE *out = open_category_file(category, SUBDIR_DATA, VSUNITS_NAME ".dat");
	fprintf(out, OUTPUT_COMMENT "" FIELD_TASK_ID "" OUTPUT_FIELD_SEPARATOR "units_processed" OUTPUT_FIELD_SEPARATOR "units");
	for ( int f=0; f < cmdline.num_fields; ++f ) {
		fprintf(out, OUTPUT_FIELD_SEPARATOR "%s", cmdline.fields[f]);
	}
	fprintf(out, OUTPUT_RECORD_SEPARATOR);

	const int UNITS_PROCESSED = 0;
	const int UNITS_TOTAL = 1;
	const char *const display_string[] = { "Work Units Processed", "Total Work Units" };
	const char *const name_string[] = { "units_processed", "units_total" };

	// Keep two-dimensional stats of {units_processed,units} vs. {all output fields}
	struct stats2 (*stat)[2] = xxmalloc(cmdline.num_fields*sizeof(*stat));
	for ( int f=0; f < cmdline.num_fields; ++f ) {
		stats2_init(&stat[f][UNITS_PROCESSED]);
		stats2_init(&stat[f][UNITS_TOTAL]);
	}

	// Write data
	char *ignored_key;
	hash_table_firstkey(grouping);
	for ( struct list *record_list; hash_table_nextkey(grouping, &ignored_key, (void **)&record_list); ) {
		list_first_item(record_list);
		for ( struct record *item; (item = list_next_item(record_list)) != NULL; ) {
			struct jx *task_id_jx;
			const char *task_id = NULL;
			if ( (task_id_jx = jx_lookup(item->json, FIELD_TASK_ID)) != NULL && task_id_jx->type == JX_STRING )
				task_id = task_id_jx->u.string_value;
			fprintf(out, "%s" OUTPUT_FIELD_SEPARATOR "%d" OUTPUT_FIELD_SEPARATOR "%d",
							task_id, item->work_units_processed, item->work_units_total);
			for ( int f=0; f < cmdline.num_fields; ++f ) {
				double value;
				if ( !get_json_value(item, cmdline.fields[f], NULL, &value) ) {
					fprintf(out, OUTPUT_FIELD_SEPARATOR "" OUTPUT_PLACEHOLDER);
				} else {
					fprintf(out, OUTPUT_FIELD_SEPARATOR "%g", value);
					stats2_insert(&stat[f][UNITS_PROCESSED], item->work_units_processed, value);
					stats2_insert(&stat[f][UNITS_TOTAL], item->work_units_total, value);
				}
			}
			fprintf(out, OUTPUT_RECORD_SEPARATOR);
		}
	}
	fclose(out);

	// Write a gnuplot script
	out = open_category_file(category, SUBDIR_PLOT, "vs_units.gp");
	fprintf(out, "set terminal pngcairo enhanced size 1024,768\n");
	fprintf(out, "set tics font ',16'\n");
	fprintf(out, "set style line 1 lc rgb 'gray20' pt 7\n");
	fprintf(out, "set style line 2 lc rgb '#880000' lw 4\n");
	fprintf(out, "set style fill transparent solid 0.1 noborder\n");
	fprintf(out, "unset key\n");
	fprintf(out, "set yrange [0:]\n");
	
	for ( int f=0; f < cmdline.num_fields; ++f ) {
		const char *pretty_field = presentation_string(cmdline.fields[f]);
		for ( int u=0; u < 2; ++u ) {
			fprintf(out, "\n# %s vs. %s\n", pretty_field, display_string[u]);
			fprintf(out, "set output '%s_vs_%s.png'\n", cmdline.fields[f], name_string[u]);
			fprintf(out, "set title '%s vs. %s  (%ld \"%s\" Summaries)' font ',22'\n",
							pretty_field, display_string[u], stat[f][u].count, category);
			fprintf(out, "set xlabel '%s' font ',20'\n", display_string[u]);
			double left = stat[f][u].min_x - 0.01*(stat[f][u].max_x - stat[f][u].min_x);
			fprintf(out, "set xrange [%g:%g]\n", left<0 ? left : 0, stat[f][u].max_x + 0.01*(stat[f][u].max_x - stat[f][u].min_x));
			const char *unit_string = hash_table_lookup(units_of_measure, cmdline.fields[f]);
			const char *original_unit = unit_string;
			double unit_conversion = 1;
			if ( strcmp(unit_string, "MB") == 0 ) {
				// Convert MB to GB
				unit_string = "GB";
				fprintf(out, "convert_unit(y) = y/1024\n");
				unit_conversion = 1024;
			} else if ( strcmp(unit_string, "s" ) == 0 ) {
				// Convert s to hr
				unit_string = "hr";
				fprintf(out, "convert_unit(y) = y/3600\n");
				unit_conversion = 3600;
			} else {
				fprintf(out, "convert_unit(y) = y\n");
				unit_conversion = 1;
			}
			fprintf(out, "set ylabel '%s", pretty_field);
			if ( unit_string != NULL ) {
				fprintf(out, " (%s)", unit_string);
			}
			fprintf(out, "' font ',20'\n");
			fprintf(out, "set style circle radius %g\n", 0.01*stat[f][u].max_x);

			double a, b;
			const int have_regression = stats2_linear_regression(&stat[f][u], &a, &b);
			if ( have_regression ) {
				fprintf(out, "set label 1 \"{/Oblique y} = (%2$g %1$s){/Oblique x} + (%3$g %1$s)\\n"
								"correlation %4$f\" at screen 0.52,0.17 left font ',18'\n",
								original_unit, a, b,	stats2_linear_correlation(&stat[f][u]));
			} else {
				fprintf(out, "set label 1 \"\"\n");
			}
			fprintf(out, "plot '%s%s" VSUNITS_NAME ".dat'", SUBDIR_DATA, SUBDIR_DATA[0] != '\0' ? "/" : "");
			fprintf(out, " using %d:(convert_unit($%d)) with circles ls 1 notitle", u+2, f+4);
			if ( have_regression ) {
				fprintf(out, ", \\\n\tconvert_unit(%g*x + %g) with lines ls 2 notitle\n", a, b);
			} else {
				fprintf(out, "\n");
			}
		}
	} 
	fclose(out);
}

// Will populate bucket_sizes[0] with bucket sizes used for original
// histogram and bucket_sizes[1] with bucket sizes for the per-unit
// histograms.
void write_avgs(struct hash_table *grouping, const char *category, struct hash_table *units_of_measure, struct hash_table *bucket_sizes[2]) {
	// Find ways to give up
	if ( hash_table_size(grouping) == 0 )
		return;
	if ( category == NULL || category[0] == '\0' ) {
		warn(D_RMON, "No category given or empty string.");
		return;
	}

	struct hash_table *output_file[2] = { hash_table_create(0, 0), hash_table_create(0, 0) };

	// Maintain stats for each split_list
	struct hash_table *stats[2] = { hash_table_create(0, 0), hash_table_create(0, 0) };

	// Maintain stats for all data, [0] is original values, [1] is value
	// per work unit processed
	struct hash_table *all_stats[2] = { hash_table_create(0, 0), hash_table_create(0, 0) };

	for ( int f=0; f < cmdline.num_fields; ++f ) {
		char *outfield = cmdline.fields[f];
		char *filename[2] = { string_format("%s.dat", outfield), string_format("%s_perunit.dat", outfield) };
		FILE *out;
		for ( int i=0; i<2; ++i ) {
			// Create a file for each output field
			out = open_category_file(category, SUBDIR_DATA, filename[i]);
			free(filename[i]);
			hash_table_insert(output_file[i], outfield, out);
			fprintf(out, OUTPUT_COMMENT "%s summaries mean stddev whisker_low Q1 median Q3 whisker_high\n", cmdline.split_field);

			// Initialize stats on each output field's values
			struct stats *stat = xxmalloc(sizeof(*stat));
			stats_init(stat);
			hash_table_insert(stats[i], outfield, stat);
			
			// Initialize stats for all data
			stat = xxmalloc(sizeof(*stat));
			stats_init(stat);
			hash_table_insert(all_stats[i], outfield, stat);
		}
	}

	// Build array of keys and sort them
	int num_splits = hash_table_size(grouping);
	char **keys_sorted = xxmalloc(num_splits*sizeof(*keys_sorted));
	struct hash_table *value_list;
	char *split_key;
	struct list *split_list;
	struct record *item;
	hash_table_firstkey(grouping);
	for ( int group=0; hash_table_nextkey(grouping, &split_key, (void **)&value_list) != 0; ) {
		keys_sorted[group++] = split_key;
	}
	qsort(keys_sorted, num_splits, sizeof(*keys_sorted), string_compare);

	// First, iterate through all data to obtain full data range (min and max)
	for ( int group_num=0; group_num < num_splits; ++group_num ) {
		struct list *split_list = hash_table_lookup(grouping, keys_sorted[group_num]);
		list_first_item(split_list);
		while ( (item = list_next_item(split_list)) != 0 ) {
			for ( int f=0; f < cmdline.num_fields; ++f ) {
				double value;
				if ( !get_json_value(item, cmdline.fields[f], NULL, &value) )
					continue;
				stats_insert(hash_table_lookup(all_stats[0], cmdline.fields[f]), value);
				stats_insert(hash_table_lookup(all_stats[1], cmdline.fields[f]), value / item->work_units_processed);
			}
		}
	}

	// Calculate bucket sizes based on cumulative stats (data ranges)
	for ( int f=0; f < cmdline.num_fields; ++f ) {
		double *bucket_size;
		for ( int i=0; i<2; ++i ) {
			struct stats *stat = hash_table_lookup(all_stats[i], cmdline.fields[f]);
			bucket_size = xxmalloc(sizeof(*bucket_size));
			*bucket_size = stats_ideal_bucket_size(stat);
			hash_table_insert(bucket_sizes[i], cmdline.fields[f], bucket_size);
		}
	}

	// Print cumulative histogram of all stats
	write_histograms_files(all_stats, bucket_sizes, ALLSTATS_NAME, category);

	// Iterate through split keys in sorted order
	for ( int group=0; group < num_splits; ++group ) {
		split_key = keys_sorted[group];
		split_list = hash_table_lookup(grouping, split_key);

		// Create file in which to dump values read
		char *this_filename = string_format("%s.dat", split_key);
		FILE *this_match_file = open_category_file(category, SUBDIR_DATA, this_filename);
		free(this_filename);

		// Read values, iterating through items of split_list
		for ( list_first_item(split_list); (item = list_next_item(split_list)) != 0; ) {
			// Work units for this task
			fprintf(this_match_file,  "%d" OUTPUT_FIELD_SEPARATOR "%d", item->work_units_processed, item->work_units_total);

			// Lookup value for each output field
			for ( int f=0; f < cmdline.num_fields; ++f ) {
				double value;
				if ( !get_json_value(item, cmdline.fields[f], units_of_measure, &value) ) {
					fprintf(this_match_file, OUTPUT_FIELD_SEPARATOR "" OUTPUT_PLACEHOLDER);
					continue;
				}
				fprintf(this_match_file, OUTPUT_FIELD_SEPARATOR "%g", value);

				// Insert to this split_list's stats
				stats_insert(hash_table_lookup(stats[0], cmdline.fields[f]), value);
				stats_insert(hash_table_lookup(stats[1], cmdline.fields[f]), value / item->work_units_processed);
			}
			fprintf(this_match_file, OUTPUT_RECORD_SEPARATOR);
		}
		fclose(this_match_file);

		// Print histogram
		write_histograms_files(stats, bucket_sizes, split_key, category);

		// Print averages
		for ( int i=0; i < 2; ++i ) {
			for ( int f=0; f < cmdline.num_fields; ++f ) {
				// Each output field's aggregated data goes to a separate file
				FILE *out = hash_table_lookup(output_file[i], cmdline.fields[f]);

				// Print matching field (split_key) and number of matches
				fprintf(out, "%s", split_key);
				fprintf(out, OUTPUT_FIELD_SEPARATOR "%d", list_size(split_list));

				// Print statistics
				struct stats *stat = hash_table_lookup(stats[i], cmdline.fields[f]);
				fprintf(out, OUTPUT_FIELD_SEPARATOR "%g", stats_mean(stat));
				fprintf(out, OUTPUT_FIELD_SEPARATOR "%g", stats_stddev(stat));
				fprintf(out, OUTPUT_FIELD_SEPARATOR "%g", stats_whisker_low(stat));
				fprintf(out, OUTPUT_FIELD_SEPARATOR "%g", stats_Q1(stat));
				fprintf(out, OUTPUT_FIELD_SEPARATOR "%g", stats_median(stat));
				fprintf(out, OUTPUT_FIELD_SEPARATOR "%g", stats_Q3(stat));
				fprintf(out, OUTPUT_FIELD_SEPARATOR "%g", stats_whisker_high(stat));
				fprintf(out, OUTPUT_RECORD_SEPARATOR);

				// Re-initialize to zero for next iteration
				stats_reset(stat);
			}
		}
	}  // each split_key

	// Clean up
	free(keys_sorted);
	for ( int i=0; i<2; ++i ) {
		for ( int f=0; f < cmdline.num_fields; ++f ) {
			fclose(hash_table_lookup(output_file[i], cmdline.fields[f]));
			free(hash_table_lookup(stats[i], cmdline.fields[f]));
		}
		hash_table_delete(output_file[i]);
		hash_table_delete(stats[i]);
	}
}

// Writes the gnuplot script for one specific output field
static void plotscript_boxplot_outfield(FILE *f, const char *outfield, struct hash_table *units_of_measure) {
	char *pretty_outfield = presentation_string(outfield);
	char *pretty_splitfield = presentation_string(cmdline.split_field);
	char *basename = string_format("%s.dat", outfield);
	char *data_filename = string_format("%s%s%s", SUBDIR_DATA, SUBDIR_DATA[0] != '\0' ? "/" : "", basename);
	free(basename);

	fprintf(f, "\n# %s\n", outfield);
	fprintf(f, "set output '%s-boxplot.png'\n", outfield);
	fprintf(f, "set title '{/=16 %s vs. %s'.title_suffix.'}'\n", pretty_outfield, pretty_splitfield);

	// Reduce the number of xtic labels
	fprintf(f, "xskip = 1; n = system(\"wc -l %s\")\n", data_filename);
	fprintf(f, "if (n > " TO_STR(GNUPLOT_SOFTMAX_XLABELS) ") xskip = int(n/" TO_STR(GNUPLOT_SOFTMAX_XLABELS) ")\n");

	// Determine unit of measure
	char *unit = hash_table_lookup(units_of_measure, outfield);
	char *gnuplot_unit_conversion = "";

	// Convert seconds to hours
	if ( unit != NULL && strcmp(unit, "s") == 0 ) {
		unit = "hr";
		gnuplot_unit_conversion = "/3600";
	}

	// Output field with unit
	fprintf(f, "set ylabel '{/=14 %s", pretty_outfield);
	if ( unit != NULL ) {
		fprintf(f, " (%s)", unit);
	}
	fprintf(f, "}'\n");

	// Mean as dot and stddev as error bars
	//fprintf(f, "plot '%s' u 0:3:xticlabels(strcol(1)) w points pt 7 lc rgb 'black', \\\n\t'' u 0:3:4 with yerrorbars ls 1\n", data_filename);

	// Boxplots
	fprintf(f, "set boxwidth 0.5 absolute\n");
	fprintf(f, "plot \"<sed 's/\\.crc\\.nd\\.edu//' %1$s\" u 0:($6%2$s):($5%2$s):($9%2$s):($8%2$s):xticlabels(int(column(0))%%xskip==0?strcol(1):'') w candlesticks ls 2, \\\n"
	           "\t'' u 0:($7%2$s):($7%2$s):($7%2$s):($7%2$s) w candlesticks ls 1 lw 2\n", data_filename, gnuplot_unit_conversion);

	free(pretty_outfield);
	free(pretty_splitfield);
	free(data_filename);
}

int write_plotscript_boxplot(struct hash_table *grouping, const char *category, struct hash_table *units_of_measure) {
	// Find ways to give up
	if ( hash_table_size(grouping) == 0 )
		return 0;
	if ( category == NULL || category[0] == '\0' ) {
		warn(D_RMON, "No category given or empty string.");
		return 0;
	}

	// Tally total number of summaries
	long num_summaries = 0;
	char *split_key;
	struct list *split_list;
	hash_table_firstkey(grouping);
	while ( hash_table_nextkey(grouping, &split_key, (void **)&split_list) != 0 ) {
		num_summaries += list_size(split_list);
	}

	FILE *gnuplot_script = open_category_file(category, SUBDIR_PLOT, GNUPLOT_BOXPLOT_FILENAME);
	fprintf(gnuplot_script, "set terminal pngcairo enhanced color size 1072,768\n");
	fprintf(gnuplot_script, "set key off\n");
	fprintf(gnuplot_script, "set xtics nomirror rotate by 60 right\n");
	fprintf(gnuplot_script, "set style line 1 lc rgb 'black'\n");
	fprintf(gnuplot_script, "set style line 2 lc rgb 'gray50'\n");
	fprintf(gnuplot_script, "title_suffix = '\t(%ld \"%s\" Summaries)'\n", num_summaries, category);

	char *pretty_splitfield = presentation_string(cmdline.split_field);
	fprintf(gnuplot_script, "set xlabel '{/=14 %s  (%d total)}'\n", pretty_splitfield, hash_table_size(grouping));
	free(pretty_splitfield);

	for ( int f=0; f < cmdline.num_fields; ++f ) {
		plotscript_boxplot_outfield(gnuplot_script, cmdline.fields[f], units_of_measure);
	}
	return 1;
}

// Writes the gnuplot script for one specific output field
static void plotscript_histogram_outfield(FILE *f, const char *outfield, int col, struct hash_table *units_of_measure, struct hash_table *bucket_sizes[2]) {
	char *pretty_outfield = presentation_string(outfield);
	char *pretty_splitfield = presentation_string(cmdline.split_field);
	char *data_filename[2] = {
		string_format("%s%s%s.dat", SUBDIR_DATA, SUBDIR_DATA[0] != '\0' ? "/" : "", outfield),
		string_format("%s%s%s_perunit.dat", SUBDIR_DATA, SUBDIR_DATA[0] != '\0' ? "/" : "", outfield)
	};

	for ( int i=0; i<2; ++i ) {
		// File name suffix
		const char *filename_suffix;
		if ( i == 1 ) {
			filename_suffix = "_perunit";
		} else {
			filename_suffix = "";
		}

		// Bucket size
		double bucket_size = *(double *)hash_table_lookup(bucket_sizes[i], outfield);

		fprintf(f, "\n# %s%s\n", outfield, i==1?" per work unit":"");
		fprintf(f, "set output '%s%s-hist.png'\n", outfield, filename_suffix);
		fprintf(f, "splits = system(\"sort -n -k 3 '%s%s%s%s.dat' | sed '1d;s/ .*$//'\")\n", SUBDIR_DATA, (SUBDIR_DATA[0] != '\0' ? "/" : ""), outfield, filename_suffix);
		fprintf(f, "set multiplot layout 2,1 title '{/=28 %s%s vs. %s'.title_suffix.'}'\n",
						pretty_outfield, i==1 ? " per Work Unit" : "", pretty_splitfield);
		//fprintf(f, "tweak(file) = sprintf(\"<awk '$%1$d==\\\"NAN\\\"{next}NR==2{x=$%1$d;y=0}NR>=2{print ($%1$d+x)/2,y;print $%1$d,(y+$%2$d)/2;x=$%1$d;y=$%2$d}END{print x,0}' '%%s'\", file)\n", col, col+1);
		fprintf(f, "tweak(file) = sprintf(\"<awk '$%1$d==\\\"NAN\\\"{next}NR==2{x=$%1$d-%3$g;y=0}NR>=2{for(;x+%4$g<$%1$d;x+=%3$g){print x,y;y=0}print $%1$d,$%2$d;x=$%1$d+%3$g;y=$%2$d}END{print x,0}' '%%s'\", file)\n", col, col+1, bucket_size, 0.5*(bucket_size));
		fprintf(f, "yscale = 1\n");
		fprintf(f, "set style line 1 lc rgb '%s'\n", i==1 ? "#ffcccc" : "grey90");
		fprintf(f, "set style line 2 lc rgb '%s'\n", i==1 ? "#ffcccc" : "black");
		fprintf(f, "set style line 3 lc rgb '#880000'\n");

		// Reduce the number of xtic labels
		fprintf(f, "yskip = 1; n = system(\"wc -l %s\")\n", data_filename[i]);
		fprintf(f, "if (n > " TO_STR(GNUPLOT_SOFTMAX_XLABELS) ") yskip = int(n/" TO_STR(GNUPLOT_SOFTMAX_XLABELS) ")\n");

		// Determine unit of measure
		char *unit = hash_table_lookup(units_of_measure, outfield);
		char *gnuplot_unit_conversion = "";

		// Convert seconds to hours
		if ( unit != NULL && strcmp(unit, "s") == 0 ) {
			unit = "hr";
			gnuplot_unit_conversion = "/3600";
		}

		// Top plot (cumulative histogram)
		fprintf(f, "set size 1,0.3\n");
		fprintf(f, "set origin 0,0.7\n");
		fprintf(f, "set bmargin 0\n");
		fprintf(f, "set tmargin 2\n");
		fprintf(f, "unset xlabel\n");
		fprintf(f, "unset ytics\n");
		fprintf(f, "set format x ''\n");
		fprintf(f, "set ylabel 'All %ss' font ',20'\n", pretty_splitfield);
		fprintf(f, "set yrange [0:]\n");
		fprintf(f, "plot tweak('%s%s" ALLSTATS_NAME "%s.hist') using ($1%s):(yscale*$2) with filledcurves ls %d notitle\n\n",
						SUBDIR_DATA, (SUBDIR_DATA[0] != '\0' ? "/" : ""), filename_suffix, gnuplot_unit_conversion, i==1 ? 3 : 2);

		// Bottom plot (decomposed by split field)
		fprintf(f, "set size 1,0.7\n");
		fprintf(f, "set origin 0,0\n");
		fprintf(f, "set bmargin 3.5\n");
		fprintf(f, "set tmargin 0\n");
		fprintf(f, "set format x '%%g'\n");
		fprintf(f, "set xlabel '{/=20 %s", pretty_outfield);
		if ( unit ) fprintf(f, " (%s%s)", unit, i==1 ? "/unit" : "");
		fprintf(f, "}'\n");
		fprintf(f, "unset ylabel\n");
		fprintf(f, "set format y ''\n");
		fprintf(f, "do for [i=1:(words(splits))] {\n");
		fprintf(f, "  lbl = system(\"echo \".word(splits, i).\" | sed 's/\\.crc\\.nd\\.edu//;s/\\.nd\\.edu//'\")\n");
		fprintf(f, "  if ( i %% yskip == 0 ) { set ytics add ( (lbl) (-vspread*i) ) }\n");
		fprintf(f, "}\n");
		fprintf(f, "set ytics font \",12\"\n");
		fprintf(f, "set yrange [-vspread*(1.05*words(splits)):]\n");
		fprintf(f, "plot for [i=1:(words(splits))] (tweak('%s%s'.word(splits, i).'%s.hist')) using ($1%s):(yscale*$2 - vspread*i) with filledcurves ls 1 notitle\n\n",
						SUBDIR_DATA, (SUBDIR_DATA[0] != '\0' ? "/" : ""), filename_suffix, gnuplot_unit_conversion);
		fprintf(f, "unset multiplot\n");

		free(data_filename[i]);
	}
	free(pretty_outfield);
	free(pretty_splitfield);
}

int write_plotscript_histogram(struct hash_table *grouping, const char *category, struct hash_table *units_of_measure, struct hash_table *bucket_sizes[2]) {
	// Find ways to give up
	if ( hash_table_size(grouping) == 0 )
		return 0;
	if ( category == NULL || category[0] == '\0' ) {
		warn(D_RMON, "No category given or empty string.");
		return 0;
	}

	// Tally total number of summaries
	long num_summaries = 0;
	char *split_key;
	struct list *split_list;
	hash_table_firstkey(grouping);
	while ( hash_table_nextkey(grouping, &split_key, (void **)&split_list) != 0 ) {
		num_summaries += list_size(split_list);
	}

	FILE *gnuplot_script = open_category_file(category, SUBDIR_PLOT, GNUPLOT_HISTOGRAM_FILENAME);
	fprintf(gnuplot_script, "set terminal pngcairo enhanced size 1280,2048\n");//640,1024
	fprintf(gnuplot_script, "set key off\n");
	fprintf(gnuplot_script, "set style fill transparent solid 0.9 border lc rgb 'black'\n");
	fprintf(gnuplot_script, "title_suffix = '    (%ld \"%s\" Summaries)'\n", num_summaries, category);
	fprintf(gnuplot_script, "vspread = 1.5\n");
	fprintf(gnuplot_script, "set lmargin at screen 0.18\n");
	fprintf(gnuplot_script, "set grid xtics\n");
	fprintf(gnuplot_script, "set grid\n");
	fprintf(gnuplot_script, "set xtics font ',20'\n");

	for ( int f=0, col=1; f < cmdline.num_fields; ++f, col += 2 ) {
		plotscript_histogram_outfield(gnuplot_script, cmdline.fields[f], col, units_of_measure, bucket_sizes);
	}
	return 1;
}

void plot_category(struct hash_table *grouping, const char *category, struct hash_table *units_of_measure, struct hash_table *bucket_sizes[2]) {
	if ( !write_plotscript_boxplot(grouping, category, units_of_measure) )
		return;
	if ( !write_plotscript_histogram(grouping, category, units_of_measure, bucket_sizes) )
		return;

	/* printf("Plotting category \"%s\"\n", category); */
	/* errno = 0; */
	/* pid_t pid; */
	/* if ( (pid = fork()) < 0 ) { // error */
	/* 	fatal("Cannot fork process: %s", strerror(errno)); */
	/* } else if ( pid > 0 ) { // child */
	/* 	// Change to directory where category stuff goes */
	/* 	char *dir = category_directory(category, ""); */
	/* 	if ( chdir(dir) != 0 ) */
	/* 		fatal("Cannot change directory to \"%s\".", dir); */
	/* 	free(dir); */

	/* 	// Invoke gnuplot */
	/* 	errno = 0; */
	/* 	printf("%s %s\n", GNUPLOT_BINARY, GNUPLOT_SCRIPT_FILENAME); */
	/* 	execlp(GNUPLOT_BINARY, GNUPLOT_BINARY, GNUPLOT_SCRIPT_FILENAME, (char *)NULL); */
	/* 	fatal("Error executing plotter: %s", strerror(errno)); */
	/* 	exit(EXIT_FAILURE); */
	/* } else { // parent */
	/* 	int status; */
	/* 	waitpid(pid, &status, 0); */
	/* 	if ( WIFEXITED(status) ) { */
	/* 		if ( WEXITSTATUS(status) != EXIT_SUCCESS ) { */
	/* 			warn(D_RMON, "Child exited with status %d", WEXITSTATUS(status)); */
	/* 		} */
	/* 	} else { */
	/* 		warn(D_RMON, "Child exited abnormally."); */
	/* 	} */
	/* } */
}

// Queries the database in db_file for the number of work units for
// tasks in a given list of records.
void query_database_for_list(const char *db_file, struct list *list) {
	sqlite3 *db;
	if ( sqlite3_open(db_file, &db) != SQLITE_OK )
		fatal("Cannot open database \"%s\": %s", db_file, sqlite3_errmsg(db));
	const char *sql_query = "SELECT units, units_processed FROM tasks WHERE id=?";

	struct record *item;
	list_first_item(list);
	while ( (item = list_next_item(list)) != NULL ) {
		// Get task ID
		int task_id;
		struct jx *jx_value;
		if ( (jx_value = jx_lookup(item->json, FIELD_TASK_ID)) == NULL )
			continue;
		if ( jx_value->type == JX_INTEGER ) {
			task_id = jx_value->u.integer_value; // Note: losing int64_t to int
		} else if ( jx_value->type == JX_STRING ) {
			task_id = atoi(jx_value->u.string_value);
		} else {
			continue;
		}

		// Query database
		sqlite3_stmt *res;
		if ( sqlite3_prepare_v2(db, sql_query, -1, &res, 0) != SQLITE_OK )
			fatal("SQL error: %s", sqlite3_errmsg(db));
		sqlite3_bind_int(res, 1, task_id);
		if ( sqlite3_step(res) != SQLITE_ROW )
			continue;

		// Note: Using only first row if multiple rows
		item->work_units_total = sqlite3_column_int(res, 0);
		item->work_units_processed = sqlite3_column_int(res, 1);
		sqlite3_finalize(res);
	}
	sqlite3_close(db);
}

int main(int argc, char *argv[]) {
	debug_config(argv[0]);
	process_cmdline(argc, argv);

	// Read input
	struct list *summaries = list_create();
	switch ( cmdline.infile_type ) {
	case INFILE_LIST:
		read_listfile(cmdline.infile, summaries);
		break;
	case INFILE_JSON:
		read_jsonfile(cmdline.infile, summaries);
		break;
	default:
		fatal("Input file not specified.");
	}

	// Split by category
	struct hash_table *grouped_by_category = group_by_field(summaries, FIELD_CATEGORY);

	// Split by user-specified field
	char *category;
	struct list *list_in_category;
	hash_table_firstkey(grouped_by_category);
	while ( hash_table_nextkey(grouped_by_category, &category, (void **)&list_in_category) ) {
		// Split category on user's split_field
		printf("Subdividing category \"%s\"...\n", category);
		struct hash_table *split_category = group_by_field(list_in_category, cmdline.split_field);
		filter_by_threshold(split_category, cmdline.threshold);
		struct hash_table *units_of_measure = hash_table_create(0, 0);

		// Query Lobster database to get more information
		struct list *split_list;
		char *split_field;
		if ( cmdline.db_file != NULL ) {
			hash_table_firstkey(split_category);
			while ( hash_table_nextkey(split_category, &split_field, (void **)&split_list) ) {
				query_database_for_list(cmdline.db_file, split_list);
			}
		}

		// Calculate statistics, write output files and plots
		struct hash_table *bucket_sizes[2] = { hash_table_create(0, 0), hash_table_create(0, 0) };
		write_avgs(split_category, category, units_of_measure, bucket_sizes);
		plot_category(split_category, category, units_of_measure, bucket_sizes);
		write_vs_units_plots(split_category, category, units_of_measure);

		// Free values from hash tables
		char *output_field, *unit_str;
		for ( int i=0; i<2; ++i ) {
			double *bucket_size;
			hash_table_firstkey(bucket_sizes[i]);
			while ( hash_table_nextkey(bucket_sizes[i], &output_field, (void **)&bucket_size) != 0 )
				free(bucket_size);
		}

		hash_table_firstkey(units_of_measure);
		while ( hash_table_nextkey(units_of_measure, &output_field, (void **)&unit_str) != 0 )
			free(unit_str);
		hash_table_delete(units_of_measure);

		hash_table_firstkey(split_category);
		while ( hash_table_nextkey(split_category, &split_field, (void **)&split_list) ) {
			list_delete(split_list);
		}

		list_delete(list_in_category);
		hash_table_delete(split_category);
	}

	// Clean up
	hash_table_delete(grouped_by_category);

	// Clean up list of all summaries
	struct record *item;
	list_first_item(summaries);
	while ( (item = list_next_item(summaries)) != 0 )
		record_delete(item);
	list_delete(summaries);

	free(cmdline.fields);
	return EXIT_SUCCESS;
}

//EOF
