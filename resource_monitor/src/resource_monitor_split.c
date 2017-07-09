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
#include "stats.h"
#include "mordor.h"

// Field names as they appear in the JSON data
#define FIELD_CATEGORY "category"
#define FIELD_TASK_ID "task_id"
#define FIELD_WALL_TIME "wall_time"

// Data file formats
#define OUTPUT_FIELD_SEPARATOR " "
#define OUTPUT_RECORD_SEPARATOR "\n"
#define OUTPUT_COMMENT "#"
#define OUTPUT_PLACEHOLDER "NAN"

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

// Command line options
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

// The previously encountered units of measure (the string paired with
// the value in the JSON input), hashed by field
struct hash_table *units_of_measure;

static double get_value(struct record *item, const char *field);

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
	if ( s == NULL )
		return NULL;
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
		/* cmdline.num_fields = 5; */
		cmdline.num_fields = 1;
		cmdline.fields = xxmalloc(cmdline.num_fields*sizeof(*cmdline.fields));
		cmdline.fields[0] = "wall_time";
		/* cmdline.fields[1] = "cpu_time"; */
		/* cmdline.fields[2] = "memory"; */
		/* cmdline.fields[3] = "disk"; */
		/* cmdline.fields[4] = "bytes_received"; */
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
struct hash_table *hash_by_field(struct list *list, const char *field) {
	struct hash_table *grouped = hash_table_create(0, 0);
	struct list *bucket;
	int num_groups = 0;
	int dropped_summaries = 0;

	list_first_item(list);
	for ( struct record *item; (item = list_next_item(list)) != 0; ) {
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

// Plots <output_field> vs. <work_units_processed>, regardless of
// split_key (e.g. host).
void write_vs_units_plots(struct hash_table *grouping, const char *category) {
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
				double value = get_value(item, cmdline.fields[f]);
				fprintf(out, OUTPUT_FIELD_SEPARATOR "%f", value);
				stats2_insert(&stat[f][UNITS_PROCESSED], item->work_units_processed, value);
				stats2_insert(&stat[f][UNITS_TOTAL], item->work_units_total, value);
			}
			fprintf(out, OUTPUT_RECORD_SEPARATOR);
		}
	}
	fclose(out);

	// Write a gnuplot script
	out = open_category_file(category, SUBDIR_PLOT, "vs_units.gp");
	for ( int f=0; f < cmdline.num_fields; ++f ) {
		const char *pretty_field = presentation_string(cmdline.fields[f]);
		for ( int u=0; u < 2; ++u ) {
			fprintf(out, "\n# %s vs. %s\n", pretty_field, display_string[u]);
			fprintf(out, "reset\nset terminal pngcairo enhanced size 1024,768\n");
			fprintf(out, "set tics font ',16'\n");
			fprintf(out, "set style line 1 lc rgb 'gray20' pt 7\n");
			fprintf(out, "set style line 2 lc rgb '#880000' lw 4\n");
			fprintf(out, "unset key\n");
			fprintf(out, "set yrange [0:]\n");
			fprintf(out, "set output '%s_vs_%s.png'\n", cmdline.fields[f], name_string[u]);
			fprintf(out, "set style fill transparent solid 0.1 noborder\n");
			fprintf(out, "set title '%s vs. %s  (%ld \"%s\" Tasks)' font ',22'\n",
							pretty_field, display_string[u], stat[f][u].count, category);
			fprintf(out, "set xlabel '%s' font ',20'\n", display_string[u]);
			double left = stat[f][u].min_x - 0.01*(stat[f][u].max_x - stat[f][u].min_x);
			fprintf(out, "set xrange [%f:%f]\n", left<0 ? left : 0, stat[f][u].max_x + 0.01*(stat[f][u].max_x - stat[f][u].min_x));
			const char *unit_string = hash_table_lookup(units_of_measure, cmdline.fields[f]);
			const char *original_unit = unit_string;
			if ( strcmp(unit_string, "MB") == 0 ) {
				// Convert MB to GB
				unit_string = "GB";
				fprintf(out, "convert_unit(y) = y/1024\n");
			} else if ( strcmp(unit_string, "s" ) == 0 ) {
				// Convert s to hr
				unit_string = "hr";
				fprintf(out, "convert_unit(y) = y/3600\n");
			} else {
				fprintf(out, "convert_unit(y) = y\n");
			}
			fprintf(out, "set ylabel '%s", pretty_field);
			if ( unit_string != NULL ) {
				fprintf(out, " (%s)", unit_string);
			}
			fprintf(out, "' font ',20'\n");
			fprintf(out, "set style circle radius %f\n", 0.01*stat[f][u].max_x);

			double a, b;
			const int have_regression = stats2_linear_regression(&stat[f][u], &a, &b);
			if ( have_regression ) {
				fprintf(out, "set label 1 \"{/Oblique y} = (%2$g %1$s/unit){/Oblique x} + (%3$g %1$s)\\n"
								"correlation %4$f\" at screen 0.52,0.17 left font ',18'\n",
								original_unit, a, b,	stats2_linear_correlation(&stat[f][u]));
			} else {
				fprintf(out, "set label 1 \"\"\n");
			}
			fprintf(out, "plot '%s%s" VSUNITS_NAME ".dat'", SUBDIR_DATA, SUBDIR_DATA[0] != '\0' ? "/" : "");
			fprintf(out, " using %d:(convert_unit($%d)) with circles ls 1 notitle", u+2, f+4);
			if ( have_regression ) {
				fprintf(out, ", \\\n\tconvert_unit(%f*x + %f) with lines ls 2 notitle\n", a, b);
			} else {
				fprintf(out, "\n");
			}

			// Plain version
			fprintf(out, "set terminal pngcairo enhanced size 512,384\n");
			fprintf(out, "set output 'thumb-%s_vs_%s.png'\n", cmdline.fields[f], name_string[u]);
			fprintf(out, "unset title\nunset tics\nunset xlabel\nunset ylabel\nunset label 1\n");
			fprintf(out, "set style fill transparent solid 0.3 noborder\n");
			fprintf(out, "set margins 0,0,0,0\n");
			fprintf(out, "set border lw 2\n");
			fprintf(out, "plot '%s%s" VSUNITS_NAME ".dat' using %d:(convert_unit($%d)) with circles ls 1 notitle\n",
							SUBDIR_DATA, SUBDIR_DATA[0] != '\0' ? "/" : "", u+2, f+4);
		}
	}
	fclose(out);
}

// Given two host names, return non-zero if they have the same prefix
// and only differ by a numeric (base 10) suffix, up to the first
// period.  Be careful when using this as it may group IP addresses
// undesirablely.
static int similar_hostnames(const char *a, const char *b) {
	// Skip matching prefix
	while ( *a != '\0' && *b != '\0' && *a != '.' && *b != '.' && *a == *b ) {
		a++;
		b++;
	}
	if ( (*a == '\0' || *a == '.') && (*b == '\0' || *b == '.') )
		return 1;  // same string

	// Advance past a possible numeric suffix
	while ( *a != '\0' && '0' <= *a && *a <= '9' )
		a++;
	while ( *b != '\0' && '0' <= *b && *b <= '9' )
		b++;
	if ( (*a == '\0' || *a == '.') && (*b == '\0' || *b == '.') )
		return 1;
	return 0;
}

struct linear_model {
	double slope, intercept;
};

static double use_linear_model(double x, void *model) {
	return ((struct linear_model *)model)->slope*x + ((struct linear_model *)model)->intercept;
}

void separate_host_groups(struct hash_table *grouping, const char *category) {
	// Merge groups of hosts with similar names
	struct hash_table *merged = hash_table_create(0, 0);
	struct list *value_list;
	hash_table_firstkey(grouping);
	for ( char *key; hash_table_nextkey(grouping, &key, (void **)&value_list); ) {
		int did_merge = 0;
		struct list *merged_list;
		hash_table_firstkey(merged);
		for ( char *merged_key; hash_table_nextkey(merged, &merged_key, (void **)&merged_list); ) {
			if ( similar_hostnames(key, merged_key) ) {
				// Add value_list into merged
				list_first_item(value_list);
				for ( void *item; (item = list_next_item(value_list)) != NULL; ) {
					list_push_tail(merged_list, item);
				}
				did_merge = 1;
				break;
			}
		}
		if ( !did_merge ) {
			// Add new entry to merged
			hash_table_insert(merged, key, list_duplicate(value_list));
		}
	}

	struct list *merged_list;
	hash_table_firstkey(merged);
	for ( char *merged_key; hash_table_nextkey(merged, &merged_key, (void **)&merged_list); ) {
		char *filename = string_format("group-%s.dat", merged_key);
		FILE *group_file = open_category_file(category, SUBDIR_DATA, filename);
		free(filename);

		// Header
		fprintf(group_file, OUTPUT_COMMENT "%s" OUTPUT_FIELD_SEPARATOR "task_id" OUTPUT_FIELD_SEPARATOR "units_processed" OUTPUT_FIELD_SEPARATOR "units", cmdline.split_field);
		for ( int f=0; f < cmdline.num_fields; ++f ) {
			fprintf(group_file, OUTPUT_FIELD_SEPARATOR "%s", cmdline.fields[f]);
			char *unit = hash_table_lookup(units_of_measure, cmdline.fields[f]);
			if ( unit != NULL )
				fprintf(group_file, "[%s]", unit);
		}
		fprintf(group_file, OUTPUT_RECORD_SEPARATOR);

		// Data
		list_first_item(merged_list);
		for ( struct record *item; (item = list_next_item(merged_list)) != NULL; ) {
			struct jx *value = jx_lookup(item->json, cmdline.split_field);
			if ( value == NULL || !jx_istype(value, JX_STRING) ) {
				fprintf(group_file, "?");
			} else {
				fprintf(group_file, "%s", value->u.string_value);
			}
			if ( (value = jx_lookup(item->json, FIELD_TASK_ID)) == NULL || !jx_istype(value, JX_STRING) ) {
				fprintf(group_file, OUTPUT_FIELD_SEPARATOR "?");
			} else {
				fprintf(group_file, OUTPUT_FIELD_SEPARATOR "%s", value->u.string_value);
			}
			fprintf(group_file, OUTPUT_FIELD_SEPARATOR "%d" OUTPUT_FIELD_SEPARATOR "%d", item->work_units_processed, item->work_units_total);
			for ( int f=0; f < cmdline.num_fields; ++f ) {
				fprintf(group_file, OUTPUT_FIELD_SEPARATOR "%g", get_value(item, cmdline.fields[f]));
			}
			fprintf(group_file, OUTPUT_RECORD_SEPARATOR);
		}
		fclose(group_file);
	}

	for ( int f=0; f < cmdline.num_fields; ++f ) {
		const char *const field = cmdline.fields[f];
		printf("Analysis of grouped %s\n", field);
		struct stats cumulative_y;
		struct stats2 cumulative_xy;
		stats_init(&cumulative_y);
		stats2_init(&cumulative_xy);

		// Linear fit data file
		char *filename = string_format("%s_vs_units-group.dat", field);
		FILE *out = open_category_file(category, SUBDIR_DATA, filename);
		free(filename);
		fprintf(out, OUTPUT_COMMENT "group(%s)" OUTPUT_FIELD_SEPARATOR "N" OUTPUT_FIELD_SEPARATOR "chi2/(N-2)" OUTPUT_FIELD_SEPARATOR "correlation", cmdline.split_field);
		char *field_unit = hash_table_lookup(units_of_measure, field);
		fprintf(out, OUTPUT_FIELD_SEPARATOR "slope[%s/unit]" OUTPUT_FIELD_SEPARATOR "intercept", field_unit ? field_unit : "1");
		if ( field_unit ) {
			fprintf(out, "[%s]", field_unit);
		}
		fprintf(out, OUTPUT_FIELD_SEPARATOR "outliers" OUTPUT_FIELD_SEPARATOR "refit_correlation");
		fprintf(out, OUTPUT_FIELD_SEPARATOR "refit_slope[%s/unit]" OUTPUT_FIELD_SEPARATOR "refit_intercept", field_unit ? field_unit : "1");
		if ( field_unit ) {
			fprintf(out, "[%s]", field_unit);
		}

		fprintf(out, OUTPUT_RECORD_SEPARATOR);
		hash_table_firstkey(merged);
		for ( char *key; hash_table_nextkey(merged, &key, (void **)&value_list); ) {
			if ( list_size(value_list) < 3 )
				continue;

			struct stats y;
			struct stats2 xy;
			stats2_init(&xy);
			stats_init(&y);

			// Feed data
			list_first_item(value_list);
			for ( struct record *item; (item = list_next_item(value_list)) != NULL; ) {
				double value = get_value(item, field);
				stats_insert(&y, value);
				stats2_insert(&xy, item->work_units_processed, value);
				stats_insert(&cumulative_y, value);
				stats2_insert(&cumulative_xy, item->work_units_processed, value);
			}

			// Fit to linear model
			struct linear_model model;
			if ( stats2_linear_regression(&xy, &model.slope, &model.intercept) ) {
				// Calculate residuals
				struct stats residuals;
				stats_init(&residuals);
				list_first_item(value_list);
				for ( struct record *item; (item = list_next_item(value_list)) != NULL; ) {
					stats_insert(&residuals, get_value(item, field) - use_linear_model(item->work_units_processed, &model));
				}

				// Print original fit results
				fprintf(out, "%s", key);
				fprintf(out, OUTPUT_FIELD_SEPARATOR "%ld", y.count);
				fprintf(out, OUTPUT_FIELD_SEPARATOR "%f", (residuals.sum_squares/stats_variance(&y))/(y.count - 2));
				fprintf(out, OUTPUT_FIELD_SEPARATOR "%f", stats2_linear_correlation(&xy));
				fprintf(out, OUTPUT_FIELD_SEPARATOR "%f", model.slope);
				fprintf(out, OUTPUT_FIELD_SEPARATOR "%f", model.intercept);

				// Throw away outliers and refit
				const double Q1 = stats_Q1(&residuals);
				const double Q3 = stats_Q3(&residuals);
				const double IQR = 1.5*abs(Q3 - Q1);
				struct stats2 refit_xy;
				stats2_init(&refit_xy);
				struct list *outliers = list_create();
				struct outlier {
					char *id;
					int x;
					double y;
					double residual;
				};
				list_first_item(value_list);
				for ( struct record *item; (item = list_next_item(value_list)) != NULL; ) {
					const double y_value = get_value(item, field);
					const double r = y_value - use_linear_model(item->work_units_processed, &model);
					if ( r >= Q1 - IQR && r <= Q3 + IQR ) {
						stats2_insert(&refit_xy, item->work_units_processed, y_value);
					} else {
						// Save outlier's task ID
						struct outlier *o = xxmalloc(sizeof(*o));
						struct jx *jx_id = jx_lookup(item->json, FIELD_TASK_ID);
						if ( jx_id->type != JX_STRING ) {
							o->id = "?";
						} else {
							o->id = jx_id->u.string_value;
						}
						o->x = item->work_units_processed;
						o->y = y_value;
						o->residual = r;
						list_push_tail(outliers, o);
					}
				}

				// Print refit results
				fprintf(out, OUTPUT_FIELD_SEPARATOR "%d", list_size(outliers));
				struct linear_model refit_model;
				if ( !stats2_linear_regression(&refit_xy, &refit_model.slope, &refit_model.intercept) ) {
					fprintf(out, OUTPUT_FIELD_SEPARATOR "NAN" OUTPUT_FIELD_SEPARATOR "NAN" OUTPUT_FIELD_SEPARATOR "NAN");
				} else {
					fprintf(out, OUTPUT_FIELD_SEPARATOR "%f", stats2_linear_correlation(&refit_xy));
					fprintf(out, OUTPUT_FIELD_SEPARATOR "%f", refit_model.slope);
					fprintf(out, OUTPUT_FIELD_SEPARATOR "%f", refit_model.intercept);
				}
				fprintf(out, OUTPUT_RECORD_SEPARATOR);

				// Print outliers file
				if ( list_size(outliers) > 0 ) {
					char *outlier_filename = string_format("%s_vs_units-outliers-%s.dat", field, key);
					FILE *outlier_file = open_category_file(category, SUBDIR_DATA, outlier_filename);
					free(outlier_filename);

					fprintf(outlier_file, OUTPUT_COMMENT "task_id" OUTPUT_FIELD_SEPARATOR "units_processed" OUTPUT_FIELD_SEPARATOR "%s", field);
					if ( field_unit ) {
						fprintf(outlier_file, "[%s]", field_unit);
					}
					fprintf(outlier_file, OUTPUT_FIELD_SEPARATOR "residual" OUTPUT_RECORD_SEPARATOR);

					list_first_item(outliers);
					for ( struct outlier *o; (o = list_next_item(outliers)) != NULL; ) {
						fprintf(outlier_file, "%s" OUTPUT_FIELD_SEPARATOR "%d" OUTPUT_FIELD_SEPARATOR "%g" OUTPUT_FIELD_SEPARATOR "%g" OUTPUT_RECORD_SEPARATOR,
										o->id, o->x, o->y, o->residual);
						free(o);
					}
					fclose(outlier_file);
				}

				list_delete(outliers);
				stats2_free(&refit_xy);
				stats_free(&residuals);
			}  // have first linear_regression
			stats_free(&y);
			stats2_free(&xy);
		}  // each key of merged
		struct linear_model model;
		if ( stats2_linear_regression(&cumulative_xy, &model.slope, &model.intercept) ) {
			// Calculate residuals
			struct stats residuals;
			stats_init(&residuals);
			hash_table_firstkey(merged);
			for ( char *key; hash_table_nextkey(merged, &key, (void **)&value_list); ) {
				list_first_item(value_list);
				for ( struct record *item; (item = list_next_item(value_list)) != NULL; ) {
					stats_insert(&residuals, get_value(item, field) - use_linear_model(item->work_units_processed, &model));
				}
			}
				
			fprintf(out, "(all)");
			fprintf(out, OUTPUT_FIELD_SEPARATOR "%ld", cumulative_y.count);
			fprintf(out, OUTPUT_FIELD_SEPARATOR "%f", (residuals.sum_squares/stats_variance(&cumulative_y))/(cumulative_y.count - 2));
			fprintf(out, OUTPUT_FIELD_SEPARATOR "%f", stats2_linear_correlation(&cumulative_xy));
			fprintf(out, OUTPUT_FIELD_SEPARATOR "%f", model.slope);
			fprintf(out, OUTPUT_FIELD_SEPARATOR "%f" OUTPUT_RECORD_SEPARATOR, model.intercept);
		}
		fclose(out);
		stats_free(&cumulative_y);
		stats2_free(&cumulative_xy);
	}  // each field

	// Clean up
	hash_table_firstkey(merged);
	for ( char *key; hash_table_nextkey(merged, &key, (void **)&value_list); ) {
		list_delete(value_list);
	}
	hash_table_delete(merged);
}

void unit_scale_by_host(struct hash_table *grouping, const char *category) {
	struct mordor *plot = mordor_create();
	FILE *out = open_category_file(category, SUBDIR_DATA, "unit_scale.dat");

	long considered_tasks = 0;
	struct list *item_list;
	hash_table_firstkey(grouping);
	for ( char *host; hash_table_nextkey(grouping, &host, (void **)&item_list); ) {
		int num_items = list_size(item_list);
		considered_tasks += num_items;
		if ( num_items < 3 )
			continue;

		fprintf(out, "%s" OUTPUT_FIELD_SEPARATOR "%d", host, num_items);
		struct stats2 xy;
		stats2_init(&xy);
		double *x = xxmalloc(num_items*sizeof(*x));
		double *y = xxmalloc(num_items*sizeof(*y));

		// Read values
		list_first_item(item_list);
		struct record *item;
		for ( int i=0; (item = list_next_item(item_list)) != NULL; ++i ) {
			x[i] = item->work_units_processed;
			y[i] = get_value(item, FIELD_WALL_TIME);
			stats2_insert(&xy, x[i], y[i]);
		}

		// Fit values to linear model and get residual
		struct linear_model model;
		if ( stats2_linear_regression(&xy, &model.slope, &model.intercept) ) {
			// Use linear model
			fprintf(out, OUTPUT_FIELD_SEPARATOR "%.6f" OUTPUT_FIELD_SEPARATOR "%.6f" OUTPUT_FIELD_SEPARATOR "%.6f",
							stats2_linear_correlation(&xy), model.slope, model.intercept);
			struct stats val;
			stats_init(&val);
			for ( int i=0; i < num_items; ++i ) {
				stats_insert(&val, (y[i] - model.intercept)/model.slope/x[i]);
			}
			const double mean = stats_mean(&val);
			for ( int i=0; i < val.count; ++i ) {
				const double ratio = val.values[i]/mean;
				mordor_insert(plot, host, ratio);
				fprintf(out, OUTPUT_FIELD_SEPARATOR "%f", ratio);
			}
			stats_free(&val);
		} else {
			// Sometimes (e.g. LHEGS) we cannot use a linear model, because
			// there is zero variance in work units (x).  In this case, we
			// can just scale by the mean of the y-values, without using a
			// linear model at all.
			fprintf(out, OUTPUT_FIELD_SEPARATOR "NAN" OUTPUT_FIELD_SEPARATOR "NAN" OUTPUT_FIELD_SEPARATOR "NAN");
			const double mean = stats2_mean_y(&xy);
			for ( int i=0; i < num_items; ++i ) {
				const double ratio = y[i]/mean;
				mordor_insert(plot, host, ratio);
				fprintf(out, OUTPUT_FIELD_SEPARATOR "%f", ratio);
			}
		}
		fprintf(out, "\n");

		// Clean up
		free(x);
		free(y);
		stats2_free(&xy);
	}
	fclose(out);

	// Write Mordor plot
	FILE *datafile = open_category_file(category, SUBDIR_DATA, "unit_scale.hist");
	FILE *gnuplot = open_category_file(category, SUBDIR_PLOT, "unit_scale.gp");
	char *pretty = presentation_string(cmdline.split_field);
	plot->title = string_format("Work Unit Scaling vs. %s for %ld \"%s\" Tasks", pretty, considered_tasks, category);
	plot->x_min = 0.0;
	plot->x_max = 2.0;
	mordor_plot(plot, "unit_scale.png", datafile, gnuplot, SUBDIR_DATA "/" "unit_scale.hist");
	mordor_delete(plot);
	free(pretty);
	fclose(gnuplot);
	fclose(datafile);
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

static double get_value(struct record *item, const char *field) {
	static int warned_inconsistent_units = 0;
	struct jx *jx_value = jx_lookup(item->json, field);
	if ( jx_value == NULL )
		return NAN;

	struct jx *jx_unit;
	if ( jx_value->type == JX_ARRAY ) {
		// Keep track of the unit of measure
		if ( units_of_measure != NULL ) {
			jx_unit = jx_array_index(jx_value, 1);
			if ( jx_unit != NULL && jx_unit->type == JX_STRING ) {
				char *previous_unit;
				if ( (previous_unit = hash_table_lookup(units_of_measure, field)) == NULL ) {
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
			return NAN;
	}
	if ( jx_value->type == JX_DOUBLE ) {
		return jx_value->u.double_value;
	} else if ( jx_value->type == JX_INTEGER ) {
		return (double)jx_value->u.integer_value;
	}
	fatal("Unexpected or unhandled JX type: %d", jx_value->type);
	return NAN; // failure
}

static double get_value_per_units(struct record *item, const char *field) {
	return get_value(item, field) / item->work_units_processed;
}

static double get_value_per_walltime(struct record *item, const char *field) {
	return get_value(item, field) / get_value(item, FIELD_WALL_TIME);
}

void plot_histograms(struct hash_table *grouping, char *category) {
	struct {
		// Function that returns a measurement, given a record and field
		double (*lambda)(struct record *, const char *);

		char *title;  // title suffix
		char *file;  // file name suffix
		char *divunit;  // "divided by" unit
		
		// Mordor histogram plot
		struct mordor *plot;
	} histograms[] = {
		{ .lambda = get_value },
		{ .lambda = get_value_per_units, .title = "/Work Unit", .file = "_per_unit", .divunit = "unit" },
		{ .lambda = get_value_per_walltime, .title = "/Wall Time", .file = "_per_wall_time", .divunit = hash_table_lookup(units_of_measure, FIELD_WALL_TIME) },
		{ .lambda = NULL }  // null terminator
	};

	// Create one set of plots for every output field
	char *pretty_split = presentation_string(cmdline.split_field);
	for ( int f=0; f < cmdline.num_fields; ++f ) {
		const char *field = cmdline.fields[f];
		char *pretty_field = presentation_string(field);

		// Create plots
		for ( int i=0; histograms[i].lambda; ++i ) {
			histograms[i].plot = mordor_create();
		}

		// Feed data
		struct list *value_list;
		hash_table_firstkey(grouping);
		for ( char *key; hash_table_nextkey(grouping, &key, (void **)&value_list); ) {
			list_first_item(value_list);
			for ( struct record *item; (item = list_next_item(value_list)) != NULL; ) {
				for ( int i=0; histograms[i].lambda; ++i ) {
					mordor_insert(histograms[i].plot, key, (*histograms[i].lambda)(item, field));
				}
			}
		}

		// Plot
		for ( int i=0; histograms[i].lambda; ++i ) {
			// Build file names
			char *data_name = string_format("%s%s.hist", field, histograms[i].file ? histograms[i].file : "");
			char *gnuplot_name = string_format("%s%s.gp", field, histograms[i].file ? histograms[i].file : "");
			char *png_name = string_format("%s%s%s%s.png", SUBDIR_PLOT, SUBDIR_PLOT[0] == '\0' ? "" : "/", field, histograms[i].file ? histograms[i].file : "");
			char *relative_name = string_format("%s%s%s", SUBDIR_DATA, SUBDIR_DATA[0] == '\0' ? "" : "/", data_name);

			struct mordor *plot = histograms[i].plot;

			char *field_unit = hash_table_lookup(units_of_measure, field);
			char *field_str;
			if ( histograms[i].divunit == NULL ) {
				// No division
				if ( field_unit == NULL ) {
					field_str = string_format("%s%s", pretty_field, histograms[i].title ? histograms[i].title : "");
				} else {
					field_str = string_format("%s%s (%s)", pretty_field, histograms[i].title ? histograms[i].title : "", field_unit);
				}
			} else {
				// Division by something
				if ( field_unit == NULL )
					field_unit = "1";
				if ( strcmp(field_unit, histograms[i].divunit) == 0 ) {
					field_str = string_format("%s%s", pretty_field, histograms[i].title ? histograms[i].title : "");
				} else {
					field_str = string_format("%s%s (%s/%s)", pretty_field, histograms[i].title ? histograms[i].title : "", field_unit, histograms[i].divunit);
				}
			}
			plot->title = string_format("%s vs. %s for %ld \"%s\" Tasks", field_str,
																	pretty_split, plot->cumulative_stats.count, category);

			// Open files and plot
			FILE *data = open_category_file(category, SUBDIR_DATA, data_name);
			FILE *gnuplot = open_category_file(category, SUBDIR_PLOT, gnuplot_name);
			mordor_plot(plot, png_name, data, gnuplot, relative_name);

			// Clean up
			free(plot->title);
			free(field_str);
			fclose(gnuplot);
			fclose(data);
			free(relative_name);
			free(png_name);
			free(gnuplot_name);
			free(data_name);
		}

		// Clean up
		for ( int i=0; histograms[i].lambda; ++i ) {
			mordor_delete(histograms[i].plot);
		}
		free(pretty_field);
	}
	free(pretty_split);
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

	// Query Lobster database if available for more information
	if ( cmdline.db_file != NULL ) {
		query_database_for_list(cmdline.db_file, summaries);
	}

	// Split by category
	struct hash_table *hashed_by_category = hash_by_field(summaries, FIELD_CATEGORY);

	// Track of measure encountered for each field
	units_of_measure = hash_table_create(0, 0);

	// Split by split_field (e.g. host)
	struct list *list_in_category;
	hash_table_firstkey(hashed_by_category);
	for ( char *category; hash_table_nextkey(hashed_by_category, &category, (void **)&list_in_category); ) {
		printf("Subdividing category \"%s\"...\n", category);
		struct hash_table *grouping = hash_by_field(list_in_category, cmdline.split_field);
		filter_by_threshold(grouping, cmdline.threshold);

		//----------------

		// Plot stuff
		plot_histograms(grouping, category);
		write_vs_units_plots(grouping, category);
		unit_scale_by_host(grouping, category);
		//separate_host_groups(grouping, category);

		//----------------

		// Delete grouping
		struct list *value_list;
		hash_table_firstkey(grouping);
		for ( char *key; hash_table_nextkey(grouping, &key, (void **)&value_list); ) {
			list_delete(value_list);
		}
		list_delete(list_in_category);
		hash_table_delete(grouping);
	}

	// Clean up
	hash_table_delete(hashed_by_category);
	hash_table_firstkey(units_of_measure);
	for ( char *field, *unit_str; hash_table_nextkey(units_of_measure, &field, (void **)&unit_str) != 0; ) {
		free(unit_str);
	}
	hash_table_delete(units_of_measure);

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
