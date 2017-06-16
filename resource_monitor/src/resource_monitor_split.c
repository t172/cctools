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

// The previously encountered unit of measure (string), hashed by
// output field
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
		cmdline.num_fields = 5;
		cmdline.fields = xxmalloc(cmdline.num_fields*sizeof(*cmdline.fields));
		cmdline.fields[0] = "wall_time";
		cmdline.fields[1] = "cpu_time";
		cmdline.fields[2] = "memory";
		cmdline.fields[3] = "disk";
		cmdline.fields[4] = "bytes_received";
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
			fprintf(out, "set title '%s vs. %s  (%ld \"%s\" Tasks)' font ',22'\n",
							pretty_field, display_string[u], stat[f][u].count, category);
			fprintf(out, "set xlabel '%s' font ',20'\n", display_string[u]);
			double left = stat[f][u].min_x - 0.01*(stat[f][u].max_x - stat[f][u].min_x);
			fprintf(out, "set xrange [%f:%f]\n", left<0 ? left : 0, stat[f][u].max_x + 0.01*(stat[f][u].max_x - stat[f][u].min_x));
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
			fprintf(out, "set style circle radius %f\n", 0.01*stat[f][u].max_x);

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
				fprintf(out, ", \\\n\tconvert_unit(%f*x + %f) with lines ls 2 notitle\n", a, b);
			} else {
				fprintf(out, "\n");
			}
		}
	}
	fclose(out);
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

		// Plot stuff
		plot_histograms(grouping, category);
		write_vs_units_plots(grouping, category);

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
