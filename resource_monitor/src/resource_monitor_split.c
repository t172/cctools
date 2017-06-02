/*
Copyright (C) 2015- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/wait.h>

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

// What a category is called in the JSON summary data
#define FIELD_CATEGORY "category"

// How to separate fields in output (text) data files
#define OUTPUT_FIELD_SEPARATOR " "

// How to separate records in output (text) data files
#define OUTPUT_RECORD_SEPARATOR "\n"

// Name of gnuplot binary
#define GNUPLOT_BINARY "gnuplot"

// Filename for gnuplot scripts (one in each category's directory)
#define GNUPLOT_SCRIPT_FILENAME "plot.gp"

// Soft maxmimum on number of x-axis labels to put on a plot (if there
// are more than twice this amount, they will be culled)
#define GNUPLOT_SOFTMAX_XLABELS 40

#define CMDLINE_OPTS  "J:L:s:t:"
#define OPT_JSON      'J'
#define OPT_LIST      'L'
#define OPT_SPLIT     's'
#define OPT_THRESHOLD 't'

#define DEFAULT_SPLIT_FIELD "host"

#define TO_STR_EVAL(x) #x
#define TO_STR(x) TO_STR_EVAL(x)

static struct {
	char *infile;
	enum { INFILE_UNDEF, INFILE_LIST, INFILE_JSON	} infile_type;
	char *output_dir;
	char *split_field;
	int threshold;
	struct list *output_fields;
} cmdline = {
	.infile = NULL,
	.infile_type = INFILE_UNDEF,
	.output_dir = ".",
	.split_field = DEFAULT_SPLIT_FIELD,
	.threshold = 1,
	.output_fields = NULL
};

struct record {
	char *filename;
	struct jx *json;
};

int string_compare(const void *a, const void *b) {
	return strcmp(*(char *const *)a, *(char *const *)b);
}

void record_delete(struct record *r) {
	free(r->filename);
	jx_delete(r->json);
	free(r);
}

void show_usage(char *cmd) {
	fprintf(stderr, "Usage:\n  %s [opts] (-%c <listfile> | -%c <jsonfile>) <outdir>\n", cmd, OPT_LIST, OPT_JSON);
	fprintf(stderr, "\nRequired: (one of the following)\n");
	fprintf(stderr, "  -%c <jsonfile>   read file with JSON-encoded summaries\n", OPT_JSON);
	fprintf(stderr, "  -%c <listfile>   read file with list of summary pathnames\n", OPT_LIST);
	fprintf(stderr, "\nOptions:\n");
	fprintf(stderr, "  -%c <field>      split on <field> (default = \"%s\")\n", OPT_SPLIT, DEFAULT_SPLIT_FIELD);
	fprintf(stderr, "  -%c <threshold>  set threshold to <threshold> matches\n", OPT_THRESHOLD);
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
	if ( cmdline.output_fields == NULL ) {
		cmdline.output_fields = list_create();
		list_push_tail(cmdline.output_fields, xxstrdup("wall_time"));
		list_push_tail(cmdline.output_fields, xxstrdup("cpu_time"));
		list_push_tail(cmdline.output_fields, xxstrdup("memory"));
		list_push_tail(cmdline.output_fields, xxstrdup("virtual_memory"));
		list_push_tail(cmdline.output_fields, xxstrdup("disk"));
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
			// from jx_parse.h on jx_parse_file(): "If the parse fails or no JSON value is present, null is returned."
			skipped_summaries++;
			continue;
		}

		struct record *item = xxmalloc(sizeof(*item));
		item->filename = xxstrdup(filename);
		item->json = json;

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
		struct record *new_record = xxmalloc(sizeof(*new_record));
		new_record->filename = NULL;
		new_record->json = json;
		list_push_tail(dst, new_record);
		count++;
	}
	if ( parse_errors != 0 )
		warn(D_RMON, "Found %d errors parsing \"%s\".", parse_errors, jsonfile);
	printf("Stopped reading at position %ld\n", ftell(jsonfile_f));
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

// Filename for a specific output field's data.  Returns a string that
// must be freed.
inline static char *outfield_filename(const char *outfield) {
	return string_format("%s.dat", outfield);
}

// Name of directory in which to place stuff for a specific category.
// Returns a string that must be freed.
inline static char *category_directory(const char *category) {
	return string_format("%s/%s", cmdline.output_dir, category);
}

// Opens an output file with the given name in a subdirectory created
// for the given category.
static FILE *open_category_file(const char *category, const char *filename) {
	char *outdir = category_directory(category);
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

void write_avgs(struct hash_table *grouping, const char *category, struct hash_table *units) {
	// Find ways to give up
	if ( hash_table_size(grouping) == 0 )
		return;
	if ( category == NULL || category[0] == '\0' ) {
		warn(D_RMON, "No category given or empty string.");
		return;
	}
	if ( cmdline.output_fields == NULL ) {
		warn(D_RMON, "No output fields, so nothing to write");
		return;
	}

	const char *output_field;
	struct hash_table *output_file = hash_table_create(0, 0);
	struct stats *stat;
	struct hash_table *stats = hash_table_create(0, 0);

	list_first_item(cmdline.output_fields);
	while ( (output_field = list_next_item(cmdline.output_fields)) != 0 ) {
		// Create a file for each output field
		char *output_filename = outfield_filename(output_field);
		FILE *outfile = open_category_file(category, output_filename);
		hash_table_insert(output_file, output_field, outfile);
		free(output_filename);
		fprintf(outfile, "#%s summaries mean stddev whisker_low Q1 median Q3 whisker_high\n", cmdline.split_field);

		// Keep stats on each output field's values
		stat = xxcalloc(sizeof(*stat), 1);
		stats_init(stat);
		hash_table_insert(stats, output_field, stat);
	}

	// Build array of matching split keys and sort them
	int num_splits = hash_table_size(grouping);
	char **keys_sorted = xxmalloc(num_splits*sizeof(*keys_sorted));
	struct hash_table *value_list;
	char *split_key;
	int index = 0;
	hash_table_firstkey(grouping);
	while ( hash_table_nextkey(grouping, &split_key, (void **)&value_list) != 0 ) {
		keys_sorted[index++] = split_key;
	}
	qsort(keys_sorted, num_splits, sizeof(*keys_sorted), string_compare);

	int warned_inconsistent_units = 0;

	// Iterate through split keys in sorted order
	for ( index = 0; index < num_splits; ++index ) {
		split_key = keys_sorted[index];
		struct list *split_list;
		split_list = hash_table_lookup(grouping, split_key);

		// Dump values read into a file
		char *this_filename = string_format("%s.dat", split_key);
		FILE *this_match_file = open_category_file(category, this_filename);
		free(this_filename);

		// Iterate through items of split_list (records matching split_key)
		struct record *item;
		list_first_item(split_list);
		while ( (item = list_next_item(split_list)) != 0 ) {
			// Lookup value for each output field
			list_first_item(cmdline.output_fields);
			while ( (output_field = list_next_item(cmdline.output_fields)) != 0 ) {
				double value = 0.0;
				struct jx *jx_value = jx_lookup(item->json, output_field);
				if ( jx_value == NULL )
					continue;

				struct jx *jx_unit;
			evaluate_jx_value:
				switch ( jx_value->type ) {
				case JX_DOUBLE:
					value = jx_value->u.double_value;
					break;
				case JX_INTEGER:
					value = jx_value->u.integer_value;
					break;
				case JX_ARRAY:
					// Keep track of the unit of measure
					jx_unit = jx_array_index(jx_value, 1);
					if ( jx_unit != NULL && jx_unit->type == JX_STRING ) {
						char *previous_unit = hash_table_lookup(units, output_field);
						if ( previous_unit == NULL ) {
							hash_table_insert(units, output_field, xxstrdup(jx_unit->u.string_value));
						} else {
							if ( strcmp(previous_unit, jx_unit->u.string_value) != 0 && !warned_inconsistent_units ) {
								warn(D_RMON, "Encountered inconsistent units for \"%s\": \"%s\" and \"%s\".",
										 output_field, previous_unit, jx_unit->u.string_value);
								warned_inconsistent_units = 1;
							}
						}
					}

					// First is the value
					jx_value = jx_array_index(jx_value, 0);
					if ( jx_value == NULL )
						continue;
					goto evaluate_jx_value;
					break;
				default:
					continue;
				}

				// Now we have a value, use it
				fprintf(this_match_file, OUTPUT_FIELD_SEPARATOR "%g", value);
				stat = hash_table_lookup(stats, output_field);
				stats_process(stat, value);
			}
			fprintf(this_match_file, OUTPUT_RECORD_SEPARATOR);
		}
		fclose(this_match_file);

		// Print results
		list_first_item(cmdline.output_fields);
		while ( (output_field = list_next_item(cmdline.output_fields)) != 0 ) {
			// Each output field's aggregated data goes to a separate file
			FILE *outfile = hash_table_lookup(output_file, output_field);

			// Print matching field (split_key) and number of matches
			fprintf(outfile, "%s", split_key);
			fprintf(outfile, OUTPUT_FIELD_SEPARATOR "%d", list_size(split_list));

			// Print statistics
			stat = hash_table_lookup(stats, output_field);
			fprintf(outfile, OUTPUT_FIELD_SEPARATOR "%g", stats_mean(stat));
			fprintf(outfile, OUTPUT_FIELD_SEPARATOR "%g", stats_stddev(stat));
			fprintf(outfile, OUTPUT_FIELD_SEPARATOR "%g", stats_whisker_low(stat));
			fprintf(outfile, OUTPUT_FIELD_SEPARATOR "%g", stats_Q1(stat));
			fprintf(outfile, OUTPUT_FIELD_SEPARATOR "%g", stats_median(stat));
			fprintf(outfile, OUTPUT_FIELD_SEPARATOR "%g", stats_Q3(stat));
			fprintf(outfile, OUTPUT_FIELD_SEPARATOR "%g", stats_whisker_high(stat));

			// Re-initialize to zero for next iteration
			stats_reset(stat);
			fprintf(outfile, OUTPUT_RECORD_SEPARATOR);
		}
	}  // each split_key

	// Clean up
	free(keys_sorted);
	list_first_item(cmdline.output_fields);
	while ( (output_field = list_next_item(cmdline.output_fields)) != 0 ) {
		fclose(hash_table_lookup(output_file, output_field));
		free(hash_table_lookup(stats, output_field));
	}
	hash_table_delete(output_file);
	hash_table_delete(stats);
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

// Writes the gnuplot script for one specific output field
void plotscript_outfield(FILE *f, const char *outfield, struct hash_table *units) {
	char *pretty_outfield = presentation_string(outfield);
	char *pretty_splitfield = presentation_string(cmdline.split_field);
	char *data_filename = outfield_filename(outfield);

	fprintf(f, "\n# %s\n", outfield);
	fprintf(f, "set output '%s.png'\n", outfield);
	fprintf(f, "set title '{/=16 %s vs. %s'.title_suffix.'}'\n", pretty_outfield, pretty_splitfield);

	// Reduce the number of xtic labels
	fprintf(f, "xskip = 1; n = system(\"wc -l %s\")\n", data_filename);
	fprintf(f, "if (n > " TO_STR(GNUPLOT_SOFTMAX_XLABELS) ") xskip = int(n/" TO_STR(GNUPLOT_SOFTMAX_XLABELS) ")\n");

	// Determine unit of measure
	char *unit = hash_table_lookup(units, outfield);
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

int write_plotscript_for_category(struct hash_table *grouping, const char *category, struct hash_table *units) {
	// Find ways to give up
	if ( hash_table_size(grouping) == 0 )
		return 0;
	if ( category == NULL || category[0] == '\0' ) {
		warn(D_RMON, "No category given or empty string.");
		return 0;
	}
	if ( cmdline.output_fields == NULL ) {
		warn(D_RMON, "No output fields, so nothing to write");
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

	FILE *gnuplot_script = open_category_file(category, GNUPLOT_SCRIPT_FILENAME);
	fprintf(gnuplot_script, "set terminal pngcairo enhanced color size 1072,768\n");
	fprintf(gnuplot_script, "set key off\n");
	fprintf(gnuplot_script, "set xtics nomirror rotate by 60 right\n");
	fprintf(gnuplot_script, "set style line 1 lc rgb 'black'\n");
	fprintf(gnuplot_script, "set style line 2 lc rgb 'gray50'\n");
	fprintf(gnuplot_script, "title_suffix = '\t(%ld \"%s\" Summaries)'\n", num_summaries, category);

	char *pretty_splitfield = presentation_string(cmdline.split_field);
	fprintf(gnuplot_script, "set xlabel '{/=14 %s  (%d total)}'\n", pretty_splitfield, hash_table_size(grouping));
	free(pretty_splitfield);

	char *output_field;
	list_first_item(cmdline.output_fields);
	while ( (output_field = list_next_item(cmdline.output_fields)) != 0 ) {
		plotscript_outfield(gnuplot_script, output_field, units);
	}
	return 1;
}

void plot_category(struct hash_table *grouping, const char *category, struct hash_table *units) {
	if ( !write_plotscript_for_category(grouping, category, units) )
		return;

	/* printf("Plotting category \"%s\"\n", category); */
	/* errno = 0; */
	/* pid_t pid; */
	/* if ( (pid = fork()) < 0 ) { // error */
	/* 	fatal("Cannot fork process: %s", strerror(errno)); */
	/* } else if ( pid > 0 ) { // child */
	/* 	// Change to directory where category stuff goes */
	/* 	char *dir = category_directory(category); */
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
		printf("Subdividing category \"%s\"...\n", category);
		struct hash_table *split_category = group_by_field(list_in_category, cmdline.split_field);
		filter_by_threshold(split_category, cmdline.threshold);

		// Keep track of units encountered
		struct hash_table *units = hash_table_create(0, 0);

		// Calculate statistics, write output files, and plot
		write_avgs(split_category, category, units);
		plot_category(split_category, category, units);

		// Free values from hash tables
		char *output_field, *unit_str;
		hash_table_firstkey(units);
		while ( hash_table_nextkey(units, &output_field, (void **)&unit_str) != 0 )
			free(unit_str);
		hash_table_delete(units);

		struct list *split_list;
		char *split_field;
		hash_table_firstkey(split_category);
		while ( hash_table_nextkey(split_category, &split_field, (void **)&split_list) ) {
			list_delete(split_list);
		}

		list_delete(list_in_category);
		hash_table_delete(split_category);
	}

	// Clean up
	hash_table_delete(grouped_by_category);
	list_free(cmdline.output_fields);
	list_delete(cmdline.output_fields);

	// Clean up list of all summaries
	struct record *item;
	list_first_item(summaries);
	while ( (item = list_next_item(summaries)) != 0 )
		record_delete(item);
	list_delete(summaries);

	return EXIT_SUCCESS;
}

//EOF