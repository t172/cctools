/*
Copyright (C) 2015- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>

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

#define CMDLINE_OPTS  "J:L:s:t:"
#define OPT_JSON      'J'
#define OPT_LIST      'L'
#define OPT_SPLIT     's'
#define OPT_THRESHOLD 't'

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
	.split_field = NULL,
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
	fprintf(stderr, "Usage:\n  %s [opts] -%c <field> (-%c <listfile> | -%c <jsonfile>) <outdir>\n\n", cmd, OPT_SPLIT, OPT_LIST, OPT_JSON);
	fprintf(stderr, "Options:\n");
	fprintf(stderr, "  -%c <listfile>   use summary list file\n", OPT_LIST);
	fprintf(stderr, "  -%c <jsonfile>   use JSON file with encoded summaries\n", OPT_JSON);
	fprintf(stderr, "  -%c <field>      split on <field>\n", OPT_SPLIT);
	fprintf(stderr, "  -%c <threshold>  set threshold to <threshold> matches\n", OPT_THRESHOLD);
}

void process_cmdline(int argc, char *argv[]) {
	int ch;
	while( (ch = getopt(argc, argv, CMDLINE_OPTS)) != -1 ) {
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
	if ( argc - optind < 1 ) {
		show_usage(argv[0]);
		exit(EXIT_FAILURE);
	}

	// Input file must be given
	if ( cmdline.infile_type == INFILE_UNDEF || cmdline.infile == NULL || cmdline.infile[0] == '\0' ) {
		fprintf(stderr, "No input file given (use -%c or -%c).\n", OPT_LIST, OPT_JSON);
		show_usage(argv[0]);
		exit(EXIT_FAILURE);
	}

	// Output directory must be given
	cmdline.output_dir = argv[optind];

	// Split field must be given
	if ( cmdline.split_field == NULL ) {
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

	long count = 0;
	struct jx *json;
	while ( (json = jx_parse_stream(jsonfile_f)) != NULL ) {
		struct record *new_record = xxmalloc(sizeof(*new_record));
		new_record->filename = NULL;
		new_record->json = json;
		list_push_tail(dst, new_record);
		count++;
	}
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
		warn(D_RMON, "Dropped %d of %d summaries when grouping by field \"%s\".\n", dropped_summaries, field);
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

// Opens an output file with the given name in a subdirectory created
// for the given category.
static FILE *open_category_file(const char *category, const char *filename) {
	char *outdir = string_format("%s/%s", cmdline.output_dir, category);
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

/* // Given a hash_table of lists (assuming all are in the same */
/* // category), writes the filenames of the summaries, dividing by keys */
/* // of the hash_table. */
/* void write_lists(struct hash_table *grouping, const char *category) { */
/* 	char *field; */
/* 	struct list *list; */

/* 	if ( category == NULL || category[0] == '\0' ) { */
/* 		warn(D_RMON, "No category given or empty string."); */
/* 		return; */
/* 	} */

/* 	hash_table_firstkey(grouping); */
/* 	while ( hash_table_nextkey(grouping, &field, (void **)&list) ) { */
/* 		FILE *listfile; */
/* 		struct record *item; */

/* 		char *filename = string_format("%s.list", field); */
/* 		listfile = open_category_file(category, filename); */

/* 		list_first_item(list); */
/* 		while ( (item = list_next_item(list)) != 0 ) { */
/* 			fprintf(listfile, "%s\n", item->filename); */
/* 		} */

/* 		fclose(listfile); */
/* 		free(filename); */
/* 	} */
/* } */

void write_avgs(struct hash_table *grouping, const char *category) {
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
		char *output_filename = string_format("%s.dat", output_field);
		hash_table_insert(output_file, output_field, open_category_file(category, output_filename));
		free(output_filename);

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

			evaluate_jx_value:
				switch ( jx_value->type ) {
				case JX_DOUBLE:
					value = jx_value->u.double_value;
					break;
				case JX_INTEGER:
					value = jx_value->u.integer_value;
					break;
				case JX_ARRAY:
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
			fprintf(outfile, OUTPUT_FIELD_SEPARATOR "%g", stats_Q1(stat));
			fprintf(outfile, OUTPUT_FIELD_SEPARATOR "%g", stats_median(stat));
			fprintf(outfile, OUTPUT_FIELD_SEPARATOR "%g", stats_Q3(stat));

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

void plot_category(struct hash_table *grouping, const char *category) {
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
		//write_lists(split_category, category);
		write_avgs(split_category, category);
		plot_category(split_category, category);

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
