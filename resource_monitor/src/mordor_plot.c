
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <limits.h>
#include <errno.h>

#include "mordor.h"

#define CMDLINE_OPTS "F:d:g:i:o:t:v"
#define OPT_DELIMS   'F'
#define OPT_HISTDATA 'd'
#define OPT_GNUPLOT  'g'
#define OPT_INFILE   'i'
#define OPT_OUTFILE  'o'
#define OPT_TITLE    't'
#define OPT_VERBOSE  'v'

struct {
	char *gnuplot_script;
	char *histogram_data;
	char *infile;
	char *outfile;
	char *title;
	char *delims;
	int verbose;
} cmdline = {
	.gnuplot_script = "plot.gp",
	.histogram_data = "plot.dat",
	.infile = "/dev/stdin",
	.outfile = "/dev/stdout",
	.title = NULL,
	.delims = " \t",
	.verbose = 0
};

#define FATAL(msg, ...) do {					 \
	fprintf(stderr, msg "\n", ##__VA_ARGS__); \
	exit(EXIT_FAILURE); } while (0)

void show_usage(const char *cmd) {
	fprintf(stderr, "Usage:\n  %s -i <infile> -o <outfile> <label_column> <value_column>\n", cmd);
}

int toInt(const char *str) {
	char *end;
	long value;
	errno = 0;
	value = strtol(str, &end, 10);
	if ( errno != 0 || value >= INT_MAX || value <= INT_MIN )
		FATAL("Invalid value: %s", str);
	return value;
}

int main(int argc, char *argv[]) {
	int ch;
	while ( (ch = getopt(argc, argv, CMDLINE_OPTS)) != -1 ) {
		switch ( ch ) {
		case OPT_DELIMS:
			cmdline.delims = optarg;
			break;
		case OPT_TITLE:
			cmdline.title = optarg;
			break;
		case OPT_HISTDATA:
			cmdline.histogram_data = optarg;
			break;
		case OPT_GNUPLOT:
			cmdline.gnuplot_script = optarg;
			break;
		case OPT_INFILE:
			cmdline.infile = optarg;
			break;
		case OPT_OUTFILE:
			cmdline.outfile = optarg;
			break;
		case OPT_VERBOSE:
			cmdline.verbose = 1;
			break;
		default:
			show_usage(argv[0]);
			FATAL("Invalid option: %c", (char)optopt);
			break;
		}
	}
	if ( argc - optind != 2 ) {
		show_usage(argv[0]);
		exit(EXIT_FAILURE);
	}

	// Validate column numbers
	int label_column = toInt(argv[optind + 0]);
	int value_column = toInt(argv[optind + 1]);
	if ( label_column == value_column )
		FATAL("Labels and values must be different columns");
	if ( label_column <= 0 )
		FATAL("Label column must be positive: %d", label_column);
	if ( value_column <= 0 )
		FATAL("Value column must be positive: %d", value_column);

	// Open input file
	FILE *in;
	if ( (in = fopen(cmdline.infile, "r")) == NULL )
		FATAL("Can't open input file for reading: %s", argv[3]);

	// Open histogram data file
	FILE *data_file;
	if ( (data_file = fopen(cmdline.histogram_data, "w")) == NULL )
		FATAL("Can't open histogram data file for writing: %s", cmdline.histogram_data);

	// Open gnuplot script file
	FILE *script_file;
	if ( (script_file = fopen(cmdline.gnuplot_script, "w")) == NULL )
		FATAL("Can't open gnuplot script for writing: %s", cmdline.gnuplot_script);

	// Initialize plot
	struct mordor *plot = mordor_create();
	if ( cmdline.title != NULL )
		plot->title = cmdline.title;

	// Read input file
	long skipped_lines = 0;
	char *line;
	size_t line_len;
	for ( long line_num=1; getline(&line, &line_len, in) != -1; ++line_num ) {
		char *tok;
		char *label = NULL;
		double value;
		int have_value = 0;
		int column = 0;

		if ( (tok = strtok(line, cmdline.delims)) == NULL ) {
			if ( cmdline.verbose )
				fprintf(stderr, "Skipping line %ld (empty)\n", line_num);
			skipped_lines++;
			continue;
		}
		do {
			column++;
			if ( column == label_column ) {
				// Take label
				label = tok;
			} else if ( column == value_column ) {
				// Take value
				char *endptr = NULL;
				errno = 0;
				value = strtod(tok, &endptr);
				if ( errno == ERANGE ) {
					if ( cmdline.verbose )
						fprintf(stderr, "Skipping line %ld with value out of range: %s\n", line_num, tok);
					skipped_lines++;
					goto next_line;
				}
				if ( value == 0 && endptr != NULL ) {
					if ( cmdline.verbose ) {
						if ( errno != 0 )
							fprintf(stderr, "%s:%ld: %s\n", cmdline.infile, line_num, strerror(errno));
						fprintf(stderr, "Skipping line %ld with invalid value: %s\n", line_num, tok);
					}
					skipped_lines++;
					goto next_line;
				}
				have_value = 1;
			}
		}	while ( (label == NULL || !have_value) && (tok = strtok(NULL, cmdline.delims)) != NULL );

		if ( label == NULL ) {
			if ( cmdline.verbose )
				fprintf(stderr, "Skipping line %ld without a label (%d columns, need %d)\n", line_num, column, label_column);
			skipped_lines++;
			goto next_line;
		}
		if ( !have_value ) {
			if ( cmdline.verbose )
				fprintf(stderr, "Skipping line %ld without a value (%d columns, need %d)\n", line_num, column, value_column);
			skipped_lines++;
			goto next_line;
		}

		// Insert value into plot
		mordor_insert(plot, label, value);

	next_line:
		continue;
	}

	if ( skipped_lines > 0 ) {
		fprintf(stderr, "Warning: Skipped %ld lines due to errors.\n", skipped_lines);
		if ( !cmdline.verbose )
			fprintf(stderr, "(Use -%c for more)\n", OPT_VERBOSE);
	}

	if ( cmdline.verbose )
		fprintf(stderr, "Writing histogram data and gnuplot script...\n");
	mordor_plot(plot, cmdline.outfile, data_file, script_file, cmdline.histogram_data);
		
	free(line);
	mordor_delete(plot);
	fclose(script_file);
	fclose(data_file);
	fclose(in);
	return EXIT_SUCCESS;
}
