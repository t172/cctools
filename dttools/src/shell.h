/*
Copyright (C) 2015- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef SHELL_H
#define SHELL_H

#include "buffer.h"

int shellcode (const char *cmd, const char * const env[], buffer_t *Bout, buffer_t *Berr, int *status);

#endif

/* vim: set noexpandtab tabstop=4: */