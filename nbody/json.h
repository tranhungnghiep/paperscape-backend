#ifndef _INCLUDED_JSON_H
#define _INCLUDED_JSON_H

#include <stdbool.h>
#include "util/hashmap.h"
#include "common.h"

bool json_load_categories(const char *filename, category_set_t **category_set_out);
bool json_load_papers(const char *filename, category_set_t *category_set, int *num_papers_out, paper_t **papers_out, hashmap_t **keyword_set_out);
bool json_load_other_links(const char *filename, int num_papers, paper_t *papers);

#endif // _INCLUDED_JSON_H
