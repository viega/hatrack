/*
 * Copyright © 2021-2022 John Viega
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *  Name:           test.c
 *
 *  Description:    Main file for test case running, along with
 *                  the function that pre-computes hash keys.
 *
 *
 *  Author:         John Viega, john@zork.org
 *
 */

#include "testhat.h" // Will NOT be installed, so leave in quotes.

#include <hatrack/hash.h>

#include <time.h>
#include <stdio.h>

hatrack_hash_t *precomputed_hashes = NULL;
static uint64_t num_hashes         = 0;

// This is obviously meant to be run single-threaded.
void
precompute_hashes(uint64_t max_range)
{
    size_t alloc_size;

    if (num_hashes > max_range) {
        return;
    }

    alloc_size = sizeof(hatrack_hash_t) * max_range;

    if (!precomputed_hashes) {
        precomputed_hashes = (hatrack_hash_t *)malloc(alloc_size);
    }
    else {
        precomputed_hashes
            = (hatrack_hash_t *)realloc(precomputed_hashes, alloc_size);
    }

    for (; num_hashes < max_range; num_hashes++) {
        precomputed_hashes[num_hashes] = hash_int(num_hashes);
    }

    return;
}

int
main(int argc, char *argv[])
{
    config_info_t *config;

    config = parse_args(argc, argv);

#ifdef HATRACK_DEBUG
    print_config(config);
#endif

    mmm_register_thread();

    if (config->run_custom_test) {
        run_performance_test(&config->custom);
    }

    if (config->run_func_tests) {
        run_functional_tests(config);
    }

    if (config->run_default_tests) {
        run_default_tests(config);
    }

    counters_output_alltime();

    printf("Press <enter> to exit.\n");
    getc(stdin);

    return 0;
}
