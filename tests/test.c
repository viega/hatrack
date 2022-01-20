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
 *  Description:    Test cases, and code to support tests.
 *
 *                  This is currently messy; I would love to
 *                  find the time to put some real work into this.
 *
 *
 *  Author:         John Viega, john@zork.org
 *
 */

#include "testhat.h" // Will NOT be installed, so leave in quotes.

#include <hatrack/hash.h>

#include <time.h>
#include <stdio.h>


hatrack_hash_t      precomputed_hashes[HATRACK_TEST_MAX_KEYS];
_Atomic test_func_t test_func;

static void
test_precompute_hashes(void)
{
    uint64_t i;

    for (i = 0; i < HATRACK_TEST_MAX_KEYS; i++) {
        precomputed_hashes[i] = hash_int(i);
    }

    return;
}

static void
test_init(void)
{
    mmm_register_thread();
    test_precompute_hashes();
    test_init_rand();

    return;
}

void *
start_one_thread(void *info)
{
    test_func_t func;
    bool        ret;

    mmm_register_thread();

    while (!(func = atomic_load(&test_func)))
        ;

    ret = (*test_func)(info);

    mmm_clean_up_before_exit();

    return (void *)ret;
}


// clang-format off
uint32_t            basic_sizes[]     = {10, 100, 1000, 10000, 0};
uint32_t            sort_sizes[]      = {10, 128, 256, 512, 1024, 2048, 4096,
                                         8192, 100000, 0};
uint32_t            large_sizes[]     = {100000, 1000000, 0};
uint32_t            shrug_sizes[]     = {1, 0};
uint32_t            small_size[]      = {10, 0};
uint32_t            one_thread[]      = {1, 0};
uint32_t            mt_only_threads[] = {2, 4, 8, 20, 100, 0};
uint32_t            basic_threads[]   = {1, 2, 4, 8, 20, 100, 0};
uint32_t            del_rate[]        = {100, 10, 3, 0};
uint32_t            write_rates[]     = {0x010a, 0x050a, 0x0a0a, 0};
//  clang-format on

int
main(int argc, char *argv[])
{
    config_info_t *config;

    config = parse_args(argc, argv);

#ifdef HATRACK_DEBUG
    print_config(config);
#endif
    
    test_init();

    if (config->run_func_tests) {
	run_functional_tests(config);
    }

    if (config->run_stress_tests) {
	run_stress_tests(config);
    }
    
    counters_output_alltime();
    
    printf("Press <enter> to exit.\n");
    getc(stdin);

    return 0;
}
