/* Copyright © 2021-2022 John Viega
 *
 * See LICENSE.txt for licensing info.
 *
 *  Name:           config.c
 *
 *  Description:    Command-line argument parsing.
 *
 *  Author:         John Viega, john@zork.org
 */

#include <stdlib.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <testhat.h>

#define S_WITH        "with"
#define S_WO          "without"
#define S_FUNC        "functional-tests"
#define S_DEFAULT     "run-default-tests"
#define S_READ_PCT    "read-pct"
#define S_PUT_PCT     "put-pct"
#define S_ADD_PCT     "add-pct"
#define S_REPLACE_PCT "replace-pct"
#define S_REMOVE_PCT  "remove-pct"
#define S_VIEW_PCT    "view-pct"
#define S_SORT_PCT    "sort-pct"
#define S_START_SIZE  "start-size"
#define S_PREFILL_PCT "prefill-pct"
#define S_NUM_THREADS "num-threads"
#define S_TOTAL_OPS   "total-ops"
#define S_KEY_RANGE   "num-keys"
#define S_SHUFFLE     "no-rand"
#define S_SEED        "seed"
#define S_HELP        "help"

#define HATRACK_DEFAULT_READ     98
#define HATRACK_DEFAULT_PUT      1
#define HATRACK_DEFAULT_ADD      0
#define HATRACK_DEFAULT_REPLACE  0
#define HATRACK_DEFAULT_REMOVE   1
#define HATRACK_DEFAULT_VIEW     0
#define HATRACK_DEFAULT_SORT     0
#define HATRACK_DEFAULT_SEED     0 // Random.
#define HATRACK_DEFAULT_START_SZ HATRACK_MIN_SIZE
#define HATRACK_DEFAULT_PREFILL  50
#define HATRACK_DEFAULT_OPS      100000
#define HATRACK_DEFAULT_NUM_KEYS 1000

enum
{
    OPT_DEFAULT,
    OPT_YES,
    OPT_NO
};

typedef struct {
    char *name;
    int   status;
} config_hat_info_t;

static char              *prog_name;
static config_hat_info_t *hat_info;

static int
prepare_hat_info(void)
{
    alg_info_t *info;
    uint32_t    i, n;

    info     = get_all_algorithm_info();
    n        = get_num_algorithms();
    hat_info = (config_hat_info_t *)calloc(n, sizeof(config_hat_info_t));

    for (i = 0; i < n; i++) {
        hat_info[i].name   = info[i].name;
        hat_info[i].status = OPT_DEFAULT;
    }

    return n;
}

static void
config_init(config_info_t *config)
{
    config->run_default_tests  = true;
    config->run_func_tests     = false;
    config->run_custom_test    = true;
    config->custom.read_pct    = HATRACK_DEFAULT_READ;
    config->custom.put_pct     = HATRACK_DEFAULT_PUT;
    config->custom.add_pct     = HATRACK_DEFAULT_ADD;
    config->custom.replace_pct = HATRACK_DEFAULT_REPLACE;
    config->custom.remove_pct  = HATRACK_DEFAULT_REMOVE;
    config->custom.view_pct    = HATRACK_DEFAULT_VIEW;
    config->custom.sort_pct    = HATRACK_DEFAULT_SORT;
    config->custom.start_sz    = HATRACK_DEFAULT_START_SZ;
    config->custom.prefill_pct = HATRACK_DEFAULT_PREFILL;
    config->custom.key_range   = HATRACK_DEFAULT_NUM_KEYS;
    config->custom.num_threads = sysconf(_SC_NPROCESSORS_ONLN);
    config->custom.total_ops   = HATRACK_DEFAULT_OPS;
    config->custom.hat_list    = config->hat_list;
    config->custom.shuffle     = false;
    config->custom.seed        = 0;

    return;
}

// Really this will be 78, but we're going to indent by two characters.
#define MAX_WIDTH 76

static void
output_algorithm_list(void)
{
    uint32_t col_width;
    uint32_t i;
    uint32_t len;
    uint32_t cols_per_row;
    uint32_t num_hats;

    col_width = 0;
    num_hats  = get_num_algorithms();

    // Scan through the list to find the maximum width.
    for (i = 0; i < num_hats; i++) {
        len = strlen(hat_info[i].name);
        if (len > col_width) {
            col_width = len;
        }
    }
    // Add 1 to the column width for a leading space.
    col_width++;

    // How many columns fit to this width?
    cols_per_row = MAX_WIDTH / col_width;

    for (i = 0; i < num_hats; i++) {
        if (!(i % cols_per_row)) {
            fprintf(stderr, "\n  ");
        }
        len = strlen(hat_info[i].name);
        fprintf(stderr, "%s", hat_info[i].name);
        while (len < col_width) {
            fputc(' ', stderr);
            len++;
        }
    }
    fprintf(stderr, "\n");
}

_Noreturn static void
usage(void)
{
    fprintf(stderr, "Usage: %s: [options]*\n", prog_name);
    fprintf(stderr, "\nOptions:\n");
    fprintf(stderr, "  --with [algorithm]+ | --without [algorithm]+ \n");
    fprintf(stderr, "  --functional-tests (Run functionality tests)\n");
    fprintf(stderr,
            "  --run-default-tests (Run default performance tests when running"
            "\nother test types)");
    fprintf(stderr, "\n\nFlags for a custom performance test:\n");
    fprintf(stderr,
            "  --read-pct=<int> (%% of ops to be reads; DEFAULT: %d)\n",
            HATRACK_DEFAULT_READ);
    fprintf(stderr,
            "  --put-pct=<int> (%% of ops to be puts; DEFAULT: %d)\n",
            HATRACK_DEFAULT_PUT);
    fprintf(stderr,
            "  --add-pct=<int> (%% of ops to be adds; DEFAULT: %d)\n",
            HATRACK_DEFAULT_ADD);
    fprintf(stderr,
            "  --replace-pct=<int> (%% of ops to be replaces; DEFAULT: %d)\n",
            HATRACK_DEFAULT_REPLACE);
    fprintf(stderr,
            "  --remove-pct=<int> (%% of ops to be removes; DEFAULT: %d)\n",
            HATRACK_DEFAULT_REMOVE);
    fprintf(stderr,
            " --view-pct=<int> (%% of ops to be views; DEFAULT: %d)\n",
            HATRACK_DEFAULT_VIEW);
    fprintf(stderr,
            " --sort-pct=>int> (%% of ops to be sorted views; DEFAULT: %d)\n",
            HATRACK_DEFAULT_SORT);
    fprintf(stderr,
            "  --start-size=<int> (Starting table size as a power of 2; "
            "  DEFAULT: %d)\n",
            HATRACK_DEFAULT_START_SZ);
    fprintf(stderr,
            "  --prefill-pct=<int> (%% of start size to pre-fill before test; "
            "DEFAULT: %d)\n",
            HATRACK_DEFAULT_PREFILL);
    fprintf(stderr,
            "  --num-threads=<int> (Number of threads to run; DEFAULT: %ld)\n",
            sysconf(_SC_NPROCESSORS_ONLN));
    fprintf(stderr,
            "  --total-ops=<int> (# of operations to run; "
            "DEFAULT: %d)\n",
            HATRACK_DEFAULT_OPS);
    fprintf(stderr,
            "  --num-keys=<int> (Max # of unique keys / key range; "
            "DEFAULT: %d)\n",
            HATRACK_DEFAULT_NUM_KEYS);
    fprintf(stderr,
            "  --no-rand (Don't call rand() during test; pre-shuffle"
            " ops before.)\n");
    fprintf(stderr,
            "  --seed=<hex-digits> (Set a seed for the rng; "
            "implies --no-rand)\n\n");

    fprintf(stderr, "When you pass --functional-tests or any of the flags ");
    fprintf(stderr, "for a custom\nperformance test, the default stress ");
    fprintf(stderr, "tests will NOT run\nUNLESS you pass --run-default-tests");
    fprintf(stderr, "\n\n");
    fprintf(stderr, "When specifying algorithms, use spaces between the names");
    fprintf(stderr, ", or pass flags\nmultiple times.\n\n");
    fprintf(stderr, "If you pass a test type without arguments, we assume ");
    fprintf(stderr, "you want that type on.\nIf all test type flags passed ");
    fprintf(stderr, "are of the same value, then\nunspecified values are ");
    fprintf(stderr, "assumed to be of the opposite type.\n\n");
    fprintf(stderr, "We use similar logic to figure out whether we should ");
    fprintf(stderr, "include unspecified\nalgorithms, but you can use the ");
    fprintf(stderr, "--other-tables flag to be explicit\nabout it.\n\n");
    fprintf(stderr, "If you supply an RNG seed, it is interpreted as a ");
    fprintf(stderr, "128-bit hex value, but\ndo NOT put on a trailing 0x.\n\n");
    fprintf(stderr, "Currently supported algorithms:");
    output_algorithm_list();

    exit(1);
}

static void
validate_operational_mix(benchmark_t *config)
{
    unsigned int sum;

    sum = config->read_pct + config->put_pct + config->add_pct
        + config->replace_pct + config->remove_pct + config->view_pct
        + config->sort_pct;

    if (sum != 100) {
        fprintf(stderr,
                "For performance tests, specified percentages must equal 100.");
        fprintf(stderr, " (current sum: %d)\n", sum);

#ifdef HATRACK_DEBUG
        fprintf(stderr, "\n");
        print_config(config);
#endif

        usage();
    }

    return;
}

static void
validate_config(config_info_t *config)
{
    if (!config->run_custom_test && !config->run_func_tests
        && !config->run_default_tests) {
        fprintf(stderr, "No tests specified.\n");
        usage();
    }

    if (config->run_custom_test) {
        validate_operational_mix(&config->custom);

        if (config->custom.start_sz > 32) {
            fprintf(stderr, "Max prealloc value is 32 (i.e., 2^32 entires)\n");
            usage();
        }

        if (config->custom.start_sz < HATRACK_MIN_SIZE_LOG) {
            fprintf(stderr,
                    "Minimum prealloc size value %d (i.e., 2^%d entires)\n",
                    HATRACK_MIN_SIZE_LOG,
                    HATRACK_MIN_SIZE_LOG);
        }

        if (config->custom.prefill_pct > 100) {
            fprintf(stderr, "Prefill percentage should be no more than 100.\n");
            usage();
        }

        if (!config->custom.num_threads) {
            fprintf(stderr, "Invalid number of threads.\n");
            usage();
        }
    }

    return;
}

static void
ensure_unspecd(bool *status, char *name)
{
    if (*status) {
        fprintf(stderr, "Error: multiple appearances of flag: --%s\n", name);
        usage();
    }

    *status = true;

    return;
}

#define MAX_HEX_CHARS 32

static bool
try_parse_seed_arg(char *p, char *flag_name, bool *b, __int128_t *seedp)
{
    __int128_t seed;
    int        num_chars;

    seed      = 0;
    num_chars = 0;

    if (strncmp(p, flag_name, strlen(flag_name))) {
        return false;
    }

    if (*p++ != '=') {
        usage();
    }

    ensure_unspecd(b, flag_name);

    do {
        switch (*p) {
        case 0:
            usage(); // Only runs for the first iteration.
        case '0':
        case '1':
        case '2':
        case '3':
        case '4':
        case '5':
        case '6':
        case '7':
        case '8':
        case '9':
            num_chars++;
            seed <<= 4;
            seed |= (*p - '0');
            break;
        case 'a':
        case 'b':
        case 'c':
        case 'd':
        case 'e':
        case 'f':
            num_chars++;
            seed <<= 4;
            seed |= (*p - 'a' + 0xa);
            break;
        case 'A':
        case 'B':
        case 'C':
        case 'D':
        case 'E':
        case 'F':
            num_chars++;
            seed <<= 4;
            seed |= (*p - 'A' + 0xa);
            break;
        default:
            fprintf(stderr, "Invalid seed.\n");
            usage();
        }
    } while (*++p);

    if (num_chars > MAX_HEX_CHARS) {
        fprintf(stderr, "Seed is too long.\n");
        usage();
    }

    *seedp = seed;

    return true;
}

static unsigned int
parse_int(char *flag)
{
    unsigned int ret;

    ret = 0;

    if (strlen(flag) > 10) {
        fprintf(stderr, "Integer argument too long.\n");
        usage();
    }

    if (!*flag) {
        fprintf(stderr, "Positive integer required.\n");
        usage();
    }

    while (*flag) {
        if (*flag < '0' || *flag > '9') {
            fprintf(stderr, "Positive integer required.\n");
            usage();
        }
        ret *= 10;
        ret += (*flag++ - '0');
    }

    return ret;
}

static bool
try_parse_int_arg(char *p, char *flag_name, bool *b, unsigned int *var)
{
    char *s;

    s = p;

    if (!strncmp(s, flag_name, strlen(flag_name))) {
        s += strlen(flag_name);
        if (*s++ != '=') {
            fprintf(stderr,
                    "Unrecognized flag: %s. Did you mean '%s'?\n",
                    p,
                    flag_name);
            usage();
        }

        ensure_unspecd(b, flag_name);

        *var = parse_int(p);

        return true;
    }

    return false;
}

static bool
try_parse_flag_arg(char *p, char *flag_name, bool *b)
{
    char *s;

    s = p;

    if (!strncmp(s, flag_name, strlen(flag_name))) {
        s += strlen(flag_name);
        if (*s) {
            fprintf(stderr,
                    "Unrecognized flag: %s. Did you mean '%s'?\n",
                    p,
                    flag_name);
            usage();
        }
        ensure_unspecd(b, flag_name);

        return true;
    }

    return false;
}

#define try_parse_int(p, flag_name, bool_name, var)                            \
    if (try_parse_int_arg(p, flag_name, &bool_name, var)) {                    \
        continue;                                                              \
    }

#define try_parse_seed(p, flag_name, bool_name, var)                           \
    if (try_parse_seed_arg(p, flag_name, &bool_name, var)) {                   \
        continue;                                                              \
    }

#define try_parse_flag(p, flag_name, bool_name, var)                           \
    if (try_parse_flag_arg(p, flag_name, &bool_name)) {                        \
        *var = true;                                                           \
        continue;                                                              \
    }

config_info_t *
parse_args(int argc, char *argv[])
{
    int            with_state           = OPT_DEFAULT;
    bool           func_test_provided   = false;
    bool           def_tests_provided   = false;
    bool           read_pct_provided    = false;
    bool           put_pct_provided     = false;
    bool           add_pct_provided     = false;
    bool           replace_pct_provided = false;
    bool           remove_pct_provided  = false;
    bool           view_pct_provided    = false;
    bool           sort_pct_provided    = false;
    bool           start_sz_provided    = false;
    bool           prefill_provided     = false;
    bool           key_range_provided   = false;
    bool           num_threads_provided = false;
    bool           total_op_provided    = false;
    bool           shuffle_provided     = false;
    bool           seed_provided        = false;
    int            alloc_len;
    int            i, j;
    int            num_hats;
    char          *cur;
    char          *p;
    size_t         cur_len;
    bool           got_one;
    config_info_t *ret;

    prog_name = argv[0];
    num_hats  = prepare_hat_info();
    alloc_len = sizeof(config_info_t) + sizeof(char *) * (num_hats + 1);
    ret       = (config_info_t *)calloc(1, alloc_len);

    config_init(ret);

    for (i = 1; i < argc; i++) {
        cur     = argv[i];
        cur_len = strlen(cur);

        if (cur[0] == '-') {
            if (with_state != OPT_DEFAULT && !got_one) {
                usage();
            }
            if (cur_len < 3 || cur[1] != '-') {
                usage();
            }
            p          = &cur[2];
            with_state = OPT_DEFAULT;

            if (!strncmp(p, S_WITH, strlen(S_WITH))) {
                if (with_state == OPT_NO) {
                    fprintf(stderr,
                            "Cannot have --with and --without together.\n");
                    usage();
                }
                with_state = OPT_YES;
                got_one    = false;
                p += strlen(S_WITH);
                switch (*p++) {
                case '=':
                    cur = p;
                    goto parse_arg;
                case 0:
                    continue;
                default:
                    usage();
                }
                continue;
            }
            if (!strncmp(p, S_WO, strlen(S_WO))) {
                if (with_state == OPT_YES) {
                    fprintf(stderr,
                            "Cannot have --with and --without together.\n");
                    usage();
                }
                with_state = OPT_NO;
                got_one    = false;
                p += strlen(S_WO);
                switch (*p++) {
                case '=':
                    cur = p;
                    goto parse_arg;
                case 0:
                    continue;
                default:
                    usage();
                }
                continue;
            }

            try_parse_flag(p, S_FUNC, func_test_provided, &ret->run_func_tests);
            try_parse_flag(p,
                           S_DEFAULT,
                           def_tests_provided,
                           &ret->run_default_tests);
            try_parse_seed(p, S_SEED, seed_provided, &ret->custom.seed);
            try_parse_int(p,
                          S_READ_PCT,
                          read_pct_provided,
                          &ret->custom.read_pct);
            try_parse_int(p, S_PUT_PCT, put_pct_provided, &ret->custom.put_pct);
            try_parse_int(p, S_ADD_PCT, add_pct_provided, &ret->custom.add_pct);
            try_parse_int(p,
                          S_REPLACE_PCT,
                          replace_pct_provided,
                          &ret->custom.replace_pct);
            try_parse_int(p,
                          S_REMOVE_PCT,
                          remove_pct_provided,
                          &ret->custom.remove_pct);
            try_parse_int(p,
                          S_VIEW_PCT,
                          view_pct_provided,
                          &ret->custom.view_pct);
            try_parse_int(p,
                          S_SORT_PCT,
                          sort_pct_provided,
                          &ret->custom.sort_pct);
            try_parse_int(p,
                          S_START_SIZE,
                          start_sz_provided,
                          &ret->custom.start_sz);
            try_parse_int(p,
                          S_PREFILL_PCT,
                          prefill_provided,
                          &ret->custom.prefill_pct);
            try_parse_int(p,
                          S_NUM_THREADS,
                          num_threads_provided,
                          &ret->custom.num_threads);
            try_parse_int(p,
                          S_TOTAL_OPS,
                          total_op_provided,
                          &ret->custom.total_ops);
            try_parse_int(p,
                          S_KEY_RANGE,
                          key_range_provided,
                          &ret->custom.key_range);
            try_parse_flag(p,
                           S_SHUFFLE,
                           shuffle_provided,
                           &ret->custom.shuffle);

            if (!strcmp(p, S_HELP)) {
                usage();
            }
            usage();
        }

        if (with_state == OPT_DEFAULT) {
            usage();
        }

parse_arg:

        got_one = true;

        for (j = 0; j < num_hats; j++) {
            if (!strcmp(cur, hat_info[j].name)) {
                hat_info[j].status = with_state;
                goto found_match;
            }
        }
        fprintf(stderr, "Unknown hash table: %s\n", cur);
        usage();

found_match:
        continue;
    }

    j = 0;

    for (i = 0; i < num_hats; i++) {
        switch (hat_info[i].status) {
        case OPT_YES:
            ret->hat_list[j++] = hat_info[i].name;
            break;
        case OPT_NO:
            continue;
        case OPT_DEFAULT:
            if (with_state != OPT_YES) {
                ret->hat_list[j++] = hat_info[i].name;
            }
        }
    }

    if (!read_pct_provided && !put_pct_provided && !add_pct_provided
        && !replace_pct_provided && !remove_pct_provided && !view_pct_provided
        && !sort_pct_provided && !start_sz_provided && !prefill_provided
        && !key_range_provided && !num_threads_provided && !total_op_provided
        && !shuffle_provided && !seed_provided) {
        ret->run_custom_test = false;

        if (!ret->run_default_tests && !ret->run_func_tests) {
            fprintf(stderr, "Error: No tests specified.\n");
            usage();
        }
    }
    else {
        ret->run_custom_test = true;
    }

    if ((ret->run_custom_test || ret->run_func_tests) && !def_tests_provided) {
        ret->run_default_tests = false;
    }

    validate_config(ret);

    return ret;
}

#ifdef HATRACK_DEBUG

void
print_config(config_info_t *config)
{
    int i;

    fprintf(stderr,
            "run_func_tests = %s\n",
            config->run_func_tests ? "true" : "false");
    fprintf(stderr,
            "run_stress_tests = %s\n",
            config->run_stress_tests ? "true" : "false");
    fprintf(stderr,
            "run_performance_tests = %s\n",
            config->run_performance_tests ? "true" : "false");
    fprintf(stderr, "read pct = %d\n", config->read_pct);
    fprintf(stderr, "put pct = %d\n", config->put_pct);
    fprintf(stderr, "add pct = %d\n", config->add_pct);
    fprintf(stderr, "replace pct = %d\n", config->replace_pct);
    fprintf(stderr, "remove pct = %d\n", config->remove_pct);
    fprintf(stderr, "start size = 2^%d\n", config->start_sz);
    fprintf(stderr, "prefill pct = %d\n", config->prefill_pct);
    fprintf(stderr, "num threads = %d\n", config->num_threads);
    fprintf(stderr, "total ops pct = %d\n", config->total_ops_pct);
    fprintf(stderr, "total ops = %llu\n", config->total_ops);
    fprintf(stderr, "seed = %p\n", (void *)config->seed);

    i = 0;

    fprintf(stderr, "Algorithms: ");
    while (config->hat_list[i]) {
        fprintf(stderr, "%s ", config->hat_list[i]);
        i++;
    }

    fprintf(stderr, "\n");

    return;
}

#endif
