#include <stdlib.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>

#define NUM_CURRENT_HATS 16

#include <testhat.h>

enum {
    OPT_DEFAULT,
    OPT_YES,
    OPT_NO
};


typedef struct {
    char *name;
    int   status;
} config_hat_info_t;

static config_hat_info_t hat_info[NUM_CURRENT_HATS] = {
    { "refhat",     OPT_DEFAULT },
    { "duncecap",   OPT_DEFAULT },
    { "swimcap",    OPT_DEFAULT }, 
    { "newshat",    OPT_DEFAULT }, 
    { "hihat",      OPT_DEFAULT }, 
    { "hihat-a",    OPT_DEFAULT }, 
    { "witchhat",   OPT_DEFAULT }, 
    { "oldhat",     OPT_DEFAULT }, 
    { "ballcap",    OPT_DEFAULT }, 
    { "lohat",      OPT_DEFAULT }, 
    { "lohat-a",    OPT_DEFAULT }, 
    { "woolhat",    OPT_DEFAULT }, 
    { "tophat-fmx", OPT_DEFAULT }, 
    { "tophat-fwf", OPT_DEFAULT }, 
    { "tophat-cmx", OPT_DEFAULT }, 
    { "tophat-cwf", OPT_DEFAULT } 
};

static char *prog_name;

_Noreturn static void
usage()
{
    fprintf(stderr, "Usage: %s: [options]*\n", prog_name);
    fprintf(stderr, "\nOptions:\n");
    fprintf(stderr, "  --with [algorithm]+ | --without [algorithm]+ \n");
    fprintf(stderr,
	    "  --other_tables=yes/no (Whether to add the unnamed tables or "
	    "not)\n");
    fprintf(stderr, "  --func[=yes/no] (Whether to run functionality tests)\n");
    fprintf(stderr, "  --stress[=yes/no] (Whether to run stress tests)\n");
    fprintf(stderr,
	    "  --throughput[=yes/no] (Whether to run throughput tests)\n");
    fprintf(stderr, "  --seed = <hex-digits> (Set a seed for the rng)\n\n");
    fprintf(stderr, "When specifying algorithms, use spaces between the names");
    fprintf(stderr, ", or pass flags\nmultiple times.\n\n");
    fprintf(stderr, "If you pass a test type without arguments, we assume ");
    fprintf(stderr, "you want that type on.\nIf all test type flags passed ");
    fprintf(stderr, "are of the same value, then\nunspecified values are ");
    fprintf(stderr, "assumed to be of the opposite type.\n\n");
    fprintf(stderr, "We use similar logic to figure out whether we should ");
    fprintf(stderr, "include unspecified\nalgorithms, but you can use the ");
    fprintf(stderr, "--other-tables flag to be explicit\nabout it.\n\n");
    fprintf(stderr, "If you supply an RNG seed, it is interpreted as a 64-bit");
    fprintf(stderr, " hex value, but\ndo NOT put on a trailing 0x.\n\n");
    fprintf(stderr, "Currently supported algorithms:\n");
    fprintf(stderr, "  refhat      duncecap    swimcap     newshat     ");
    fprintf(stderr, "hihat       hihat-a     \n  witchhat    oldhat      ");
    fprintf(stderr, "ballcap     lohat       lohat-a     woolhat\n");
    fprintf(stderr, "  tophat-fmx  tophat-fwf  tophat-cmx  tophat-cwf\n");
    exit(1);
}

#define ARG_FLAG_NAME(x) x, (sizeof(x) - 1)


static inline bool
parse_opt_yes_or_no(char *flag)
{
    if (!*flag) {
	return true;
    }
    if (*flag++ != '=') {
	usage();
    }
    switch (*flag) {
    case 'y':
    case 'Y':
    case 't':
    case 'T':
	return true;
    case 'n':
    case 'N':
    case 'f':
    case 'F':
	return false;
    default:
	usage();
    }
}

static inline bool
parse_req_yes_or_no(char *flag)
{
    if (!*flag) {
	usage();
    }
    return parse_opt_yes_or_no(flag);
}

#define MAX_HEX_CHARS 16

static inline int64_t
parse_seed(char *flag)
{
    uint64_t seed      = 0;
    int      num_chars = 0;
    char    *p         = flag;

    if (*p++ != '=') {
	usage();
    }
    do {
	switch (*p) {
	case 0:
	    usage();
	case '0': case '1': case '2': case '3': case '4':
	case '5': case '6': case '7': case '8': case '9':
	    num_chars++;
	    seed <<= 4;
	    seed |= (*p - '0');
	    break;
	case 'a': case 'b': case 'c': case 'd': case 'e': case 'f':
	    num_chars++;
	    seed <<= 4;
	    seed |= (*p - 'a' + 0xa);
	    break;
	case 'A': case 'B': case 'C': case 'D': case 'E': case 'F':
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

    return (int64_t)seed;
}

static inline void
ensure_unspecd(int64_t value, char *str)
{
    if (value != OPT_DEFAULT) {
	fprintf(stderr, "Error: multiple appearances of flag: --%s\n", str);
	usage();
    }
}

#define S_WITH         "with"
#define S_WO           "without"
#define S_OTHER_TABLES "other_tables"
#define S_FUNC         "func"
#define S_STRESS       "stress"
#define S_THROUGHPUT   "throughput"
#define S_SEED         "seed"
#define S_HELP         "help"

config_info_t *
parse_args(int argc, char *argv[])
{
    int            with_state       = OPT_DEFAULT;
    int            func_tests       = OPT_DEFAULT;
    int            stress_tests     = OPT_DEFAULT;
    int            throughput_tests = OPT_DEFAULT;
    int            other_tables     = OPT_DEFAULT;
    int64_t        seed             = 0;
    int            alloc_len;
    int            i, j;
    char          *cur;
    char          *p;
    size_t         cur_len;
    bool           got_one;
    bool           the_default;
    config_info_t *ret;
    
    prog_name = argv[0];
    alloc_len = sizeof(config_info_t) + sizeof(char *) * (NUM_CURRENT_HATS + 1);
    ret       = (config_info_t *)calloc(1, alloc_len); 

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
	    
	    if (!strncmp(p, ARG_FLAG_NAME(S_WITH))) {
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
	    if (!strncmp(p, ARG_FLAG_NAME(S_WO))) {
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
	    if (!strncmp(p, ARG_FLAG_NAME(S_OTHER_TABLES))) {
		p += strlen(S_OTHER_TABLES);
		ensure_unspecd(other_tables, S_OTHER_TABLES);
		other_tables = parse_req_yes_or_no(p) ? OPT_YES : OPT_NO;
		continue;
	    }
	    if (!strncmp(p, ARG_FLAG_NAME(S_FUNC))) {
		p += strlen(S_FUNC);
		ensure_unspecd(func_tests, S_FUNC);
		func_tests = parse_opt_yes_or_no(p) ? OPT_YES : OPT_NO;
		continue;
	    }
	    if (!strncmp(p, ARG_FLAG_NAME(S_STRESS))) {
		p += strlen(S_STRESS);
		ensure_unspecd(stress_tests, S_STRESS);
		stress_tests = parse_opt_yes_or_no(p) ?
		    OPT_YES : OPT_NO;
		continue;
	    }
	    if (!strncmp(p, ARG_FLAG_NAME(S_THROUGHPUT))) {
		p += strlen(S_THROUGHPUT);
		ensure_unspecd(throughput_tests, S_THROUGHPUT);
		throughput_tests = parse_opt_yes_or_no(p) ?
		    OPT_YES : OPT_NO;
		continue;
	    }
	    if (!strncmp(p, ARG_FLAG_NAME(S_SEED))) {
		p += strlen(S_SEED);
		ensure_unspecd(seed, S_SEED);
		seed = parse_seed(p);
		continue;
	    }
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
	
	for (j = 0; j < NUM_CURRENT_HATS; j++) {
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

    the_default = false;
    
    if (func_tests == stress_tests == throughput_tests == OPT_DEFAULT) {
	the_default = true;
    } else {
	if (func_tests != OPT_YES &&
	    stress_tests != OPT_YES &&
	    throughput_tests != OPT_YES) {
	    the_default = true;
	}
    }

    switch (func_tests) {
    case OPT_YES:
	ret->run_func_tests = true;
	break;
    case OPT_NO:
	ret->run_func_tests= false;
	break;
    default:
	ret->run_func_tests = the_default;
	break;
    }
    
    switch (stress_tests) {
    case OPT_YES:
	ret->run_stress_tests = true;
	break;
    case OPT_NO:
	ret->run_stress_tests = false;
	break;
    default:
	ret->run_stress_tests = the_default;
	break;
    }

    switch (throughput_tests) {
    case OPT_YES:
	ret->run_throughput_tests = true;
	break;
    case OPT_NO:
	ret->run_throughput_tests = false;
	break;
    default:
	ret->run_throughput_tests = the_default;
	break;
    }
    
    ret->seed = seed;

    switch (other_tables) {
    case OPT_YES:
	the_default = true;
	break;
    case OPT_NO:
	the_default = false;
	break;
    default:
	j = 0;
	the_default = false;
	
	for (i = 0; i < NUM_CURRENT_HATS; i++) {
	    switch (hat_info[i].status) {
	    case OPT_NO:
		the_default = true;
		continue;
	    case OPT_YES:
		j++;
	    }
	}
	// If there was no explicit no, but no explicit yes,
	// then we're giving you all algorithms.
	if (!the_default && !j) {
	    the_default = true;
	}
    }
    
    j = 0;
    
    for (i = 0; i < NUM_CURRENT_HATS; i++) {
	switch (hat_info[i].status) {
	case OPT_YES:
	    ret->hat_list[j++] = hat_info[i].name;
	    break;
	case OPT_NO:
	    continue;
	case OPT_DEFAULT:
	    if (the_default) {
		ret->hat_list[j++] = hat_info[i].name;
	    }
	    
	}
    }

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
	    "run_throughput_tests = %s\n",
	    config->run_throughput_tests ? "true" : "false");
    fprintf(stderr,
	    "seed = %p\n", (void *)config->seed);

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
	     
