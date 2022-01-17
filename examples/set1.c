#include <hatrack.h>
#include <stdio.h>

const char *CONST_PROPER       = "\xe2\x8a\x83";
const char *CONST_NOT_PROPER   = "\xe2\x8a\x85";
const char *CONST_SUPERSET     = "\xe2\x8a\x87";
const char *CONST_NOT_SUPER    = "\xe2\x8a\x89";
const char *CONST_UNION        = "\xe2\x88\xaa";
const char *CONST_INTERSECTION = "\xe2\x88\xa9";
const char *CONST_EMPTY_SET    = "\xe2\x88\x85";

typedef struct {
    hatrack_set_t *set;
    char          *name;
} set_info_t;

static void
print_set(char *prefix, hatrack_set_t *set)
{
    int64_t *view;
    uint64_t i;
    uint64_t num;

    view = hatrack_set_items_sort(set, &num);

    printf("%s = { ", prefix);

    for (i = 0; i < num; i++) {
        if (i) {
            printf(", %lld", (long long)view[i]);
        }
        else {
            printf("%lld", (long long)view[i]);
        }
    }

    printf(" }\n");

    free(view);

    return;
}

static void
print_sets(set_info_t *sets, int num_sets)
{
    int i;

    printf("The sets:\n");

    for (i = 0; i < num_sets; i++) {
        print_set(sets[i].name, sets[i].set);
    }

    return;
}

static void
show_one_subset_relationship(hatrack_set_t *s1,
                             char          *name1,
                             hatrack_set_t *s2,
                             char          *name2)
{
    printf("%s %s %s; %s %s %s\n",
           name1,
           hatrack_set_is_superset(s1, s2, true) ? CONST_PROPER
                                                 : CONST_NOT_PROPER,
           name2,
           name1,
           hatrack_set_is_superset(s1, s2, false) ? CONST_SUPERSET
                                                  : CONST_NOT_SUPER,
           name2);

    return;
}

static void
show_subset_info(set_info_t *sets, int num_sets)
{
    int i, j;

    printf("\nAre sets subsets?\n");

    for (i = 0; i < num_sets - 1; i++) {
        for (j = 0; j < num_sets; j++) {
            show_one_subset_relationship(sets[i].set,
                                         sets[i].name,
                                         sets[j].set,
                                         sets[j].name);
        }
    }

    return;
}

static void
show_one_are_disjoint(hatrack_set_t *s1,
                      char          *name1,
                      hatrack_set_t *s2,
                      char          *name2)
{
    printf("%s %s %s %c= %s\n",
           name1,
           CONST_INTERSECTION,
           name2,
           hatrack_set_is_disjoint(s1, s2) ? '=' : '!',
           CONST_EMPTY_SET);

    return;
}

static void
show_are_disjoint(set_info_t *sets, int num_sets)
{
    int i, j;

    printf("\nAre sets disjoint?\n");

    for (i = 0; i < num_sets - 1; i++) {
        for (j = i + 1; j < num_sets; j++) {
            show_one_are_disjoint(sets[i].set,
                                  sets[i].name,
                                  sets[j].set,
                                  sets[j].name);
        }
    }

    return;
}

static void
show_one_difference(hatrack_set_t *s1,
                    char          *name1,
                    hatrack_set_t *s2,
                    char          *name2)
{
    hatrack_set_t *diff;

    diff = hatrack_set_difference(s1, s2);

    printf("%s - ", name1);
    print_set(name2, diff);

    hatrack_set_delete(diff);

    return;
}

static void
show_set_differences(set_info_t *sets, int num_sets)
{
    int i, j;

    printf("\nDifferences:\n");

    for (i = 0; i < num_sets - 1; i++) {
        for (j = 0; j < num_sets; j++) {
            if (i == j) {
                continue;
            }
            show_one_difference(sets[i].set,
                                sets[i].name,
                                sets[j].set,
                                sets[j].name);
        }
    }

    return;
}

static void
show_one_union(hatrack_set_t *s1, char *name1, hatrack_set_t *s2, char *name2)
{
    hatrack_set_t *u;

    u = hatrack_set_union(s1, s2);

    printf("%s %s ", name1, CONST_UNION);
    print_set(name2, u);

    hatrack_set_delete(u);

    return;
}

static void
show_set_unions(set_info_t *sets, int num_sets)
{
    int i, j;

    printf("\nUnions:\n");

    for (i = 0; i < num_sets - 1; i++) {
        for (j = i + 1; j < num_sets; j++) {
            show_one_union(sets[i].set,
                           sets[i].name,
                           sets[j].set,
                           sets[j].name);
        }
    }

    return;
}

static void
show_one_intersection(hatrack_set_t *s1,
                      char          *name1,
                      hatrack_set_t *s2,
                      char          *name2)
{
    hatrack_set_t *intersection;

    intersection = hatrack_set_intersection(s1, s2);

    printf("%s %s ", name1, CONST_INTERSECTION);
    print_set(name2, intersection);

    hatrack_set_delete(intersection);

    return;
}

static void
show_set_intersections(set_info_t *sets, int num_sets)
{
    int i, j;

    printf("\nIntersections:\n");

    for (i = 0; i < num_sets - 1; i++) {
        for (j = i + 1; j < num_sets; j++) {
            show_one_intersection(sets[i].set,
                                  sets[i].name,
                                  sets[j].set,
                                  sets[j].name);
        }
    }

    return;
}

static void
show_one_set_disjunction(hatrack_set_t *s1,
                         char          *name1,
                         hatrack_set_t *s2,
                         char          *name2)
{
    hatrack_set_t *disjunction;

    disjunction = hatrack_set_disjunction(s1, s2);

    printf("(%s %s %s) - (%s %s %s)",
           name1,
           CONST_UNION,
           name2,
           name1,
           CONST_INTERSECTION,
           name2);

    print_set("", disjunction);

    hatrack_set_delete(disjunction);

    return;
}

static void
show_set_disjunctions(set_info_t *sets, int num_sets)
{
    int i, j;

    printf("\nDisjunctions (symmetric differences):\n");

    for (i = 0; i < num_sets - 1; i++) {
        for (j = i + 1; j < num_sets; j++) {
            show_one_set_disjunction(sets[i].set,
                                     sets[i].name,
                                     sets[j].set,
                                     sets[j].name);
        }
    }

    return;
}

#define NUM_TEST_SETS 3

set_info_t test_sets[NUM_TEST_SETS + 1] = {};

int
main(void)
{
    hatrack_set_t *s1, *s2, *s3;
    uint64_t       i;

    s1 = hatrack_set_new(HATRACK_DICT_KEY_TYPE_INT);
    s2 = hatrack_set_new(HATRACK_DICT_KEY_TYPE_INT);
    s3 = hatrack_set_new(HATRACK_DICT_KEY_TYPE_INT);

    for (i = 0; i < 20; i++) {
        hatrack_set_put(s1, (void *)i);
        if (!hatrack_set_contains(s1, (void *)i)) {
            abort();
        }
    }

    for (i = 0; i < 5; i++) {
        hatrack_set_put(s2, (void *)i);
        if (!hatrack_set_contains(s2, (void *)i)) {
            abort();
        }
    }

    for (i = 10; i < 25; i++) {
        hatrack_set_put(s3, (void *)i);
        if (!hatrack_set_contains(s3, (void *)i)) {
            abort();
        }
    }

    test_sets[0].set  = s1;
    test_sets[0].name = "s1";
    test_sets[1].set  = s2;
    test_sets[1].name = "s2";
    test_sets[2].set  = s3;
    test_sets[2].name = "s3";

    print_sets(test_sets, NUM_TEST_SETS);
    show_subset_info(test_sets, NUM_TEST_SETS);
    show_are_disjoint(test_sets, NUM_TEST_SETS);
    show_set_differences(test_sets, NUM_TEST_SETS);
    show_set_unions(test_sets, NUM_TEST_SETS);
    show_set_intersections(test_sets, NUM_TEST_SETS);
    show_set_disjunctions(test_sets, NUM_TEST_SETS);

    hatrack_set_delete(s1);
    hatrack_set_delete(s2);
    hatrack_set_delete(s3);

    return 0;
}
