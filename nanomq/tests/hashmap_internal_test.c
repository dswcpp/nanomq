#include <assert.h>
#include <stdio.h>
#include <string.h>

#include "../hashmap.c"

static int
remove_iterator(void *context, struct hashmap_element_s *element)
{
    int *count = context;
    assert(element != NULL);
    (*count)++;
    return -1;
}

static int
stop_iterator(void *context, struct hashmap_element_s *element)
{
    int *count = context;
    assert(element != NULL);
    (*count)++;
    return 7;
}

static void
test_create_rejects_invalid_sizes(void)
{
    struct hashmap_s map;
    memset(&map, 0, sizeof(map));
    assert(hashmap_create(0, &map) == 1);
    assert(hashmap_create(3, &map) == 1);
}

static void
test_put_same_key_updates_without_growing_size(void)
{
    struct hashmap_s map;
    const char *key = "dup-key";

    memset(&map, 0, sizeof(map));
    assert(hashmap_create(2, &map) == 0);
    assert(hashmap_put(&map, key, strlen(key), 11) == 0);
    assert(hashmap_num_entries(&map) == 1);
    assert(hashmap_put(&map, key, strlen(key), 22) == 0);
    assert(hashmap_num_entries(&map) == 1);
    assert(hashmap_get(&map, key, strlen(key)) == 22);
    hashmap_destroy(&map);
}

static void
test_rehash_grows_table_and_preserves_entries(void)
{
    struct hashmap_s map;
    char             keys[12][16];

    memset(&map, 0, sizeof(map));
    assert(hashmap_create(2, &map) == 0);

    for (int i = 0; i < 12; ++i) {
        snprintf(keys[i], sizeof(keys[i]), "key-%d", i);
        assert(hashmap_put(&map, keys[i], strlen(keys[i]), (uint32_t) i + 1) == 0);
    }

    assert(map.table_size >= 16);
    assert(hashmap_num_entries(&map) == 12);
    for (int i = 0; i < 12; ++i) {
        assert(hashmap_get(&map, keys[i], strlen(keys[i])) == (uint32_t) i + 1);
    }
    hashmap_destroy(&map);
}

static void
test_remove_missing_key_returns_error(void)
{
    struct hashmap_s map;

    memset(&map, 0, sizeof(map));
    assert(hashmap_create(2, &map) == 0);
    assert(hashmap_remove(&map, "missing", strlen("missing")) == 1);
    hashmap_destroy(&map);
}

static void
test_iterate_pairs_can_remove_entries(void)
{
    struct hashmap_s map;
    int              count = 0;

    memset(&map, 0, sizeof(map));
    assert(hashmap_create(4, &map) == 0);
    assert(hashmap_put(&map, "a", 1, 1) == 0);
    assert(hashmap_put(&map, "b", 1, 2) == 0);
    assert(hashmap_put(&map, "c", 1, 3) == 0);

    assert(hashmap_iterate_pairs(&map, remove_iterator, &count) == 0);
    assert(count == 3);
    assert(hashmap_num_entries(&map) == 0);
    hashmap_destroy(&map);
}

static void
test_iterate_pairs_can_stop_early(void)
{
    struct hashmap_s map;
    int              count = 0;

    memset(&map, 0, sizeof(map));
    assert(hashmap_create(4, &map) == 0);
    assert(hashmap_put(&map, "x", 1, 1) == 0);
    assert(hashmap_put(&map, "y", 1, 2) == 0);

    assert(hashmap_iterate_pairs(&map, stop_iterator, &count) == 1);
    assert(count == 1);
    assert(hashmap_num_entries(&map) == 2);
    hashmap_destroy(&map);
}

int
main(void)
{
    test_create_rejects_invalid_sizes();
    test_put_same_key_updates_without_growing_size();
    test_rehash_grows_table_and_preserves_entries();
    test_remove_missing_key_returns_error();
    test_iterate_pairs_can_remove_entries();
    test_iterate_pairs_can_stop_early();
    return 0;
}
