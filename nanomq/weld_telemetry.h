#pragma once

#include <stdint.h>

#include "nng/supplemental/nanolib/rule.h"

#ifdef __cplusplus
extern "C" {
#endif

struct pub_packet_struct;

int weld_telemetry_handle_publish(
    const rule_taos *taos_rule, const struct pub_packet_struct *pub_packet);

int weld_telemetry_handle_publish_raw(const rule_taos *taos_rule,
    const char *topic_name, uint8_t qos, uint16_t packet_id,
    const uint8_t *payload, uint32_t payload_len);

#ifdef __cplusplus
}
#endif
