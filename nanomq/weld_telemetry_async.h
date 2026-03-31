#pragma once

#include <stdint.h>

#include "nng/supplemental/nanolib/rule.h"

#ifdef __cplusplus
extern "C" {
#endif

int weld_telemetry_async_enqueue(const rule_taos *taos_rule,
    const char *topic_name, uint8_t qos, uint16_t packet_id,
    const uint8_t *payload, uint32_t payload_len);

void weld_telemetry_async_stop_all(void);

#ifdef __cplusplus
}
#endif
