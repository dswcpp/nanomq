#pragma once

#include <stdint.h>
#include <stddef.h>

#include "taos_sink.hpp"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
    const char *stable;

    int64_t     ts_us;
    int64_t     recv_ts_us;
    const char *msg_id;
    int64_t     seq;
    const char *topic_name;
    const char *spec_ver;

    uint8_t     has_temperature;
    double      temperature;
    uint8_t     has_humidity;
    double      humidity;

    uint8_t     has_instant_flow;
    double      instant_flow;
    uint8_t     has_total_flow;
    double      total_flow;

    uint8_t     has_current;
    double      current;
    uint8_t     has_voltage;
    double      voltage;

    const char *raw_adc_unit;
    const char *cal_version;
    uint8_t     has_cal_k;
    double      cal_k;
    uint8_t     has_cal_b;
    double      cal_b;

    uint8_t     has_quality_code;
    int         quality_code;
    const char *quality_text;

    const char *source_bus;
    const char *source_port;
    const char *source_protocol;

    uint8_t     has_collect_period_ms;
    int         collect_period_ms;
    uint8_t     has_collect_timeout_ms;
    int         collect_timeout_ms;
    uint8_t     has_collect_retries;
    int         collect_retries;

    int         qos;
    int         packet_id;

    const char *version;
    const char *site_id;
    const char *line_id;
    const char *station_id;
    const char *gateway_id;
    const char *device_id;
    const char *device_type;
    const char *device_model;
    const char *metric_group;
    const char *signal_type;
    const char *channel_id;
} weld_taos_row;

int weld_taos_sink_enqueue_row_with_config(
    const taos_sink_config *cfg, const weld_taos_row *row);

int weld_taos_sink_enqueue_rows_with_config(const taos_sink_config *cfg,
    const weld_taos_row *rows, size_t row_count);

int weld_taos_sink_can_accept_rows_with_config(
    const taos_sink_config *cfg, size_t row_count);

int weld_taos_sink_is_started(void);

void weld_taos_sink_stop_all(void);

#ifdef __cplusplus
}
#endif
