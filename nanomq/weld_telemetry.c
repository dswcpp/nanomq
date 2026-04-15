#include "weld_telemetry.h"

#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#if defined(_WIN32)
#include <windows.h>
#else
#include <sys/time.h>
#endif
#if defined(NANOMQ_HAS_ZSTD)
#include <zstd.h>
#endif

#include "include/pub_handler.h"
#include "nng/supplemental/nanolib/base64.h"
#include "nng/supplemental/nanolib/cJSON.h"
#include "nng/supplemental/nanolib/log.h"
#include "weld_taos_sink.hpp"

#if defined(_WIN32)
#define strtok_r strtok_s
#endif

typedef struct {
	char *version;
	char *site_id;
	char *line_id;
	char *station_id;
	char *gateway_id;
	char *metric_group;
	char *device_id;
} weld_topic_parts;

// Payload size limit for TDengine VARCHAR(49152) column
#define WELD_RAW_PAYLOAD_MAX_BYTES (49152)

typedef struct {
	int temp_idx;
	int hum_idx;
	int flow_idx;
	int total_flow_idx;
	int current_idx;
	int voltage_idx;
} weld_field_indices;

typedef struct {
	uint64_t accept_msg_count;
	uint64_t accept_point_count;
	uint64_t fail_msg_count;
	uint64_t duplicate_ts_reject_count;
	uint64_t sink_overload_reject_count;
	uint64_t high_freq_ts_ms_warn_count;
	uint64_t sink_enqueue_fail_count;
	uint64_t accept_env_msg_count;
	uint64_t accept_flow_msg_count;
	uint64_t accept_current_msg_count;
	uint64_t accept_voltage_msg_count;
	uint64_t last_log_accept_msg_count;
	uint64_t last_log_accept_point_count;
	uint64_t last_log_fail_msg_count;
	uint64_t last_log_duplicate_ts_reject_count;
	uint64_t last_log_sink_overload_reject_count;
	uint64_t last_log_high_freq_ts_ms_warn_count;
	uint64_t last_log_sink_enqueue_fail_count;
	int64_t  last_log_ts_us;
	int64_t  next_log_ts_us;
} weld_telemetry_stats;

static weld_telemetry_stats g_weld_telemetry_stats = { 0 };

#if defined(_WIN32)
static int64_t
weld_now_epoch_us(void)
{
	FILETIME        ft;
	ULARGE_INTEGER  uli;

	GetSystemTimeAsFileTime(&ft);
	uli.LowPart  = ft.dwLowDateTime;
	uli.HighPart = ft.dwHighDateTime;
	return (int64_t) ((uli.QuadPart - 116444736000000000ULL) / 10ULL);
}

static void
weld_stats_inc(uint64_t *value)
{
	InterlockedIncrement64((volatile LONG64 *) value);
}

static void
weld_stats_add(uint64_t *value, uint64_t delta)
{
	InterlockedAdd64((volatile LONG64 *) value, (LONG64) delta);
}

static uint64_t
weld_stats_read(uint64_t *value)
{
	return (uint64_t) InterlockedCompareExchange64(
	    (volatile LONG64 *) value, 0, 0);
}

static bool
weld_stats_cas_i64(int64_t *target, int64_t expected, int64_t desired)
{
	return InterlockedCompareExchange64((volatile LONG64 *) target,
	           (LONG64) desired, (LONG64) expected) ==
	    (LONG64) expected;
}
#else
static int64_t
weld_now_epoch_us(void)
{
	struct timeval tv;
	gettimeofday(&tv, NULL);
	return (int64_t) tv.tv_sec * 1000000LL + (int64_t) tv.tv_usec;
}

static void
weld_stats_inc(uint64_t *value)
{
	__sync_fetch_and_add(value, 1);
}

static void
weld_stats_add(uint64_t *value, uint64_t delta)
{
	__sync_fetch_and_add(value, delta);
}

static uint64_t
weld_stats_read(uint64_t *value)
{
	return (uint64_t) __sync_add_and_fetch(value, 0);
}

static bool
weld_stats_cas_i64(int64_t *target, int64_t expected, int64_t desired)
{
	return __sync_bool_compare_and_swap(target, expected, desired);
}
#endif

static void
weld_stats_inc_table_accept(const char *table)
{
	if (table == NULL) {
		return;
	}
	if (strcmp(table, "weld_env_point") == 0) {
		weld_stats_inc(&g_weld_telemetry_stats.accept_env_msg_count);
		return;
	}
	if (strcmp(table, "weld_flow_point") == 0) {
		weld_stats_inc(&g_weld_telemetry_stats.accept_flow_msg_count);
		return;
	}
	if (strcmp(table, "weld_current_point") == 0 ||
	    strcmp(table, "weld_current_raw") == 0) {
		weld_stats_inc(&g_weld_telemetry_stats.accept_current_msg_count);
		return;
	}
	if (strcmp(table, "weld_voltage_point") == 0 ||
	    strcmp(table, "weld_voltage_raw") == 0) {
		weld_stats_inc(&g_weld_telemetry_stats.accept_voltage_msg_count);
	}
}

static void
weld_log_stats_if_due(int64_t now_us)
{
	int64_t expected = g_weld_telemetry_stats.next_log_ts_us;
	if (expected == 0) {
		int64_t next = now_us + 30000000LL;
		if (!weld_stats_cas_i64(
		        &g_weld_telemetry_stats.next_log_ts_us, 0, next)) {
			return;
		}
		g_weld_telemetry_stats.last_log_ts_us = now_us;
		return;
	}
	if (now_us < expected) {
		return;
	}
	if (!weld_stats_cas_i64(&g_weld_telemetry_stats.next_log_ts_us,
	        expected, now_us + 30000000LL)) {
		return;
	}

	{
		uint64_t accept_msg = weld_stats_read(
		    &g_weld_telemetry_stats.accept_msg_count);
		uint64_t accept_point = weld_stats_read(
		    &g_weld_telemetry_stats.accept_point_count);
		uint64_t fail_msg = weld_stats_read(
		    &g_weld_telemetry_stats.fail_msg_count);
		uint64_t duplicate_ts_reject = weld_stats_read(
		    &g_weld_telemetry_stats.duplicate_ts_reject_count);
		uint64_t sink_overload_reject = weld_stats_read(
		    &g_weld_telemetry_stats.sink_overload_reject_count);
		uint64_t high_freq_ts_ms_warn = weld_stats_read(
		    &g_weld_telemetry_stats.high_freq_ts_ms_warn_count);
		uint64_t sink_enqueue_fail = weld_stats_read(
		    &g_weld_telemetry_stats.sink_enqueue_fail_count);
		uint64_t accept_env_msg = weld_stats_read(
		    &g_weld_telemetry_stats.accept_env_msg_count);
		uint64_t accept_flow_msg = weld_stats_read(
		    &g_weld_telemetry_stats.accept_flow_msg_count);
		uint64_t accept_current_msg = weld_stats_read(
		    &g_weld_telemetry_stats.accept_current_msg_count);
		uint64_t accept_voltage_msg = weld_stats_read(
		    &g_weld_telemetry_stats.accept_voltage_msg_count);
		double elapsed_sec =
		    g_weld_telemetry_stats.last_log_ts_us > 0 &&
		            now_us > g_weld_telemetry_stats.last_log_ts_us ?
		        (double) (now_us - g_weld_telemetry_stats.last_log_ts_us) /
		            1000000.0 :
		        0.0;

		log_info("weld_telemetry: accept_msg=%llu accept_point=%llu "
		         "fail_msg=%llu duplicate_ts_reject=%llu "
		         "sink_overload_reject=%llu high_freq_ts_ms_warn=%llu "
		         "sink_enqueue_fail=%llu env_msg=%llu flow_msg=%llu "
		         "current_msg=%llu voltage_msg=%llu window_s=%.2f "
		         "accept_msg_rate=%.2f accept_point_rate=%.2f "
		         "fail_msg_rate=%.2f",
		    (unsigned long long) accept_msg,
		    (unsigned long long) accept_point,
		    (unsigned long long) fail_msg,
		    (unsigned long long) duplicate_ts_reject,
		    (unsigned long long) sink_overload_reject,
		    (unsigned long long) high_freq_ts_ms_warn,
		    (unsigned long long) sink_enqueue_fail,
		    (unsigned long long) accept_env_msg,
		    (unsigned long long) accept_flow_msg,
		    (unsigned long long) accept_current_msg,
		    (unsigned long long) accept_voltage_msg,
		    elapsed_sec,
		    elapsed_sec > 0.0 ?
		        (accept_msg -
		            g_weld_telemetry_stats.last_log_accept_msg_count) /
		                elapsed_sec :
		        0.0,
		    elapsed_sec > 0.0 ?
		        (accept_point -
		            g_weld_telemetry_stats.last_log_accept_point_count) /
		                elapsed_sec :
		        0.0,
		    elapsed_sec > 0.0 ?
		        (fail_msg - g_weld_telemetry_stats.last_log_fail_msg_count) /
		                elapsed_sec :
		        0.0);

		g_weld_telemetry_stats.last_log_accept_msg_count = accept_msg;
		g_weld_telemetry_stats.last_log_accept_point_count = accept_point;
		g_weld_telemetry_stats.last_log_fail_msg_count = fail_msg;
		g_weld_telemetry_stats.last_log_duplicate_ts_reject_count =
		    duplicate_ts_reject;
		g_weld_telemetry_stats.last_log_sink_overload_reject_count =
		    sink_overload_reject;
		g_weld_telemetry_stats.last_log_high_freq_ts_ms_warn_count =
		    high_freq_ts_ms_warn;
		g_weld_telemetry_stats.last_log_sink_enqueue_fail_count =
		    sink_enqueue_fail;
		g_weld_telemetry_stats.last_log_ts_us = now_us;
	}
}

static const char *
weld_expected_table(const char *signal_type)
{
	if (signal_type == NULL) {
		return NULL;
	}
	if (strcmp(signal_type, "environment") == 0) {
		return "weld_env_point";
	}
	if (strcmp(signal_type, "gas_flow") == 0) {
		return "weld_flow_point";
	}
	if (strcmp(signal_type, "current") == 0) {
		return "weld_current_point";
	}
	if (strcmp(signal_type, "voltage") == 0) {
		return "weld_voltage_point";
	}
	return NULL;
}

static int
weld_is_raw_power_table(const char *table)
{
	return table != NULL &&
	    (strcmp(table, "weld_current_raw") == 0 ||
	        strcmp(table, "weld_voltage_raw") == 0);
}

static int
weld_table_matches_signal_type(const char *table, const char *signal_type)
{
	if (table == NULL || signal_type == NULL) {
		return 0;
	}
	if (strcmp(signal_type, "environment") == 0) {
		return strcmp(table, "weld_env_point") == 0;
	}
	if (strcmp(signal_type, "gas_flow") == 0) {
		return strcmp(table, "weld_flow_point") == 0;
	}
	if (strcmp(signal_type, "current") == 0) {
		return strcmp(table, "weld_current_point") == 0 ||
		    strcmp(table, "weld_current_raw") == 0;
	}
	if (strcmp(signal_type, "voltage") == 0) {
		return strcmp(table, "weld_voltage_point") == 0 ||
		    strcmp(table, "weld_voltage_raw") == 0;
	}
	return 0;
}

static int
weld_expected_device_type(const char *signal_type, const char **device_type)
{
	if (signal_type == NULL || device_type == NULL) {
		return -1;
	}
	if (strcmp(signal_type, "environment") == 0) {
		*device_type = "temp_humidity_transmitter";
		return 0;
	}
	if (strcmp(signal_type, "gas_flow") == 0) {
		*device_type = "gas_flow_meter";
		return 0;
	}
	if (strcmp(signal_type, "current") == 0) {
		*device_type = "current_transducer";
		return 0;
	}
	if (strcmp(signal_type, "voltage") == 0) {
		*device_type = "voltage_transducer";
		return 0;
	}
	return -1;
}

static int
weld_validate_metric_group(const char *metric_group, const char *signal_type)
{
	if (metric_group == NULL || signal_type == NULL) {
		return -1;
	}
	if (strcmp(metric_group, "env") == 0 &&
	    strcmp(signal_type, "environment") == 0) {
		return 0;
	}
	if (strcmp(metric_group, "flow") == 0 &&
	    strcmp(signal_type, "gas_flow") == 0) {
		return 0;
	}
	if (strcmp(metric_group, "power") == 0 &&
	    (strcmp(signal_type, "current") == 0 ||
	        strcmp(signal_type, "voltage") == 0)) {
		return 0;
	}
	return -1;
}

static int
weld_read_point_value(
	cJSON *values, int index, const char *field_name, int required, double *value)
{
	cJSON *item = NULL;

	if (value == NULL || values == NULL || index < 0) {
		return required ? -1 : 0;
	}

	item = cJSON_GetArrayItem(values, index);
	if (!cJSON_IsNumber(item)) {
		if (required) {
			log_error(
			    "weld_telemetry: field %s must be numeric", field_name);
			return -1;
		}
		return 0;
	}

	*value = cJSON_GetNumberValue(item);
	return 1;
}

static int
weld_validate_point_timestamp(
	cJSON *point, int use_ts_us, int64_t *ts_us_out)
{
	cJSON *ts_us = NULL;
	cJSON *ts_ms = NULL;

	if (point == NULL || ts_us_out == NULL) {
		return -1;
	}

	ts_us = cJSON_GetObjectItem(point, "ts_us");
	ts_ms = cJSON_GetObjectItem(point, "ts_ms");

	if ((ts_us != NULL) == (ts_ms != NULL)) {
		log_error(
		    "weld_telemetry: each point must contain exactly one of ts_us or ts_ms");
		return -1;
	}

	if (use_ts_us) {
		if (!cJSON_IsNumber(ts_us)) {
			log_error("weld_telemetry: point ts_us missing or invalid");
			return -1;
		}
		*ts_us_out = (int64_t) cJSON_GetNumberValue(ts_us);
		return 0;
	}

	if (!cJSON_IsNumber(ts_ms)) {
		log_error("weld_telemetry: point ts_ms missing or invalid");
		return -1;
	}

	*ts_us_out = (int64_t) cJSON_GetNumberValue(ts_ms) * 1000LL;
	return 0;
}

static int
weld_parse_topic(const char *topic, weld_topic_parts *parts)
{
	char *topic_copy = NULL;
	char *saveptr    = NULL;
	char *token      = NULL;
	char *segments[9] = { 0 };
	int   count      = 0;
	int   rc         = -1;

	memset(parts, 0, sizeof(*parts));

	if (topic == NULL) {
		return -1;
	}

	topic_copy = nng_strdup(topic);
	if (topic_copy == NULL) {
		return -1;
	}

	for (token = strtok_r(topic_copy, "/", &saveptr); token != NULL;
	     token = strtok_r(NULL, "/", &saveptr)) {
		if (count >= 9) {
			goto out;
		}
		segments[count++] = token;
	}

	if (count != 9) {
		goto out;
	}
	if (strcmp(segments[0], "weld") != 0 ||
	    strcmp(segments[6], "telemetry") != 0) {
		goto out;
	}

	parts->version    = nng_strdup(segments[1]);
	parts->site_id    = nng_strdup(segments[2]);
	parts->line_id    = nng_strdup(segments[3]);
	parts->station_id = nng_strdup(segments[4]);
	parts->gateway_id = nng_strdup(segments[5]);
	parts->metric_group = nng_strdup(segments[7]);
	parts->device_id  = nng_strdup(segments[8]);
	rc = 0;

out:
	nng_strfree(topic_copy);
	if (rc != 0) {
		nng_strfree(parts->version);
		nng_strfree(parts->site_id);
		nng_strfree(parts->line_id);
		nng_strfree(parts->station_id);
		nng_strfree(parts->gateway_id);
		nng_strfree(parts->metric_group);
		nng_strfree(parts->device_id);
		memset(parts, 0, sizeof(*parts));
	}
	return rc;
}

static void
weld_free_topic_parts(weld_topic_parts *parts)
{
	nng_strfree(parts->version);
	nng_strfree(parts->site_id);
	nng_strfree(parts->line_id);
	nng_strfree(parts->station_id);
	nng_strfree(parts->gateway_id);
	nng_strfree(parts->metric_group);
	nng_strfree(parts->device_id);
}

static int
weld_build_field_indices(cJSON *fields, weld_field_indices *indices)
{
	int size = 0;

	if (fields == NULL || indices == NULL) {
		return -1;
	}

	indices->temp_idx = -1;
	indices->hum_idx = -1;
	indices->flow_idx = -1;
	indices->total_flow_idx = -1;
	indices->current_idx = -1;
	indices->voltage_idx = -1;

	size = cJSON_GetArraySize(fields);
	for (int i = 0; i < size; ++i) {
		cJSON *field = cJSON_GetArrayItem(fields, i);
		cJSON *name = cJSON_GetObjectItem(field, "name");
		const char *field_name = NULL;
		if (!cJSON_IsString(name)) {
			continue;
		}

		field_name = cJSON_GetStringValue(name);
		if (strcmp(field_name, "temperature") == 0) {
			indices->temp_idx = i;
		} else if (strcmp(field_name, "humidity") == 0) {
			indices->hum_idx = i;
		} else if (strcmp(field_name, "instant_flow") == 0) {
			indices->flow_idx = i;
		} else if (strcmp(field_name, "total_flow") == 0) {
			indices->total_flow_idx = i;
		} else if (strcmp(field_name, "current") == 0) {
			indices->current_idx = i;
		} else if (strcmp(field_name, "voltage") == 0) {
			indices->voltage_idx = i;
		}
	}

	return 0;
}

static int
weld_compare_int64(const void *lhs, const void *rhs)
{
	const int64_t left = *(const int64_t *) lhs;
	const int64_t right = *(const int64_t *) rhs;

	if (left < right) {
		return -1;
	}
	if (left > right) {
		return 1;
	}
	return 0;
}

static int
weld_get_optional_int(cJSON *obj, const char *key, int *value)
{
	cJSON *item = cJSON_GetObjectItem(obj, key);
	if (!cJSON_IsNumber(item)) {
		return 0;
	}
	*value = (int) cJSON_GetNumberValue(item);
	return 1;
}

static void
weld_fill_power_row_metadata(weld_taos_row *row, cJSON *root)
{
	cJSON *raw = NULL;
	cJSON *cal = NULL;

	if (row == NULL || root == NULL) {
		return;
	}

	raw = cJSON_GetObjectItem(root, "raw");
	cal = cJSON_GetObjectItem(root, "calibration");

	if (cJSON_IsObject(raw)) {
		cJSON *adc_unit = cJSON_GetObjectItem(raw, "adc_unit");
		row->raw_adc_unit = cJSON_IsString(adc_unit) ?
		    cJSON_GetStringValue(adc_unit) : NULL;
	}
	if (cJSON_IsObject(cal)) {
		cJSON *version = cJSON_GetObjectItem(cal, "version");
		cJSON *k = cJSON_GetObjectItem(cal, "k");
		cJSON *b = cJSON_GetObjectItem(cal, "b");
		row->cal_version = cJSON_IsString(version) ?
		    cJSON_GetStringValue(version) : NULL;
		if (cJSON_IsNumber(k)) {
			row->has_cal_k = 1;
			row->cal_k = cJSON_GetNumberValue(k);
		}
		if (cJSON_IsNumber(b)) {
			row->has_cal_b = 1;
			row->cal_b = cJSON_GetNumberValue(b);
		}
	}
}

static int
weld_fill_row_signal_values_from_json(weld_taos_row *row, cJSON *root,
    const char *signal_type, cJSON *values, const weld_field_indices *field_indices)
{
	double value = 0.0;

	if (row == NULL || root == NULL || signal_type == NULL || values == NULL ||
	    field_indices == NULL) {
		return -1;
	}

	if (strcmp(signal_type, "environment") == 0) {
		if (weld_read_point_value(
		        values, field_indices->temp_idx, "temperature", 1, &value) <= 0) {
			return -1;
		}
		row->has_temperature = 1;
		row->temperature = value;
		if (weld_read_point_value(
		        values, field_indices->hum_idx, "humidity", 1, &value) <= 0) {
			return -1;
		}
		row->has_humidity = 1;
		row->humidity = value;
		return 0;
	}

	if (strcmp(signal_type, "gas_flow") == 0) {
		if (weld_read_point_value(
		        values, field_indices->flow_idx, "instant_flow", 1, &value) <= 0) {
			return -1;
		}
		row->has_instant_flow = 1;
		row->instant_flow = value;
		if (weld_read_point_value(values, field_indices->total_flow_idx,
		        "total_flow", 0, &value) > 0) {
			row->has_total_flow = 1;
			row->total_flow = value;
		}
		return 0;
	}

	if (strcmp(signal_type, "current") == 0) {
		if (weld_read_point_value(
		        values, field_indices->current_idx, "current", 1, &value) <= 0) {
			return -1;
		}
		row->has_current = 1;
		row->current = value;
		weld_fill_power_row_metadata(row, root);
		return 0;
	}

	if (strcmp(signal_type, "voltage") == 0) {
		if (weld_read_point_value(
		        values, field_indices->voltage_idx, "voltage", 1, &value) <= 0) {
			return -1;
		}
		row->has_voltage = 1;
		row->voltage = value;
		weld_fill_power_row_metadata(row, root);
		return 0;
	}

	log_error("weld_telemetry: unsupported signal_type");
	return -1;
}

static int
weld_fill_row_signal_value_from_uniform_sample(weld_taos_row *row, cJSON *root,
    const char *signal_type, double sample_value)
{
	if (row == NULL || root == NULL || signal_type == NULL) {
		return -1;
	}

	if (strcmp(signal_type, "current") == 0) {
		row->has_current = 1;
		row->current = sample_value;
		weld_fill_power_row_metadata(row, root);
		return 0;
	}

	if (strcmp(signal_type, "voltage") == 0) {
		row->has_voltage = 1;
		row->voltage = sample_value;
		weld_fill_power_row_metadata(row, root);
		return 0;
	}

	log_error("weld_telemetry: uniform_series_binary only supports current/voltage");
	return -1;
}

static int
weld_validate_uniform_binary_data(cJSON *root, cJSON *data, cJSON *fields,
    const char *signal_type, int *point_count_out, int64_t *start_us_out,
    int64_t *sample_rate_hz_out, const char **encoding_out,
    const char **payload_out)
{
	cJSON *layout = NULL;
	cJSON *window = NULL;
	cJSON *point_count_item = NULL;
	cJSON *window_point_count = NULL;
	cJSON *start_us = NULL;
	cJSON *sample_rate_hz = NULL;
	cJSON *encoding = NULL;
	cJSON *payload = NULL;
	cJSON *value_type = NULL;
	cJSON *byte_order = NULL;
	int    point_count = 0;
	int    window_count = 0;

	if (root == NULL || data == NULL || fields == NULL || signal_type == NULL ||
	    point_count_out == NULL || start_us_out == NULL ||
	    sample_rate_hz_out == NULL || encoding_out == NULL ||
	    payload_out == NULL) {
		return -1;
	}

	layout = cJSON_GetObjectItem(data, "layout");
	if (!cJSON_IsString(layout) ||
	    strcmp(cJSON_GetStringValue(layout), "uniform_series_binary") != 0) {
		log_error("weld_telemetry: unsupported data layout without points");
		return -1;
	}

	if (strcmp(signal_type, "current") != 0 &&
	    strcmp(signal_type, "voltage") != 0) {
		log_error("weld_telemetry: uniform_series_binary only supports current/voltage");
		return -1;
	}

	if (cJSON_GetArraySize(fields) != 1) {
		log_error("weld_telemetry: uniform_series_binary currently requires exactly one field");
		return -1;
	}

	point_count_item = cJSON_GetObjectItem(data, "point_count");
	window = cJSON_GetObjectItem(data, "window");
	if (!cJSON_IsObject(window)) {
		window = cJSON_GetObjectItem(root, "window");
	}
	window_point_count = window != NULL ? cJSON_GetObjectItem(window, "point_count") : NULL;
	start_us = window != NULL ? cJSON_GetObjectItem(window, "start_us") : NULL;
	sample_rate_hz =
	    window != NULL ? cJSON_GetObjectItem(window, "sample_rate_hz") : NULL;
	encoding = cJSON_GetObjectItem(data, "encoding");
	payload = cJSON_GetObjectItem(data, "payload");
	value_type = cJSON_GetObjectItem(data, "value_type");
	byte_order = cJSON_GetObjectItem(data, "byte_order");

	if (!cJSON_IsObject(window) || !cJSON_IsNumber(point_count_item) ||
	    !cJSON_IsNumber(window_point_count) || !cJSON_IsNumber(start_us) ||
	    !cJSON_IsNumber(sample_rate_hz) || !cJSON_IsString(encoding) ||
	    !cJSON_IsString(payload)) {
		log_error("weld_telemetry: uniform_series_binary missing required fields");
		return -1;
	}

	point_count = (int) cJSON_GetNumberValue(point_count_item);
	window_count = (int) cJSON_GetNumberValue(window_point_count);
	if (point_count <= 0 || point_count != window_count) {
		log_error("weld_telemetry: uniform_series_binary point_count mismatch");
		return -1;
	}

	if (cJSON_IsString(value_type) &&
	    strcmp(cJSON_GetStringValue(value_type), "float32") != 0) {
		log_error("weld_telemetry: unsupported uniform value_type");
		return -1;
	}
	if (cJSON_IsString(byte_order) &&
	    strcmp(cJSON_GetStringValue(byte_order), "little_endian") != 0) {
		log_error("weld_telemetry: unsupported uniform byte_order");
		return -1;
	}

	if ((int64_t) cJSON_GetNumberValue(sample_rate_hz) <= 0) {
		log_error("weld_telemetry: invalid uniform sample_rate_hz");
		return -1;
	}

	*point_count_out = point_count;
	*start_us_out = (int64_t) cJSON_GetNumberValue(start_us);
	*sample_rate_hz_out = (int64_t) cJSON_GetNumberValue(sample_rate_hz);
	*encoding_out = cJSON_GetStringValue(encoding);
	*payload_out = cJSON_GetStringValue(payload);

	// Validate payload length
	if (*payload_out != NULL) {
		size_t payload_len = strlen(*payload_out);
		if (payload_len > WELD_RAW_PAYLOAD_MAX_BYTES) {
			log_error("weld_telemetry: payload too large (%zu > %d bytes)",
			    payload_len, WELD_RAW_PAYLOAD_MAX_BYTES);
			return -1;
		}
	}

	return 0;
}

static int64_t
weld_uniform_point_ts_us(int64_t start_us, int64_t sample_rate_hz, int index)
{
	uint64_t offset_us =
	    ((uint64_t) index * 1000000ULL) / (uint64_t) sample_rate_hz;
	return start_us + (int64_t) offset_us;
}

static size_t
weld_base64_decode_capacity(size_t encoded_len)
{
	return ((encoded_len + 3U) / 4U) * 3U + 4U;
}

static double
weld_decode_float32_le(const uint8_t *data)
{
	float value = 0.0f;
#if defined(__BYTE_ORDER__) && (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
	uint8_t tmp[sizeof(float)];
	tmp[0] = data[3];
	tmp[1] = data[2];
	tmp[2] = data[1];
	tmp[3] = data[0];
	memcpy(&value, tmp, sizeof(value));
#else
	memcpy(&value, data, sizeof(value));
#endif
	return (double) value;
}

static int
weld_decode_uniform_binary_payload(const char *encoding,
    const char *payload_base64, int point_count, uint8_t **samples_out,
    size_t *sample_len_out)
{
	size_t   encoded_len = 0;
	size_t   decode_cap = 0;
	size_t   expected_len = 0;
	uint8_t *decoded = NULL;
	uint8_t *raw = NULL;
	unsigned int raw_len = 0;

	if (encoding == NULL || payload_base64 == NULL || point_count <= 0 ||
	    samples_out == NULL || sample_len_out == NULL) {
		return -1;
	}

	if ((size_t) point_count > ((size_t) -1) / sizeof(float)) {
		log_error("weld_telemetry: uniform point_count too large");
		return -1;
	}

	expected_len = (size_t) point_count * sizeof(float);
	encoded_len = strlen(payload_base64);
	decode_cap = weld_base64_decode_capacity(encoded_len);
	raw = (uint8_t *) nng_alloc(decode_cap);
	if (raw == NULL) {
		return -1;
	}

	raw_len = base64_decode(payload_base64, (unsigned int) encoded_len, raw);
	if (raw_len == 0) {
		log_error("weld_telemetry: uniform payload base64 decode failed");
		goto fail;
	}

	if (strcmp(encoding, "base64") == 0) {
		if ((size_t) raw_len != expected_len) {
			log_error("weld_telemetry: uniform payload length mismatch");
			goto fail;
		}
		*samples_out = raw;
		*sample_len_out = (size_t) raw_len;
		return 0;
	}

	if (strcmp(encoding, "zstd_base64_f32_le") == 0) {
#if defined(NANOMQ_HAS_ZSTD)
		size_t rv;

		decoded = (uint8_t *) nng_alloc(expected_len);
		if (decoded == NULL) {
			goto fail;
		}

		rv = ZSTD_decompress(decoded, expected_len, raw, (size_t) raw_len);
		if (ZSTD_isError(rv) || rv != expected_len) {
			log_error("weld_telemetry: uniform payload zstd decode failed");
			goto fail;
		}

		nng_free(raw, decode_cap);
		*samples_out = decoded;
		*sample_len_out = rv;
		return 0;
#else
		log_error("weld_telemetry: zstd payload decoding not enabled in this build");
		goto fail;
#endif
	}

	log_error("weld_telemetry: unsupported uniform encoding");

fail:
	if (decoded != NULL) {
		nng_free(decoded, expected_len);
	}
	if (raw != NULL) {
		nng_free(raw, decode_cap);
	}
	return -1;
}

static int
weld_fill_common_row(
	weld_taos_row *row, const rule_taos *taos_rule, const pub_packet_struct *pub_packet,
	const weld_topic_parts *topic_parts, cJSON *root, int64_t recv_ts_us)
{
	cJSON *msg_class   = cJSON_GetObjectItem(root, "msg_class");
	cJSON *msg_ts_ms   = cJSON_GetObjectItem(root, "ts_ms");
	cJSON *gateway_id  = cJSON_GetObjectItem(root, "gateway_id");
	cJSON *device_id   = cJSON_GetObjectItem(root, "device_id");
	cJSON *msg_id      = cJSON_GetObjectItem(root, "msg_id");
	cJSON *spec_ver    = cJSON_GetObjectItem(root, "spec_ver");
	cJSON *task_id     = cJSON_GetObjectItem(root, "task_id");
	cJSON *seq         = cJSON_GetObjectItem(root, "seq");
	cJSON *device_type = cJSON_GetObjectItem(root, "device_type");
	cJSON *device_model = cJSON_GetObjectItem(root, "device_model");
	cJSON *signal_type = cJSON_GetObjectItem(root, "signal_type");
	cJSON *channel_id  = cJSON_GetObjectItem(root, "channel_id");
	cJSON *quality     = cJSON_GetObjectItem(root, "quality");
	cJSON *source      = cJSON_GetObjectItem(root, "source");
	cJSON *collect     = cJSON_GetObjectItem(root, "collect");
	cJSON *quality_code = quality != NULL ? cJSON_GetObjectItem(quality, "code") : NULL;
	cJSON *quality_text = quality != NULL ? cJSON_GetObjectItem(quality, "text") : NULL;
	const char *expected_table = NULL;
	const char *expected_device_type = NULL;

	if (!cJSON_IsString(msg_class) ||
	    strcmp(cJSON_GetStringValue(msg_class), "telemetry") != 0) {
		log_error("weld_telemetry: msg_class must be telemetry");
		return -1;
	}
	if (!cJSON_IsString(gateway_id) ||
	    strcmp(cJSON_GetStringValue(gateway_id), topic_parts->gateway_id) != 0) {
		log_error("weld_telemetry: gateway_id mismatch");
		return -1;
	}
	if (!cJSON_IsString(device_id) ||
	    strcmp(cJSON_GetStringValue(device_id), topic_parts->device_id) != 0) {
		log_error("weld_telemetry: device_id mismatch");
		return -1;
	}
	if (!cJSON_IsString(msg_id) || !cJSON_IsString(spec_ver) ||
	    !cJSON_IsNumber(msg_ts_ms) || !cJSON_IsNumber(seq) ||
	    !cJSON_IsString(device_type) ||
	    !cJSON_IsString(signal_type) || !cJSON_IsNumber(quality_code)) {
		log_error("weld_telemetry: missing common required fields");
		return -1;
	}

	expected_table = weld_expected_table(cJSON_GetStringValue(signal_type));
	if (expected_table == NULL ||
	    !weld_table_matches_signal_type(
	        taos_rule->table, cJSON_GetStringValue(signal_type))) {
		log_error("weld_telemetry: signal_type and target table mismatch");
		return -1;
	}
	if (weld_expected_device_type(
	        cJSON_GetStringValue(signal_type), &expected_device_type) != 0 ||
	    strcmp(cJSON_GetStringValue(device_type), expected_device_type) != 0) {
		log_error("weld_telemetry: device_type and signal_type mismatch");
		return -1;
	}
	if (weld_validate_metric_group(topic_parts->metric_group,
	        cJSON_GetStringValue(signal_type)) != 0) {
		log_error("weld_telemetry: metric_group and signal_type mismatch");
		return -1;
	}

	memset(row, 0, sizeof(*row));
	row->stable      = taos_rule->table;
	row->recv_ts_us  = recv_ts_us;
	row->msg_id      = cJSON_GetStringValue(msg_id);
	row->seq         = (int64_t) cJSON_GetNumberValue(seq);
	row->topic_name  = pub_packet->var_header.publish.topic_name.body;
	row->spec_ver    = cJSON_GetStringValue(spec_ver);
	row->task_id     = (cJSON_IsString(task_id) &&
	        cJSON_GetStringValue(task_id) != NULL &&
	        cJSON_GetStringValue(task_id)[0] != '\0') ?
	    cJSON_GetStringValue(task_id) : NULL;
	row->has_quality_code = 1;
	row->quality_code = (int) cJSON_GetNumberValue(quality_code);
	row->quality_text = cJSON_IsString(quality_text) ?
	    cJSON_GetStringValue(quality_text) : NULL;
	row->qos         = pub_packet->fixed_header.qos;
	row->packet_id   = pub_packet->var_header.publish.packet_id;
	row->version     = topic_parts->version;
	row->site_id     = topic_parts->site_id;
	row->line_id     = topic_parts->line_id;
	row->station_id  = topic_parts->station_id;
	row->gateway_id  = topic_parts->gateway_id;
	row->device_id   = topic_parts->device_id;
	row->device_type = cJSON_GetStringValue(device_type);
	row->device_model = cJSON_IsString(device_model) ?
	    cJSON_GetStringValue(device_model) : NULL;
	row->metric_group = topic_parts->metric_group;
	row->signal_type  = cJSON_GetStringValue(signal_type);
	row->channel_id   = cJSON_IsString(channel_id) ?
	    cJSON_GetStringValue(channel_id) : NULL;

	if (cJSON_IsObject(source)) {
		cJSON *bus = cJSON_GetObjectItem(source, "bus");
		cJSON *port = cJSON_GetObjectItem(source, "port");
		cJSON *protocol = cJSON_GetObjectItem(source, "protocol");
		row->source_bus = cJSON_IsString(bus) ? cJSON_GetStringValue(bus) : NULL;
		row->source_port = cJSON_IsString(port) ? cJSON_GetStringValue(port) : NULL;
		row->source_protocol = cJSON_IsString(protocol) ?
		    cJSON_GetStringValue(protocol) : NULL;
	}
	if (cJSON_IsObject(collect)) {
		int value = 0;
		if (weld_get_optional_int(collect, "period_ms", &value)) {
			row->has_collect_period_ms = 1;
			row->collect_period_ms = value;
		}
		if (weld_get_optional_int(collect, "timeout_ms", &value)) {
			row->has_collect_timeout_ms = 1;
			row->collect_timeout_ms = value;
		}
		if (weld_get_optional_int(collect, "retries", &value)) {
			row->has_collect_retries = 1;
			row->collect_retries = value;
		}
	}

	return 0;
}

static int
weld_fill_raw_power_row(weld_taos_row *row, const rule_taos *taos_rule,
	const pub_packet_struct *pub_packet, const weld_topic_parts *topic_parts,
	cJSON *root, cJSON *data, cJSON *fields, int64_t recv_ts_us)
{
	const char *signal_type = NULL;
	const char *encoding = NULL;
	const char *payload = NULL;
	int         point_count = 0;
	int64_t     window_start_us = 0;
	int64_t     sample_rate_hz = 0;

	if (row == NULL || taos_rule == NULL || pub_packet == NULL ||
	    topic_parts == NULL || root == NULL || data == NULL || fields == NULL) {
		return -1;
	}

	{
		cJSON *signal_type_item = cJSON_GetObjectItem(root, "signal_type");
		if (!cJSON_IsString(signal_type_item)) {
			log_error("weld_telemetry: signal_type missing");
			return -1;
		}
		signal_type = cJSON_GetStringValue(signal_type_item);
	}

	if (!weld_is_raw_power_table(taos_rule->table)) {
		log_error("weld_telemetry: raw power handler requires raw table");
		return -1;
	}
	if (weld_validate_uniform_binary_data(root, data, fields, signal_type,
	        &point_count, &window_start_us, &sample_rate_hz, &encoding,
	        &payload) != 0) {
		return -1;
	}
	if (weld_fill_common_row(
	        row, taos_rule, pub_packet, topic_parts, root, recv_ts_us) != 0) {
		return -1;
	}

	row->ts_us = window_start_us;
	row->has_window_start_us = 1;
	row->window_start_us = window_start_us;
	row->has_sample_rate_hz = 1;
	row->sample_rate_hz = (int) sample_rate_hz;
	row->has_point_count = 1;
	row->point_count = point_count;
	row->encoding = encoding;
	row->payload = payload;
	weld_fill_power_row_metadata(row, root);
	return 0;
}

int
weld_telemetry_handle_publish(
	const rule_taos *taos_rule, const struct pub_packet_struct *pub_packet)
{
	weld_topic_parts topic_parts;
	taos_sink_config sink_cfg;
	cJSON          *root = NULL;
	cJSON          *data = NULL;
	cJSON          *fields = NULL;
	cJSON          *points = NULL;
	cJSON          *signal_type = NULL;
	const char     *signal_type_str = NULL;
	const char     *uniform_encoding = NULL;
	const char     *uniform_payload = NULL;
	uint8_t        *uniform_samples = NULL;
	size_t          uniform_sample_len = 0;
	int             rc = -1;
	int64_t         recv_ts_us = 0;
	int             use_ts_us = 0;
	int             use_uniform_binary = 0;
	int             point_count = 0;
	int64_t         uniform_start_us = 0;
	int64_t         uniform_sample_rate_hz = 0;
	int64_t         stats_now_us = weld_now_epoch_us();
	int64_t        *ts_seen = NULL;
	weld_taos_row  *rows = NULL;
	weld_field_indices field_indices;

	memset(&topic_parts, 0, sizeof(topic_parts));

	if (taos_rule == NULL || pub_packet == NULL ||
	    pub_packet->var_header.publish.topic_name.body == NULL ||
	    pub_packet->payload.data == NULL || pub_packet->payload.len == 0) {
		return -1;
	}

	if (weld_parse_topic(
	        pub_packet->var_header.publish.topic_name.body, &topic_parts) != 0) {
		log_error("weld_telemetry: invalid topic");
		goto done;
	}

	root = cJSON_ParseWithLength(
	    (const char *) pub_packet->payload.data, pub_packet->payload.len);
	if (root == NULL) {
		log_error("weld_telemetry: payload is not valid json");
		goto done;
	}

	data = cJSON_GetObjectItem(root, "data");
	fields = data != NULL ? cJSON_GetObjectItem(data, "fields") : NULL;
	points = data != NULL ? cJSON_GetObjectItem(data, "points") : NULL;
	signal_type = cJSON_GetObjectItem(root, "signal_type");

	if (!cJSON_IsString(signal_type)) {
		log_error("weld_telemetry: signal_type missing");
		goto done;
	}
	signal_type_str = cJSON_GetStringValue(signal_type);

	if (!cJSON_IsObject(data) || !cJSON_IsArray(fields)) {
		log_error("weld_telemetry: data.fields missing");
		goto done;
	}

	memset(&sink_cfg, 0, sizeof(sink_cfg));
	sink_cfg.host = taos_rule->host;
	sink_cfg.port = taos_rule->port > 0 ? taos_rule->port : 6041;
	sink_cfg.username = taos_rule->username;
	sink_cfg.password = taos_rule->password;
	sink_cfg.db = taos_rule->db;
	sink_cfg.stable = taos_rule->table;

	if (weld_is_raw_power_table(taos_rule->table)) {
		weld_taos_row row;
		int accept_rc;

		point_count = 1;
		accept_rc = weld_taos_sink_can_accept_rows_with_config(&sink_cfg, 1);
		if (accept_rc < 0) {
			log_error("weld_telemetry: raw sink capacity check failed");
			goto done;
		}
		if (accept_rc == 0) {
			log_warn("weld_telemetry: raw sink overloaded, reject message "
			         "(table=%s, topic=%s)",
			    taos_rule->table,
			    pub_packet->var_header.publish.topic_name.body);
			weld_stats_inc(&g_weld_telemetry_stats.sink_overload_reject_count);
			goto done;
		}

		recv_ts_us = weld_now_epoch_us();
		if (weld_fill_raw_power_row(
		        &row, taos_rule, pub_packet, &topic_parts, root, data, fields,
		        recv_ts_us) != 0) {
			goto done;
		}
		if (weld_taos_sink_enqueue_row_with_config(&sink_cfg, &row) != 0) {
			log_error("weld_telemetry: failed to enqueue raw power row");
			weld_stats_inc(&g_weld_telemetry_stats.sink_enqueue_fail_count);
			goto done;
		}

			rc = 0;
			weld_stats_inc(&g_weld_telemetry_stats.accept_msg_count);
			weld_stats_inc_table_accept(taos_rule->table);
			weld_stats_add(&g_weld_telemetry_stats.accept_point_count,
			    (uint64_t) row.point_count);
		goto done;
	}

	if (cJSON_IsArray(points)) {
		point_count = cJSON_GetArraySize(points);
		{
			cJSON *point_count_item = cJSON_GetObjectItem(data, "point_count");
			if (!cJSON_IsNumber(point_count_item) ||
			    (int) cJSON_GetNumberValue(point_count_item) != point_count) {
				log_error("weld_telemetry: point_count mismatch");
				goto done;
			}
		}

		if (point_count <= 0) {
			log_error("weld_telemetry: empty points");
			goto done;
		}
	} else {
		use_uniform_binary = 1;
			if (weld_validate_uniform_binary_data(root, data, fields, signal_type_str,
		        &point_count, &uniform_start_us, &uniform_sample_rate_hz,
		        &uniform_encoding, &uniform_payload) != 0) {
			goto done;
		}
	}

	if (!use_uniform_binary) {
		cJSON *first_point = cJSON_GetArrayItem(points, 0);
		int has_ts_us = cJSON_GetObjectItem(first_point, "ts_us") != NULL;
		int has_ts_ms = cJSON_GetObjectItem(first_point, "ts_ms") != NULL;
		if (has_ts_us == has_ts_ms) {
			log_error(
			    "weld_telemetry: each point must contain exactly one of ts_us or ts_ms");
			goto done;
		}
		use_ts_us = has_ts_us;
	}

	if (weld_build_field_indices(fields, &field_indices) != 0) {
		log_error("weld_telemetry: failed to build field indices");
		goto done;
	}

	if (strcmp(signal_type_str, "environment") == 0) {
		if (field_indices.temp_idx < 0 || field_indices.hum_idx < 0) {
			log_error("weld_telemetry: environment fields missing");
			goto done;
		}
	} else if (strcmp(signal_type_str, "gas_flow") == 0) {
		if (field_indices.flow_idx < 0) {
			log_error("weld_telemetry: gas flow field missing");
			goto done;
		}
	} else if (strcmp(signal_type_str, "current") == 0) {
		if (field_indices.current_idx < 0) {
			log_error("weld_telemetry: current field missing");
			goto done;
		}
	} else if (strcmp(signal_type_str, "voltage") == 0) {
		if (field_indices.voltage_idx < 0) {
			log_error("weld_telemetry: voltage field missing");
			goto done;
		}
	} else {
		log_error("weld_telemetry: unsupported signal_type");
		goto done;
	}

	if (!use_ts_us &&
	    !use_uniform_binary &&
	    (strcmp(signal_type_str, "current") == 0 ||
	        strcmp(signal_type_str, "voltage") == 0) &&
	    point_count > 1) {
		log_warn("weld_telemetry: high-frequency power message uses ts_ms; "
		         "unique ts_us is recommended to avoid timestamp collisions "
		         "(signal_type=%s, points=%d, topic=%s)",
		    signal_type_str, point_count,
		    pub_packet->var_header.publish.topic_name.body);
		weld_stats_inc(&g_weld_telemetry_stats.high_freq_ts_ms_warn_count);
	}

	{
		int accept_rc =
		    weld_taos_sink_can_accept_rows_with_config(&sink_cfg, (size_t) point_count);
		if (accept_rc < 0) {
			log_error("weld_telemetry: sink capacity check failed");
			goto done;
		}
		if (accept_rc == 0) {
			log_warn("weld_telemetry: sink overloaded, reject message before row expansion "
			         "(signal_type=%s, points=%d, topic=%s)",
			    signal_type_str, point_count,
			    pub_packet->var_header.publish.topic_name.body);
			weld_stats_inc(&g_weld_telemetry_stats.sink_overload_reject_count);
			goto done;
		}
	}

	if (use_uniform_binary &&
	    weld_decode_uniform_binary_payload(uniform_encoding, uniform_payload,
	        point_count, &uniform_samples, &uniform_sample_len) != 0) {
		goto done;
	}

	ts_seen = (int64_t *) nng_alloc((size_t) point_count * sizeof(int64_t));
	if (ts_seen == NULL) {
		goto done;
	}
	rows = (weld_taos_row *) nng_zalloc(
	    (size_t) point_count * sizeof(weld_taos_row));
	if (rows == NULL) {
		goto done;
	}
	recv_ts_us = weld_now_epoch_us();

	for (int i = 0; i < point_count; ++i) {
		weld_taos_row *row = &rows[i];
		int64_t ts_us = 0;
		ts_seen[i] = 0;

		if (!use_uniform_binary) {
			cJSON *point = cJSON_GetArrayItem(points, i);
			cJSON *values = cJSON_GetObjectItem(point, "values");

			if (!cJSON_IsObject(point) || !cJSON_IsArray(values)) {
				log_error("weld_telemetry: invalid point object");
				goto done;
			}
			if ((int) cJSON_GetArraySize(values) != cJSON_GetArraySize(fields)) {
				log_error("weld_telemetry: values length mismatch");
				goto done;
			}

			if (weld_validate_point_timestamp(point, use_ts_us, &ts_us) != 0) {
				goto done;
			}
			ts_seen[i] = ts_us;

			if (weld_fill_common_row(
			        row, taos_rule, pub_packet, &topic_parts, root, recv_ts_us) != 0) {
				goto done;
			}
			row->ts_us = ts_us;
			if (weld_fill_row_signal_values_from_json(
			        row, root, signal_type_str, values, &field_indices) != 0) {
				goto done;
			}
			continue;
		}

		if (weld_fill_common_row(
		        row, taos_rule, pub_packet, &topic_parts, root, recv_ts_us) != 0) {
			goto done;
		}
		ts_us = weld_uniform_point_ts_us(
		    uniform_start_us, uniform_sample_rate_hz, i);
		row->ts_us = ts_us;
		ts_seen[i] = ts_us;
		if (weld_fill_row_signal_value_from_uniform_sample(row, root,
		        signal_type_str,
		        weld_decode_float32_le(
		            uniform_samples + ((size_t) i * sizeof(float)))) != 0) {
			goto done;
		}
	}

	qsort(ts_seen, (size_t) point_count, sizeof(int64_t), weld_compare_int64);
	for (int i = 1; i < point_count; ++i) {
		if (ts_seen[i - 1] == ts_seen[i]) {
			log_error("weld_telemetry: duplicate point timestamp within message "
			         "(signal_type=%s, points=%d, topic=%s)",
			    signal_type_str, point_count,
			    pub_packet->var_header.publish.topic_name.body);
			weld_stats_inc(&g_weld_telemetry_stats.duplicate_ts_reject_count);
			goto done;
		}
	}

	if (weld_taos_sink_enqueue_rows_with_config(
	        &sink_cfg, rows, (size_t) point_count) != 0) {
		log_error("weld_telemetry: failed to enqueue structured rows");
		weld_stats_inc(&g_weld_telemetry_stats.sink_enqueue_fail_count);
		goto done;
	}

	rc = 0;
	weld_stats_inc(&g_weld_telemetry_stats.accept_msg_count);
	weld_stats_inc_table_accept(taos_rule->table);
	weld_stats_add(
	    &g_weld_telemetry_stats.accept_point_count, (uint64_t) point_count);

done:
	if (rc != 0) {
		weld_stats_inc(&g_weld_telemetry_stats.fail_msg_count);
	}
	weld_log_stats_if_due(stats_now_us);
	if (ts_seen != NULL) {
		nng_free(ts_seen, (size_t) point_count * sizeof(int64_t));
	}
	if (rows != NULL) {
		nng_free(rows, (size_t) point_count * sizeof(weld_taos_row));
	}
	if (root != NULL) {
		cJSON_Delete(root);
	}
	if (uniform_samples != NULL) {
		nng_free(uniform_samples, uniform_sample_len);
	}
	weld_free_topic_parts(&topic_parts);
	return rc;
}

int
weld_telemetry_handle_publish_raw(const rule_taos *taos_rule,
	const char *topic_name, uint8_t qos, uint16_t packet_id,
	const uint8_t *payload, uint32_t payload_len)
{
	struct pub_packet_struct packet;

	memset(&packet, 0, sizeof(packet));
	packet.fixed_header.qos = qos;
	packet.var_header.publish.packet_id = packet_id;
	packet.var_header.publish.topic_name.body = (char *) topic_name;
	packet.var_header.publish.topic_name.len =
	    topic_name != NULL ? (uint16_t) strlen(topic_name) : 0;
	packet.payload.data = (uint8_t *) payload;
	packet.payload.len = payload_len;

	return weld_telemetry_handle_publish(taos_rule, &packet);
}
