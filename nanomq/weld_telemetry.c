#include "weld_telemetry.h"

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>

#include "include/pub_handler.h"
#include "nng/supplemental/nanolib/cJSON.h"
#include "nng/supplemental/nanolib/log.h"
#include "weld_taos_sink.hpp"

typedef struct {
	char *version;
	char *site_id;
	char *line_id;
	char *station_id;
	char *gateway_id;
	char *metric_group;
	char *device_id;
} weld_topic_parts;

typedef struct {
	int temp_idx;
	int hum_idx;
	int flow_idx;
	int total_flow_idx;
	int current_idx;
	int voltage_idx;
} weld_field_indices;

static int64_t
weld_now_epoch_us(void)
{
	struct timeval tv;
	gettimeofday(&tv, NULL);
	return (int64_t) tv.tv_sec * 1000000LL + (int64_t) tv.tv_usec;
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
	    strcmp(expected_table, taos_rule->table) != 0) {
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
	row->stable      = expected_table;
	row->recv_ts_us  = recv_ts_us;
	row->msg_id      = cJSON_GetStringValue(msg_id);
	row->seq         = (int64_t) cJSON_GetNumberValue(seq);
	row->topic_name  = pub_packet->var_header.publish.topic_name.body;
	row->spec_ver    = cJSON_GetStringValue(spec_ver);
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
	int             rc = -1;
	int64_t         recv_ts_us = 0;
	int             use_ts_us = 0;
	int             point_count = 0;
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

	if (!cJSON_IsObject(data) || !cJSON_IsArray(fields) || !cJSON_IsArray(points)) {
		log_error("weld_telemetry: data.fields or data.points missing");
		goto done;
	}

	if (!cJSON_IsString(signal_type)) {
		log_error("weld_telemetry: signal_type missing");
		goto done;
	}

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

	memset(&sink_cfg, 0, sizeof(sink_cfg));
	sink_cfg.host = taos_rule->host;
	sink_cfg.port = taos_rule->port > 0 ? taos_rule->port : 6041;
	sink_cfg.username = taos_rule->username;
	sink_cfg.password = taos_rule->password;
	sink_cfg.db = taos_rule->db;
	sink_cfg.stable = taos_rule->table;

	{
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

	if (strcmp(cJSON_GetStringValue(signal_type), "environment") == 0) {
		if (field_indices.temp_idx < 0 || field_indices.hum_idx < 0) {
			log_error("weld_telemetry: environment fields missing");
			goto done;
		}
	} else if (strcmp(cJSON_GetStringValue(signal_type), "gas_flow") == 0) {
		if (field_indices.flow_idx < 0) {
			log_error("weld_telemetry: gas flow field missing");
			goto done;
		}
	} else if (strcmp(cJSON_GetStringValue(signal_type), "current") == 0) {
		if (field_indices.current_idx < 0) {
			log_error("weld_telemetry: current field missing");
			goto done;
		}
	} else if (strcmp(cJSON_GetStringValue(signal_type), "voltage") == 0) {
		if (field_indices.voltage_idx < 0) {
			log_error("weld_telemetry: voltage field missing");
			goto done;
		}
	} else {
		log_error("weld_telemetry: unsupported signal_type");
		goto done;
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
			    cJSON_GetStringValue(signal_type), point_count,
			    pub_packet->var_header.publish.topic_name.body);
			goto done;
		}
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
		cJSON *point = cJSON_GetArrayItem(points, i);
		cJSON *values = cJSON_GetObjectItem(point, "values");
		weld_taos_row *row = &rows[i];
		int64_t ts_us = 0;
		double value = 0.0;

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

			if (strcmp(cJSON_GetStringValue(signal_type), "environment") == 0) {
				if (weld_read_point_value(
				        values, field_indices.temp_idx, "temperature", 1, &value) <= 0) {
					goto done;
				}
				row->has_temperature = 1;
				row->temperature = value;
				if (weld_read_point_value(
				        values, field_indices.hum_idx, "humidity", 1, &value) <= 0) {
					goto done;
				}
				row->has_humidity = 1;
				row->humidity = value;
			} else if (strcmp(cJSON_GetStringValue(signal_type), "gas_flow") == 0) {
				if (weld_read_point_value(
				        values, field_indices.flow_idx, "instant_flow", 1, &value) <= 0) {
					goto done;
				}
				row->has_instant_flow = 1;
				row->instant_flow = value;
				if (weld_read_point_value(
				        values, field_indices.total_flow_idx, "total_flow", 0, &value) > 0) {
					row->has_total_flow = 1;
					row->total_flow = value;
				}
			} else if (strcmp(cJSON_GetStringValue(signal_type), "current") == 0) {
				cJSON *raw = cJSON_GetObjectItem(root, "raw");
				cJSON *cal = cJSON_GetObjectItem(root, "calibration");
				if (weld_read_point_value(
				        values, field_indices.current_idx, "current", 1, &value) <= 0) {
					goto done;
				}
			row->has_current = 1;
			row->current = value;
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
		} else if (strcmp(cJSON_GetStringValue(signal_type), "voltage") == 0) {
				cJSON *raw = cJSON_GetObjectItem(root, "raw");
				cJSON *cal = cJSON_GetObjectItem(root, "calibration");
				if (weld_read_point_value(
				        values, field_indices.voltage_idx, "voltage", 1, &value) <= 0) {
					goto done;
				}
			row->has_voltage = 1;
			row->voltage = value;
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
	}

	if (!use_ts_us) {
		qsort(ts_seen, (size_t) point_count, sizeof(int64_t), weld_compare_int64);
		for (int i = 1; i < point_count; ++i) {
			if (ts_seen[i - 1] == ts_seen[i]) {
				log_error("weld_telemetry: duplicate ts_ms within message");
				goto done;
			}
		}
	}

	if (weld_taos_sink_enqueue_rows_with_config(
	        &sink_cfg, rows, (size_t) point_count) != 0) {
		log_error("weld_telemetry: failed to enqueue structured rows");
		goto done;
	}

	rc = 0;

done:
	if (ts_seen != NULL) {
		nng_free(ts_seen, (size_t) point_count * sizeof(int64_t));
	}
	if (rows != NULL) {
		nng_free(rows, (size_t) point_count * sizeof(weld_taos_row));
	}
	if (root != NULL) {
		cJSON_Delete(root);
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
