-- WARNING:
-- This script is destructive. It drops the existing mqtt_rule database
-- and recreates it with microsecond precision required by weld telemetry.

DROP DATABASE IF EXISTS mqtt_rule;
CREATE DATABASE mqtt_rule PRECISION 'us';

USE mqtt_rule;

CREATE STABLE IF NOT EXISTS weld_env_point (
    ts TIMESTAMP,
    recv_ts TIMESTAMP,
    msg_id NCHAR(128),
    seq BIGINT,
    topic_name NCHAR(256),
    spec_ver NCHAR(32),
    temperature DOUBLE,
    humidity DOUBLE,
    quality_code INT,
    quality_text NCHAR(64),
    source_bus NCHAR(32),
    source_port NCHAR(64),
    source_protocol NCHAR(32),
    collect_period_ms INT,
    collect_timeout_ms INT,
    collect_retries INT,
    qos INT,
    packet_id INT
) TAGS (
    version NCHAR(16),
    site_id NCHAR(64),
    line_id NCHAR(64),
    station_id NCHAR(64),
    gateway_id NCHAR(64),
    device_id NCHAR(64),
    device_type NCHAR(64),
    device_model NCHAR(64),
    metric_group NCHAR(32),
    signal_type NCHAR(32),
    channel_id NCHAR(32)
);

CREATE STABLE IF NOT EXISTS weld_flow_point (
    ts TIMESTAMP,
    recv_ts TIMESTAMP,
    msg_id NCHAR(128),
    seq BIGINT,
    topic_name NCHAR(256),
    spec_ver NCHAR(32),
    instant_flow DOUBLE,
    total_flow DOUBLE,
    quality_code INT,
    quality_text NCHAR(64),
    source_bus NCHAR(32),
    source_port NCHAR(64),
    source_protocol NCHAR(32),
    collect_period_ms INT,
    collect_timeout_ms INT,
    collect_retries INT,
    qos INT,
    packet_id INT
) TAGS (
    version NCHAR(16),
    site_id NCHAR(64),
    line_id NCHAR(64),
    station_id NCHAR(64),
    gateway_id NCHAR(64),
    device_id NCHAR(64),
    device_type NCHAR(64),
    device_model NCHAR(64),
    metric_group NCHAR(32),
    signal_type NCHAR(32),
    channel_id NCHAR(32)
);

CREATE STABLE IF NOT EXISTS weld_current_point (
    ts TIMESTAMP,
    recv_ts TIMESTAMP,
    msg_id NCHAR(128),
    seq BIGINT,
    topic_name NCHAR(256),
    spec_ver NCHAR(32),
    current DOUBLE,
    raw_adc_unit NCHAR(16),
    cal_version NCHAR(64),
    cal_k DOUBLE,
    cal_b DOUBLE,
    quality_code INT,
    quality_text NCHAR(64),
    source_bus NCHAR(32),
    source_port NCHAR(64),
    source_protocol NCHAR(32),
    collect_period_ms INT,
    collect_timeout_ms INT,
    collect_retries INT,
    qos INT,
    packet_id INT
) TAGS (
    version NCHAR(16),
    site_id NCHAR(64),
    line_id NCHAR(64),
    station_id NCHAR(64),
    gateway_id NCHAR(64),
    device_id NCHAR(64),
    device_type NCHAR(64),
    device_model NCHAR(64),
    metric_group NCHAR(32),
    signal_type NCHAR(32),
    channel_id NCHAR(32)
);

CREATE STABLE IF NOT EXISTS weld_voltage_point (
    ts TIMESTAMP,
    recv_ts TIMESTAMP,
    msg_id NCHAR(128),
    seq BIGINT,
    topic_name NCHAR(256),
    spec_ver NCHAR(32),
    voltage DOUBLE,
    raw_adc_unit NCHAR(16),
    cal_version NCHAR(64),
    cal_k DOUBLE,
    cal_b DOUBLE,
    quality_code INT,
    quality_text NCHAR(64),
    source_bus NCHAR(32),
    source_port NCHAR(64),
    source_protocol NCHAR(32),
    collect_period_ms INT,
    collect_timeout_ms INT,
    collect_retries INT,
    qos INT,
    packet_id INT
) TAGS (
    version NCHAR(16),
    site_id NCHAR(64),
    line_id NCHAR(64),
    station_id NCHAR(64),
    gateway_id NCHAR(64),
    device_id NCHAR(64),
    device_type NCHAR(64),
    device_model NCHAR(64),
    metric_group NCHAR(32),
    signal_type NCHAR(32),
    channel_id NCHAR(32)
);
