# 焊接遥测运维交付清单

本文档用于给运维提供焊接遥测接入所需的最小交付物。

## 1. 交付文件

- Debian 安装包: `public/0.24.14/nanomq-nng-v0.24.14.deb`
- MQTT 规则配置: [etc/nanomq_weld_taos.conf](/home/tery/project/nanomq/etc/nanomq_weld_taos.conf)
- TDengine 建库建表脚本: [etc/weld_tdengine_schema.sql](/home/tery/project/nanomq/etc/weld_tdengine_schema.sql)
- TDengine 重建脚本: [etc/weld_tdengine_schema_reset.sql](/home/tery/project/nanomq/etc/weld_tdengine_schema_reset.sql)

## 2. MQTT 规则说明

当前四类遥测数据对应四条规则：

- `telemetry/env` 写入 `weld_env_point`
- `telemetry/flow` 写入 `weld_flow_point`
- `telemetry/power` 且 `payload.signal_type = 'current'` 写入 `weld_current_raw`
- `telemetry/power` 且 `payload.signal_type = 'voltage'` 写入 `weld_voltage_raw`

所有规则统一使用 `parser = "weld_telemetry"`。

## 3. TDengine 表结构说明

当前实现采用：

- 一个数据库: `mqtt_rule`
- 四张超表: `weld_env_point`、`weld_flow_point`、`weld_current_raw`、`weld_voltage_raw`
- `weld_current_raw`/`weld_voltage_raw` 直接写入原始 `data.payload`，NanoMQ 不再为高频电流/电压逐点展开；下游通过 `window_start_us`、`sample_rate_hz`、`point_count` 恢复时间轴
- 子表按消息维度动态创建，无需运维预先建子表
- 数据库精度固定为微秒: `PRECISION 'us'`

注意：

- 程序启动后即使未预建表，也会在首批写入前自动执行 `CREATE DATABASE` 和 `CREATE STABLE`
- 如果 `mqtt_rule` 已存在且精度不是 `us`，`CREATE DATABASE IF NOT EXISTS` 不会修正旧库精度
- 运维若希望提前校验权限、配额和字段，可先执行 `weld_tdengine_schema.sql`
- 若现场已有旧的 `mqtt_rule`，应先执行 `SHOW CREATE DATABASE mqtt_rule` 校验精度；若不是 `PRECISION 'us'`，应使用 `weld_tdengine_schema_reset.sql` 重建或换新库名

## 4. 部署顺序

1. 在 TDengine 执行 [etc/weld_tdengine_schema.sql](/home/tery/project/nanomq/etc/weld_tdengine_schema.sql)
2. 将 NanoMQ 配置切换为 [etc/nanomq_weld_taos.conf](/home/tery/project/nanomq/etc/nanomq_weld_taos.conf)
3. 启动 NanoMQ
4. 用 MQTT 客户端发布四类遥测消息
5. 在 TDengine 查询四张超表和自动创建的子表

## 5. 验证命令

TDengine:

```sql
USE mqtt_rule;
SHOW STABLES;
DESCRIBE weld_env_point;
DESCRIBE weld_flow_point;
DESCRIBE weld_current_raw;
DESCRIBE weld_voltage_raw;
```

NanoMQ:

```bash
nanomq start --conf /usr/local/etc/nanomq_weld_taos.conf --log_level info
```

上线后建议同时观察三段统计日志：

```text
pub_handler weld_taos
weld_telemetry
weld_taos_sink
```
