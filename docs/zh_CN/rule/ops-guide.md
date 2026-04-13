# NanoMQ 焊接遥测接入运维手册

本文档面向运维人员，提供 NanoMQ MQTT 规则配置和 TDengine 数据库表结构的完整操作步骤，无需阅读设计文档。

---

## 1. 前置条件

| 组件 | 版本要求 | 说明 |
|------|----------|------|
| NanoMQ | 含焊接解析链路的构建版本 | 需包含 `weld_telemetry` parser |
| TDengine | 3.x | REST 端口 6041 可达 |
| 网络 | NanoMQ 节点可访问 TDengine REST 接口 | |

---

## 1.1 当前交付建议

当前推荐直接使用 Debian 安装包交付：

- `public/0.24.14/nanomq-nng-v0.24.14.deb`

安装后再覆盖以下交付配置：

- `nanomq_weld_taos.conf`
- `weld_tdengine_schema.sql`
- `nanomq_pwd.conf`

## 2. TDengine 数据库初始化

以下 SQL 按顺序执行一次即可，支持重复执行（均使用 `IF NOT EXISTS`）。

### 2.1 创建数据库

```sql
CREATE DATABASE IF NOT EXISTS mqtt_rule PRECISION 'us';
```

> 精度必须为 `us`（微秒），不可使用默认毫秒精度，否则电流/电压高频点位时间戳会冲突。

> 如果 `mqtt_rule` 已经按旧版焊接表结构创建过，升级到带 `task_id` 的版本前，需要先执行 `ALTER STABLE ... ADD COLUMN task_id NCHAR(128)`，或使用 `etc/weld_tdengine_schema_reset.sql` 重建数据库。

### 2.2 创建温湿度超表

```sql
CREATE STABLE IF NOT EXISTS mqtt_rule.weld_env_point (
    ts                TIMESTAMP,
    recv_ts           TIMESTAMP,
    msg_id            NCHAR(128),
    seq               BIGINT,
    topic_name        NCHAR(256),
    spec_ver          NCHAR(32),
    task_id           NCHAR(128),
    temperature       DOUBLE,
    humidity          DOUBLE,
    quality_code      INT,
    quality_text      NCHAR(64),
    source_bus        NCHAR(32),
    source_port       NCHAR(64),
    source_protocol   NCHAR(32),
    collect_period_ms  INT,
    collect_timeout_ms INT,
    collect_retries    INT,
    qos               INT,
    packet_id         INT
) TAGS (
    version           NCHAR(16),
    site_id           NCHAR(64),
    line_id           NCHAR(64),
    station_id        NCHAR(64),
    gateway_id        NCHAR(64),
    device_id         NCHAR(64),
    device_type       NCHAR(64),
    device_model      NCHAR(64),
    metric_group      NCHAR(32),
    signal_type       NCHAR(32),
    channel_id        NCHAR(32)
);
```

### 2.3 创建气体流量超表

```sql
CREATE STABLE IF NOT EXISTS mqtt_rule.weld_flow_point (
    ts                TIMESTAMP,
    recv_ts           TIMESTAMP,
    msg_id            NCHAR(128),
    seq               BIGINT,
    topic_name        NCHAR(256),
    spec_ver          NCHAR(32),
    task_id           NCHAR(128),
    instant_flow      DOUBLE,
    total_flow        DOUBLE,
    quality_code      INT,
    quality_text      NCHAR(64),
    source_bus        NCHAR(32),
    source_port       NCHAR(64),
    source_protocol   NCHAR(32),
    collect_period_ms  INT,
    collect_timeout_ms INT,
    collect_retries    INT,
    qos               INT,
    packet_id         INT
) TAGS (
    version           NCHAR(16),
    site_id           NCHAR(64),
    line_id           NCHAR(64),
    station_id        NCHAR(64),
    gateway_id        NCHAR(64),
    device_id         NCHAR(64),
    device_type       NCHAR(64),
    device_model      NCHAR(64),
    metric_group      NCHAR(32),
    signal_type       NCHAR(32),
    channel_id        NCHAR(32)
);
```

### 2.4 创建电流原始消息表

```sql
CREATE STABLE IF NOT EXISTS mqtt_rule.weld_current_raw (
    ts                TIMESTAMP,
    recv_ts           TIMESTAMP,
    msg_id            VARCHAR(128),
    seq               BIGINT,
    topic_name        VARCHAR(256),
    spec_ver          VARCHAR(32),
    task_id           VARCHAR(128),
    signal_type       VARCHAR(32),
    qos               INT,
    packet_id         INT,
    window_start_us   BIGINT,
    sample_rate_hz    INT,
    point_count       INT,
    encoding          VARCHAR(32),
    payload           VARCHAR(49152),
    raw_adc_unit      VARCHAR(16),
    cal_version       VARCHAR(64),
    cal_k             DOUBLE,
    cal_b             DOUBLE
) TAGS (
    version           VARCHAR(16),
    site_id           VARCHAR(64),
    line_id           VARCHAR(64),
    station_id        VARCHAR(64),
    gateway_id        VARCHAR(64),
    device_id         VARCHAR(64),
    device_type       VARCHAR(64),
    device_model      VARCHAR(64),
    metric_group      VARCHAR(32),
    channel_id        VARCHAR(32)
);
```

### 2.5 创建电压原始消息表

```sql
CREATE STABLE IF NOT EXISTS mqtt_rule.weld_voltage_raw (
    ts                TIMESTAMP,
    recv_ts           TIMESTAMP,
    msg_id            VARCHAR(128),
    seq               BIGINT,
    topic_name        VARCHAR(256),
    spec_ver          VARCHAR(32),
    task_id           VARCHAR(128),
    signal_type       VARCHAR(32),
    qos               INT,
    packet_id         INT,
    window_start_us   BIGINT,
    sample_rate_hz    INT,
    point_count       INT,
    encoding          VARCHAR(32),
    payload           VARCHAR(49152),
    raw_adc_unit      VARCHAR(16),
    cal_version       VARCHAR(64),
    cal_k             DOUBLE,
    cal_b             DOUBLE
) TAGS (
    version           VARCHAR(16),
    site_id           VARCHAR(64),
    line_id           VARCHAR(64),
    station_id        VARCHAR(64),
    gateway_id        VARCHAR(64),
    device_id         VARCHAR(64),
    device_type       VARCHAR(64),
    device_model      VARCHAR(64),
    metric_group      VARCHAR(32),
    channel_id        VARCHAR(32)
);
```

### 2.6 验证建表结果

```sql
USE mqtt_rule;
SHOW STABLES;
```

预期输出包含以下四张超表：

```
weld_env_point
weld_flow_point
weld_current_raw
weld_voltage_raw
```

---

## 3. NanoMQ MQTT 规则配置

将以下配置写入 `nanomq.conf`（或等价配置文件），替换其中的连接参数后重启 NanoMQ。

### 3.1 完整配置片段

```properties
# 启用规则引擎和 TAOS sink
rule_option=ON
rule_option.taos=enable

# TDengine 连接参数（修改为实际地址）
rule.taos.1.host=127.0.0.1
rule.taos.1.port=6041
rule.taos.1.username=root
rule.taos.1.password=taosdata
rule.taos.1.db=mqtt_rule

# 规则 1：温湿度
rule.taos.event.publish.1.sql="SELECT * FROM \"weld/+/+/+/+/+/telemetry/env/#\""
rule.taos.event.publish.1.table="weld_env_point"
rule.taos.event.publish.1.parser="weld_telemetry"

# 规则 2：气体流量
rule.taos.event.publish.2.sql="SELECT * FROM \"weld/+/+/+/+/+/telemetry/flow/#\""
rule.taos.event.publish.2.table="weld_flow_point"
rule.taos.event.publish.2.parser="weld_telemetry"

# 规则 3：电流原始消息（power 主题，signal_type=current）
rule.taos.event.publish.3.sql="SELECT * FROM \"weld/+/+/+/+/+/telemetry/power/#\" WHERE payload.signal_type = 'current'"
rule.taos.event.publish.3.table="weld_current_raw"
rule.taos.event.publish.3.parser="weld_telemetry"

# 规则 4：电压原始消息（power 主题，signal_type=voltage）
rule.taos.event.publish.4.sql="SELECT * FROM \"weld/+/+/+/+/+/telemetry/power/#\" WHERE payload.signal_type = 'voltage'"
rule.taos.event.publish.4.table="weld_voltage_raw"
rule.taos.event.publish.4.parser="weld_telemetry"
```

### 3.2 配置说明

| 参数 | 说明 |
|------|------|
| `rule_option=ON` | 启用规则引擎 |
| `rule_option.taos=enable` | 启用 TAOS sink |
| `rule.taos.1.host` | TDengine 地址，修改为实际 IP |
| `rule.taos.1.port` | TDengine REST 端口，默认 6041 |
| `rule.taos.1.db` | 目标数据库名，必须与第 2 节建库一致 |
| `table` | 目标超表名，必须是四个超表之一 |
| `parser="weld_telemetry"` | 启用焊接专用解析链路，不可省略 |

### 3.3 注意事项

- 电流和电压共用 `power` 主题前缀，**必须**通过 `WHERE payload.signal_type` 区分，否则同一条消息会同时命中两条规则导致双写
- `parser="weld_telemetry"` 不可省略，省略后退回通用模式，只保存原始 payload 文本，不做结构化解析
- 四条规则编号（`.1.`、`.2.`、`.3.`、`.4.`）必须连续，不可跳号
- 所有四条规则必须指向同一个 `rule.taos.1.*` 连接配置，不支持不同规则写入不同数据库
- 电流/电压 power 规则现在直接写入 `weld_current_raw` / `weld_voltage_raw`，NanoMQ 不再解码 `uniform_series_binary`、展开采样点或重建每个 `ts_us`，只保存 `window_start_us`、`sample_rate_hz`、`point_count` 以及原始 `data.payload` 供下游恢复时间轴

---

## 4. 订阅的 MQTT Topic

四类数据对应的 Topic 格式：

```
weld/{version}/{site_id}/{line_id}/{station_id}/{gateway_id}/telemetry/{metric_group}/{device_id}
```

当前现场冻结的四条 Topic 示例：

| 数据类型 | Topic |
|----------|-------|
| 温湿度 | `weld/v1/sz01/line_01/station_03/gw3568_01/telemetry/env/th01` |
| 气体流量 | `weld/v1/sz01/line_01/station_03/gw3568_01/telemetry/flow/mf01` |
| 电流 | `weld/v1/sz01/line_01/station_03/gw3568_01/telemetry/power/chb01` |
| 电压 | `weld/v1/sz01/line_01/station_03/gw3568_01/telemetry/power/chv01` |

---

## 5. 子表命名规则

子表由 NanoMQ 自动创建，命名格式：

```
{超表名}_{gateway_id}_{device_id}
```

示例：

| 超表 | gateway_id | device_id | 子表名 |
|------|-----------|-----------|--------|
| weld_env_point | gw3568_01 | th01 | `weld_env_point_gw3568_01_th01` |
| weld_flow_point | gw3568_01 | mf01 | `weld_flow_point_gw3568_01_mf01` |
| weld_current_raw | gw3568_01 | chb01 | `weld_current_raw_gw3568_01_chb01` |
| weld_voltage_raw | gw3568_01 | chv01 | `weld_voltage_raw_gw3568_01_chv01` |

> 子表名中非字母数字字符自动替换为 `_`。名称超过 192 字节时自动截断并追加哈希后缀，正常现场设备名不会触发截断。

---

## 6. 启动与验证

### 6.1 启动 NanoMQ

推荐使用 `setsid` 方式后台运行（`nohup` 方式存在已知兼容性问题）：

```bash
setsid ./nanomq start --conf /path/to/nanomq.conf > /var/log/nanomq.log 2>&1 &
```

或使用 systemd 服务管理（推荐生产环境）。

### 6.2 验证数据写入

客户端向任意一条 Topic 发送合法消息后，在 TDengine 中执行：

```sql
-- 查询温湿度最新 10 条
SELECT ts, recv_ts, gateway_id, device_id, temperature, humidity
FROM mqtt_rule.weld_env_point
ORDER BY ts DESC LIMIT 10;

-- 查询电流最新 10 条
SELECT ts, recv_ts, gateway_id, device_id, topic_name, point_count, encoding
FROM mqtt_rule.weld_current_raw
ORDER BY ts DESC LIMIT 10;

-- 查看所有子表
SHOW mqtt_rule.TABLES;
```

### 6.3 常见问题排查

| 现象 | 可能原因 | 排查步骤 |
|------|----------|----------|
| 消息发送后无数据入库 | `parser` 参数未生效或规则未命中 | 检查 NanoMQ 日志中是否有 `weld_telemetry` 相关输出 |
| TDengine 中有表但无数据 | `signal_type` 与规则 `WHERE` 条件不匹配 | 确认消息 payload 中 `signal_type` 字段值 |
| 数据库连接失败 | TDengine REST 端口不通 | `curl http://<host>:6041/rest/sql -u root:taosdata` 验证连通性 |
| 时间戳写入失败 | `ts_ms` 值过小（相对时间而非 epoch） | 确认客户端使用 Unix 时间戳（毫秒），例如 `1700000000000` |
| 电流和电压同时写入同一行 | 规则 3/4 缺少 `WHERE payload.signal_type` | 检查配置，确保规则 3 和 4 均有 WHERE 过滤 |

---

## 7. 字段说明

### 7.1 公共列（四类超表均有）

| 列名 | 类型 | 说明 |
|------|------|------|
| `ts` | TIMESTAMP | 采样点时间戳（微秒精度） |
| `recv_ts` | TIMESTAMP | Broker 接收消息时间戳 |
| `msg_id` | NCHAR(128) | 消息唯一 ID |
| `seq` | BIGINT | 消息序号 |
| `topic_name` | NCHAR(256) | 完整 MQTT Topic |
| `spec_ver` | NCHAR(32) | 数据规范版本 |
| `task_id` | NCHAR(128) | 公共头任务 ID，旧消息为空，当前占位符常见为 `110120119` |
| `quality_code` | INT | 数据质量码 |
| `quality_text` | NCHAR(64) | 数据质量描述（可空） |
| `source_bus` | NCHAR(32) | 采集总线（可空） |
| `source_port` | NCHAR(64) | 采集端口（可空） |
| `source_protocol` | NCHAR(32) | 采集协议（可空） |
| `collect_period_ms` | INT | 采集周期毫秒（可空） |
| `collect_timeout_ms` | INT | 采集超时毫秒（可空） |
| `collect_retries` | INT | 采集重试次数（可空） |
| `qos` | INT | MQTT QoS 等级 |
| `packet_id` | INT | MQTT 报文 ID |

### 7.2 公共 TAG（四类超表均有）

| TAG 名 | 类型 | 说明 |
|--------|------|------|
| `version` | NCHAR(16) | 规范版本，如 `v1` |
| `site_id` | NCHAR(64) | 站点 ID |
| `line_id` | NCHAR(64) | 产线 ID |
| `station_id` | NCHAR(64) | 工位 ID |
| `gateway_id` | NCHAR(64) | 网关 ID |
| `device_id` | NCHAR(64) | 设备 ID |
| `device_type` | NCHAR(64) | 设备类型 |
| `device_model` | NCHAR(64) | 设备型号（可空） |
| `metric_group` | NCHAR(32) | 指标组，如 `env`/`flow`/`power` |
| `signal_type` | NCHAR(32) | 信号类型，如 `current`/`voltage` |
| `channel_id` | NCHAR(32) | 通道 ID（可空） |

### 7.3 各类型专有列

**温湿度（weld_env_point）**

| 列名 | 类型 | 说明 |
|------|------|------|
| `temperature` | DOUBLE | 温度值（单位：℃） |
| `humidity` | DOUBLE | 湿度值（单位：%RH） |

**气体流量（weld_flow_point）**

| 列名 | 类型 | 说明 |
|------|------|------|
| `instant_flow` | DOUBLE | 瞬时流量（单位：L/min） |
| `total_flow` | DOUBLE | 累计流量，可为 NULL（单位：L） |

**电流原始消息（weld_current_raw）**

| 列名 | 类型 | 说明 |
|------|------|------|
| `signal_type` | VARCHAR(32) | 固定值 `current` |
| `window_start_us` | BIGINT | 采样窗口起始时间（微秒） |
| `sample_rate_hz` | INT | 采样率（Hz） |
| `point_count` | INT | 消息中点数 |
| `encoding` | VARCHAR(32) | `base64` / `zstd_base64_f32_le` 等 |
| `payload` | VARCHAR(49152) | 原始 `data.payload`，由客户端自管理时间轴 |
| `raw_adc_unit` | VARCHAR(16) | 若有则记录 ADC 单位 |
| `cal_version` | VARCHAR(64) | 若有则记录校准版本 |
| `cal_k` | DOUBLE | 校准系数 k（可空） |
| `cal_b` | DOUBLE | 校准系数 b（可空） |

**电压原始消息（weld_voltage_raw）**

| 列名 | 类型 | 说明 |
|------|------|------|
| `signal_type` | VARCHAR(32) | 固定值 `voltage` |
| `window_start_us` | BIGINT | 采样窗口起始时间（微秒） |
| `sample_rate_hz` | INT | 采样率（Hz） |
| `point_count` | INT | 消息中点数 |
| `encoding` | VARCHAR(32) | `base64` / `zstd_base64_f32_le` 等 |
| `payload` | VARCHAR(49152) | 原始 `data.payload`，由客户端自管理时间轴 |
| `raw_adc_unit` | VARCHAR(16) | 若有则记录 ADC 单位 |
| `cal_version` | VARCHAR(64) | 若有则记录校准版本 |
| `cal_k` | DOUBLE | 校准系数 k（可空） |
| `cal_b` | DOUBLE | 校准系数 b（可空） |

---

## 8. 参考文档

- 数据规范：`docs/zh_CN/焊接数据采集与边缘监测数据规范.md`
- 解析设计：`docs/zh_CN/rule/taos-current-points-design.md`
- 健壮性问题与优化草案：`docs/zh_CN/rule/weld-nanomq-reliability-problem-statement.md`
