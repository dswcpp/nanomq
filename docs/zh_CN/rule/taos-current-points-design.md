# 焊接四类遥测解析入库设计

## 1. 文档目的

本文档用于冻结《焊接数据采集与边缘监测数据规范》中四类常规遥测数据的接入、解析和入库方案。

本次设计覆盖以下四类数据：

- 温湿度
- 气体流量
- 电流
- 电压

本文档默认目标数据库为 TDengine，因为当前仓库已经具备可复用的 TAOS REST Sink 能力；如果后续目标数据库改为 SQLite、MySQL、PostgreSQL 或 TimescaleDB，需要在保留解析模型不变的前提下，单独补充对应 Sink 设计。

## 2. 需求确认

### 2.1 本次冻结范围

本次冻结以下内容：

- 四类消息均来自 `weld/v1/.../telemetry/...` 主题体系
- 四类消息均属于常规 `telemetry`，不属于 `status`、`event`、`cmd`、`ack`、`wave`
- 四类消息统一采用 `data.fields + data.points` 结构
- Broker 侧需要对四类消息做结构化解析，而不是仅保存原始 `payload_text`
- 数据库存储单位为“点级存储”，即 `data.points` 中每个点写一行
- 数据库存储需保留 Topic 维度和消息体维度，支持后续追溯和查询

### 2.2 不在本次范围内

本次不处理以下内容：

- 第 13 章波形消息
- `status`、`event`、`cmd`、`ack`、`config` 消息
- 云平台业务统计、看板和告警规则
- 多数据库同时写入
- 第三方系统字段映射

### 2.3 现状差距

当前仓库中的通用 TAOS Sink 具备以下特点：

- 可以按规则匹配 Topic
- 可以将命中的 MQTT 报文写入 TDengine
- 当前落库模型以 `topic/qos/packet_id/username/payload_text/client_id` 为主
- 不会自动解析 `weld/v1/...` Topic 层级
- 不会自动把 `data.fields + data.points` 展开成业务列

因此，单靠现有通用 TAOS Sink 配置，无法完成这四类遥测数据的结构化入库，必须新增“焊接规范专用解析链路”。

## 3. 输入模型

### 3.1 Topic 模型

四类遥测统一使用以下 Topic 结构：

```text
weld/{version}/{site_id}/{line_id}/{station_id}/{gateway_id}/telemetry/{metric_group}/{device_id}
```

固定约束如下：

- 第 1 级必须为 `weld`
- 第 7 级必须为 `telemetry`
- `metric_group` 仅允许 `env`、`flow`、`power`

本次四类 Topic 冻结如下：

```text
weld/v1/sz01/line_01/station_03/gw3568_01/telemetry/env/th01
weld/v1/sz01/line_01/station_03/gw3568_01/telemetry/flow/mf01
weld/v1/sz01/line_01/station_03/gw3568_01/telemetry/power/chb01
weld/v1/sz01/line_01/station_03/gw3568_01/telemetry/power/chv01
```

### 3.2 Payload 公共模型

四类消息都必须满足以下公共约束：

- `msg_class = telemetry`
- `gateway_id` 必须与 Topic 中的 `{gateway_id}` 一致
- `device_id` 必须与 Topic 中的 `{device_id}` 一致
- 必须存在 `spec_ver`
- 必须存在 `msg_id`
- 必须存在 `ts_ms`
- 必须存在 `seq`
- 必须存在 `device_type`
- 必须存在 `signal_type`
- 必须存在 `data.point_count`
- 必须存在 `data.fields`
- 必须存在 `data.points`
- 必须存在 `quality.code`

### 3.3 fields + points 统一约束

四类消息统一遵循以下规则：

- `data.point_count` 必须等于 `data.points` 数组长度
- 每个点必须包含 `ts_us` 或 `ts_ms`，同一消息内不得混用
- 每个点的 `values` 长度必须等于 `data.fields` 长度
- `values` 的顺序必须与 `fields` 的顺序一致
- 解析时不得依赖固定数组下标，必须以 `fields[].name` 建立字段映射

## 4. 四类消息冻结规则

### 4.1 温湿度

Topic 约束：

- `metric_group = env`
- `signal_type = environment`
- `device_type = temp_humidity_transmitter`

字段约束：

- `fields` 必须包含 `temperature`
- `fields` 必须包含 `humidity`
- `temperature.unit` 推荐为 `C`
- `humidity.unit` 推荐为 `pct_rh`

单点落库列：

- `temperature`
- `humidity`

### 4.2 气体流量

Topic 约束：

- `metric_group = flow`
- `signal_type = gas_flow`
- `device_type = gas_flow_meter`

字段约束：

- `fields` 必须包含 `instant_flow`
- `fields` 可选包含 `total_flow`
- `instant_flow.unit` 推荐为 `L_min`
- `total_flow.unit` 推荐为 `L`

单点落库列：

- `instant_flow`
- `total_flow`

说明：

- 若报文仅提供 `instant_flow`，则 `total_flow` 列写 `NULL`

### 4.3 电流

Topic 约束：

- `metric_group = power`
- `signal_type = current`
- `device_type = current_transducer`

字段约束：

- `fields` 必须包含 `current`
- `current.unit` 推荐为 `A`

扩展字段：

- `channel_id` 可选
- `raw.adc_unit` 可选
- `calibration.version` 可选
- `calibration.k` 可选
- `calibration.b` 可选

单点落库列：

- `current`
- `raw_adc_unit`
- `cal_version`
- `cal_k`
- `cal_b`

### 4.4 电压

Topic 约束：

- `metric_group = power`
- `signal_type = voltage`
- `device_type = voltage_transducer`

字段约束：

- `fields` 必须包含 `voltage`
- `voltage.unit` 推荐为 `V`

扩展字段：

- `channel_id` 可选
- `raw.adc_unit` 可选
- `calibration.version` 可选
- `calibration.k` 可选
- `calibration.b` 可选

单点落库列：

- `voltage`
- `raw_adc_unit`
- `cal_version`
- `cal_k`
- `cal_b`

## 5. 入库策略

### 5.1 基本策略

本次采用“一条消息按点展开，一点一行”的策略。

映射规则如下：

- `data.points[i]` 对应数据库中的一行
- 行时间戳来自 `data.points[i].ts_us` 或 `data.points[i].ts_ms`
- 若输入为 `ts_ms`，入库前统一按 `ts_us = ts_ms * 1000` 转换为微秒精度
- 消息级字段在同一消息拆出的多行中重复写入
- `recv_ts` 取 Broker 进入 `weld_telemetry` 专用解析链路时的接收时间，使用系统实时时钟采集一次，并复用于该消息拆出的所有行

### 5.2 为什么不用“整条消息一行”

不采用“整条消息一行 + JSON 列”的原因如下：

- 规范的核心语义是 `fields + points`
- 后续查询通常按采样时间范围检索
- 电流、电压存在更高频率的点级分析需求
- 将 `points` 保留为原始 JSON 会让查询、聚合和补偿都变复杂

### 5.3 为什么不用“通用长表”

本次不采用 `field_name/field_value` 形式的通用长表，原因如下：

- 温湿度和流量天然是多列语义，宽表更符合业务查询
- 电流、电压字段单一，独立表更清晰
- TDengine 超表按业务类型拆分后，查询和维护更直接

### 5.4 `ts_ms` 转微秒策略

当点时间戳字段使用 `ts_ms` 时，统一按以下规则处理：

- 入库时间统一换算为 `ts_us = ts_ms * 1000`
- 该转换只做单位换算，不虚构亚毫秒时间
- 解析器必须只在单条消息内部检查换算后的点时间是否重复
- 若换算后出现重复时间戳，则整条消息判定为非法并丢弃
- 不允许通过“自动加偏移”方式规避冲突
- 不允许依赖 TDengine 覆盖写入解决冲突
- 不要求在写入前查询历史数据是否重复

这样处理的原因如下：

- 自动加偏移会伪造采样时间
- 覆盖写入会导致点数据静默丢失
- 电流、电压等高频数据应由上游明确提供 `ts_us`

## 6. TDengine 表设计

### 6.1 数据库要求

数据库精度冻结为微秒：

```sql
CREATE DATABASE IF NOT EXISTS mqtt_rule PRECISION 'us';
```

原因如下：

- 规范示例普遍使用 `ts_us`
- 电流、电压点位间隔可能小于毫秒
- 用微秒精度可以同时兼容 `ts_us` 和 `ts_ms`

### 6.2 公共维度设计

四类超表统一采用以下 tag 维度：

- `version`
- `site_id`
- `line_id`
- `station_id`
- `gateway_id`
- `device_id`
- `device_type`
- `device_model`
- `metric_group`
- `signal_type`
- `channel_id`

说明：

- `channel_id` 对温湿度、气体流量可为空字符串
- `version` 对应 Topic 第 2 级，例如 `v1`

### 6.3 温湿度超表

```sql
CREATE STABLE IF NOT EXISTS mqtt_rule.weld_env_point (
    ts                TIMESTAMP,
    recv_ts           TIMESTAMP,
    msg_id            NCHAR(128),
    seq               BIGINT,
    topic_name        NCHAR(256),
    spec_ver          NCHAR(32),
    temperature       DOUBLE,
    humidity          DOUBLE,
    quality_code      INT,
    quality_text      NCHAR(64),
    source_bus        NCHAR(32),
    source_port       NCHAR(64),
    source_protocol   NCHAR(32),
    collect_period_ms INT,
    collect_timeout_ms INT,
    collect_retries   INT,
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

### 6.4 气体流量超表

```sql
CREATE STABLE IF NOT EXISTS mqtt_rule.weld_flow_point (
    ts                TIMESTAMP,
    recv_ts           TIMESTAMP,
    msg_id            NCHAR(128),
    seq               BIGINT,
    topic_name        NCHAR(256),
    spec_ver          NCHAR(32),
    instant_flow      DOUBLE,
    total_flow        DOUBLE,
    quality_code      INT,
    quality_text      NCHAR(64),
    source_bus        NCHAR(32),
    source_port       NCHAR(64),
    source_protocol   NCHAR(32),
    collect_period_ms INT,
    collect_timeout_ms INT,
    collect_retries   INT,
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

### 6.5 电流超表

```sql
CREATE STABLE IF NOT EXISTS mqtt_rule.weld_current_point (
    ts                TIMESTAMP,
    recv_ts           TIMESTAMP,
    msg_id            NCHAR(128),
    seq               BIGINT,
    topic_name        NCHAR(256),
    spec_ver          NCHAR(32),
    current           DOUBLE,
    raw_adc_unit      NCHAR(16),
    cal_version       NCHAR(64),
    cal_k             DOUBLE,
    cal_b             DOUBLE,
    quality_code      INT,
    quality_text      NCHAR(64),
    source_bus        NCHAR(32),
    source_port       NCHAR(64),
    source_protocol   NCHAR(32),
    collect_period_ms INT,
    collect_timeout_ms INT,
    collect_retries   INT,
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

### 6.6 电压超表

```sql
CREATE STABLE IF NOT EXISTS mqtt_rule.weld_voltage_point (
    ts                TIMESTAMP,
    recv_ts           TIMESTAMP,
    msg_id            NCHAR(128),
    seq               BIGINT,
    topic_name        NCHAR(256),
    spec_ver          NCHAR(32),
    voltage           DOUBLE,
    raw_adc_unit      NCHAR(16),
    cal_version       NCHAR(64),
    cal_k             DOUBLE,
    cal_b             DOUBLE,
    quality_code      INT,
    quality_text      NCHAR(64),
    source_bus        NCHAR(32),
    source_port       NCHAR(64),
    source_protocol   NCHAR(32),
    collect_period_ms INT,
    collect_timeout_ms INT,
    collect_retries   INT,
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

### 6.7 子表命名规则

子表名按“超表名 + 网关 + 设备”生成：

```text
{stable}_{gateway_id}_{device_id}
```

示例：

```text
weld_env_point_gw3568_01_th01
weld_flow_point_gw3568_01_mf01
weld_current_point_gw3568_01_chb01
weld_voltage_point_gw3568_01_chv01
```

要求如下：

- 非字母数字字符统一替换为 `_`
- 子表名按 192 字节上限约束
- 若名称长度不超过 192 字节，则直接使用
- 若名称长度超过 192 字节，则保留前 175 字节，再追加 `_` 和原始完整名称哈希的前 16 个十六进制字符
- 截断后的最终名称必须保证稳定且可重复计算

示例规则：

```text
sanitized_name_len <= 192:
  use sanitized_name

sanitized_name_len > 192:
  use sanitized_name[0:175] + "_" + hash16(full_sanitized_name)
```

## 7. 解析流程设计

### 7.1 总流程

```text
MQTT PUBLISH
  -> 规则引擎命中 weld telemetry 规则
  -> 解析 Topic 九段结构
  -> 解析 JSON payload
  -> 校验公共头与 Topic 一致性
  -> 按 signal_type 进入四类解析分支
  -> 将 fields 映射到业务列
  -> 将 points 展开为多行
  -> 批量写入对应 TDengine 超表
```

### 7.2 Topic 解析步骤

解析时必须取出以下字段：

- `version`
- `site_id`
- `line_id`
- `station_id`
- `gateway_id`
- `metric_group`
- `device_id`

校验规则如下：

- Topic 段数必须等于 9
- 第 1 段必须为 `weld`
- 第 7 段必须为 `telemetry`
- `metric_group = env` 只能对应 `signal_type = environment`
- `metric_group = flow` 只能对应 `signal_type = gas_flow`
- `metric_group = power` 只能对应 `signal_type = current` 或 `voltage`

### 7.3 Payload 解析步骤

公共解析顺序冻结如下：

1. 校验 JSON 合法性
2. 校验 `msg_class`
3. 记录 `recv_ts`，取当前 Broker 实时时钟，单条消息只采集一次
4. 校验 `gateway_id` 和 `device_id`
5. 校验 `signal_type` 与命中的规则目标表是否一致
6. 读取 `data.fields`
7. 建立 `field_name -> values[index]` 映射
8. 遍历 `data.points`
9. 为每个点生成目标行对象
10. 批量写入对应超表

其中 `recv_ts` 定义如下：

- 含义：Broker 接收到该 MQTT 消息并进入专用解析链路的时间
- 作用域：一条 MQTT 消息只记录一个 `recv_ts`
- 使用方式：同一消息拆出的多行共用同一个 `recv_ts`
- 禁止使用 `ts_ms` 或 `data.points[*].ts_*` 替代 `recv_ts`

### 7.4 字段映射策略

必须使用字段名映射，不允许写死数组下标。

示例：

- 温湿度从 `temperature`、`humidity` 取值
- 气体流量从 `instant_flow`、`total_flow` 取值
- 电流从 `current` 取值
- 电压从 `voltage` 取值

这样做的原因如下：

- 规范明确要求以 `fields` 描述语义
- 流量存在只上送 `instant_flow` 的合法场景
- 后续同类消息扩展字段时更容易兼容

## 8. 配置设计

### 8.1 配置目标

配置层设计目标如下：

- 复用现有 TAOS 连接配置
- 增加“焊接遥测结构化解析”模式
- 让通用 TAOS Sink 和专用解析链路并存

### 8.1.1 配置扩展级别

`parser = weld_telemetry` 不是普通业务参数，而是 Rule Engine 配置模型的一级扩展。

实现时必须同步修改以下位置：

- `rule_taos` 或等价规则配置结构体，新增 parser 类型字段
- 规则对象生命周期管理，包括 init/check/free/copy 路径
- 旧式配置文件解析，即 `conf.c`
- 若项目启用了 HOCON 配置，则同步修改 `conf_ver2.c`
- 发布路径调度接口，即 `pub_handler.c` 到专用 parser 的调用参数

禁止采用以下临时方案：

- 不改配置结构体，只在 `pub_handler.c` 里硬编码 topic 分支
- 不改规则模型，只靠字符串匹配 `raw_sql`
- 不改配置解析器，依赖运行时推断 parser 类型

这样处理的原因如下：

- 可以保持 Rule Engine 的职责边界稳定
- 可以避免把焊接场景逻辑散落到 Broker 通用发布路径
- 可以减少后续扩展第二种 parser 时的返工

### 8.2 建议配置模型

建议在现有 TAOS 规则上新增 `parser = weld_telemetry`：

```properties
rule_option=ON
rule_option.taos=enable

rule.taos.1.host=127.0.0.1
rule.taos.1.port=6041
rule.taos.1.username=root
rule.taos.1.password=taosdata
rule.taos.1.db=mqtt_rule

rule.taos.event.publish.1.sql="SELECT * FROM \"weld/+/+/+/+/+/telemetry/env/#\""
rule.taos.event.publish.1.table="weld_env_point"
rule.taos.event.publish.1.parser="weld_telemetry"

rule.taos.event.publish.2.sql="SELECT * FROM \"weld/+/+/+/+/+/telemetry/flow/#\""
rule.taos.event.publish.2.table="weld_flow_point"
rule.taos.event.publish.2.parser="weld_telemetry"

rule.taos.event.publish.3.sql="SELECT * FROM \"weld/+/+/+/+/+/telemetry/power/#\" WHERE payload.signal_type = 'current'"
rule.taos.event.publish.3.table="weld_current_point"
rule.taos.event.publish.3.parser="weld_telemetry"

rule.taos.event.publish.4.sql="SELECT * FROM \"weld/+/+/+/+/+/telemetry/power/#\" WHERE payload.signal_type = 'voltage'"
rule.taos.event.publish.4.table="weld_voltage_point"
rule.taos.event.publish.4.parser="weld_telemetry"
```

说明：

- `table` 指向目标超表
- `parser = weld_telemetry` 表示不走通用 `payload_text` 模式
- `power` 主题必须在规则层通过 `payload.signal_type` 做第一次分流，避免同一消息双命中
- `weld_telemetry` parser 必须再按 `signal_type` 和 `table` 做第二次一致性校验
- 运行时一条消息只能写入一张目标表，不允许同时写入电流表和电压表

### 8.3 配置校验要求

配置加载时必须校验：

- `parser = weld_telemetry` 时，`db` 必须存在
- `table` 必须是四个受支持超表之一
- 同一数据库目标必须一致
- 不允许不同焊接规则指向不同的 TAOS 目标
- `weld_current_point` 对应的规则必须包含 `payload.signal_type = 'current'`
- `weld_voltage_point` 对应的规则必须包含 `payload.signal_type = 'voltage'`

### 8.4 管理面边界

本功能一期只支持文件配置加载，不支持通过 REST API 动态创建、修改和查询 `weld_telemetry` 规则。

边界冻结如下：

- 支持 `nanomq.conf` 或等价静态配置文件中的焊接规则
- 不要求在一期中补齐 REST Rule API 对 TAOS parser 扩展字段的序列化和反序列化
- 若管理面调用尝试创建此类规则，应明确返回“不支持”或保持不可见，而不是生成不完整规则对象

这样处理的原因如下：

- 当前 REST Rule API 对 TAOS 规则支持本就不完整
- 若只补执行链路而不补管理链路，容易出现“静态配置可用、动态配置损坏”的半成品状态
- 一期先把静态配置链路打通，更符合最小闭环原则

## 9. 代码改造建议

### 9.1 模块拆分

建议新增以下模块：

- `nanomq/weld_topic_parser.h`
- `nanomq/weld_topic_parser.c`
- `nanomq/weld_payload_parser.h`
- `nanomq/weld_payload_parser.c`
- `nanomq/weld_taos_sink.hpp`
- `nanomq/weld_taos_sink.cpp`

### 9.2 职责划分

各模块职责如下：

- `weld_topic_parser`：解析并校验 Topic 九段结构
- `weld_payload_parser`：解析 JSON、校验公共头、提取四类业务点
- `weld_taos_sink`：建库建表、子表管理、批量 SQL 拼接和写入
- `pub_handler.c`：在规则命中后将消息分流到专用解析链路
- `conf.c` 和相关结构体：解析 `parser = weld_telemetry`

同时必须同步检查以下兼容面：

- `rule_free` 和相关资源释放路径
- 规则复制或更新路径
- 启动时配置打印和调试日志路径
- 单元测试和现有 TAOS sink 测试

### 9.3 Parser 调用接口

`weld_telemetry` parser 不应设计成无上下文纯函数。

调用接口冻结如下：

- 输入一：原始 MQTT 元数据和 payload
- 输入二：由命中规则传入的目标超表名 `target_table`
- 输入三：当前 TAOS 连接配置和 parser 配置上下文
- 输出：结构化点记录批次和最终目标超表

实现要求如下：

- `pub_handler.c` 在命中某条 `parser = weld_telemetry` 规则后，必须把该规则的 `table` 作为 `target_table` 传入 parser
- parser 必须校验 `signal_type` 与 `target_table` 是否一致
- 若不一致，则立即报错并丢弃该消息
- parser 不负责从全局规则集中反查目标表
- parser 不自行重新选择另一张表，以避免绕开规则层配置

一致性映射冻结如下：

- `environment` -> `weld_env_point`
- `gas_flow` -> `weld_flow_point`
- `current` -> `weld_current_point`
- `voltage` -> `weld_voltage_point`

### 9.4 与现有通用 TAOS Sink 的关系

行为边界冻结如下：

- `parser != weld_telemetry` 时，继续走现有通用 TAOS Sink
- `parser = weld_telemetry` 时，走结构化解析入库
- 专用解析失败时，不降级写入 `payload_text`

这样处理的原因如下：

- 失败后降级为原始落库会掩盖数据质量问题
- 同一主题混用两种表结构会增加维护成本

## 10. 异常处理

### 10.1 直接丢弃并记错日志的情况

以下情况必须整条消息丢弃：

- Topic 段数错误
- Topic 固定段不合法
- `msg_class != telemetry`
- `gateway_id` 或 `device_id` 与 Topic 不一致
- `data.point_count` 与 `points` 长度不一致
- `values` 长度与 `fields` 长度不一致
- 点时间戳由 `ts_ms` 换算为微秒后发生重复
- `metric_group` 与 `signal_type` 不匹配
- 四类消息缺少其必需字段
- TDengine 写入失败且超过重试次数

### 10.2 可接受的兼容情况

以下情况允许写入：

- 气体流量缺少 `total_flow`
- 温湿度和流量缺少 `channel_id`
- 温湿度、流量、电流、电压缺少 `quality.text`
- 电流、电压缺少 `raw` 或 `calibration`

## 11. 测试要求

至少需要以下测试：

- Topic 解析单元测试
- 四类 payload 成功解析单元测试
- 四类 payload 缺字段失败单元测试
- `fields/values` 顺序错乱但字段名正确时的解析测试
- `point_count` 不一致失败测试
- `gateway_id/device_id` 与 Topic 不一致失败测试
- TDengine SQL 构造测试
- 端到端发布入库测试

## 12. 实施顺序

建议实施顺序如下：

1. 先补配置模型和结构体
2. 再补 Topic 解析器
3. 再补 Payload 解析器
4. 再补四个超表初始化逻辑
5. 再补批量写入逻辑
6. 最后补端到端测试和示例配置

## 13. 已冻结与当前不展开项

本版已冻结以下事项：

- 目标数据库固定为 TDengine
- 表名采用本文冻结的四个超表名

以下事项本版明确不展开，不阻塞开发：

- `quality.flags` 不单独入库
- `source.slave_addr`、`source.channel` 不展开成独立列
- 不额外保留原始 JSON 备查表
