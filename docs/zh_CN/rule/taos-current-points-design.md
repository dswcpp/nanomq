# 四类采集主题解析入库设计

## 1. 目标

本文档将采集卡上送的 4 类 MQTT 主题统一纳入设计范围：

- 电流
- 电压
- 温湿度
- 气体流量

本版文档冻结以下内容：

- 4 类主题的路由与匹配规则
- 电流、电压两类 51 点波形数据的落库表结构
- 温湿度、气体流量两类主题已纳入专用解析范围，但字段级表结构需在拿到 payload 样例后再冻结

这样处理的原因很简单：

- 电流样例已提供，可以把表结构定死
- 电压大概率与电流同构，可以同步冻结
- 温湿度、气体流量目前没有 payload 样例，不能凭空编字段，否则文档会误导实现

---

## 2. 主题分类

4 类主题按数据模型分成两组：

| 分类 | 主题 | 数据形态 | 当前文档状态 |
|------|------|----------|--------------|
| 波形类 | 电流 | `point_count = 51`，`points[]` 展开 | 已冻结 |
| 波形类 | 电压 | 预计与电流同构，`point_count = 51`，`points[]` 展开 | 已冻结 |
| 状态类 | 温湿度 | 单条 JSON 状态值 | 已纳入范围，待样例冻结字段 |
| 状态类 | 气体流量 | 单条 JSON 状态值 | 已纳入范围，待样例冻结字段 |

---

## 3. Topic 匹配规则

### 3.1 设计结论

4 类主题统一采用“白名单精确匹配”。

不采用以下方式：

- `#`
- `+`
- 前缀匹配
- 后缀匹配
- 正则匹配

原因如下：

- 避免把其他 JSON 主题误送入专用解析器
- 4 条解析链路的行为差异很大，必须显式隔离
- 后续灰度上线时可以逐主题放开

### 3.2 配置模型

建议将 4 类主题拆成 4 组白名单：

```text
current_topics = [
  "sensor/current/current_sensor_001/data"
]

voltage_topics = [
  "sensor/voltage/voltage_sensor_001/data"
]

temp_humidity_topics = [
  "sensor/temp_humidity/th_sensor_001/data"
]

gas_flow_topics = [
  "sensor/gas_flow/gas_flow_sensor_001/data"
]
```

### 3.3 匹配优先级

匹配优先级冻结如下：

1. `current_topics`
2. `voltage_topics`
3. `temp_humidity_topics`
4. `gas_flow_topics`
5. 未命中以上任一白名单时，继续走现有通用 TAOS Sink

要求如下：

- 一个 topic 只能属于一类白名单
- 配置加载时必须校验 4 组白名单之间不能重复
- 一旦重复，Broker 启动时直接报错，不允许带病运行

### 3.4 4 主题扩展示例

当前 4 个固定主题冻结如下：

```text
current_topics = [
  "sensor/current/current_sensor_001/data"
]

voltage_topics = [
  "sensor/voltage/voltage_sensor_001/data"
]

temp_humidity_topics = [
  "sensor/temp_humidity/th_sensor_001/data"
]

gas_flow_topics = [
  "sensor/gas_flow/gas_flow_sensor_001/data"
]
```

以上 4 个 topic 为当前冻结值。  
如果现场 topic 后续变更，必须先更新设计文档，再进入实现。

---

## 4. 波形类主题设计

### 4.1 适用范围

波形类主题包括：

- 电流
- 电压

这两类消息按一条消息拆多点入库。

### 4.2 输入消息模型

当前已知电流样例如下：

```json
{
  "device_id": "welding_daq_001",
  "sensor_type": "current",
  "seq": 725,
  "point_count": 51,
  "points": [
    { "ts_us": 1773988330000004, "value": -0.000153 },
    { "ts_us": 1773988330009475, "value": 0.000458 }
  ]
}
```

本版设计对电压作如下约束性假设：

- payload 结构与电流一致
- `sensor_type = voltage`
- `point_count = 51`
- `points[i]` 仍然包含 `ts_us` 和 `value`

如果后续电压 payload 与该假设不一致，则必须先修改设计文档，再进入实现。

### 4.3 波形类公共校验规则

命中 `current_topics` 或 `voltage_topics` 后，必须校验：

- `device_id` 必填
- `sensor_type` 必填
- `seq` 必填
- `point_count` 必填
- `points` 必填
- `points` 必须是数组
- `points` 数组长度必须等于 `point_count`
- `point_count` 必须等于 `51`
- 每个点都必须包含 `ts_us`
- 每个点都必须包含 `value`

分类校验如下：

- 电流主题要求 `sensor_type = current`
- 电压主题要求 `sensor_type = voltage`

### 4.4 建库要求

波形类数据必须保留微秒级时间，因此数据库精度冻结为 `us`：

```sql
CREATE DATABASE IF NOT EXISTS mqtt_rule PRECISION 'us';
```

如果目标库精度不是 `us`，则禁止启用波形类专用解析。

### 4.5 电流超表

```sql
CREATE STABLE IF NOT EXISTS mqtt_rule.welding_current_point (
    ts          TIMESTAMP,
    seq         BIGINT,
    point_index INT,
    point_count INT,
    value       DOUBLE,
    topic_name  NCHAR(256),
    recv_ts     TIMESTAMP,
    qos         INT,
    packet_id   INT
) TAGS (
    device_id   NCHAR(64),
    sensor_type NCHAR(32)
);
```

子表命名规则：

```text
welding_current_point_<sanitized_device_id>
```

### 4.6 电压超表

```sql
CREATE STABLE IF NOT EXISTS mqtt_rule.welding_voltage_point (
    ts          TIMESTAMP,
    seq         BIGINT,
    point_index INT,
    point_count INT,
    value       DOUBLE,
    topic_name  NCHAR(256),
    recv_ts     TIMESTAMP,
    qos         INT,
    packet_id   INT
) TAGS (
    device_id   NCHAR(64),
    sensor_type NCHAR(32)
);
```

子表命名规则：

```text
welding_voltage_point_<sanitized_device_id>
```

### 4.7 波形类字段说明

| 字段 | 类型 | 含义 | 来源 |
|------|------|------|------|
| `ts` | `TIMESTAMP` | 单个采样点时间 | `points[i].ts_us` |
| `seq` | `BIGINT` | 采样包序号 | `seq` |
| `point_index` | `INT` | 点位序号，范围 `0..50` | 数组下标 |
| `point_count` | `INT` | 报文中的点数 | `point_count` |
| `value` | `DOUBLE` | 采样值 | `points[i].value` |
| `topic_name` | `NCHAR(256)` | 来源 MQTT 主题 | MQTT topic |
| `recv_ts` | `TIMESTAMP` | Broker 接收时间 | Broker 当前时间 |
| `qos` | `INT` | MQTT QoS | MQTT metadata |
| `packet_id` | `INT` | MQTT packet id | MQTT metadata |
| `device_id` | `NCHAR(64)` | 设备号 | `device_id` |
| `sensor_type` | `NCHAR(32)` | 传感器类型 | `sensor_type` |

### 4.8 单条消息落库规则

波形类一条消息必须拆成 `51` 行。

映射规则如下：

- 第 `i` 个点写一行
- `point_index = i`
- `ts = points[i].ts_us`
- `value = points[i].value`
- `seq`、`point_count`、`topic_name`、`recv_ts`、`qos`、`packet_id` 在 51 行中重复
- `device_id`、`sensor_type` 作为 tag 写入子表

---

## 5. 状态类主题设计

### 5.1 适用范围

状态类主题包括：

- 温湿度
- 气体流量

### 5.2 当前冻结范围

这两类主题本版只冻结以下内容：

- 已纳入专用解析范围
- 已定义独立 topic 白名单
- 已确定不能再走当前 `payload_text` 原样 hex 存储方案
- 已确定必须走 topic 专用解析分支

### 5.3 当前未冻结范围

由于还没有拿到温湿度、气体流量 payload 样例，本版不冻结以下内容：

- 字段名
- 时间字段来源
- 是否单值上送还是多值上送
- 是否包含 `device_id`
- 是否包含 `sensor_type`
- 是否需要单条消息拆多行
- 最终超表 DDL

### 5.4 预留表名

为避免后续命名漂移，先冻结表名，不冻结字段：

- 温湿度超表预留名：`welding_temp_humidity_status`
- 气体流量超表预留名：`welding_gas_flow_status`

注意：

- 这里只冻结名字
- 不代表字段设计已完成
- 拿到样例后必须补一版字段设计

---

## 6. 异常处理规则

### 6.1 白名单未命中

未命中 4 类白名单的消息：

- 不进入专用解析链路
- 继续走现有通用 TAOS Sink

### 6.2 波形类解析失败

以下情况视为波形类解析失败：

- payload 不是合法 JSON
- `sensor_type` 不匹配 topic 分类
- 缺少 `device_id`
- 缺少 `seq`
- 缺少 `point_count`
- `point_count != 51`
- 缺少 `points`
- `points` 不是数组
- `points` 数组长度不等于 `51`
- 任一点缺少 `ts_us`
- 任一点缺少 `value`

处理原则：

- 记录错误日志
- 本条消息整体丢弃
- 不写入波形类点表
- 不降级写入 `payload_text`

### 6.3 状态类解析失败

温湿度、气体流量在字段冻结前，不允许进入正式实现阶段。  
如果提前开发，只能视为实验性逻辑，不能进入发布版本。

---

## 7. 与当前通用 TAOS Sink 的关系

本设计不替换通用 TAOS Sink，而是在发布路径中增加 4 条专用解析分支。

行为边界如下：

- 电流 topic：走电流 51 点解析
- 电压 topic：走电压 51 点解析
- 温湿度 topic：走温湿度专用解析
- 气体流量 topic：走气体流量专用解析
- 其他 topic：继续走当前通用 TAOS Sink

---

## 8. 实施顺序建议

建议按以下顺序推进：

1. 先实现 4 组 topic 白名单和冲突校验
2. 实现电流 51 点解析与入库
3. 实现电压 51 点解析与入库
4. 补齐温湿度 payload 样例并冻结表结构
5. 实现温湿度解析与入库
6. 补齐气体流量 payload 样例并冻结表结构
7. 实现气体流量解析与入库

---

## 9. 冻结项

本轮先冻结以下内容：

- 系统范围是 4 类主题，不再只讨论电流
- topic 采用 4 组白名单精确匹配
- 电流和电压都按 51 点波形类处理
- 电流超表名固定为 `welding_current_point`
- 电压超表名固定为 `welding_voltage_point`
- 温湿度预留超表名固定为 `welding_temp_humidity_status`
- 气体流量预留超表名固定为 `welding_gas_flow_status`
- 温湿度、气体流量在拿到样例前，不冻结字段级表结构
