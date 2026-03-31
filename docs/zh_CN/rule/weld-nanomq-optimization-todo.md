# 焊接遥测链路 NanoMQ 优化待办清单

本文档用于收敛当前焊接遥测接入链路中，NanoMQ 侧仍需优化完善的事项。

目标不是重做架构，而是在不破坏现有设计文档约束的前提下，补齐当前实现中的吞吐、可靠性、可观测性和运维可用性短板。

## 1. 当前实现概况

当前链路已经具备以下能力：

- 规则引擎匹配 `weld/.../telemetry/...` 主题
- `weld_telemetry_async` 异步解析原始 MQTT 消息
- `weld_telemetry` 将 `data.points` 展开为点级行
- `weld_taos_sink` 按批异步写入 TDengine
- TDengine 使用四张超表
- 子表按 `stable + gateway_id + device_id` 自动生成

当前实现中几个关键常量：

- 写库批大小 `WELD_BATCH_SIZE = 1000`
- flush 等待时间 `WELD_FLUSH_MS = 100`
- sink 队列上限 `WELD_QUEUE_MAX = 40000`
- parser 队列上限 `WELD_PARSE_QUEUE_MAX_MSG = 256`
- parser 队列字节上限 `WELD_PARSE_QUEUE_MAX_BYTES = 64MB`

## 2. 现状判断

结合当前源码，NanoMQ 侧已经不是“只能单条写库”，而是：

- 一条 MQTT 消息会展开为 `point_count` 行
- 多行一次性入 sink 队列
- sink worker 每次最多取 1000 行拼成一条批量 INSERT SQL
- worker 被 `notify_all` 唤醒后可以立即刷库，不需要等满 100ms

因此，当前瓶颈和异常主要不在“有没有批量”，而在以下几类问题：

- 时间戳语义正确但保护不完整
- 数据库前置条件未强校验
- 失败处理策略过粗
- 观测和运维提示不足

## 3. 必须马上做

### 3.1 对 `ts_us` 也做同消息内重复检测

优先级：P0

现状：

- 当前仅在 `ts_ms` 路径下做重复时间戳检测
- `ts_us` 路径不会检查同一消息内是否存在重复时间
- 同一设备固定写同一子表，时间列一旦重复，TDengine 层会发生覆盖语义风险

源码依据：

- [weld_telemetry.c](/home/tery/project/nanomq/nanomq/weld_telemetry.c:694)
- [weld_taos_sink.cpp](/home/tery/project/nanomq/nanomq/weld_taos_sink.cpp:370)

风险：

- 400 个点若共用同一个 `ts_us`，最终可能只保留 1 行
- 这是静默数据损失，运维侧难以及时发现

优化要求：

- `ts_us` 与 `ts_ms` 一样，统一执行“同一消息内重复时间戳检测”
- 发现重复时直接拒绝整条消息
- 错误日志中必须包含 `topic`、`signal_type`、`point_count`

验收标准：

- 同消息内 400 个点如果有重复 `ts_us`，该消息应被拒绝
- 不允许再出现“消息成功但点被静默覆盖”的情况

### 3.2 sink 首次启动前强校验 TDengine 数据库精度必须为 `us`

优先级：P0

现状：

- 当前只执行 `CREATE DATABASE IF NOT EXISTS ... PRECISION 'us'`
- 如果现场已存在同名旧库且精度不是 `us`，该语句不会修正旧库
- 后续批量写入时会报 `Timestamp data out of range`
- 当前该检查点发生在 sink lazy start 阶段，即首条匹配消息触发 sink 启动时，而不是 Broker 进程启动时

源码依据：

- [weld_taos_sink.cpp](/home/tery/project/nanomq/nanomq/weld_taos_sink.cpp:745)

风险：

- 现场错误不会在启动期暴露，而是在写入高峰期放大
- 日志上看像“消息时间戳坏了”，实际根因是数据库配置不匹配

优化要求：

- sink 首次启动或首批写入前必须校验目标数据库精度
- 若不是 `PRECISION 'us'`，直接 fail-fast
- 错误日志需明确提示：重建数据库或切换新库名

验收标准：

- 旧的 `ms` 精度库不得进入正常写入流程
- 现场一眼能从日志看出是库精度问题，而不是消息格式问题

### 3.3 区分可重试错误与不可重试错误

优先级：P0

现状：

- 当前所有批量写入失败统一重试 3 次
- 对于时间戳越界、字段非法、库精度不匹配这类确定性错误，重试没有意义

源码依据：

- [weld_taos_sink.cpp](/home/tery/project/nanomq/nanomq/weld_taos_sink.cpp:632)

风险：

- 无效重试放大日志
- 占用 worker 时间
- 降低整体吞吐

优化要求：

- 根据 TDengine 返回码区分错误类型
- 网络抖动、HTTP 超时、连接失败可重试
- 时间戳越界、库精度错误、语法错误、表结构错误应立即终止该批次

验收标准：

- 不可重试错误不得继续进行第 2、第 3 次重试
- 日志中要区分 `retryable` 和 `non-retryable`

### 3.4 保持运维日志与实际运行状态一致

优先级：P0

现状：

- 误导性日志问题已经完成修复
- 当前需要做的是把该类问题纳入后续改动的回归检查项，避免重新引入

源码依据：

- [conf_ver2.c](/home/tery/project/nanomq/nng/src/supplemental/nanolib/conf_ver2.c:392)
- [broker.c](/home/tery/project/nanomq/nanomq/apps/broker.c:1282)

风险：

- 若后续改动再次引入误导性日志，运维会误判 websocket 或 HTTP 服务状态

优化要求：

- 将 websocket 缺省配置日志、HTTP 服务启动日志纳入回归检查
- 发布前确认不会出现 `(null):8081` 或无意义 `WARN`

验收标准：

- 正常启动日志中不出现误导性 `WARN`
- 新版本发布时保留该项回归校验

## 4. 本周可做

### 4.1 让批量参数可配置

优先级：P1

现状：

- 批大小、flush 周期、队列深度上限是代码常量
- worker 实际数量按 `hardware_concurrency()` 动态选择，但上限仍是代码常量

源码依据：

- [weld_taos_sink.cpp](/home/tery/project/nanomq/nanomq/weld_taos_sink.cpp:25)
- [weld_telemetry_async.cpp](/home/tery/project/nanomq/nanomq/weld_telemetry_async.cpp:18)

风险：

- 不同现场的数据密度、CPU、网络、TDengine 性能差异很大
- 运维无法通过配置调优

优化要求：

- 支持从配置文件或环境变量覆盖以下参数：
- sink `batch_size`
- sink `flush_ms`
- sink `queue_max`
- sink `worker_count`
- async parser `queue_max_msg`
- async parser `queue_max_bytes`
- async parser `worker_count`

验收标准：

- 不改代码即可完成现场调参

### 4.2 批量失败时支持降级拆分定位

优先级：P1

现状：

- 1000 行批量 SQL 中只要有 1 行非法，整批失败

源码依据：

- [weld_taos_sink.cpp](/home/tery/project/nanomq/nanomq/weld_taos_sink.cpp:639)

风险：

- 单条坏消息放大成整批失败
- 数据损失范围过大

优化要求：

- 批量失败且判定为数据错误时，支持二分拆批定位
- 最终至少能识别是“哪几行坏”

验收标准：

- 非网络类失败时，批量坏数据能被压缩到最小影响范围

### 4.3 增加更完整的运行指标

优先级：P1

现状：

- 当前只有周期性 log stats

源码依据：

- [weld_taos_sink.cpp](/home/tery/project/nanomq/nanomq/weld_taos_sink.cpp:581)

建议增加：

- parser 队列深度
- sink 队列深度
- 每秒解析消息数
- 每秒展开点数
- 每秒写库成功行数
- 每秒写库失败行数
- 丢弃消息数
- 不可重试错误数
- 重复时间戳拒绝数

验收标准：

- 不依赖 grep 海量日志，也能判断当前瓶颈落在哪一层

### 4.4 给“高频点必须使用唯一 `ts_us`”增加显式日志提示

优先级：P1

现状：

- 设计文档明确高频点应使用 `ts_us`
- 但运行时没有足够直白的提示

优化要求：

- 对电流、电压这类高频消息，如果检测到 `ts_ms`，输出提示级别日志
- 明确告知：`ts_ms` 仅适合低频点，高频点应改用唯一 `ts_us`

验收标准：

- 运维在第一轮排障时就能看到时间戳模型不适合高频的提示

## 5. 后续架构优化

### 5.1 复用 TDengine HTTP client，减少每批次建连开销

优先级：P2

现状：

- 每次 HTTP 写入都重新创建和释放 client/request/response/aio

源码依据：

- [weld_taos_sink.cpp](/home/tery/project/nanomq/nanomq/weld_taos_sink.cpp:200)

风险：

- 高频批量写入时 CPU 和系统调用开销偏高
- 影响整体吞吐上限

优化要求：

- 在 worker 维度复用 HTTP client
- 减少重复初始化成本

验收标准：

- 同样压力下，CPU 占用和写入延迟下降

### 5.2 让 parser 内部分流成为唯一可信路由

优先级：P2

现状：

- 当前 `current` / `voltage` 分流在规则层依赖 SQL 过滤
- parser 已有 `signal_type` 与目标表的一致性校验，但尚未做到“完全自主决定最终路由”

风险：

- 不同 NanoMQ 规则 SQL 能力差异可能导致未来再次双写或误路由

优化要求：

- parser 内部基于 `signal_type` 执行最终路由确认
- 规则层只负责粗匹配

验收标准：

- 即使规则层过滤能力有限，也不会出现 current/voltage 双写

### 5.3 补齐系统化压测基线

优先级：P2

建议建立以下固定压测模型：

- `20ms * 400 点`
- `100ms * 2000 点`
- `8 路 power 并发`
- `env + flow + power 混合`
- 不同 QoS 组合

验收标准：

- 每次改动都能知道吞吐、丢弃率、延迟、写库成功率是否退化

## 6. 不建议做的事

以下做法不建议作为正式方案：

- 自动给重复时间戳加偏移
- 靠 TDengine 覆盖写入“容忍”重复点
- 仅仅继续加大队列，不处理时间戳语义问题
- 只靠客户端降速回避所有服务端短板

原因：

- 自动加偏移会伪造采样时间
- 覆盖写入会造成静默数据损失
- 盲目加队列会掩盖根因并放大内存占用
- 服务端健壮性问题仍然存在

## 7. 推荐落地顺序

第一阶段：

1. 补 `ts_us` 重复检测
2. 补数据库精度强校验
3. 修复不可重试错误仍重试的问题
4. 将误导性日志纳入发布回归检查

第二阶段：

1. 批量参数配置化
2. 增加运行指标
3. 批量失败降级拆分

第三阶段：

1. HTTP client 复用
2. parser 内最终路由收口
3. 压测基线与回归体系

## 8. 结论

当前 NanoMQ 侧最需要优化的，不是“再多开几个线程”，而是：

- 先堵住静默数据损失
- 先把数据库前置条件校验清楚
- 先把失败类型分清楚
- 先让运维看得明白

只有这四件事补齐后，后续的吞吐优化才有意义。
