# AGENTS.md

## 1. 目标

本文件面向在本仓库内工作的代码代理，目标是让代理在最少试错下完成以下事情：

- 正确理解 NanoMQ 主工程与 `nng` 子仓库的关系
- 使用正确的配置和构建产物启动服务
- 区分 MQTT 连接层问题与 TDengine 入库问题
- 在修改代码后用最小但有效的方式验证结果

本仓库当前不仅是上游 NanoMQ 源码，也承载了本地焊接遥测入库场景相关配置、文档和运维脚本。

## 2. 仓库结构

- `README.md`
  - 项目概览、基础构建方法、上游功能说明
- `CMakeLists.txt`
  - 顶层构建入口，控制 `ENABLE_TAOS`、`ENABLE_RULE_ENGINE` 等开关
- `nanomq/`
  - NanoMQ broker 应用层代码
  - 例如 `nanomq/apps/broker.c`、`nanomq/pub_handler.c`
- `nng/`
  - 关键子仓库
  - 这里包含 MQTT 协议层、transport、pipe、message 等核心实现
  - 修改 `nng/src/**` 时要意识到这是嵌套 git 仓库
- `etc/`
  - 运行配置
  - `etc/nanomq.conf` 是默认 broker 配置
  - `etc/nanomq_weld_taos.conf` 是焊接遥测入 TDengine 的配置
  - `etc/weld_tdengine_schema.sql`、`etc/weld_tdengine_schema_reset.sql` 是建库建表脚本
- `scripts/`
  - 本地运维辅助脚本
  - `scripts/start-nanomq-taos.sh`
  - `scripts/stop-nanomq.sh`
  - `scripts/check-taos-ingest.sh`
- `docs/zh_CN/rule/`
  - 当前项目最相关的规则引擎和 TAOS 文档
- `public/0.24.14/`
  - 当前交付物和上线说明

## 2.1 .gitignore 说明

本仓库的 `.gitignore` 严格控制什么可以提交：

- **应忽略（必须）**
  - 所有编译产物（`.o`、`.a`、`.so`、`.exe` 等）
  - 所有 CMake 构建目录（`build/`、`build-deb-*`、`build-tests/` 等）
  - IDE 配置（`.idea/`、`.vscode/`、`.claude/`）
  - 操作系统文件（`.DS_Store`、`Thumbs.db`）

- **错误提交示例**
  - 本地 `build/` 中的编译产物
  - IDE 自动生成的 `.vscode/settings.json`
  - `.deb` 包文件

详见 `.gitignore-guide.md`，包含设计原则、规则详解、常见陷阱。

**对代理的影响**：
- 修改代码后生成的 `build-*/` 目录自动被忽略，不会误提交
- 如果修改涉及构建配置，需要验证 `CMakeLists.txt` 正确（不是 `.gitignore` 问题）


- 当前常用可执行文件是 `build-deb-notests/nanomq/nanomq`
- 当前排障重点是焊接遥测通过 `rules.taos` 写入 TDengine
- TDengine REST 默认端口是 `6041`
- `etc/nanomq.conf` 不启用 `rules.taos`
- `etc/nanomq_weld_taos.conf` 才会把匹配的 `weld/.../telemetry/...` 主题写入 `mqtt_rule`
- 如果误用 `etc/nanomq.conf` 启动，broker 看起来是正常的，但不会写入 TAOS
- `nng/` 是单独 git 仓库，查看变更时要同时看顶层仓库和 `nng` 子仓库状态

## 4. 启动与停止

优先使用脚本，不要重复手敲长命令。

### 启动 TAOS 入库实例

```bash
scripts/start-nanomq-taos.sh
```

默认行为：

- 使用 `etc/nanomq_weld_taos.conf`
- 通过 `tmux` 会话 `nanomq-taos` 后台运行
- 自动检查端口占用并在必要时先停旧实例

前台调试：

```bash
scripts/start-nanomq-taos.sh --foreground
```

### 停止实例

```bash
scripts/stop-nanomq.sh
```

该脚本会：

- 清理 `tmux` 会话 `nanomq` / `nanomq-taos`
- 尝试调用 `nanomq stop`
- 回退到按真实进程发送 `SIGTERM`

不要只依赖 `nanomq stop`，因为当前构建里 `.pid` 跟真实进程可能不同步。

## 5. 入库检查

使用：

```bash
scripts/check-taos-ingest.sh
```

该脚本会输出：

- 当前 `nanomq` 进程
- TDengine `6041` 监听状态
- `mqtt_rule` 下的 stables
- `weld_env_point`
- `weld_flow_point`
- `weld_current_point`
- `weld_voltage_point`

每个 stable 会打印：

- `count`
- 最新 `ts`
- 最新 `topic_name`

如果日志出现以下关键字，说明不是 MQTT 接收层问题，而是 TAOS 异步解析/入队压力问题：

- `weld_telemetry_async: queue pressure`
- `weld_telemetry_async: queue full, drop message`
- `weld_telemetry_async_enqueue failed: queue full`

当前可通过环境变量调整焊接解析异步队列：

- `NANOMQ_WELD_PARSE_WORKER_MAX`
- `NANOMQ_WELD_PARSE_QUEUE_MAX_MSG`
- `NANOMQ_WELD_PARSE_QUEUE_MAX_BYTES`

如果数据库中有历史数据但今天没有新增，优先排查：

1. 是否起错配置
2. MQTT 客户端是否实际连上 broker
3. 发布主题是否匹配 `etc/nanomq_weld_taos.conf` 中的 `rules.taos`
4. TDengine `mqtt_rule` 是否可写

## 6. 当前最常见问题

### 6.1 端口占用

症状：

- `broker nng_listen: Address in use`

原因：

- 已有 `nanomq` 实例占用了 `1883` 或 `8083`

处理：

```bash
scripts/stop-nanomq.sh
ss -ltnp '( sport = :1883 or sport = :8083 or sport = :8081 )'
```

### 6.2 Client ID 冲突

症状：

- 同一个 client id 反复 `offline`
- 旧日志里常见 `Client ID collision or set ID failed!`

当前已补充更明确日志，修改点在：

- `nng/src/core/pipe.c`
- `nng/src/sp/protocol/mqtt/mqtt_parser.c`
- `nng/src/sp/transport/mqtt/broker_tcp.c`

优先结论通常是：

- 客户端重复连接
- 多实例共用同一个 client id
- 重连过快导致旧连接尚未完全释放

### 6.3 看起来 broker 正常，但数据库没新数据

优先先确认是不是用错了配置：

```bash
pgrep -a nanomq
```

如果命令行里不是：

```bash
--conf /home/tery/project/nanomq/etc/nanomq_weld_taos.conf
```

那就不是 TAOS 入库实例。

## 7. 修改代码时的约束

### 顶层仓库与子仓库

- 顶层仓库里新增脚本、文档、配置时，看顶层 `git status`
- 修改 `nng/src/**` 后，要额外看：

```bash
git -C nng status --short
```

### 相关代码区域

- Broker 生命周期和信号处理：
  - `nanomq/apps/broker.c`
- Rule engine 和入库路径：
  - `nanomq/pub_handler.c`
- MQTT 连接参数、上下线通知：
  - `nng/src/sp/protocol/mqtt/mqtt_parser.c`
- Pipe 管理、client id 冲突：
  - `nng/src/core/pipe.c`
- TCP transport 协商与收发错误：
  - `nng/src/sp/transport/mqtt/broker_tcp.c`

### 配置相关代码

- 规则解析、配置结构：
  - `nng/src/supplemental/nanolib/conf.c`
  - `nng/include/nng/supplemental/nanolib/conf.h`
  - `nng/include/nng/supplemental/nanolib/rule.h`

## 8. 代码开发地图

### 8.1 主干调用链

理解本项目最有用的是先抓住这条主链：

1. transport 接收连接和 MQTT 报文
2. protocol 层解析 CONNECT / PUBLISH / SUBSCRIBE
3. broker 层把消息交给业务处理
4. rule engine 根据 SQL 规则匹配消息
5. sink 或 repub 模块把结果落地或转发

对应文件大致是：

- transport
  - `nng/src/sp/transport/mqtt/broker_tcp.c`
- protocol
  - `nng/src/sp/protocol/mqtt/mqtt_parser.c`
  - `nng/src/sp/protocol/mqtt/nmq_mqtt.c`
- broker app
  - `nanomq/apps/broker.c`
- publish handler / rule engine glue
  - `nanomq/pub_handler.c`
- config parse
  - `nng/src/supplemental/nanolib/conf.c`
  - `nng/src/supplemental/nanolib/rule.c`

### 8.2 按问题类型找文件

如果问题是：

- 端口监听、CONNECT 协商、socket 收发异常
  - 先看 `nng/src/sp/transport/mqtt/broker_tcp.c`
- client id 冲突、pipe 生命周期、连接替换
  - 先看 `nng/src/core/pipe.c`
  - 再看 `nng/src/sp/protocol/mqtt/nmq_mqtt.c`
- 上下线通知、CONNECT 报文解析、reason code
  - 先看 `nng/src/sp/protocol/mqtt/mqtt_parser.c`
- broker 启停、HTTP server、信号处理
  - 先看 `nanomq/apps/broker.c`
- MQTT 消息进入 rule engine 后没有落库
  - 先看 `nanomq/pub_handler.c`
  - 再看 `nng/src/supplemental/nanolib/conf.c`
- 配置改了但启动输出不符合预期
  - 先看 `nng/src/supplemental/nanolib/conf.c`
  - 再看 `etc/*.conf`

### 8.3 本地焊接遥测入库链路

当前项目最常用链路是：

1. 客户端发布 `weld/.../telemetry/...`
2. `etc/nanomq_weld_taos.conf` 中的 `rules.taos` 匹配主题
3. Rule engine 提取消息字段
4. Sink 写入 `mqtt_rule`
5. TDengine 中的 stable 为：
   - `weld_env_point`
   - `weld_flow_point`
   - `weld_current_point`
   - `weld_voltage_point`

排查这条链时，优先不要跳着查。顺序应该是：

1. broker 是否用 TAOS 配置启动
2. MQTT 客户端是否真的连上并成功发布
3. topic 是否命中规则
4. sink 是否写出错误
5. TDengine 表中最新时间是否前进

## 9. 开发规则

### 9.1 改动策略

- 优先小步改动，不要在同一轮同时重写 transport、protocol、broker 三层
- 如果只是增强日志，尽量不要改变控制流
- 如果只是修入库配置或运维流程，优先改 `scripts/` 和 `etc/`，不要先碰核心 C 代码
- 改 `nng/src/**` 时，默认把它当成底层公共组件处理，避免写死焊接场景逻辑
- 焊接业务特有逻辑尽量放在配置、规则或 `nanomq/` 应用层，不要下沉到 `nng` 通用层

### 9.2 日志修改规则

- 日志必须优先带上下文：
  - `client_id`
  - 源 IP
  - reason code 或原始错误码
  - 是 broker 主动关闭还是对端关闭
- 仅凭 `rv: 139`、`offline!`、`Object closed` 这种日志不够，新增日志时不要重复制造低信息量输出
- 如果日志语义是正常连接替换，不要轻易打成 `ERROR`

### 9.3 配置修改规则

- 改 `etc/nanomq_weld_taos.conf` 时，必须同时核对：
  - 监听端口
  - `rules.taos`
  - TDengine host/port/user/password/database
  - topic pattern
- 改 schema 时，必须同步核对：
  - `etc/weld_tdengine_schema.sql`
  - `etc/weld_tdengine_schema_reset.sql`
  - `public/0.24.14/` 中的交付文档是否需要更新

### 9.4 子仓库规则

- 修改 `nng/` 后，不要只看顶层 `git status`
- 提交或汇报改动时，要明确区分：
  - 顶层仓库改了什么
  - `nng` 子仓库改了什么

## 10. 开发验证矩阵

### 10.1 只改脚本或文档

最少验证：

```bash
bash -n scripts/start-nanomq-taos.sh
bash -n scripts/stop-nanomq.sh
bash -n scripts/check-taos-ingest.sh
```

### 10.2 改配置

最少验证：

```bash
scripts/start-nanomq-taos.sh
scripts/check-taos-ingest.sh
scripts/stop-nanomq.sh
```

### 10.3 改 broker / transport / protocol 代码

最少验证：

```bash
cmake --build /home/tery/project/nanomq/build-deb-notests --target nanomq
scripts/start-nanomq-taos.sh
scripts/check-taos-ingest.sh
scripts/stop-nanomq.sh
```

如果改动涉及连接、断连、冲突日志，还应补：

```bash
tmux attach -t nanomq-taos
```

手动观察：

- CONNECT 是否成功
- 是否有重复 client id
- 是否出现协商期断开
- 是否出现新的入库错误

### 10.4 改 rule engine / TAOS 入库路径

除上述验证外，至少再确认：

- `mqtt_rule` 的最新时间是否前进
- stable 计数是否增长
- topic 是否落到预期的 stable

## 11. 构建建议

如果只是验证本地 `nanomq` 改动，优先使用已有构建目录：

```bash
cmake --build /home/tery/project/nanomq/build-deb-notests --target nanomq
```

如果新增脚本，至少做：

```bash
bash -n scripts/start-nanomq-taos.sh
bash -n scripts/stop-nanomq.sh
bash -n scripts/check-taos-ingest.sh
```

如果修改了 broker 或 transport 代码，推荐的最小验证顺序：

1. 重新编译 `nanomq`
2. `scripts/start-nanomq-taos.sh`
3. `scripts/check-taos-ingest.sh`
4. 手动观察日志或 `tmux attach -t nanomq-taos`
5. `scripts/stop-nanomq.sh`

## 12. 代码评审关注点

如果在本项目里做 review，优先检查：

- 是否把焊接场景的特化逻辑错误地下沉进 `nng` 通用层
- 是否只改了日志文本，却悄悄改变了关闭连接或重试的控制流
- 是否把“正常替换旧连接”误报成高优先级错误
- 是否引入新的端口冲突或配置歧义
- 是否修改了 `rules.taos` 却没有同步更新校验脚本或文档
- 是否遗漏了 `nng` 子仓库状态说明

## 13. 日志排障建议

如果用户贴了日志，先把问题归类：

- `Address in use`
  - 端口占用
- `Duplicate client connection detected`
  - 重复 client id 抢占连接
- `Client offline: client_id=... reason_code=...`
  - 客户端下线，需要结合上游事件看原因
- `CONNECT negotiation aborted by peer`
  - 协商阶段对端主动断开
- `mapped_reason=0x8b (server_shutting_down)`
  - 已连接期 socket 被关闭，通常是对端关闭或 broker 主动回收旧连接

不要仅凭 MQTT 连接层日志判断 TAOS 写库失败。先用 `scripts/check-taos-ingest.sh` 看库里的最新时间，再决定是否深入查 sink。

## 14. 文档优先级

当需要了解当前项目实际运维方式，优先看：

1. 本文件 `AGENTS.md`
2. `scripts/*.sh`
3. `etc/nanomq_weld_taos.conf`
4. `docs/zh_CN/rule/tdengine-sink.md`
5. `public/0.24.14/焊接遥测入库上线说明.md`

当需要了解上游通用能力，再看：

1. `README.md`
2. `docs/en_US/**`
3. `docs/zh_CN/**`

## 15. 提交前检查

最少做以下确认：

```bash
git status --short
git -C nng status --short
bash -n scripts/start-nanomq-taos.sh
bash -n scripts/stop-nanomq.sh
bash -n scripts/check-taos-ingest.sh
cmake --build /home/tery/project/nanomq/build-deb-notests --target nanomq
```

如果改动涉及入库路径，再补：

```bash
scripts/start-nanomq-taos.sh
scripts/check-taos-ingest.sh
scripts/stop-nanomq.sh
```
