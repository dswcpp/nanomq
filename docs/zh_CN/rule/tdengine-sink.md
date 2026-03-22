# NanoMQ TDengine REST Sink 功能开发文档

## 1. 功能概述

### 1.1 背景

NanoMQ 规则引擎（Rule Engine）支持将 MQTT 消息按用户定义的 SQL 规则提取字段，并转发至外部存储。本功能新增 **TDengine REST Sink**，使 NanoMQ 能够通过 TDengine 的 RESTful API（HTTP 端口 6041）将 MQTT 消息写入 TDengine 时序数据库，无需依赖 TDengine 客户端驱动（taosc），降低部署复杂度。

### 1.2 功能定位

| 维度       | 说明                                                     |
|------------|----------------------------------------------------------|
| 功能名称   | TDengine REST Sink                                       |
| 编译开关   | `-DENABLE_TAOS=ON`（默认 OFF）                           |
| 预处理宏   | `SUPP_TAOS`                                              |
| 外部依赖   | 无（使用 nng 内置 HTTP 客户端）                          |
| 通信协议   | HTTP POST → `http://<host>:<port>/rest/sql`              |
| 认证方式   | HTTP Basic Auth（`Authorization: Basic <base64>`）       |
| 数据模型   | TDengine 超表（STable）+ 自动子表（以 client_id 为 tag） |

### 1.3 与现有 Sink 对比

| 特性           | SQLite | MySQL | PostgreSQL | TimescaleDB | **TDengine** |
|----------------|--------|-------|------------|-------------|--------------|
| 编译开关       | RULE_ENGINE + SQLITE | ENABLE_MYSQL | ENABLE_POSTGRESQL | ENABLE_TIMESCALEDB | **ENABLE_TAOS** |
| 连接方式       | 嵌入式 | 客户端库 | libpq | libpq | **REST HTTP** |
| 外部依赖       | 内置 sqlite3 | libmysqlclient | libpq | libpq | **无** |
| 写入模式       | 同步 | 同步 | 同步 | 同步 | **异步批量** |
| 连接管理       | 文件句柄 | 连接池 | PGconn | PGconn | **nng HTTP client** |
| 自动建表       | ALTER TABLE | - | - | - | **CREATE STABLE** |
| Rule Engine 标志 | `RULE_ENG_SDB` | `RULE_ENG_MDB` | `RULE_ENG_PDB` | `RULE_ENG_TDB` | **`RULE_ENG_TAOS`** |

---

## 2. 架构设计

### 2.1 整体架构

```
                           nanomq.conf
                               │
                               ▼
                      ┌─────────────────┐
                      │  conf_rule_parse │ (nng/src/supplemental/nanolib/conf.c)
                      │  解析配置文件    │
                      └────────┬────────┘
                               │ rule_taos 结构体
                               ▼
┌──────────┐    MQTT    ┌──────────────┐    rule match    ┌───────────────────┐
│  Client  │ ─────────> │  nano_work   │ ──────────────>  │ rule_engine_      │
│          │  PUBLISH   │ (pub_handler)│                  │ insert_sql()      │
└──────────┘            └──────────────┘                  │ (pub_handler.c)   │
                                                          └────────┬──────────┘
                                                                   │
                                                    ┌──────────────┼──────────────┐
                                                    │ taos_sink_config             │
                                                    │ taos_rule_result             │
                                                    ▼                              │
                                             ┌─────────────┐              首次调用 │
                                             │ taos_sink_  │ <────────────────────┘
                                             │ enqueue()   │  (非阻塞入队)
                                             └──────┬──────┘
                                                    │ nng_mtx + nng_cv
                                                    ▼
                                             ┌─────────────┐
                                             │ 独立消费者   │  (nng_thread)
                                             │ 线程         │  批量 100 条/100ms
                                             └──────┬──────┘
                                                    │
                                              HTTP POST
                                              /rest/sql
                                         (nng_http_client_transact)
                                                    │
                                                    ▼
                                           ┌──────────────┐
                                           │  TDengine    │
                                           │  REST API    │
                                           │  :6041       │
                                           └──────────────┘
```

### 2.2 数据流转

```
1. MQTT Client 发布消息到匹配 topic
2. pub_handler.c :: handle_pub() 调用 rule_engine_insert_sql()
3. 遍历所有 rule，匹配 forword_type == RULE_FORWORD_TAOS
4. 首次匹配时调用 taos_sink_start() 启动后台线程 + 建库建表
5. 从 pub_packet_struct 和 conn_param 中提取字段
6. 构造 taos_rule_result
7. 调用 taos_sink_enqueue() 非阻塞入队（内部深拷贝）
8. 消费者线程按 100 条/批 或 100ms 超时批量 INSERT
9. 通过 nng HTTP client → TDengine REST API
```

### 2.3 模块依赖关系

```
CMakeLists.txt (根)
  └── option(ENABLE_TAOS) + add_definitions(-DSUPP_TAOS)

nanomq/CMakeLists.txt
  └── if(ENABLE_TAOS) → target_sources(taos_sink.cpp)

nng/include/nng/supplemental/nanolib/
  ├── rule.h          → rule_taos 结构体、RULE_FORWORD_TAOS 枚举
  └── conf.h          → RULE_ENG_TAOS 宏定义 (1 << 6)

nng/src/supplemental/nanolib/
  ├── rule.c          → rule_taos_init/free/check
  └── conf.c          → conf_rule_taos_parse 配置解析

nanomq/
  ├── taos_sink.hpp   → C 接口声明（extern "C"）
  ├── taos_sink.cpp   → C++ 实现（nng HTTP + 异步批量）
  └── pub_handler.c   → 规则引擎集成入口
```

---

## 3. 源文件详解

### 3.1 taos_sink.hpp — 公共接口声明

**路径**: `nanomq/taos_sink.hpp`

```c
#pragma once
#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
    const char *host;       // TDengine 地址
    int         port;       // REST 端口，默认 6041
    const char *username;   // 用户名，默认 "root"
    const char *password;   // 密码，默认 "taosdata"
    const char *db;         // 数据库名
    const char *stable;     // 超表名
} taos_sink_config;

typedef struct {
    int         qos;
    int         packet_id;
    const char *topic;
    const char *client_id;
    const char *username;
    const char *payload;
    size_t      payload_len;
    int64_t     timestamp_ms;
} taos_rule_result;

int  taos_sink_valid_identifier(const char *name);
int  taos_sink_start(const taos_sink_config *cfg);
int  taos_sink_enqueue(const taos_rule_result *result);
void taos_sink_stop(void);

#ifdef __cplusplus
}
#endif
```

**设计要点**：
- `taos_sink_start()` — 启动后台消费者线程，内部建库建表
- `taos_sink_enqueue()` — 非阻塞入队，内部深拷贝所有字段
- `taos_sink_stop()` — flush 剩余数据后停止线程，支持 double-call 保护
- 使用 `extern "C"` 确保 C++ 实现可被 C 代码调用

### 3.2 taos_sink.cpp — 核心实现

**路径**: `nanomq/taos_sink.cpp`

#### 3.2.1 内部数据结构

| 结构 | 用途 |
|------|------|
| `taos_queued_item` | 队列元素，深拷贝所有 MQTT 消息字段 |
| `taos_queue_t` | 简易动态数组（替代 cvector，避免 C++ void* 转换） |
| `dyn_buf` | 动态 buffer，用于 SQL 拼接 |

#### 3.2.2 函数列表

| 函数 | 可见性 | 说明 |
|------|--------|------|
| `base64_encode` | static | Base64 编码（用于 HTTP Basic Auth） |
| `build_auth_header` | static | 构造 `"Basic <base64(user:pass)>"` |
| `tdengine_exec` | static | 通过 nng HTTP client 发送 SQL |
| `taos_sink_valid_identifier` | **public** | 标识符白名单校验 |
| `sql_escape_str` | static | SQL 字符串转义 |
| `payload_to_hex` | static | 二进制 payload 转 HEX |
| `build_batch_insert_sql` | static | 构造多行批量 INSERT SQL |
| `taos_sink_flush` | static | 批量发送到 TDengine |
| `taos_sink_flush_thread` | static | 消费者线程主循环 |
| `taos_sink_init_db` | static | 建库 + 建超表 |
| `taos_sink_start` | **public** | 启动后台线程 |
| `taos_sink_enqueue` | **public** | 非阻塞入队 |
| `taos_sink_stop` | **public** | 停止后台线程 |

#### 3.2.3 tdengine_exec 流程

```
tdengine_exec(url_str, auth_hdr, sql)
  │
  ├── 1. nng_url_parse(url_str)
  ├── 2. nng_http_client_alloc()
  ├── 3. nng_http_req_alloc() → set POST, Authorization, Content-Type
  ├── 4. nng_http_req_copy_data(sql)
  ├── 5. nng_aio_alloc(NULL, NULL) → 同步模式
  ├── 6. nng_aio_set_timeout(5000ms)
  ├── 7. nng_http_client_transact() + nng_aio_wait()
  ├── 8. 检查 HTTP status == 200
  ├── 9. 检查响应体包含 "code":0
  └── 10. 清理所有 nng 资源，返回 0/-1
```

#### 3.2.4 批量写入流程

```
消费者线程循环:
  ├── nng_cv_until(cv, deadline) — 等待 100ms 或被唤醒
  ├── 取出 min(queue.len, 100) 条消息
  ├── build_batch_insert_sql() — 构造多行 INSERT:
  │     INSERT INTO db.t1 USING db.stable TAGS('c1') VALUES(...)
  │                  db.t2 USING db.stable TAGS('c2') VALUES(...)
  ├── tdengine_exec(batch_sql)
  └── 释放 batch 内存
```

### 3.3 pub_handler.c — 规则引擎集成

**路径**: `nanomq/pub_handler.c`

关键步骤：
1. 检查 `RULE_ENG_TAOS` 标志和 `RULE_FORWORD_TAOS` 类型
2. 首次匹配时调用 `taos_sink_start()` 启动后台线程
3. 从 MQTT 报文提取字段填充 `taos_rule_result`
4. 调用 `taos_sink_enqueue()` 非阻塞入队

---

## 4. 构建配置

### 4.1 CMakeLists.txt（根目录）

```cmake
option (ENABLE_TAOS "Enable TDengine REST sink" OFF)

if (ENABLE_TAOS)
  add_definitions(-DSUPP_TAOS)
endif (ENABLE_TAOS)
```

### 4.2 nanomq/CMakeLists.txt

```cmake
if(ENABLE_TAOS)
  target_sources(nanomq PRIVATE taos_sink.cpp)
  set_source_files_properties(taos_sink.cpp PROPERTIES LANGUAGE CXX)
endif(ENABLE_TAOS)
```

**说明**：
- 无外部依赖，使用 nng 内置 HTTP 客户端
- `taos_sink.cpp` 被显式标记为 CXX 语言

### 4.3 编译命令

```bash
mkdir build && cd build
cmake .. -DENABLE_RULE_ENGINE=ON \
         -DENABLE_TAOS=ON \
         -DNNG_ENABLE_SQLITE=ON
make nanomq
```

---

## 5. TDengine 数据模型

### 5.1 超表 Schema

```sql
CREATE STABLE IF NOT EXISTS <db>.<stable> (
    ts        TIMESTAMP,       -- MQTT 消息到达时间（毫秒精度）
    qos       INT,             -- MQTT QoS 等级 (0/1/2)
    packet_id INT,             -- MQTT Packet ID
    topic     NCHAR(256),      -- 发布主题
    username  NCHAR(256),      -- 客户端用户名
    payload   NCHAR(2048)      -- 消息内容（HEX 编码）
) TAGS (
    client_id NCHAR(128)       -- 客户端标识符
)
```

### 5.2 子表（自动创建）

子表名格式: `<stable>_<sanitized_client_id>`，无 client_id 时使用 `<stable>_default`。

### 5.3 Payload 编码

Payload 采用 HEX 编码存储，限制最大 1024 字节原始数据（超出部分截断并记录 warning 日志）。

---

## 6. 配置说明

### 6.1 KV 格式（nanomq_old.conf）

```ini
rule_option=ON
rule_option.taos=enable
rule.taos.1.host=127.0.0.1
rule.taos.1.port=6041
rule.taos.1.username=root
rule.taos.1.password=taosdata
rule.taos.1.db=mqtt_rule
rule.taos.1.table=mqtt_data
rule.taos.event.publish.1.sql=SELECT * FROM "sensor/#"
```

---

## 7. 安全设计

| 层级 | 措施 |
|------|------|
| 标识符 | `taos_sink_valid_identifier()` 白名单校验（仅 `[a-zA-Z0-9_]`） |
| 字符串值 | `sql_escape_bytes()` 转义 `'` 和 `\` |
| 二进制数据 | `payload_to_hex()` HEX 编码 |
| 子表名 | `isalnum()` 白名单 + 192 字节限制 |
| 认证 | HTTP Basic Auth |
| 超时 | 5 秒 |
| double-stop | `started` 标志位保护 |

---

## 8. 测试指南

### 8.1 环境准备

```bash
# 启动 TDengine
docker run -d --name tdengine -p 6041:6041 tdengine/tdengine:latest

# 编译 nanomq
mkdir build && cd build
cmake .. -DENABLE_RULE_ENGINE=ON -DENABLE_TAOS=ON -DNNG_ENABLE_SQLITE=ON
make nanomq
```

### 8.2 测试用例

| 测试场景 | 预期结果 |
|----------|----------|
| 正常消息写入 | TDengine 中能查到对应数据 |
| 空 payload | 写入成功，payload 字段为空 |
| 超长 payload（> 1024 字节） | 截断 + warning 日志 |
| TDengine 不可达 | `log_error` 报错，不崩溃 |
| 多 client 并发写入 | 各自创建子表，数据不混乱 |
| 高频写入（10000 msg/s） | 批量 INSERT，不阻塞 broker |

---

## 9. 变更记录

| 日期 | 版本 | 变更内容 |
|------|------|----------|
| 2026-03-22 | v1.0 | 初始实现：TDengine REST Sink |
| 2026-03-22 | v2.0 | 性能优化：独立线程 + 批量写入 + nng HTTP 替换 libcurl |
| 2026-03-22 | v2.1 | 评审修复：配置解析、stop 安全性、frag 动态分配、文档对齐 |
