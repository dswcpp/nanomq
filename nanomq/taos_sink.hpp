#pragma once

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

// TDengine REST sink 配置
typedef struct {
    const char *host;       // TDengine 地址，例如 "192.168.1.100"
    int         port;       // REST 端口，默认 6041
    const char *username;   // 用户名，默认 "root"
    const char *password;   // 密码，默认 "taosdata"
    const char *db;         // 数据库名，例如 "mqtt_rule"
    const char *stable;     // 超表名，例如 "mqtt_data"
} taos_sink_config;

// rule 执行结果（从 MQTT 消息中提取的字段）
typedef struct {
    int         qos;
    int         packet_id;
    const char *topic;
    const char *client_id;
    const char *username;
    const char *payload;
    size_t      payload_len;
    int64_t     timestamp_ms; // 毫秒时间戳
} taos_rule_result;

// 校验标识符（db/table 名）是否合法：仅允许 [a-zA-Z0-9_]
// 返回 1 合法，0 不合法
int taos_sink_valid_identifier(const char *name);

// 启动/复用 sink 后台线程，并确保目标表已初始化
// 返回 0 成功，非 0 失败
int taos_sink_start(const taos_sink_config *cfg);

// 非阻塞入队一条数据（broker 热路径调用）
// 内部深拷贝所有字段，调用方无需保持指针有效
// 返回 0 成功，非 0 失败
int taos_sink_enqueue(const taos_rule_result *result);

// 按配置入队一条数据；同库不同表时由 cfg->stable 决定写入目标表
int taos_sink_enqueue_with_config(
    const taos_sink_config *cfg, const taos_rule_result *result);

// 返回 1 已启动，0 未启动
int taos_sink_is_started(void);

// 停止所有 sink 并 flush 剩余数据（broker 关闭时调用）
void taos_sink_stop_all(void);

// 兼容旧调用
void taos_sink_stop(void);

#ifdef __cplusplus
}
#endif
