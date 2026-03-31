# 焊接遥测入库上线说明

本文档用于运维上线 NanoMQ 焊接遥测解析入库能力。

## 1. 交付物

- 安装包: [nanomq-nng-v0.24.14-amd64.deb](/home/tery/project/nanomq/build-runtime/_packages/nanomq-nng-v0.24.14-amd64.deb)
- MQTT 规则配置: [nanomq_weld_taos.conf](/home/tery/project/nanomq/etc/nanomq_weld_taos.conf)
- TDengine 建表脚本: [weld_tdengine_schema.sql](/home/tery/project/nanomq/etc/weld_tdengine_schema.sql)
- TDengine 重建脚本: [weld_tdengine_schema_reset.sql](/home/tery/project/nanomq/etc/weld_tdengine_schema_reset.sql)

## 2. 安装

```bash
sudo dpkg -i /home/tery/project/nanomq/build-runtime/_packages/nanomq-nng-v0.24.14-amd64.deb
```

安装后默认文件位置：

- `/usr/bin/nanomq`
- `/usr/local/etc/nanomq_weld_taos.conf`
- `/usr/local/etc/weld_tdengine_schema.sql`

## 3. 上线前检查

确认以下条件：

- TDengine REST 服务可访问，默认端口 `6041`
- NanoMQ 监听端口 `1883` 未被占用
- NanoMQ HTTP 管理端口 `8083` 未被占用
- TDengine 用户对目标库有建库建表和写入权限

检查命令：

```bash
ss -ltnp | rg ':1883|:8083|:6041'
```

## 4. TDengine 初始化

全新环境执行建库建表脚本：

```bash
taos < /usr/local/etc/weld_tdengine_schema.sql
```

如果 `mqtt_rule` 已经存在，必须先确认其精度是 `us`：

```sql
SHOW CREATE DATABASE mqtt_rule;
```

若不是 `PRECISION 'us'`，不要继续使用旧库。处理方式二选一：

1. 删除旧库后执行重建脚本

```bash
taos < /usr/local/etc/weld_tdengine_schema_reset.sql
```

2. 修改 NanoMQ 配置中的 `database`，换一个全新的库名，再按初始化脚本创建

脚本会创建：

- 数据库 `mqtt_rule`
- 超表 `weld_env_point`
- 超表 `weld_flow_point`
- 超表 `weld_current_point`
- 超表 `weld_voltage_point`

## 5. 配置

本次上线使用：

- 配置文件 `/usr/local/etc/nanomq_weld_taos.conf`
- 数据库 `mqtt_rule`
- TDengine 地址 `127.0.0.1:6041`

如果现场账号、密码、地址不同，只修改以下配置项：

- `host`
- `port`
- `username`
- `password`
- `database`

## 6. 启动

```bash
nanomq start --conf /usr/local/etc/nanomq_weld_taos.conf --log_level info
```

如需停止：

```bash
nanomq stop
```

## 7. 规则说明

四类 MQTT 遥测规则如下：

- `weld/+/+/+/+/+/telemetry/env/#` -> `weld_env_point`
- `weld/+/+/+/+/+/telemetry/flow/#` -> `weld_flow_point`
- `weld/+/+/+/+/+/telemetry/power/#` 且 `payload.signal_type = 'current'` -> `weld_current_point`
- `weld/+/+/+/+/+/telemetry/power/#` 且 `payload.signal_type = 'voltage'` -> `weld_voltage_point`

## 8. 上线后验证

检查 NanoMQ 进程：

```bash
ps -ef | rg 'nanomq start'
```

检查端口：

```bash
ss -ltnp | rg ':1883|:8083'
```

检查 TDengine 表：

```sql
USE mqtt_rule;
SHOW STABLES;
DESCRIBE weld_env_point;
DESCRIBE weld_flow_point;
DESCRIBE weld_current_point;
DESCRIBE weld_voltage_point;
```

检查是否已产生子表：

```sql
USE mqtt_rule;
SHOW TABLES;
```

检查是否已有写入：

```sql
SELECT COUNT(*) FROM weld_env_point;
SELECT COUNT(*) FROM weld_flow_point;
SELECT COUNT(*) FROM weld_current_point;
SELECT COUNT(*) FROM weld_voltage_point;
```

## 9. 异常排查

若 NanoMQ 启动失败，优先检查：

- `1883` 端口已被旧 NanoMQ 或其他 MQTT Broker 占用
- 配置文件路径错误
- TDengine `6041` 未启动或不可访问

若 MQTT 已接收但未写库，优先检查：

- `payload.signal_type` 是否与规则匹配
- TDengine 账号密码是否正确
- 目标数据库是否为 `mqtt_rule`
- `mqtt_rule` 是否为 `PRECISION 'us'`
- 报文是否符合焊接遥测规范

## 10. 回滚

停止新版本：

```bash
nanomq stop
```

切回旧配置或旧二进制后重新启动。

如果只需要临时关闭焊接入库，可直接停用：

- `/usr/local/etc/nanomq_weld_taos.conf`

并切回原有 NanoMQ 配置文件启动。
