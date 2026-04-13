# Ubuntu 全新主机 NanoMQ 安装命令

本文档适用于一台全新的 Ubuntu 主机，目标是在本机安装 NanoMQ，并使用焊接遥测入库配置接入本机 TDengine。

## 1. 交付目录

以下文件放在同一个目录下使用：

- `nanomq-nng-v0.24.14.deb`
- `nanomq_weld_taos.conf`
- `nanomq_pwd.conf`
- `weld_tdengine_schema.sql`
- `weld_tdengine_schema_reset.sql`
- `02-Ubuntu全新主机-TDengine安装与初始化.md`

假设当前目录为：

```bash
cd /path/to/0.24.14
sha256sum ./nanomq-nng-v0.24.14.deb
```

## 2. 安装 NanoMQ

```bash
sudo apt update
sudo apt install -y curl tmux
sudo dpkg -i ./nanomq-nng-v0.24.14.deb
dpkg -s nanomq | sed -n '1,20p'
```

安装完成后，建议把交付配置覆盖到运行目录：

```bash
sudo cp ./nanomq_weld_taos.conf /usr/local/etc/nanomq_weld_taos.conf
sudo cp ./nanomq_pwd.conf /usr/local/etc/nanomq_pwd.conf
sudo cp ./weld_tdengine_schema.sql /usr/local/etc/weld_tdengine_schema.sql
sudo chmod 644 /usr/local/etc/nanomq_weld_taos.conf
sudo chmod 600 /usr/local/etc/nanomq_pwd.conf
sudo chmod 644 /usr/local/etc/weld_tdengine_schema.sql
```

建议再核对一次交付配置中的写库账号和库名：

```bash
sed -n '1,120p' /usr/local/etc/nanomq_weld_taos.conf
```

## 3. 当前配置中的固定账号

### 3.1 NanoMQ MQTT/HTTP 账号

- username: `HshiAdmin`
- password: `H8jkLk99Lsdf`

### 3.2 TDengine 写库账号

- username: `iwsuser`
- password: `YWkE1jc_j`
- database: `mqtt_rule`

## 4. 启动前检查

确认以下端口未被占用：

```bash
ss -ltnp | grep -E ':1883|:8083|:6041'
```

确认 TDengine 已经完成安装和初始化：

```bash
curl -I http://127.0.0.1:6041
```

如果还未安装 TDengine，先执行：

- `02-Ubuntu全新主机-TDengine安装与初始化.md`

## 5. 启动 NanoMQ

```bash
sudo nanomq start --conf /usr/local/etc/nanomq_weld_taos.conf
```

前台调试可使用：

```bash
sudo nanomq start --conf /usr/local/etc/nanomq_weld_taos.conf --log_level info
```

停止：

```bash
sudo nanomq stop
```

## 6. 启动后验证

检查进程：

```bash
pgrep -a nanomq
```

检查端口：

```bash
ss -ltnp '( sport = :1883 )'
ss -ltnp '( sport = :8083 )'
```

检查 TDengine 超表：

```bash
taos -s "USE mqtt_rule; SHOW STABLES;"
```

检查两张 raw 高频表：

```bash
taos -s "USE mqtt_rule; DESCRIBE weld_current_raw;"
taos -s "USE mqtt_rule; DESCRIBE weld_voltage_raw;"
```

检查 NanoMQ 是否已按预期启用认证和 TDengine 写库配置：

```bash
sed -n '1,120p' /usr/local/etc/nanomq_weld_taos.conf
sed -n '1,40p' /usr/local/etc/nanomq_pwd.conf
```

## 7. MQTT 客户端连接参数

客户端连接 NanoMQ 时使用：

- host: 当前 Ubuntu 主机 IP
- port: `1883`
- username: `HshiAdmin`
- password: `H8jkLk99Lsdf`

如果客户端连接后立即断开，优先检查：

1. 连接的 IP 是否真的是这台 Ubuntu 主机
2. `1883` 是否已监听
3. 用户名密码是否一致
4. 防火墙是否放通 `1883`

## 8. 常用排障命令

查看 NanoMQ 运行状态：

```bash
pgrep -a nanomq
ss -ltnp '( sport = :1883 or sport = :8083 )'
```

查看最近入库情况：

```bash
curl -sS -u iwsuser:YWkE1jc_j http://127.0.0.1:6041/rest/sql/mqtt_rule -d "SHOW STABLES"
curl -sS -u iwsuser:YWkE1jc_j http://127.0.0.1:6041/rest/sql/mqtt_rule -d "SELECT COUNT(*) FROM weld_current_raw"
curl -sS -u iwsuser:YWkE1jc_j http://127.0.0.1:6041/rest/sql/mqtt_rule -d "SELECT COUNT(*) FROM weld_voltage_raw"
```

前台运行时建议重点观察三段日志：

- `pub_handler weld_taos`
- `weld_telemetry`
- `weld_taos_sink`
