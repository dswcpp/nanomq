# 麒麟系统 `.deb` 安装与服务化运维手册

本文面向现场运维，目标是让一台全新的 Kylin（麒麟）Linux 主机在拿到 NanoMQ 的 `.deb` 安装包后，完成以下工作：

- 安装 NanoMQ
- 修改焊接遥测入 TDengine 的配置
- 配置 `systemd` 服务并设置开机自启
- 验证 MQTT 接收与 TDengine 入库
- 在故障时快速定位问题

本文针对当前项目里的焊接遥测场景，默认使用：

- MQTT 监听端口：`1883`
- NanoMQ HTTP 管理端口：`8083`
- TDengine REST 端口：`6041`
- 焊接入库配置文件：`/usr/local/etc/nanomq_weld_taos.conf`

## 1. 前提条件

部署前确认：

- 目标系统是 Kylin Linux，且可使用 `sudo`
- 已拿到 NanoMQ Debian 安装包，例如 `nanomq-nng-v0.24.15-kylin-amd64.deb`
- 目标主机可以访问 TDengine REST 地址 `http://<tdengine-host>:6041`
- TDengine 账号已经具备目标数据库的建库、建表、写入权限
- 现场没有其他进程占用 `1883`、`8083`

建议预装基础工具：

```bash
sudo apt update
sudo apt install -y curl jq net-tools iproute2 procps
```

## 2. 安装包会安装什么

当前 `.deb` 安装后通常会提供这些文件：

- `/usr/bin/nanomq`
- `/usr/bin/nanomq_cli`
- `/usr/local/etc/nanomq.conf`
- `/usr/local/etc/nanomq_taos.conf`
- `/usr/local/etc/nanomq_weld_taos.conf`

注意：

- 该安装包默认不自带 `systemd` 服务单元
- 焊接遥测所需的 TDengine schema SQL 通常不在 `.deb` 内，需要随交付包单独提供

如果你只有 `.deb`，但没有 schema 脚本，先不要直接上线焊接入库功能。

## 3. 安装 NanoMQ

将安装包上传到目标机器后执行：

```bash
cd /path/to/package
sudo dpkg -i ./nanomq-nng-v0.24.15-kylin-amd64.deb
```

如果 `dpkg` 提示依赖问题，继续执行：

```bash
sudo apt --fix-broken install -y
```

安装完成后验证：

```bash
command -v nanomq
command -v nanomq_cli
ls -l /usr/local/etc/nanomq.conf /usr/local/etc/nanomq_weld_taos.conf
dpkg -s nanomq
```

## 4. 初始化 TDengine

如果目标是焊接遥测入库，必须先准备数据库和超表。

建议使用本仓库提供的脚本：

- `etc/weld_tdengine_schema.sql`
- `etc/weld_tdengine_schema_reset.sql`

在 TDengine 侧执行初始化：

```bash
taos < weld_tdengine_schema.sql
```

如果 `mqtt_rule` 已存在，先确认数据库精度：

```sql
SHOW CREATE DATABASE mqtt_rule;
```

必须是：

```sql
PRECISION 'us'
```

如果不是 `us`，不要继续直接复用旧库。应选择以下方式之一：

1. 删除旧库后重建
2. 改用新的数据库名重新初始化

初始化完成后验证：

```bash
curl -u <user>:<password> \
  http://<tdengine-host>:6041/rest/sql/mqtt_rule \
  -d 'show stables'
```

预期至少能看到：

- `weld_env_point`
- `weld_flow_point`
- `weld_current_raw`
- `weld_voltage_raw`

## 5. 配置 NanoMQ

焊接遥测场景不要使用默认的 `/usr/local/etc/nanomq.conf`。

应修改：

```bash
sudo vi /usr/local/etc/nanomq_weld_taos.conf
```

至少核对以下配置块：

```hocon
listeners.tcp {
    bind = "0.0.0.0:1883"
}

http_server {
    enable = true
    port = 8083
}

auth {
    allow_anonymous = true
}

rules.taos = [
    {
        conn = {
            host = "101.132.117.146"
            port = 6041
            username = "iwsuser"
            password = "YWkE1jc_j"
            database = "mqtt_rule"
        }
        rules = [
            {
                sql = "SELECT * FROM \"weld/+/+/+/+/+/telemetry/env/#\""
                table = "weld_env_point"
                parser = "weld_telemetry"
            },
            {
                sql = "SELECT * FROM \"weld/+/+/+/+/+/telemetry/flow/#\""
                table = "weld_flow_point"
                parser = "weld_telemetry"
            },
            {
                sql = "SELECT * FROM \"weld/+/+/+/+/+/telemetry/power/#\" WHERE payload.signal_type = 'current'"
                table = "weld_current_raw"
                parser = "weld_telemetry"
            },
            {
                sql = "SELECT * FROM \"weld/+/+/+/+/+/telemetry/power/#\" WHERE payload.signal_type = 'voltage'"
                table = "weld_voltage_raw"
                parser = "weld_telemetry"
            }
        ]
    }
]
```

重点说明：

- `http_server.port` 是 NanoMQ 自己的 HTTP 管理口，建议保持 `8083`
- `rules.taos[].conn.port` 才是 TDengine REST 端口，通常是 `6041`
- 两者不要混改，否则可能把 NanoMQ 管理口错误地改到 TDengine 端口
- `parser = "weld_telemetry"` 不能删

保存后建议先检查关键项：

```bash
grep -n -E 'http_server|rules.taos|host = |port = |username = |database =' /usr/local/etc/nanomq_weld_taos.conf
```

## 6. 创建运行用户

推荐使用专用系统账号运行 NanoMQ，而不是直接用 root 常驻。

```bash
sudo useradd --system --home /var/lib/nanomq --create-home --shell /usr/sbin/nologin nanomq
sudo chown nanomq:nanomq /var/lib/nanomq
```

如果现场已经有专门的运维账号，也可以把后面的服务单元中的 `User` 和 `Group` 改成现有账号。

## 7. 配置 `systemd` 服务

创建服务文件：

```bash
sudo tee /etc/systemd/system/nanomq.service >/dev/null <<'EOF'
[Unit]
Description=NanoMQ Broker (Weld TAOS)
Wants=network-online.target
After=network-online.target

[Service]
Type=simple
PermissionsStartOnly=true
User=nanomq
Group=nanomq
WorkingDirectory=/var/lib/nanomq
ExecStartPre=/usr/bin/rm -f /tmp/nanomq_cmd.ipc /tmp/nanomq_hook.ipc
ExecStart=/usr/bin/nanomq start --conf /usr/local/etc/nanomq_weld_taos.conf
ExecStop=/usr/bin/nanomq stop
ExecStopPost=/usr/bin/rm -f /tmp/nanomq_cmd.ipc /tmp/nanomq_hook.ipc
Restart=on-failure
RestartSec=3
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF
```

然后执行：

```bash
sudo systemctl daemon-reload
sudo systemctl enable nanomq.service
sudo systemctl restart nanomq.service
```

验证服务状态：

```bash
sudo systemctl status nanomq.service --no-pager -l
```

正常时应看到：

- `Loaded: loaded (/etc/systemd/system/nanomq.service; enabled; ...)`
- `Active: active (running)`
- 主进程为 `/usr/bin/nanomq start --conf /usr/local/etc/nanomq_weld_taos.conf`

## 8. 验证监听端口

检查端口：

```bash
ss -ltnp | grep -E ':(1883|8083) '
```

预期：

- `1883` 由 `nanomq` 监听
- `8083` 由 `nanomq` 监听

如果看不到 `1883` 或 `8083`，先回头查服务日志。

## 9. 验证 TDengine 认证

先直接验证 NanoMQ 里使用的同一组账号是否真的可用：

```bash
curl -u iwsuser:YWkE1jc_j \
  http://101.132.117.146:6041/rest/sql/mqtt_rule \
  -d 'show stables'
```

如果返回 `Authentication failure`，不要继续查 MQTT 规则，先修正 TDengine 账号密码。

## 10. 验证 MQTT 发布与入库

### 10.1 发布一条环境遥测样本

```bash
TS_MS=$(date +%s)000
MSG_ID=verify-env-$(date +%Y%m%d-%H%M%S)
TOPIC='weld/v1/siteA/lineA/stationA/gw001/telemetry/env/dev001'

cat >/tmp/verify-env.json <<EOF
{"msg_class":"telemetry","ts_ms":$TS_MS,"gateway_id":"gw001","device_id":"dev001","msg_id":"$MSG_ID","spec_ver":"1.0","task_id":"verify","seq":1,"device_type":"temp_humidity_transmitter","device_model":"demo","signal_type":"environment","channel_id":"ch1","quality":{"code":0,"text":"ok"},"source":{"bus":"modbus","port":"ttyS1","protocol":"rtu"},"collect":{"period_ms":1000,"timeout_ms":500,"retries":1},"data":{"fields":[{"name":"temperature"},{"name":"humidity"}],"point_count":1,"points":[{"ts_ms":$TS_MS,"values":[25.5,60.1]}]}}
EOF

nanomq_cli pub -h 127.0.0.1 -p 1883 -t "$TOPIC" -f /tmp/verify-env.json
```

### 10.2 查库验证

```bash
curl -u iwsuser:YWkE1jc_j \
  http://101.132.117.146:6041/rest/sql/mqtt_rule \
  -d "select ts, msg_id, topic_name, temperature, humidity from weld_env_point where msg_id='$MSG_ID' order by ts desc limit 1"
```

如果返回该条记录，说明链路已打通：

- MQTT 发布成功
- NanoMQ 规则命中成功
- `weld_telemetry` 解析成功
- TDengine 写入成功

## 11. 日常运维命令

启动、停止、重启：

```bash
sudo systemctl start nanomq
sudo systemctl stop nanomq
sudo systemctl restart nanomq
```

查看状态：

```bash
sudo systemctl status nanomq --no-pager -l
```

持续看日志：

```bash
sudo journalctl -u nanomq -f
```

查看最近日志：

```bash
sudo journalctl -u nanomq -n 100 --no-pager
```

查看进程：

```bash
ps -ef | grep '[n]anomq'
```

## 12. 常见问题

### 12.1 `Unit nanomq.service could not be found`

原因：

- 安装包只装了二进制和配置，没有自带 `systemd` 单元

处理：

- 按本文第 7 节手工创建 `/etc/systemd/system/nanomq.service`

### 12.2 `Abort finding default config path`

这是当前构建里一条误导性日志，不能单凭它判断启动失败。

真正判断方式是看：

- `systemctl status nanomq`
- 是否成功监听 `1883`、`8083`

### 12.3 `nng_listen ipc: Address in use`

原因通常是旧的 IPC 文件残留：

- `/tmp/nanomq_cmd.ipc`
- `/tmp/nanomq_hook.ipc`

处理：

- 服务单元里保留 `ExecStartPre` 和 `ExecStopPost` 清理
- 不要删除本文里的 `PermissionsStartOnly=true`

### 12.4 `TDengine error: {"code":855,"desc":"Authentication failure"}`

这不是 MQTT 接收问题，而是 TDengine 认证失败。

优先检查：

- `/usr/local/etc/nanomq_weld_taos.conf` 中的 `username`
- `/usr/local/etc/nanomq_weld_taos.conf` 中的 `password`
- 目标数据库名是否正确
- 目标用户是否对 `mqtt_rule` 有权限

### 12.5 broker 正常，但数据库没有新数据

按以下顺序排查：

1. 是否用的是 `/usr/local/etc/nanomq_weld_taos.conf`
2. MQTT 客户端是否真的连到了 `1883`
3. 发布主题是否匹配 `weld/.../telemetry/...`
4. `payload.signal_type` 是否和规则一致
5. TDengine 认证是否通过

### 12.6 端口冲突

如果日志里有 `Address in use`，检查：

```bash
ss -ltnp | grep -E ':(1883|8083|6041) '
```

注意：

- `1883` 属于 MQTT
- `8083` 属于 NanoMQ HTTP
- `6041` 属于 TDengine REST

不要把 `http_server.port` 错误改成 `6041`

## 13. 回滚

如果变更后需要回滚：

1. 停止服务
2. 恢复旧配置文件
3. 重新启动服务

命令示例：

```bash
sudo systemctl stop nanomq
sudo cp /usr/local/etc/nanomq_weld_taos.conf.<backup> /usr/local/etc/nanomq_weld_taos.conf
sudo systemctl start nanomq
```

如果只是临时关闭焊接入库，也可以直接停用该服务，或改回普通 broker 配置后再启动。
