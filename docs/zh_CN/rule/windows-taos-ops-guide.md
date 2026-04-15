# NanoMQ Windows TDengine 运维文档

本文面向 Windows 现场运维人员，说明 NanoMQ TAOS 安装包安装后如何初始化 TDengine、修改配置、启动服务、验证写入和排查问题。

本文以焊接遥测入库为目标场景，默认使用以下组件：

- NanoMQ Windows 安装目录：`C:\Program Files\NanoMQ TAOS`
- NanoMQ 配置文件：`C:\Program Files\NanoMQ TAOS\config\nanomq_weld_taos.conf`
- TDengine REST 地址示例：`192.168.204.128:6041`
- TDengine 数据库：`mqtt_rule`

## 1. 先明确一件事

当前安装包安装后，`nanomq.exe` 本身不是 Windows Service。

本文中“起服务”指以下两种方式之一：

- 手工启动 `nanomq.exe`
- 通过安装包创建的开机自启计划任务启动 `nanomq.exe`

如果安装时勾选了“Start NanoMQ TAOS automatically at system startup”，则安装器会创建计划任务，实现开机自启。

## 2. 安装后目录说明

安装完成后，默认目录结构如下：

```text
C:\Program Files\NanoMQ TAOS\
├─ bin\
│  ├─ nanomq.exe
│  ├─ nanomq_cli.exe
│  ├─ nngcat.exe
│  └─ libwinpthread-1.dll
├─ config\
│  ├─ nanomq.conf
│  ├─ nanomq_weld_taos.conf
│  ├─ weld_tdengine_schema.sql
│  └─ weld_tdengine_schema_reset.sql
├─ include\
├─ lib\
└─ share\
```

运维最常用的文件只有这几个：

- `C:\Program Files\NanoMQ TAOS\bin\nanomq.exe`
- `C:\Program Files\NanoMQ TAOS\bin\nanomq_cli.exe`
- `C:\Program Files\NanoMQ TAOS\config\nanomq_weld_taos.conf`
- `C:\Program Files\NanoMQ TAOS\config\weld_tdengine_schema.sql`
- `C:\Program Files\NanoMQ TAOS\config\weld_tdengine_schema_reset.sql`

## 3. TDengine 初始化

### 3.1 必须先确认 REST 可访问

在 NanoMQ 所在 Windows 主机执行：

```powershell
Test-NetConnection 192.168.204.128 -Port 6041
```

如果 `TcpTestSucceeded` 不是 `True`，先不要启动 NanoMQ，优先排查：

- 目标 IP 是否正确
- TDengine REST/adapter 是否启动
- 防火墙是否放通 `6041`

### 3.2 新建数据库和表

推荐在 TDengine 服务器上直接执行安装包自带的 schema 文件。

如果 TDengine 安装在 Linux 服务器上，可执行：

```bash
taos < weld_tdengine_schema.sql
```

如果你要在 Windows 侧通过 REST 验证数据库是否已建好，可执行：

```powershell
$auth = 'Basic ' + [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes('root:taosdata'))
Invoke-WebRequest `
  -Uri 'http://192.168.204.128:6041/rest/sql' `
  -Method Post `
  -Headers @{ Authorization = $auth } `
  -Body 'SHOW DATABASES;' `
  -UseBasicParsing |
Select-Object -ExpandProperty Content
```

### 3.3 必须满足的数据库要求

`mqtt_rule` 必须存在，并且数据库精度必须是 `us`。

安装包自带 schema 的核心约束是：

```sql
CREATE DATABASE IF NOT EXISTS mqtt_rule PRECISION 'us';
```

不要把这个库建成默认毫秒精度。否则高频点位时间写入会出错。

### 3.4 需要存在的表

初始化完成后，至少应存在以下 4 张 stable：

- `weld_env_point`
- `weld_flow_point`
- `weld_current_raw`
- `weld_voltage_raw`

可通过 REST 验证：

```powershell
$auth = 'Basic ' + [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes('root:taosdata'))
Invoke-WebRequest `
  -Uri 'http://192.168.204.128:6041/rest/sql' `
  -Method Post `
  -Headers @{ Authorization = $auth } `
  -Body 'USE mqtt_rule; SHOW STABLES;' `
  -UseBasicParsing |
Select-Object -ExpandProperty Content
```

## 4. 修改 NanoMQ 配置

打开：

`C:\Program Files\NanoMQ TAOS\config\nanomq_weld_taos.conf`

重点检查并修改以下连接段：

```hocon
rules.taos = [
    {
        conn = {
            host = "192.168.204.128"
            port = 6041
            username = "root"
            password = "taosdata"
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

至少确认下面 5 个值正确：

- `host`
- `port`
- `username`
- `password`
- `database`

如果数据库在远端机器，`host` 不能保留成 `127.0.0.1`。

## 5. 手工启动 NanoMQ

以管理员 PowerShell 或 CMD 打开：

```powershell
cd /d "C:\Program Files\NanoMQ TAOS\bin"
.\nanomq.exe start --conf "C:\Program Files\NanoMQ TAOS\config\nanomq_weld_taos.conf"
```

如果启动成功，通常会看到类似信息：

- Broker started successfully
- HTTP server listening on `8083`
- Rule engine TAOS 配置打印

停止命令：

```powershell
cd /d "C:\Program Files\NanoMQ TAOS\bin"
.\nanomq.exe stop
```

## 6. 开机自启

如果安装时勾选了开机自启，安装器会创建系统计划任务。

建议检查是否已创建：

```powershell
schtasks /Query /TN "NanoMQ TAOS Autostart"
```

如果存在，系统开机后会执行：

```cmd
C:\Program Files\NanoMQ TAOS\bin\nanomq.exe start --conf "C:\Program Files\NanoMQ TAOS\config\nanomq_weld_taos.conf"
```

如果安装时没有勾选，可以重新安装勾选，或者后续手工创建等价计划任务。

## 7. 验证 NanoMQ 是否正常运行

### 7.1 进程检查

```powershell
Get-Process nanomq -ErrorAction SilentlyContinue | Select-Object Id,ProcessName,Path
```

### 7.2 端口检查

```powershell
netstat -ano | findstr ":1883"
netstat -ano | findstr ":8083"
```

正常情况下应看到：

- `1883` 被 `nanomq.exe` 监听
- `8083` 被 `nanomq.exe` 监听

## 8. 发送一条测试消息

推荐使用安装包自带的 `nanomq_cli.exe`。

### 8.1 先生成 UTF-8 测试载荷文件

注意：

- Windows 下不要直接把复杂 JSON 长串塞给 `-m`
- 推荐先写成 UTF-8 文件，再用 `-f`
- `points[].ts_ms` 必须写真实 epoch 毫秒时间，不能写 `5000` 这种相对值

在 PowerShell 执行：

```powershell
$nowMs = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
$msgId = "msg-env-live-$nowMs"
$payload = @"
{"msg_class":"telemetry","gateway_id":"gw3568_01","device_id":"th01","msg_id":"$msgId","spec_ver":"1.0","ts_ms":$nowMs,"seq":52,"device_type":"temp_humidity_transmitter","device_model":"TH-485","signal_type":"environment","quality":{"code":0,"text":"ok"},"source":{"bus":"rs485","port":"ttyS1","protocol":"modbus"},"collect":{"period_ms":1000,"timeout_ms":200,"retries":2},"data":{"point_count":1,"fields":[{"name":"temperature","unit":"C"},{"name":"humidity","unit":"pct_rh"}],"points":[{"ts_ms":$nowMs,"values":[25.1,60.2]}]}}
"@
[System.IO.File]::WriteAllText(
  "C:\Program Files\NanoMQ TAOS\payload-env.json",
  $payload,
  [System.Text.UTF8Encoding]::new($false)
)
```

### 8.2 发布测试消息

```powershell
cd /d "C:\Program Files\NanoMQ TAOS\bin"
.\nanomq_cli.exe pub `
  -h 127.0.0.1 `
  -p 1883 `
  -q 1 `
  -i "ops-test-client" `
  -t "weld/v1/sz01/line_01/station_03/gw3568_01/telemetry/env/th01" `
  -f "C:\Program Files\NanoMQ TAOS\payload-env.json"
```

如果客户端连接成功，通常会看到：

```text
connect_cb: mqtt-tcp://127.0.0.1:1883 connect result: 0
```

## 9. 验证 TDengine 是否收到数据

### 9.1 查计数

```powershell
$auth = 'Basic ' + [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes('root:taosdata'))
Invoke-WebRequest `
  -Uri 'http://192.168.204.128:6041/rest/sql' `
  -Method Post `
  -Headers @{ Authorization = $auth } `
  -Body 'SELECT COUNT(*) FROM mqtt_rule.weld_env_point;' `
  -UseBasicParsing |
Select-Object -ExpandProperty Content
```

### 9.2 查刚刚写入的消息

将上一步生成的 `$msgId` 替换进 SQL：

```powershell
$auth = 'Basic ' + [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes('root:taosdata'))
Invoke-WebRequest `
  -Uri 'http://192.168.204.128:6041/rest/sql' `
  -Method Post `
  -Headers @{ Authorization = $auth } `
  -Body "SELECT msg_id, topic_name, temperature, humidity FROM mqtt_rule.weld_env_point WHERE msg_id = 'msg-env-live-xxxxxxxxxxxxx';" `
  -UseBasicParsing |
Select-Object -ExpandProperty Content
```

如果返回 1 行，并带出：

- `msg_id`
- `topic_name`
- `temperature = 25.1`
- `humidity = 60.2`

则说明链路已打通。

## 10. 现场排障顺序

如果“程序启动正常，但数据库没有新数据”，请按这个顺序查，不要跳步：

### 10.1 先查 TDengine 连通性

```powershell
Test-NetConnection 192.168.204.128 -Port 6041
```

### 10.2 再查配置是否真的指向目标数据库

检查：

`C:\Program Files\NanoMQ TAOS\config\nanomq_weld_taos.conf`

至少确认：

- `host = "192.168.204.128"`
- `port = 6041`
- `database = "mqtt_rule"`

### 10.3 再查数据库结构

确认：

- `mqtt_rule` 存在
- 精度是 `us`
- 4 张 stable 存在

### 10.4 再查是否真有 MQTT 消息发到 broker

如果 `nanomq_cli pub` 都连不上，那不是 TAOS 问题，而是 broker 没起、端口不对，或被防火墙挡住。

### 10.5 再看 NanoMQ 日志

重点关注这些日志：

- `rule engine taos`
- `weld_telemetry_async: started`
- `weld_taos_sink: started`
- `payload is not valid json`
- `Timestamp data out of range`
- `queue full`
- `batch insert failed`

## 11. 最常见问题

### 11.1 `payload is not valid json`

常见原因：

- 直接用 `-m` 发布复杂 JSON，被 PowerShell/CMD 转义破坏
- 文件不是 UTF-8

建议：

- 一律先写文件
- 用 `-f`
- 用 UTF-8 无 BOM 保存

### 11.2 `Timestamp data out of range`

常见原因：

- `points[].ts_ms` 写成 `5000`
- `ts_ms` / `ts_us` 不是 epoch 时间

正确做法：

- `ts_ms` 填当前 Unix 毫秒时间
- 或 `ts_us` 填当前 Unix 微秒时间

### 11.3 程序起了，但数据库没增长

优先检查：

- `host` 是否还是 `127.0.0.1`
- `6041` 是否可达
- `mqtt_rule` 是否存在
- schema 是否已执行
- MQTT 主题是否命中 `rules.taos`

### 11.4 开机后没有自动启动

检查：

```powershell
schtasks /Query /TN "NanoMQ TAOS Autostart"
```

如果没有这个任务，说明安装时没勾选开机自启，或任务创建失败。

## 12. 建议的现场交付口径

给现场用户时，建议明确说明下面 3 句话：

1. NanoMQ 安装后默认不是 Windows Service，但可以通过安装器勾选开机自启。
2. 真正入库前必须先初始化 TDengine 的 `mqtt_rule` 数据库和 4 张 stable。
3. 如果 TDengine 不在本机，必须手工修改 `nanomq_weld_taos.conf` 里的 `host`。

## 13. 版本管控

### 13.1 文档版本

| 项目 | 内容 |
|------|------|
| 文档名称 | NanoMQ Windows TDengine 运维文档 |
| 当前文档版本 | v1.0 |
| 适用 NanoMQ 版本 | 0.24.15 |
| 适用安装方式 | Windows ZIP / Windows ISS 安装包 |
| 适用场景 | 焊接遥测写入 TDengine |

### 13.2 运维侧必须管控的版本对象

现场至少要同步管理下面 4 类版本：

- NanoMQ 程序版本
- 安装包版本
- 配置文件版本
- TDengine schema 版本

建议每次发版时，固定保留以下信息：

- 安装包文件名
- 安装包 SHA-256
- 配置文件修改人和修改时间
- TDengine schema 执行时间
- TDengine 目标地址

### 13.3 配置变更管控要求

`nanomq_weld_taos.conf` 属于受控文件，现场不要无记录修改。

每次修改以下任一项，都应登记：

- `host`
- `port`
- `username`
- `password`
- `database`
- `rules.taos` 中的 `sql`
- `rules.taos` 中的 `table`
- `parser`

建议在现场维护一个独立变更登记表，至少记录：

- 变更时间
- 变更人
- 变更前
- 变更后
- 变更原因
- 是否已验证

### 13.4 Schema 版本管控要求

以下文件应和安装包版本一起管理，不要混用：

- `weld_tdengine_schema.sql`
- `weld_tdengine_schema_reset.sql`

如果 NanoMQ 升级了，但数据库仍沿用旧 schema，可能出现：

- 缺列
- 写入失败
- 时间精度不一致
- 原始载荷字段不兼容

所以现场升级顺序必须固定为：

1. 确认本次 NanoMQ 对应的 schema 文件版本
2. 确认 `mqtt_rule` 当前结构是否匹配
3. 必要时先执行 schema 升级或重建
4. 再替换 NanoMQ 程序和配置

### 13.5 安装包版本识别建议

Windows 现场建议每次安装前先保存安装包文件名和哈希。

示例：

```powershell
Get-FileHash "nanomq-nng-v0.24.15-taos.zip" -Algorithm SHA256
```

如果是 ISS 安装包，也建议保留：

- 安装包文件名
- 安装时间
- 安装目录
- 是否勾选开机自启

### 13.6 修改记录

| 版本 | 日期 | 修改内容 |
|------|------|----------|
| v1.0 | 2026-04-14 | 初版，补充 Windows 安装后目录说明、TDengine 初始化、NanoMQ 启动/停止、开机自启、远端 `192.168.204.128:6041` 配置、测试写入、排障顺序和版本管控要求。 |
