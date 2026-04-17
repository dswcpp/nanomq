# 麒麟现场问题排查与修复记录（2026-04-16）

本文记录 `192.168.9.28` 这台 Kylin 主机上，NanoMQ 与本地 TDengine 入库链路的现场排查、修复和最终状态。

本文不是部署手册，而是一次实际问题处理记录，目标是让后续运维能快速知道：

- 现场最初是什么状态
- 问题根因是什么
- 实际做了哪些修复
- 当前机器已经稳定在什么配置上

## 1. 现场背景

目标主机：

- 主机 IP：`192.168.9.28`
- 登录用户：`kylin`
- 系统：`Kylin Industry V10 SP1`

相关组件：

- NanoMQ：`0.24.15`
- 本地 TDengine REST：`127.0.0.1:6041`
- 目标数据库：`mqtt_rule`

## 2. 初始状态

初始检查时，机器上出现了以下情况：

- `nanomq` Debian 包已经安装，二进制位于 `/usr/bin/nanomq`
- `/usr/local/etc/nanomq_weld_taos.conf` 存在
- `/home/kylin/etc/nanomq_weld_taos.conf` 也存在
- 没有 `nanomq.service`
- 端口 `1883` 被 `mosquitto` 占用，不是 NanoMQ
- 用户目录下保留了源码、构建产物和脚本：
  - `/home/kylin/build-deb-notests/nanomq/nanomq`
  - `/home/kylin/scripts/start-nanomq-taos.sh`
  - `/home/kylin/scripts/stop-nanomq.sh`

## 3. 关键排查结论

### 3.1 以前不是通过 `systemd` 启动 NanoMQ

现场历史记录显示，NanoMQ 以前更像是通过手工命令或脚本启动，而不是标准服务：

- `./nanomq start --conf /backup/etc/nanomq_weld_taos.conf`
- `nanomq start --conf nanomq_weld_taos.conf`
- `/home/kylin/scripts/start-nanomq-taos.sh`

对应脚本的默认配置路径是：

```bash
/home/kylin/etc/nanomq_weld_taos.conf
```

所以“以前启动脚本默认用哪份配置”，结论是：

- 默认使用 `/home/kylin/etc/nanomq_weld_taos.conf`
- 不是 `/usr/local/etc/nanomq_weld_taos.conf`

### 3.2 `1883` 被 `mosquitto` 占用

现场最直接的冲突点是：

- `1883` 的监听者是 `mosquitto`
- 不是 `nanomq`

这会导致：

- 即使 NanoMQ 启动命令正确，也无法绑定 `1883`
- 现场容易误以为是 NanoMQ 启动失败或 TAOS 配置有问题

### 3.3 现场存在两份焊接 TAOS 配置

两份配置都存在：

- `/home/kylin/etc/nanomq_weld_taos.conf`
- `/usr/local/etc/nanomq_weld_taos.conf`

它们在排障过程中一度都被改为云库 `101.132.117.146`，随后又按现场要求切回本地库 `127.0.0.1`。

最终确认实际要使用的是：

- `/usr/local/etc/nanomq_weld_taos.conf`

### 3.4 `/usr/bin/nanomq start --conf ... -d` 未稳定留存进程

现场实际验证中，直接使用：

```bash
/usr/bin/nanomq start --conf /usr/local/etc/nanomq_weld_taos.conf -d
```

并没有稳定留下可用进程。

因此最终采用的服务化方式是：

- 由 `systemd` 以前台模式托管
- 使用 `Type=simple`
- 让 `systemd` 负责守护

## 4. 实际执行的修复

### 4.1 停掉现场已有的 NanoMQ 相关进程

处理动作包括：

- 清理 `tmux` 会话 `nanomq` / `nanomq-taos`
- 调用 `/home/kylin/scripts/stop-nanomq.sh`
- 对残留 `nanomq` 进程执行 `TERM/KILL`

目标是确保现场没有旧的 NanoMQ 进程残留。

### 4.2 停掉并卸载 `mosquitto`

执行结果：

- `mosquitto` 已停掉
- `mosquitto` 包已卸载
- `mosquitto-clients` 包已卸载
- 相关无用依赖已执行 `autoremove`

处理后：

- `1883` 已释放
- 后续由 NanoMQ 独占 `1883`

### 4.3 将 TAOS 连接切回本地数据库

根据现场要求，最终配置改为指向本地 TDengine：

- `host = "127.0.0.1"`
- `port = 6041`
- `database = "mqtt_rule"`

最终保留的实际运行配置文件为：

```bash
/usr/local/etc/nanomq_weld_taos.conf
```

其关键连接参数为：

```hocon
host = "127.0.0.1"
port = 6041
username = "root"
password = "Qwer_1234"
database = "mqtt_rule"
```

### 4.4 用指定配置重新启动 NanoMQ

现场最终验证启动命令为：

```bash
/usr/bin/nanomq start --conf /usr/local/etc/nanomq_weld_taos.conf
```

并确认：

- `1883` 监听成功
- `8083` 监听成功
- 日志中规则引擎 TAOS 配置打印正常
- TAOS 主机显示为 `127.0.0.1`

### 4.5 配置 `systemd` 服务并开机自启

新增服务文件：

```bash
/etc/systemd/system/nanomq.service
```

最终服务内容核心如下：

```ini
[Service]
Type=simple
PermissionsStartOnly=true
User=kylin
Group=kylin
WorkingDirectory=/home/kylin
ExecStartPre=/usr/bin/rm -f /tmp/nanomq_cmd.ipc /tmp/nanomq_hook.ipc
ExecStart=/usr/bin/nanomq start --conf /usr/local/etc/nanomq_weld_taos.conf
ExecStop=/usr/bin/nanomq stop
ExecStopPost=/usr/bin/rm -f /tmp/nanomq_cmd.ipc /tmp/nanomq_hook.ipc
Restart=on-failure
RestartSec=3
```

服务已执行：

```bash
sudo systemctl enable nanomq.service
sudo systemctl restart nanomq.service
```

## 5. 最终状态

截至本次处理结束，现场最终状态如下：

- `nanomq.service` 已存在
- `nanomq.service` 已启用开机自启
- `nanomq.service` 当前状态为 `active (running)`
- `mosquitto` 已卸载
- `mosquitto-clients` 已卸载
- `1883` 当前由 NanoMQ 监听
- `8083` 当前由 NanoMQ 监听
- NanoMQ 实际使用的配置文件是：
  - `/usr/local/etc/nanomq_weld_taos.conf`
- NanoMQ 当前连接的是本地 TDengine：
  - `127.0.0.1:6041`

## 6. 验证结果

现场已验证：

- `systemctl status nanomq` 正常
- `ss -ltnp '( sport = :1883 or sport = :8083 )'` 正常
- `systemd-analyze verify /etc/systemd/system/nanomq.service` 未报 NanoMQ 服务本身错误

服务检查结果显示：

- `User=kylin`
- `Group=kylin`
- `ExecStart=/usr/bin/nanomq start --conf /usr/local/etc/nanomq_weld_taos.conf`
- `Restart=on-failure`

## 7. 现场遗留信息

本次排障中保留了配置备份，文件名带时间戳后缀，例如：

- `/usr/local/etc/nanomq_weld_taos.conf.<timestamp>.bak`
- `/home/kylin/etc/nanomq_weld_taos.conf.<timestamp>.bak`

后续如需回滚，可优先使用这些备份。

## 8. 后续运维建议

后续在这台机器上继续维护时，建议遵循以下规则：

- 不要再重新安装 `mosquitto`
- 不要再让其他 broker 占用 `1883`
- 不要混用 `/home/kylin/etc/nanomq_weld_taos.conf` 和 `/usr/local/etc/nanomq_weld_taos.conf`
- 当前统一以 `/usr/local/etc/nanomq_weld_taos.conf` 为准
- 统一通过 `systemctl start|stop|restart nanomq` 管理

建议现场常用命令：

```bash
sudo systemctl status nanomq --no-pager -l
sudo journalctl -u nanomq -f
ss -ltnp '( sport = :1883 or sport = :8083 or sport = :6041 )'
```

## 9. 一句话结论

这台 Kylin 主机上的核心问题不是 NanoMQ 本身不可用，而是：

- 以前启动方式不统一
- `mosquitto` 占用了 `1883`
- 没有标准服务化
- 现场同时存在两份 TAOS 配置

本次处理后，已经收敛为：

- 卸载 `mosquitto`
- 使用 `/usr/local/etc/nanomq_weld_taos.conf`
- 连接本地 TDengine
- 通过 `systemd` 管理 NanoMQ 并开机自启
