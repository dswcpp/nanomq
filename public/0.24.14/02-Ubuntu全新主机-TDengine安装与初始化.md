# Ubuntu 全新主机 TDengine 安装与初始化

本文档面向一台全新的 Ubuntu 主机，目标是把 TDengine 3.x 安装成功，并初始化成 NanoMQ 可直接写入的状态。

适用场景：

- 机器上还没有装过 TDengine
- 或者之前装过，但服务状态混乱，准备重新按步骤确认

本文档默认：

- Ubuntu 使用 `systemd`
- 你有 `sudo` 权限
- NanoMQ 将通过 TDengine REST `6041` 写库

本项目约定的 TDengine 业务账号如下：

- username: `iwsuser`
- password: `YWkE1jc_j`
- database: `mqtt_rule`

如果你只是想先把 TDengine 装起来，再让 NanoMQ 连本机，严格按本文执行即可。

## 1. 安装前确认

先在 Ubuntu 主机执行：

```bash
uname -m
lsb_release -a
```

重点确认：

- 架构通常应为 `x86_64`
- 系统是 Ubuntu

再确认机器上当前是否已经有 TDengine：

```bash
command -v taos || true
command -v taosd || true
sudo systemctl list-unit-files | grep -E 'taosd|taosadapter' || true
ps -ef | grep -E 'taosd|taosadapter' | grep -v grep || true
ss -ltnp | grep -E ':6030|:6041' || true
```

如果这里已经看到：

- `taosd`
- `taosadapter`
- 或 `6030` / `6041` 已监听

说明机器上很可能已经装过 TDengine。不要直接重复安装，先看：

```bash
sudo systemctl status taosd --no-pager || true
sudo systemctl status taosadapter --no-pager || true
```

如果你就是要重装，建议先停服务：

```bash
sudo systemctl stop taosadapter || true
sudo systemctl stop taosd || true
```

## 2. 准备安装包

先从 TDengine 官方下载 Linux x64 安装包。

常见文件名类似：

```bash
tdengine-tsdb-<VERSION>-linux-x64.tar.gz
```

假设你已经把安装包下载到当前目录：

```bash
cd /path/to/tdengine-package
ls -lh
```

你应该能看到类似：

```bash
tdengine-tsdb-3.x.x.x-linux-x64.tar.gz
```

解压：

```bash
tar -zxvf tdengine-tsdb-<VERSION>-linux-x64.tar.gz
ls -la
```

解压后通常会出现一个同名目录，进入它：

```bash
cd tdengine-tsdb-<VERSION>-linux-x64
ls -la
```

此时应能看到类似文件：

- `install.sh`
- `driver/`
- `examples/`
- `taosd`
- `taosadapter`

如果你看不到 `install.sh`，说明：

- 进错目录了
- 或下载的不是完整 Linux 安装包

不要继续，先回到上一层重新 `ls -la` 检查。

## 3. 执行安装

先给脚本执行权限：

```bash
chmod +x install.sh
```

再执行安装。推荐直接用 `bash` 调，避免权限或解释器问题：

```bash
sudo bash ./install.sh
```

如果安装脚本会询问：

- 安装路径
- 是否覆盖旧文件
- 是否安装服务

对全新主机，一般保持默认即可。

如果执行时出现：

- `Permission denied`
- `bad interpreter`

仍然用下面这个命令重试：

```bash
sudo bash ./install.sh
```

如果安装中途失败，先不要反复重跑，先看最后几行输出。

## 4. 安装后立即检查命令和服务

安装脚本执行完后，先检查命令是否可用：

```bash
command -v taos
command -v taosd
command -v taosadapter || true
```

常见情况：

1. 能看到 `/usr/bin/taos`
2. 能看到 `/usr/bin/taosd`
3. `taosadapter` 可能也在 `/usr/bin/taosadapter`

如果 `command -v taos` 没输出，别急，继续检查：

```bash
ls -l /usr/bin/taos /usr/bin/taosd /usr/bin/taosadapter 2>/dev/null || true
```

如果这里也没有，说明安装脚本没有把程序装进系统路径，安装并没有成功，需要回到安装目录重新执行并检查报错。

## 5. 启动服务

先尝试直接启动并设置开机自启：

```bash
sudo systemctl enable --now taosd
sudo systemctl enable --now taosadapter
```

然后检查状态：

```bash
sudo systemctl status taosd --no-pager
sudo systemctl status taosadapter --no-pager
```

正常情况下，应该看到：

- `active (running)`

再检查端口：

```bash
ss -ltnp | grep -E ':6030|:6041'
```

正常情况下：

- `6030` 是 TDengine 服务端口
- `6041` 是 REST `taosadapter`

## 6. 如果服务启动失败，怎么查

如果 `taosd` 或 `taosadapter` 不是 `active (running)`，直接查日志：

```bash
sudo journalctl -u taosd -n 100 --no-pager
sudo journalctl -u taosadapter -n 100 --no-pager
```

如果 `systemctl` 提示没有这个服务名，再执行：

```bash
sudo systemctl list-unit-files | grep -E 'taos|adapter'
```

看实际的 service 名字。

如果 `taosd` 正常，但 `taosadapter` 启不来，优先检查：

- `taosd` 是否已先启动
- `6041` 是否被别的程序占用
- 配置文件是否损坏

占用检查：

```bash
ss -ltnp | grep ':6041' || true
```

如果有端口冲突，先停掉占用程序，再重启：

```bash
sudo systemctl restart taosd
sudo systemctl restart taosadapter
```

## 7. 验证 REST 是否可用

先测 REST 端口：

```bash
curl -I http://127.0.0.1:6041
```

如果返回 HTTP 响应头，说明 `taosadapter` 已经起来了。

如果这里不通，NanoMQ 就一定写不进去。

## 8. 进入 taos 命令行

直接执行：

```bash
taos
```

如果能进入交互式命令行，说明客户端工具可用。

如果提示命令不存在，再确认：

```bash
command -v taos
ls -l /usr/bin/taos 2>/dev/null || true
```

## 9. 初始化数据库和业务账号

本项目固定使用：

- database: `mqtt_rule`
- username: `iwsuser`
- password: `YWkE1jc_j`

直接执行下面这段：

```bash
taos <<'SQL'
CREATE DATABASE IF NOT EXISTS mqtt_rule PRECISION 'us';
CREATE USER IF NOT EXISTS iwsuser PASS 'YWkE1jc_j';
GRANT READ, WRITE ON mqtt_rule.* TO iwsuser;
SQL
```

说明：

- `mqtt_rule` 必须是 `PRECISION 'us'`
- NanoMQ 走 REST `6041` 写库
- 如果 `iwsuser` 已存在，`IF NOT EXISTS` 会跳过创建

## 10. 检查数据库精度

执行：

```bash
taos -s "SHOW CREATE DATABASE mqtt_rule;"
```

输出里必须包含：

```sql
PRECISION 'us'
```

如果不是 `us`，不要继续往下做。直接重建：

```bash
taos -s "DROP DATABASE IF EXISTS mqtt_rule;"
taos -s "CREATE DATABASE mqtt_rule PRECISION 'us';"
taos <<'SQL'
CREATE USER IF NOT EXISTS iwsuser PASS 'YWkE1jc_j';
GRANT READ, WRITE ON mqtt_rule.* TO iwsuser;
SQL
```

## 11. 执行焊接建表脚本

如果交付文件已经复制到 `/usr/local/etc`：

```bash
taos < /usr/local/etc/weld_tdengine_schema.sql
```

如果脚本还在交付目录：

```bash
cd /path/to/0.24.14
taos < ./weld_tdengine_schema.sql
```

如果你想整库重建：

```bash
cd /path/to/0.24.14
taos < ./weld_tdengine_schema_reset.sql
```

该脚本会创建：

- `weld_env_point`
- `weld_flow_point`
- `weld_current_raw`
- `weld_voltage_raw`

高频表含义：

- `weld_current_raw` 保存电流原始 payload
- `weld_voltage_raw` 保存电压原始 payload
- 下游按 `window_start_us`、`sample_rate_hz`、`point_count` 恢复时间轴

## 12. 检查建表结果

执行：

```bash
taos -s "USE mqtt_rule; SHOW STABLES;"
taos -s "USE mqtt_rule; DESCRIBE weld_current_raw;"
taos -s "USE mqtt_rule; DESCRIBE weld_voltage_raw;"
```

你至少应该看到四张超表：

- `weld_env_point`
- `weld_flow_point`
- `weld_current_raw`
- `weld_voltage_raw`

## 13. 用业务账号验证 REST 权限

直接执行：

```bash
curl -sS -u iwsuser:YWkE1jc_j \
  http://127.0.0.1:6041/rest/sql/mqtt_rule \
  -d "SHOW STABLES"
```

如果这里返回 JSON 结果，说明：

- `taosadapter` 正常
- `iwsuser` 账号密码正确
- `mqtt_rule` 可访问

再查一下 raw 表：

```bash
curl -sS -u iwsuser:YWkE1jc_j \
  http://127.0.0.1:6041/rest/sql/mqtt_rule \
  -d "DESCRIBE weld_current_raw"

curl -sS -u iwsuser:YWkE1jc_j \
  http://127.0.0.1:6041/rest/sql/mqtt_rule \
  -d "DESCRIBE weld_voltage_raw"
```

## 14. NanoMQ 中应使用的 TDengine 连接参数

NanoMQ 配置里应写成：

```hocon
rules.taos = [
    {
        conn = {
            host = "127.0.0.1"
            port = 6041
            username = "iwsuser"
            password = "YWkE1jc_j"
            database = "mqtt_rule"
        }
    }
]
```

## 15. 最常见失败点

### 15.1 `install.sh` 执行失败

优先重试：

```bash
sudo bash ./install.sh
```

并检查：

- 是否进到了真正解压后的目录
- 是否存在 `install.sh`
- 安装包是否下载完整

### 15.2 `taos` 命令不存在

执行：

```bash
command -v taos
ls -l /usr/bin/taos 2>/dev/null || true
```

如果都没有，说明安装没有成功。

### 15.3 `taosd` 起不来

执行：

```bash
sudo systemctl status taosd --no-pager
sudo journalctl -u taosd -n 100 --no-pager
```

### 15.4 `taosadapter` 起不来

执行：

```bash
sudo systemctl status taosadapter --no-pager
sudo journalctl -u taosadapter -n 100 --no-pager
ss -ltnp | grep ':6041' || true
```

### 15.5 `curl -I http://127.0.0.1:6041` 不通

说明 REST 没起来，优先检查：

- `taosadapter` 是否 `active (running)`
- `6041` 是否被占用
- `taosd` 是否已先启动

### 15.6 NanoMQ 能启动，但写不进 TDengine

优先检查：

1. `curl -I http://127.0.0.1:6041` 是否通
2. `iwsuser` / `YWkE1jc_j` 是否正确
3. `mqtt_rule` 是否存在
4. `SHOW CREATE DATABASE mqtt_rule` 是否为 `PRECISION 'us'`
5. 四张超表是否已创建
6. NanoMQ 配置里的 `username/password/database` 是否与本文一致

## 16. 失败时请把这些信息贴出来

如果你后面还装失败，直接把下面几段输出贴出来，我可以继续对症处理：

```bash
uname -m
lsb_release -a
sudo systemctl status taosd --no-pager
sudo systemctl status taosadapter --no-pager
sudo journalctl -u taosd -n 100 --no-pager
sudo journalctl -u taosadapter -n 100 --no-pager
ss -ltnp | grep -E ':6030|:6041' || true
command -v taos
taos -s "SHOW DATABASES;" || true
```
