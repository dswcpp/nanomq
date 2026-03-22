#!/bin/bash
# NanoMQ TDengine Sink 测试脚本

cd /home/vere/workplace/nanomq/build

echo "=== 1. 停止旧进程 ==="
pkill -9 nanomq
sleep 1

echo "=== 2. 启动 NanoMQ ==="
./nanomq/nanomq start --old_conf ../etc/nanomq_taos_example.conf &
NANOMQ_PID=$!
sleep 3

echo "=== 3. 检查进程 ==="
if pgrep -x nanomq > /dev/null; then
    echo "✓ NanoMQ 运行中"
else
    echo "✗ NanoMQ 启动失败"
    exit 1
fi

echo "=== 4. 发送测试消息 ==="
for i in {1..3}; do
    echo "发送消息 $i"
    ./nanomq_cli/nanomq_cli pub -h localhost -t "sensor/temp" -m "{\"temperature\":$((20+i)),\"humidity\":$((50+i))}" -q 1
    sleep 1
done

echo "=== 5. 等待数据写入 ==="
sleep 3

echo "=== 6. 查询 TDengine ==="
taos -s "USE mqtt_rule; SHOW TABLES;"
taos -s "USE mqtt_rule; SELECT * FROM mqtt_data ORDER BY ts DESC LIMIT 5;"

echo "=== 测试完成 ==="
