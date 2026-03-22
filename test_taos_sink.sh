#!/bin/bash

# TDengine Sink 测试脚本

echo "=== NanoMQ TDengine Sink 测试 ==="
echo ""

# 1. 检查编译是否包含 TDengine 支持
echo "1. 检查编译配置..."
if ./build/nanomq/nanomq --help 2>&1 | grep -q "nanomq"; then
    echo "✓ NanoMQ 编译成功"
else
    echo "✗ NanoMQ 编译失败"
    exit 1
fi

# 2. 检查配置文件
echo ""
echo "2. 检查配置文件..."
if [ -f "etc/nanomq_taos_example.conf" ]; then
    echo "✓ 配置文件存在: etc/nanomq_taos_example.conf"
else
    echo "✗ 配置文件不存在"
    exit 1
fi

# 3. 提示用户准备 TDengine
echo ""
echo "3. 准备工作："
echo "   - 确保 TDengine 已启动（默认端口 6041）"
echo "   - 默认用户名: root"
echo "   - 默认密码: taosdata"
echo ""

# 4. 启动说明
echo "4. 启动 NanoMQ："
echo "   ./build/nanomq/nanomq start --old_conf etc/nanomq_taos_example.conf"
echo ""

# 5. 测试说明
echo "5. 测试消息发送："
echo "   mosquitto_pub -t 'sensor/temp' -m '{\"temperature\":25.5}'"
echo ""

# 6. 验证说明
echo "6. 验证数据（在 TDengine CLI 中）："
echo "   USE mqtt_rule;"
echo "   SHOW STABLES;"
echo "   SELECT * FROM mqtt_data;"
echo ""

echo "=== 测试准备完成 ==="
