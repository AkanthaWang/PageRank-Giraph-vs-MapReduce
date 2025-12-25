uster_monitor.sh
#!/bin/bash

# ==============================
# cluster_monitor.sh (FINAL)
# 功能：在多个节点上启动/停止 dstat 监控（含 CPU + 时间戳）
# ==============================

# === 配置区 ===
DATA_NODES=("node100" "node101" "node102" "node103" "node104" "node105")  # 替换为你的节点名
DURATION=10000                                 # 监控时长（秒）
OUTPUT_PREFIX="metrics"
LOG_FILE="/tmp/dstat_monitor.log"

# === 函数定义 ===

start_monitoring() {
    echo "[INFO] Starting dstat monitoring on ${#DATA_NODES[@]} nodes..."
    for node in "${DATA_NODES[@]}"; do
        echo "  → $node"
        # 关键：添加 -c（CPU）、-T（时间戳）
        ssh "$node" "nohup dstat -T -c -d -n -m --output ${OUTPUT_PREFIX}_\$(hostname).csv 1 $DURATION > $LOG_FILE 2>&1 &"
    done
    echo "[OK] Monitoring started. Data saved as ${OUTPUT_PREFIX}_<hostname>.csv"
    echo "[TIP] Run '$0 stop' to stop early."
}

stop_monitoring() {
    echo "[INFO] Stopping dstat on all nodes..."
    for node in "${DATA_NODES[@]}"; do
        echo "  → $node"
        # 更健壮的 kill 方式：匹配 dstat + output
        ssh "$node" "pkill -f 'dstat.*--output.*${OUTPUT_PREFIX}'"
    done
    echo "[OK] All dstat processes terminated."
}

# === 主逻辑 ===
case "${1:-}" in
    start)
        start_monitoring
        ;;
    stop)
        stop_monitoring
        ;;
    *)
        echo "Usage: $0 {start|stop}"
        echo "  start : Begin monitoring"
        echo "  stop  : Terminate monitoring"
        exit 1
        ;;
esac
