import os
import glob
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime

# ========================
# 配置
# ========================
Method = "Giraph"
scale = "S3"
INPUT_DIR = f"{Method}_{scale}"          # 存放 metrics_*.csv 的目录
OUTPUT_DIR = f"{Method}_{scale}_Figure"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ====== 新增：设置分析时间窗口（秒）======
ANALYSIS_START_SEC = 0   # 从第 100 秒开始
ANALYSIS_END_SEC = 5300     # 到第 800 秒结束
# ======================================



COLUMNS = [
    "epoch", "usr", "sys", "idl", "wai", "hiq", "siq",
    "read", "writ",
    "recv", "send",
    "used", "buff", "cach", "free"
]

plt.rcParams.update({
    "figure.figsize": (14, 6),
    "font.size": 10,
    "axes.grid": True,
    "grid.alpha": 0.4
})

def load_dstat_csv(filepath):
    try:
        df = pd.read_csv(
            filepath,
            skiprows=6,           # 👈 根据你提供的 CSV，应为 6（不是 7）
            header=None,
            names=COLUMNS,
            on_bad_lines='skip',
            engine='python'
        )
        for col in COLUMNS:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        df = df.dropna(subset=['epoch']).fillna(0)
        basename = os.path.basename(filepath)
        node = basename.replace("metrics_", "").replace(".csv", "")
        return df, node
    except Exception as e:
        print(f"❌ 加载失败 {filepath}: {e}")
        return None, None

# ========================
# 主流程
# ========================
all_data = {}
for f in glob.glob(os.path.join(INPUT_DIR, "metrics_*.csv")):
    df, node = load_dstat_csv(f)
    if df is not None and not df.empty:
        all_data[node] = df
        print(f"✅ 加载 {node}: {len(df)} 条记录")

if not all_data:
    raise SystemExit("未找到有效数据！")

# 计算全局起始时间（最小 epoch）
global_start_epoch = min(df['epoch'].min() for df in all_data.values())
print(f"\n🕒 以 epoch={global_start_epoch:.3f} 作为 t=0")

# # 为每个节点添加相对时间（秒）
# for node, df in all_data.items():
#     df['rel_time_sec'] = df['epoch'] - global_start_epoch

# 为每个节点添加相对时间，并裁剪到分析窗口
filtered_data = {}
for node, df in all_data.items():
    df['rel_time_sec'] = df['epoch'] - global_start_epoch
    # 👇 关键：只保留 [ANALYSIS_START_SEC, ANALYSIS_END_SEC] 区间的数据
    df_window = df[
        (df['rel_time_sec'] >= ANALYSIS_START_SEC) &
        (df['rel_time_sec'] <= ANALYSIS_END_SEC)
    ].copy()
    if not df_window.empty:
        df_window['rel_time_sec'] -= ANALYSIS_START_SEC
        filtered_data[node] = df_window
        print(f"✂️  {node}: 裁剪后保留 {len(df_window)} 条记录 ({ANALYSIS_START_SEC}s ~ {ANALYSIS_END_SEC}s)")
    else:
        print(f"⚠️  {node}: 在指定时间窗口内无数据")

if not filtered_data:
    raise SystemExit(f"在 [{ANALYSIS_START_SEC}, {ANALYSIS_END_SEC}] 秒内未找到有效数据！")

all_data = filtered_data  # 替换为裁剪后的数据

# ========================
# 绘图函数（使用 rel_time_sec 作为横轴）
# ========================
def plot_metric(y_func, title, ylabel, filename):
    plt.figure()
    for node, df in all_data.items():
        y = y_func(df)
        plt.plot(df['rel_time_sec'], y, label=node, linewidth=1.2)
    plt.title(title)
    plt.xlabel("Time (seconds)")
    plt.ylabel(ylabel)
    plt.legend(loc='upper right')
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, filename), dpi=200)
    plt.close()
    print(f"✅ 保存图表: {filename}")

# 1. CPU 使用率 (%)
plot_metric(
    y_func=lambda df: df['usr'] + df['sys'],
    title="Per-Node CPU Utilization",
    ylabel="CPU Usage (%)",
    filename="cpu_usage.png"
)

# 2. CPU I/O Wait (%)
plot_metric(
    y_func=lambda df: df['wai'],
    title="Per-Node CPU I/O Wait Time",
    ylabel="I/O Wait (%)",
    filename="cpu_iowait.png"
)

# 3. 内存使用量（GB）
plot_metric(
    y_func=lambda df: (df['used']+df['buff']) / (1024**3),
    title="Per-Node Memory Used",
    ylabel="Memory Used (GB)",
    filename="memory_used_gb.png"
)

# 4. 磁盘写入速率（MB/s）
plot_metric(
    y_func=lambda df: df['writ'] / (1024**2),
    title="Per-Node Disk Write Rate",
    ylabel="Write Rate (MB/s)",
    filename="disk_write_mbs.png"
)

# 5. 网络发送速率（MB/s）
plot_metric(
    y_func=lambda df: df['send'] / (1024**2),
    title="Per-Node Network Send Rate",
    ylabel="Send Rate (MB/s)",
    filename="network_send_mbs.png"
)

# ========================
# 生成统计摘要（可选：也可基于 rel_time 裁剪时间段）
# ========================
summary = []
for node, df in all_data.items():
    total_mem = df['used'] + df['buff'] + df['cach'] + df['free']
    mem_used_pct = (df['used']+df['buff'] / total_mem * 100).where(total_mem > 0, 0)

    summary.append({
        "Node": node,
        "Duration (s)": df['rel_time_sec'].max(),
        "CPU_Usage_avg(%)": (df['usr'] + df['sys']).mean(),
        "CPU_IOWait_avg(%)": df['wai'].mean(),
        "Memory_Used_avg(GB)": (df['used']+df['buff'] / (1024**3)).mean(),
        "Memory_Usage_avg(%)": mem_used_pct.mean(),
        "Disk_Write_peak(MB/s)": (df['writ'] / (1024**2)).max(),
        "Network_Send_peak(MB/s)": (df['send'] / (1024**2)).max(),
    })

summary_df = pd.DataFrame(summary).set_index("Node")
summary_df.round(2).to_csv(os.path.join(OUTPUT_DIR, "summary.csv"))
print("\n📊 统计摘要:")
print(summary_df.round(2))

print(f"\n🎉 所有结果已保存至 '{OUTPUT_DIR}'")