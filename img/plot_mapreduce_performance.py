import pandas as pd
import matplotlib.pyplot as plt
import glob
import os
import re

# ========================
# 用户配置区
# ========================
INPUT_DIR = "MapReduce_S2"
# dstat CSV 文件名的匹配模式，例如 "metrics_node1.csv", "metrics_node2.csv"
CSV_PATTERN = "metrics_*.csv"
# 包含 MapReduce 迭代时间的性能报告文件名
PERFORMANCE_REPORT = "performance_report.txt"

# 图表和统计结果的输出目录
OUTPUT_DIR = "MapReduce_S2_Figure"

# 您希望分析的 MapReduce 迭代轮数
NUM_ITERATIONS = 10


# --- 初始化 ---
os.makedirs(OUTPUT_DIR, exist_ok=True)

# 设置图表默认样式
plt.rcParams.update({
    "figure.figsize": (14, 7),
    "font.size": 11,
    "axes.grid": True,
    "grid.alpha": 0.5
})

# dstat CSV 文件中的列名
COLUMNS = [
    "epoch", "usr", "sys", "idl", "wai", "hiq", "siq",
    "read", "writ",
    "recv", "send",
    "used", "buff", "cach", "free"
]


def parse_performance_report(filepath, num_iterations):
    """
    解析性能报告，提取前 N 轮的 "Total Iteraction Time" 并计算总时长。
    """
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"性能报告文件未找到: {filepath}")

    iteration_times_ms = []

    with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
        for line in f:
            line = line.strip()
            # 匹配: Iteration_1_Total Iteraction Time       :      94555 ms
            match = re.search(r'Iteration_(\d+)_Total\s+Iteraction\s+Time\s*:\s*(\d+)\s*ms', line)
            if match:
                iter_num = int(match.group(1))
                time_ms = int(match.group(2))
                if iter_num <= num_iterations:
                    # 使用列表索引对齐（iter_num 从 1 开始）
                    while len(iteration_times_ms) < iter_num:
                        iteration_times_ms.append(None)
                    iteration_times_ms[iter_num - 1] = time_ms
                if len([t for t in iteration_times_ms if t is not None]) >= num_iterations:
                    break

    valid_times = [t for t in iteration_times_ms if t is not None][:num_iterations]

    if not valid_times:
        raise ValueError(
            "在性能报告中未找到 'Iteration_X_Total Iteraction Time' 条目！\n"
            "请确保报告中包含类似以下的行:\n"
            "  Iteration_1_Total Iteraction Time       :      94555 ms"
        )

    total_ms = sum(valid_times)
    total_sec = total_ms / 1000.0
    print(f"✅ 成功从报告中解析了 {len(valid_times)} 轮迭代。")
    print(f"⏱️  前 {num_iterations} 轮迭代总耗时: {total_sec:.3f} 秒")
    return total_sec


def read_dstat_csv(filepath):
    """
    读取并预处理单个 dstat CSV 文件。
    """
    df = pd.read_csv(
        filepath,
        skiprows=5,
        header=None,
        names=COLUMNS,
        on_bad_lines='skip',
        engine='python'
    )

    for col in df.columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    df = df.dropna(subset=['epoch'])
    df = df.fillna(0)
    df['time'] = pd.to_datetime(df['epoch'], unit='s', origin='unix')

    base_name = os.path.basename(filepath).replace("metrics_", "").replace(".csv", "")
    node_name = base_name.split('(')[0]

    return df, node_name


# ========================
# 步骤 1: 解析性能报告，获取前 N 轮总时长
# ========================
TOTAL_DURATION_SEC = parse_performance_report(PERFORMANCE_REPORT, NUM_ITERATIONS)

# ========================
# 步骤 2: 加载所有 dstat 数据
# ========================
all_data = {}
for f in glob.glob(CSV_PATTERN):
    try:
        df, node = read_dstat_csv(f)
        all_data[node] = df
        print(f"✅ 已加载 {f} ({len(df)} 条样本)")
    except Exception as e:
        print(f"❌ 跳过 {f}: {e}")

if not all_data:
    raise RuntimeError("未找到有效的 dstat 数据！")

# ========================
# 步骤 3: 基于系统活动检测任务实际开始时间
# ========================
activity_start_epochs = []
for df in all_data.values():
    # 当 CPU 使用率 > 5% 或磁盘写入 > 1MB/s 时，认为任务开始
    cpu_active = (100 - df['idl']) > 5
    disk_active = df['writ'] > (1 * 1024 * 1024)
    active = cpu_active | disk_active

    if active.any():
        first_active_epoch = df.loc[active.idxmax(), 'epoch']
        activity_start_epochs.append(first_active_epoch)

if not activity_start_epochs:
    task_start_epoch = min(df['epoch'].min() for df in all_data.values())
else:
    task_start_epoch = min(activity_start_epochs)

task_end_epoch = task_start_epoch + TOTAL_DURATION_SEC

print(f"🎯 检测到任务开始于: {pd.to_datetime(task_start_epoch, unit='s')}")
print(f"🔚 {NUM_ITERATIONS} 轮迭代后结束于: {pd.to_datetime(task_end_epoch, unit='s')}")

# ========================
# 步骤 4: 根据任务起止时间裁剪数据
# ========================
filtered_data = {}
for node, df in all_data.items():
    mask = (df['epoch'] >= task_start_epoch) & (df['epoch'] <= task_end_epoch)
    filtered_df = df[mask].copy()

    # --- 为新指标计算做好准备 ---
    # 计算总内存，避免后续重复计算
    filtered_df['total_mem'] = filtered_df['used'] + filtered_df['buff'] + filtered_df['cach'] + filtered_df['free']

    filtered_data[node] = filtered_df
    print(f"📊 已过滤 {node}: 保留 {len(filtered_df)} / {len(df)} 条样本")


# ========================
# 通用绘图函数
# ========================
def plot_metric(title, ylabel, filename, get_y, legend_loc='upper right'):
    plt.figure()
    for node, df in filtered_data.items():
        if not df.empty:
            y_values = get_y(df)
            plt.plot(df['time'], y_values, label=node, linewidth=1.5)

    plt.title(title, fontsize=14)
    plt.xlabel("Time")
    plt.ylabel(ylabel)
    plt.legend(title="Node", loc=legend_loc)
    ax = plt.gca()
    ax.ticklabel_format(style='plain', axis='y')
    ax.set_ylim(bottom=0)  # Y轴从0开始
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}/{filename}", dpi=200)
    plt.close()
    print(f"✅ 已保存图表: {filename}")


# ========================
# 绘制论文中对应的三个核心指标图表
# ========================

# 1. CPU 使用率 (%)
plot_metric(
    f"Per-Node CPU Utilization (First {NUM_ITERATIONS} Iterations)",
    "CPU Usage (%)",
    "cpu_usage_percent.png",
    lambda df: df['usr']+df['sys'] #100 - df['idl']  # CPU使用率 = 100 - 空闲率
)

# 2. CPU I/O 等待时间 (%)
plot_metric(
    f"Per-Node CPU I/O Wait Time (First {NUM_ITERATIONS} Iterations)",
    "CPU I/O Wait (%)",
    "cpu_io_wait_percent.png",
    lambda df: df['wai']  # 'wai' 列本身就是 I/O 等待时间的百分比
)

# 3. 内存使用率 (%)
plot_metric(
    f"Per-Node Memory Utilization (First {NUM_ITERATIONS} Iterations)",
    "Memory Usage (%)",
    "memory_usage_percent.png",
    # 内存使用率 = (已用内存 / 总内存) * 100
    # 为避免除以零，在分母为0时返回0
    lambda df: (df['used']+df['buff'] / df['total_mem'] * 100).where(df['total_mem'] > 0, 0)
)

# ========================
# 生成核心指标的统计摘要
# ========================
stats = {}
for node, df in filtered_data.items():
    if not df.empty:
        stats[node] = {
            "CPU_Usage_avg(%)": (100 - df['idl']).mean(),
            "CPU_Usage_peak(%)": (100 - df['idl']).max(),
            "CPU_IOWait_avg(%)": df['wai'].mean(),
            "CPU_IOWait_peak(%)": df['wai'].max(),
            "Memory_Usage_avg(%)": ((df['used'] / df['total_mem'] * 100).where(df['total_mem'] > 0, 0)).mean(),
            "Memory_Usage_peak(%)": ((df['used'] / df['total_mem'] * 100).where(df['total_mem'] > 0, 0)).max(),
        }

if stats:
    stats_df = pd.DataFrame(stats).T
    stats_df.round(3).to_csv(f"{OUTPUT_DIR}/paper_metrics_summary.csv")

    print(f"\n📊 Performance Summary (First {NUM_ITERATIONS} Iterations):")
    print(stats_df.round(3))

    with open(f"{OUTPUT_DIR}/paper_metrics_summary.txt", "w") as f:
        f.write(f"=== Hadoop PageRank Performance Summary (First {NUM_ITERATIONS} Iterations) ===\n\n")
        f.write(stats_df.round(3).to_string())

    print(f"\n✅ 统计摘要已保存至: {OUTPUT_DIR}/paper_metrics_summary.txt and .csv")
else:
    print("\n⚠️ 未生成统计摘要，因为没有符合时间范围的数据。")

print(f"\n🎉 所有图表和统计数据已保存至目录 '{OUTPUT_DIR}'!")