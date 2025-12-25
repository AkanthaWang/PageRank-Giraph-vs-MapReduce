import pandas as pd
import matplotlib.pyplot as plt
import os

# ================= 配置区域 =================
INPUT_FILE = '_timings (2)(2).csv'  # 输入文件名
OUTPUT_FILE = 'giraph_gantt_chart.png'  # 输出图片名


# ===========================================

def main():
    # 1. 检查文件是否存在
    if not os.path.exists(INPUT_FILE):
        print(f"错误：在当前路径下未找到文件 '{INPUT_FILE}'")
        print("请确保csv文件与本脚本在同一目录下。")
        return

    # 2. 读取 CSV 文件
    try:
        df = pd.read_csv(INPUT_FILE)
        print(f"成功读取文件: {INPUT_FILE}")
    except Exception as e:
        print(f"读取文件失败: {e}")
        return

    iteration_count = df['Phase'].astype(str).str.contains('Superstep|Job|Iteration', case=False).sum()
    print(f"检测到的迭代次数: {iteration_count}")

    # 3. 数据预处理
    # 将毫秒转换为秒
    df['Duration_s'] = df['Duration_ms'] / 1000.0

    # 计算每个阶段的起始时间（累加前一个阶段的持续时间）
    # shift(1) 将数据下移一位，fillna(0) 将第一个阶段的开始时间设为 0
    df['Start_s'] = df['Duration_s'].cumsum().shift(1).fillna(0)

    # 4. 绘图设置
    fig, ax = plt.subplots(figsize=(10, 3))  # 宽10，高3，适合单条时间轴

    # 定义颜色 (参考 PDF 方案一)
    colors = {
        'Setup': '#d62728',  # 红色: Setup/Loading
        'Superstep': '#1f77b4',  # 蓝色: Compute
        'Cleanup': '#ff7f0e',  # 橙色: Write/Cleanup
        'Default': 'gray'
    }

    # 辅助集合用于去重图例
    legend_labels = set()

    y_pos = 0
    bar_height = 0.6

    # 5. 循环绘制每一段
    for index, row in df.iterrows():
        phase = row['Phase']
        duration = row['Duration_s']
        start = row['Start_s']

        # 根据阶段名称匹配颜色和标签
        if 'Setup' in phase:
            color = colors['Setup']
            label_text = 'Graph Loading / Setup'
        elif 'Superstep' in phase:
            color = colors['Superstep']
            label_text = 'Computation (Supersteps)'
        elif 'Cleanup' in phase or 'Write' in phase:
            color = colors['Cleanup']
            label_text = 'Cleanup / Write'
        else:
            color = colors['Default']
            label_text = 'Other'

        # 仅当标签未添加过时，才添加 label 参数（避免图例重复）
        if label_text not in legend_labels:
            lbl = label_text
            legend_labels.add(label_text)
        else:
            lbl = None

        # 绘制水平条形
        # edgecolor='white' 在连续的 Superstep 之间画出分割线，视觉效果更好
        ax.barh(y_pos, duration, left=start, height=bar_height,
                color=color, edgecolor='white', linewidth=1, label=lbl)

        # (可选) 如果阶段时间较长(>2秒)，在条形内部显示名称
        if duration > 2.0:
            ax.text(start + duration / 2, y_pos, f"{phase}\n{duration:.1f}s",
                    ha='center', va='center', color='white', fontsize=8, fontweight='bold')

    # 6. 图表美化
    ax.set_yticks([0])
    ax.set_yticklabels(['Giraph Execution'], fontweight='bold')

    # 限制 Y 轴范围，让条形居中
    ax.set_ylim(-0.5, 0.5)

    # 设置 X 轴
    total_time = df['Duration_s'].sum()
    ax.set_xlabel('Time (Seconds)')
    ax.set_xlim(0, total_time * 1.05)  # 右侧留出 5% 空隙

    # 标题
    final_title = f"Giraph PageRank Full Timeline ({iteration_count} Iterations)"
    ax.set_title(final_title, fontsize=14, pad=15, fontweight='bold')

    # 图例 (放置在图表下方)
    ax.legend(loc='upper center', bbox_to_anchor=(0.5, -0.25), ncol=3, frameon=False)

    # 去除多余边框
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_visible(False)

    # 添加垂直网格线
    ax.grid(axis='x', linestyle='--', alpha=0.5)

    plt.tight_layout()

    # 7. 保存图片
    plt.savefig(OUTPUT_FILE, dpi=300, bbox_inches='tight')
    print(f"绘图完成！图片已保存为: {os.path.abspath(OUTPUT_FILE)}")


if __name__ == "__main__":
    main()