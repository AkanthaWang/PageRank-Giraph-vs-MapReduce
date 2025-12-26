package edu.practice.pagerank;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

/**
 * PageRank Reduce 阶段:
 * 1. 聚合所有传入的 PageRank 贡献值。
 * 2. 识别悬挂节点（无出链的节点）。
 * 3. 应用 PageRank 公式计算新的 PageRank 值。
 * 4. 处理悬挂节点的 PR 贡献。
 */
public class PageRankReducer extends Reducer<Text, Text, Text, Text> {

    private double D; // 阻尼系数 (Damping Factor)
    private long N; // 总节点数 (Total Nodes)
    private double danglingPRSum; // 悬挂节点 PR 总和 (来自上一轮迭代的计数器)
    private long wallStart;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 从配置中获取全局参数
        D = context.getConfiguration().getDouble("DAMPING_FACTOR", 0.85);
        N = context.getConfiguration().getLong(PageRankDriver.TOTAL_NODES_KEY, 1);
        danglingPRSum = context.getConfiguration().getDouble(PageRankDriver.DANGLING_PR_SUM_KEY, 0.0);
        
        if (N <= 0) {
            N = 1; // 防止除以零
        }
        wallStart = System.currentTimeMillis();
    }

    @Override
    public void reduce(Text nodeId, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // S = Sum(PR(Pj) / L(Pj))
        double linkContributionSum = 0.0;
        String outlinksStr = "";
        double previousPR = 0.0;
        boolean isDanglingNode = false;
        boolean hasStructure = false;
        int valueCount = 0;
        
        // 迭代 Reducer 接收到的所有值
        for (Text value : values) {
            String strValue = value.toString().trim();
            valueCount++;
            
            if (strValue.isEmpty()) {
                context.getCounter("DataQuality", "Empty_Values").increment(1);
                continue;
            }

            if (strValue.startsWith(PageRankMapper.STRUCTURE_PREFIX)) {
                // 找到节点结构信息，格式：STRUCT|<PrevPR>|Outlink1,Outlink2,...
                hasStructure = true;
                String rest = strValue.substring(PageRankMapper.STRUCTURE_PREFIX.length());
                int sepIdx = rest.indexOf("|");
                if (sepIdx >= 0) {
                    try {
                        String prStr = rest.substring(0, sepIdx).trim();
                        previousPR = prStr.isEmpty() ? 0.0 : Double.parseDouble(prStr);
                    } catch (NumberFormatException e) {
                        context.getCounter("DataQuality", "PR_Parse_Error").increment(1);
                        System.err.println("ERROR: Invalid PR value in structure for node " + nodeId + ": " + rest.substring(0, sepIdx));
                        previousPR = 0.0;
                    }
                    outlinksStr = rest.substring(sepIdx + 1);
                } else {
                    // 兼容性：若没有包含上一次 PR，则把全部作为 outlinks
                    outlinksStr = rest;
                }
                
                // 检测悬挂节点（无出链）
                isDanglingNode = outlinksStr.trim().isEmpty();
                if (isDanglingNode) {
                    // 累加悬挂节点 PR 值到全局计数器
                    context.getCounter(PageRankDriver.PageRankCounter.DANGLING_PR_SUM).increment(
                        Math.round(previousPR * PageRankDriver.SCALE_FACTOR_LONG)
                    );
                }
            } else {
                // PageRank 贡献值 (Vote)
                try {
                    double contribution = Double.parseDouble(strValue);
                    linkContributionSum += contribution;
                } catch (NumberFormatException e) {
                    context.getCounter("DataQuality", "Contribution_Parse_Error").increment(1);
                    System.err.println("ERROR: Invalid PR contribution for node " + nodeId + ": " + strValue);
                }
            }
        }
        
        // 数据完整性检查
        if (!hasStructure) {
            context.getCounter("DataQuality", "Missing_Structure").increment(1);
            System.err.println("WARNING: No structure information for node " + nodeId + ", received " + valueCount + " values");
        }

        // 1. 随机跳转项 (Random Jump): (1 - D) / N
        double randomJumpTerm = (1.0 - D) / N;

        // 2. 悬挂节点贡献项 (Dangling Node Contribution) - 将上一轮的悬挂质量均分给所有节点
        double danglingTerm = D * (danglingPRSum / N);

        // 3. 链接贡献项 (Link Contribution)
        double linkTerm = D * linkContributionSum;

        // 4. 计算新的 PageRank 值
        double newPageRank = randomJumpTerm + danglingTerm + linkTerm;
        
        // 确保 PR 值为非负数
        if (newPageRank < 0) {
            newPageRank = 0.0;
        }

        // 5. 输出节点的新状态 (PageRank|Outlinks)
        // Key: NodeID, Value: NewPageRank|Outlinks
        context.write(nodeId, new Text(String.format("%.10f", newPageRank) + "|" + outlinksStr));

        // 6. 计算本节点的 PR 变化并累加到全局计数器，用于收敛检测
        double diff = Math.abs(newPageRank - previousPR);
        try {
            long scaled = (long) Math.ceil(diff * PageRankDriver.SCALE_FACTOR_LONG);
            if (scaled < 0) scaled = 0;
            context.getCounter(PageRankDriver.PageRankCounter.PR_DIFF_SUM).increment(scaled);
        } catch (Exception e) {
            // 安全防护：计数器可能在某些环境不可用，忽略异常
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        long duration = System.currentTimeMillis() - wallStart;
        try {
            context.getCounter(PageRankDriver.PageRankCounter.REDUCE_WALL_MS).increment(duration);
        } catch (Exception ignored) {}
    }
}