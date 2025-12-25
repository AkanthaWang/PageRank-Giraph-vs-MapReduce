package edu.practice.pagerank;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

/**
 * PageRank Map 阶段:
 * 1. 解析输入状态 (NodeID, PR|Outlinks)。
 * 2. 将 PR 贡献值分发给所有出链目标节点。
 * 3. 传递节点结构信息 (Outlinks) 给 Reducer。
 * 4. 准确处理悬挂节点和孤立节点。
 */
public class PageRankMapper extends Mapper<LongWritable, Text, Text, Text> {

    // 分隔符用于区分 PageRank 值和出链列表
    private static final String SEPARATOR = "|";
    // 传递节点结构信息的前缀
    public static final String STRUCTURE_PREFIX = "STRUCT|";
    // 用于表示悬挂节点（无出链）的标记
    public static final String DANGLING_MARKER = "!DANGLING!";
    private final Text outKey = new Text();
    private final Text outVal = new Text();
    private long wallStart;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        wallStart = System.currentTimeMillis();
    }
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        if (line.isEmpty() || line.startsWith("#")) {
            return;
        }
        
        // 输入格式: NodeID\tPageRank|Outlink1,Outlink2,...
        String[] parts = line.split("\t");
        if (parts.length != 2) {
            // 跳过格式错误的行
            return;
        }

        String nodeIdStr = parts[0].trim();
        if (nodeIdStr.isEmpty()) {
            return;
        }
        
        Text nodeId = new Text(nodeIdStr);
        String prAndLinks = parts[1].trim();

        // 查找 PageRank 值和出链列表的分隔符 '|'
        int separatorIndex = prAndLinks.indexOf(SEPARATOR);
        if (separatorIndex == -1) {
            // 结构信息缺失，跳过
            return;
        }

        // 1. 解析当前 PageRank (PR_current) 和出链列表 (Outlinks)
        double currentPageRank;
        try {
            currentPageRank = Double.parseDouble(prAndLinks.substring(0, separatorIndex).trim());
        } catch (NumberFormatException e) {
            return;
        }
        
        String outlinksStr = prAndLinks.substring(separatorIndex + 1).trim();
        String[] outlinks = outlinksStr.isEmpty() ? new String[0] : outlinksStr.split(",");
        
        // 2. 传递节点结构信息（包含当前 PageRank），格式: STRUCT|<PR>|Outlink1,Outlink2,...
        String nodeStructure = STRUCTURE_PREFIX + String.format("%.10f", currentPageRank) + SEPARATOR + outlinksStr;
        outKey.set(nodeId);
        outVal.set(nodeStructure);
        
        int outDegree = outlinks.length;
        
        // 只对非悬挂节点发送结构信息
        if (outDegree > 0) {
            context.write(outKey, outVal);
        }

        // 3. 计算并分发 PR 贡献值
        if (outDegree > 0) {
            // 非悬挂节点: 计算贡献值 = PR_current / OutDegree
            double contribution = currentPageRank / outDegree;
            String contributionStr = String.format("%.10f", contribution);
            outVal.set(contributionStr);
            for (String targetId : outlinks) {
                String cleanTargetId = targetId.trim();
                if (!cleanTargetId.isEmpty()) {
                    outKey.set(cleanTargetId);
                    context.write(outKey, outVal);
                }
            }
        } else {
            // 悬挂节点: 发送结构信息（不再使用 DANGLING_MARKER）
            outVal.set(nodeStructure);
            context.write(outKey, outVal);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        long duration = System.currentTimeMillis() - wallStart;
        try {
            context.getCounter(PageRankDriver.PageRankCounter.MAP_WALL_MS).increment(duration);
        } catch (Exception ignored) {}
    }
}