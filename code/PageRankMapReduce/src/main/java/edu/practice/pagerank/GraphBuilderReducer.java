package edu.practice.pagerank;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.LinkedHashSet;

/**
 * GraphBuilderReducer: 预处理阶段的 Reducer。
 * 聚合所有目标节点，输出初始 PageRank 格式，并统计总节点数 N。
 */
public class GraphBuilderReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // 使用 LinkedHashSet 存储目标节点，按输入顺序保留且去重
        LinkedHashSet<String> targets = new LinkedHashSet<>();
        boolean hasValue = false;

        for (Text val : values) {
            String node = val.toString().trim();
            if (!node.isEmpty()) {
                hasValue = true;
                if (!"!".equals(node)) {
                    targets.add(node);
                }
            }
        }

        if (hasValue) {
            // 统计总节点数 N
            context.getCounter(PageRankDriver.PageRankCounter.TOTAL_NODES_COUNT).increment(1);

            // 初始 PR 默认 1.0
            String initialPR = "1.0";
            String links = String.join(",", targets);
            context.write(key, new Text(initialPR + "|" + links));
        }
    }
}