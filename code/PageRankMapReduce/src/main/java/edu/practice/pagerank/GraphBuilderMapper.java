package edu.practice.pagerank;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

/**
 * GraphBuilderMapper: 预处理阶段的 Mapper。
 * 从原始输入文件 (如：A B) 中提取边，并将目标节点映射到源节点。
 * Key: 源节点 ID
 * Value: 目标节点 ID
 */
public class GraphBuilderMapper extends Mapper<LongWritable, Text, Text, Text> {

    // 使用 \t 分隔符，以便处理原始数据中的空格或制表符
    private static final String SEPARATOR = "\\s+";
    private final Text outKey = new Text();
    private final Text outVal = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        if (line.isEmpty() || line.startsWith("#")) {
            return;
        }

        // 输入格式是 SourceNodeID DestinationNodeID
        String[] parts = line.split(SEPARATOR);

        if (parts.length >= 2) {
            String sourceNode = parts[0];
            String destinationNode = parts[1];

            // 输出格式: (SourceNode, DestinationNode)
            outKey.set(sourceNode);
            outVal.set(destinationNode);
            context.write(outKey, outVal);

            // 发射占位符，确保 Reducer 能看到仅出现在目标端的节点
            outKey.set(destinationNode);
            outVal.set("!");
            context.write(outKey, outVal);
        }
    }
}