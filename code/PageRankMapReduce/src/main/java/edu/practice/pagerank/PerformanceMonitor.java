package edu.practice.pagerank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 性能监控工具类
*/
public class PerformanceMonitor {

    // 保留的时间类别常量
    public static final String JOB_SETUP = "Job Setup";
    public static final String MAP_PHASE = "Map Phase";
    public static final String REDUCE_PHASE = "Reduce Phase";
    public static final String TOTAL_ITERACTION_TIME = "Total Iteraction Time";
    public static final String PREPROCESS = "Preprocess";
    public static final String FINALIZE = "Finalize";

    private Map<String, Long> timings;          // 时间统计（毫秒）
    private Map<String, String> descriptions;   // 与 timings 同步的描述信息

    public PerformanceMonitor() {
        this.timings = new LinkedHashMap<>();
        this.descriptions = new LinkedHashMap<>();
    }

    // 记录单个时间点
    public void record(String key, long durationMs, String description) {
        timings.put(key, durationMs);
        descriptions.put(key, description);
    }

    // 记录开始和结束时间段
    public void recordInterval(String key, long startMs, long endMs, String description) {
        record(key, endMs - startMs, description);
    }

    // 记录迭代的详细时间数据
    public void recordIterationDetails(int iteration, long setupTime, long mapTime,
                                       long reduceTime, long totaliteractionTime, String notes) {
        String prefix = String.format("Iteration_%d_", iteration);

        record(prefix + JOB_SETUP, setupTime, "Job setup for iteration " + iteration);
        record(prefix + MAP_PHASE, mapTime, "Map phase time for iteration " + iteration);
        record(prefix + REDUCE_PHASE, reduceTime, "Reduce phase time for iteration " + iteration);
        record(prefix + TOTAL_ITERACTION_TIME, totaliteractionTime, "Job totaltime for iteration " + iteration);

        if (notes != null && !notes.isEmpty()) {
            descriptions.put(prefix + "notes", notes);
        }
    }

    // 获取总记录时间
    public long getTotalTime() {
        long total = 0;
        for (long time : timings.values()) {
            total += time;
        }
        return total;
    }

    // 在控制台打印时间摘要
    public void printSummary() {
        System.out.println("\n--- Performance Summary ---");
        for (Map.Entry<String, Long> entry : timings.entrySet()) {
            System.out.printf("%-30s: %d ms\n", entry.getKey(), entry.getValue());
        }
        System.out.println("---------------------------\n");
    }

    
    // 将时间数据保存到 HDFS 文件
    public void saveToHDFS(Configuration conf, Path outputDir, String filename) throws IOException {
        FileSystem fs = FileSystem.get(conf);

        if (!fs.exists(outputDir)) {
            fs.mkdirs(outputDir);
        }

        Path reportPath = new Path(outputDir, filename);

        try (FSDataOutputStream out = fs.create(reportPath, true);
             BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8))) {

            writeReportContent(writer);
        }

        System.out.println("✓ 性能报告已保存到 HDFS: " + reportPath);
    }

    
    
    // 将时间数据保存到本地文件 
    public void saveToLocal(String filePath) throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new java.io.FileWriter(filePath))) {
            writeReportContent(writer);
        }
        System.out.println("✓ 性能报告已保存到本地: " + filePath);
    }

    // 写入报告内容
    private void writeReportContent(BufferedWriter writer) throws IOException {
        writer.write("========================================\n");
        writer.write("PageRank Performance Report\n");
        writer.write("Generated: " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()) + "\n");
        writer.write("========================================\n\n");

        writer.write("Detailed Timing Information:\n");
        writer.write("----------------------------------------\n");

        // 直接遍历输出所有记录的时间，不再进行复杂的分类计算
        for (Map.Entry<String, Long> entry : timings.entrySet()) {
            String key = entry.getKey();
            long timeMs = entry.getValue();
            String desc = descriptions.getOrDefault(key, "");

            writer.write(String.format("%-40s: %10d ms (%.3f s) - %s\n",
                    key, timeMs, timeMs / 1000.0, desc));
        }

        long totalMs = getTotalTime();
        writer.write("----------------------------------------\n");
        writer.write(String.format("%-40s: %10d ms (%.3f s)\n", "TOTAL RECORDED TIME", totalMs, totalMs / 1000.0));
        writer.write("========================================\n");
    }
}