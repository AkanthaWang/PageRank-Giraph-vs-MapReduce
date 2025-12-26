package edu.practice.pagerank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Counter;
import java.io.IOException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.PriorityQueue;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

/**
 * PageRankMapReduce 主驱动类。
 */
public class PageRankDriver implements Tool {
    // 用于传递总节点数 N 的配置键
    public static final String TOTAL_NODES_KEY = "pagerank.total.nodes";
    // 用于传递悬挂节点 PageRank 总和的配置键
    public static final String DANGLING_PR_SUM_KEY = "pagerank.dangling.sum";
    // 放大倍数（提高精度，减小被四舍五入为 0 的概率）
    public static final long SCALE_FACTOR_LONG = 1_000_000_000_000L; // 1e12
    // 用于检测收敛的阈值（初始化）
    public static final double CONVERGENCE_THRESHOLD = 1.0e-6;
    // 悬挂节点 PageRank 求和计数器组名和计数器名
    public static enum PageRankCounter {
        DANGLING_PR_SUM,
        TOTAL_NODES_COUNT,
        PR_DIFF_SUM,
        MAP_WALL_MS,
        REDUCE_WALL_MS
    }

    private Configuration conf;
    // 性能监视器
    private PerformanceMonitor perfMonitor;

    @Override
    public int run(String[] args) throws Exception {
        perfMonitor = new PerformanceMonitor();
        
        // 获取并验证输入参数
        if (args.length < 2) {
            System.err.println("用法: PageRankDriver <原始输入路径> <最终输出目录> [最大迭代次数] [阻尼系数] [收敛阈值]");
            System.err.println("示例: PageRankDriver /input/raw_graph /output 20 0.85 1e-6");
            return 1;
        }

        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        int maxIterations = (args.length >= 3) ? Integer.parseInt(args[2]) : 10;
        double dampingFactor = (args.length >= 4) ? Double.parseDouble(args[3]) : 0.85;
        double convergenceThreshold = (args.length >= 5) ? Double.parseDouble(args[4]) : CONVERGENCE_THRESHOLD;
        int minIterations = (args.length >= 6) ? Integer.parseInt(args[5]) : 5; // 最少迭代次数，避免首轮早停
        System.out.println("配置参数 - 迭代次数: " + maxIterations + ", 阻尼系数: " + dampingFactor + ", 收敛阈值: " + convergenceThreshold + ", 最少迭代: " + minIterations);

        // 文件系统实例与输入路径存在性校验
        FileSystem fs = FileSystem.get(getConf());
        if (!fs.exists(inputPath)) {
            System.err.println("错误: 原始输入路径不存在: " + inputPath);
            return 1;
        }

        // --- Step 1: 预处理和图结构初始化 ---
        // 使用与迭代一致的命名规则：将预处理输出放在 output/iteration_0
        Path graphInput = new Path(outputPath, "iteration_0");

        // 预处理操作
        long preprocessStart = System.currentTimeMillis();
        long totalNodes = runPreprocessJob(inputPath, graphInput);
        long preprocessEnd = System.currentTimeMillis();
        
        System.out.printf("预处理耗时: %s 秒\n", formatSeconds(preprocessEnd - preprocessStart));
        perfMonitor.record(PerformanceMonitor.PREPROCESS, preprocessEnd - preprocessStart, "Graph structure initialization");
        
        if (totalNodes <= 0) {
            System.err.println("错误: 无法在预处理阶段计算出总节点数 N。");
            return 3;
        }

        // 将总节点数 N 存入配置，供所有迭代使用
        getConf().setLong(TOTAL_NODES_KEY, totalNodes);
        System.out.println("成功初始化，总节点数 N = " + totalNodes);

        // 将初始 PR 归一化为 1/N（生成 iteration_0_uniform）
        Path normalizedGraphInput = normalizeInitialPRToUniform(graphInput, totalNodes);
        if (normalizedGraphInput != null) {
            graphInput = normalizedGraphInput;
            System.out.println("已将初始 PR 归一化为 1/N，使用目录: " + graphInput);
        } else {
            System.err.println("警告: 初始 PR 归一化失败，继续使用原始 iteration_0（初值为 1.0）");
        }

        // --- Step 2: 运行 PageRank 迭代 ---
        // 计算第0轮（归一化后）中的悬挂质量总和，确保第1轮迭代与 NetworkX 对齐
        double initialDanglingSum = computeInitialDanglingSum(graphInput);
        getConf().setDouble(DANGLING_PR_SUM_KEY, initialDanglingSum);
        System.out.printf("初始化悬挂质量 DanglingSum(迭代前): %.15f\n", initialDanglingSum);

        long iterStartTime = System.currentTimeMillis();
        int finalIteration = maxIterations;
        boolean converged = false;

        for (int i = 0; i < maxIterations; i++) {
            Path currentInput = (i == 0) ? graphInput : new Path(outputPath, "iteration_" + i);
            Path currentOutput = new Path(outputPath, "iteration_" + (i + 1));

            System.out.println("\n[DEBUG] 迭代 " + (i+1) + ": 输入=" + currentInput);

            // 自动清理输出目录
            if (currentOutput.getFileSystem(getConf()).exists(currentOutput)) {
                currentOutput.getFileSystem(getConf()).delete(currentOutput, true);
            }


            System.out.println(">>> 正在运行 PageRank 迭代: " + (i + 1) + "/" + maxIterations);

            // 1. 设置 Job
            long iterJobStart = System.currentTimeMillis();
            long jobSetupStart = System.currentTimeMillis();
            Job job = Job.getInstance(getConf(), "PageRank Iteration " + (i + 1));
            job.setJarByClass(PageRankDriver.class);

            // 2. Mapper 和 Reducer 配置
            job.setMapperClass(PageRankMapper.class);
            job.setReducerClass(PageRankReducer.class);

            // 3. 设置输入输出类型
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            // 4. 设置配置参数
            Configuration jobConf = job.getConfiguration();
            jobConf.setDouble("DAMPING_FACTOR", dampingFactor);

            // 5. 设置输入输出路径
            // 在提交 Job 前确保输入路径存在
            if (!currentInput.getFileSystem(getConf()).exists(currentInput)) {
                System.err.println("错误: 迭代输入路径不存在: " + currentInput + "，停止于第 " + (i+1) + " 次迭代。");
                return 4;
            }
            FileInputFormat.addInputPath(job, currentInput);
            FileOutputFormat.setOutputPath(job, currentOutput);


            long jobSetupEnd = System.currentTimeMillis();
            long setupTime = jobSetupEnd - jobSetupStart;

            // 6. 运行 Job
            long computeStart = System.currentTimeMillis();
            boolean jobSuccess = job.waitForCompletion(true);
            long computeEnd = System.currentTimeMillis();
            long computeTime = computeEnd - computeStart;

            if (!jobSuccess) {
                System.err.println("PageRank 迭代失败，停止于第 " + (i + 1) + " 次。");
                return 2;
            }
            // 清理本轮输入目录以节省存储（确保结果已成功写入 currentOutput）
            try {
                FileSystem cleanupFs = currentInput.getFileSystem(getConf());
                if (cleanupFs.exists(currentInput)) {
                    cleanupFs.delete(currentInput, true);
                }
            } catch (IOException e) {
                System.err.println("警告: 无法删除迭代输入目录 " + currentInput + ": " + e.getMessage());
            }
            
            long iterJobEnd = System.currentTimeMillis();
            long totalIterationTime = iterJobEnd - iterJobStart;
            System.out.printf("   > 第 %d 次迭代 Job 耗时: %s 秒\n", (i + 1), formatSeconds(totalIterationTime));

            // 7. 更新计数器
            Counters counters = job.getCounters();
            
            // 累积悬挂节点 PR 总和
            long scaledSum = counters.findCounter(PageRankCounter.DANGLING_PR_SUM).getValue();
            double currentIterationDanglingSum = (double) scaledSum / SCALE_FACTOR_LONG;
            
            // 清零计数器，并将其 PR 总和存入全局配置，供下一轮 Reducer 使用
            getConf().setDouble(DANGLING_PR_SUM_KEY, currentIterationDanglingSum);
            
            System.out.printf("   > 第 %d 次迭代悬挂节点 PR 总和: %.15f\n", (i+1), currentIterationDanglingSum);
            
            // 重置计数器（在下一次迭代前）
            if (i < maxIterations - 1) {
                // Hadoop 会在新的 Job 实例中自动重置计数器，所以这里不需要显式操作
            }
            // 检查收敛
            long scaledDiffSum = counters.findCounter(PageRankCounter.PR_DIFF_SUM).getValue();
            double avgDiff = (double) scaledDiffSum / SCALE_FACTOR_LONG / (double) totalNodes;
            System.out.printf("   > 第 %d 次迭代平均 PR 变化: %.12e\n", (i+1), avgDiff);
            if ((i + 1) >= minIterations && avgDiff <= convergenceThreshold) {
                converged = true;
                finalIteration = i + 1;
                System.out.println("   > 达到收敛阈值，提前停止。");
                break;
            }
            // 从自定义计数器获取每轮 Map/Reduce 墙钟时间（所有任务累计）
            long mapWallMs = counters.findCounter(PageRankCounter.MAP_WALL_MS).getValue();
            long reduceWallMs = counters.findCounter(PageRankCounter.REDUCE_WALL_MS).getValue();
            // 输入迭代轮次、迭代一轮启动时间、map时间、reduce时间、迭代总时间
            perfMonitor.recordIterationDetails(i + 1, setupTime, mapWallMs, reduceWallMs, totalIterationTime,
            String.format("Dangling PR Sum: %.15f, Avg Diff: %.12e", currentIterationDanglingSum, avgDiff));
        }
        
        long iterEndTime = System.currentTimeMillis();
        long elapsedMs = iterEndTime - iterStartTime;

        int usedIteration = converged ? finalIteration : maxIterations;
        Path finalOutputPath = new Path(outputPath, "iteration_" + usedIteration);

        System.out.println("\nPageRank 计算完成. 总耗时: " + (elapsedMs / 1000.0) + " 秒");

        // 清理中间迭代
        System.out.println("\n正在清理中间输出文件...");
        long cleanupStartTime = System.currentTimeMillis();
        for (int i = 0; i < usedIteration; i++) {
            Path iterPath = new Path(outputPath, "iteration_" + i);
            try {
                if (fs.exists(iterPath)) {
                    fs.delete(iterPath, true);
                }
            } catch (IOException e) {
                System.err.println("警告: 无法删除 " + iterPath + e.getMessage());
            }
        }
        long cleanupEndTime = System.currentTimeMillis();
        perfMonitor.record("Middle Output Cleanup", cleanupEndTime - cleanupStartTime, "Clean up intermediate output");

        // 最终结果处理
        try {
            long finalizeStart = System.currentTimeMillis();
            Path cleaned = new Path(outputPath, "final_scores");
            writeFinalScores(finalOutputPath, cleaned);
            long finalizeEnd = System.currentTimeMillis();
            System.out.println("清理后的最终 PR 文件在: " + cleaned);
            perfMonitor.record(PerformanceMonitor.FINALIZE, finalizeEnd - finalizeStart, "Clean and format result");

            Path topFile = new Path(cleaned, "pagerankTop_50.txt");
            Path mergedPart = new Path(cleaned, "part-00000");
            writeTopK(mergedPart, topFile, 50);
            System.out.println("Top50 文件在: " + topFile);
        } catch (IOException e) {
            System.err.println("警告: 无法生成最终文件: " + e.getMessage());
        }

        // 保存性能报告
        try {
            perfMonitor.saveToHDFS(getConf(), outputPath, "performance_report.txt");
        } catch (IOException e) {
            System.err.println("警告: 无法保存性能报告: " + e.getMessage());
        }

        return 0;
    }

    private long runPreprocessJob(Path inputPath, Path outputPath) throws Exception {
        System.out.println("--- 正在运行预处理 Job，初始化图结构... ---");

        // 自动清理输出目录
        if (outputPath.getFileSystem(getConf()).exists(outputPath)) {
            outputPath.getFileSystem(getConf()).delete(outputPath, true);
        }


        Job job = Job.getInstance(getConf(), "PageRank Preprocess: Graph Builder");
        job.setJarByClass(PageRankDriver.class);
        // Mapper 和 Reducer 配置
        job.setMapperClass(GraphBuilderMapper.class);
        job.setReducerClass(GraphBuilderReducer.class);
        
        // 输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 输入输出路径
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        if (!job.waitForCompletion(true)) {
            throw new RuntimeException("预处理 Job 失败!");
        }
        
        // 从计数器中获取总节点数 N
        Counters counters = job.getCounters();
        Counter totalNodesCounter = counters.findCounter(PageRankCounter.TOTAL_NODES_COUNT);
        return totalNodesCounter.getValue();
    }

    @Override
    public Configuration getConf() { return conf; }

    @Override
    public void setConf(Configuration conf) { this.conf = conf; }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int exitCode = ToolRunner.run(conf, new PageRankDriver(), args);
        System.exit(exitCode);
    }

    // --- 辅助工具方法 ---

    private void writeFinalScores(Path sourceDir, Path destDir) throws IOException {
        FileSystem fs = sourceDir.getFileSystem(getConf());
        if (!fs.exists(destDir)) fs.mkdirs(destDir);

        FileStatus[] statuses = fs.listStatus(sourceDir);
        try (FSDataOutputStream out = fs.create(new Path(destDir, "part-00000"));
             BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out, java.nio.charset.StandardCharsets.UTF_8))) {

            for (FileStatus status : statuses) {
                if (!status.isFile() || !status.getPath().getName().startsWith("part")) continue;
                try (FSDataInputStream in = fs.open(status.getPath());
                     BufferedReader reader = new BufferedReader(new InputStreamReader(in, java.nio.charset.StandardCharsets.UTF_8))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        if (line.trim().isEmpty()) continue;
                        String[] parts = line.split("\t", 2);
                        if (parts.length < 2) continue;
                        // 提取 PR 值，去掉 | 后的链接部分
                        String val = parts[1];
                        int sep = val.indexOf("|");
                        String pr = (sep >= 0) ? val.substring(0, sep) : val;
                        writer.write(parts[0] + "\t" + pr);
                        writer.newLine();
                    }
                }
            }
        }
    }

    private void writeTopK(Path srcFile, Path dstFile, int k) throws IOException {
        FileSystem fs = srcFile.getFileSystem(getConf());
        if (!fs.exists(srcFile)) return;

        PriorityQueue<NodeScore> pq = new PriorityQueue<>(k, Comparator.comparingDouble(a -> a.score));

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(srcFile), java.nio.charset.StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\t", 2);
                if (parts.length < 2) continue;
                try {
                    double pr = Double.parseDouble(parts[1].trim());
                    if (pq.size() < k) {
                        pq.offer(new NodeScore(parts[0], pr));
                    } else if (pr > pq.peek().score) {
                        pq.poll();
                        pq.offer(new NodeScore(parts[0], pr));
                    }
                } catch (NumberFormatException ignored) {}
            }
        }

        List<NodeScore> top = new ArrayList<>(pq);
        top.sort((a, b) -> Double.compare(b.score, a.score));

        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fs.create(dstFile), java.nio.charset.StandardCharsets.UTF_8))) {
            for (NodeScore ns : top) {
                writer.write(ns.nodeId + "\t" + ns.score);
                writer.newLine();
            }
        }
    }

    private String formatSeconds(long elapsedMs) {
        return String.format("%.3f", elapsedMs / 1000.0);
    }

    /**
     * 将预处理输出 iteration_0 的初始 PR 值统一改为 1/N，并输出到新的目录 iteration_0_uniform。
     * 返回新的目录路径；如失败返回 null。
     */
    private Path normalizeInitialPRToUniform(Path sourceDir, long totalNodes) {
        try {
            if (totalNodes <= 0) return null;
            double uniform = 1.0 / (double) totalNodes;

            FileSystem fs = sourceDir.getFileSystem(getConf());
            if (!fs.exists(sourceDir)) return null;

            Path destDir = new Path(sourceDir.getParent(), "iteration_0_uniform");
            if (fs.exists(destDir)) fs.delete(destDir, true);
            fs.mkdirs(destDir);

            FileStatus[] statuses = fs.listStatus(sourceDir);
            for (FileStatus status : statuses) {
                if (!status.isFile() || !status.getPath().getName().startsWith("part")) continue;

                Path destFile = new Path(destDir, status.getPath().getName());
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(status.getPath()), java.nio.charset.StandardCharsets.UTF_8));
                     BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fs.create(destFile, true), java.nio.charset.StandardCharsets.UTF_8))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        if (line.trim().isEmpty()) continue;
                        String[] parts = line.split("\t", 2);
                        if (parts.length < 2) continue;

                        String val = parts[1];
                        int sep = val.indexOf("|");
                        if (sep < 0) continue; // 非法行，跳过
                        String outlinks = val.substring(sep + 1);
                        writer.write(parts[0]);
                        writer.write('\t');
                        writer.write(String.format(java.util.Locale.ROOT, "%.10f", uniform));
                        writer.write('|');
                        writer.write(outlinks);
                        writer.newLine();
                    }
                }
            }
            return destDir;
        } catch (Exception e) {
            System.err.println("规范化初始 PR 失败: " + e.getMessage());
            return null;
        }
    }

    /**
     * 读取预处理输出（iteration_0），计算初始悬挂节点的 PageRank 总和。
     * 公式中第1轮迭代需要上一轮的悬挂质量，本方法保证与 NetworkX 的处理一致。
     *
     * 格式：每行形如 NodeID\tPR|Outlink1,Outlink2,...，当 '|' 后为空表示悬挂节点。
     */
    private double computeInitialDanglingSum(Path sourceDir) throws IOException {
        FileSystem fs = sourceDir.getFileSystem(getConf());
        if (!fs.exists(sourceDir)) return 0.0;

        double sum = 0.0;
        FileStatus[] statuses = fs.listStatus(sourceDir);
        for (FileStatus status : statuses) {
            if (!status.isFile() || !status.getPath().getName().startsWith("part")) continue;
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(status.getPath()), java.nio.charset.StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (line.trim().isEmpty()) continue;
                    String[] parts = line.split("\t", 2);
                    if (parts.length < 2) continue;
                    String val = parts[1];
                    int sep = val.indexOf("|");
                    if (sep < 0) continue;
                    String prStr = val.substring(0, sep).trim();
                    String outlinks = val.substring(sep + 1).trim();
                    if (outlinks.isEmpty()) {
                        try {
                            double pr = Double.parseDouble(prStr);
                            sum += pr;
                        } catch (NumberFormatException ignored) {}
                    }
                }
            }
        }
        return sum;
    }

    private static class NodeScore {
        String nodeId;
        double score;
        NodeScore(String nodeId, double score) {
            this.nodeId = nodeId;
            this.score = score;
        }
    }
}