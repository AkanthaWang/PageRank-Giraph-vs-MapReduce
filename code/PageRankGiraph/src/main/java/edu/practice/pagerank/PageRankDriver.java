package edu.practice.pagerank;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.edge.LongNullHashSetEdges;
import org.apache.giraph.io.formats.GiraphFileInputFormat;
import org.apache.giraph.io.formats.IdWithValueTextOutputFormat;
import org.apache.giraph.io.formats.LongLongNullTextInputFormat;
import org.apache.giraph.job.GiraphJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

public class PageRankDriver implements Tool {

    private Configuration conf;

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.printf("用法: PageRankDriver <input> <output> [maxIter] [damping] [threshold] [minIter] [minWorkers] [maxWorkers]\n");
            return 1;
        }

        // 1. 捕获作业启动时间（用于计算 Setup Time）
        long jobStartTime = System.currentTimeMillis();

        String inputPath = args[0];
        String outputPath = args[1];

        // 处理参数
        int defaultMaxIter = getConf().getInt(PageRankMasterCompute.CONF_MAX_ITER, 10);
        int maxIter = args.length > 2 ? Integer.parseInt(args[2]) : defaultMaxIter;

        double defaultDamping = getConf().getDouble(PageRankMasterCompute.CONF_DAMPING, 0.85);
        double damping = args.length > 3 ? Double.parseDouble(args[3]) : defaultDamping;

        double defaultThreshold = getConf().getDouble(PageRankMasterCompute.CONF_THRESHOLD, 1e-6);
        double convergenceThreshold = args.length > 4 ? Double.parseDouble(args[4]) : defaultThreshold;

        int defaultMinIter = getConf().getInt(PageRankMasterCompute.CONF_MIN_ITER, 5);
        int minIter = args.length > 5 ? Integer.parseInt(args[5]) : defaultMinIter;

        int minWorkersArg = args.length > 6 ? Integer.parseInt(args[6]) : 1;
        int maxWorkersArg = args.length > 7 ? Integer.parseInt(args[7]) : minWorkersArg;
        if (maxWorkersArg < minWorkersArg) {
            maxWorkersArg = minWorkersArg;
        }

        // 2. 将参数回写到 Conf
        getConf().setInt(PageRankMasterCompute.CONF_MAX_ITER, maxIter);
        getConf().setDouble(PageRankMasterCompute.CONF_DAMPING, damping);
        getConf().setDouble(PageRankMasterCompute.CONF_THRESHOLD, convergenceThreshold);
        getConf().setInt(PageRankMasterCompute.CONF_MIN_ITER, minIter);

        // 3. 传递时间统计配置
        getConf().setLong(PageRankMasterCompute.CONF_JOB_START_TIME, jobStartTime);
        // 统计文件将保存在输出目录下的 _timings.csv 文件中
        getConf().set(PageRankMasterCompute.CONF_TIMING_OUTPUT_PATH, outputPath + "/_timings.csv");

        GiraphConfiguration giraphConf = new GiraphConfiguration(getConf());

        giraphConf.setComputationClass(PageRankVertex.class);
        giraphConf.setMasterComputeClass(PageRankMasterCompute.class);
        giraphConf.setVertexInputFormatClass(LongLongNullTextInputFormat.class);
        giraphConf.setVertexOutputFormatClass(IdWithValueTextOutputFormat.class);
        // 使用 HashSet 去重出边，避免重复边导致贡献重复，语义与 NetworkX 一致
        giraphConf.setOutEdgesClass(LongNullHashSetEdges.class);

        // Worker 设置
        giraphConf.setInt("giraph.minWorkers", minWorkersArg);
        giraphConf.setInt("giraph.maxWorkers", maxWorkersArg);

        GiraphJob job = new GiraphJob(giraphConf, "PageRank Giraph Timing Experiment");

        GiraphFileInputFormat.addVertexInputPath(job.getConfiguration(), new Path(inputPath));
        FileOutputFormat.setOutputPath(job.getInternalJob(), new Path(outputPath));

        boolean success = job.run(true);

        long jobEndTime = System.currentTimeMillis();

        if (success) {
            System.out.printf("--------------------------------------------\n");
            System.out.printf("PageRank 计算完成.\n");
            System.out.printf("总耗时 (Driver Wall Clock): " + (jobEndTime - jobStartTime) + " ms\n");
            System.out.printf("详细迭代耗时文件已生成: " + outputPath + "/_timings.csv\n");
            System.out.printf("--------------------------------------------\n");

            // 生成 Top50 文件
            try {
                Path outputDir = new Path(outputPath);
                Path topFile = new Path(outputDir, "pagerankTop_50.txt");
                writeTopKFromDir(outputDir, topFile, 50);
                // System.out.printf("Top50 文件已生成: " + topFile + "\n");
            } catch (Exception e) {
                // System.err.printf("警告: 生成 Top50 失败: " + e.getMessage()+"\n");
            }
            return 0;
        } else {
            // System.err.printf("PageRank 计算失败\n");
            return 1;
        }
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

    // 将输出目录中的所有 part* 文件扫描并写出 TopK
    private void writeTopKFromDir(Path sourceDir, Path dstFile, int k) throws Exception {
        FileSystem fs = sourceDir.getFileSystem(getConf());
        if (!fs.exists(sourceDir)) return;

        PriorityQueue<NodeScore> pq = new PriorityQueue<>(k, Comparator.comparingDouble(a -> a.score));

        FileStatus[] statuses = fs.listStatus(sourceDir);
        for (FileStatus status : statuses) {
            if (!status.isFile()) continue;
            String name = status.getPath().getName();
            if (!name.startsWith("part")) continue;

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(status.getPath()), java.nio.charset.StandardCharsets.UTF_8))) {
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
        }

        List<NodeScore> top = new ArrayList<>(pq);
        top.sort((a, b) -> Double.compare(b.score, a.score));

        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fs.create(dstFile, true), java.nio.charset.StandardCharsets.UTF_8))) {
            for (NodeScore ns : top) {
                writer.write(ns.nodeId + "\t" + ns.score);
                writer.newLine();
            }
        }
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