package edu.practice.pagerank;

import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.giraph.aggregators.DoubleSumAggregator;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PageRankMasterCompute extends DefaultMasterCompute {

    // 现有的聚合器
    public static final String AGG_PR_DIFF_SUM = "pagerank.agg.prDiffSum";
    public static final String AGG_DANGLING_SUM = "pagerank.agg.danglingSum";

    // 用于统计全图 PageRank 总和的聚合器
    public static final String AGG_TOTAL_PR = "pagerank.agg.totalPR";

    public static final String CONF_DAMPING = "pagerank.damping";
    public static final String CONF_MAX_ITER = "pagerank.maxIterations";
    public static final String CONF_MIN_ITER = "pagerank.minIterations";
    public static final String CONF_THRESHOLD = "pagerank.convergenceThreshold";
    public static final String CONF_TIMING_OUTPUT_PATH = "pagerank.timing.path";
    public static final String CONF_JOB_START_TIME = "pagerank.job.start.time";

    private List<String> timeRecords;
    private long lastSuperstepTime;

    @Override
    public void initialize() throws InstantiationException, IllegalAccessException {
        registerAggregator(AGG_PR_DIFF_SUM, DoubleSumAggregator.class);
        registerAggregator(AGG_DANGLING_SUM, DoubleSumAggregator.class);

        // 注册 Total PR 聚合器
        registerAggregator(AGG_TOTAL_PR, DoubleSumAggregator.class);

        // 初始化聚合器
        setAggregatedValue(AGG_PR_DIFF_SUM, new DoubleWritable(0.0));
        setAggregatedValue(AGG_DANGLING_SUM, new DoubleWritable(0.0));
        // 注意：TotalPR 不需要手动 set 0，DoubleSum 默认就是 0，但显式设置也没问题

        timeRecords = new ArrayList<>();
        lastSuperstepTime = System.currentTimeMillis();

        long jobStartTime = getConf().getLong(CONF_JOB_START_TIME, lastSuperstepTime);
        long setupTime = lastSuperstepTime - jobStartTime;
        timeRecords.add("Setup," + setupTime);
    }

    @Override
    public void compute() {
        long currentTime = System.currentTimeMillis();
        long duration = 0;

        // 1. 记录耗时
        if (getSuperstep() > 0) {
            duration = currentTime - lastSuperstepTime;
            timeRecords.add("Superstep_" + (getSuperstep() - 1) + "," + duration);
        }
        lastSuperstepTime = currentTime;

        int maxIter = getConf().getInt(CONF_MAX_ITER, 10);
        int minIter = getConf().getInt(CONF_MIN_ITER, 5);
        double threshold = getConf().getDouble(CONF_THRESHOLD, 1e-6);
        long totalVertices = getTotalNumVertices();

        // 获取聚合结果
        double diffSum = ((DoubleWritable) getAggregatedValue(AGG_PR_DIFF_SUM)).get();
        double danglingSum = ((DoubleWritable) getAggregatedValue(AGG_DANGLING_SUM)).get();
        double totalPR = ((DoubleWritable) getAggregatedValue(AGG_TOTAL_PR)).get();

        // 计算平均误差
        double avgDiff = diffSum / totalVertices;

        // ============================================================
        // 打印详细监控日志 (Standard Output)
        // 这些日志会出现在 YARN Container 的 stdout 中
        // ============================================================
        System.out.printf("==================================================\n");
        System.out.printf(">>> Superstep: " + getSuperstep() + " (Finished SS " + (getSuperstep() - 1) + ")"+ "\n");
        System.out.printf(">>> 耗时 (Duration): " + duration + " ms\n");
        System.out.printf(">>> 节点总数 (Total Vertices): " + totalVertices + "\n");

        // Superstep 0 只是初始化，没有 Diff，Superstep 1 才开始有 Diff
        if (getSuperstep() > 0) {
            System.out.printf(">>> 当前收敛误差 (Avg Diff): %.12f (阈值: %.12f)\n", avgDiff, threshold);
            System.out.printf(">>> 全图 PR 总和 (Total PR): %.6f (理论值应接近 %.1f)\n", totalPR, 1.0); // 假设是Sum-to-1模型
            System.out.printf(">>> 悬挂能量 (Dangling Sum): %.6f\n", danglingSum);

            if (avgDiff <= threshold) {
                System.out.printf(">>> [状态]：已收敛 (Converged)！\n");
            } else {
                System.out.printf(">>> [状态]：继续迭代...\n");
            }
        } else {
            System.out.printf(">>> [状态]：初始化完成 (Initialization Done)\n");
        }
        System.out.printf("==================================================\n");


        // 2. 收敛逻辑判断 (从第2轮开始判断第1轮的结果)
        boolean shouldHalt = false;
        if (getSuperstep() >= minIter) {
            if (avgDiff <= threshold) {
                shouldHalt = true;
            }
        }

        if (getSuperstep() >= maxIter) {
            System.out.printf("达到最大迭代次数 " + maxIter+ "\n");
            shouldHalt = true;
        }

        // 3. 停止与写入
        if (shouldHalt) {
            haltComputation();
            writeTimingsToHDFS();
        }
    }

    private void writeTimingsToHDFS() {
        String outputPath = getConf().get(CONF_TIMING_OUTPUT_PATH);
        if (outputPath == null) return;
        try {
            FileSystem fs = FileSystem.get(getConf());
            Path path = new Path(outputPath);
            FSDataOutputStream out = fs.create(path, true);
            out.writeBytes("Phase,Duration_ms\n");
            for (String record : timeRecords) {
                out.writeBytes(record + "\n");
            }
            long closingTime = System.currentTimeMillis();
            out.writeBytes("Cleanup_And_Write," + (closingTime - lastSuperstepTime) + "\n");
            out.close();
            System.out.printf("统计文件已写入: " + outputPath+ "\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}