package edu.practice.pagerank;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.edge.LongNullArrayEdges;
import org.apache.giraph.io.formats.GiraphFileInputFormat;
import org.apache.giraph.io.formats.IdWithValueTextOutputFormat;
import org.apache.giraph.io.formats.LongLongNullTextInputFormat;
import org.apache.giraph.job.GiraphJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PageRankDriver implements Tool {

    private Configuration conf;

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("用法: PageRankDriver <input> <output> [maxIter] [damping] [threshold]");
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

        // 2. 将参数回写到 Conf
        getConf().setInt(PageRankMasterCompute.CONF_MAX_ITER, maxIter);
        getConf().setDouble(PageRankMasterCompute.CONF_DAMPING, damping);
        getConf().setDouble(PageRankMasterCompute.CONF_THRESHOLD, convergenceThreshold);

        // 3. 传递时间统计配置
        getConf().setLong(PageRankMasterCompute.CONF_JOB_START_TIME, jobStartTime);
        // 统计文件将保存在输出目录下的 _timings.csv 文件中
        getConf().set(PageRankMasterCompute.CONF_TIMING_OUTPUT_PATH, outputPath + "/_timings.csv");

        GiraphConfiguration giraphConf = new GiraphConfiguration(getConf());

        giraphConf.setComputationClass(PageRankVertex.class);
        giraphConf.setMasterComputeClass(PageRankMasterCompute.class);
        giraphConf.setVertexInputFormatClass(LongLongNullTextInputFormat.class);
        giraphConf.setVertexOutputFormatClass(IdWithValueTextOutputFormat.class);
        giraphConf.setOutEdgesClass(LongNullArrayEdges.class);

        // Worker 设置
        int minWorkers = giraphConf.getInt("giraph.minWorkers", 1);
        int maxWorkers = giraphConf.getInt("giraph.maxWorkers", 1);
        giraphConf.setInt("giraph.minWorkers", minWorkers);
        giraphConf.setInt("giraph.maxWorkers", maxWorkers);

        GiraphJob job = new GiraphJob(giraphConf, "PageRank Giraph Timing Experiment");

        GiraphFileInputFormat.addVertexInputPath(job.getConfiguration(), new Path(inputPath));
        FileOutputFormat.setOutputPath(job.getInternalJob(), new Path(outputPath));

        boolean success = job.run(true);

        long jobEndTime = System.currentTimeMillis();

        if (success) {
            System.out.println("--------------------------------------------");
            System.out.println("PageRank 计算完成.");
            System.out.println("总耗时 (Driver Wall Clock): " + (jobEndTime - jobStartTime) + " ms");
            System.out.println("详细迭代耗时文件已生成: " + outputPath + "/_timings.csv");
            System.out.println("--------------------------------------------");
            return 0;
        } else {
            System.err.println("PageRank 计算失败");
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
}