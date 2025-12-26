package edu.practice.pagerank;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;

public class PageRankVertex extends BasicComputation<LongWritable, DoubleWritable, NullWritable, DoubleWritable> {

    @Override
    public void compute(Vertex<LongWritable, DoubleWritable, NullWritable> vertex,
            Iterable<DoubleWritable> messages) throws IOException {

        long totalVertices = getTotalNumVertices();
        final double damping = getConf().getDouble(PageRankMasterCompute.CONF_DAMPING, 0.85);
        final int maxIterations = getConf().getInt(PageRankMasterCompute.CONF_MAX_ITER, 10);

        if (getSuperstep() == 0) {
            // 初始化为均匀分布 1/N，与 NetworkX 对齐
            double initialValue = 1.0 / (double) totalVertices;

            vertex.setValue(new DoubleWritable(initialValue));

            aggregate(PageRankMasterCompute.AGG_TOTAL_PR, new DoubleWritable(initialValue));

        } else {
            double sum = 0.0;
            for (DoubleWritable message : messages) {
                sum += message.get();
            }

            double danglingSum = ((DoubleWritable) getAggregatedValue(PageRankMasterCompute.AGG_DANGLING_SUM)).get();
            double danglingTerm = danglingSum / totalVertices;

            double oldPageRank = vertex.getValue().get();

            double newPageRank = (1.0 - damping) / totalVertices +
                    damping * (sum + danglingTerm);

            vertex.setValue(new DoubleWritable(newPageRank));

            double delta = Math.abs(newPageRank - oldPageRank);
            aggregate(PageRankMasterCompute.AGG_PR_DIFF_SUM, new DoubleWritable(delta));

            // 聚合每一轮的 PR 总和
            aggregate(PageRankMasterCompute.AGG_TOTAL_PR, new DoubleWritable(newPageRank));
        }

        if (getSuperstep() < maxIterations) {
            double pageRank = vertex.getValue().get();
            int outDegree = vertex.getNumEdges();

            if (outDegree > 0) {
                double contribution = pageRank / outDegree;
                sendMessageToAllEdges(vertex, new DoubleWritable(contribution));
            } else {
                aggregate(PageRankMasterCompute.AGG_DANGLING_SUM, new DoubleWritable(pageRank));
            }
        }

        vertex.voteToHalt();
    }
}