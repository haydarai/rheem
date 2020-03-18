package org.qcri.rheem.giraph.algorithms;

import com.google.common.collect.Lists;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.examples.GeneratedVertexReader;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexReader;
import org.apache.giraph.io.formats.GeneratedVertexInputFormat;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.giraph.worker.WorkerContext;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;

public class ShortestPathAlgorithm extends BasicComputation<LongWritable, DoubleWritable, FloatWritable, DoubleWritable> {

    @Override
    public void compute(Vertex<LongWritable, DoubleWritable, FloatWritable> vertex, Iterable<DoubleWritable> messages)
            throws IOException {
        double minDist = Double.MAX_VALUE;
        for (DoubleWritable message : messages) {
            minDist = Math.min(minDist, message.get());
        }

        if (minDist < vertex.getValue().get()) {
            vertex.setValue(new DoubleWritable(minDist));
            for (Edge<LongWritable, FloatWritable> edge : vertex.getEdges()) {
                double distance = minDist + edge.getValue().get();
                sendMessage(edge.getTargetVertexId(), new DoubleWritable(distance));
            }
        }
        vertex.voteToHalt();
    }

    /**
     * Master compute associated with {@link ShortestPathAlgorithm}.
     * It registers required aggregators.
     */
    public static class ShortestPathMasterCompute extends DefaultMasterCompute {
        @Override
        public void initialize() throws InstantiationException,
                IllegalAccessException {
        }
    }

    /**
     * Worker context used with {@link PageRankAlgorithm}.
     */
    public static class ShortestPathWorkerContext extends WorkerContext {

        @Override
        public void preApplication()
                throws InstantiationException, IllegalAccessException {
        }

        @Override
        public void postApplication() {
        }

        @Override
        public void preSuperstep() {
        }

        @Override
        public void postSuperstep() {
        }
    }

    /**
     * Simple VertexInputFormat that supports {@link ShortestPathAlgorithm}
     */
    public static class ShortestPathVertexInputFormat extends
            GeneratedVertexInputFormat<LongWritable, DoubleWritable, FloatWritable> {
        @Override
        public VertexReader<LongWritable, DoubleWritable,
                FloatWritable> createVertexReader(InputSplit split,
                                                  TaskAttemptContext context)
                throws IOException {
            return new ShortestPathVertexReader();
        }
    }

    /**
     * Simple VertexReader that supports {@link ShortestPathAlgorithm}
     */
    public static class ShortestPathVertexReader extends GeneratedVertexReader<LongWritable, DoubleWritable, FloatWritable> {
        /**
         * Class logger
         */
        private static final Logger LOG =
                Logger.getLogger(ShortestPathVertexReader.class);

        @Override
        public boolean nextVertex() {
            return totalRecords >= recordsRead;
        }

        @Override
        public Vertex<LongWritable, DoubleWritable, FloatWritable>
        getCurrentVertex() throws IOException {
            Vertex<LongWritable, DoubleWritable, FloatWritable> vertex =
                    getConf().createVertex();
            LongWritable vertexId = new LongWritable(
                    (inputSplit.getSplitIndex() * totalRecords) + recordsRead);
            DoubleWritable vertexValue = new DoubleWritable(vertexId.get() * 10d);
            long targetVertexId =
                    (vertexId.get() + 1) %
                            (inputSplit.getNumSplits() * totalRecords);
            float edgeValue = vertexId.get() * 100f;
            List<Edge<LongWritable, FloatWritable>> edges = Lists.newLinkedList();
            edges.add(EdgeFactory.create(new LongWritable(targetVertexId),
                    new FloatWritable(edgeValue)));
            vertex.initialize(vertexId, vertexValue, edges);
            ++recordsRead;
            if (LOG.isInfoEnabled()) {
                LOG.info("next: Return vertexId=" + vertex.getId().get() +
                        ", vertexValue=" + vertex.getValue() +
                        ", targetVertexId=" + targetVertexId + ", edgeValue=" + edgeValue);
            }
            return vertex;
        }

    }

    /**
     * Simple VertexOutputFormat that supports {@link PageRankAlgorithm}
     */
    public static class ShortestPathVertexOutputFormat extends
            TextVertexOutputFormat<LongWritable, DoubleWritable, FloatWritable> {
        @Override
        public TextVertexWriter createVertexWriter(TaskAttemptContext context) {
            return new ShortestPathVertexWriter();
        }

        /**
         * Simple VertexWriter that supports {@link ShortestPathAlgorithm}
         */
        public class ShortestPathVertexWriter extends TextVertexWriter {
            @Override
            public void writeVertex(
                    Vertex<LongWritable, DoubleWritable, FloatWritable> vertex)
                    throws IOException, InterruptedException {
                getRecordWriter().write(
                        new Text(vertex.getId().toString()),
                        new Text(vertex.getValue().toString()));
            }
        }
    }
}
