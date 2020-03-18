package org.qcri.rheem.giraph.operators;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.job.GiraphJob;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.counters.Limits;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.qcri.rheem.basic.channels.FileChannel;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.basic.operators.ShortestPathOperator;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimators;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.core.platform.lineage.ExecutionLineageNode;
import org.qcri.rheem.core.util.Tuple;
import org.qcri.rheem.core.util.fs.FileSystem;
import org.qcri.rheem.core.util.fs.FileSystems;
import org.qcri.rheem.giraph.algorithms.ShortestPathAlgorithm;
import org.qcri.rheem.giraph.execution.GiraphExecutor;
import org.qcri.rheem.giraph.platform.GiraphPlatform;
import org.qcri.rheem.java.channels.StreamChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

public class GiraphShortestPathOperator extends ShortestPathOperator implements GiraphExecutionOperator {

    public GiraphShortestPathOperator(ShortestPathOperator shortestPathOperator) {
        super(shortestPathOperator);
        setPathOut(null, null);
    }

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private String path_out;

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> execute(
            ChannelInstance[] inputChannelInstances,
            ChannelInstance[] outputChannelInstances,
            GiraphExecutor giraphExecutor,
            OptimizationContext.OperatorContext operatorContext) {
        assert inputChannelInstances.length == this.getNumInputs();
        assert outputChannelInstances.length == this.getNumOutputs();

        final FileChannel.Instance inputChannel = (FileChannel.Instance) inputChannelInstances[0];
        final StreamChannel.Instance outputChannel = (StreamChannel.Instance) outputChannelInstances[0];
        try {
            return this.runGiraph(inputChannel, outputChannel, giraphExecutor, operatorContext);
        } catch (IOException e) {
            throw new RheemException(String.format("Running %s failed.", this), e);
        } catch (URISyntaxException e) {
            throw new RheemException(e);
        } catch (InterruptedException e) {
            throw new RheemException(e);
        } catch (ClassNotFoundException e) {
            throw new RheemException(e);
        }
    }

    private Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> runGiraph(
            FileChannel.Instance inputFileChannelInstance,
            StreamChannel.Instance outputChannelInstance,
            GiraphExecutor giraphExecutor,
            OptimizationContext.OperatorContext operatorContext)
            throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {

        assert inputFileChannelInstance.wasProduced();
        Configuration configuration = operatorContext.getOptimizationContext().getConfiguration();
        String tempDirPath = this.getPathOut(configuration);

//        PageRankParameters.setParameter(PageRankParameters.PageRankEnum.ITERATION, this.getNumIterations());


        FileSystem fs = FileSystems.getFileSystem(tempDirPath).orElseThrow(
                () -> new RheemException(String.format("Cannot access file system of %s.", tempDirPath))
        );
        //delete the file the output if exist
        fs.delete(tempDirPath, true);

        final String inputPath = inputFileChannelInstance.getSinglePath();

        GiraphConfiguration conf = giraphExecutor.getGiraphConfiguration();
        //vertex reader
        conf.set("giraph.vertex.input.dir", inputPath);
        conf.set("mapred.job.tracker", "local");
        conf.set("mapreduce.job.counters.limit", "5242880");
        conf.set("mapreduce.job.counters.max", "5242880");
        conf.setWorkerConfiguration(1, 1, 100.0f);
        conf.set("giraph.SplitMasterWorker", "false");
        conf.set("mapreduce.output.fileoutputformat.outputdir", tempDirPath);
        conf.setComputationClass(ShortestPathAlgorithm.class);
        conf.setVertexInputFormatClass(
                ShortestPathAlgorithm.ShortestPathVertexInputFormat.class);
        conf.setWorkerContextClass(
                ShortestPathAlgorithm.ShortestPathWorkerContext.class);
        conf.setMasterComputeClass(
                ShortestPathAlgorithm.ShortestPathMasterCompute.class);
        conf.setNumComputeThreads((int)configuration.getLongProperty("rheem.giraph.numThread"));

        conf.setVertexOutputFormatClass(ShortestPathAlgorithm.ShortestPathVertexOutputFormat.class);
        Limits.init(conf);

        GiraphJob job = new GiraphJob(conf, "rheem-giraph");
        FileOutputFormat.setOutputPath(job.getInternalJob(), new Path(tempDirPath));
        job.run(true);

        final String actualInputPath = FileSystems.findActualSingleInputPath(tempDirPath);
        Stream<Tuple2<Long, Float>> stream = this.createStream(actualInputPath);

        outputChannelInstance.accept(stream);

        final ExecutionLineageNode mainExecutionLineage = new ExecutionLineageNode(operatorContext);
        mainExecutionLineage.add(LoadProfileEstimators.createFromSpecification(
                "rheem.giraph.pagerank.load.main", configuration
        ));
        mainExecutionLineage.addPredecessor(inputFileChannelInstance.getLineage());

        final ExecutionLineageNode outputExecutionLineage = new ExecutionLineageNode(operatorContext);
        outputExecutionLineage.add(LoadProfileEstimators.createFromSpecification(
                "rheem.giraph.pagerank.load.output", configuration
        ));
        outputChannelInstance.getLineage().addPredecessor(outputExecutionLineage);

        return mainExecutionLineage.collectAndMark();
    }

    @Override
    public Platform getPlatform() {
        return GiraphPlatform.getInstance();
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        return Collections.singletonList(FileChannel.HDFS_TSV_DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        return Collections.singletonList(StreamChannel.DESCRIPTOR);
    }

    public void setPathOut(String path, Configuration configuration) {
        if(path == null && configuration != null) {
            path = configuration.getStringProperty("rheem.giraph.hdfs.tempdir");
        }
        this.path_out = path;
    }

    public String getPathOut(Configuration configuration) {
        if(this.path_out == null){
            setPathOut(null, configuration);
        }
        return this.path_out;
    }

    private Stream<Tuple2<Long, Float>> createStream(String path) {
        return org.qcri.rheem.core.util.fs.FileUtils.streamLines(path).map(line -> {
            String[] part = line.split("\t");
            return new Tuple2<>(Long.parseLong(part[0]), Float.parseFloat(part[1]));
        });
    }
}
