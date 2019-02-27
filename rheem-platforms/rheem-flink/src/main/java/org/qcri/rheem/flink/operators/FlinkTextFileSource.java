package org.qcri.rheem.flink.operators;

import org.apache.flink.api.java.DataSet;
import org.qcri.rheem.basic.operators.TextFileSource;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimators;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.platform.lineage.ExecutionLineageNode;
import org.qcri.rheem.core.util.Tuple;
import org.qcri.rheem.flink.channels.DataSetChannel;
import org.qcri.rheem.flink.execution.FlinkExecutor;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Spliterator;

/**
 * Provides a {@link Collection} to a Flink job.
 */
public class FlinkTextFileSource extends TextFileSource implements FlinkExecutionOperator {

    public FlinkTextFileSource(String inputUrl, String encoding) {
        super(inputUrl, encoding);
    }

    public FlinkTextFileSource(String inputUrl) {
        super(inputUrl);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public FlinkTextFileSource(TextFileSource that) {
        super(that);
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            FlinkExecutor flinkExecutor,
            OptimizationContext.OperatorContext operatorContext) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        DataSetChannel.Instance output = (DataSetChannel.Instance) outputs[0];
        flinkExecutor.fee.setParallelism(flinkExecutor.getNumDefaultPartitions());
        String file = this.getInputUrl();
        DataSet<String> dataSet = null;
        if(this.getInputUrl().startsWith("file://")){
            try {
                dataSet = flinkExecutor.fee.fromCollection(
                    Files.lines(Paths.get(new URI(file))).iterator(),
                    String.class
                ).rebalance().setParallelism(flinkExecutor.getNumDefaultPartitions());
            }catch (Exception e){
                throw new RheemException(e);
            }
            //dataSet = flinkExecutor.fee.readTextFile(file).setParallelism(flinkExecutor.getNumDefaultPartitions());
        }else {
            dataSet = flinkExecutor.fee.readTextFile(file).setParallelism(flinkExecutor.getNumDefaultPartitions());
        }


        output.accept(dataSet, flinkExecutor);

        ExecutionLineageNode prepareLineageNode = new ExecutionLineageNode(operatorContext);
        prepareLineageNode.add(LoadProfileEstimators.createFromSpecification(
                "rheem.flink.textfilesource.load.prepare", flinkExecutor.getConfiguration()
        ));
        ExecutionLineageNode mainLineageNode = new ExecutionLineageNode(operatorContext);
        mainLineageNode.add(LoadProfileEstimators.createFromSpecification(
                "rheem.flink.textfilesource.load.main", flinkExecutor.getConfiguration()
        ));
        output.getLineage().addPredecessor(mainLineageNode);

        return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new FlinkTextFileSource(this.getInputUrl(), this.getEncoding());
    }

    @Override
    public Collection<String> getLoadProfileEstimatorConfigurationKeys() {
        return Arrays.asList("rheem.flink.textfilesource.load.prepare", "rheem.flink.textfilesource.load.main");
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        throw new UnsupportedOperationException(String.format("%s does not have input channels.", this));
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        return Arrays.asList(DataSetChannel.DESCRIPTOR, DataSetChannel.DESCRIPTOR_MANY);
    }

    @Override
    public boolean containsAction() {
        return false;
    }


}
