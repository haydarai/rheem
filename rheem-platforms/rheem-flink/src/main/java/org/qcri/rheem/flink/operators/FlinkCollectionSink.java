package org.qcri.rheem.flink.operators;

import org.apache.commons.lang3.Validate;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.apache.flink.api.java.io.PrintingOutputFormat;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimator;
import org.qcri.rheem.core.optimizer.cardinality.DefaultCardinalityEstimator;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.plan.rheemplan.UnaryToUnaryOperator;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.platform.lineage.ExecutionLineageNode;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.util.ReflectionUtils;
import org.qcri.rheem.core.util.Tuple;
import org.qcri.rheem.flink.channels.DataSetChannel;
import org.qcri.rheem.flink.execution.FlinkExecutor;
import org.qcri.rheem.java.channels.CollectionChannel;

import java.util.*;


/**
 * Converts {@link DataSetChannel} into a {@link CollectionChannel}
 */
public class FlinkCollectionSink<Type> extends UnaryToUnaryOperator<Type, Type>
        implements FlinkExecutionOperator  {
    public FlinkCollectionSink(DataSetType<Type> type) {
        super(type, type, false);
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(ChannelInstance[] inputs, ChannelInstance[] outputs, FlinkExecutor flinkExecutor, OptimizationContext.OperatorContext operatorContext) throws Exception {

        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        final DataSetChannel.Instance input  = (DataSetChannel.Instance) inputs[0];
        final CollectionChannel.Instance output = (CollectionChannel.Instance) outputs[0];

        final DataSet<Type> dataSetInput = input.provideDataSet();

//        List<Type> collection = new ArrayList<Type>();
//        dataSetInput.output(new LocalCollectionOutputFormat<Type>(collection));
//        output.accept(collection);

        output.accept(dataSetInput.filter(a -> true).setParallelism(1).collect());

//        final List<Type> collectedRdd = (List<Type>) dataSetInput.collect();
//        output.accept(collectedRdd);
        //FlinkExecutor.get.execute();
            //this.collector.add();
            //this.collection.addAll(dataSetInput.filter(a -> true).setParallelism(1).collect());
            //final String path = "/root/anis/data/lala";
            //final String path = "/home/migiwara/tmp";

            //inputDataSet.write(new RheemFileOutputFormat<Type>(path), path, FileSystem.WriteMode.OVERWRITE);


            //inputDataSet.filter(a -> true).setParallelism(1).output(new LocalCollectionOutputFormat<Type>(this.collector));

        return ExecutionOperator.modelEagerExecution(inputs, outputs, operatorContext);

    }

    @Override
    public boolean containsAction() {
        return true;
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        return Collections.singletonList(DataSetChannel.DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        return Collections.singletonList(CollectionChannel.DESCRIPTOR);
    }

    @Override
    public Optional<CardinalityEstimator> createCardinalityEstimator(
            final int outputIndex,
            final Configuration configuration) {
        Validate.inclusiveBetween(0, 0, outputIndex);
        return Optional.of(new DefaultCardinalityEstimator(1d, 1, this.isSupportingBroadcastInputs(),
                inputCards -> inputCards[0]));
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "rheem.flink.collect.load";
    }
}
