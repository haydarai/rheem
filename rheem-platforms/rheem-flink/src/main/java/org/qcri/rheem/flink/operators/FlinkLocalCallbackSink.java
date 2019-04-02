package org.qcri.rheem.flink.operators;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.SerializedListAccumulator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.Utils;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.apache.flink.api.java.io.PrintingOutputFormat;
import org.apache.flink.api.java.operators.DataSink;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.AbstractID;
import org.qcri.rheem.basic.operators.LocalCallbackSink;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.function.ConsumerDescriptor;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.platform.lineage.ExecutionLineageNode;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.util.Tuple;
import org.qcri.rheem.flink.channels.DataSetChannel;
import org.qcri.rheem.flink.compiler.RheemFileOutputFormat;
import org.qcri.rheem.flink.execution.FlinkExecutor;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Random;

/**
 * Implementation of the {@link LocalCallbackSink} operator for the Flink platform.
 */
public class FlinkLocalCallbackSink <Type extends Serializable> extends LocalCallbackSink<Type> implements FlinkExecutionOperator {

    /**
     * Creates a new instance.
     *
     * @param callback callback that is executed locally for each incoming data unit
     * @param type     type of the incoming elements
     */
    public FlinkLocalCallbackSink(ConsumerDescriptor.SerializableConsumer<Type> callback, DataSetType type) {
        super(callback, type);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public FlinkLocalCallbackSink(LocalCallbackSink<Type> that) {
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

        final DataSetChannel.Instance input = (DataSetChannel.Instance) inputs[0];
        final DataSet<Type> inputDataSet = input.provideDataSet();
        try {
            if (this.collector != null) {
                System.out.println("antes de ejecutar");
                String path = this.generateTempPath(flinkExecutor.getConfiguration());
                //TODO :: poner aqui porque nose puede ejecutar el localcallbacksink en flink
                final DataSink<Type> tDataSink = input.<Type>provideDataSet()
                        .write(new RheemFileOutputFormat<Type>(path), path, FileSystem.WriteMode.OVERWRITE)
                        .setParallelism(flinkExecutor.getNumDefaultPartitions());



                /*final String id = new AbstractID().toString();
                final TypeSerializer<Type> serializer = inputDataSet.getType().createSerializer(inputDataSet.getExecutionEnvironment().getConfig());

                inputDataSet.output(new Utils.CollectHelper<>(id, serializer)).name("collectRheem()");
                System.out.println("aca ejecutando flink");
                JobExecutionResult res = inputDataSet.getExecutionEnvironment().execute();
                System.out.println("we have the answer??");

                ArrayList<byte[]> accResult = res.getAccumulatorResult(id);
                if (accResult != null) {
                    try {
                        System.out.println("peliando en la serializaion");
                        this.collector.addAll( SerializedListAccumulator.deserializeList(accResult, serializer) );
                        System.out.println("ALl in memory");
                    } catch (ClassNotFoundException e) {
                        throw new RuntimeException("Cannot find type class of collected data type.", e);
                    } catch (IOException e) {
                        throw new RuntimeException("Serialization error while deserializing collected data", e);
                    }
                } else {
                    throw new RuntimeException("The call to collect() could not retrieve the DataSet.");
                }*/

                //this.collector.addAll(inputDataSet.collect());
                System.out.println("despues de ejecutar");
            } else {
                inputDataSet.output(new PrintingOutputFormat<Type>());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return ExecutionOperator.modelEagerExecution(inputs, outputs, operatorContext);
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new FlinkLocalCallbackSink<Type>(this);
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "rheem.flink.localcallbacksink.load";
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        assert index <= this.getNumInputs() || (index == 0 && this.getNumInputs() == 0);
        return Arrays.asList(DataSetChannel.DESCRIPTOR, DataSetChannel.DESCRIPTOR_MANY);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        throw new UnsupportedOperationException(String.format("%s does not have output channels.", this));
    }

    @Override
    public boolean containsAction() {
        return true;
    }

    String generateTempPath(Configuration configuration) {
        final String tempDir = configuration.getStringProperty("rheem.basic.tempdir");
        Random random = new Random();
        return String.format("%s/%04x-%04x-%04x-%04x.tmp", tempDir,
                random.nextInt() & 0xFFFF,
                random.nextInt() & 0xFFFF,
                random.nextInt() & 0xFFFF,
                random.nextInt() & 0xFFFF
        );
    }

}
