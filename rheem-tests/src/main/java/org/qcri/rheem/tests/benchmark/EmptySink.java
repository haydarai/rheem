package org.qcri.rheem.tests.benchmark;

import org.qcri.rheem.basic.channels.FileChannel;
import org.qcri.rheem.basic.channels.IteratorChannel;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.plan.rheemplan.UnarySink;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.platform.lineage.ExecutionLineageNode;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.util.Tuple;
import org.qcri.rheem.java.channels.CollectionChannel;
import org.qcri.rheem.java.channels.JavaChannelInstance;
import org.qcri.rheem.java.channels.StreamChannel;
import org.qcri.rheem.java.debug.stream.StreamRheem;
import org.qcri.rheem.java.execution.JavaExecutor;
import org.qcri.rheem.java.operators.JavaExecutionOperator;

import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class EmptySink<T> extends UnarySink<T> implements JavaExecutionOperator {

    public EmptySink(Class<T> tClass){
        this(DataSetType.createDefault(tClass));
    }

    public EmptySink(DataSetType<T> type){
        super(type);
    }


    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(ChannelInstance[] inputs, ChannelInstance[] outputs, JavaExecutor javaExecutor, OptimizationContext.OperatorContext operatorContext) {

        final CollectionChannel.Instance input  = (CollectionChannel.Instance) inputs[0];

        StreamSupport.stream(
            Spliterators.spliteratorUnknownSize(
                    input.provideCollection().iterator(),
                    Spliterator.ORDERED
            ),
            false
        ).forEach( ele -> {
            System.out.println("sniffer: "+ele);
        });
        return ExecutionOperator.modelEagerExecution(inputs, outputs, operatorContext);
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        assert index <= this.getNumInputs() || (index == 0 && this.getNumInputs() == 0);
        return Arrays.asList(CollectionChannel.DESCRIPTOR, StreamChannel.DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        assert index <= this.getNumInputs() || (index == 0 && this.getNumInputs() == 0);
        return Collections.singletonList(FileChannel.HDFS_OBJECT_FILE_DESCRIPTOR);
    }
}
