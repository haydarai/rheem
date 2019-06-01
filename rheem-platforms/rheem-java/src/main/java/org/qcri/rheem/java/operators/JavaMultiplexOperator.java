package org.qcri.rheem.java.operators;

import org.qcri.rheem.basic.channels.IteratorChannel;
import org.qcri.rheem.basic.operators.MultiplexOperator;
import org.qcri.rheem.core.data.Tuple;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.plan.rheemplan.UnaryToManyOperator;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.platform.lineage.ExecutionLineageNode;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.types.TupleType;
import org.qcri.rheem.java.channels.CollectionChannel;
import org.qcri.rheem.java.channels.JavaChannelInstance;
import org.qcri.rheem.java.channels.StreamChannel;
import org.qcri.rheem.java.execution.JavaExecutor;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * Created by bertty on 09-11-17.
 */
public class JavaMultiplexOperator<InputType extends Tuple, OutputType extends Tuple>
        extends MultiplexOperator<InputType, OutputType>
        implements JavaExecutionOperator  {

    public JavaMultiplexOperator(DataSetType<InputType> inputType, TupleType<OutputType> outputType, boolean isSupportingBroadcastInputs) {
        super(inputType, outputType, isSupportingBroadcastInputs);
    }

    public JavaMultiplexOperator(UnaryToManyOperator<InputType, OutputType> that) {
        super(that);
    }

    @Override
    public org.qcri.rheem.core.util.Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>>
           evaluate(
               ChannelInstance[] inputs,
               ChannelInstance[] outputs,
               JavaExecutor javaExecutor,
               OptimizationContext.OperatorContext operatorContext) {
        final CollectionChannel.Instance input  = (CollectionChannel.Instance) inputs[0];

        final int size_output = outputs.length;
        if(size_output != this.getNumOutputs()){
            //TODO: build exception
            return null;
        }

        for(int i = 0; i < size_output; i++){
            IteratorChannel.Instance outStream = (IteratorChannel.Instance) outputs[i];
            outStream.accept(input.provideCollection().iterator(), converts[i]);
        }

        return ExecutionOperator.modelEagerExecution(inputs, outputs, operatorContext);
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        return Collections.singletonList(CollectionChannel.DESCRIPTOR);

    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        return Collections.singletonList(IteratorChannel.DESCRIPTOR);
    }
}
