package org.qcri.rheem.java.operators;

import org.qcri.rheem.basic.channels.IteratorChannel;
import org.qcri.rheem.basic.operators.IteratorSink;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.platform.lineage.ExecutionLineageNode;
import org.qcri.rheem.core.util.Tuple;
import org.qcri.rheem.java.channels.StreamChannel;
import org.qcri.rheem.java.execution.JavaExecutor;

import java.util.*;
import java.util.function.Function;

/**
 * This is execution operator implements the {@link IteratorSink}.
 */
public class JavaIteratorSink<InputType, OutputType>
        extends IteratorSink<InputType, OutputType>
        implements JavaExecutionOperator {

    public JavaIteratorSink(Class<InputType> inputTypeClass, Class<OutputType> outputTypeClass) {
        super(inputTypeClass, outputTypeClass);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public JavaIteratorSink(IteratorSink<InputType, OutputType> that) {
        super(that);
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            JavaExecutor javaExecutor,
            OptimizationContext.OperatorContext operatorContext) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        IteratorChannel.Instance input = (IteratorChannel.Instance) inputs[0];
        IteratorChannel.Instance output = (IteratorChannel.Instance) outputs[0];

        Iterator<InputType> iterator = (this.getIterator() != null)? this.getIterator(): input.provideIterator();

        Function<InputType, OutputType> function = null;
        if(this.getTransformation() != null) {
            function = javaExecutor.getCompiler().compile(this.getTransformation());
        }else if(input.provideTransformation() != null) {
            function = input.provideTransformation();
        }
        if(iterator == null){
            throw new RheemException("The Iterator no exist for operator "+this.getName());
        }
        if(function == null && this.getOutputType().equals(this.getType().getDataUnitType().getTypeClass()) ){
            throw new RheemException("No exist the transformation function for operator "+this.getName());
        }

        output.accept(iterator, function);

        return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);
    }

    @Override
    public Collection<String> getLoadProfileEstimatorConfigurationKeys() {
        return Arrays.asList("rheem.java.iteratorsink.load.prepare", "rheem.java.iteratorsink.load.main");
    }

    @Override
    public JavaIteratorSink copy() {
        return new JavaIteratorSink(this);
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        assert index <= this.getNumOutputs() || (index == 0 && this.getNumOutputs() == 0);
        return Collections.singletonList(IteratorChannel.DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        assert index <= this.getNumOutputs() || (index == 0 && this.getNumOutputs() == 0);
        return Collections.singletonList(IteratorChannel.DESCRIPTOR);
    }

}
