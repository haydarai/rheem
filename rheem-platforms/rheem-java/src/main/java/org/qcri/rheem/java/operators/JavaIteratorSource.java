package org.qcri.rheem.java.operators;

import org.qcri.rheem.basic.channels.IteratorChannel;
import org.qcri.rheem.basic.operators.IteratorSource;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.platform.lineage.ExecutionLineageNode;
import org.qcri.rheem.core.util.Tuple;
import org.qcri.rheem.java.channels.StreamChannel;
import org.qcri.rheem.java.execution.JavaExecutor;
import sun.awt.windows.ThemeReader;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * This is execution operator implements the {@link org.qcri.rheem.basic.operators.IteratorSource}.
 */
public class JavaIteratorSource<InputType, OutputType>
        extends IteratorSource<InputType, OutputType>
        implements JavaExecutionOperator{

    public JavaIteratorSource(Class<InputType> inputTypeClass, Class<OutputType> outputTypeClass) {
        super(inputTypeClass, outputTypeClass);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public JavaIteratorSource(IteratorSource<InputType, OutputType> that) {
        super(that);
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            JavaExecutor javaExecutor,
            OptimizationContext.OperatorContext operatorContext) {

        System.err.println("hebra: "+ Thread.currentThread().getName()+"tamaño input: "+inputs.length);
        System.err.println("hebra: "+ Thread.currentThread().getName()+"tamaño output: "+outputs.length);
        IteratorChannel.Instance input = null;
        if(inputs.length > 0) {
            input = (IteratorChannel.Instance) inputs[0];
        }
        StreamChannel.Instance output = (StreamChannel.Instance) outputs[0];

        Iterator<InputType> sourceIterator = (this.getIterator() != null)? this.getIterator(): input.provideIterator();

        Function<InputType, OutputType> function = null;
        if(this.getTransformation() != null) {
            function = javaExecutor.getCompiler().compile(this.getTransformation());
        }else if(input.provideTransformation() != null) {
            function = input.provideTransformation();
        }

        if(sourceIterator == null){
            throw new RheemException("The Iterator no exist for operator "+this.getName());
        }

        if(function == null && this.getInputType().equals(this.getType().getDataUnitType().getTypeClass()) ){
            throw new RheemException("No exist the transformation function for operator "+this.getName());
        }

        Stream outputStream = null;

        Stream<InputType> generateStream = StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(
                        sourceIterator,
                        Spliterator.DISTINCT
                ),
                false);

        if( function != null ){
            outputStream = generateStream.map(function);
        }else{
            outputStream = generateStream;
        }

        output.accept(outputStream);

        return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);
    }

    @Override
    public Collection<String> getLoadProfileEstimatorConfigurationKeys() {
        // TODO: CREATE ESTIMATOR
        return Arrays.asList("rheem.java.iteratorsource.load.template", "rheem.java.iteratorsource.load");
    }

    @Override
    public JavaIteratorSource copy() {
        return new JavaIteratorSource(this);
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        assert index <= this.getNumOutputs() || (index == 0 && this.getNumOutputs() == 0);
        return Collections.singletonList(IteratorChannel.DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        assert index <= this.getNumOutputs() || (index == 0 && this.getNumOutputs() == 0);
        return Collections.singletonList(StreamChannel.DESCRIPTOR);
    }


}
