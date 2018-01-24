package org.qcri.rheem.java.operators;

import org.qcri.rheem.basic.collection.MultiplexCollection;
import org.qcri.rheem.basic.collection.MultiplexList;
import org.qcri.rheem.basic.operators.SnifferOperator;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.plan.rheemplan.SnifferBase;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.platform.lineage.ExecutionLineageNode;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.util.Tuple;
import org.qcri.rheem.java.channels.CollectionChannel;
import org.qcri.rheem.java.channels.StreamChannel;
import org.qcri.rheem.java.execution.JavaExecutor;

import java.util.*;
import java.util.stream.Stream;

/**
 * Created by bertty on 30-05-17.
 */
public class JavaSnifferOperator<Type, TypeTuple extends org.qcri.rheem.core.data.Tuple> extends SnifferOperator<Type, TypeTuple> implements JavaExecutionOperator{
    public JavaSnifferOperator(DataSetType<Type> inputType, DataSetType<TypeTuple> typeTuple) {
        super(inputType, typeTuple);
    }

    protected JavaSnifferOperator(DataSetType<Type> inputType, DataSetType<Type> outputType0, DataSetType<TypeTuple> outputType1, boolean isSupportingBroadcastInputs) {
        super(inputType, outputType0, outputType1, isSupportingBroadcastInputs);
    }

    public JavaSnifferOperator(SnifferBase<Type, Type, TypeTuple> that) {
        super(that);
    }


    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(ChannelInstance[] inputs, ChannelInstance[] outputs, JavaExecutor javaExecutor, OptimizationContext.OperatorContext operatorContext) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();


      /*  PredicateDescriptor predicateDescriptor = null;
        try {
            Socket s = new Socket("localhost", 9090);
            PrintWriter out = new PrintWriter(s.getOutputStream());

            FunctionDescriptor.SerializablePredicate<Type> function =
                    element -> {
                    //    out.println(element);
                        return true;
                    };

            predicateDescriptor = new PredicateDescriptor<Type>(
                    function,
                    this.getInputType().getDataUnitType().getTypeClass()
            );

        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        final Predicate<Type> predicate = javaExecutor.getCompiler().compile(predicateDescriptor);
        JavaExecutor.openFunction(this, predicate, inputs, operatorContext);
        ((StreamChannel.Instance) outputs[0]).accept(((JavaChannelInstance) inputs[0]).<Type>provideStream().filter(predicate));
*/
        final StreamChannel.Instance     input   = (StreamChannel.Instance)     inputs[0];
        final StreamChannel.Instance     output  = (StreamChannel.Instance)     outputs[0];
        final CollectionChannel.Instance output1 = (CollectionChannel.Instance) outputs[1];



        this.multiplexCollection = new MultiplexList<>();

        Stream<Type> sniffer = input
                .<Type>provideStream()
                .filter(
                    element -> {
                        multiplexCollection.add(function.apply(element));
                        return true;
                    }
                );
        output.accept(sniffer);
        output1.accept(multiplexCollection);


        return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index){
        assert index <= this.getNumInputs() || (index == 0 && this.getNumInputs() == 0);
        if (this.getInput(index).isBroadcast()) return Collections.singletonList(CollectionChannel.DESCRIPTOR);
        return Arrays.asList(CollectionChannel.DESCRIPTOR, StreamChannel.DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        if(index == 0) {
            return Collections.singletonList(StreamChannel.DESCRIPTOR);
        }
        if(index == 1){
            return Collections.singletonList(CollectionChannel.DESCRIPTOR);
        }
        return null;
    }
}
