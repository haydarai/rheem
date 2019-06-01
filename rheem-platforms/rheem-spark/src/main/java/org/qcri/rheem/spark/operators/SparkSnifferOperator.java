package org.qcri.rheem.spark.operators;

import com.google.api.client.http.ByteArrayContent;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpContent;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.javanet.NetHttpTransport;
import org.apache.directory.api.util.Base64;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.qcri.rheem.basic.operators.SnifferOperator;
import org.qcri.rheem.core.function.FunctionDescriptor;
import org.qcri.rheem.core.function.PredicateDescriptor;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.platform.lineage.ExecutionLineageNode;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.util.Tuple;
import org.qcri.rheem.spark.channels.BroadcastChannel;
import org.qcri.rheem.spark.channels.RddChannel;
import org.qcri.rheem.spark.debug.SnifferDispacher;
import org.qcri.rheem.spark.debug.SocketSerializable;
import org.qcri.rheem.spark.execution.SparkExecutor;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Created by bertty on 31-05-17.
 */
public class SparkSnifferOperator<Type, TypeTuple extends org.qcri.rheem.core.data.Tuple>
        extends SnifferOperator<Type, TypeTuple>
        implements SparkExecutionOperator {

    /**
     * Creates a new instance.
     *
     * @param type type of the dataset elements
     */
    public SparkSnifferOperator(DataSetType<Type> type, DataSetType<TypeTuple> typeTuple) {
        super(type, typeTuple);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public SparkSnifferOperator(SnifferOperator<Type, TypeTuple> that) {
        super(that);
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            SparkExecutor sparkExecutor,
            OptimizationContext.OperatorContext operatorContext) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        PredicateDescriptor<Type> predicateDescriptor = null;

        //SocketSerializable<Type> socket = new SocketSerializable<>(""+Math.random(), "localhost", 9090);
        FunctionDescriptor.SerializablePredicate<Type> funct = new SnifferDispacher<>();

        predicateDescriptor = new PredicateDescriptor<>(
            funct,
            this.getInputType().getDataUnitType().getTypeClass()
        );

        final Function<Type, Boolean> filterFunction = sparkExecutor.getCompiler().compile(
                predicateDescriptor, this, operatorContext, inputs
        );

        final JavaRDD<Type> inputRdd = ((RddChannel.Instance) inputs[0]).provideRdd();
        final JavaRDD<Type> outputRdd = inputRdd.filter(filterFunction);
        this.name(outputRdd);
        ((RddChannel.Instance) outputs[0]).accept(outputRdd, sparkExecutor);

        return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);
    }


    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        if (index == 0) {
            return Arrays.asList(RddChannel.UNCACHED_DESCRIPTOR, RddChannel.CACHED_DESCRIPTOR);
        } else {
            return Collections.singletonList(BroadcastChannel.DESCRIPTOR);
        }
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        return Collections.singletonList(RddChannel.UNCACHED_DESCRIPTOR);
    }

    @Override
    public boolean containsAction() {
        return false;
    }


}
