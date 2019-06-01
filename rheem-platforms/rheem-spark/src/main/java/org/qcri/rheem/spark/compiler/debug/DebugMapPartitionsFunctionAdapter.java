package org.qcri.rheem.spark.compiler.debug;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.function.FunctionDescriptor;
import org.qcri.rheem.core.util.Iterators;
import org.qcri.rheem.spark.execution.SparkExecutionContext;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Function;
import java.util.stream.StreamSupport;

/**
 * Wraps a {@link Function} as a {@link FlatMapFunction}.
 */
public class DebugMapPartitionsFunctionAdapter<InputType, OutputType>
        implements FlatMapFunction<Iterator<InputType>, OutputType> {

    private final FunctionDescriptor.SerializableFunction<Iterable<InputType>, Iterable<OutputType>> impl;

    private final SparkExecutionContext executionContext;


    private boolean isFirstRun = true;

    public DebugMapPartitionsFunctionAdapter(
            FunctionDescriptor.SerializableFunction<Iterable<InputType>, Iterable<OutputType>> extendedFunction,
            SparkExecutionContext sparkExecutionContext) {
        this.impl = extendedFunction;
        this.executionContext = sparkExecutionContext;
        if(this.executionContext == null){
            this.isFirstRun = false;
        }else{
            if( ! (this.impl instanceof FunctionDescriptor.ExtendedSerializableFunction)) {
                throw new RheemException("The Function not have the implementation of the method open");
            }
        }
    }

    @Override
    public Iterator<OutputType> call(Iterator<InputType> it) throws Exception {
        if (this.isFirstRun) {
            ((FunctionDescriptor.ExtendedSerializableFunction)this.impl).open(this.executionContext);
            this.isFirstRun = false;
        }
        List<OutputType> out = new ArrayList<>();
        while (it.hasNext()) {
            final Iterable<OutputType> mappedPartition = this.impl.apply(Iterators.wrapWithIterable(it));
            for (OutputType dataQuantum : mappedPartition) {
                out.add(dataQuantum);
            }
        }

        return out.iterator();
    }
}