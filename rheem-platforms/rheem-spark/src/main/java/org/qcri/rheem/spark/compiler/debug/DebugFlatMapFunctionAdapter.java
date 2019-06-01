package org.qcri.rheem.spark.compiler.debug;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.qcri.rheem.basic.data.debug.DebugHeader;
import org.qcri.rheem.basic.data.debug.DebugKey;
import org.qcri.rheem.basic.data.debug.DebugTuple;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.function.ExecutionContext;
import org.qcri.rheem.core.function.FunctionDescriptor;
import org.qcri.rheem.core.util.stream.Streams;
import org.qcri.rheem.spark.execution.SparkExecutionContext;

import java.util.Iterator;

/**
 * Implements a {@link FlatMapFunction} that calls {@link org.qcri.rheem.core.function.ExtendedFunction#open(ExecutionContext)}
 * of its implementation before delegating the very first {@link Function#call(Object)}.
 */
public class DebugFlatMapFunctionAdapter<InputType, OutputType> implements FlatMapFunction<InputType, DebugTuple> {

    private final FunctionDescriptor.SerializableFunction<InputType, Iterable<OutputType>> impl;

    private final SparkExecutionContext executionContext;

    private boolean isFirstRun = true;
    private boolean isOpenFunction = true;
    private boolean isDebugTuple = true;

    public DebugFlatMapFunctionAdapter(FunctionDescriptor.SerializableFunction extendedFunction,
                                       SparkExecutionContext sparkExecutionContext) {
        this.impl = extendedFunction;
        this.executionContext = sparkExecutionContext;
        if(this.executionContext == null){
            this.isOpenFunction = false;
        }else{
            if( ! (this.impl instanceof FunctionDescriptor.ExtendedSerializableFunction)) {
                throw new RheemException("The Function not have the implementation of the method open");
            }
        }
    }

    @Override
    public Iterator<DebugTuple> call(InputType v1) throws Exception {
        if (this.isFirstRun) {
            this.isDebugTuple = v1.getClass() == DebugTuple.class;
            if(isOpenFunction) {
                ((FunctionDescriptor.ExtendedSerializableFunction) this.impl).open(this.executionContext);
            }
            this.isFirstRun = false;
        }
        InputType value;
        DebugKey parent;
        if(this.isDebugTuple){
            value = (InputType) ((DebugTuple)v1).getValue();
            parent = ((DebugTuple)v1).getHeader();
            return new IteratorDebug<>(
                    this.impl.apply(value),
                    element -> {
                        return new DebugTuple(parent.createChild(), element);
                    }
            );
        }else{
            value = v1;
            return new IteratorDebug<>(
                    this.impl.apply(value),
                    element -> {
                        return new DebugTuple(element);
                    }
            );
        }
    }

}
