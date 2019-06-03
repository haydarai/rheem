package org.qcri.rheem.spark.compiler.debug;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.qcri.rheem.basic.data.debug.DebugKey;
import org.qcri.rheem.basic.data.debug.DebugTuple;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.function.ExecutionContext;
import org.qcri.rheem.core.function.FunctionDescriptor;
import org.qcri.rheem.spark.execution.SparkExecutionContext;

import java.util.Iterator;

/**
 * Implements a {@link FlatMapFunction} that calls {@link org.qcri.rheem.core.function.ExtendedFunction#open(ExecutionContext)}
 * of its implementation before delegating the very first {@link Function#call(Object)}.
 */
public class DebugFlatMapFunctionAdapter<InputType, OutputType> implements FlatMapFunction<InputType, DebugTuple<OutputType>> {

    private final FunctionDescriptor.SerializableFunction<InputType, Iterable<OutputType>> impl;

    private final SparkExecutionContext executionContext;

    private boolean isFirstRun = true;
    private boolean isOpenFunction = true;
    private boolean isDebugTuple = true;
    private Class<OutputType> outputTypeClass;

    public DebugFlatMapFunctionAdapter(FunctionDescriptor.SerializableFunction extendedFunction,
                                       SparkExecutionContext sparkExecutionContext,
                                       Class<OutputType> outputTypeClass
                                       ) {
        this.impl = extendedFunction;
        this.executionContext = sparkExecutionContext;
        this.outputTypeClass = outputTypeClass;
        if(this.executionContext == null){
            this.isOpenFunction = false;
        }else{
            if( ! (this.impl instanceof FunctionDescriptor.ExtendedSerializableFunction)) {
                throw new RheemException("The Function not have the implementation of the method open");
            }
        }
    }

    @Override
    public Iterator<DebugTuple<OutputType>> call(InputType v1) throws Exception {
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
            DebugTuple<InputType> tuple = (DebugTuple<InputType>) v1;
            value = tuple.getValue();
            parent = tuple.getHeader();
            return new IteratorDebug<>(
                    this.impl.apply(value),
                    element -> {
                        return new DebugTuple<OutputType>(parent.createChild(), element, outputTypeClass);
                    }
            );
        }else{
            value = v1;
            return new IteratorDebug<>(
                    this.impl.apply(value),
                    element -> {
                        return new DebugTuple<OutputType>(element, outputTypeClass);
                    }
            );
        }
    }

}
