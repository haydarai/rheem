package org.qcri.rheem.spark.compiler.debug;

import org.apache.spark.api.java.function.Function;
import org.qcri.rheem.basic.data.debug.DebugTuple;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.function.FunctionDescriptor;
import org.qcri.rheem.spark.execution.SparkExecutionContext;


public class DebugReducer<Type> implements Function<Iterable<DebugTuple<Type>>, DebugTuple<Type>> {

    private final FunctionDescriptor.SerializableBinaryOperator<Type> impl;

    private final SparkExecutionContext executionContext;

    private boolean isFirstRun = true;
    private boolean isOpenFunction = true;
    private boolean isDebugTuple = false;
    private Class<Type> outputTypeClass;

    public DebugReducer(FunctionDescriptor.SerializableBinaryOperator<Type> extendedFunction,
                        SparkExecutionContext sparkExecutionContext,
                        Class<Type> outputTypeClass
                        ) {

        this.impl = extendedFunction;
        this.executionContext = sparkExecutionContext;
        this.outputTypeClass = outputTypeClass;
        if(this.executionContext == null){
            this.isOpenFunction = false;
        }else{
            if( ! (this.impl instanceof FunctionDescriptor.ExtendedSerializableBinaryOperator)) {
                throw new RheemException("The Function not have the implementation of the method open");
            }
        }
    }

    @Override
    public DebugTuple<Type> call(Iterable<DebugTuple<Type>> v1) throws Exception {

        return new IteratorReducer<Type>(
                v1,
                tuple -> tuple.getValue(),
                this.impl
        ).reduce();
    }
}
