package org.qcri.rheem.spark.compiler.debug;

import org.apache.spark.api.java.function.Function;
import org.qcri.rheem.basic.data.debug.DebugKey;
import org.qcri.rheem.basic.data.debug.DebugTuple;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.function.ExecutionContext;
import org.qcri.rheem.core.function.FunctionDescriptor;
import org.qcri.rheem.spark.execution.SparkExecutionContext;

import java.util.UUID;

/**
 * Implements a {@link Function} that calls {@link org.qcri.rheem.core.function.ExtendedFunction#open(ExecutionContext)}
 * of its implementation before delegating the very first {@link Function#call(Object)}.
 */
public class DebugMapFunctionAdapter<InputType, OutputType> implements Function<InputType, DebugTuple<OutputType>> {

    private final FunctionDescriptor.SerializableFunction<InputType, OutputType> impl;

    private final SparkExecutionContext executionContext;

    private boolean isFirstRun = true;
    private boolean isOpenFunction = true;
    private boolean isDebugTuple = false;
   // private boolean changeType = false;
    private Class<OutputType> outputTypeClass;

    public DebugMapFunctionAdapter(FunctionDescriptor.SerializableFunction<InputType, OutputType> extendedFunction,
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
    public DebugTuple<OutputType> call(InputType dataQuantume) throws Exception {
        if (this.isFirstRun) {
            if(dataQuantume.getClass() == DebugTuple.class) {
                this.isDebugTuple = true;
               /* if(((DebugTuple)dataQuantume).getType() != outputTypeClass){
                    this.changeType = true;
                }*/
            }
            if(isOpenFunction) {
                ((FunctionDescriptor.ExtendedSerializableFunction) this.impl).open(this.executionContext);
            }
            this.isFirstRun = false;
        }
        if(this.isDebugTuple){
            DebugTuple<InputType> tuple = (DebugTuple<InputType>)dataQuantume;
            //if(changeType){
                return tuple.setValue(this.impl.apply(tuple.getValue()), this.outputTypeClass);
            /*}else {
                return tuple.setValue(this.impl.apply(tuple.getValue()));
            }*/
        }else{
            return new DebugTuple<OutputType>(this.impl.apply(dataQuantume), this.outputTypeClass);
        }

    }

}
