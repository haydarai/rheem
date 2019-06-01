package org.qcri.rheem.spark.compiler.debug;

import org.apache.spark.api.java.function.Function;
import org.qcri.rheem.basic.data.debug.DebugTuple;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.function.ExecutionContext;
import org.qcri.rheem.core.function.PredicateDescriptor;
import org.qcri.rheem.spark.execution.SparkExecutionContext;

/**
 * Implements a {@link Function} that calls {@link org.qcri.rheem.core.function.ExtendedFunction#open(ExecutionContext)}
 * of its implementation before delegating the very first {@link Function#call(Object)}.
 */
public class DebugPredicateAdapater<Type> implements Function<Type, Boolean> {

    private final PredicateDescriptor.SerializablePredicate impl;

    private final SparkExecutionContext executionContext;

    private boolean isFirstRun = true;
    private boolean isOpenFunction = true;
    private boolean isDebugTuple = false;

    public DebugPredicateAdapater(PredicateDescriptor.SerializablePredicate<Type> extendedFunction,
                                  SparkExecutionContext sparkExecutionContext) {
        this.impl = extendedFunction;
        this.executionContext = sparkExecutionContext;
        if(this.executionContext == null){
            this.isOpenFunction = false;
        }else{
            if( ! (this.impl instanceof PredicateDescriptor.ExtendedSerializablePredicate)) {
                throw new RheemException("The Function not have the implementation of the method open");
            }
        }
    }

    @Override
    public Boolean call(Type dataQuantume) throws Exception {
        if (this.isFirstRun) {
            if(dataQuantume != null) {
                this.isDebugTuple = dataQuantume.getClass() == DebugTuple.class;
            }else{
                this.isDebugTuple = false;
            }
            if(this.isOpenFunction) {
                ((PredicateDescriptor.ExtendedSerializablePredicate) this.impl).open(this.executionContext);
            }
            this.isFirstRun = false;
        }
        Object value;
        if(this.isDebugTuple){
            value = ((DebugTuple)dataQuantume).getValue();
        }else{
            value = dataQuantume;
        }
        return this.impl.test( value);
    }

}
