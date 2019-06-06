package org.qcri.rheem.spark.compiler.debug;

import org.apache.spark.api.java.function.Function;
import org.qcri.rheem.basic.data.debug.DebugTuple;
import org.qcri.rheem.basic.data.debug.tag.MonitorDebugTag;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.function.ExecutionContext;
import org.qcri.rheem.core.function.PredicateDescriptor;
import org.qcri.rheem.spark.execution.SparkExecutionContext;

import java.net.InetAddress;

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
    private String operator_name;
    private transient String myIp;


    public DebugPredicateAdapater(PredicateDescriptor.SerializablePredicate<Type> extendedFunction,
                                  SparkExecutionContext sparkExecutionContext,
                                  String operator_name
                                    ) {
        this.impl = extendedFunction;
        this.executionContext = sparkExecutionContext;
        this.operator_name = operator_name;
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
            this.myIp = InetAddress.getLocalHost().getHostAddress();

        }
        Object value;
        boolean test_result;
        if(this.isDebugTuple){
            DebugTuple<Type> tuple = ((DebugTuple<Type>)dataQuantume);
            value = tuple.getValue();
            //* long start = System.currentTimeMillis();
            test_result = this.impl.test( value);
            //*long end = System.currentTimeMillis();
            tuple.addTag(
                new MonitorDebugTag(
                    this.operator_name,
                    this.myIp
                )
                        //*.setTimeStart(start)
                    //*.setTimeEnd(end)
            );

        }else{
            value = dataQuantume;
            test_result = this.impl.test( value);
        }
        return test_result;
    }

}
