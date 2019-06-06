package org.qcri.rheem.spark.compiler.debug;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.qcri.rheem.basic.data.debug.DebugKey;
import org.qcri.rheem.basic.data.debug.DebugTag;
import org.qcri.rheem.basic.data.debug.DebugTagType;
import org.qcri.rheem.basic.data.debug.DebugTuple;
import org.qcri.rheem.basic.data.debug.tag.MonitorDebugTag;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.function.ExecutionContext;
import org.qcri.rheem.core.function.FunctionDescriptor;
import org.qcri.rheem.spark.execution.SparkExecutionContext;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Implements a {@link FlatMapFunction} that calls {@link org.qcri.rheem.core.function.ExtendedFunction#open(ExecutionContext)}
 * of its implementation before delegating the very first {@link Function#call(Object)}.
 */
public class DebugFlatMapFunctionAdapter<InputType, OutputType> implements FlatMapFunction<InputType, DebugTuple<OutputType>> {

    private List<java.util.function.Function<DebugTuple<InputType>, DebugTag>> preTagProcessing = new ArrayList<>();
    private List<java.util.function.Function> postTagProcessing = new ArrayList<>();
    private final FunctionDescriptor.SerializableFunction<InputType, Iterable<OutputType>> impl;

    private final SparkExecutionContext executionContext;

    private boolean isFirstRun = true;
    private boolean isOpenFunction = true;
    private boolean isDebugTuple = true;
    private Class<OutputType> outputTypeClass;
    private String operator_name;
    private transient String myIp;

    public DebugFlatMapFunctionAdapter(FunctionDescriptor.SerializableFunction extendedFunction,
                                       SparkExecutionContext sparkExecutionContext,
                                       Class<OutputType> outputTypeClass,
                                       String operator_name
                                       ) {
        this.impl = extendedFunction;
        this.executionContext = sparkExecutionContext;
        this.outputTypeClass = outputTypeClass;
        this.operator_name = operator_name;
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
            this.myIp = InetAddress.getLocalHost().getHostAddress();
            final String ip_host = this.myIp;
            final String op_name = this.operator_name;
            this.preTagProcessing.add(debugTuple -> { return new MonitorDebugTag(op_name, ip_host).setTimeStart();});
           // this.postTagProcessing.add(debugTuple -> { return debugTuple.getTag(DebugTagType.MONITOR).setTimeEnd();});

        }
        InputType value;
        DebugKey parent;
        if(this.isDebugTuple){
            DebugTuple<InputType> tuple = (DebugTuple<InputType>) v1;
            value = tuple.getValue();
            parent = tuple.getHeader();

           //* long start_time = System.currentTimeMillis();
            Iterable<OutputType> iter = this.impl.apply(value);
           //* long end_time = System.currentTimeMillis();


            return new IteratorDebug<>(
                    iter,
                    element -> {
                        return new DebugTuple<OutputType>(parent.createChild(), element, outputTypeClass)
                                .addTag(
                                    new MonitorDebugTag(
                                        this.operator_name,
                                        this.myIp
                                    )
                              //*      .setTimeStart(start_time)
                              //*      .setTimeEnd(end_time)
                                )
                        ;
                    }
            );
        }else{
            value = v1;
          //*  long start_time = System.currentTimeMillis();
            Iterable<OutputType> iter = this.impl.apply(value);
          //*  long end_time = System.currentTimeMillis();
            return new IteratorDebug<>(
                    iter,
                    element -> {
                        return new DebugTuple<OutputType>(element, outputTypeClass)
                                .addTag(
                                        new MonitorDebugTag(
                                                this.operator_name,
                                                this.myIp
                                        )
                      //*                          .setTimeStart(start_time)
                      //*                          .setTimeEnd(end_time)
                                )
                        ;
                    }
            );
        }
    }

}
