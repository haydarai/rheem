package org.qcri.rheem.spark.compiler.debug;

import org.apache.spark.api.java.function.Function2;
import org.qcri.rheem.basic.data.debug.DebugKey;
import org.qcri.rheem.basic.data.debug.DebugTuple;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.function.ExecutionContext;
import org.qcri.rheem.core.function.FunctionDescriptor;
import org.qcri.rheem.spark.execution.SparkExecutionContext;

/**
 * Implements a {@link Function2} that calls {@link org.qcri.rheem.core.function.ExtendedFunction#open(ExecutionContext)}
 * of its implementation before delegating the very first {@link Function2#call(Object, Object)}.
 */
public class DebugBinaryOperatorAdapterDebugTuple<Type> implements Function2<DebugTuple, DebugTuple, DebugTuple> {

    private final FunctionDescriptor.SerializableBinaryOperator impl;

    private final SparkExecutionContext executionContext;

    private boolean isFirstRun = true;
    private boolean isOpenFunction = true;
    private boolean isDebugTuple = false;


    /*TEST*
    private transient URI uri;
    private transient HttpRequestFactory requestFactory;
    private transient HttpRequest request;
    private transient ByteArrayOutputStream bos;
    private transient ObjectOutputStream oos;
    *END TEST*/

    public DebugBinaryOperatorAdapterDebugTuple(FunctionDescriptor.SerializableBinaryOperator extendedFunction,
                                                SparkExecutionContext sparkExecutionContext) {
        this.impl = extendedFunction;
        this.executionContext = sparkExecutionContext;
        if(this.executionContext == null){
            this.isOpenFunction = false;
        }else{
            if( ! (this.impl instanceof FunctionDescriptor.ExtendedSerializableBinaryOperator)) {
                throw new RheemException("The Function not have the implementation of the method open");
            }
        }
        /*TEST*
        try {
            uri = URI.create("http://10.4.4.32:8080/debug/reduce");
            this.requestFactory = new NetHttpTransport().createRequestFactory();
            this.request = requestFactory.buildPostRequest(new GenericUrl(uri), null);
            this.bos = new ByteArrayOutputStream();
            this.oos = new ObjectOutputStream(bos);
        } catch (IOException e) {
            e.printStackTrace();
        }
        *END TEST*/
    }

    @Override
    public DebugTuple call(DebugTuple tuple0, DebugTuple tuple1) throws Exception {
       /* if (this.isFirstRun) {
            if(isOpenFunction) {
                ((FunctionDescriptor.ExtendedSerializableBinaryOperator) this.impl).open(this.executionContext);
            }
            this.isFirstRun = false;
        }*/
       /* DebugKey key0 = tuple0.getHeader();
        DebugKey key1 = tuple1.getHeader();*/

        Object value0 = tuple0.getValue();
        Object value1 = tuple1.getValue();

        DebugTuple nextTuple = tuple0;
        //TODO add the validation of key is empty in the uuid
       /* if(key0.hasParent() && key1.hasParent()){
            key0.plus(key1);
            nextTuple = tuple0;
        }else if ( key0.hasParent() && !key1.hasParent()){
            key0.addParent(key1.getValue());
            nextTuple = tuple0;
        }else if (!key0.hasParent() &&  key1.hasParent()) {
            key1.addParent(key0.getValue());
            nextTuple = tuple1;
        }else{
      //}else if (!key0.hasParent() && !key1.hasParent()) {
            //TODO recovery the
            DebugKey nextKey = DebugTuple
                    .buildKey()
                    .addParent(key0.getValue())
                    .addParent(key1.getValue());
            nextTuple = new DebugTuple(nextKey, null);
        }*/
       // key0.getBytes();
       // key1.getBytes();
        //nextTuple.getHeader().getBytes();
        /*key0.cleanParent();
        key1.cleanParent();
        nextTuple.getHeader().cleanParent();*/
        /*try {
            this.request
                .setContent(
                    new ByteArrayContent(
                    null,
                        nextTuple.getHeader().getBytes())
                )
                .executeAsync();
        } catch (Exception e) {
            e.printStackTrace();
        }*/

        return  nextTuple.setValue(this.impl.apply(value0, value1));

    }

}
