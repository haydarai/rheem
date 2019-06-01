package org.qcri.rheem.spark.compiler.debug;

import org.apache.spark.api.java.function.PairFunction;
import org.qcri.rheem.basic.data.debug.DebugTuple;
import org.qcri.rheem.core.function.FunctionDescriptor;
import org.qcri.rheem.spark.compiler.MetaFunctionCompiler;
import scala.Tuple2;

public class DebugKeyExtractorAdapter<T, K> implements MetaFunctionCompiler.KeyExtractor<T, K> {

    private final FunctionDescriptor.SerializableFunction<Object, K> impl;

    private boolean isFirstRun = true;
    private boolean isDebugTuple = false;

    public DebugKeyExtractorAdapter(FunctionDescriptor.SerializableFunction<T, K> impl) {
        this.impl = (FunctionDescriptor.SerializableFunction<Object, K>) impl;
    }

    @Override
    public scala.Tuple2<K, T> call(T t) throws Exception {
        if(this.isFirstRun){
            this.isDebugTuple = t.getClass() == DebugTuple.class;
            this.isFirstRun = false;
        }
        K key;
        if(isDebugTuple){
            key = this.impl.apply(((DebugTuple)t).field1);
        }else{
            key = this.impl.apply(t);
        }
        return new scala.Tuple2<>(key, t);
    }
}
