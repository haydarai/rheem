package org.qcri.rheem.spark.compiler.debug;

import org.qcri.rheem.basic.data.debug.DebugTuple;
import org.qcri.rheem.core.function.FunctionDescriptor;

import java.io.Serializable;
import java.util.Iterator;
import java.util.function.Function;

public class IteratorReducer<Type> implements Iterator<DebugTuple<Type>>, Serializable {

    private Iterator<DebugTuple<Type>> base;
    private Function<DebugTuple<Type>, Type> function;
    private FunctionDescriptor.SerializableBinaryOperator<Type> reducer;

    public IteratorReducer(Iterable base,
                           Function<DebugTuple<Type>, Type> function,
                           FunctionDescriptor.SerializableBinaryOperator<Type> reducer
                           ) {
        this.base = base.iterator();
        this.function = function;
        this.reducer = reducer;
    }

    @Override
    public boolean hasNext() {
        return this.base.hasNext();
    }

    @Override
    public DebugTuple<Type> next() {
        return this.base.next();
    }

    public DebugTuple<Type> reduce() {
        //ASUMING THE ITERATOR AT LEAST CONTAINS ONE ELEMENT
        DebugTuple<Type> result = this.base.next();
        while(this.base.hasNext()){
            result.setValue(reducer.apply(result.getValue(), this.function.apply(this.base.next())));
        }
        return result;
    }
}
