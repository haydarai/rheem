package org.qcri.rheem.spark.compiler.debug;

import org.qcri.rheem.basic.data.debug.DebugTuple;

import java.io.Serializable;
import java.util.Iterator;
import java.util.function.Function;

public class IteratorDebug<Type> implements Iterator<DebugTuple<Type>>, Serializable {

    private Iterator<Type> base;
    private Function<Type, DebugTuple<Type>> function;

    public IteratorDebug(Iterable base, Function<Type, DebugTuple<Type>> function) {
        this.base = base.iterator();
        this.function = function;
    }

    @Override
    public boolean hasNext() {
        return this.base.hasNext();
    }

    @Override
    public DebugTuple<Type> next() {
        return this.function.apply(this.base.next());
    }

}
