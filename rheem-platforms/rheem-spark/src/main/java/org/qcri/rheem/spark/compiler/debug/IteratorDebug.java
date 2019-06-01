package org.qcri.rheem.spark.compiler.debug;

import java.util.Iterator;
import java.util.function.Function;

public class IteratorDebug<TypeInput, TypeOutput> implements Iterator<TypeOutput> {

    private Iterator<TypeInput> base;
    private Function<TypeInput, TypeOutput> function;

    public IteratorDebug(Iterable base, Function<TypeInput, TypeOutput> function) {
        this.base = base.iterator();
        this.function = function;
    }

    @Override
    public boolean hasNext() {
        return this.base.hasNext();
    }

    @Override
    public TypeOutput next() {
        return this.function.apply(this.base.next());
    }

}
