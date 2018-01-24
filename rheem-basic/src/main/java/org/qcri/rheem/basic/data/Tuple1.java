package org.qcri.rheem.basic.data;

import org.qcri.rheem.core.data.Tuple;

import java.util.Objects;

/**
 * A type for tuples. Might be replaced by existing classes for this purpose, such as from the Scala library.
 */
public class Tuple1<T0> extends Tuple {

    static {
        CLASSES.put("Tuple1", Tuple1.class);
    }

    private final static int arity = 1;

    public T0 field0;

    public Tuple1() {}

    public Tuple1(T0 field0) {
        this.field0 = field0;
    }

    public T0 getField0() { return this.field0; }

    @Override
    public int getArity() {
        return arity;
    }

    @Override
    public <T> T getField(int index) {
        if(! this.validArity(index)){
            return null;
        }
        switch (index){
            case 0: return (T) this.field0;
            default: return null;
        }
    }

    @Override
    public <T> void setField(T value, int index) {
        if( ! this.validArity(index)){
            return;
        }
        switch (index){
            case 0:
                this.field0 = (T0) value;
                break;
            default: return;
        }
    }

    @Override
    public Tuple1<T0> copy() {
        return new Tuple1<T0>(
            this.field0
        );
    }

    public Tuple1<T0> swap() {
        return new Tuple1<T0>(
            this.field0
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || this.getClass() != o.getClass()) return false;
        Tuple1<?> tuple = (Tuple1<?>) o;
        return Objects.equals(this.field0, tuple.field0);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            this.field0
        );
    }

    @Override
    public String toString() {
        return String.format(
            "(%s)",
            this.field0
        );
    }
}
