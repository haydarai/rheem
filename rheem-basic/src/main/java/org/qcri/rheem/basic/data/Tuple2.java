package org.qcri.rheem.basic.data;

import org.qcri.rheem.core.data.Tuple;

import java.util.Objects;

/**
 * A type for tuples. Might be replaced by existing classes for this purpose, such as from the Scala library.
 */
public class Tuple2<T0, T1> extends Tuple {

    static {
        CLASSES.put("Tuple2", Tuple2.class);
    }

    private final static int arity = 2;

    public T0 field0;
    public T1 field1;

    public Tuple2() {}

    public Tuple2(T0 field0, T1 field1) {
        this.field0 = field0;
        this.field1 = field1;
    }

    public T0 getField0() { return this.field0; }
    public T1 getField1() { return this.field1; }

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
            case 1: return (T) this.field1;
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
            case 1:
                this.field1 = (T1) value;
                break;
            default: return;
        }
    }

    @Override
    public Tuple2<T0, T1> copy() {
        return new Tuple2<T0, T1>(
            this.field0,
            this.field1
        );
    }

    @Override
    public Tuple2<T1, T0> swap() {
        return new Tuple2<T1, T0>(
            this.field1,
            this.field0
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || this.getClass() != o.getClass()) return false;
        Tuple2<?, ?> tuple = (Tuple2<?, ?>) o;
        return Objects.equals(this.field0, tuple.field0) &&
                Objects.equals(this.field1, tuple.field1);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            this.field0,
            this.field1
        );
    }

    @Override
    public String toString() {
        return String.format(
            "(%s, %s)",
            this.field0,
            this.field1
        );
    }
}
