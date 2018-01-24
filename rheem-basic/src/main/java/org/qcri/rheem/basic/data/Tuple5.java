package org.qcri.rheem.basic.data;

import org.qcri.rheem.core.data.Tuple;

import java.util.Objects;

/**
 * A type for tuples. Might be replaced by existing classes for this purpose, such as from the Scala library.
 */
public class Tuple5<T0, T1, T2, T3, T4> extends Tuple {

    static {
        CLASSES.put("Tuple5", Tuple5.class);
    }

    private final static int arity = 5;

    public T0 field0;
    public T1 field1;
    public T2 field2;
    public T3 field3;
    public T4 field4;

    public Tuple5() {
    }

    public Tuple5(T0 field0, T1 field1, T2 field2, T3 field3, T4 field4) {
        this.field0 = field0;
        this.field1 = field1;
        this.field2 = field2;
        this.field3 = field3;
        this.field4 = field4;
    }

    public T0 getField0() { return this.field0; }
    public T1 getField1() { return this.field1; }
    public T2 getField2() { return this.field2; }
    public T3 getField3() { return this.field3; }
    public T4 getField4() { return this.field4; }

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
            case 2: return (T) this.field2;
            case 3: return (T) this.field3;
            case 4: return (T) this.field4;
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
            case 2:
                this.field2 = (T2) value;
                break;
            case 3:
                this.field3 = (T3) value;
                break;
            case 4:
                this.field4 = (T4) value;
                break;
            default: return;
        }
    }

    @Override
    public Tuple5<T0, T1, T2, T3, T4> copy() {
        return new Tuple5<T0, T1, T2, T3, T4>(
            this.field0,
            this.field1,
            this.field2,
            this.field3,
            this.field4
        );
    }

    @Override
    public Tuple5<T4, T3, T2, T1, T0> swap() {
        return new Tuple5<T4, T3, T2, T1, T0>(
                this.field4,
                this.field3,
                this.field2,
                this.field1,
                this.field0
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || this.getClass() != o.getClass()) return false;
        Tuple5<?, ?, ?, ?, ?> tuple = (Tuple5<?, ?, ?, ?, ?>) o;
        return Objects.equals(this.field0, tuple.field0) &&
                Objects.equals(this.field1, tuple.field1) &&
                Objects.equals(this.field2, tuple.field2) &&
                Objects.equals(this.field3, tuple.field3) &&
                Objects.equals(this.field4, tuple.field4);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                this.field0,
                this.field1,
                this.field2,
                this.field3,
                this.field4
        );
    }

    @Override
    public String toString() {
        return String.format(
                "(%s, %s, %s, %s, %s)",
                this.field0,
                this.field1,
                this.field2,
                this.field3,
                this.field4
        );
    }
}
