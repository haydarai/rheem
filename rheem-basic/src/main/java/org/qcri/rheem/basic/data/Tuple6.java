package org.qcri.rheem.basic.data;

import org.qcri.rheem.core.data.Tuple;

import java.util.Objects;

/**
 * A type for tuples. Might be replaced by existing classes for this purpose, such as from the Scala library.
 */
public class Tuple6<T0, T1, T2, T3, T4, T5> extends Tuple {

    static {
        CLASSES.put("Tuple6", Tuple6.class);
    }

    private final static int arity = 6;

    public T0 field0;
    public T1 field1;
    public T2 field2;
    public T3 field3;
    public T4 field4;
    public T5 field5;

    public Tuple6() {}

    public Tuple6(T0 field0, T1 field1, T2 field2, T3 field3, T4 field4, T5 field5) {
        this.field0 = field0;
        this.field1 = field1;
        this.field2 = field2;
        this.field3 = field3;
        this.field4 = field4;
        this.field5 = field5;
    }

    public T0 getField0() { return this.field0; }
    public T1 getField1() { return this.field1; }
    public T2 getField2() { return this.field2; }
    public T3 getField3() { return this.field3; }
    public T4 getField4() { return this.field4; }
    public T5 getField5() { return this.field5; }

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
            case 5: return (T) this.field5;
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
            case 5:
                this.field5 = (T5) value;
                break;
            default: return;
        }
    }

    @Override
    public Tuple6<T0, T1, T2, T3, T4, T5> copy() {
        return new Tuple6<T0, T1, T2, T3, T4, T5>(
            this.field0,
            this.field1,
            this.field2,
            this.field3,
            this.field4,
            this.field5
        );
    }

    @Override
    public Tuple6<T5, T4, T3, T2, T1, T0> swap() {
        return new Tuple6<T5, T4, T3, T2, T1, T0>(
                this.field5,
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
        Tuple6<?, ?, ?, ?, ?, ?> tuple = (Tuple6<?, ?, ?, ?, ?, ?>) o;
        return Objects.equals(this.field0, tuple.field0) &&
                Objects.equals(this.field1, tuple.field1) &&
                Objects.equals(this.field2, tuple.field2) &&
                Objects.equals(this.field3, tuple.field3) &&
                Objects.equals(this.field4, tuple.field4) &&
                Objects.equals(this.field5, tuple.field5);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                this.field0,
                this.field1,
                this.field2,
                this.field3,
                this.field4,
                this.field5
        );
    }

    @Override
    public String toString() {
        return String.format(
                "(%s, %s, %s, %s, %s, %s)",
                this.field0,
                this.field1,
                this.field2,
                this.field3,
                this.field4,
                this.field5
        );
    }
}
