package org.qcri.rheem.basic.data;

import java.io.Serializable;
import java.util.Objects;

/**
 * A type for tuples. Might be replaced by existing classes for this purpose, such as from the Scala library.
 */
public class Tuple4<T0, T1, T2, T3> implements Serializable {

    public T0 field0;

    public T1 field1;

    public T2 field2;

    public T3 field3;

    public Tuple4() {
    }

    public Tuple4(T0 field0, T1 field1, T2 field2, T3 field3) {
        this.field0 = field0;
        this.field1 = field1;
        this.field2 = field2;
        this.field3 = field3;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || this.getClass() != o.getClass()) return false;
        Tuple4<?, ?, ?, ?> tuple2 = (Tuple4<?, ?, ?, ?>) o;
        return Objects.equals(this.field0, tuple2.field0) &&
                Objects.equals(this.field1, tuple2.field1) &&
                Objects.equals(this.field2, tuple2.field2) &&
                Objects.equals(this.field3, tuple2.field3);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.field0, this.field1, this.field2, this.field3);
    }

    @Override
    public String toString() {
        return String.format("(%s, %s, %s, %s)", this.field0, this.field1, this.field2, this.field3);
    }

    public T0 getField0() {
        return this.field0;
    }

    public T1 getField1() {
        return this.field1;
    }

    public T2 getField2() {
        return this.field2;
    }

    public T3 getField3() {
        return this.field3;
    }

    /**
     * @return a new instance with the fields of this instance swapped
     */
    public Tuple4<T3, T2, T1, T0> swap() {
        return new Tuple4<>(this.field3, this.field2, this.field1, this.field0);
    }
}
