package org.qcri.rheem.basic.data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * A type for tuples. Might be replaced by existing classes for this purpose, such as from the Scala library.
 */
public class Tuple3<T0, T1, T2> implements Serializable {

    public T0 field0;

    public T1 field1;

    public T2 field2;

    public Tuple3() {
    }

    public Tuple3(T0 field0, T1 field1, T2 field2) {
        this.field0 = field0;
        this.field1 = field1;
        this.field2 = field2;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || this.getClass() != o.getClass()) return false;
        Tuple3<?, ?, ?> tuple3 = (Tuple3<?, ?, ?>) o;
        return Objects.equals(this.field0, tuple3.field0) &&
                Objects.equals(this.field1, tuple3.field1) &&
                Objects.equals(this.field2, tuple3.field2);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.field0, this.field1);
    }

    @Override
    public String toString() {
        return String.format("(%s, %s, %s)", this.field0, this.field1, this.field2);
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

    /**
     * @return a new instance with the fields of this instance swapped
     */
    public Tuple3<T2, T1, T0> swap() {
        return new Tuple3<>(this.field2, this.field1, this.field0);
    }

    public List<String> asList() {
        return Arrays.asList(this.field0.toString(), this.field1.toString(), this.field2.toString());
    }
}
