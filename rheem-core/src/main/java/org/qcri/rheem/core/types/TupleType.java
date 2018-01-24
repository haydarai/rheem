package org.qcri.rheem.core.types;

import org.qcri.rheem.core.data.NumberOfFieldException;
import org.qcri.rheem.core.data.Tuple;
import org.qcri.rheem.core.util.ReflectionUtils;

import java.util.Arrays;
import java.util.Objects;

/**
 * Created by bertty on 09-11-17.
 */
public class TupleType<T extends Tuple> extends DataSetType<T> {

    /**
     * Type of the data units within the data set.
     */
    private final Class<T> tupleType;

    private final Class[] fieldTypes;

    public static TupleType TUPLE_VOID = new TupleType(Void.class, Void.class);


    public TupleType(Class ... fieldTypes) {
        super(new BasicDataUnitType<>(fieldTypes[0]));
        int num_fields = fieldTypes.length - 1;
        this.tupleType = fieldTypes[0];
        this.fieldTypes = Arrays.copyOfRange(fieldTypes, 1, fieldTypes.length);
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || this.getClass() != o.getClass()) return false;
        BasicDataUnitType that = (BasicDataUnitType) o;
        return Objects.equals(this.tupleType, that.getTypeClass());
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.tupleType);
    }

    @Override
    public String toString() {
        return String.format("%s[%s]", this.getClass().getSimpleName(), this.tupleType.getSimpleName());
    }

    public int getArity(){
        return this.fieldTypes.length;
    }

    public DataSetType getField(int index){
        return DataSetType.createDefault(this.fieldTypes[index]);
    }

}
