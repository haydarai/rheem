package org.qcri.rheem.basic.operators;

import org.qcri.rheem.basic.collection.MultiplexCollection;
import org.qcri.rheem.basic.collection.MultiplexStatus;
import org.qcri.rheem.basic.data.Tuple1;
import org.qcri.rheem.core.data.Tuple;
import org.qcri.rheem.core.plan.rheemplan.SnifferBase;
import org.qcri.rheem.core.types.BasicDataUnitType;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.types.TupleType;

import java.util.function.Function;

/**
 * Created by bertty on 28-05-17.
 */
public class SnifferOperator<Type, TypeTuple extends Tuple> extends SnifferBase<Type, Type, TypeTuple> {

    protected MultiplexCollection<Tuple> multiplexCollection;

    protected Function<Type, TypeTuple> function;
/*
    {
        this.function = new Function<Type, TypeTuple>() {
            @Override
            public TypeTuple apply(Type type) {
                return (TypeTuple) null;
            }
        };
    }
*/
    public SnifferOperator(DataSetType<Type> inputType, DataSetType<TypeTuple> typeTuple){
        this(inputType, inputType, typeTuple, false);
    }

    public SnifferOperator(DataSetType<Type> inputType, DataSetType<Type> outputType0, DataSetType<TypeTuple> outputType1, boolean isSupportingBroadcastInputs) {
        super(inputType, outputType0, outputType1, isSupportingBroadcastInputs);
    }

    public SnifferOperator(SnifferBase<Type, Type, TypeTuple> that) {
        super(that);
        this.function = that.getFunction();
    }

    public SnifferOperator(Class<Type> clazz, Class<TypeTuple> _classTuple) {
        this(
            DataSetType.<Type>createDefault(BasicDataUnitType.createBasic(clazz)),
            DataSetType.<TypeTuple>createDefault(BasicDataUnitType.createBasic(_classTuple))
        );
    }


    public SnifferOperator<Type, TypeTuple> setFunction(Function<Type, TypeTuple> function){
        this.function = function;
        return this;
    }

    @Override
    public Function<Type, TypeTuple> getFunction(){
        return this.function;
    }
    @Override
    public void selfKill() {
        multiplexCollection.setStatus(MultiplexStatus.FINISH_INPUT);
    }
}

