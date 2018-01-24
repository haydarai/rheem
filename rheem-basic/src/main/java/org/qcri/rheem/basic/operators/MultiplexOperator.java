package org.qcri.rheem.basic.operators;

import org.qcri.rheem.core.data.Tuple;
import org.qcri.rheem.core.function.FunctionDescriptor;
import org.qcri.rheem.core.plan.rheemplan.UnaryToManyOperator;
import org.qcri.rheem.core.plan.rheemplan.UnaryToUnaryOperator;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.types.TupleType;

import java.util.function.Function;

/**
 * Created by bertty on 09-11-17.
 */
public class MultiplexOperator<InputType extends Tuple, OutputType extends Tuple> extends UnaryToManyOperator<InputType, OutputType> {

    protected FunctionDescriptor.SerializableFunction[] converts;


    public MultiplexOperator(DataSetType<InputType> inputType, TupleType<OutputType> outputType, boolean isSupportingBroadcastInputs) {
        super(inputType, outputType, isSupportingBroadcastInputs);
        this.converts = new FunctionDescriptor.SerializableFunction[this.getNumOutputs()];
    }

    public MultiplexOperator(UnaryToManyOperator<InputType, OutputType> that) {
        super(that);
        this.converts = ((MultiplexOperator)that).getConverts();

    }

    public void setFunction(int index, FunctionDescriptor.SerializableFunction<InputType, ?> function) throws Exception {
        if(index < 0 || index > converts.length ){
            throw new Exception("error    ");
            //TODO: generate exception for this case
        }
        if(this.converts[index] != null){
            throw new Exception("error2    ");
            //TODO: generate exception for this case
        }
        this.converts[index] = function;
    }

    public FunctionDescriptor.SerializableFunction[] getConverts(){
        return this.converts;
    }

}
