package org.qcri.rheem.core.plan.rheemplan;

import org.qcri.rheem.core.data.Tuple;
import org.qcri.rheem.core.types.DataSetType;

import java.util.function.Function;

/**
 * This operator has a single input and two output.
 */
public abstract class SnifferBase<InputType, OutputType0, OutputType1 extends Tuple> extends OperatorBase implements ElementaryOperator{

    /**
     * Creates a new instance.
     */
    public SnifferBase(DataSetType<InputType> inputType,
                       DataSetType<OutputType0> outputType0,
                       DataSetType<OutputType1> outputType1,
                       boolean isSupportingBroadcastInputs) {
        super(1, 1, isSupportingBroadcastInputs);
        this.inputSlots[0] = new InputSlot<>("in", this, inputType);
        this.outputSlots[0] = new OutputSlot<>("out0", this, outputType0);
      //  this.outputSlots[1] = new OutputSlot<>("out1", this, outputType1);
    }

    /*
       Copies the given instance.
     *
     * @see BinaryToUnaryOperator#BinaryToUnaryOperator(DataSetType, DataSetType, DataSetType, boolean)
     * @see OperatorBase#OperatorBase(OperatorBase)
     */
    protected SnifferBase(SnifferBase<InputType, OutputType0, OutputType1> that) {
        super(that);
        this.inputSlots[0] = new InputSlot<>("in", this, that.getInputType());
        this.outputSlots[0] = new OutputSlot<>("out0", this, that.getOutputType0());
       // this.outputSlots[1] = new OutputSlot<>("out1", this, that.getOutputType1());
    }

    @SuppressWarnings("unchecked")
    public DataSetType<InputType> getInputType() {
        return ((InputSlot<InputType>) this.getInput(0)).getType();
    }

    @SuppressWarnings("unchecked")
    public DataSetType<OutputType0> getOutputType0() {
        return ((OutputSlot<OutputType0>) this.getOutput(0)).getType();
    }

/*
    @SuppressWarnings("unchecked")
    public DataSetType<OutputType1> getOutputType1() {
        return ((OutputSlot<OutputType1>) this.getOutput(1)).getType();
    }
*/
    public abstract void selfKill();

    public abstract Function<InputType, OutputType1> getFunction();
}
