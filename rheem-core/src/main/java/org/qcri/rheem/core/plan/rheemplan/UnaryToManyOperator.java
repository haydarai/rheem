package org.qcri.rheem.core.plan.rheemplan;

import org.qcri.rheem.core.data.Tuple;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.types.TupleType;

/**
 * This operator has a single input and generate the same output, but the outputs is running in parallel and is posible
 * that is consumed for diferents channels
 */
public abstract class UnaryToManyOperator<InputType extends Tuple, OutputType extends Tuple> extends OperatorBase implements ElementaryOperator{

    private TupleType tupleOutput;

    /**
     * Creates a new instance.
     */
    protected UnaryToManyOperator(DataSetType<InputType> inputType,
                                  TupleType<OutputType> outputType,
                                   boolean isSupportingBroadcastInputs) {
        super(1, outputType.getArity(), isSupportingBroadcastInputs);
        this.inputSlots[0] = new InputSlot<>("in", this, inputType);
        for(int i = 0; i < outputType.getArity(); i++) {
            this.outputSlots[i] = new OutputSlot<>("out"+i, this, outputType.getField(i));
        }
        this.tupleOutput = outputType;
    }

    /**
     * Copies the given instance.
     *
     * @see UnaryToUnaryOperator#UnaryToUnaryOperator(DataSetType, DataSetType, boolean)
     * @see OperatorBase#OperatorBase(OperatorBase)
     */
    protected UnaryToManyOperator(UnaryToManyOperator<InputType, OutputType> that) {
        super(that);
        this.inputSlots[0] = new InputSlot<>("in", this, that.getInputType());
        TupleType output = that.tupleOutput;
        for(int i = 0; i < output.getArity(); i++) {
            this.outputSlots[i] = new OutputSlot<>("out"+i, this, output.getField(i));
        }
        this.tupleOutput = output;
    }

    @SuppressWarnings("unchecked")
    public InputSlot<InputType> getInput() {
        return (InputSlot<InputType>) this.getInput(0);
    }


    public DataSetType<InputType> getInputType() {
        return this.getInput().getType();
    }

    public DataSetType getOutputType(int index) {
        return this.getOutput(index).getType();
    }

    public TupleType getTupleOutput(){
        return this.getTupleOutput();
    }
}
