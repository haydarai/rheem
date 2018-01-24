package org.qcri.rheem.basic.operators;

import org.qcri.rheem.core.function.FunctionDescriptor;
import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.plan.rheemplan.UnarySource;
import org.qcri.rheem.core.types.DataSetType;

import java.util.Iterator;

/**
 * This source reads the data units from an Iterator
 */
public class IteratorSource<InputType, OutputType> extends UnarySource<OutputType> {

    protected Class<InputType> inputTypeClass = null;

    protected Iterator<InputType> iterator = null;

    protected TransformationDescriptor<InputType, OutputType> functionDescriptor = null;

    public IteratorSource(Class<InputType> inputTypeClass, Class<OutputType> outputTypeClass) {
        super(DataSetType.createDefault(outputTypeClass));
        this.inputTypeClass = inputTypeClass;
    }

    public IteratorSource(
            Iterator<InputType> iterator,
            FunctionDescriptor.SerializableFunction<InputType, OutputType> function,
            Class<InputType> inputTypeClass,
            Class<OutputType> outputTypeClass){
        this(inputTypeClass, outputTypeClass);
        this.iterator = iterator;
        this.inputTypeClass = inputTypeClass;
        this.functionDescriptor = new TransformationDescriptor<InputType, OutputType>(function, inputTypeClass, outputTypeClass);
    }

    /**
     * Copies an instance
     *
     * @param that that should be copied
     */
    public IteratorSource(IteratorSource that) {
        super(that);
        this.iterator = that.getIterator();
        this.functionDescriptor = that.getTransformation();
        this.inputTypeClass = that.getInputType();
    }

    public void setIterator(Iterator<InputType> iterator){
        this.iterator = iterator;
    }

    public Iterator<InputType> getIterator(){
        return this.iterator;
    }

    public void setFunctionDescriptor(TransformationDescriptor<InputType, OutputType>  functionDescriptor){
        this.functionDescriptor = functionDescriptor;
    }

    public TransformationDescriptor<InputType, OutputType> getTransformation(){
        return this.functionDescriptor;
    }

    public void setInputType(Class<InputType> inputTypeClass){
        this.inputTypeClass = inputTypeClass;
    }

    public Class<InputType> getInputType(){
        return this.inputTypeClass;
    }
}
