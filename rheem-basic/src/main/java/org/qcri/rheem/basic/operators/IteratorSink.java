package org.qcri.rheem.basic.operators;

import org.qcri.rheem.core.function.FunctionDescriptor;
import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.plan.rheemplan.UnarySink;
import org.qcri.rheem.core.types.DataSetType;

import java.util.Iterator;

/**
 * This {@link UnarySink} that represent the output as a Iterator
 */
public class IteratorSink<InputType, OutputType> extends UnarySink<InputType> {

    protected Class<OutputType> outputTypeClass = null;

    protected Iterator<InputType> iterator = null;

    protected TransformationDescriptor<InputType, OutputType> functionDescriptor = null;


    public IteratorSink(Class<InputType> inputTypeClass, Class<OutputType> outputTypeClass) {
        super(DataSetType.createDefault(inputTypeClass));
        this.outputTypeClass = outputTypeClass;
    }

    /**
     * TODO: traducir al ingles
     * @param iterator iterator que se pasara a la siguiente etapa o que obtendra
     * @param function esta funcion es la aplicara cuando se comienze a procesar el iterator, es decir, el primer map
     * @param inputTypeClass el tipo del input
     * @param outputTypeClass el tipo del output pero es el output del siguiente operador
     */
    public IteratorSink(
            Iterator<InputType> iterator,
            FunctionDescriptor.SerializableFunction<InputType, OutputType> function,
            Class<InputType> inputTypeClass,
            Class<OutputType> outputTypeClass){
        this(inputTypeClass, outputTypeClass);
        this.iterator = iterator;
        this.outputTypeClass = outputTypeClass;
        this.functionDescriptor = new TransformationDescriptor<InputType, OutputType>(function, inputTypeClass, outputTypeClass);
    }

    /**
     * Creates a copied instance.
     *
     * @param that should be copied
     */
    public IteratorSink(IteratorSink that) {
        super(that);
        this.iterator = that.getIterator();
        this.functionDescriptor = that.getTransformation();
        this.outputTypeClass = that.getOutputType();
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

    public void setOutputType(Class<OutputType> outputTypeClass){
        this.outputTypeClass = outputTypeClass;
    }

    public Class<OutputType> getOutputType(){
        return this.outputTypeClass;
    }
}
