package org.qcri.rheem.jena.operators;

import org.qcri.rheem.basic.operators.JoinOperator;
import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.types.DataSetType;

public class JenaJoinOperator<InputType0, InputType1, KeyType> extends JoinOperator<InputType0, InputType1, KeyType>
        implements JenaExecutionOperator {

    public JenaJoinOperator(DataSetType<InputType0> inputType0,
                            DataSetType<InputType1> inputType1,
                            TransformationDescriptor<InputType0, KeyType> keyDescriptor0,
                            TransformationDescriptor<InputType1, KeyType> keyDescriptor1) {
        super(keyDescriptor0, keyDescriptor1, inputType0, inputType1);
    }

    public JenaJoinOperator(JoinOperator<InputType0, InputType1, KeyType> that) {
        super(that);
    }
}
