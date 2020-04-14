package org.qcri.rheem.jena.operators;

import org.qcri.rheem.basic.data.Record;
import org.qcri.rheem.basic.operators.JoinOperator;
import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.types.DataSetType;

public class JenaJoinOperator extends JoinOperator<Record, Record, String>
        implements JenaExecutionOperator {

    public JenaJoinOperator(DataSetType<Record> inputType0,
                            DataSetType<Record> inputType1,
                            TransformationDescriptor<Record, String> keyDescriptor0,
                            TransformationDescriptor<Record, String> keyDescriptor1) {
        super(keyDescriptor0, keyDescriptor1, inputType0, inputType1);
    }

    public JenaJoinOperator(JoinOperator<Record, Record, String> that) {
        super(that);
    }
}
