package org.qcri.rheem.jena.operators;

import org.qcri.rheem.basic.data.Record;
import org.qcri.rheem.basic.function.ProjectionDescriptor;
import org.qcri.rheem.basic.operators.MapOperator;
import org.qcri.rheem.core.types.DataSetType;

public class JenaProjectionOperator extends MapOperator<Record, Record>
        implements JenaExecutionOperator {

    public JenaProjectionOperator(String... fieldNames) {
        super(
                new ProjectionDescriptor<>(Record.class, Record.class, fieldNames),
                DataSetType.createDefault(Record.class),
                DataSetType.createDefault(Record.class)
        );
    }

    public JenaProjectionOperator(ProjectionDescriptor<Record, Record> functionDescriptor) {
        super(functionDescriptor);
    }

    public JenaProjectionOperator(MapOperator<Record, Record> that) {
        super(that);
        if (!(that.getFunctionDescriptor() instanceof ProjectionDescriptor)) {
            throw new IllegalArgumentException("Can only copy from MapOperators with ProjectionDescriptors.");
        }
    }
}
