package org.qcri.rheem.jena.operators;

import org.qcri.rheem.basic.data.Record;
import org.qcri.rheem.basic.operators.FilterOperator;
import org.qcri.rheem.core.function.PredicateDescriptor;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;

public class JenaFilterOperator<InputType> extends FilterOperator<InputType> implements JenaExecutionOperator {

    public JenaFilterOperator(PredicateDescriptor<InputType> predicateDescriptor) {
        super(predicateDescriptor);
    }

    public JenaFilterOperator(FilterOperator<InputType> that) {
        super(that);
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new JenaFilterOperator(this);
    }
}
