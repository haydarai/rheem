package org.qcri.rheem.profiler.core.api;

import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.plan.rheemplan.Operator;

import java.util.ArrayList;
import java.util.List;

/**
 * Basic implementation of {@link OperatorProfiler}
 */
public class OperatorProfilerBase extends OperatorProfiler {

    Operator rheemOperator;
    /**
     *
     * @param operator
     */
    public OperatorProfilerBase(Operator operator){
        this.rheemOperator = operator;
        //this.setOperator(executionOperator);
    }
    @Override
    protected void prepareInput(int inputIndex, long dataQuantaSize, long inputCardinality) {

    }
}
