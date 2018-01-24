package org.qcri.rheem.ignite.operators;

import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.platform.lineage.ExecutionLineageNode;
import org.qcri.rheem.core.util.Tuple;
import org.qcri.rheem.ignite.execution.IgniteExecutor;
import org.qcri.rheem.ignite.platform.IgnitePlatform;

import java.io.Serializable;
import java.util.Collection;

/**
 * Execution operator for the Ignite platform.
 */
public interface IgniteExecutionOperator extends ExecutionOperator, Serializable {
    @Override
    default IgnitePlatform getPlatform() {
        return IgnitePlatform.getInstance();
    }

    Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            IgniteExecutor flinkExecutor,
            OptimizationContext.OperatorContext operatorContext) throws Exception;

    boolean containsAction();
}
