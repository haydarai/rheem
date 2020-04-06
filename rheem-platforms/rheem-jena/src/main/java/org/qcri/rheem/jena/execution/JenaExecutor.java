package org.qcri.rheem.jena.execution;

import org.qcri.rheem.basic.operators.ModelSource;
import org.qcri.rheem.basic.operators.TableSource;
import org.qcri.rheem.core.api.Job;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.executionplan.ExecutionStage;
import org.qcri.rheem.core.plan.executionplan.ExecutionTask;
import org.qcri.rheem.core.platform.ExecutionState;
import org.qcri.rheem.core.platform.ExecutorTemplate;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.jena.channels.SparqlQueryChannel;
import org.qcri.rheem.jena.platform.JenaPlatform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

public class JenaExecutor extends ExecutorTemplate {

    private final JenaPlatform platform;

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    public JenaExecutor(JenaPlatform platform, Job job) {
        super(job.getCrossPlatformExecutor());
        this.platform = platform;
    }

    @Override
    public void execute(ExecutionStage stage, OptimizationContext optimizationContext, ExecutionState executionState) {
        Collection<?> startTasks = stage.getStartTasks();
        Collection<?> termTasks = stage.getTerminalTasks();

        ExecutionTask startTask = (ExecutionTask) startTasks.toArray()[0];
        ExecutionTask termTask = (ExecutionTask) termTasks.toArray()[0];

        assert startTask.getOperator() instanceof ModelSource : "Invalid Jena stage: Start task has to be a ModelSource";

        ModelSource modelOp = (ModelSource) startTask.getOperator();
        SparqlQueryChannel.Instance tipChannelInstance = this.instantiateOutboundChannel(startTask, optimizationContext);

        tipChannelInstance.setModelUrl(modelOp.getInputUrl());
        tipChannelInstance.setTriple(modelOp.getTriple());

        executionState.register(tipChannelInstance);
    }

    @Override
    public Platform getPlatform() {
        return this.platform;
    }

    private SparqlQueryChannel.Instance instantiateOutboundChannel(ExecutionTask task,
                                                                OptimizationContext optimizationContext) {
        assert task.getNumOuputChannels() == 1 : String.format("Illegal task: %s.", task);
        assert task.getOutputChannel(0) instanceof SparqlQueryChannel : String.format("Illegal task: %s.", task);

        SparqlQueryChannel outputChannel = (SparqlQueryChannel) task.getOutputChannel(0);
        OptimizationContext.OperatorContext operatorContext = optimizationContext.getOperatorContext(task.getOperator());
        return outputChannel.createInstance(this, operatorContext, 0);
    }
}
