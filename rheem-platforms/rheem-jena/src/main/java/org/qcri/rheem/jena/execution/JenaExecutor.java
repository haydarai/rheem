package org.qcri.rheem.jena.execution;

import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpProject;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Var;
import org.qcri.rheem.basic.data.Record;
import org.qcri.rheem.basic.data.Tuple3;
import org.qcri.rheem.basic.function.ProjectionDescriptor;
import org.qcri.rheem.basic.operators.ModelSource;
import org.qcri.rheem.basic.operators.TableSource;
import org.qcri.rheem.core.api.Job;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.function.FunctionDescriptor;
import org.qcri.rheem.core.function.PredicateDescriptor;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ExecutionStage;
import org.qcri.rheem.core.plan.executionplan.ExecutionTask;
import org.qcri.rheem.core.plan.rheemplan.InputSlot;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.plan.rheemplan.OutputSlot;
import org.qcri.rheem.core.platform.ExecutionState;
import org.qcri.rheem.core.platform.ExecutorTemplate;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.core.types.DataUnitType;
import org.qcri.rheem.core.util.RheemCollections;
import org.qcri.rheem.java.compiler.FunctionCompiler;
import org.qcri.rheem.jena.channels.SparqlQueryChannel;
import org.qcri.rheem.jena.operators.JenaExecutionOperator;
import org.qcri.rheem.jena.operators.JenaFilterOperator;
import org.qcri.rheem.jena.operators.JenaProjectionOperator;
import org.qcri.rheem.jena.platform.JenaPlatform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

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

        ExecutionTask projectionTask = null;
        Collection<ExecutionTask> filterTasks = new ArrayList<>();

        ExecutionTask nextTask = this.findJenaExecutionOperatorTaskInStage(startTask, stage);
        while (nextTask != null) {
            if (nextTask.getOperator() instanceof JenaProjectionOperator) {
                assert projectionTask == null;
                projectionTask = nextTask;
            } else if (nextTask.getOperator() instanceof JenaFilterOperator) {
                filterTasks.add(nextTask);
            } else {
                throw new RheemException(String.format("Unsupported Jena execution task %s", nextTask.toString()));
            }

            // Move the tipChannelInstance.
            tipChannelInstance = this.instantiateOutboundChannel(nextTask, optimizationContext, tipChannelInstance);

            // Go to the next nextTask.
            nextTask = this.findJenaExecutionOperatorTaskInStage(nextTask, stage);
        }

        Tuple3<String, String, String> variables = modelOp.getTriple();
        Triple triple = new Triple(
                Var.alloc(variables.getField0()),
                Var.alloc(variables.getField1()),
                Var.alloc(variables.getField2())
        );

        BasicPattern bp = new BasicPattern();
        bp.add(triple);
        Op op = new OpBGP(bp);

        List<String> fieldNames;

        for (ExecutionTask executionTask : filterTasks) {
            JenaFilterOperator operator = (JenaFilterOperator) executionTask.getOperator();
            // TODO: extract Java/Scala function passed to operator to create respective Jena's operator
        }

        if (projectionTask != null) {
            JenaProjectionOperator operator = (JenaProjectionOperator) projectionTask.getOperator();
            ProjectionDescriptor projectionDescriptor = (ProjectionDescriptor) operator.getFunctionDescriptor();
            fieldNames = projectionDescriptor.getFieldNames();
            List<Var> projectionFields = fieldNames.stream().map(Var::alloc).collect(Collectors.toList());
            op = new OpProject(op, projectionFields);
        } else {
            fieldNames = variables.asList();
        }

        tipChannelInstance.setModelUrl(modelOp.getInputUrl());
        tipChannelInstance.setTriple(modelOp.getTriple());
        tipChannelInstance.setProjectedFields(fieldNames);
        tipChannelInstance.setOp(op);

        executionState.register(tipChannelInstance);
    }

    private ExecutionTask findJenaExecutionOperatorTaskInStage(ExecutionTask task, ExecutionStage stage) {
        assert  task.getNumOuputChannels() == 1;
        final Channel outputChannel = task.getOutputChannel(0);
        final ExecutionTask consumer = RheemCollections.getSingle(outputChannel.getConsumers());
        return consumer.getStage() == stage && consumer.getOperator() instanceof JenaExecutionOperator ?
                consumer :
                null;
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

    private SparqlQueryChannel.Instance instantiateOutboundChannel(ExecutionTask task,
                                                                OptimizationContext optimizationContext,
                                                                   SparqlQueryChannel.Instance predecessorChannelInstance) {
        final SparqlQueryChannel.Instance newInstance = this.instantiateOutboundChannel(task, optimizationContext);
        newInstance.getLineage().addPredecessor(predecessorChannelInstance.getLineage());
        return newInstance;
    }
}
