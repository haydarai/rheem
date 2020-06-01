package org.qcri.rheem.jena.execution;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpFilter;
import org.apache.jena.sparql.algebra.op.OpProject;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.expr.*;
import org.qcri.rheem.basic.data.Tuple3;
import org.qcri.rheem.basic.function.ProjectionDescriptor;
import org.qcri.rheem.basic.operators.ModelSource;
import org.qcri.rheem.core.api.Job;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.function.PredicateDescriptor;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ExecutionStage;
import org.qcri.rheem.core.plan.executionplan.ExecutionTask;
import org.qcri.rheem.core.platform.ExecutionState;
import org.qcri.rheem.core.platform.ExecutorTemplate;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.core.util.RheemCollections;
import org.qcri.rheem.jena.channels.SparqlQueryChannel;
import org.qcri.rheem.jena.operators.JenaExecutionOperator;
import org.qcri.rheem.jena.operators.JenaFilterOperator;
import org.qcri.rheem.jena.operators.JenaJoinOperator;
import org.qcri.rheem.jena.operators.JenaProjectionOperator;
import org.qcri.rheem.jena.platform.JenaPlatform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;
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
        ExecutionTask joinTask = null;

        ExecutionTask nextTask = this.findJenaExecutionOperatorTaskInStage(startTask, stage);
        while (nextTask != null) {
            if (nextTask.getOperator() instanceof JenaProjectionOperator) {
                assert projectionTask == null;
                projectionTask = nextTask;
            } else if (nextTask.getOperator() instanceof JenaFilterOperator) {
                filterTasks.add(nextTask);
            } else if (nextTask.getOperator() instanceof JenaJoinOperator) {
                assert joinTask == null;
                joinTask = nextTask;
            } else {
                throw new RheemException(String.format("Unsupported Jena execution task %s", nextTask.toString()));
            }

            // Move the tipChannelInstance.
            tipChannelInstance = this.instantiateOutboundChannel(nextTask, optimizationContext, tipChannelInstance);

            // Go to the next nextTask.
            nextTask = this.findJenaExecutionOperatorTaskInStage(nextTask, stage);
        }

        BasicPattern bp = new BasicPattern();
        List<Tuple3<String, String, String>> variablesTriples = modelOp.getTriples();
        for (Tuple3<String, String, String> variables : variablesTriples) {
            Node[] fields = new Node[3];
            List<String> variableNames = variables.asList();
            for (int i = 0; i < variableNames.size(); i++) {
                String variable = variableNames.get(i);
                try {
                    new URL(variable).toURI();
                    fields[i] = NodeFactory.createURI(variable);
                } catch (Exception e) {
                    fields[i] = Var.alloc(variable);
                }
            }
            Triple triple = new Triple(fields[0], fields[1], fields[2]);
            bp.add(triple);
        }

        Op op = new OpBGP(bp);

        List<String> fieldNames;

        for (ExecutionTask executionTask : filterTasks) {
            JenaFilterOperator operator = (JenaFilterOperator) executionTask.getOperator();
            PredicateDescriptor predicateDescriptor = operator.getPredicateDescriptor();
            String sqlImplementation = predicateDescriptor.getSqlImplementation();
            Expr expr = null;
            Expression conditionExpression = null;
            try {
                conditionExpression = CCJSqlParserUtil.parseCondExpression(sqlImplementation);
            } catch (JSQLParserException e) {
                e.printStackTrace();
            }
            if (conditionExpression instanceof EqualsTo) {
                EqualsTo equalsTo = (EqualsTo) conditionExpression;
                String columnName = ((Column) equalsTo.getLeftExpression()).getColumnName();
                String columnValue = ((StringValue) equalsTo.getRightExpression()).getValue();

                NodeValue nodeValue;
                boolean valueIsIRI;
                try {
                    new URL(columnValue).toURI();
                    valueIsIRI = true;
                } catch (Exception e) {
                    valueIsIRI = false;
                }

                if (valueIsIRI) {
                    nodeValue = NodeValue.makeNode(NodeFactory.createURI(columnValue));
                } else {
                    nodeValue = NodeValue.makeString(columnValue);
                }
                expr = new E_Equals(new ExprVar(Var.alloc(columnName)), nodeValue);
            } else if (conditionExpression instanceof NotEqualsTo) {
                NotEqualsTo notEqualsTo = (NotEqualsTo) conditionExpression;
                String columnName = ((Column) notEqualsTo.getLeftExpression()).getColumnName();
                String columnValue = ((StringValue) notEqualsTo.getRightExpression()).getValue();

                NodeValue nodeValue;
                boolean valueIsIRI;
                try {
                    new URL(columnValue).toURI();
                    valueIsIRI = true;
                } catch (Exception e) {
                    valueIsIRI = false;
                }

                if (valueIsIRI) {
                    nodeValue = NodeValue.makeNode(NodeFactory.createURI(columnValue));
                } else {
                    nodeValue = NodeValue.makeString(columnValue);
                }
                expr = new E_NotEquals(new ExprVar(Var.alloc(columnName)), nodeValue);
            }

            if (expr != null) {
                op = OpFilter.filter(expr, op);
            }
        }

        if (projectionTask != null) {
            JenaProjectionOperator operator = (JenaProjectionOperator) projectionTask.getOperator();
            ProjectionDescriptor projectionDescriptor = (ProjectionDescriptor) operator.getFunctionDescriptor();
            fieldNames = projectionDescriptor.getFieldNames();
            List<Var> projectionFields = fieldNames.stream().map(Var::alloc).collect(Collectors.toList());
            op = new OpProject(op, projectionFields);
        } else {
            fieldNames = modelOp.getVariables().stream().filter(fieldName -> {
                try {
                    new URL(fieldName).toURI();
                    return false;
                } catch (Exception e) {
                    return true;
                }
            }).collect(Collectors.toList());
        }

        tipChannelInstance.setModelUrl(modelOp.getInputUrl());
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
