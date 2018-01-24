package org.qcri.rheem.ignite.execution;

import org.apache.ignite.Ignite;
import org.qcri.rheem.core.api.Job;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.executionplan.ExecutionTask;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.platform.Executor;
import org.qcri.rheem.core.platform.PartialExecution;
import org.qcri.rheem.core.platform.PushExecutorTemplate;
import org.qcri.rheem.core.platform.lineage.ExecutionLineageNode;
import org.qcri.rheem.core.util.Formats;
import org.qcri.rheem.core.util.Tuple;
import org.qcri.rheem.ignite.compiler.FunctionCompiler;
import org.qcri.rheem.ignite.operators.IgniteExecutionOperator;
import org.qcri.rheem.ignite.platform.IgnitePlatform;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * {@link Executor} implementation for the {@link IgnitePlatform}.
 */
public class IgniteExecutor extends PushExecutorTemplate {

    /**
     * Reference to a {@link Ignite} to be used by this instance.
     */
    private final IgniteContextReference igniteContextReference;

    /**
     * The {@link Ignite} to be used by this instance.
     *
     * @see #igniteContextReference
     */
    public final Ignite ignite;

    /**
     * Compiler to create Spark UDFs.
     */
    public FunctionCompiler compiler = new FunctionCompiler();

    /**
     * Reference to the {@link IgnitePlatform} that provides the {@link #igniteContextReference}.
     */
    private final IgnitePlatform platform;

    /**
     * The requested number of partitions. Should be incorporated by {@link IgniteExecutionOperator}s.
     */
    private final int numDefaultPartitions;

    /**
     * Counts the number of issued Spark actions.
     */
    private int numActions = 0;

    public IgniteExecutor(IgnitePlatform platform, Job job) {
        super(job);
        this.platform = platform;
        this.igniteContextReference = this.platform.getIgniteContext(job);
        this.igniteContextReference.noteObtainedReference();
        this.ignite = this.igniteContextReference.getIgniteEnviroment();
        this.numDefaultPartitions = 1;
    }

    @Override
    protected Tuple<List<ChannelInstance>, PartialExecution> execute(ExecutionTask task,
                                                                     List<ChannelInstance> inputChannelInstances,
                                                                     OptimizationContext.OperatorContext producerOperatorContext,
                                                                     boolean isRequestEagerExecution) {
        // Provide the ChannelInstances for the output of the task.
        final ChannelInstance[] outputChannelInstances = task.getOperator().createOutputChannelInstances(
                this, task, producerOperatorContext, inputChannelInstances
        );

        // Execute.
        final Collection<ExecutionLineageNode> executionLineageNodes;
        final Collection<ChannelInstance> producedChannelInstances;
        // TODO: Use proper progress estimator.
        this.job.reportProgress(task.getOperator().getName(), 50);

        long startTime = System.currentTimeMillis();
        try {
            final Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> results =
                    cast(task.getOperator()).evaluate(
                            toArray(inputChannelInstances),
                            outputChannelInstances,
                            this,
                            producerOperatorContext
                    );
            //Thread.sleep(1000);
            executionLineageNodes = results.getField0();
            producedChannelInstances = results.getField1();
        } catch (Exception e) {
            throw new RheemException(String.format("Executing %s failed.", task), e);
        }
        long endTime = System.currentTimeMillis();
        long executionDuration = endTime - startTime;
        this.job.reportProgress(task.getOperator().getName(), 100);

        // Check how much we executed.
        PartialExecution partialExecution = this.createPartialExecution(executionLineageNodes, executionDuration);
        if (partialExecution != null && cast(task.getOperator()).containsAction()) {
            if (this.numActions == 0) partialExecution.addInitializedPlatform(IgnitePlatform.getInstance());
            this.numActions++;
        }

        if (partialExecution == null && executionDuration > 10) {
            this.logger.warn("Execution of {} took suspiciously long ({}).", task, Formats.formatDuration(executionDuration));
        }

        // Collect any cardinality updates.
        this.registerMeasuredCardinalities(producedChannelInstances);


        // Warn if requested eager execution did not take place.
        if (isRequestEagerExecution && partialExecution == null) {
            this.logger.info("{} was not executed eagerly as requested.", task);
        }

        return new Tuple<>(Arrays.asList(outputChannelInstances), partialExecution);
    }

    private static IgniteExecutionOperator cast(ExecutionOperator executionOperator) {
        return (IgniteExecutionOperator) executionOperator;
    }

    private static ChannelInstance[] toArray(List<ChannelInstance> channelInstances) {
        final ChannelInstance[] array = new ChannelInstance[channelInstances.size()];
        return channelInstances.toArray(array);
    }


    @Override
    public IgnitePlatform getPlatform() {
        return this.platform;
    }

    /**
     * Hint to {@link IgniteExecutionOperator}s on how many partitions they should request.
     *
     * @return the default number of partitions
     */
    public int getNumDefaultPartitions() {
        return this.numDefaultPartitions;
    }

    @Override
    public void dispose() {
        super.dispose();
        this.igniteContextReference.noteDiscardedReference(true);
    }

    /**
     * Provide a {@link FunctionCompiler}.
     *
     * @return the {@link FunctionCompiler}
     */
    public FunctionCompiler getCompiler() {
        return this.compiler;
    }
}
