package org.qcri.rheem.java.operators;

import org.qcri.rheem.basic.operators.FilterOperator;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.function.PredicateDescriptor;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimators;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.platform.lineage.ExecutionLineageNode;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.util.Tuple;
import org.qcri.rheem.java.channels.CollectionChannel;
import org.qcri.rheem.java.channels.JavaChannelInstance;
import org.qcri.rheem.java.channels.StreamChannel;
import org.qcri.rheem.java.execution.JavaExecutor;

import java.util.*;
import java.util.function.Predicate;

/**
 * Java implementation of the {@link FilterOperator}.
 */
public class JavaFilterOperator<Type>
        extends FilterOperator<Type>
        implements JavaExecutionOperator {


    /**
     * Creates a new instance.
     *
     * @param type type of the dataset elements
     */
    public JavaFilterOperator(DataSetType<Type> type, PredicateDescriptor<Type> predicateDescriptor) {
        super(predicateDescriptor, type);
    }

    public JavaFilterOperator(DataSetType<Type> type, PredicateDescriptor.SerializablePredicate<Type> predicateDescriptor) {
        super(type, predicateDescriptor);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public JavaFilterOperator(FilterOperator<Type> that) {
        super(that);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            JavaExecutor javaExecutor,
            OptimizationContext.OperatorContext operatorContext) {

        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        // Counter used to fix the selectivity for profiling the Filter operator
        final int[] counter = {0};
        OptionalLong cardinality = inputs[0].getMeasuredCardinality();
        //long step = (cardinality.getAsLong()*5)/100;

        final Predicate<Type> filterFunction = javaExecutor.getCompiler().compile(this.predicateDescriptor);
        JavaExecutor.openFunction(this, filterFunction, inputs, operatorContext);

        // Check if the profiling evaluation of the operator is enabled
        if (!operatorContext.getOptimizationContext().getConfiguration().getBooleanProperty("rheem.profiling.operatorEvaluation")){
            // Perform normal operator evaluation
            ((StreamChannel.Instance) outputs[0]).accept(((JavaChannelInstance) inputs[0]).<Type>provideStream().filter(filterFunction));
        } else {
            // Perform profiling operator evaluation
            ((StreamChannel.Instance) outputs[0]).accept(((JavaChannelInstance) inputs[0]).<Type>provideStream().filter(el -> {
                // Selectivity is set to 10% of input cardinality
                counter[0]++;
                if (counter[0]%10==0){
                    // Evaluate the predicate
                    if (filterFunction.test(el)==true){
                    }
                    return true;
                }
                // Return false when exceeding the selectivity
                else {
                    // Add Complex Operation
                    return false;
                }

            }));
        }

        return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);
    }


    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluateProfiling(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            JavaExecutor javaExecutor,
            OptimizationContext.OperatorContext operatorContext) {

        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        // Counter used to fix the selectivity for profiling the Filter operator
        final int[] counter = {1};
        OptionalLong cardinality = inputs[0].getMeasuredCardinality();

        final Predicate<Type> filterFunction = javaExecutor.getCompiler().compile(this.predicateDescriptor);
        JavaExecutor.openFunction(this, filterFunction, inputs, operatorContext);
        ((StreamChannel.Instance) outputs[0]).accept(((JavaChannelInstance) inputs[0]).<Type>provideStream().filter(el -> {
            // Selectivity is set to 25% of input cardinality
            if (counter[0]>((int)cardinality.getAsLong()/4))
                // Return false when exceeding the selectivity
                return false;
            else {
                // Check the filter predicate
                if (filterFunction.test(el)) {
                    // increment the counter
                    counter[0]++;
                    return true;
                }else{
                    return false;
                }
            }

        }));

        return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "rheem.java.filter.load";
    }

    @Override
    public Optional<LoadProfileEstimator> createLoadProfileEstimator(Configuration configuration) {
        final Optional<LoadProfileEstimator> optEstimator =
                JavaExecutionOperator.super.createLoadProfileEstimator(configuration);
        LoadProfileEstimators.nestUdfEstimator(optEstimator, this.predicateDescriptor, configuration);
        return optEstimator;
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new JavaFilterOperator<>(this.getInputType(), this.getPredicateDescriptor());
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        assert index <= this.getNumInputs() || (index == 0 && this.getNumInputs() == 0);
        if (this.getInput(index).isBroadcast()) return Collections.singletonList(CollectionChannel.DESCRIPTOR);
        return Arrays.asList(CollectionChannel.DESCRIPTOR, StreamChannel.DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        assert index <= this.getNumOutputs() || (index == 0 && this.getNumOutputs() == 0);
        return Collections.singletonList(StreamChannel.DESCRIPTOR);
    }

}
