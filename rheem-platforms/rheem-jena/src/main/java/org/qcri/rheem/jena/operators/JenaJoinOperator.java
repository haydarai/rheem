package org.qcri.rheem.jena.operators;

import org.qcri.rheem.basic.operators.JoinOperator;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimators;
import org.qcri.rheem.core.types.DataSetType;

import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;

public class JenaJoinOperator<InputType0, InputType1, KeyType> extends JoinOperator<InputType0, InputType1, KeyType>
        implements JenaExecutionOperator {

    public JenaJoinOperator(DataSetType<InputType0> inputType0,
                            DataSetType<InputType1> inputType1,
                            TransformationDescriptor<InputType0, KeyType> keyDescriptor0,
                            TransformationDescriptor<InputType1, KeyType> keyDescriptor1) {
        super(keyDescriptor0, keyDescriptor1, inputType0, inputType1);
    }

    public JenaJoinOperator(JoinOperator<InputType0, InputType1, KeyType> that) {
        super(that);
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "rheem.jena.join.load";
    }

    @Override
    public Collection<String> getLoadProfileEstimatorConfigurationKeys() {
        return Arrays.asList("rheem.jena.join.load");
    }

    @Override
    public Optional<LoadProfileEstimator> createLoadProfileEstimator(Configuration configuration) {
        final Optional<LoadProfileEstimator> optEstimator =
                JenaExecutionOperator.super.createLoadProfileEstimator(configuration);
        LoadProfileEstimators.nestUdfEstimator(optEstimator, this.keyDescriptor0, configuration);
        LoadProfileEstimators.nestUdfEstimator(optEstimator, this.keyDescriptor1, configuration);
        return optEstimator;
    }
}
