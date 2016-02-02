package org.qcri.rheem.core.plan.test;


import org.qcri.rheem.core.optimizer.costs.CardinalityEstimate;
import org.qcri.rheem.core.optimizer.costs.CardinalityEstimator;
import org.qcri.rheem.core.optimizer.costs.DefaultCardinalityEstimator;
import org.qcri.rheem.core.plan.*;
import org.qcri.rheem.core.types.DataSetType;

import java.util.Map;
import java.util.Optional;

/**
 * Join-like operator.
 */
public class TestJoin<In1, In2, Out> extends OperatorBase implements ActualOperator {

    public TestJoin(DataSetType<In1> inputType1, DataSetType<In2> inputType2, DataSetType<Out> outputType) {
        super(2, 1, null);
        this.inputSlots[0] = new InputSlot<>("in1", this, inputType1);
        this.inputSlots[1] = new InputSlot<>("in2", this, inputType2);
        this.outputSlots[0] = new OutputSlot<>("out", this, outputType);
    }

    @Override
    public <Payload, Return> Return accept(TopDownPlanVisitor<Payload, Return> visitor, OutputSlot<?> outputSlot, Payload payload) {
        return visitor.visit(this, outputSlot, payload);
    }

    @Override
    public Optional<CardinalityEstimator> getCardinalityEstimator(int outputIndex,
                                                                  Map<OutputSlot<?>, CardinalityEstimate> cache) {
        return Optional.of(new DefaultCardinalityEstimator(
                0.7d,
                2,
                cards -> cards[0] * cards[1],
                this.getOutput(outputIndex),
                cache));
    }
}