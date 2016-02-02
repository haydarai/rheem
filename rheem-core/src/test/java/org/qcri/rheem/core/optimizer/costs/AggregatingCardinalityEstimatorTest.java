package org.qcri.rheem.core.optimizer.costs;

import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.core.plan.OutputSlot;
import org.qcri.rheem.core.plan.Subplan;
import org.qcri.rheem.core.plan.test.TestJoin;
import org.qcri.rheem.core.plan.test.TestMapOperator;
import org.qcri.rheem.core.types.DataSetType;

import java.util.Arrays;

import static org.mockito.Mockito.mock;

/**
 * Test suite for {@link AggregatingCardinalityEstimator}.
 */
public class AggregatingCardinalityEstimatorTest {

    @Test
    public void testEstimate() {
        CardinalityEstimator partialEstimator1 = new DefaultCardinalityEstimator(0.9, 1, cards -> cards[0] * 2, mock(OutputSlot.class), null);
        CardinalityEstimator partialEstimator2 = new DefaultCardinalityEstimator(0.8, 1, cards -> cards[0] * 3, mock(OutputSlot.class), null);
        CardinalityEstimator estimator = new AggregatingCardinalityEstimator(
                Arrays.asList(partialEstimator1, partialEstimator2),
                mock(OutputSlot.class),
                null);

        CardinalityEstimate inputEstimate = new CardinalityEstimate(10, 100, 0.3);
        CardinalityEstimate outputEstimate = estimator.estimate(mock(RheemContext.class), inputEstimate);
        CardinalityEstimate expectedEstimate = new CardinalityEstimate(2 * 10, 2 * 100, 0.3 * 0.9);

        Assert.assertEquals(expectedEstimate, outputEstimate);
    }
}
