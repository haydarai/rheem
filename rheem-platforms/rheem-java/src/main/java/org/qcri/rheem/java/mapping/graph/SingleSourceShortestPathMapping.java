package org.qcri.rheem.java.mapping.graph;

import org.qcri.rheem.basic.operators.SingleSourceShortestPathOperator;
import org.qcri.rheem.core.mapping.*;
import org.qcri.rheem.java.operators.graph.JavaSingleSourceShortestPathOperator;
import org.qcri.rheem.java.platform.JavaPlatform;

import java.util.Collection;
import java.util.Collections;

public class SingleSourceShortestPathMapping implements Mapping {
    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(
                this.createSubplanPattern(), this.createReplacementSubplanFactory(), JavaPlatform.getInstance()
        ));
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern operatorPattern = new OperatorPattern<>(
                "singleSourceShortestPath", new SingleSourceShortestPathOperator(0), false
        );
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<SingleSourceShortestPathOperator>(
                (matchedOperator, epoch) -> new JavaSingleSourceShortestPathOperator(matchedOperator).at(epoch)
        );
    }
}
