package org.qcri.rheem.java.mapping.graph;

import org.qcri.rheem.basic.operators.DegreeCentralityOperator;
import org.qcri.rheem.basic.operators.PageRankOperator;
import org.qcri.rheem.core.mapping.*;
import org.qcri.rheem.java.operators.graph.JavaDegreeCentralityOperator;
import org.qcri.rheem.java.operators.graph.JavaPageRankOperator;
import org.qcri.rheem.java.platform.JavaPlatform;

import java.util.Collection;
import java.util.Collections;

public class DegreeCentralityMapping implements Mapping {
    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(
                this.createSubplanPattern(), this.createReplacementSubplanFactory(), JavaPlatform.getInstance()
        ));
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern operatorPattern = new OperatorPattern<>(
                "degreeCentrality", new DegreeCentralityOperator(), false
        );
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<DegreeCentralityOperator>(
                (matchedOperator, epoch) -> new JavaDegreeCentralityOperator(matchedOperator).at(epoch)
        );
    }
}
