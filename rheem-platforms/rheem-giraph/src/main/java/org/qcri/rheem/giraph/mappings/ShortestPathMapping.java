package org.qcri.rheem.giraph.mappings;

import org.qcri.rheem.basic.operators.PageRankOperator;
import org.qcri.rheem.basic.operators.ShortestPathOperator;
import org.qcri.rheem.core.mapping.*;
import org.qcri.rheem.giraph.operators.GiraphPageRankOperator;
import org.qcri.rheem.giraph.operators.GiraphShortestPathOperator;
import org.qcri.rheem.giraph.platform.GiraphPlatform;

import java.util.Collection;
import java.util.Collections;

public class ShortestPathMapping implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(
                new PlanTransformation(
                        this.createSubplanPattern(),
                        this.createReplacementSubplanFactory(),
                        GiraphPlatform.getInstance()
                )
        );
    }

    @SuppressWarnings("unchecked")
    private SubplanPattern createSubplanPattern() {
        final OperatorPattern operatorPattern = new OperatorPattern(
                "shortestPath", new ShortestPathOperator(), false);
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<ShortestPathOperator>(
                (matchedOperator, epoch) -> new GiraphShortestPathOperator(matchedOperator).at(epoch)
        );
    }
}

