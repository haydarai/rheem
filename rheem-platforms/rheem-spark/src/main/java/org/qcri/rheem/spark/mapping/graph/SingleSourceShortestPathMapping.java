package org.qcri.rheem.spark.mapping.graph;

import org.qcri.rheem.basic.operators.SingleSourceShortestPathOperator;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.mapping.*;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.spark.platform.SparkPlatform;

import java.lang.reflect.Constructor;
import java.util.Collection;
import java.util.Collections;

public class SingleSourceShortestPathMapping implements Mapping {
    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(
                this.createSubplanPattern(),
                this.createReplacementSubplanFactory(),
                SparkPlatform.getInstance()
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
                (matchedOperator, epoch) -> {
                    // We need to instantiate the SparkPageRankOperator via reflection, because the Scala code will
                    // be compiled only after the Java code, which might cause compile errors.
                    try {
                        final Class<?> cls = Class.forName("org.qcri.rheem.spark.operators.graph.SparkSingleSourceShortestPathOperator");
                        final Constructor<?> constructor = cls.getConstructor(SingleSourceShortestPathOperator.class);
                        return (Operator) constructor.newInstance(matchedOperator);
                    } catch (Exception e) {
                        throw new RheemException(String.format("Could not apply %s.", this), e);
                    }
                }
        );
    }
}
