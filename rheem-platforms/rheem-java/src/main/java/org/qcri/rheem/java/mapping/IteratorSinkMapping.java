package org.qcri.rheem.java.mapping;

import org.qcri.rheem.basic.operators.IteratorSink;
import org.qcri.rheem.core.mapping.*;
import org.qcri.rheem.java.operators.JavaIteratorSink;
import org.qcri.rheem.java.platform.JavaPlatform;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link IteratorSinkMapping} to {@link JavaIteratorSink}.
 */
public class IteratorSinkMapping implements Mapping {
    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(
                this.createSubplanPattern(),
                this.createReplacementSubplanFactory(),
                JavaPlatform.getInstance()
        ));
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern operatorPattern = new OperatorPattern<>(
                "iterator_sink", new IteratorSink<>(Void.class, Void.class), false
        );
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<IteratorSink<?, ?>>(
                (matchedOperator, epoch) -> new JavaIteratorSink<>(matchedOperator).at(epoch)
        );
    }
}
