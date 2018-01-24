package org.qcri.rheem.java.mapping;

import org.qcri.rheem.basic.operators.IteratorSource;
import org.qcri.rheem.core.mapping.*;
import org.qcri.rheem.java.operators.JavaIteratorSource;
import org.qcri.rheem.java.platform.JavaPlatform;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link IteratorSourceMapping} to {@link JavaIteratorSource}.
 */
public class IteratorSourceMapping implements Mapping {
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
                "iterator_source", new IteratorSource<>(Void.class, Void.class), false
        );
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<IteratorSource<?, ?>>(
                (matchedOperator, epoch) -> new JavaIteratorSource<>(matchedOperator).at(epoch)
        );
    }
}
