package org.qcri.rheem.java.mapping;

import org.qcri.rheem.basic.operators.MultiplexOperator;
import org.qcri.rheem.core.mapping.*;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.types.TupleType;
import org.qcri.rheem.java.operators.JavaMultiplexOperator;
import org.qcri.rheem.java.platform.JavaPlatform;

import java.util.Collection;
import java.util.Collections;

/**
 * Created by bertty on 09-11-17.
 */
public class MultiplexMapping implements Mapping {
    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(
                new PlanTransformation(
                        this.createSubplanPattern(),
                        this.createReplacementSubplanFactory(),
                        JavaPlatform.getInstance()
                )
        );
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern operatorPattern = new OperatorPattern(
                "multiplex",
                new MultiplexOperator(DataSetType.none(), TupleType.TUPLE_VOID, false),
                false);
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<MultiplexOperator>(
                (matchedOperator, epoch) -> new JavaMultiplexOperator(matchedOperator).at(epoch)
        );
    }
}
