package org.qcri.rheem.jena.mapping;

import org.qcri.rheem.basic.operators.JoinOperator;
import org.qcri.rheem.core.mapping.*;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.jena.operators.JenaJoinOperator;
import org.qcri.rheem.jena.platform.JenaPlatform;

import java.util.Collection;
import java.util.Collections;

public class JoinMapping implements Mapping {
    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(
                this.createSubplanPattern(),
                this.createReplacementSubplanFactory(),
                JenaPlatform.getInstance()
        ));
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern operatorPattern = new OperatorPattern<>(
                "join", new JoinOperator<>(null, null, DataSetType.none(), DataSetType.none()), false
        );
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<JoinOperator<Object, Object, Object>>(
                (matchedOperator, epoch) -> new JenaJoinOperator(matchedOperator).at(epoch)
        );
    }
}
