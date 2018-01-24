package org.qcri.rheem.java.mapping;

import org.qcri.rheem.basic.operators.SnifferOperator;
import org.qcri.rheem.core.data.Tuple;
import org.qcri.rheem.core.mapping.*;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.java.operators.JavaSnifferOperator;
import org.qcri.rheem.java.platform.JavaPlatform;
import scala.None;

import java.util.Collection;
import java.util.Collections;

/**
 * Created by bertty on 30-05-17.
 */
public class SnifferMapping implements Mapping {

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
                "snifferOperator", new SnifferOperator<Void, Tuple>(DataSetType.none(), DataSetType.createDefault(Tuple.class)), false);
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<SnifferOperator>(
                (matchedOperator, epoch) -> new JavaSnifferOperator(matchedOperator).at(epoch)
        );
    }
}
