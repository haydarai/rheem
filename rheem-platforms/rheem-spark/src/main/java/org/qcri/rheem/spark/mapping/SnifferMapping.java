package org.qcri.rheem.spark.mapping;

import org.qcri.rheem.basic.operators.SnifferOperator;
import org.qcri.rheem.core.data.Tuple;
import org.qcri.rheem.core.mapping.*;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.spark.operators.SparkSnifferOperator;
import org.qcri.rheem.spark.platform.SparkPlatform;

import java.util.Collection;
import java.util.Collections;

/**
 * Created by bertty on 31-05-17.
 */
public class SnifferMapping implements Mapping {
    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(
                this.createSubplanPattern(),
                this.createReplacementSubplanFactory(),
                SparkPlatform.getInstance()
        ));
    }


    private SubplanPattern createSubplanPattern() {
        final OperatorPattern operatorPattern = new OperatorPattern(
                "snifferOperator", new SnifferOperator<Void, Tuple>(DataSetType.none(), DataSetType.createDefault(Tuple.class)), false);
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<SnifferOperator>(
                (matchedOperator, epoch) -> new SparkSnifferOperator(matchedOperator).at(epoch)
        );
    }
}
