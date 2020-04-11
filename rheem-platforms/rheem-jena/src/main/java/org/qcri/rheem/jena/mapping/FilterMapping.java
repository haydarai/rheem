package org.qcri.rheem.jena.mapping;

import org.qcri.rheem.basic.data.Record;
import org.qcri.rheem.basic.operators.FilterOperator;
import org.qcri.rheem.core.mapping.*;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.jena.operators.JenaFilterOperator;
import org.qcri.rheem.jena.platform.JenaPlatform;

import java.util.Collection;
import java.util.Collections;

public class FilterMapping implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(
                this.createSubplanPattern(),
                this.createReplacementSubplanFactory(),
                JenaPlatform.getInstance()
        ));
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern<FilterOperator<Record>> operatorPattern = new OperatorPattern<>(
                "filter", new FilterOperator<>(null, DataSetType.createDefault(Record.class)),
                false
        );
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<FilterOperator>(
                (matchedOperator, epoch) -> new JenaFilterOperator(matchedOperator).at(epoch)
        );
    }
}
