package org.qcri.rheem.jena.mapping;

import org.qcri.rheem.basic.data.Record;
import org.qcri.rheem.basic.function.ProjectionDescriptor;
import org.qcri.rheem.basic.operators.MapOperator;
import org.qcri.rheem.core.mapping.*;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.jena.operators.JenaProjectionOperator;
import org.qcri.rheem.jena.platform.JenaPlatform;

import java.util.Collection;
import java.util.Collections;

public class ProjectionMapping implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(
                this.createSubplanPattern(),
                this.createReplacementSubplanFactory(),
                JenaPlatform.getInstance()
        ));
    }

    private SubplanPattern createSubplanPattern() {
        OperatorPattern<MapOperator<Record, Record>> operatorPattern = new OperatorPattern<>(
                "projection",
                new MapOperator<>(
                        null,
                        DataSetType.createDefault(Record.class),
                        DataSetType.createDefault(Record.class)
                ),
                false
        )
                .withAdditionalTest(op -> op.getFunctionDescriptor() instanceof ProjectionDescriptor)
                .withAdditionalTest(op -> op.getNumInputs() == 1); // No broadcasts.
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<MapOperator<Record, Record>>(
                (matchedOperator, epoch) -> new JenaProjectionOperator(matchedOperator).at(epoch)
        );
    }
}
