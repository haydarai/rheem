package org.qcri.rheem.java.mapping;

import org.qcri.rheem.basic.operators.SnifferOperator;
import org.qcri.rheem.basic.operators.SocketSink;
import org.qcri.rheem.core.mapping.*;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.java.operators.JavaSnifferOperator;
import org.qcri.rheem.java.platform.JavaPlatform;

import java.util.Collection;
import java.util.Collections;

/**
 * Created by bertty on 14-11-17.
 */
public class SocketSinkMapping implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(
                new PlanTransformation(
                  null,//      this.createSubplanPattern(),
                  null, //    this.createReplacementSubplanFactory(),
                        JavaPlatform.getInstance()
                )
        );
    }

    /*private SubplanPattern createSubplanPattern() {
        final OperatorPattern operatorPattern = new OperatorPattern(
                "socketSink", new SocketSink<>(DataSetType.none()), false);
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<SnifferOperator>(
                (matchedOperator, epoch) -> new SocketSink(matchedOperator).at(epoch)
        );
    }*/
}