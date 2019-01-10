package org.qcri.rheem.experiment.utils.udf.type;

import java.util.function.Predicate;

public interface UDFPredicate<T> extends Predicate<T>, UDF {
}
