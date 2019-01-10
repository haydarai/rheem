package org.qcri.rheem.experiment.utils.udf.type;

import java.util.function.Function;

public interface UDFFunction<I, O> extends Function<I, O>, UDF {
}
