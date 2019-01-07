package org.qcri.rheem.utils.udf.type;

import java.io.Serializable;
import java.util.function.Function;

public interface UDFFunction<I, O> extends Function<I, O>, Serializable, UDF {
}
