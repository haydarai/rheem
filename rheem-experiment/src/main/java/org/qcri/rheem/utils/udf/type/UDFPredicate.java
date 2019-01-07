package org.qcri.rheem.utils.udf.type;

import java.io.Serializable;
import java.util.function.Predicate;

public interface UDFPredicate<T> extends Predicate<T>, Serializable, UDF {
}
