package org.qcri.rheem.serialize.signature;

import org.qcri.rheem.core.plan.rheemplan.Operator;

import java.lang.reflect.Constructor;

public interface OperatorConstructor {



    public Operator build(String class_name, Object... objs);

}
