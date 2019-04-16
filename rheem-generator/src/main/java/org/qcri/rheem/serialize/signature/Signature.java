package org.qcri.rheem.serialize.signature;

import org.qcri.rheem.basic.operators.LocalCallbackSink;
import org.qcri.rheem.core.plan.rheemplan.Operator;

import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class Signature {

    //TODO we need do this with reflexion

    public static Map<String, OperatorConstructor> SIGNATURES = new HashMap<>();


    static {
        SIGNATURES.put(("org.qcri.rheem.basic.operators.TextFileSource"),
                (class_name, objs) -> {
                    try {
                        return (Operator) Class
                                .forName(class_name)
                                .getConstructor(String.class)
                                .newInstance(objs);
                    } catch (ClassNotFoundException e) {
                        e.printStackTrace();
                    } catch (NoSuchMethodException e) {
                        e.printStackTrace();
                    } catch (IllegalAccessException e) {
                        e.printStackTrace();
                    } catch (InstantiationException e) {
                        e.printStackTrace();
                    } catch (InvocationTargetException e) {
                        e.printStackTrace();
                    }
                    return null;
                }
        );
        SIGNATURES.put(("org.qcri.rheem.basic.operators.FlatMapOperator"),
                (class_name, objs) -> {
                    try {
                        return (Operator) Class
                                .forName(class_name)
                                .getConstructor(
                                        Class.forName("org.qcri.rheem.core.function.FunctionDescriptor$SerializableFunction"),
                                        Class.class,
                                        Class.class
                                ).newInstance(objs);
                    } catch (ClassNotFoundException e) {
                        e.printStackTrace();
                    } catch (NoSuchMethodException e) {
                        e.printStackTrace();
                    } catch (IllegalAccessException e) {
                        e.printStackTrace();
                    } catch (InstantiationException e) {
                        e.printStackTrace();
                    } catch (InvocationTargetException e) {
                        e.printStackTrace();
                    }
                    return null;
                }
        );
        SIGNATURES.put(("org.qcri.rheem.basic.operators.FilterOperator"),
                (class_name, objs) -> {
                    try {
                        return (Operator) Class
                                .forName(class_name)
                                .getConstructor(
                                        Class.forName("org.qcri.rheem.core.function.FunctionDescriptor$SerializablePredicate"),
                                        Class.class
                                ).newInstance(objs);
                    } catch (ClassNotFoundException e) {
                        e.printStackTrace();
                    } catch (NoSuchMethodException e) {
                        e.printStackTrace();
                    } catch (IllegalAccessException e) {
                        e.printStackTrace();
                    } catch (InstantiationException e) {
                        e.printStackTrace();
                    } catch (InvocationTargetException e) {
                        e.printStackTrace();
                    }
                    return null;
                }
        );
        SIGNATURES.put(("org.qcri.rheem.basic.operators.MapOperator"),
                (class_name, objs) -> {
                    try {
                        return (Operator) Class
                                .forName(class_name)
                                .getConstructor(
                                        Class.forName("org.qcri.rheem.core.function.FunctionDescriptor$SerializableFunction"),
                                        Class.class,
                                        Class.class
                                ).newInstance(objs);
                    } catch (ClassNotFoundException e) {
                        e.printStackTrace();
                    } catch (NoSuchMethodException e) {
                        e.printStackTrace();
                    } catch (IllegalAccessException e) {
                        e.printStackTrace();
                    } catch (InstantiationException e) {
                        e.printStackTrace();
                    } catch (InvocationTargetException e) {
                        e.printStackTrace();
                    }
                    return null;
                }
        );
        SIGNATURES.put("org.qcri.rheem.basic.operators.ReduceByOperator",
                (class_name, objs) -> {
                    try {
                        return (Operator) Class
                                .forName(class_name)
                                .getConstructor(
                                        Class.forName("org.qcri.rheem.core.function.FunctionDescriptor$SerializableFunction"),
                                        Class.forName("org.qcri.rheem.core.function.FunctionDescriptor$SerializableBinaryOperator"),
                                        Class.class,
                                        Class.class
                                ).newInstance(objs);
                    } catch (ClassNotFoundException e) {
                        e.printStackTrace();
                    } catch (NoSuchMethodException e) {
                        e.printStackTrace();
                    } catch (IllegalAccessException e) {
                        e.printStackTrace();
                    } catch (InstantiationException e) {
                        e.printStackTrace();
                    } catch (InvocationTargetException e) {
                        e.printStackTrace();
                    }
                    return null;
                }
        );
        //TODO se asume que es a collectionSink
        SIGNATURES.put("org.qcri.rheem.basic.operators.LocalCallbackSink",
                (class_name, objs) -> {
                    try {
                        return (Operator) Class
                                .forName(class_name)
                                .getConstructor(
                                        Class.forName("java.util.function.Consumer"),
                                        Class.class
                                ).newInstance(objs);
                    } catch (ClassNotFoundException e) {
                        e.printStackTrace();
                    } catch (NoSuchMethodException e) {
                        e.printStackTrace();
                    } catch (IllegalAccessException e) {
                        e.printStackTrace();
                    } catch (InstantiationException e) {
                        e.printStackTrace();
                    } catch (InvocationTargetException e) {
                        e.printStackTrace();
                    }
                    return null;
                }
        );
        SIGNATURES.put("org.qcri.rheem.basic.operators.TextFileSink",
                (class_name, objs) -> {
                    try {
                        return (Operator) Class
                                .forName(class_name)
                                .getConstructor(
                                        String.class,
                                        Class.forName("org.qcri.rheem.core.function.FunctionDescriptor$SerializableFunction"),
                                        Class.class
                                ).newInstance(objs);
                    } catch (ClassNotFoundException e) {
                        e.printStackTrace();
                    } catch (NoSuchMethodException e) {
                        e.printStackTrace();
                    } catch (IllegalAccessException e) {
                        e.printStackTrace();
                    } catch (InstantiationException e) {
                        e.printStackTrace();
                    } catch (InvocationTargetException e) {
                        e.printStackTrace();
                    }
                    return null;
                }
        );
        SIGNATURES.put("org.qcri.rheem.basic.operators.ZipWithIdOperator",
                (class_name, objs) -> {
                    try {
                        return (Operator) Class
                                .forName(class_name)
                                .getConstructor(
                                        Class.class
                                ).newInstance(objs);
                    } catch (ClassNotFoundException e) {
                        e.printStackTrace();
                    } catch (NoSuchMethodException e) {
                        e.printStackTrace();
                    } catch (IllegalAccessException e) {
                        e.printStackTrace();
                    } catch (InstantiationException e) {
                        e.printStackTrace();
                    } catch (InvocationTargetException e) {
                        e.printStackTrace();
                    }
                    return null;
                }
        );
    }

    public static OperatorConstructor getConstructor(String name_class){
        return SIGNATURES.get(name_class);
    }
}
