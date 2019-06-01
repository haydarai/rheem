package org.qcri.rheem.spark.compiler;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.qcri.rheem.core.function.FlatMapDescriptor;
import org.qcri.rheem.core.function.MapPartitionsDescriptor;
import org.qcri.rheem.core.function.PredicateDescriptor;
import org.qcri.rheem.core.function.ReduceDescriptor;
import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.spark.compiler.adapter.ExtendedFunction;
import org.qcri.rheem.spark.operators.SparkExecutionOperator;

import java.util.Iterator;
import java.util.function.BinaryOperator;

public abstract class MetaFunctionCompiler {
    /**
     * Create an appropriate {@link Function} for deploying the given {@link TransformationDescriptor}
     * on Apache Spark.
     *
     * @param descriptor      describes the transformation function
     * @param operator        that executes the {@link Function}; only required if the {@code descriptor} describes an {@link ExtendedFunction}
     * @param operatorContext contains optimization information for the {@code operator}
     * @param inputs          that feed the {@code operator}; only required if the {@code descriptor} describes an {@link ExtendedFunction}
     */
    public abstract <I, O> Function<I, O> compile(TransformationDescriptor<I, O> descriptor,
                                         SparkExecutionOperator operator,
                                         OptimizationContext.OperatorContext operatorContext,
                                         ChannelInstance[] inputs);

    /**
     * Create an appropriate {@link Function} for deploying the given {@link MapPartitionsDescriptor}
     * on Apache Spark's {@link JavaRDD#mapPartitions(FlatMapFunction)}.
     *
     * @param descriptor      describes the function
     * @param operator        that executes the {@link Function}; only required if the {@code descriptor} describes an {@link ExtendedFunction}
     * @param operatorContext contains optimization information for the {@code operator}
     * @param inputs          that feed the {@code operator}; only required if the {@code descriptor} describes an {@link ExtendedFunction}
     */
    public abstract <I, O> FlatMapFunction<Iterator<I>, O> compile(MapPartitionsDescriptor<I, O> descriptor,
                                                          SparkExecutionOperator operator,
                                                          OptimizationContext.OperatorContext operatorContext,
                                                          ChannelInstance[] inputs) ;

    /**
     * Compile a key extraction.
     *
     * @return a compiled function
     */
    public abstract <T, K> KeyExtractor<T, K> compileToKeyExtractor(TransformationDescriptor<T, K> descriptor);


    /**
     * Create an appropriate {@link FlatMapFunction} for deploying the given {@link FlatMapDescriptor}
     * on Apache Spark.
     *
     * @param descriptor      describes the function
     * @param operator        that executes the {@link Function}; only required if the {@code descriptor} describes an {@link ExtendedFunction}
     * @param operatorContext contains optimization information for the {@code operator}
     * @param inputs          that feed the {@code operator}; only required if the {@code descriptor} describes an {@link ExtendedFunction}
     */
    public abstract <I, O> FlatMapFunction<I, O> compile(FlatMapDescriptor<I, O> descriptor,
                                                SparkExecutionOperator operator,
                                                OptimizationContext.OperatorContext operatorContext,
                                                ChannelInstance[] inputs);
    /**
     * Create an appropriate {@link Function} for deploying the given {@link ReduceDescriptor}
     * on Apache Spark.
     */
    public abstract <T> Function2<T, T, T> compile(ReduceDescriptor<T> descriptor,
                                          SparkExecutionOperator operator,
                                          OptimizationContext.OperatorContext operatorContext,
                                          ChannelInstance[] inputs) ;

    /**
     * Create an appropriate {@link Function}-based predicate for deploying the given {@link PredicateDescriptor}
     * on Apache Spark.
     *
     * @param predicateDescriptor describes the function
     * @param operator            that executes the {@link Function}; only required if the {@code descriptor} describes an {@link ExtendedFunction}
     * @param operatorContext     contains optimization information for the {@code operator}
     * @param inputs              that feed the {@code operator}; only required if the {@code descriptor} describes an {@link ExtendedFunction}
     */
    public abstract <Type> Function<Type, Boolean> compile(
            PredicateDescriptor<Type> predicateDescriptor,
            SparkExecutionOperator operator,
            OptimizationContext.OperatorContext operatorContext,
            ChannelInstance[] inputs);



    public interface KeyExtractor<T, K> extends PairFunction<T, K, T> {}
}
