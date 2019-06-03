package org.qcri.rheem.spark.compiler;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.qcri.rheem.basic.data.debug.DebugTuple;
import org.qcri.rheem.core.function.FlatMapDescriptor;
import org.qcri.rheem.core.function.FunctionDescriptor;
import org.qcri.rheem.core.function.MapPartitionsDescriptor;
import org.qcri.rheem.core.function.PredicateDescriptor;
import org.qcri.rheem.core.function.ReduceDescriptor;
import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.spark.compiler.debug.DebugBinaryOperatorAdapter;
import org.qcri.rheem.spark.compiler.debug.DebugBinaryOperatorAdapterDebugTuple;
import org.qcri.rheem.spark.compiler.debug.DebugFlatMapFunctionAdapter;
import org.qcri.rheem.spark.compiler.debug.DebugKeyExtractorAdapter;
import org.qcri.rheem.spark.compiler.debug.DebugMapFunctionAdapter;
import org.qcri.rheem.spark.compiler.debug.DebugMapPartitionsFunctionAdapter;
import org.qcri.rheem.spark.compiler.debug.DebugPredicateAdapater;
import org.qcri.rheem.spark.execution.SparkExecutionContext;
import org.qcri.rheem.spark.operators.SparkExecutionOperator;

import java.util.Iterator;

public class DebugFunctionCompiler extends MetaFunctionCompiler {
    @Override
    public <I, O> Function<I, O> compile(
                                    TransformationDescriptor<I, O> descriptor,
                                    SparkExecutionOperator operator,
                                    OptimizationContext.OperatorContext operatorContext,
                                    ChannelInstance[] inputs
    ) {
        FunctionDescriptor.SerializableFunction<I, O> function =
                (FunctionDescriptor.SerializableFunction<I, O>) descriptor.getJavaImplementation();

        return new DebugMapFunctionAdapter(
            function,
            ((function instanceof FunctionDescriptor.ExtendedSerializableFunction) ?
                new SparkExecutionContext(
                    operator,
                    inputs,
                    operatorContext.getOptimizationContext().getIterationNumber()
                )
                :
                null
            ),
            descriptor.getOutputType().getTypeClass()
        );
    }

    @Override
    public <I, O> FlatMapFunction<Iterator<I>, O> compile(
                                    MapPartitionsDescriptor<I, O> descriptor,
                                    SparkExecutionOperator operator,
                                    OptimizationContext.OperatorContext operatorContext,
                                    ChannelInstance[] inputs
    ) {
        FunctionDescriptor.SerializableFunction<Iterable<I>, Iterable<O>> function =
                (FunctionDescriptor.SerializableFunction<Iterable<I>, Iterable<O>>) descriptor.getJavaImplementation();

        return new DebugMapPartitionsFunctionAdapter<>(
            function,
            (function instanceof FunctionDescriptor.ExtendedSerializableFunction) ?
                new SparkExecutionContext(
                    operator,
                    inputs,
                    operatorContext.getOptimizationContext().getIterationNumber()
                )
            :
                null
        );
    }

    @Override
    public <T, K>  KeyExtractor<T, K> compileToKeyExtractor(TransformationDescriptor<T, K> descriptor) {
        return new DebugKeyExtractorAdapter<T, K>(
                (FunctionDescriptor.SerializableFunction<T, K>) descriptor.getJavaImplementation(),
                descriptor.getOutputType().getTypeClass()
        );
    }

    @Override
    public <I, O> FlatMapFunction<I, O> compile(
                                    FlatMapDescriptor<I, O> descriptor,
                                    SparkExecutionOperator operator,
                                    OptimizationContext.OperatorContext operatorContext,
                                    ChannelInstance[] inputs
    ) {
        FunctionDescriptor.SerializableFunction<I, Iterable<O>> function =
                (FunctionDescriptor.SerializableFunction<I, Iterable<O>>) descriptor.getJavaImplementation();

        return new DebugFlatMapFunctionAdapter(
            function,
            ((function instanceof FunctionDescriptor.ExtendedSerializableFunction) ?
                new SparkExecutionContext(
                    operator,
                    inputs,
                    operatorContext.getOptimizationContext().getIterationNumber()
                )
                :
                null
            ),
            descriptor.getOutputType().getTypeClass()
        );
    }

    @Override
    public <T> Function2<T, T, T> compile(
                                    ReduceDescriptor<T> descriptor,
                                    SparkExecutionOperator operator,
                                    OptimizationContext.OperatorContext operatorContext,
                                    ChannelInstance[] inputs
    ) {
        FunctionDescriptor.SerializableBinaryOperator<T> function =
                (FunctionDescriptor.SerializableBinaryOperator<T>) descriptor.getJavaImplementation();
        /*    return new DebugBinaryOperatorAdapterDebugTuple(
                function,
                ((function instanceof FunctionDescriptor.ExtendedSerializableBinaryOperator) ?
                    new SparkExecutionContext(
                        operator,
                        inputs,
                        operatorContext.getOptimizationContext().getIterationNumber()
                    )
                    :
                    null
                ),
                descriptor.getOutputType().getTypeClass()
            );
         }else {*/
            System.out.println("USING THE CASTING VERSION");
            return new DebugBinaryOperatorAdapter(
                function,
                (function instanceof FunctionDescriptor.ExtendedSerializableBinaryOperator) ?
                    new SparkExecutionContext(
                        operator,
                        inputs,
                        operatorContext.getOptimizationContext().getIterationNumber()
                    )
                    :
                    null
            );
         //}
     }

    @Override
    public <T> Function<T, Boolean> compile(
                                    PredicateDescriptor<T> predicateDescriptor,
                                    SparkExecutionOperator operator,
                                    OptimizationContext.OperatorContext operatorContext,
                                    ChannelInstance[] inputs
    ) {
        PredicateDescriptor.SerializablePredicate<T>  function =
                (PredicateDescriptor.SerializablePredicate<T>) predicateDescriptor.getJavaImplementation();

        return new DebugPredicateAdapater<>(
            function,
            (function instanceof PredicateDescriptor.ExtendedSerializablePredicate) ?
                new SparkExecutionContext(
                    operator,
                    inputs,
                    operatorContext.getOptimizationContext().getIterationNumber()
                )
            :
                null
        );
    }
}
