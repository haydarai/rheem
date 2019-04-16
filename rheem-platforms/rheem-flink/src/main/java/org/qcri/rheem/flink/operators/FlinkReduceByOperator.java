package org.qcri.rheem.flink.operators;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.ReduceOperator;
import org.apache.flink.api.java.typeutils.PojoField;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.basic.data.Tuple3;
import org.qcri.rheem.basic.operators.ReduceByOperator;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.function.ReduceDescriptor;
import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimators;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.platform.lineage.ExecutionLineageNode;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.util.Tuple;
import org.qcri.rheem.flink.channels.DataSetChannel;
import org.qcri.rheem.flink.compiler.FunctionCompiler;
import org.qcri.rheem.flink.execution.FlinkExecutor;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

/**
 * Flink implementation of the {@link ReduceByOperator}.
 */
public class FlinkReduceByOperator<InputType, KeyType>
        extends ReduceByOperator<InputType, KeyType>
        implements FlinkExecutionOperator {


    /**
     * Creates a new instance.
     *
     * @param type             type of the reduce elements (i.e., type of {@link #getInput()} and {@link #getOutput()})
     * @param keyDescriptor    describes how to extract the key from data units
     * @param reduceDescriptor describes the reduction to be performed on the elements
     */
    public FlinkReduceByOperator(DataSetType<InputType> type, TransformationDescriptor<InputType, KeyType> keyDescriptor,
                                 ReduceDescriptor<InputType> reduceDescriptor) {
        super(keyDescriptor, reduceDescriptor, type);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public FlinkReduceByOperator(ReduceByOperator<InputType, KeyType> that) {
        super(that);
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            FlinkExecutor flinkExecutor,
            OptimizationContext.OperatorContext operatorContext) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        DataSetChannel.Instance input = (DataSetChannel.Instance) inputs[0];
        DataSetChannel.Instance output = (DataSetChannel.Instance) outputs[0];

        final DataSet<InputType> dataSetInput = input.provideDataSet();

        FunctionCompiler compiler = flinkExecutor.getCompiler();

   //     KeySelector<InputType, KeyType> keySelector = compiler.compileKeySelector(this.keyDescriptor);

        KeySelector<InputType, ?> keySelector;

        if(this.keyDescriptor.getOutputType().getTypeClass() == Tuple2.class) {
            //THIS FOR AGGREGATE
            keySelector = new KeySelectorFunctionTuple<InputType>(this.keyDescriptor);
        }else if (this.keyDescriptor.getOutputType().getTypeClass() == Tuple3.class){
            //THIS FOR JOIN
           keySelector = new KeySelectorFunctionTuple3<InputType>(this.keyDescriptor);
        }else{
            keySelector = compiler.compileKeySelector(this.keyDescriptor);
        }

         ReduceFunction<InputType> reduceFunction = compiler.compile(this.reduceDescriptor);
        //BiFunction<InputType, InputType, InputType> reduce_function = this.reduceDescriptor.getJavaImplementation();



        ReduceOperator<InputType> tmpOutput = dataSetInput
                .groupBy(keySelector)
                .reduce(reduceFunction)
                .returns(TypeInformation.of(this.getOutputType().getDataUnitType().getTypeClass()))
                .setParallelism(flinkExecutor.getNumDefaultPartitions());

        DataSet<InputType> dataSetOutput = tmpOutput;

        output.accept(dataSetOutput, flinkExecutor);

        return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new FlinkReduceByOperator<>(this.getType(), this.getKeyDescriptor(), this.getReduceDescriptor());
    }


    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "rheem.flink.reduceby.load";
    }

    @Override
    public Optional<LoadProfileEstimator> createLoadProfileEstimator(Configuration configuration) {
        final Optional<LoadProfileEstimator> optEstimator =
                FlinkExecutionOperator.super.createLoadProfileEstimator(configuration);
        LoadProfileEstimators.nestUdfEstimator(optEstimator, this.keyDescriptor, configuration);
        LoadProfileEstimators.nestUdfEstimator(optEstimator, this.reduceDescriptor, configuration);
        return optEstimator;
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        return Arrays.asList(DataSetChannel.DESCRIPTOR, DataSetChannel.DESCRIPTOR_MANY);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        assert index <= this.getNumOutputs() || (index == 0 && this.getNumOutputs() == 0);
        return Collections.singletonList(DataSetChannel.DESCRIPTOR);
    }

    @Override
    public boolean containsAction() {
        return false;
    }

    public class KeySelectorFunctionTuple<T> implements KeySelector<T, Tuple2<?, ?>>, ResultTypeQueryable<Tuple2<?, ?>>, Serializable {

        public Function<T, Tuple2<?, ?>> impl;

        public KeySelectorFunctionTuple(TransformationDescriptor transformationDescriptor) {
            this.impl = transformationDescriptor.getJavaImplementation();
        }

        public Tuple2<?, ?> getKey(T object){
            return this.impl.apply(object);
        }

        @Override
        public TypeInformation getProducedType() {
            try{
                return new PojoTypeInfo(Tuple2.class, Arrays.asList(
                        new PojoField(Tuple2.class.getField("field0"),  TypeInformation.of(String.class))
                       // new PojoField(Tuple2.class.getField("field1"),  TypeInformation.of(Tuple2.class.getField("field1").getType() ))
                ));
            } catch (NoSuchFieldException e) {
                e.printStackTrace();
                return TypeInformation.of(new TypeHint<Tuple2<String, String>>(){});
            }
        }
    }

    public class KeySelectorFunctionTuple3<T> implements KeySelector<T, Tuple3<?, ?, ?>>, ResultTypeQueryable<Tuple3<?, ?, ?>>, Serializable {

        public Function<T, Tuple3<?, ?, ?>> impl;

        public KeySelectorFunctionTuple3(TransformationDescriptor transformationDescriptor) {
            this.impl = transformationDescriptor.getJavaImplementation();
        }

        public Tuple3<?, ?, ?> getKey(T object){
            return this.impl.apply(object);
        }

        @Override
        public TypeInformation getProducedType() {
            /*try{
                return new PojoTypeInfo(Tuple3.class, Arrays.asList(
                        new PojoField(Tuple3.class.getField("field0"),  TypeInformation.of(Object.class)),
                        new PojoField(Tuple3.class.getField("field1"),  TypeInformation.of(Object.class)),
                        new PojoField(Tuple3.class.getField("field2"),  TypeInformation.of(Object.class))
                        // new PojoField(Tuple2.class.getField("field1"),  TypeInformation.of(Tuple2.class.getField("field1").getType() ))
                ));
            } catch (NoSuchFieldException e) {
                e.printStackTrace();

             */
                return TypeInformation.of(new TypeHint<Tuple3<?, ?, ?>>(){});
            //}
        }
    }

    public class KeySelectorFunctionKeyType implements KeySelector<InputType, KeyType>, ResultTypeQueryable<KeyType>, Serializable {

        public Function<InputType, KeyType> impl;
        public Class<KeyType> type;

        public KeySelectorFunctionKeyType(TransformationDescriptor transformationDescriptor) {
            this.impl = transformationDescriptor.getJavaImplementation();
            this.type = transformationDescriptor.getOutputType().getTypeClass();
        }

        public KeyType getKey(InputType object){
            return this.impl.apply(object);
        }

        @Override
        public TypeInformation getProducedType() {
            //if(this.type == Tuple3.class){
                return TypeInformation.of(new TypeHint<Tuple3<scala.Long, scala.Int, scala.Int>>(){});
            /*}else {
                return TypeInformation.of(this.type);
            }*/
            /*new PojoTypeInfo(Tuple3.class, Arrays.asList(
                    Arrays.stream(Tuple3.class.getFields()).map(
                            field -> new PojoField(field, TypeInformation.of(Object.class))
                    ).collect(Collectors.toList())
            ));*/
        }
    }

}
