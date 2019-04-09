package org.qcri.rheem.serialize.protocol.protobuf.encode;

import com.google.protobuf.ByteString;
import org.qcri.rheem.basic.operators.FilterOperator;
import org.qcri.rheem.basic.operators.FlatMapOperator;
import org.qcri.rheem.basic.operators.LocalCallbackSink;
import org.qcri.rheem.basic.operators.MapOperator;
import org.qcri.rheem.basic.operators.ReduceByOperator;
import org.qcri.rheem.basic.operators.TextFileSource;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.function.FunctionDescriptor;
import org.qcri.rheem.core.function.PredicateDescriptor;
import org.qcri.rheem.core.plan.rheemplan.InputSlot;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.plan.rheemplan.OutputSlot;
import org.qcri.rheem.core.plan.rheemplan.RheemPlan;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.serialize.RheemSerialized;
import org.qcri.rheem.serialize.graph.RheemTraversal;
import org.qcri.rheem.serialize.protocol.RheemEncode;

import org.qcri.rheem.serialize.protocol.protobuf.ProtoBufSerialized;
import org.qcri.rheem.serialize.protocol.protobuf.RheemProtoBuf;
import org.qcri.rheem.serialize.protocol.protobuf.RheemProtoBuf.RheemPlanProtoBuf;
import org.qcri.rheem.serialize.protocol.protobuf.RheemProtoBuf.RheemPlanProtoBuf.RheemOperatorProtoBuf;
import org.qcri.rheem.serialize.protocol.protobuf.RheemProtoBuf.RheemPlanProtoBuf.RheemSlotProtoBuf;
import org.qcri.rheem.serialize.protocol.protobuf.RheemProtoBuf.RheemPlanProtoBuf.RheemParameterProtoBuf;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ProtobufEncode implements RheemEncode<RheemProtoBuf.RheemPlanProtoBuf> {


    @Override
    public RheemSerialized<RheemProtoBuf.RheemPlanProtoBuf> enconde(RheemPlan plan) {
        RheemTraversal traversal = new RheemTraversal(plan);

        RheemPlanProtoBuf.Builder proto_plan = RheemPlanProtoBuf.newBuilder();
        Map<Operator, RheemOperatorProtoBuf.Builder> op_map = makeMapOperator(traversal);

        for(Operator op: traversal){
            RheemOperatorProtoBuf.Builder op_builder = op_map.get(op);
            InputSlot<?>[] inputs = op.getAllInputs();
            for(int i = 0; i < inputs.length; i++){
                InputSlot input = inputs[i];
                RheemSlotProtoBuf.Builder slot = op_builder.getInputSlotBuilder(input.getIndex());

                OutputSlot slot_ocupant = input.getOccupant();
                RheemOperatorProtoBuf.Builder op_builder_tmp = op_map.get(slot_ocupant.getOwner());
                slot.addOcupants(
                    op_builder_tmp.getOutputSlot(
                            slot_ocupant.getIndex()
                    )
                );
            }

            OutputSlot<?>[] outputs = op.getAllOutputs();
            for(int i = 0; i < outputs.length; i++){
                OutputSlot output = outputs[i];
                RheemSlotProtoBuf.Builder slot = op_builder.getOutputSlotBuilder(output.getIndex());

                List<InputSlot<?>> list_inputs = output.getOccupiedSlots();
                System.out.println(list_inputs);
                for(InputSlot<?>slot_ocupant: list_inputs){
                    RheemOperatorProtoBuf.Builder op_builder_tmp = op_map.get(slot_ocupant.getOwner());
                    slot.addOcupants(
                        op_builder_tmp.getInputSlot(
                            slot_ocupant.getIndex()
                        )
                    );
                }
            }

            proto_plan.addOperators(op_builder);
        }


        return new ProtoBufSerialized(proto_plan);
    }


    private Map<Operator, RheemOperatorProtoBuf.Builder> makeMapOperator(RheemTraversal traversal){
        Map<Operator, RheemOperatorProtoBuf.Builder> op_map = new HashMap<>();
        for(Operator op: traversal){
            RheemOperatorProtoBuf.Builder op_builder = RheemOperatorProtoBuf.newBuilder();
            op_builder.setName(op.getName());
            op_builder.setTypeClass(op.getClass().toString());

            InputSlot<?>[] inputs = op.getAllInputs();
            for(int i = 0; i < inputs.length; i++){
                InputSlot<?> input = inputs[i];

                RheemSlotProtoBuf.Builder slot = RheemSlotProtoBuf.newBuilder();
                slot.setOwner(op_builder);
                slot.setPosition(input.getIndex());

                slot.setType(input.getType().getDataUnitType().getTypeClass().toString());
                op_builder.addInputSlot(input.getIndex(), slot);
            }

            OutputSlot<?>[] outputs = op.getAllOutputs();
            for(int i = 0; i < outputs.length; i++){
                OutputSlot<?> output = outputs[i];

                RheemSlotProtoBuf.Builder slot = RheemSlotProtoBuf.newBuilder();
                slot.setOwner(op_builder);
                slot.setPosition(output.getIndex());

                slot.setType(output.getType().getDataUnitType().getTypeClass().toString());
                op_builder.addOutputSlot(output.getIndex(), slot);
            }
            Set<Platform> platform = op.getTargetPlatforms();
            if(platform.size() > 1){
                throw new RheemException(
                    String.format(
                        "The operator %s of the class %s have more than one platform, the platforms are %s",
                            op.getName(),
                            op.getClass(),
                            Arrays.toString(platform.toArray())
                    )
                );
            }
            this.getParameters(op).stream().forEach(
                rheemParameterProtoBuf -> {
                    op_builder.addParameters(rheemParameterProtoBuf.getPosition(), rheemParameterProtoBuf);
                }
            );

            op_map.put(op, op_builder);
        }
        traversal.restart();
        return op_map;
    }


    private Collection<RheemParameterProtoBuf.Builder> getParameters(Operator op){
        Collection<RheemParameterProtoBuf.Builder> parameters = new ArrayList<>();

        if(op instanceof TextFileSource){
            RheemParameterProtoBuf.Builder param = RheemParameterProtoBuf.newBuilder();
            param.setPosition(0);
            param.setFieldName("inputUrl");
            param.setType(String.class.toString());
            param.setValue(
                this.obj2ByteString(
                    ((TextFileSource)op).getInputUrl()
                )
            );
            parameters.add(param);
        }

        if(op instanceof FlatMapOperator){
            RheemParameterProtoBuf.Builder param = RheemParameterProtoBuf.newBuilder();
            param.setPosition(0);
            param.setFieldName("function");
            param.setType(FunctionDescriptor.SerializableFunction.class.toString());
            param.setValue(
                this.obj2ByteString(
                    ((FlatMapOperator)op).getFunctionDescriptor().getJavaImplementation()
                )
            );
            parameters.add(param);

            param = RheemParameterProtoBuf.newBuilder();
            param.setPosition(1);
            param.setFieldName("inputTypeClass");
            param.setType(Class.class.toString());
            param.setValue(
                this.obj2ByteString(
                    ((FlatMapOperator)op).getInputType().getDataUnitType().getTypeClass()
                )
            );
            parameters.add(param);

            param = RheemParameterProtoBuf.newBuilder();
            param.setPosition(2);
            param.setFieldName("outputTypeClass");
            param.setType(Class.class.toString());
            param.setValue(
                this.obj2ByteString(
                    ((FlatMapOperator)op).getOutputType().getDataUnitType().getTypeClass()
                )
            );
            parameters.add(param);
        }

        if(op instanceof FilterOperator){
            RheemParameterProtoBuf.Builder param = RheemParameterProtoBuf.newBuilder();
            param.setPosition(0);
            param.setFieldName("predicateDescriptor");
            param.setType(PredicateDescriptor.SerializablePredicate.class.toString());
            param.setValue(
                this.obj2ByteString(
                    ((FilterOperator)op).getPredicateDescriptor().getJavaImplementation()
                )
            );
            parameters.add(param);

            param = RheemParameterProtoBuf.newBuilder();
            param.setPosition(1);
            param.setFieldName("typeClass");
            param.setType(Class.class.toString());
            param.setValue(
                    this.obj2ByteString(
                            ((FilterOperator)op).getType().getDataUnitType().getTypeClass()
                    )
            );
            parameters.add(param);
            return null;
        }

        if(op instanceof MapOperator){
            RheemParameterProtoBuf.Builder param = RheemParameterProtoBuf.newBuilder();
            param.setPosition(0);
            param.setFieldName("function");
            param.setType(FunctionDescriptor.SerializableFunction.class.toString());
            param.setValue(
                this.obj2ByteString(
                    ((MapOperator)op).getFunctionDescriptor().getJavaImplementation()
                )
            );
            parameters.add(param);

            param = RheemParameterProtoBuf.newBuilder();
            param.setPosition(1);
            param.setFieldName("inputTypeClass");
            param.setType(Class.class.toString());
            param.setValue(
                this.obj2ByteString(
                    ((MapOperator)op).getInputType().getDataUnitType().getTypeClass()
                )
            );
            parameters.add(param);

            param = RheemParameterProtoBuf.newBuilder();
            param.setPosition(2);
            param.setFieldName("outputTypeClass");
            param.setType(Class.class.toString());
            param.setValue(
                this.obj2ByteString(
                    ((MapOperator)op).getOutputType().getDataUnitType().getTypeClass()
                )
            );
            parameters.add(param);
        }

        if(op instanceof ReduceByOperator){
            RheemParameterProtoBuf.Builder param = RheemParameterProtoBuf.newBuilder();
            param.setPosition(0);
            param.setFieldName("keyFunction");
            param.setType(FunctionDescriptor.SerializableFunction.class.toString());
            param.setValue(
                this.obj2ByteString(
                    ((ReduceByOperator)op).getKeyDescriptor().getJavaImplementation()
                )
            );
            parameters.add(param);


            param = RheemParameterProtoBuf.newBuilder();
            param.setPosition(1);
            param.setFieldName("reduceDescriptor");
            param.setType(FunctionDescriptor.SerializableBinaryOperator.class.toString());
            param.setValue(
                this.obj2ByteString(
                    ((ReduceByOperator)op).getReduceDescriptor().getJavaImplementation()
                )
            );
            parameters.add(param);


            param = RheemParameterProtoBuf.newBuilder();
            param.setPosition(2);
            param.setFieldName("keyClass");
            param.setType(Class.class.toString());
            param.setValue(
                this.obj2ByteString(
                    ((ReduceByOperator)op).getKeyDescriptor().getOutputType().getTypeClass()
                )
            );
            parameters.add(param);

            param = RheemParameterProtoBuf.newBuilder();
            param.setPosition(3);
            param.setFieldName("typeClass");
            param.setType(Class.class.toString());
            param.setValue(
                this.obj2ByteString(
                    ((ReduceByOperator)op).getKeyDescriptor().getInputType().getTypeClass()
                )
            );
            parameters.add(param);
        }

        if(op instanceof LocalCallbackSink){

            return null;
        }
        return parameters;
    }

    private ByteString obj2ByteString(Object obj){
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(obj);
            oos.flush();
            return ByteString.copyFrom(bos.toByteArray());
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}
