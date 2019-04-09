package org.qcri.rheem.serialize.protocol.protobuf.encode;

import org.qcri.rheem.core.api.exception.RheemException;
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

import java.util.Arrays;
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


            op_map.put(op, op_builder);
        }
        traversal.restart();
        return op_map;
    }
}
