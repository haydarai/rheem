package org.qcri.rheem.serialize.protocol.protobuf.decode;

import com.google.protobuf.ByteString;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.plan.rheemplan.RheemPlan;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.flink.Flink;
import org.qcri.rheem.java.Java;
import org.qcri.rheem.serialize.RheemSerialized;

import org.qcri.rheem.serialize.protocol.RheemDecode;
import org.qcri.rheem.serialize.protocol.protobuf.RheemProtoBuf;
import org.qcri.rheem.serialize.protocol.protobuf.RheemProtoBuf.RheemPlanProtoBuf;
import org.qcri.rheem.serialize.protocol.protobuf.RheemProtoBuf.RheemPlanProtoBuf.RheemOperatorProtoBuf;
import org.qcri.rheem.serialize.protocol.protobuf.RheemProtoBuf.RheemPlanProtoBuf.RheemSlotProtoBuf;
import org.qcri.rheem.serialize.protocol.protobuf.RheemProtoBuf.RheemPlanProtoBuf.RheemPlatformProtoBuf;
import org.qcri.rheem.serialize.signature.OperatorConstructor;
import org.qcri.rheem.serialize.signature.Signature;
import org.qcri.rheem.spark.Spark;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toMap;

public class ProtoBufDecode implements RheemDecode<RheemProtoBuf.RheemPlanProtoBuf> {
    private Configuration configuration;

    public ProtoBufDecode(Configuration configuration) {
        this.configuration = configuration;
    }

    @Override
    public RheemPlan decode(RheemSerialized<RheemProtoBuf.RheemPlanProtoBuf> protocol) {
        RheemPlanProtoBuf base_plan = protocol.getValue();
        Map<String, RheemOperatorProtoBuf> original = new HashMap<>();
        Map<RheemOperatorProtoBuf, Operator> operators = base_plan.getOperatorsList()
            .stream()
            .collect(
                toMap(
                    base_op -> {
                        original.put(base_op.getId(), base_op);
                        return base_op;
                    },
                    base_op ->  {
                       return buildOperator(base_op);
                    }
                )
            );


        List<Operator> sinks = operators
            .values()
            .stream()
            .filter(Operator::isSink)
            .collect( Collectors.toList() );

        for(Map.Entry<RheemOperatorProtoBuf, Operator> entry: operators.entrySet()){
            RheemOperatorProtoBuf op_base = entry.getKey();
            Operator op_current = entry.getValue();
            if( ! op_current.isSink() ) {
                for (RheemSlotProtoBuf slot : op_base.getOutputSlotList()) {
                    for (RheemSlotProtoBuf ocupant : slot.getOcupantsList()) {
                        RheemOperatorProtoBuf owner = original.get(ocupant.getOwner());


                        Operator op_next = operators.get(owner);
                        op_current.connectTo(slot.getPosition(), op_next, ocupant.getPosition());
                    }
                }
            }
        }


        return new RheemPlan(sinks.toArray(new Operator[0]));
    }

    private Operator buildOperator(RheemOperatorProtoBuf base_op){
        String base_op_class = base_op.getTypeClass().replaceAll("\\$", ".");
        Map<Integer, Object> params_map = base_op.getParametersList()
               .stream()
               .collect(
                   toMap(
                       param_base -> param_base.getPosition(),
                       param_base -> {
                           try {
                               ByteString bytes = param_base.getValue();
                               ByteArrayInputStream in = new ByteArrayInputStream(bytes.toByteArray());
                               ObjectInputStream is = new ObjectInputStream(in);
                               return is.readObject();
                           } catch (IOException e) {
                               e.printStackTrace();
                           } catch (ClassNotFoundException e) {
                               e.printStackTrace();
                           }
                           return null;
                       }
                   )
               );

        Object[] params_real = new Object[params_map.size()];
        for(int i = 0; i < params_real.length; i++){
            params_real[i] = params_map.get(i);
        }

        OperatorConstructor constructor = Signature.getConstructor(base_op_class);
        Operator op = constructor.build(base_op_class, params_real);
        op.setName(base_op.getName());

        Platform platform_target = getPlatform(base_op.getPlatform());
        if(platform_target != null) {
            op.addTargetPlatform(platform_target);
        }
        return op;
    }

    private Platform getPlatform(RheemPlatformProtoBuf platform_base){
        switch (platform_base){
            case JAVA:
                return Java.platform();
            case SPARK:
                return Spark.platform();
            case FLINK:
                return Flink.platform();
            default:
                return null;

        }
    }


}
