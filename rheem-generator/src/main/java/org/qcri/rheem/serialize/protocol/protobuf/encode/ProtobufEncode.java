package org.qcri.rheem.serialize.protocol.protobuf.encode;

import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.plan.rheemplan.RheemPlan;
import org.qcri.rheem.serialize.RheemSerialized;
import org.qcri.rheem.serialize.graph.RheemTraversal;
import org.qcri.rheem.serialize.protocol.RheemEncode;

import org.qcri.rheem.serialize.protocol.protobuf.RheemProtoBuf;
import org.qcri.rheem.serialize.protocol.protobuf.RheemProtoBuf.RheemPlanProtoBuf;
import org.qcri.rheem.serialize.protocol.protobuf.RheemProtoBuf.RheemPlanProtoBuf.RheemOperatorProtoBuf;

public class ProtobufEncode implements RheemEncode<RheemProtoBuf.RheemPlanProtoBuf> {

    @Override
    public RheemSerialized<RheemProtoBuf.RheemPlanProtoBuf> enconde(RheemPlan plan) {
        RheemTraversal traversal = new RheemTraversal(plan);

        RheemPlanProtoBuf.Builder proto_plan = RheemPlanProtoBuf.newBuilder();

        for(Operator op: traversal){
            //proto_plan.addOperators();
        }


        //RheemPlanProtoBuf sus =
        RheemOperatorProtoBuf tmp = RheemOperatorProtoBuf.newBuilder().build();
        return null;
    }

    private void generateOperator(Operator op){
        RheemOperatorProtoBuf.Builder proto_operator = RheemOperatorProtoBuf.newBuilder();
        //proto_operator.
    }
}
