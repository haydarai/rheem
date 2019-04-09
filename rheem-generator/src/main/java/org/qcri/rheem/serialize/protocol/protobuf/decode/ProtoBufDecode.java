package org.qcri.rheem.serialize.protocol.protobuf.decode;

import org.qcri.rheem.core.plan.rheemplan.RheemPlan;
import org.qcri.rheem.serialize.RheemSerialized;
import org.qcri.rheem.serialize.protocol.RheemDecode;
import org.qcri.rheem.serialize.protocol.protobuf.RheemProtoBuf;

public class ProtoBufDecode implements RheemDecode<RheemProtoBuf.RheemPlanProtoBuf> {


    @Override
    public RheemPlan decode(RheemSerialized<RheemProtoBuf.RheemPlanProtoBuf> protocol) {
        System.out.println(protocol.getValue());

        return null;
    }
}
