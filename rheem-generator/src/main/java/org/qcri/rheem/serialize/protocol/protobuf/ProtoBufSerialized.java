package org.qcri.rheem.serialize.protocol.protobuf;

import org.qcri.rheem.serialize.RheemSerialized;

public class ProtoBufSerialized extends RheemSerialized<RheemProtoBuf.RheemPlanProtoBuf> {


    public ProtoBufSerialized(RheemProtoBuf.RheemPlanProtoBuf.Builder value) {
        super();
        value.setIdentifier(this.getId().getId().toString());
        this.setValue(value.build());
    }

    public ProtoBufSerialized(RheemProtoBuf.RheemPlanProtoBuf value) {
        super(value);
    }

    @Override
    public byte[] toBytes() {
        return this.getValue().toByteArray();
    }
}
