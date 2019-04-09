package org.qcri.rheem.serialize.protocol.protobuf;

import com.google.protobuf.InvalidProtocolBufferException;
import org.qcri.rheem.serialize.RheemIdentifier;
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

    public ProtoBufSerialized(RheemIdentifier id) {
        super(id, null);
    }

    @Override
    public byte[] toBytes() {
        return this.getValue().toByteArray();
    }

    @Override
    public void fromBytes(byte[] bytes) {
        try {
            this.setValue(RheemProtoBuf.RheemPlanProtoBuf.parseFrom(bytes));
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
    }


}
