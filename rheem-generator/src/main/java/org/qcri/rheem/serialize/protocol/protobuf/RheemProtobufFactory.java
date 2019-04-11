package org.qcri.rheem.serialize.protocol.protobuf;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.serialize.protocol.RheemAutoEnconderFactory;
import org.qcri.rheem.serialize.protocol.RheemDecode;
import org.qcri.rheem.serialize.protocol.RheemEncode;
import org.qcri.rheem.serialize.protocol.protobuf.decode.ProtoBufDecode;
import org.qcri.rheem.serialize.protocol.protobuf.encode.ProtobufEncode;

public class RheemProtobufFactory implements RheemAutoEnconderFactory<RheemProtoBuf.RheemPlanProtoBuf> {

    @Override
    public RheemEncode<RheemProtoBuf.RheemPlanProtoBuf> buildEncode(Configuration conf) {
        //TODO: recovery from configuration
        return new ProtobufEncode(conf);
    }

    @Override
    public RheemDecode<RheemProtoBuf.RheemPlanProtoBuf> buildDecode(Configuration conf) {
        //TODO: recovery from configuration
        return new ProtoBufDecode(conf);
    }

    @Override
    public Class<RheemProtoBuf.RheemPlanProtoBuf> getProtocolClass() {
        return RheemProtoBuf.RheemPlanProtoBuf.class;
    }
}
