package org.qcri.rheem.serialize.protocol.protobuf.store;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.serialize.store.RheemStoreFactory;
import org.qcri.rheem.serialize.store.RheemStoreReader;
import org.qcri.rheem.serialize.store.RheemStoreWriter;

public class ProtoBufStoreFactory implements RheemStoreFactory {

    //TODO recovery from configuration
    @Override
    public RheemStoreWriter buildWriter(Configuration conf) {
        return new ProtoBufStoreWriter(conf);
    }

    @Override
    public RheemStoreReader buildReader(Configuration conf) {
        return new ProtoBufStoreReader(conf);
    }
}
