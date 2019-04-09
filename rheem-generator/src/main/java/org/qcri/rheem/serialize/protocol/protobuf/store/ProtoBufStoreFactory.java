package org.qcri.rheem.serialize.protocol.protobuf.store;

import org.qcri.rheem.serialize.store.RheemStoreFactory;
import org.qcri.rheem.serialize.store.RheemStoreReader;
import org.qcri.rheem.serialize.store.RheemStoreWriter;

public class ProtoBufStoreFactory implements RheemStoreFactory {

    //TODO recovery from configuration
    @Override
    public RheemStoreWriter buildWriter() {
        return new ProtoBufStoreWriter();
    }

    @Override
    public RheemStoreReader buildReader() {
        return new ProtoBufStoreReader();
    }
}
