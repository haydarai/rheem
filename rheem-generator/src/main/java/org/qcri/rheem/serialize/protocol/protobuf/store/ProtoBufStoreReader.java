package org.qcri.rheem.serialize.protocol.protobuf.store;

import org.qcri.rheem.serialize.RheemIdentifier;
import org.qcri.rheem.serialize.RheemSerialized;
import org.qcri.rheem.serialize.protocol.protobuf.ProtoBufSerialized;
import org.qcri.rheem.serialize.protocol.protobuf.RheemProtoBuf;
import org.qcri.rheem.serialize.store.RheemStoreReader;
import org.qcri.rheem.serialize.store.repository.file.RheemRepositoryMultiFile;

public class ProtoBufStoreReader extends ProtoBufStoreIO implements RheemStoreReader<RheemProtoBuf.RheemPlanProtoBuf> {


    public ProtoBufStoreReader() {
        super(new RheemRepositoryMultiFile());
        this.getRepository().open("/Users/notjarvis/IdeaProjects/rheem-experiments/plans");
    }

    @Override
    public RheemSerialized<RheemProtoBuf.RheemPlanProtoBuf> read(RheemIdentifier identifier) {
        ProtoBufSerialized serialized = new ProtoBufSerialized(identifier);
        serialized.fromBytes(this.getRepository().read(identifier));
        return serialized;
    }


}
