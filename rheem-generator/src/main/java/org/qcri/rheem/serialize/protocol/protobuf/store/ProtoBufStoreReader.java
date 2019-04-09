package org.qcri.rheem.serialize.protocol.protobuf.store;

import org.qcri.rheem.serialize.RheemIdentifier;
import org.qcri.rheem.serialize.RheemSerialized;
import org.qcri.rheem.serialize.store.RheemStoreReader;
import org.qcri.rheem.serialize.store.repository.file.RheemRepositoryMultiFile;

public class ProtoBufStoreReader extends ProtoBufStoreIO implements RheemStoreReader {


    public ProtoBufStoreReader() {
        super(new RheemRepositoryMultiFile());
        this.getRepository().open("/Users/notjarvis/IdeaProjects/rheem-experiments/plans");
    }

    @Override
    public RheemSerialized read(RheemIdentifier identifier) {
        return null;
    }


}
