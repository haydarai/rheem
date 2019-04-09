package org.qcri.rheem.serialize.protocol.protobuf.store;

import org.qcri.rheem.serialize.RheemSerialized;
import org.qcri.rheem.serialize.protocol.protobuf.RheemProtoBuf;
import org.qcri.rheem.serialize.store.RheemStoreWriter;
import org.qcri.rheem.serialize.store.repository.RheemRepository;
import org.qcri.rheem.serialize.store.repository.file.RheemRepositoryMultiFile;

public class ProtoBufStoreWriter extends ProtoBufStoreIO implements RheemStoreWriter<RheemProtoBuf.RheemPlanProtoBuf> {



    public ProtoBufStoreWriter() {
        super(new RheemRepositoryMultiFile());
        this.getRepository().open("/Users/notjarvis/IdeaProjects/rheem-experiments/plans");
    }

    @Override
    public boolean save(RheemSerialized<RheemProtoBuf.RheemPlanProtoBuf> write) {
        RheemRepository repo = this.getRepository();

        return repo.write(write);
    }
}
