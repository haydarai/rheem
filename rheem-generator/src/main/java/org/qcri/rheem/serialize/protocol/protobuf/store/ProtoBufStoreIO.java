package org.qcri.rheem.serialize.protocol.protobuf.store;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.serialize.store.RheemStoreIO;
import org.qcri.rheem.serialize.store.repository.RheemRepository;

public abstract class ProtoBufStoreIO implements RheemStoreIO {
    private Configuration configuration;
    private RheemRepository repository;

    public ProtoBufStoreIO(Configuration conf, RheemRepository repository){
        this.configuration = conf;
        this.repository = repository;
    }

    @Override
    public RheemRepository getRepository() {
        return this.repository;
    }

    @Override
    public void setRepository(RheemRepository repository) {
        this.repository = repository;
    }

    @Override
    public Configuration getConfiguration() {
        return this.configuration;
    }
}
