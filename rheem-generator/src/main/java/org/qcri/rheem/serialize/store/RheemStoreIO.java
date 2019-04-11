package org.qcri.rheem.serialize.store;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.serialize.store.repository.RheemRepository;

public interface RheemStoreIO {
    public RheemRepository getRepository();

    public void setRepository(RheemRepository repository);

    public Configuration getConfiguration();
}
