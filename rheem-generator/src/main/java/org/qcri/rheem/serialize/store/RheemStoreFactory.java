package org.qcri.rheem.serialize.store;

import org.qcri.rheem.core.api.Configuration;

public interface RheemStoreFactory<Protocol> {

    RheemStoreWriter buildWriter(Configuration conf);

    RheemStoreReader buildReader(Configuration conf);
}
