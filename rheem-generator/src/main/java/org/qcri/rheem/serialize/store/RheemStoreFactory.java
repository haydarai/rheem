package org.qcri.rheem.serialize.store;

public interface RheemStoreFactory<Protocol> {

    RheemStoreWriter buildWriter();

    RheemStoreReader buildReader();
}
