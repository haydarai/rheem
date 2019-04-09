package org.qcri.rheem.serialize.store;

import org.qcri.rheem.serialize.RheemIdentifier;
import org.qcri.rheem.serialize.RheemSerialized;
import org.qcri.rheem.serialize.store.repository.RheemRepository;

public interface RheemStoreReader<Protocol> extends RheemStoreIO {

    RheemSerialized<Protocol> read(RheemIdentifier identifier);


}
