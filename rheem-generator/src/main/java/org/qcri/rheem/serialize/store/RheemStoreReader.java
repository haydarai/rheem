package org.qcri.rheem.serialize.store;

import org.qcri.rheem.serialize.RheemIdentifier;
import org.qcri.rheem.serialize.RheemSerialized;

public interface RheemStoreReader<Protocol> {

    RheemSerialized<Protocol> read(RheemIdentifier identifier);
}
