package org.qcri.rheem.serialize.store;

import org.qcri.rheem.serialize.RheemSerialized;

public interface RheemStoreWriter<Protocol> {

    public boolean save(RheemSerialized<Protocol> write);
}
