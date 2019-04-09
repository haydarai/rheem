package org.qcri.rheem.serialize.store;

import org.qcri.rheem.serialize.RheemSerialized;
import org.qcri.rheem.serialize.store.repository.RheemRepository;

public interface RheemStoreWriter<Protocol>  extends RheemStoreIO {

    public boolean save(RheemSerialized<Protocol> write);

}
