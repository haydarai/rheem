package org.qcri.rheem.serialize.store.repository;

import org.qcri.rheem.serialize.RheemIdentifier;
import org.qcri.rheem.serialize.RheemSerialized;

public interface RheemRepository {

    public boolean open(String seed_path);

    public boolean write(RheemSerialized<?> serialized);

    public RheemSerialized<?> read(RheemIdentifier identifier);

    public boolean close();

}
