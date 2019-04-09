package org.qcri.rheem.serialize.store.repository.file;

import org.qcri.rheem.serialize.RheemIdentifier;
import org.qcri.rheem.serialize.RheemSerialized;
import org.qcri.rheem.serialize.store.repository.RheemRepository;

public class RheemRepositoryFile implements RheemRepository {
    @Override
    public boolean open(String seed_path) {
        return false;
    }

    @Override
    public boolean write(RheemSerialized<?> serialized) {
        return false;
    }

    @Override
    public byte[] read(RheemIdentifier identifier) {
        return null;
    }

    @Override
    public boolean close() {
        return false;
    }
}
