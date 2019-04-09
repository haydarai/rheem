package org.qcri.rheem.serialize.store.repository.file;

import org.qcri.rheem.serialize.RheemIdentifier;
import org.qcri.rheem.serialize.RheemSerialized;
import org.qcri.rheem.serialize.store.repository.RheemRepository;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class RheemRepositoryMultiFile implements RheemRepository {

    File folder;

    @Override
    public boolean open(String seed_path) {
        this.folder = new File(Paths.get(seed_path).toUri());
        if(this.folder.exists() && this.folder.isDirectory()) {
            return true;
        }
        return false;
    }

    @Override
    public boolean write(RheemSerialized<?> serialized) {
        try{
            Files.write(Paths.get(this.folder.getPath(), serialized.getId().getId().toString()), serialized.toBytes());
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public RheemSerialized<?> read(RheemIdentifier identifier) {
        return null;
    }

    @Override
    public boolean close() {
        return false;
    }
}
