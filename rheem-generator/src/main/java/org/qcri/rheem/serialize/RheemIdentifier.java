package org.qcri.rheem.serialize;

import java.util.UUID;

public class RheemIdentifier {

    UUID id;

    public RheemIdentifier(){
        this( UUID.randomUUID());
    }

    public RheemIdentifier(UUID id){
        this.id = id;
    }

    public static RheemIdentifier build(){
        return new RheemIdentifier();
    }

    public UUID getId(){
        return this.id;
    }

}
