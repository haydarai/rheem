package org.qcri.rheem.serialize;

import java.util.UUID;

public class RheemIdentifier {

    private UUID id;

    public RheemIdentifier(){
        this( UUID.randomUUID());
    }

    public RheemIdentifier(UUID id){
        this.id = id;
    }

    public RheemIdentifier(String str_id){
        this(UUID.fromString(str_id));
    }

    public static RheemIdentifier build(){
        return new RheemIdentifier();
    }

    public UUID getId(){
        return this.id;
    }

}
