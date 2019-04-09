package org.qcri.rheem.serialize;

public abstract class RheemSerialized<Protocol> {

    private RheemIdentifier id;
    private Protocol value;

    public RheemSerialized(){
        this(null);
    }

    public RheemSerialized(Protocol value){
        this(new RheemIdentifier(), value);
    }

    public RheemSerialized(RheemIdentifier id, Protocol value){
        this.id = id;
        this.value = value;
    }

    public RheemIdentifier getId() {
        return id;
    }

    public void setId(RheemIdentifier id) {
        this.id = id;
    }

    public Protocol getValue(){
        return this.value;
    }

    public void setValue(Protocol value){
        this.value = value;
    }

    public abstract byte[] toBytes();

    public abstract void fromBytes(byte[] bytes);


}
