package org.qcri.rheem.serialize;

public abstract class RheemSerialized<Protocol> {

    private Protocol value;

    public abstract Protocol getValue();

    public void setValue(Protocol value){
        this.value = value;
    }

    public abstract byte[] toBytes();
}
