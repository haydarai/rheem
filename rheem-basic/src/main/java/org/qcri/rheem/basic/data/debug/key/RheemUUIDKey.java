package org.qcri.rheem.basic.data.debug.key;

import org.qcri.rheem.basic.data.debug.DebugKey;
import org.qcri.rheem.core.util.RheemUUID;

public class RheemUUIDKey extends DebugKey<RheemUUID> {


    public RheemUUIDKey(){
        super();
    }

    public RheemUUIDKey(boolean withdefault) {
        super(withdefault);
    }

    public RheemUUIDKey(RheemUUID value, Object... options){
        super(value, options);
    }

    @Override
    public DebugKey<RheemUUID> createChild() {
        return new RheemUUIDKey(this.value.createChild());
    }

    @Override
    protected RheemUUID generateValue() {
        return RheemUUID.randomUUID();
    }

    @Override
    public DebugKey<RheemUUID> build(boolean withDefault) {
        return new RheemUUIDKey(withDefault);
    }


    @Override
    public DebugKey<RheemUUID> plus(DebugKey<RheemUUID> other) {
        this.addParents(other.getParents());
        return this;
    }

    @Override
    public byte[] getBytes() {
        return this.getValue().tobyte();
    }

    @Override
    public String toString() {
        if(this.getValue() == null){
            this.setValue(this.generateValue());
        }
        return super.toString();
    }
}
