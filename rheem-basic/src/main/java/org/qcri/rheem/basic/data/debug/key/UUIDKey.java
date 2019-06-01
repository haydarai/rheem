package org.qcri.rheem.basic.data.debug.key;

import org.qcri.rheem.basic.data.debug.DebugKey;

import java.util.UUID;

public class UUIDKey extends DebugKey<UUID> {

    public UUIDKey(){
        super();
    }

    public UUIDKey(UUID value, Object... options){
        super(value, options);
    }

    @Override
    public DebugKey<UUID> createChild() {
        //TODO implement this method
        return null;
    }

    @Override
    protected UUID generateValue() {
        return UUID.randomUUID();
    }

    @Override
    public DebugKey<UUID> build() {
        return new UUIDKey();
    }

    @Override
    public DebugKey<UUID> build(boolean withDefault) {
        return null;
    }

    @Override
    public DebugKey<UUID> plus(DebugKey<UUID> other) {
        return null;
    }

    @Override
    public byte[] getBytes() {
        return new byte[0];
    }

    @Override
    public String toString() {
        return this.getValue().toString();
    }
}
