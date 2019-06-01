package org.qcri.rheem.basic.data.debug.key;

import org.qcri.rheem.basic.data.debug.DebugKey;

public class LongKey extends DebugKey<Long> {
    public static long SERIAL = 0;
    public int correlative = 0;
    public int n_child = 0;

    public LongKey() {
        super();
    }

    public LongKey(Long value, Object... options) {
        super(value, options);
    }

    @Override
    public DebugKey<Long> createChild() {
        LongKey key = new LongKey();
        key.correlative = ++n_child;
        return key.createValue();
    }

    @Override
    protected Long generateValue() {
        return LongKey.SERIAL++;
    }

    @Override
    public DebugKey<Long> build() {
        return new LongKey().createValue();
    }

    @Override
    public DebugKey<Long> build(boolean withDefault) {
        return null;
    }

    @Override
    public DebugKey<Long> plus(DebugKey<Long> other) {
        return null;
    }

    @Override
    public byte[] getBytes() {
        return new byte[0];
    }
}
