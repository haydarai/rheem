package org.qcri.rheem.basic.data.debug;

public enum DebugTagType {
    MONITOR((byte)1),
    ALERT((byte)2),
    WATCH((byte)3),
    BREAKPOINT((byte)4),
    EXPLORE((byte)5),
    SAMPLE((byte)6),
    LOG((byte)7),
    ML_CHECKPOINT((byte)8),
    CULPRIT((byte)9),
    TRACE((byte)10),
    REPLAY((byte)11),
    VALIDATE((byte)12),
    PROFILE((byte)13),
    EXCEPTION((byte)14),
    SKIP((byte)15),
    FILTER((byte)16),
    PROVENANCE((byte)17),
    PAUSE((byte)18),
    TERMINATE((byte)19),
    DEBUG((byte)20),
    DISPLAY((byte)21),
    REPAIR((byte)22),
    REINJECT((byte)23),
    DELETE((byte)24),
    USER_DEFINE_LOCAL((byte)25),
    USER_DEFINE_REMOTE((byte)26),
    USER_DEFINE_NOT_DELETE((byte)27);

    protected byte value;

    DebugTagType(byte value) {
        this.value = value;
    }

    public byte getValue() {
        return this.value;
    }
}
