package org.qcri.rheem.core.debug;

/**
 * Created by bertty on 20-11-17.
 */
public enum TypeRunMode {
    NORMAL,
    DEBUG;

    public boolean isDebugMode(){
        return this == DEBUG;
    }

    public boolean isNormalMode(){
        return this == NORMAL;
    }
}
